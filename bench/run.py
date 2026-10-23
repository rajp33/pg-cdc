#!/usr/bin/env python3
import argparse
import http.client
import json
import os
import signal
import socket
import statistics
import subprocess
import threading
import time
import urllib.request

BENCH = os.path.dirname(os.path.abspath(__file__))
CONFIGS = os.path.join(BENCH, "configs")
NET = "cdcbench"
LBL = ["--label", "cdcbench=1"]
PG, AWS, RECV, TOOL, SEQPG, SEQREDIS = "cdcb-pg", "cdcb-aws", "cdcb-recv", "cdcb-tool", "cdcb-seqpg", "cdcb-seqredis"
RECV_PORT = int(os.environ.get("BENCH_RECV_PORT", "18080"))
LIMITS = ["--cpus", "2", "--memory", "2g", "--memory-swap", "2g"]
# moto closes the connection after every response, so the proxy and clients need wide port reuse or they run out of ephemeral ports.
NETSYS = ["--sysctl", "net.ipv4.ip_local_port_range=1024 65535", "--sysctl", "net.ipv4.tcp_tw_reuse=1"]
TOPIC_ARN = "arn:aws:sns:us-east-1:123456789012:bench"
SNS_PROXY = f"http://{RECV}:4566"
WEBHOOK = f"http://{RECV}:8080/hook"
MOTO, POSTGRES, REDIS, ALPINE = "motoserver/moto:5.2.4", "postgres:16", "redis:7", "alpine:3.22"
IMAGES = {
    "pg-cdc": os.environ.get("PGCDC_IMAGE", "cdcbench/pg-cdc:bench"),
    "debezium": "quay.io/debezium/server:3.7.0.Final",
    "sequin": "sequin/sequin:v0.14.6",
    "pgcdc": os.environ.get("FLORINUTZ_PGCDC_IMAGE", "cdcbench/pgcdc:bench"),
}


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


def sh(args, check=True, timeout=600):
    r = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    if check and r.returncode != 0:
        raise RuntimeError(f"{args} failed: {r.stdout}\n{r.stderr}")
    return r.stdout.strip()


def psql(sql, db="bench", container=PG):
    return sh(["docker", "exec", container, "psql", "-U", "postgres", "-d", db, "-v", "ON_ERROR_STOP=1", "-qAt", "-c", sql])


def rm(*names):
    for n in names:
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)


def running(name):
    r = subprocess.run(["docker", "inspect", "-f", "{{.State.Running}}", name], capture_output=True, text=True)
    return r.returncode == 0 and r.stdout.strip() == "true"


def write(path, text):
    with open(path, "w") as f:
        f.write(text)


def render(name, **values):
    with open(os.path.join(CONFIGS, name)) as f:
        return f.read().format(**values)


def docker_socket():
    host = os.environ.get("DOCKER_HOST") or sh(["docker", "context", "inspect", "-f", "{{.Endpoints.docker.Host}}"])
    if not host.startswith("unix://"):
        raise SystemExit(f"container stats are read over a unix Docker socket, but the Docker host is {host}")
    return host[len("unix://"):]


def ensure_network():
    if subprocess.run(["docker", "network", "inspect", NET], capture_output=True).returncode != 0:
        sh(["docker", "network", "create", *LBL, NET])


def wait_pg(container, db):
    for _ in range(120):
        if subprocess.run(["docker", "exec", container, "pg_isready", "-U", "postgres", "-d", db], capture_output=True).returncode == 0:
            try:
                psql("select 1", db=db, container=container)
                return
            except RuntimeError:
                pass
        time.sleep(0.5)
    raise RuntimeError(f"{container} not ready")


def ensure_pg():
    ensure_network()
    if running(PG):
        return
    rm(PG)
    sh(["docker", "run", "-d", "--name", PG, *LBL, "--network", NET, "-e", "POSTGRES_PASSWORD=postgres", "-e", "POSTGRES_DB=bench",
        POSTGRES, "-c", "wal_level=logical", "-c", "max_replication_slots=32", "-c", "max_wal_senders=32",
        "-c", "max_wal_size=4GB", "-c", "max_connections=200"])
    wait_pg(PG, "bench")


def benchctl(args, timeout=600):
    return sh(["docker", "run", "--rm", *LBL, *NETSYS, "--network", NET, "-v", f"{BENCH}/bin:/b:ro", ALPINE, "/b/benchctl", *args], timeout=timeout)


def container_ip(name):
    return sh(["docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", name])


def fresh_aws(bucket=None):
    rm(AWS)
    sh(["docker", "run", "-d", "--name", AWS, *LBL, "--network", NET, MOTO])
    for _ in range(120):
        if subprocess.run(["docker", "run", "--rm", *LBL, "--network", NET, ALPINE, "wget", "-qO-", f"http://{AWS}:5000/moto-api/"], capture_output=True).returncode == 0:
            break
        time.sleep(0.5)
    args = ["setup-aws", "--aws-endpoint", f"http://{AWS}:5000"] + (["--bucket", bucket] if bucket else [])
    out = json.loads(benchctl(args))
    out["aws_ip"] = container_ip(AWS)
    return out


def start_recv():
    rm(RECV)
    sh(["docker", "run", "-d", "--name", RECV, *LBL, *NETSYS, "--network", NET, "-p", f"127.0.0.1:{RECV_PORT}:8080",
        "-v", f"{BENCH}/bin:/b:ro", ALPINE, "/b/benchctl", "recv", "--sns-proxy-listen", ":4566", "--sns-upstream", f"http://{AWS}:5000"])
    for _ in range(60):
        try:
            recv_get("/count")
            return
        except OSError:
            time.sleep(0.25)
    raise RuntimeError("recv not ready")


def recv_get(path):
    with urllib.request.urlopen(f"http://127.0.0.1:{RECV_PORT}{path}", timeout=60) as r:
        return json.loads(r.read())


def prepare_source(run_id):
    tag = run_id.replace("-", "_")
    slot, pub = f"slot_{tag}"[:63], f"pub_{tag}"[:63]
    for s in psql("select slot_name from pg_replication_slots").split():
        psql(f"select pg_terminate_backend(active_pid) from pg_replication_slots where slot_name='{s}' and active_pid is not null")
        time.sleep(0.5)
        psql(f"select pg_drop_replication_slot('{s}')")
    for p in psql("select pubname from pg_publication").split():
        psql(f"drop publication {p}")
    psql("drop table if exists bench_events")
    for t in psql("select tablename from pg_tables where schemaname='public' and tablename like 'pgcdc%'").split():
        psql(f"drop table if exists {t} cascade")
    psql("create table bench_events (id bigserial primary key, bench_seq bigint not null, "
         "bench_ts bigint not null default (extract(epoch from clock_timestamp())*1000000)::bigint, payload text not null)")
    psql(f"create publication {pub} for table public.bench_events")
    psql(f"select pg_create_logical_replication_slot('{slot}', 'pgoutput')")
    psql("checkpoint")
    return slot, pub


class UnixHTTP(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__("localhost", timeout=10)
        self.unix_path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(self.unix_path)


class Sampler(threading.Thread):
    def __init__(self, names, sock, interval=0.5):
        super().__init__(daemon=True)
        self.names, self.sock, self.interval = names, sock, interval
        self.samples = {n: [] for n in names}
        self.stopped = False

    def one(self, name):
        c = UnixHTTP(self.sock)
        c.request("GET", f"/containers/{name}/stats?stream=false&one-shot=true")
        r = c.getresponse()
        body = r.read()
        c.close()
        if r.status != 200:
            return None
        s = json.loads(body)
        m = s.get("memory_stats") or {}
        st = m.get("stats") or {}
        if "usage" not in m:
            return None
        return (time.time(), s["cpu_stats"]["cpu_usage"]["total_usage"], m["usage"] - st.get("inactive_file", 0), st.get("anon", 0))

    def run(self):
        while not self.stopped:
            for n in self.names:
                try:
                    v = self.one(n)
                    if v:
                        self.samples[n].append(v)
                except (OSError, ValueError, KeyError):
                    pass
            time.sleep(self.interval)

    def window(self, name, ta, tb):
        xs = self.samples[name]
        inw = [x for x in xs if ta <= x[0] <= tb]
        before = [x for x in xs if x[0] <= ta]
        after = [x for x in xs if x[0] >= tb]
        a = before[-1] if before else (inw[0] if inw else None)
        b = after[0] if after else (inw[-1] if inw else None)
        if not a or not b:
            return {}
        pool = inw or [a, b]
        return {
            "cpu_seconds": (b[1] - a[1]) / 1e9,
            "cpu_window_s": b[0] - a[0],
            "peak_mem_mib": max(x[2] for x in pool) / 2**20,
            "peak_anon_mib": max(x[3] for x in pool) / 2**20,
            "median_mem_mib": statistics.median(x[2] for x in pool) / 2**20,
        }


def tool_containers(tool):
    return [TOOL] + ([SEQPG, SEQREDIS] if tool == "sequin" else [])


def start_tool(tool, rd, slot, pub, aws):
    base = ["docker", "run", "-d", "--name", TOOL, *LBL, "--network", NET, *LIMITS]
    awsenv = ["-e", "AWS_REGION=us-east-1", "-e", "AWS_ACCESS_KEY_ID=test", "-e", "AWS_SECRET_ACCESS_KEY=test"]
    values = {"pg_host": PG, "slot": slot, "publication": pub, "topic_arn": TOPIC_ARN, "sns_endpoint": SNS_PROXY, "webhook_url": WEBHOOK}
    if tool == "pg-cdc":
        write(f"{rd}/pg-cdc.yaml", render("pg-cdc.yaml", **values))
        # An IP endpoint makes the AWS SDK fall back to path-style S3 addressing, which moto needs.
        s3 = f"http://{aws['aws_ip']}:5000"
        cmd = [*base, *awsenv, "-e", "PGCDC__SOURCE__CREDENTIALS__PASSWORD=postgres", "-e", f"AWS_ENDPOINT_URL_SNS={SNS_PROXY}",
               "-e", f"AWS_ENDPOINT_URL_S3={s3}", "-v", f"{rd}/pg-cdc.yaml:/etc/pg-cdc/config.yaml:ro", IMAGES[tool]]
    elif tool == "debezium":
        os.makedirs(f"{rd}/dbz/conf", exist_ok=True)
        os.makedirs(f"{rd}/dbz/data", exist_ok=True)
        os.chmod(f"{rd}/dbz/data", 0o777)
        write(f"{rd}/dbz/conf/application.properties", render("debezium.properties", **values))
        cmd = [*base, *awsenv, "-v", f"{rd}/dbz/conf:/debezium/config:ro", "-v", f"{rd}/dbz/data:/debezium/data", IMAGES[tool]]
    elif tool == "sequin":
        write(f"{rd}/sequin.yaml", render("sequin.yaml", **values))
        cmd = [*base, "-e", f"PG_HOSTNAME={SEQPG}", "-e", "PG_DATABASE=sequin", "-e", "PG_PORT=5432", "-e", "PG_USERNAME=postgres",
               "-e", "PG_PASSWORD=postgres", "-e", "PG_POOL_SIZE=20",
               "-e", "SECRET_KEY_BASE=wDPLYus0pvD6qJhKJICO4dauYPXfO/Yl782Zjtpew5qRBDp7CZvbWtQmY0eB13If",
               "-e", "VAULT_KEY=2Sig69bIpuSm2kv0VQfDekET2qy8qUZGI8v3/h3ASiY=", "-e", f"REDIS_URL=redis://{SEQREDIS}:6379",
               "-e", "CONFIG_FILE_PATH=/config/sequin.yaml", "-v", f"{rd}/sequin.yaml:/config/sequin.yaml:ro", IMAGES[tool]]
    elif tool == "pgcdc":
        args = render("pgcdc.args", **values).split()
        write(f"{rd}/pgcdc.args.json", json.dumps(args))
        cmd = [*base, IMAGES[tool], *args]
    else:
        raise ValueError(tool)
    write(f"{rd}/docker-run.json", json.dumps(cmd))
    return sh(cmd)


def start_sequin_deps():
    rm(SEQPG, SEQREDIS)
    sh(["docker", "run", "-d", "--name", SEQPG, *LBL, "--network", NET, "-e", "POSTGRES_PASSWORD=postgres",
        "-e", "POSTGRES_DB=sequin", POSTGRES, "-c", "wal_level=logical"])
    sh(["docker", "run", "-d", "--name", SEQREDIS, *LBL, "--network", NET, REDIS, "redis-server", "--port", "6379"])
    wait_pg(SEQPG, "sequin")


def wait_seq(seq, timeout):
    t_end = time.time() + timeout
    while time.time() < t_end:
        r = recv_get(f"/seen?seq={seq}")
        if r["recv_us"]:
            return r["recv_us"]
        time.sleep(0.2)
    return None


def wait_delivered(expect, t0, max_wait, stall=60):
    last_n, last_change = -1, time.time()
    while True:
        c = recv_get("/count")
        n = c["distinct"] - (1 if c["warmup_seen"] else 0)
        now = time.time()
        if n != last_n:
            last_n, last_change = n, now
        if n >= expect:
            return True
        if now - t0 > max_wait or now - last_change > stall:
            return False
        time.sleep(1)


def stop_tool(timeout=30):
    t = time.time()
    sh(["docker", "stop", "-t", str(timeout), TOOL], timeout=timeout + 30)
    return time.time() - t


def load(rows, extra):
    return json.loads(benchctl(["load", "--dsn", f"postgres://postgres:postgres@{PG}:5432/bench", "--rows", str(rows), *extra]))


def backlog(a, res, sampler, slot):
    res["stop_s"] = stop_tool()
    log("tool stopped; writing", a.rows, "rows")
    res["load"] = load(a.rows, ["--txn", "100", "--conns", "4"])
    res["slot_lag_bytes_before_start"] = int(psql(f"select pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) from pg_replication_slots where slot_name='{slot}'"))
    log("loaded at", round(res["load"]["rows_per_s"]), "rows/s; restarting tool")
    time.sleep(2)
    t0 = time.time()
    sh(["docker", "start", TOOL])
    res["complete"] = wait_delivered(a.rows, t0, a.max_drain)
    time.sleep(5)
    s = res["summary"] = recv_get(f"/summary?expect={a.rows}&min_seq=1")
    if s["received"]:
        t_first, t_last = s["first_recv_us"] / 1e6, s["last_recv_us"] / 1e6
        res["restart_to_first_event_s"] = t_first - t0
        res["drain_wall_s"] = t_last - t0
        res["events_per_s_incl_restart"] = s["distinct"] / (t_last - t0)
        res["events_per_s_first_to_last"] = s["distinct"] / max(t_last - t_first, 1e-6)
        res["window"] = {n: sampler.window(n, t0, t_last) for n in sampler.names}
        res["window_s"] = t_last - t0


def latency(a, res, sampler):
    rows = a.rate * a.seconds
    res["offered_rate"] = a.rate
    log("offering", a.rate, "rows/s for", a.seconds, "s")
    t0 = time.time()
    res["load"] = load(rows, ["--rate", str(a.rate)])
    t_end = time.time()
    res["complete"] = wait_delivered(rows, t_end, 120)
    time.sleep(3)
    s = res["summary"] = recv_get(f"/summary?expect={rows}&min_seq=1")
    res["window"] = {n: sampler.window(n, t0, t_end) for n in sampler.names}
    res["window_s"] = t_end - t0
    if s["received"]:
        res["drain_after_load_s"] = s["last_recv_us"] / 1e6 - t_end


class Budget(Exception):
    pass


def on_alarm(*_):
    raise Budget("run exceeded its time budget")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("tool", choices=list(IMAGES))
    ap.add_argument("scenario", choices=["backlog", "latency"])
    ap.add_argument("--rows", type=int, default=500_000)
    ap.add_argument("--rate", type=int, default=1000)
    ap.add_argument("--seconds", type=int, default=60)
    ap.add_argument("--max-drain", type=int, default=270)
    ap.add_argument("--idle", type=int, default=20)
    ap.add_argument("--budget", type=int, default=300, help="seconds before the run is abandoned")
    ap.add_argument("--out", default=os.path.join(BENCH, "results"))
    a = ap.parse_args()

    signal.signal(signal.SIGALRM, on_alarm)
    signal.alarm(a.budget)
    t_begin = time.time()
    run_id = f"{a.tool}-{a.scenario}{'' if a.scenario == 'backlog' else a.rate}-{time.strftime('%Y%m%d-%H%M%S')}"
    rd = os.path.join(os.path.abspath(a.out), run_id)
    os.makedirs(rd, exist_ok=True)
    os.chmod(rd, 0o777)
    image = IMAGES[a.tool]
    res = {"run_id": run_id, "tool": a.tool, "scenario": a.scenario, "image": image,
           "image_id": sh(["docker", "image", "inspect", "-f", "{{.Id}}", image]),
           "image_bytes": int(sh(["docker", "image", "inspect", "-f", "{{.Size}}", image])),
           "limits": LIMITS, "idle_s": a.idle, "loadavg_start": os.getloadavg()}
    sampler = Sampler(tool_containers(a.tool), docker_socket())
    log("run", run_id)
    try:
        rm(TOOL, SEQPG, SEQREDIS)
        ensure_pg()
        aws = fresh_aws("pgcdc-state" if a.tool == "pg-cdc" else None)
        start_recv()
        slot, pub = prepare_source(run_id)
        res["slot"], res["publication"] = slot, pub
        if a.tool == "sequin":
            start_sequin_deps()
        psql("insert into bench_events (bench_seq, payload) values (0, 'warmup')")
        t_run = time.time()
        start_tool(a.tool, rd, slot, pub, aws)
        sampler.start()
        warm = wait_seq(0, 120)
        if not warm:
            res["error"] = "warm-up event not delivered within 120 s"
            return
        res["startup_to_first_event_s"] = warm / 1e6 - t_run
        log("first event after", round(res["startup_to_first_event_s"], 2), "s; idling", a.idle, "s")
        time.sleep(a.idle)
        t_idle = time.time()
        res["idle"] = {n: sampler.window(n, t_idle - 10, t_idle) for n in sampler.names}
        if a.scenario == "backlog":
            backlog(a, res, sampler, slot)
        else:
            latency(a, res, sampler)
        s = res["summary"]
        log("summary", json.dumps({k: s[k] for k in ("received", "distinct", "duplicates", "missing", "lat_ms_p50", "lat_ms_p99")}))
    except Budget as e:
        res["error"] = str(e)
        log("error:", e)
    finally:
        signal.alarm(0)
        finish(res, rd, sampler, a.tool)
        res["run_wall_s"] = time.time() - t_begin
        write(f"{rd}/result.json", json.dumps(res, indent=1))
        log("wrote", f"{rd}/result.json", "after", round(res["run_wall_s"]), "s")


def finish(res, rd, sampler, tool):
    sampler.stopped = True
    res["loadavg_end"] = os.getloadavg()
    try:
        ps = recv_get("/proxy-stats")
        if ps.get("publish_batch"):
            ps["entries_per_publish_batch"] = ps["entries"] / ps["publish_batch"]
        res["sns_proxy_stats"] = ps
    except OSError as e:
        res["sns_proxy_stats_error"] = str(e)
    res["samples"] = {n: [[round(x[0], 3), x[1], x[2], x[3]] for x in v] for n, v in sampler.samples.items()}
    for n in tool_containers(tool) + [RECV]:
        r = subprocess.run(["docker", "logs", n], capture_output=True, text=True)
        write(f"{rd}/{n}.log", (r.stdout + r.stderr)[-2_000_000:])
    rm(TOOL, SEQPG, SEQREDIS)


if __name__ == "__main__":
    main()
