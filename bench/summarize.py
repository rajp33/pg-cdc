#!/usr/bin/env python3
import argparse
import glob
import json
import os
import statistics

TOOLS = ["pg-cdc", "sequin", "debezium", "pgcdc"]
NAMES = {"pg-cdc": "pg-cdc", "sequin": "Sequin", "debezium": "Debezium Server", "pgcdc": "pgcdc (webhook)"}


def load(root):
    runs, calibs = [], []
    for p in sorted(glob.glob(os.path.join(root, "**", "result.json"), recursive=True)):
        r = json.load(open(p))
        r["_dir"] = os.path.dirname(p)
        runs.append(r)
    for p in sorted(glob.glob(os.path.join(root, "**", "calibration.json"), recursive=True)):
        c = json.load(open(p))
        c["_dir"] = os.path.dirname(p)
        calibs.append(c)
    return runs, calibs


def f(x, d=0):
    return "-" if x is None else f"{x:,.{d}f}"


def tool_window(r):
    return (r.get("window") or {}).get("cdcb-tool") or {}


def cpu_us_per_event(r):
    w, s = tool_window(r), r.get("summary") or {}
    return w["cpu_seconds"] * 1e6 / s["distinct"] if w.get("cpu_seconds") is not None and s.get("distinct") else None


def cores(r):
    w = tool_window(r)
    return w["cpu_seconds"] / w["cpu_window_s"] if w.get("cpu_window_s") else None


def entries_per_call(r):
    return (r.get("sns_proxy_stats") or {}).get("entries_per_publish_batch")


def calls_per_s(r):
    n = (r.get("sns_proxy_stats") or {}).get("publish_batch")
    seconds = (r.get("window_s") or 0) + max(r.get("drain_after_load_s") or 0, 0)
    return n / seconds if n and seconds else None


def idle_anon(r):
    return ((r.get("idle") or {}).get("cdcb-tool") or {}).get("peak_anon_mib")


def idle_cores(r):
    i = (r.get("idle") or {}).get("cdcb-tool") or {}
    return i["cpu_seconds"] / i["cpu_window_s"] if i.get("cpu_window_s") else None


def select(runs, tool, scenario, rate=None):
    return sorted((r for r in runs if r["tool"] == tool and r["scenario"] == scenario and (rate is None or r.get("offered_rate") == rate)),
                  key=lambda r: r["run_id"][-15:])


def joined(rs, fn, d=0):
    return " / ".join(f(fn(r), d) for r in rs)


def span(xs, d=0):
    xs = [x for x in xs if x is not None]
    if not xs:
        return "-"
    lo, hi = min(xs), max(xs)
    return f(lo, d) if f(lo, d) == f(hi, d) else f"{f(lo, d)} to {f(hi, d)}"


def correctness(runs):
    bad = [r["run_id"] for r in runs if r.get("error") or not r.get("complete") or (r.get("summary") or {}).get("missing") or (r.get("summary") or {}).get("duplicates")]
    total = sum((r.get("summary") or {}).get("distinct", 0) for r in runs)
    return bad, total


def tables(runs, calibs):
    out = []
    bad, total = correctness(runs)
    out.append(f"{len(runs)} runs, {total:,} rows delivered. Runs with missing rows, duplicates, errors or an incomplete drain: {', '.join(bad) if bad else 'none'}.\n")

    out.append("### Backlog drain, 500,000 rows\n")
    out.append("| Tool | Restart to first event (s) | Restart to last event (s) | Events/s, first to last | Tool CPU-s | CPU µs/event | Peak RSS, anon (MiB) | Message bytes | Entries per PublishBatch |")
    out.append("|---|---|---|---|---|---|---|---|---|")
    for t in TOOLS:
        rs = select(runs, t, "backlog")
        if rs:
            out.append(f"| {NAMES[t]} | {joined(rs, lambda r: r.get('restart_to_first_event_s'), 2)} | {joined(rs, lambda r: r.get('drain_wall_s'), 1)} | "
                       f"{joined(rs, lambda r: r.get('events_per_s_first_to_last'))} | {joined(rs, lambda r: tool_window(r).get('cpu_seconds'), 1)} | "
                       f"{joined(rs, cpu_us_per_event, 1)} | {joined(rs, lambda r: tool_window(r).get('peak_anon_mib'))} | "
                       f"{joined(rs, lambda r: (r.get('summary') or {}).get('avg_body_bytes'))} | {joined(rs, entries_per_call, 1)} |")
    out.append("")

    for rate in sorted({r.get("offered_rate") for r in runs if r["scenario"] == "latency"}):
        out.append(f"### Sustained {rate:,} rows/s for 60 s, commit to delivery\n")
        out.append("| Tool | p50 ms | p95 ms | p99 ms | max ms | Avg cores | CPU µs/event | Peak RSS, anon (MiB) | Entries per PublishBatch | PublishBatch calls/s |")
        out.append("|---|---|---|---|---|---|---|---|---|---|")
        for t in TOOLS:
            rs = select(runs, t, "latency", rate)
            if rs:
                s = lambda k: (lambda r: (r.get("summary") or {}).get(k))
                out.append(f"| {NAMES[t]} | {joined(rs, s('lat_ms_p50'), 1)} | {joined(rs, s('lat_ms_p95'), 1)} | {joined(rs, s('lat_ms_p99'), 1)} | "
                           f"{joined(rs, s('lat_ms_max'))} | {joined(rs, cores, 2)} | {joined(rs, cpu_us_per_event, 1)} | "
                           f"{joined(rs, lambda r: tool_window(r).get('peak_anon_mib'))} | {joined(rs, entries_per_call, 1)} | {joined(rs, calls_per_s)} |")
        out.append("")

    out.append("### Startup, idle and shutdown\n")
    out.append("| Tool | Runs | Cold start to first event (s) | Restart to first event with backlog (s) | Idle RSS, anon (MiB) | Idle CPU (cores) | SIGTERM to exit (s) | Image (MB) |")
    out.append("|---|---|---|---|---|---|---|---|")
    for t in TOOLS:
        rs = [r for r in runs if r["tool"] == t]
        if rs:
            out.append(f"| {NAMES[t]} | {len(rs)} | {span([r.get('startup_to_first_event_s') for r in rs], 2)} | "
                       f"{span([r.get('restart_to_first_event_s') for r in rs], 2)} | {span([idle_anon(r) for r in rs])} | "
                       f"{span([idle_cores(r) for r in rs], 3)} | {span([r.get('stop_s') for r in rs], 1)} | {f(rs[0]['image_bytes'] / 1e6)} |")
    out.append("")

    seq = [r for r in runs if r["tool"] == "sequin" and r.get("window")]
    if seq:
        out.append("### Sequin's own Postgres and Redis, outside the tool's limits\n")
        out.append("| Run | Postgres CPU-s | Postgres peak (MiB) | Redis CPU-s | Redis peak (MiB) |")
        out.append("|---|---|---|---|---|")
        for r in seq:
            a, b = r["window"].get("cdcb-seqpg", {}), r["window"].get("cdcb-seqredis", {})
            out.append(f"| {r['run_id']} | {f(a.get('cpu_seconds'), 1)} | {f(a.get('peak_mem_mib'))} | {f(b.get('cpu_seconds'), 1)} | {f(b.get('peak_mem_mib'))} |")
        out.append("")

    full = [c for c in calibs if "quick" not in os.path.basename(c["_dir"])]
    if full:
        keys = [k for k in full[0] if isinstance(full[0][k], dict)]
        out.append("### Emulator calibration, one column per full calibration\n")
        out.append("| Path | Body bytes | Concurrency | Rate | " + " | ".join(f"msg/s, call p50/p99 ms ({i + 1})" for i in range(len(full))) + " |")
        out.append("|---|---|---|---|" + "---|" * len(full))
        for k in keys:
            c0 = full[0][k]
            def cell(m):
                text = f"{f(m['messages_per_s'])}, {m['call_ms_p50']:.2f} / {m['call_ms_p99']:.2f}"
                if m["offered_rate"]:
                    text += f"; delivery {m['recorded']['lat_ms_p50']:.2f} / {m['recorded']['lat_ms_p99']:.2f}"
                return text
            cells = [cell(c[k]) for c in full if k in c]
            rate = f(c0["offered_rate"]) if c0["offered_rate"] else "unpaced"
            out.append(f"| {k} | {f(c0['recorded']['avg_body_bytes'] or None)} | {c0['concurrency']} | {rate} | " + " | ".join(cells) + " |")
        out.append("")
    quick = [c for c in calibs if "quick" in os.path.basename(c["_dir"])]
    if quick:
        v = [c["sns_batch10_small_c8"]["calls_per_s"] for c in quick]
        out.append(f"Quick check before each run (PublishBatch of 10, concurrency 8, 5 s): {len(v)} checks, {f(min(v))} to {f(max(v))} calls/s.\n")
    return "\n".join(out)


def chart_data(runs):
    def mean(xs):
        xs = [x for x in xs if x is not None]
        return statistics.mean(xs) if xs else None

    def median(xs):
        xs = [x for x in xs if x is not None]
        return statistics.median(xs) if xs else None

    def deps_anon(r):
        idle = r.get("idle") or {}
        parts = [(idle.get(n) or {}).get("peak_anon_mib") for n in ("cdcb-seqpg", "cdcb-seqredis")]
        return sum(parts) if all(p is not None for p in parts) else None

    out = {}
    for t in TOOLS:
        rs = [r for r in runs if r["tool"] == t]
        lat = select(runs, t, "latency", 3000)
        out[t] = {
            "backlog_cpu_us_per_event": mean(cpu_us_per_event(r) for r in select(runs, t, "backlog")),
            "latency3000_p50_ms": mean((r.get("summary") or {}).get("lat_ms_p50") for r in lat),
            "latency3000_drain_after_load_s": max((r.get("drain_after_load_s") or 0 for r in lat), default=None),
            "idle_rss_anon_mib": median(idle_anon(r) for r in rs),
            "idle_rss_companions_anon_mib": median(deps_anon(r) for r in rs) if t == "sequin" else None,
        }
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("root")
    ap.add_argument("--json", action="store_true", help="print the chart's inputs instead of the tables")
    a = ap.parse_args()
    runs, calibs = load(a.root)
    print(json.dumps(chart_data(runs), indent=1) if a.json else tables(runs, calibs))


if __name__ == "__main__":
    main()
