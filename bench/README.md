# CDC benchmark harness

This directory holds the harness behind [docs/benchmarks.md](../docs/benchmarks.md). It runs pg-cdc, Sequin, Debezium Server and [florinutz/pgcdc](https://github.com/florinutz/pgcdc) one at a time against the same Postgres, with SNS served by [moto](https://github.com/getmoto/moto), and checks that every row arrives exactly once.

## What you need

- Docker with a Unix socket (Docker Desktop, OrbStack, or Docker Engine on Linux). The VM, if there is one, needs at least 8 vCPUs and 6 GiB of memory, because each tool gets 2 CPUs and 2 GiB while Postgres, moto and the receiver run unlimited beside it.
- Python 3.10 or later (standard library only), git, and about 6 GB of disk for images.
- A quiet machine. Close what you can and pause background indexers; the emulator is single-threaded, so host load shows up directly in the numbers.

## Steps

Run these from the repository root.

1. Build the pg-cdc image from a fresh checkout of the commit you want to measure:

   ```sh
   docker build --no-cache -t cdcbench/pg-cdc:bench .
   ```

   The Dockerfile keeps cargo's `target` directory in a BuildKit cache mount, and `--no-cache` does not clear it. On a reused checkout whose sources are older than the cached artifacts, cargo can decide nothing changed and ship a stale binary. A fresh clone avoids that.

2. Pull the other images, build the `benchctl` helper and build the florinutz/pgcdc image:

   ```sh
   bench/setup.sh
   ```

3. Run the suite. Two passes of every scenario for every tool take about an hour:

   ```sh
   bench/suite.sh bench/results/my-run
   ```

   It writes one directory per run under `bench/results/my-run/pass<N>/`, and the tables to `bench/results/my-run/tables.md`.

4. Redraw the README chart from your results:

   ```sh
   python3 bench/summarize.py bench/results/my-run --json > /tmp/chart.json
   python3 bench/chart.py /tmp/chart.json
   ```

A single run, or a single calibration:

```sh
python3 bench/run.py pg-cdc latency --rate 3000
python3 bench/run.py sequin backlog
python3 bench/calibrate.py
```

If a run is interrupted, remove what it left behind:

```sh
docker ps -aq --filter label=cdcbench=1 | xargs docker rm -f
docker network rm cdcbench
```

## Settings

| Variable | Default | Meaning |
|---|---|---|
| `PGCDC_IMAGE` | `cdcbench/pg-cdc:bench` | pg-cdc image under test |
| `FLORINUTZ_PGCDC_IMAGE` | `cdcbench/pgcdc:bench` | florinutz/pgcdc image |
| `FLORINUTZ_PGCDC_REF` | `adea859…` | florinutz/pgcdc commit that `setup.sh` builds |
| `PASSES` | `2` | passes `suite.sh` runs |
| `BENCH_RECV_PORT` | `18080` | host port, bound to 127.0.0.1, where the driver reads the receiver |
| `DOCKER_HOST` | current Docker context | must be a `unix://` socket; container stats are read from it |

`run.py --help` lists the per-run options: rows, rate, duration, idle period and the time budget (300 s by default).

## Files

| Path | What it is |
|---|---|
| `run.py` | Runs one tool through one scenario and writes `result.json` |
| `calibrate.py` | Measures moto through the recording proxy; `--quick` is the 5 s check run before each benchmark run |
| `suite.sh` | The run order used for the published results |
| `summarize.py` | Builds the Markdown tables, or the chart's inputs with `--json` |
| `chart.py` | Writes `docs/benchmarks-light.svg` and `docs/benchmarks-dark.svg` |
| `setup.sh` | Pulls pinned images and builds `bench/bin/benchctl` and the florinutz/pgcdc image |
| `benchctl/` | Go helper: HTTP receiver, recording SNS proxy, load generator, calibration client, moto setup |
| `configs/` | The configuration each tool runs with; `run.py` fills in the slot, publication and endpoints |

## How a run works

Each run gets a fresh moto, a fresh receiver, a fresh `bench_events` table, publication and slot, and for Sequin a fresh Postgres and Redis of its own. The source Postgres container is shared across runs.

- The SNS tools publish to `benchctl recv`'s proxy, which forwards to moto and records an entry only when moto lists it under `Successful`. florinutz/pgcdc posts to the same receiver over HTTP.
- Every row carries `bench_seq` and `bench_ts` (the insert time in µs from Postgres's clock). The receiver parses both out of whatever message format the tool emits, so latency is the arrival time minus `bench_ts`, on the same clock.
- CPU and memory come from the Docker API every 0.5 s. Memory is reported as anonymous RSS, which page cache does not move.
- Every run ends by comparing the delivered sequence numbers with those written; missing rows and duplicates are in each `result.json` and in the first line of `tables.md`.
