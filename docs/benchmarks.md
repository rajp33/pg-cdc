# Benchmarks

pg-cdc compared with three other Postgres change-data-capture tools on one machine, with SNS emulated locally. Every number on this page comes from one session on 2026-10-22, run with the harness in [`bench/`](../bench), and every run delivered every row exactly once.

These are small, local measurements. They are good at showing what each tool costs to run (CPU per event, memory, startup) and whether it delivers correctly. They are weaker at throughput and latency, because the SNS emulator is single-threaded and sits close to the tools' limits; see [Caveats](#caveats) before quoting those.

## Summary

Every one of the 24 runs (4 tools, 3 scenarios, 2 passes) delivered every row it wrote: 5.92 million rows in all, with no rows missing and no duplicates.

Where pg-cdc is ahead of the other SNS tools:

- **CPU.** pg-cdc used 22 µs of CPU per event while draining a 500,000-row backlog. Debezium Server used 97 µs and Sequin 204 µs. At 3,000 rows/s it averaged 0.07 cores, against 0.40 for Sequin and 0.29 for Debezium.
- **Latency at 1,000 and 3,000 rows/s.**
  - At 3,000 rows/s its p50 was 5.9 ms and its p99 44 to 46 ms, against Sequin's 28 ms and 68 to 77 ms.
  - At 1,000 rows/s its p50 was 7.6 to 7.7 ms and its p99 22 ms, against Sequin's 28 to 29 ms and 74 to 76 ms.
  - Debezium's p50 was 1.07 to 1.10 s at 1,000 rows/s. At 3,000 rows/s it fell behind: it delivered about 2,000 rows/s, and its last row arrived 30 s after the load stopped.
- **Memory.** pg-cdc idles at 11 to 12 MiB of anonymous memory and peaked at 53 to 54 MiB while draining the backlog. Sequin idles at 245 to 283 MiB, plus about 51 MiB for its own Postgres and Redis, and Debezium at 299 to 330 MiB. During the drain they peaked at 908 to 919 MiB and 434 to 435 MiB.
- **Startup and shutdown.** pg-cdc delivered its first row 0.31 to 0.35 s after `docker run`, and exited 0.2 s after SIGTERM. Debezium took 3.3 to 3.5 s to start and 0.9 s to stop. Sequin took 9.9 to 10.1 s to start, and did not exit within the 30 s grace period, so Docker killed it.

Where pg-cdc ties or loses:

- **Drain throughput is a tie with Sequin, bound by the emulator.** pg-cdc drained at 5,235 to 5,298 events/s and Sequin at 5,562 to 5,598, about 6% faster. Both run at about 70% of moto's measured ceiling, so the gap does not rank them. pg-cdc restarts faster, 0.4 s against 8.2 s, so counting from the restart it finished first: 94.7 to 95.9 s against 97.5 to 98.0 s. Both drained 2.5 times as fast as Debezium.
- **pg-cdc makes more SNS calls than Sequin.** At 1,000 rows/s it sent 7.4 entries per `PublishBatch`, at 135 calls/s, against Sequin's 8.7 entries at 114 calls/s: about 18% more calls for the same rows. At 3,000 rows/s it made 336 calls/s against 321, about 5% more.
- **Its CPU cost per event is higher at low rates.** At 1,000 rows/s pg-cdc spent 52 µs per event, more than twice its 22 to 24 µs at 3,000 rows/s or during the drain. It was still the lowest of the SNS tools at that rate.
- **pg-cdc's tail is wide compared with its median.** At 3,000 rows/s its p99 (44 to 46 ms) is about 7.5 times its p50, and its slowest row took 119 to 123 ms.
- **pgcdc (webhook) is smaller and starts faster.** pgcdc idles at 10 MiB and starts in 0.21 to 0.28 s. Its webhook drain reached 21,700 to 22,000 events/s at 26 µs per event, slightly more CPU per event than pg-cdc's 22 µs. It delivers over HTTP, so this is not a like-for-like comparison.

## Tools

| Tool | Version | Image | Sink |
|---|---|---|---|
| pg-cdc | 0.2.0 at `dc5aba6`, the head of [#18](https://github.com/theia-vision-inc/pg-cdc/pull/18) (the cross-lane sink batcher), whose tree is identical to `main` at `86c828d`; built from this repo's Dockerfile with `--no-cache` | 23 MB | SNS `PublishBatch` |
| [Sequin](https://github.com/sequinstream/sequin) | v0.14.6, with its own Postgres 16 and Redis 7 | 225 MB | SNS `PublishBatch` |
| [Debezium Server](https://debezium.io/documentation/reference/stable/operations/debezium-server.html) | 3.7.0.Final | 912 MB | SNS `PublishBatch` |
| [pgcdc](https://github.com/florinutz/pgcdc) (florinutz) | `main` at `adea859`, built from its Dockerfile | 26 MB | HTTP webhook, one POST per event |

pgcdc has no SNS sink, so it posts to the benchmark's HTTP receiver instead. Its numbers describe a different, much cheaper sink and are shown for reference, not ranked against the SNS tools.

### Configuration

No tool was tuned. Each ran with its defaults plus the settings its own documentation gives for this use. The exact files are in [`bench/configs/`](../bench/configs); `run.py` fills in the slot, publication and endpoints.

- **pg-cdc:** the README's production shape: an SNS sink and an S3 state store, both on the emulator. Everything else is at its default: 64 lanes, `sink.max_concurrency` 32, a 5 ms batch linger, `max_inflight_bytes` 256 MiB, `order_by: key`.
- **Sequin:** `batch_size: 10`, which every SNS example in Sequin's documentation uses; the built-in default is 1. Its SNS pipeline hard-codes a 1 ms batch timeout and 400 concurrent processors. `message_grouping` is at its default (on). Its Postgres and Redis use the settings from Sequin's docker-compose example, including `PG_POOL_SIZE=20`.
- **Debezium Server:** the `pgoutput` plugin and the SNS sink, with `snapshot.mode=no_data`. The rest is at its defaults, and several of them matter here:
  - the JSON converter includes the schema in every message, which makes each one about 3 KB;
  - `poll.interval.ms` is 500, `max.batch.size` 2048 and `max.queue.size` 8192;
  - the SNS sink sends one 10-entry `PublishBatch` at a time;
  - the image's bundled OpenTelemetry Java agent is on.
- **pgcdc:** `--bus-mode reliable --persistent-slot --cooperative-checkpoint`. Its default `--bus-mode fast` drops events when a subscriber falls behind (it logs "subscriber channel full, dropping event"), and in a trial it lost most of a 500k-row backlog, so `reliable`, its documented no-drop mode, is used here.

## Method

### Machine

- Apple M4 Mac mini (10 cores, 16 GB), macOS 26.3.1.
- OrbStack with Docker 29.4.0; the Linux VM had 10 vCPUs and 7.8 GiB (kernel 7.0.14). Every image ran natively as linux/arm64.
- The Photos and media-analysis daemons (photolibraryd, photoanalysisd, mediaanalysisd, cloudphotod) were paused with SIGSTOP for the whole session. Spotlight and a few idle terminal sessions kept running. The 1-minute load average was 1.3 to 3.6 at the start of each run.

### Setup

- Each tool ran in a container limited to 2 CPUs and 2 GiB (`--cpus 2 --memory 2g --memory-swap 2g`). Postgres, the emulator, the receiver, the load generator, and Sequin's own Postgres and Redis ran without limits; Sequin's companions are reported separately.
- One tool ran at a time.
- **Source:** Postgres 16.15 (`postgres:16`) with `wal_level=logical` and otherwise default durability settings (`fsync=on`, `synchronous_commit=on`, `shared_buffers=128MB`). One container served every run.
- **Table:** `bench_events(id bigserial primary key, bench_seq bigint, bench_ts bigint default <clock_timestamp in µs>, payload text)`, with a 200-character payload, inserts only and the default replica identity.
- **Per run:** a fresh table, publication and slot (all other slots dropped first), a fresh emulator, a fresh receiver, and for Sequin a fresh Postgres and Redis.

### Delivery point

SNS ran on [moto](https://github.com/getmoto/moto) 5.2.4 (`motoserver/moto:5.2.4`). LocalStack's current image requires an auth token, so it was not used.

The SNS tools publish through a recording proxy (`benchctl recv`) that forwards each request to moto and records an entry only when moto lists its `Id` under `Successful`. **For the SNS tools, "delivered" means SNS acknowledged the publish**, and latency runs from the row's commit to that acknowledgement. SNS's own fan-out to subscribers is not measured; it would be the same for every tool. Delivery was measured at the API because, in earlier testing, both consumer paths were unusable on moto: SNS to SQS slowed down linearly with queue depth, and SNS to an HTTP subscription ran synchronously inside `Publish` and silently dropped entries under concurrency.

pgcdc posts straight to the same receiver process, where "delivered" means the receiver read the POST body.

Latency is the receiver's arrival time minus `bench_ts`, taken from each row's first delivery. All containers share the Linux VM's clock.

### Scenarios

- **Backlog drain.** The tool starts, delivers one warm-up row, idles 20 s and is stopped with SIGTERM. 500,000 rows are then inserted while it is down (5,000 transactions of 100 rows over 4 connections, about 200k rows/s). The tool is started again, and the clock runs until the last row is delivered.
- **Sustained rate.** After the warm-up and idle period, one connection commits a transaction of `rate / 100` rows every 10 ms for 60 s: 10 rows at 1,000 rows/s and 30 rows at 3,000 rows/s. 5,000 rows/s was left out: an earlier investigation found that real batches from pg-cdc and Sequin, which carry message attributes, cost moto about 2 ms per call, so 5,000 rows/s runs moto at its limit and results there swing with host noise.
- **Startup, idle and shutdown.** Cold start runs from `docker run` to the first delivered row, and covers process start, connecting and, for Sequin, migrating its empty internal database. Idle CPU and memory come from the last 10 s of the 20 s idle period after the warm-up row, in every run. SIGTERM to exit is measured when the backlog scenario stops the tool, with a 30 s grace period before SIGKILL.

CPU comes from the container's cgroup counters and memory is anonymous RSS (`anon` in the cgroup's memory stats), both sampled every 0.5 s through the Docker API. Anonymous RSS is used because page cache moves the "usage" figure around; the usage figures are kept in each `result.json`.

Every scenario ran twice for every tool, in two passes, in about 55 minutes. A full emulator calibration ran before each pass, and a 5 s check before each run. A run had 300 s to finish, and no run hit that limit; the longest, Debezium's backlog drain, took 282 s. One pg-cdc backlog run in pass 1 was replaced, because a version check started two short-lived containers while it was draining; the replacement and the original agreed within 2%.

## Emulator calibration

Calibration used `benchctl calib`, a minimal Go client sending `PublishBatch` calls of 10 entries through the same recording proxy. It ran in full before each pass, and its 5 s concurrency-8 check ran before each of the 25 runs, including one replacement run. The two cells in each row are the two full calibrations.

| Path | Body bytes | Concurrency | Messages/s | Call p50 / p99 (ms) | Delivery p50 / p99 (ms) |
|---|---|---|---|---|---|
| SNS via proxy | 520 | 1 | 6,205 / 6,286 | 1.46 / 4.71; 1.45 / 5.14 | |
| | 520 | 8 | 7,700 / 7,696 | 10.2 / 14.2; 10.1 / 14.9 | |
| | 520 | 32 | 7,571 / 7,745 | 41.1 / 86.2; 40.8 / 47.4 | |
| SNS direct to moto | 520 | 8 | 7,717 / 7,750 | 10.1 / 18.2; 10.1 / 18.2 | |
| SNS via proxy | 820 | 8 | 7,585 / 7,537 | 10.4 / 13.7; 10.3 / 18.1 | |
| | 3,075 | 8 | 6,719 / 6,878 | 11.6 / 20.3; 11.5 / 15.5 | |
| SNS via proxy, paced at 1,000 msg/s | 519 | 8 | 999 / 996 | 5.1 / 9.2; 5.0 / 8.7 | 4.71 / 8.81; 4.59 / 8.36 |
| HTTP POST to the receiver | 521 | 1 | 37,487 / 37,222 | 0.02 / 0.05 | |
| | 521 | 32 | 103,528 / 104,152 | 0.12 / 4.7 | |
| HTTP POST, paced | 519 | 1 | 383 / 413 | 0.20 / 0.34; 0.20 / 0.46 | 0.11 / 0.20; 0.11 / 0.25 |

The 25 checks before each run gave 750 to 786 calls/s.

How to read it:

- **moto's ceiling is about 7.6k messages/s** for full 10-entry batches of 520 to 820 bytes, and about 6.8k for 3 KB bodies.
- **moto serves one request at a time.** A call takes about 1.45 ms on its own, and its latency grows in proportion to the number of calls in flight: about 10 ms at 8 and 41 ms at 32, for no extra throughput. Real SNS does not behave this way.
- **The proxy costs nothing measurable:** 7,700 messages/s through it against 7,717 direct.
- **Latency floors:** about 4.6 ms from send to recorded acknowledgement for a paced SNS publisher, and about 0.1 ms for an HTTP POST. The paced HTTP client reached only about 400 requests/s because of its ticker; it measures the floor, not a rate.
- **The receiver is not a bottleneck** for pgcdc: it took 37k serial POSTs/s, against pgcdc's 22k/s.

## Results

Cells with two values (`a / b`) are pass 1 and pass 2. The tables are the `tables.md` that `bench/suite.sh` writes; the raw `result.json` files, with every resource sample, are not committed.

### Backlog drain

500,000 rows written while the tool was stopped, timed from restart.

| Tool | Restart to first event (s) | Restart to last event (s) | Events/s, first to last | Tool CPU-s | CPU µs/event | Peak RSS, anon (MiB) | Message bytes | Entries per PublishBatch |
|---|---|---|---|---|---|---|---|---|
| pg-cdc | 0.41 / 0.37 | 95.9 / 94.7 | 5,235 / 5,298 | 11.1 / 11.0 | 22.2 / 22.1 | 53 / 54 | 519 / 518 | 10.0 / 10.0 |
| Sequin | 8.20 / 8.15 | 97.5 / 98.0 | 5,598 / 5,562 | 101.3 / 103.0 | 202.6 / 206.0 | 908 / 919 | 820 / 825 | 10.0 / 10.0 |
| Debezium Server | 3.40 / 3.45 | 240.8 / 242.3 | 2,106 / 2,093 | 48.5 / 48.8 | 97.0 / 97.6 | 435 / 434 | 3,075 / 3,079 | 10.0 / 10.0 |
| pgcdc (webhook) | 0.23 / 0.19 | 23.0 / 23.3 | 21,968 / 21,684 | 12.8 / 12.9 | 25.6 / 25.9 | 20 / 20 | 515 / 515 | - / - |

- pg-cdc and Sequin both drain at about 70% of moto's ceiling for their message sizes (see the calibration), so their rates mostly describe the emulator.
- Debezium reached 2.1k events/s, with every call carrying 10 entries and about 0.2 cores in use. Its SNS sink sends one batch at a time, so each call pays moto's full round trip; with 8 calls in flight moto takes 6.8k messages/s of this size, so its total capacity is not what holds Debezium back.
- pgcdc's webhook drain is shown for reference only.

### Sustained 1,000 rows/s

60 s of load, 10 rows committed every 10 ms. Latency runs from commit to SNS acknowledgement, or to the receiver reading the POST for pgcdc.

| Tool | p50 ms | p95 ms | p99 ms | max ms | Avg cores | CPU µs/event | Peak RSS, anon (MiB) | Entries per PublishBatch | PublishBatch calls/s |
|---|---|---|---|---|---|---|---|---|---|
| pg-cdc | 7.6 / 7.7 | 14.9 / 14.9 | 21.8 / 22.2 | 90 / 83 | 0.05 / 0.05 | 52.0 / 52.0 | 12 / 11 | 7.4 / 7.4 | 135 / 135 |
| Sequin | 27.9 / 28.8 | 41.8 / 57.5 | 73.5 / 76.2 | 300 / 194 | 0.23 / 0.22 | 232.5 / 228.2 | 539 / 546 | 8.7 / 8.7 | 114 / 114 |
| Debezium Server | 1,099.8 / 1,069.0 | 1,439.2 / 1,395.3 | 1,539.6 / 1,511.1 | 1,599 / 1,628 | 0.26 / 0.25 | 259.2 / 250.3 | 352 / 344 | 10.0 / 10.0 | 98 / 99 |
| pgcdc (webhook) | 3.8 / 3.8 | 5.7 / 5.7 | 9.1 / 9.6 | 29 / 67 | 0.21 / 0.21 | 208.9 / 211.5 | 14 / 14 | - / - | - / - |

### Sustained 3,000 rows/s

60 s of load, 30 rows committed every 10 ms.

| Tool | p50 ms | p95 ms | p99 ms | max ms | Avg cores | CPU µs/event | Peak RSS, anon (MiB) | Entries per PublishBatch | PublishBatch calls/s |
|---|---|---|---|---|---|---|---|---|---|
| pg-cdc | 5.9 / 5.9 | 13.4 / 13.5 | 45.8 / 44.1 | 123 / 119 | 0.07 / 0.07 | 23.6 / 23.5 | 12 / 12 | 8.9 / 8.9 | 336 / 336 |
| Sequin | 28.0 / 28.1 | 41.8 / 43.1 | 68.0 / 77.4 | 162 / 208 | 0.40 / 0.40 | 134.5 / 135.4 | 541 / 570 | 9.3 / 9.3 | 321 / 321 |
| Debezium Server | 17,224.0 / 17,636.0 | 28,772.9 / 29,129.3 | 30,224.3 / 30,018.9 | 30,435 / 30,784 | 0.29 / 0.29 | 96.4 / 97.9 | 415 / 413 | 10.0 / 10.0 | 199 / 198 |
| pgcdc (webhook) | 3.7 / 3.8 | 6.9 / 7.2 | 10.4 / 10.7 | 40 / 31 | 0.27 / 0.27 | 90.4 / 90.7 | 14 / 14 | - / - | - / - |

Debezium cannot sustain 3,000 rows/s with this configuration. It delivered about 2,000 rows/s, its last row arrived 30.3 to 30.6 s after the load ended, and its latency figures here measure a growing backlog. Its calls/s is taken over the whole delivery window.

### Startup, idle and shutdown

Cold start and idle figures cover all six runs of each tool; restart and SIGTERM figures come from the two backlog runs.

| Tool | Runs | Cold start to first event (s) | Restart to first event with backlog (s) | Idle RSS, anon (MiB) | Idle CPU (cores) | SIGTERM to exit (s) | Image (MB) |
|---|---|---|---|---|---|---|---|
| pg-cdc | 6 | 0.31 to 0.35 | 0.37 to 0.41 | 11 to 12 | 0.002 | 0.2 | 23 |
| Sequin | 6 | 9.94 to 10.07 | 8.15 to 8.20 | 245 to 283 | 0.008 to 0.010 | 30.3 | 225 |
| Debezium Server | 6 | 3.30 to 3.48 | 3.40 to 3.45 | 299 to 330 | 0.009 to 0.011 | 0.9 | 912 |
| pgcdc (webhook) | 6 | 0.21 to 0.28 | 0.19 to 0.23 | 10 | 0.000 to 0.001 | 0.2 | 26 |

### Sequin's Postgres and Redis

These ran outside the 2-CPU, 2-GiB limit and are not included in Sequin's figures above.

| Window | Postgres CPU-s | Postgres peak (MiB) | Redis CPU-s | Redis peak (MiB) |
|---|---|---|---|---|
| Backlog drain | 0.4 / 0.4 | 86 / 86 | 4.8 / 4.7 | 25 / 25 |
| 1,000 rows/s | 0.2 / 0.2 | 81 / 81 | 0.9 / 0.9 | 13 / 12 |
| 3,000 rows/s | 0.1 / 0.2 | 104 / 115 | 1.6 / 1.6 | 25 / 25 |
| Idle (anonymous RSS) | | 45 to 47 | | 5 |

### Tool quirks seen during the runs

None of these lost or duplicated data.

- Sequin logged `Call to SlotMessageStore failed with :noproc` 485 to 499 times in each backlog run, and up to 7 times in each rate run.
- Sequin does not exit within 30 s of SIGTERM.
- Debezium's bundled OpenTelemetry agent logged "Failed to add measurement event to queue, queue is full" 728 to 1,875 times per backlog run.
- pgcdc creates `pgcdc_*` tables in the source database, which the harness drops between runs. It logged a `heartbeat failed` warning (a missing `last_beat` column in `pgcdc_heartbeat`) every 30 s.

## Caveats

- **The emulator is the ceiling.** moto serves one request at a time, so its time per call grows with the number of calls in flight, which real SNS's does not. pg-cdc and Sequin both drain at about 70% of moto's measured ceiling, so their drain rates mostly describe moto, and the small gap between them does not rank them. On real SNS, where round trips are longer but do not queue behind each other, throughput and latency will differ, possibly a lot. CPU per event, memory, startup and correctness carry over better.
- **pgcdc's webhook is not comparable.** An HTTP POST to a local receiver costs about 0.1 ms, against about 1.45 ms for a lone emulated SNS call. Its latency is left out of the chart for that reason, and its throughput is not a ranking.
- **Message sizes differ.** pg-cdc sends about 518 bytes per row, pgcdc 515, Sequin 820 and Debezium 3,075 (its default converter embeds the schema). Larger messages cost moto more: 11 to 13% fewer messages per second at 3 KB than at 520 bytes (see the calibration).
- **Debezium ran at its defaults.** Its 500 ms poll interval is a likely contributor to its latency at 1,000 rows/s, and its one-batch-at-a-time SNS sink to its drain rate, but neither was isolated. A tuned Debezium would likely do better on both.
- **One machine, short runs.** Postgres, moto, the proxy, the load generator and the tool share one VM. Each run is 60 s or one 500k backlog, twice. There are no soak, failure or failover scenarios: pg-cdc's parking, handover and fencing, Sequin's backfills and Debezium's snapshots were not exercised, and the workload is a healthy insert-only stream into one table.
- **Sequin's companions are extra.** Sequin's Postgres and Redis ran outside the 2-CPU limit, and their CPU and memory are listed separately rather than added to Sequin's.

## Reproduce

[`bench/README.md`](../bench/README.md) has the exact steps. In short, from a fresh clone:

```sh
docker build --no-cache -t cdcbench/pg-cdc:bench .
bench/setup.sh
bench/suite.sh bench/results/my-run
```

The suite takes about an hour and writes the tables on this page to `bench/results/my-run/tables.md`.
