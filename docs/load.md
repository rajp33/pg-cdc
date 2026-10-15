# Load and chaos results

Numbers recorded here come from the `loadgen` binary, which drives pg-cdc as a child process against Postgres and moto in Docker. Nothing here touches real AWS. Each section states its machine, versions and command so a reader can repeat it.

## Chaos

`loadgen chaos` runs section 18.3 end to end:

- It starts Postgres and moto, one SNS fault proxy and one S3 fault proxy in front of moto, and two `pg-cdc run` processes that share one config (FIFO SNS topic, S3 state store, eight lanes sharded by key, `park_after: 2s`, `lease_timeout: 4s`, `fence_delay: 4500ms`, `drain_timeout: 1s`, no payload store, so oversize rows park dead).
- A writer upserts 300 changes a second over 50 keys (Zipf exponent 1.0, transactions of 1 to 20 changes, a 16 KiB TOAST payload every 50th change, one 300 KiB row every 400th transaction).
- Once a second a seeded draw picks one action: SIGKILL the active instance (restarted 0.5 to 2 s later), SIGTERM it (restarted once it exits), terminate pg-cdc's Postgres connections, an SNS outage, an S3 brownout, an error burst on both proxies, or nothing. At most one kill or handover is in progress.
- The operator pass runs every second on the active instance: it redrives each dead queue once, then discards its dead head after placing a discard marker in the delivery log.
- After the chaos phase it stops the writer, restores both proxies, keeps both instances running and keeps the operator pass going until every committed change is delivered or discarded, then stops both instances with SIGTERM.
- The checker passes when every committed change was delivered at least once, is still parked in S3 or was discarded by the harness, when the first delivery of each change comes after the delivery or discard of every smaller committed change of its key, and when nothing was delivered that was not written.

Deliveries are recorded at the SNS proxy, in the order moto accepted the publishes, rather than read back from a FIFO queue behind the topic. Moto's FIFO queue cost grows with its depth: sending 3,000 messages into one without reading took 128 s, and after every takeover the backlog flush buried it, so a smoke run that needs seconds took six minutes. Discard markers go into the same log at the moment before the discard request.

The settle window is a stall limit. `--settle` is how long the run waits without any committed change leaving the outstanding set, and the run ends at ten times that in any case. Parked queues of hot keys drain one event at a time, so a backlog from a long outage legitimately takes longer than one window.

Each run writes its report as JSON (`target/chaos/smoke.json` or `full.json`), the instances' config and one log file per process generation beside it. CI uploads that directory when the smoke job fails.

### Smoke run

`mise run chaos-smoke` builds the debug binary and runs seed 1 for 15 s with a 30 s settle window. It runs as its own CI job and in `mise run ci`, and takes about a minute and a half on the machine below.

### Ten-minute run

Machine: Apple M3 Max, 16 cores, 64 GiB, macOS 26.5.2. Docker: OrbStack 29.4.0 (linux/arm64, 10 CPUs, 15.7 GiB). Postgres 17 (`postgres:17-alpine`), moto 5.2.4, Rust 1.99.0, a release build of pg-cdc at commit `932ee1b`.

Command: `CHAOS_SEED=1792076253 mise run chaos`

| | |
|---|---|
| Seed | 1792076253 |
| Chaos duration | 10 minutes |
| Wall clock | 58 min 13 s (10 minutes of chaos, then the backlog drained) |
| Writer transactions | 17,049 |
| Takeovers | 19 |
| Redrives | 26 |
| Discards | 142 (0 refused, 0 resolved after the run) |
| Committed changes | 180,114 |
| Delivered (distinct) | 179,972 |
| Duplicate deliveries | 2,845 |
| Still parked at the end | 0 |
| Discarded | 142 |
| Missing, phantom, out of order | 0, 0, 0 |
| Verdict | pass |

Actions drawn (600 draws): 207 nothing, 87 SNS outages, 67 connection drops, 62 S3 brownouts, 56 error bursts, 33 SIGKILLs, 32 SIGTERMs. Another 46 kills and terminations were skipped because a handover was already in progress or no instance reported itself active.

Proxy counts:

| Proxy | Requests | Forwarded | Injected errors | Parked puts | Parked deletes | Publish requests |
|---|---|---|---|---|---|---|
| SNS | 112,685 | 111,657 | 1,111 | | | 112,487 |
| S3 | 456,547 | 455,308 | 1,124 | 116,479 | 100,103 | |

Reading the numbers:

- Delivered plus discarded equals committed: 179,972 + 142 = 180,114.
- The duplicates are replays after takeovers. All 19 takeovers were unclean, because with `drain_timeout: 1s` and thousands of events in flight an instance cannot settle before SIGTERM's drain window ends; the next holder waits `fence_delay`, then replays from the confirmed position. A clean handover with nothing in flight is covered by `crates/pg-cdc/tests/handover.rs`.
- Dead queues come from the 300 KiB rows, which exceed SNS's 256 KiB limit with no payload store. Each is redriven once, dies again and is then discarded.
- Outages and brownouts parked a backlog of about 45,000 events on three hot keys. A key's parked queue drains serially (a store read, a publish and a delete per event), at 20 to 30 events a second per key against moto, so the drain took 47 minutes after the writer stopped. This is the known limit of the inline S3 layout (section 20, item 3), not a loss: the events were in the store throughout.

## Load

`loadgen load <scenario>` measures criteria 4, 6 and 7 of section 4. Each scenario starts its own Postgres (and moto where it needs one), writes a pg-cdc config into a temp directory (`status_interval: 1s`, `order_by: key`, `lanes: 64`, `admin.listen: 127.0.0.1:0`), drives a release build of `pg-cdc` as a child process, reads its progress from `/status`, and writes `<report-dir>/<scenario>.json` with the config, the machine and the numbers. `mise run load` builds the default and `--no-default-features` binaries and runs every scenario into `target/load`; `LOAD_SCENARIO=ceiling|sns|overhead|memory` runs one.

Machine: Apple M3 Max, 16 cores, 64 GiB, macOS 26.5.2. Docker: OrbStack 29.4.0 (linux/arm64). Postgres 17.11 (`postgres:17-alpine`, `fsync=off`), moto 5.2.4, Rust 1.99.0, pg-cdc and loadgen release builds at commit `933a205`. Postgres and moto run in Docker on the same machine as pg-cdc and the load generator, and other jobs shared the machine (load average 3 to 5 during the runs), so absolute numbers are this machine's and the runs are noisy.

CPU time and resident memory are read from outside the process with `ps -o rss=,time=`, so they cover everything pg-cdc does, including startup. The workload is the one from the chaos section without TOAST or oversize rows: upserts into `loadgen_items` from one connection in transactions of exactly 100 changes (`row_bytes 200`, 1,000 keys, uniform), with `REPLICA IDENTITY FULL`.

| Criterion | Scenario | Result |
|---|---|---|
| 4. At least 5,000 events/s with p99 commit-to-acknowledgement under 1 s against SNS | `sns` | unverified (emulator-bound): moto ceilinged at 4,364 messages/s |
| 6. Memory within `max_inflight_bytes` plus fixed overhead | `memory` | pass |
| 7. Telemetry unconfigured is within 1% of a build without `otel` | `overhead` | not resolved: one run of the scenario failed (+1.71%), one passed (-0.73%), the 36 runs pooled give -0.31% |

Criterion 4 stays unmet against SNS until `loadgen load sns --topic-arn <arn> --real-aws` passes (the pre-production check in the runbook).

### Ceiling (against the discard sink)

Command: `LOAD_SCENARIO=ceiling mise run load`. Discard sink, filesystem state store, one slot.

| Phase | Result |
|---|---|
| Backlog: 2,000,000 events written with pg-cdc stopped, then drained | 8.74 s, 228,772 events/s |
| Backlog CPU time | 18.47 s (2.1 cores busy) |
| Backlog peak RSS | 20 MiB |
| Sustained: 6,000 events/s offered for 60 s, telemetry on (`trace_sample_ratio: 1.0`, `OTEL_METRIC_EXPORT_INTERVAL=1000`) | 360,900 events written at 6,000/s |
| Kept up (lag below 16 MiB at the end) | yes: lag peaked at 58 KiB and was 0 at the end |
| `pgcdc.commit_to_delivery` | 360,900 observations, p99 upper bound 0.01 s |

pg-cdc's own bound is far above 6,000 events/s: it drains a backlog at about 38 times that rate, with Postgres decoding and pg-cdc sharing the machine. This is context only; criterion 4 is judged against SNS.

### SNS (criterion 4)

Command: `LOAD_SCENARIO=sns mise run load`. A standard topic on moto with no subscription, the S3 state store on moto, telemetry as in the sustained phase, 6,000 events/s offered for 60 s. Moto's own ceiling is measured afterwards with a direct `PublishBatch` client (32 concurrent calls of 10 entries for 20 s, 300-byte bodies) on a second topic.

| | |
|---|---|
| Offered | 6,000 events/s for 60.18 s (361,100 events) |
| Delivered over the window (`pgcdc.events{outcome=delivered}`) | 229,981 events, 3,822 events/s |
| Kept up (lag below 16 MiB at the end) | no: 140 MB behind at the end of the window, drained afterwards |
| p99 upper bound of `pgcdc.commit_to_delivery` | 60 s (the backlog was delivered late) |
| Moto's ceiling for direct `PublishBatch` | 4,364 messages/s (8,753 calls, 87,530 entries in 20.06 s) |
| Criterion 4 | unverified (emulator-bound) |

Cause: moto bounds this run. Its direct `PublishBatch` ceiling of 4,364 messages/s is below the 5,000 events/s that criterion 4 asks for, and pg-cdc's 3,822 events/s sits under that ceiling while it also writes its state to the same moto process. pg-cdc kept up with the discard sink at the same offered load, so the SNS path is where it fell behind. The run says nothing about real SNS, so the criterion is recorded as unverified rather than failed or passed (R128). The delivered rate is the counter's value at the end of the writer window, which can lag the true count by up to one export interval (1 s), so it understates slightly.

### Telemetry overhead (criterion 7)

Command: `LOAD_SCENARIO=overhead mise run load`, run twice. 18 slots are created before one backlog of 1,000,000 events is written. The default build (telemetry not configured) and the `--no-default-features` build then alternate, each run on its own slot with the discard sink and its own filesystem store, recording CPU time and wall time until `/status` reports the backlog's end LSN. Both invocations saw a spread above 1.01 in CPU time, so each ran nine runs per build.

| | Default build | Baseline build |
|---|---|---|
| Run 1: median CPU s per million events | 9.51 | 9.35 |
| Run 1: CPU spread (max / min) | 1.084 | 1.112 |
| Run 1: median events/s | 235,825 | 241,811 |
| Run 2: median CPU s per million events | 9.53 | 9.60 |
| Run 2: CPU spread (max / min) | 1.140 | 1.128 |
| Run 2: median events/s | 242,196 | 235,973 |

Run 1 gives an overhead of +1.71% (fail by the method); run 2 gives -0.73% (pass). Pooling the 18 runs of each build from both invocations gives medians of 9.52 and 9.55 CPU s per million events, -0.31%.

Cause: the runs of one build spread by 8% to 14% in CPU time, an order of magnitude above the 1% that criterion 7 asks the method to resolve, so single invocations fall on either side of 1% by chance and the two builds' medians swap places between invocations. The spread is run-to-run variation on a machine shared with other jobs and with Docker; there is no consistent sign to the difference between the builds. Nothing here shows telemetry costing more than the noise, and nothing here can prove it costs less than 1%. Resolving 1% needs a quiet machine or a hardware instruction counter, neither of which this method has.

### Memory (criterion 6)

Command: `LOAD_SCENARIO=memory mise run load`. `max_inflight_bytes: 64MiB`, 1 KiB rows, 1,000 keys. RSS is sampled every 100 ms. Pass per R130: in each mode the larger transaction's peak is within 10% of the smaller's, and every peak is at most idle plus 2 x 64 MiB.

| Mode | Rows in the transaction | Idle RSS (after 5 s) | Peak RSS | Peak in-flight bytes |
|---|---|---|---|---|
| Discard sink | 50,000 | 16.0 MiB | 22.0 MiB | |
| Discard sink | 500,000 | 16.0 MiB | 22.0 MiB | |
| SNS sink in an outage, `park_after: 1h` | 50,000 | 24.2 MiB | 121.8 MiB | 67,109,056 |
| SNS sink in an outage, `park_after: 1h` | 500,000 | 24.5 MiB | 122.3 MiB | 67,110,215 |

In the stalled mode, in-flight bytes reached the bound (64 MiB = 67,108,864) and stayed there for the 30-second observation; the larger transaction's peak is 0.4% above the smaller's and both are about 30 MiB under the allowance of idle plus 128 MiB (152 MiB). Criterion 6 passes in both modes: memory is flat across a 10x larger transaction, and with the sink stalled it settles at about 100 MiB above idle for a 64 MiB bound. The stalled runs use a fresh slot and process per transaction size, with the proxy switched to an outage once pg-cdc reports ready.

Findings worth knowing:

- The 100,000-key variant of the stalled run did not finish. With the sink down, every key with an event in flight retries on its own 100 ms to 30 s backoff, the retries arrive as new connections, and about 16,000 of them within seconds exhausted macOS's ephemeral ports (`Can't assign requested address`) for the whole machine until the sockets left `TIME_WAIT`. The recorded runs use 1,000 keys, the keyspace of the other scenarios. The retry connection churn grows with the number of distinct keys failing at once, and the port exhaustion itself comes from the sink endpoint sitting on the same host as the load generator.
