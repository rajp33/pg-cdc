# Load and chaos results

The section 4 criteria this milestone measures, as `loadgen report` summarizes the reports recorded below. Criteria 1 and 2 (nothing lost, per-key order) are the chaos checker's, in the next section.

| Criterion | Target | Measured | Verdict | Scenario |
|---|---|---|---|---|
| 3. WAL stays bounded during a sink outage | retained WAL about park_after of write volume | peak retained 77.3 MiB against 2.8 MiB written in park_after (5 s); parking stored 588 events/s against 1000 written; drain incomplete at the 90 s limit: 95,015 events still parked 91 s after writes stopped and still rising (401.9 events/s), so no drain rate was measured; moto's own PutObject ceiling 628 puts/s; pending the real-S3 check (R128) | unverified (emulator-bound) | outage |
| 4. Throughput against SNS | at least 5,000 events/s with p99 commit-to-acknowledgement under 1 s | delivered 3822 events/s, p99 upper bound 60.00 s; moto's own ceiling 4364 messages/s; pending the real-SNS check (R128) | unverified (emulator-bound) | sns |
| 5. Handover | a deploy handover under 10 s with nothing lost | 5 handovers: worst SIGTERM to /readyz 1.85 s, worst delivery gap 1.95 s, worst exit 0.10 s; evidence intact | pass | handover |
| 6. Memory | within max_inflight_bytes plus fixed overhead whatever the transaction size | peak RSS 122.3 MiB with max_inflight_bytes 64 MiB, flat from 50,000 to 500,000 rows | pass | memory |
| 7. Telemetry overhead | within 1% of a build without the otel feature when unconfigured | -0.73% CPU per event, runs spread 14.0%, beyond the 1% to resolve | not resolved | overhead |

Numbers recorded here come from the `loadgen` binary, which drives pg-cdc as a child process against Postgres and moto in Docker. Nothing here touches real AWS. Each section states its machine, versions and command so a reader can repeat it.

## Chaos

`loadgen chaos` runs section 18.3 end to end:

- It starts Postgres and moto, one SNS fault proxy and one S3 fault proxy in front of moto, and two `pg-cdc run` processes that share one config (FIFO SNS topic, S3 state store, eight lanes sharded by key, `park_after: 2s`, `lease_timeout: 4s`, `fence_delay: 4500ms`, `drain_timeout: 1s`, no payload store, so oversize rows park dead).
- A writer upserts 300 changes a second over 50 keys (Zipf exponent 1.0, transactions of 1 to 20 changes, a 16 KiB TOAST payload every 50th change, one 300 KiB row every 400th transaction).
- Each run opens, with the writer running and both proxies healthy, with one SIGTERM handover and then one SIGKILL takeover, each waited for until the standby holds a new epoch. Section 18.3 asks for both kinds of takeover, and the random schedule alone does not guarantee a clean one.
- Then, once a second, a seeded draw picks one action: SIGKILL the active instance (restarted 0.5 to 2 s later), SIGTERM it (restarted once it exits), terminate pg-cdc's Postgres connections, an SNS outage, an S3 brownout, an error burst on both proxies, or nothing. At most one kill or handover is in progress.
- The operator pass runs every second on the active instance: it redrives each dead queue once, then discards its dead head after placing a discard marker in the delivery log.
- After the chaos phase it stops the writer, restores both proxies, keeps both instances running and keeps the operator pass going until every committed change is delivered or discarded, or for at most `--settle`, then stops both instances with SIGTERM and lists what is still parked in S3.
- The checker passes when every committed change was delivered at least once under the key it was written with, is still parked in S3, is still retained by the slot or was discarded by the harness, when the first delivery of each change comes after the delivery or discard of every smaller committed change of its key, and when nothing was delivered that was not written. A delivery under a different key counts as a phantom.
- The run also fails when an instance exits non-zero after SIGTERM, when a published message cannot be read, when a key's messages arrive under two FIFO message groups, or when the instances' logs do not show at least one `clean` and one `unclean` takeover (`took over the slot`).

Deliveries are recorded at the SNS proxy rather than read back from a FIFO queue behind the topic. Moto's FIFO queue cost grows with its depth: sending 3,000 messages into one without reading took 128 s, and after every takeover the backlog flush buried it, so a smoke run that needs seconds took six minutes. The proxy forwards one SNS request at a time per FIFO message group: a request waits while another request for any of its groups is at moto. For each key, therefore, the recorded order is the order moto accepted the publishes, and keys do not wait for each other. It records a `PublishBatch` entry only when moto's reply lists the entry's `Id` under `Successful`, so a partly failed batch counts only its accepted entries. Discard markers go into the same log at the moment before the discard request.

`--settle` is a hard limit on the settle phase: it ends as soon as nothing is outstanding, and at the limit in any case, so a backlog does not hold the run open; loadgen prints how many changes were outstanding when the limit cut the settle short. Once both instances have stopped, the harness lists what is still parked in S3 and reads the slot's `confirmed_flush_lsn`. A change counts as retained by the slot when the WAL insert position the writer read just before its transaction's `COMMIT` is at or past that position: such a transaction cannot have been confirmed, so Postgres keeps it and the next start replays it. The comparison errs only towards counting a change missing. Hot keys need this rule: one order key moves one event per sink round trip, so under the Zipf workload the hottest keys fall behind while the writer runs, and their backlog stays in flight, neither parked nor delivered, until it drains.

Each run writes its report as JSON (`target/chaos/smoke.json` or `full.json`), the instances' config and one log file per process generation beside it. CI uploads that directory when the smoke job fails.

### Smoke run

`mise run chaos-smoke` builds the debug binary and runs seed 1 for 15 s, after the opening takeovers, with at most 30 s to settle. It runs as its own CI job and in `mise run ci`, and takes about two minutes on the machine below. Three runs in a row on commit `622b3e8`:

```
chaos seed=1 duration=15s verdict=pass committed=6586 delivered=6583 duplicates=250 stored=0 discarded=3 takeovers=3 clean=2 unclean=1 redrives=1 discards=3 missing=0 phantoms=0 out_of_order=0
chaos seed=1 duration=15s verdict=pass committed=6580 delivered=6577 duplicates=38 stored=0 discarded=3 takeovers=3 clean=2 unclean=1 redrives=1 discards=3 missing=0 phantoms=0 out_of_order=0
chaos seed=1 duration=15s verdict=pass committed=6623 delivered=6620 duplicates=256 stored=0 discarded=3 takeovers=3 clean=2 unclean=1 redrives=1 discards=3 missing=0 phantoms=0 out_of_order=0
```

### Five-minute run

Machine: Apple M3 Max, 16 cores, 64 GiB, macOS 26.5.2. Docker: OrbStack 29.4.0 (linux/arm64, 10 CPUs, 15.7 GiB). Postgres 17 (`postgres:17-alpine`), moto 5.2.4, Rust 1.99.0, a release build at commit `16230f7` (pg-cdc unchanged since `8fd05b1`). Another agent's test containers ran on the same machine during the run.

Command: `CHAOS_SEED=1792100599 mise run chaos` (3 minutes of chaos, `--settle 1m`)

| | |
|---|---|
| Seed | 1792100599 |
| Chaos duration | 3 minutes |
| Wall clock | 5 min 24 s with the binaries already built: the opening takeovers, 3 minutes of chaos, the 1-minute settle and the shutdown |
| Writer transactions | 5,446 |
| Takeovers | 8 (1 clean, 7 unclean) |
| Redrives | 3 |
| Discards | 6 (0 refused) |
| Committed changes | 56,830 |
| Delivered (distinct) | 9,551 |
| Duplicate deliveries | 3,542 |
| Still parked in S3 at the end | 16,854 |
| Retained by the slot at the end | 30,798 |
| Discarded | 6 |
| Missing, phantom, out of order | 0, 0, 0 |
| Verdict | pass |

Actions drawn (180 draws): 64 nothing, 31 SNS outages, 20 connection drops, 13 error bursts, 11 S3 brownouts, 7 SIGKILLs, 7 SIGTERMs. Another 23 kills and terminations were skipped because a handover was already in progress or no instance reported itself active.

Proxy counts:

| Proxy | Requests | Forwarded | Injected errors | Parked puts | Parked deletes | Publish requests |
|---|---|---|---|---|---|---|
| SNS | 5,798 | 5,019 | 825 | | | 5,740 |
| S3 | 45,974 | 45,146 | 504 | 34,460 | 4,418 | |

Reading the numbers:

- The settle limit cut the settle short with 47,317 changes neither delivered nor discarded. The checker found every one of them still parked in S3 or retained by the slot, and none out of order.
- The chaos phase keeps the sink down most of the time: each second has a 15% chance of a 3 to 10 s SNS outage. Most keys therefore spend it parked, and only the settle minute is healthy.
- A key moves one event per sink round trip, and once parked one per read, publish and delete. The Zipf workload sends 22% of its writes, 67 a second, to its hottest key, so the hottest keys' backlog takes minutes to deliver. The earlier ten-minute run (commit `932ee1b`, seed 1792076253) waited for it: 58 minutes of wall clock, 47 of them draining about 45,000 events on three hot keys, and every change was delivered or discarded.
- The first five-minute attempt with this seed, before the slot rule, failed with 34,528 changes missing. They were the same kind of backlog, retained by the slot, which the checker did not yet look at (`u6-drain-chaos.log` in the milestone workspace).

## Load

`loadgen load <scenario>` measures criteria 3 to 7 of section 4. Each scenario starts its own Postgres (and moto where it needs one), writes a pg-cdc config into a temp directory (`status_interval: 1s`, `order_by: key`, `lanes: 64`, `admin.listen: 127.0.0.1:0`), drives a release build of `pg-cdc` as a child process, reads its progress from `/status`, and writes `<report-dir>/<scenario>.json` with the config, the machine and the numbers. `mise run load` builds the default and `--no-default-features` binaries and runs every scenario into `target/load`; `LOAD_SCENARIO=ceiling|sns|overhead|memory|outage|brownout|handover` runs one. `loadgen report --report-dir <dir> --out <file>` writes the summary table at the top of this page from every report in a directory.

Machine: Apple M3 Max, 16 cores, 64 GiB, macOS 26.5.2. Docker: OrbStack 29.4.0 (linux/arm64). Postgres 17.11 (`postgres:17-alpine`, `fsync=off`), moto 5.2.4, Rust 1.99.0, pg-cdc and loadgen release builds at commit `933a205`. Postgres and moto run in Docker on the same machine as pg-cdc and the load generator, and other jobs shared the machine (load average 3 to 5 during the runs), so absolute numbers are this machine's and the runs are noisy.

CPU time and resident memory are read from outside the process with `ps -o rss=,time=`, so they cover everything pg-cdc does, including startup. The workload is the one from the chaos section without TOAST or oversize rows: upserts into `loadgen_items` from one connection in transactions of exactly 100 changes (`row_bytes 200`, 1,000 keys, uniform), with `REPLICA IDENTITY FULL`.

The summary at the top of this page gives each criterion's verdict. For criterion 7 it reads the second overhead run; the first failed (+1.71%), and the 36 runs of both pooled give -0.31%.

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

pg-cdc measures `pgcdc.commit_to_delivery` as its host clock minus the commit time Postgres recorded, so a database clock ahead of the host would shrink every latency. The scenario therefore measures the database-to-host clock offset before and after the run, with each query's round trip as its uncertainty. Criterion 4 reads a verified bound: none unless the histogram holds an observation for every written event and pg-cdc drained the backlog, otherwise the p99 upper bound plus the largest amount by which the database clock may run ahead. The run recorded below predates that check. Its verdict does not depend on it, because moto's ceiling alone makes it unverified.

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

### Sink outage (criterion 3)

Command: `LOAD_SCENARIO=outage mise run load`. Release build of commit `16230f7` (pg-cdc unchanged since `8fd05b1`), on the machine above. A standard SNS topic and the S3 state store on moto, each behind its own fault proxy; `park_after: 5s`, `max_inflight_bytes: 64MiB`, 64 lanes, the default `state_store.max_concurrency` (256). The writer upserts 1,000 events/s over 1,000 keys for 150 s (transactions of 100 changes, 200-byte rows). From 10 s to 70 s the SNS proxy answers every request with a 500. The scenario samples `/status`, the S3 proxy's counts and `pg_current_wal_lsn()` every second, then watches the drain for `--drain-limit` after the writer stops (default 90 s), or less if it catches up. Afterwards it measures moto's own `PutObject` ceiling with a direct client (32 workers, 700-byte bodies, 10 s). The whole run took 4 min 33 s.

| | |
|---|---|
| Written | 150,100 events in 150.01 s (1,000 events/s, 573 KiB of WAL per second) |
| Parking during the outage (15 s to 70 s) | 588 events/s stored in S3 against 1,000 written; 20,737 parked at the outage's end |
| Moto's `PutObject` ceiling (direct client) | 628 puts/s |
| Peak in flight | 55 MiB, under the 64 MiB bound |
| Peak slot retained bytes | 77.3 MiB, against 2.8 MiB written in one `park_after` |
| Criterion 3 | unverified (emulator-bound) |
| Parked 90 s after the writer stopped | 95,015 events on 1,000 keys, still rising (58,062 when the writer stopped) |
| In flight and lag at the deadline | 30 MiB in flight, 44 MiB of WAL not yet confirmed |
| Parked events removed after redelivery | 809 in the whole run; 5.8 a second in the 90 s after the writer stopped |
| SNS requests | 12,026 publish requests, 6,518 refused during the outage |

Cause: moto bounds the parking rate. pg-cdc stored 588 parked events a second while moto took at most 628 direct `PutObject` calls a second, so parking could not keep up with 1,000 events/s and retained WAL grew with the backlog instead of staying near one `park_after` of writes. Because moto, not pg-cdc, set the ceiling, criterion 3 is recorded as unverified rather than failed (R128): it needs a run against real S3. Once the SNS proxy recovered every key already had a parked queue (all 1,000 by 60 s), so every later event queued behind it and parked too. Parking that backlog used all of moto's capacity until the deadline, so the drain got almost none of it; see the drain section below. The report is `target/load/outage.json`, copied to the milestone workspace as `u6-drain-outage.json`. An earlier run at commit `dea6918` with the old code and a 3-minute drain window measured the same shape: 632 events/s parked, 77.7 MiB retained, 658 parked events removed in the whole run (`u6-t14-outage.json`).

### S3 brownout during a sink outage (R136)

Command: `LOAD_SCENARIO=brownout mise run load`, the outage scenario with the S3 proxy adding 500 ms to every request from 10 s to 40 s and 2 s from 40 s to 70 s, while SNS is down.

| | |
|---|---|
| Written | 151,000 events in 150.96 s |
| Parking with S3 at +500 ms (15 s to 40 s) | 58 events/s stored against 1,000 written |
| Parking with S3 at +2 s (40 s to 70 s) | 15 events/s stored |
| Parking after both ended (70 s to 150 s) | 627 parked `PUT`s/s accepted by S3; moto's direct `PutObject` ceiling in this run 781 puts/s |
| Reader stalled at `max_inflight_bytes` | yes, from about 111 s |
| Peak slot retained bytes | 84.5 MiB, against 2.8 MiB written in one `park_after` |
| Parked 3 minutes after the writer stopped | 102,427 events on 948 keys, still rising (649 when the writer stopped) |
| Lag at the deadline | 79 MiB of WAL not yet confirmed, 24 MiB in flight |
| Parked events removed after redelivery (S3 deletes, whole run) | 2,241 |

Parking falls far behind the write rate during an S3 brownout: about 58 events a second at 500 ms of added latency and 15 at 2 s, against 1,000 written, because each parked event is its own S3 write and at most `state_store.max_concurrency` store calls are in flight, 32 in this run: 32 calls at 0.5 s and at 2 s allow about 64 and 16 writes a second. The default is now 256, which allows eight times that; the brownout was not re-run. The reader stalls at `max_inflight_bytes` and Postgres holds the WAL; nothing is lost, and the backlog parks once S3 recovers. This is a v1 limit, recorded in the runbook, and the reason for the segmented S3 layout in v1.1 (section 20, item 3); the inline S3 write of M3.10 is unchanged. The `/status` parked count stayed near 650 from 60 s to 150 s while the S3 proxy accepted about 627 parked writes a second, then rose by about 100,000 in the next three minutes; the approximate count lags the store during a backlog. The report is `u6-t14-brownout.json` in the milestone workspace.

### Drain after an outage

Draining a parked event used to cost three S3 requests in series: a `ListObjectsV2` to find the queue's head, a `GetObject` and a `DeleteObject`. Every new event of a parked key added a `PutObject`, and all of these shared 32 permits. v1 now reads up to 1,000 ids of a queue ahead with one listing and fetches the following heads by name, removes each delivered batch with one `DeleteObjects`, and allows 256 store requests in flight by default. That leaves about 1.1 requests per drained event (a GET, a share of a batched delete and a listing per 1,000 events), plus the PUT that parked it.

| Measurement | Before | After |
|---|---|---|
| Drain probe on moto: 112,000 parked events on 1,000 keys, S3 and SNS behind the fault proxies, 64 lanes, store concurrency 32, no new writes | 21 to 31 events/s (four 60 s windows, commit `dea6918`) | 133 events/s over a 30 s window, 229 to 263 events/s in its last 10 s and still rising (commit `8fd05b1`) |
| Model S3 at 20 ms per request, 20,000 events on 1,000 keys, 64 lanes, three requests per drained event (`lane::tests::drain`) | 38.6 s at 32 permits (518 events/s) | 5.9 s at 256 (3,390 events/s) |
| Same model with 1,000 new events/s for 30 s | the parked count doubles, to 40,487 | the parked count falls to 0 |
| Outage scenario: parked events removed per second while the WAL backlog still parks | 2.5 and 8.6 (whole run after the outage, the outage and brownout runs) | 5.8 (90 s after the writer stopped) |

Moto still bounds what these runs show:

- **Listing.** Moto's `ListObjectsV2` takes time in proportion to the whole bucket, about 32 ms at 112,000 objects, and moto runs listings one at a time. Filling the read-ahead for 1,000 keys therefore takes about 30 s of moto's time, which is why the probe was still accelerating at the end of its window. The prototype of the same changes reached 630 to 690 events/s once warm.
- **Request capacity.** One moto server handles about 600 requests a second in all. In the outage scenario, parking the backlog at 1,000 written events a second takes all of it, so the drain gets almost nothing until parking catches up.
- **Queueing.** The probe run with store concurrency 256 removed nothing in 30 s. Moto queues requests instead of throttling them, so 256 queued listings waited past the S3 client's 10 s attempt timeout, and the retries added load. Real S3 neither queues requests behind each other nor makes listing cost grow with the bucket, so this applies to moto only. The outage and chaos scenarios run pg-cdc with the default 256 against moto, so their drain is subject to it.

Against real S3 at 20 to 50 ms a request, 256 permits and about 1.1 requests per drained event allow roughly 4,000 to 10,000 events a second, or less while new events of parked keys still need PUTs; this is unmeasured until the pre-production run (runbook section 7).

Nothing is lost while a backlog drains, and keys without a parked queue keep flowing. But after an outage that parked every key, every key's new events queue behind its backlog, so delivery lags by the drain time; alert on `pgcdc.parked.oldest_age`.

The outage scenarios stop `--drain-limit` after the writer stops (default 90 s), or earlier once no key is parked and lag is under 16 MiB. They report what is still parked, the rate at which the parked count fell over that window (from the once-a-second `/status` samples), the parked events removed per second (the S3 proxy's count of deleted parked objects) and the time the rest would take at the falling rate. The report's `drain.complete` is true only when the last sample was caught up, and the summary row reads "drain complete" or "drain incomplete at the N s limit".

### Handover (criterion 5)

Command: `LOAD_SCENARIO=handover mise run load`. pg-cdc release build (unchanged since commit `dea6918`), loadgen at commit `622b3e8`. A FIFO SNS topic and the S3 state store on moto, two pg-cdc instances with the default leadership settings (`lease_timeout: 6s`, `fence_delay: 10s`, `drain_timeout: 5s`), the writer at 500 events/s over 1,000 keys. Five times: wait 20 s plus a random share of the 2 s standby poll, SIGTERM the active instance, and measure three times: to the old instance's exit, until the standby's `/readyz` answers 200, and the largest gap between consecutive deliveries around the handover. Then start the stopped instance again as the next standby. Deliveries are recorded at the SNS proxy as in the chaos run: only accepted entries, in the order moto accepted them per message group. After the last round the writer stops, the run settles and the chaos checker reads the evidence. Criterion 5 passes only when every handover is ready and every gap is under 10 s, the checker passes, and the run recorded no problem: no non-zero exit, forced kill, unreadable message or message group mismatch.

| Round | SIGTERM to old exit | SIGTERM to new `/readyz` 200 | Largest delivery gap |
|---|---|---|---|
| 1 | 0.02 s | 0.91 s | 0.98 s |
| 2 | 0.01 s | 1.17 s | 1.20 s |
| 3 | 0.02 s | 1.85 s | 1.95 s |
| 4 | 0.10 s | 0.64 s | 0.60 s |
| 5 | 0.04 s | 0.19 s | 0.24 s |

The checker saw 57,200 committed changes and 57,200 delivered. There were no duplicates, nothing missing, no phantoms, nothing out of order and no problems. Criterion 5 passes: every handover was ready in under 10 s (worst 1.85 s), the worst delivery gap was 1.95 s, and nothing was lost. Readiness varies by up to one standby poll of `pg_replication_slots` (2 s; section 12.5, "the drain plus one poll"), depending on where the poll falls after the old instance releases the slot. Three earlier runs also passed: two with the previous evidence collection (worst ready 2.21 s and gap 2.05 s, then 1.73 s and 1.83 s), and one with a single global ordering gate (2.21 s and 2.06 s). The report is `u6-r1-handover.json` in the milestone workspace.
