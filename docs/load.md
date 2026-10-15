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
