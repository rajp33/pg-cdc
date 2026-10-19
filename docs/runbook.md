# pg-cdc runbook

How to set up, deploy, watch and repair a pg-cdc pipeline. [docs/configuration.md](configuration.md) describes the configuration; [docs/load.md](load.md) has the measured numbers quoted here. Commands assume the pipeline `orders`, the slot and publication `pgcdc_orders`, and the role `pgcdc`.

## 1. Setup

Create the publication before the slot. pgoutput reads the publication as of each change's WAL position, so changes made before the publication existed are skipped or fail to decode. pg-cdc never creates, drops or advances a slot or publication itself.

```sql
CREATE PUBLICATION pgcdc_orders FOR TABLE public.orders, public.order_items;
ALTER TABLE public.orders REPLICA IDENTITY FULL;
ALTER TABLE public.order_items REPLICA IDENTITY FULL;
SELECT pg_create_logical_replication_slot('pgcdc_orders', 'pgoutput');
```

Grants:

- **Self-managed Postgres:** `ALTER ROLE pgcdc REPLICATION;` and `wal_level = logical` (a restart).
- **Aurora and RDS:** `GRANT rds_replication TO pgcdc;`, and for IAM authentication `GRANT rds_iam TO pgcdc;` with `credentials: { type: aws_rds_iam }`. The cluster parameter group needs `rds.logical_replication = 1`, which takes effect after a writer reboot, and IAM on replication connections also needs `rds.iam_auth_for_replication = 1`.
- **Tables:** streaming needs no table privileges.

**A spare WAL sender.** Every task that starts checks its replication login by opening a walsender for a moment, so the source needs one sender free beyond those in use while a task starts. Check it with:

```sql
SELECT setting::int - (SELECT count(*) FROM pg_stat_replication) AS free_wal_senders
FROM pg_settings WHERE name = 'max_wal_senders';
```

With none free, the new task logs SQLSTATE 53300 (`number of requested standby connections exceeds max_wal_senders`), stays unhealthy and retries. Raising `max_wal_senders` needs a restart; size it with one spare per task that may start at the same time.

**FULL identity.** Give every published table `REPLICA IDENTITY FULL`, so updates and deletes carry the whole old row. `/status` and the `pgcdc.tables_without_full_identity` gauge count published tables without it.

**S3.** One bucket (or prefix) for the state store and, if oversize rows are possible, one for the payload store. Turn on default encryption (SSE-KMS): parked events and payloads hold row data. Give the payload prefix a lifecycle rule that expires objects after your consumers' retention; never expire the state store's `parked/` objects or `leader.json`. The state store lives under `<prefix>/<pipeline>/` (`leader.json`, `parked/<order key id>/<event id>.json`), payloads under `<prefix>/<pipeline>/payloads/<event id>.json`.

**IAM for the task role:**

| Service | Actions | Resource |
|---|---|---|
| SNS | `sns:Publish` (covers `PublishBatch`), `sns:GetTopicAttributes` | the topic |
| S3 | `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` | `arn:aws:s3:::<bucket>/<prefix>/*` for the state and payload prefixes |
| S3 | `s3:ListBucket` | the bucket, conditioned on `s3:prefix` |
| KMS | `kms:GenerateDataKey`, `kms:Decrypt` | the bucket's key, if SSE-KMS uses a customer key |
| RDS | `rds-db:connect` | `arn:aws:rds-db:<region>:<account>:dbuser:<cluster resource id>/pgcdc`, with IAM authentication |

## 2. ECS settings

- **Deployment:** minimum healthy 100%, maximum 200%, deployment circuit breaker on. The new task starts, validates and waits as a standby; the old one hands over on SIGTERM. A new task that never turns healthy rolls the deploy back with the old task still streaming.
- **`stopTimeout`:** at least `2 x drain_timeout + 3 s`, 13 s at the defaults; use 15 s or more. A SIGKILL at `stopTimeout` turns the handover into a crash takeover, which loses nothing but costs the successor `fence_delay`.
- **Health check in the task definition:** `["CMD", "/usr/local/bin/pg-cdc", "health"]`, for example with interval 10 s, timeout 3 s, retries 3 and start period 30 s. ECS ignores the image's own `HEALTHCHECK`. Set `PGCDC__ADMIN__LISTEN` in the task's environment when `admin.listen` is not `127.0.0.1:9187`; `pg-cdc health` and `pg-cdc parked` read it.
- **Desired count:** 1, or 2 for a hot standby. Two tasks that share a network namespace need distinct admin ports.
- **Source capacity:** one free WAL sender while a task starts (section 1).
- **Verify the image.** Every published image is signed keylessly by the `ci` workflow. Before deploying a tag, resolve it to a digest, check the signature, and deploy by digest:

  ```sh
  cosign verify ghcr.io/rajp33/pg-cdc@sha256:<digest> \
    --certificate-oidc-issuer https://token.actions.githubusercontent.com \
    --certificate-identity-regexp '^https://github\.com/rajp33/pg-cdc/\.github/workflows/ci\.yml@refs/(heads/main|tags/v.+)$'
  ```

  `docker buildx imagetools inspect ghcr.io/rajp33/pg-cdc@sha256:<digest> --format '{{json .SBOM}}'` (or `.Provenance`) shows the attached SBOM and build provenance.

## 3. Clean handover and crash takeover

The replication slot is the lock: one task streams, every other task waits as a standby and polls `pg_replication_slots` every 2 s. What the logs show:

| Log message | Meaning |
|---|---|
| `waiting as standby` (`holder_pid`) | the slot is held by another task's walsender |
| `took over the slot` (`epoch`, `kind`, `holder`) | this task is active; `kind` is `first` (no record yet), `clean` (the previous holder released the record) or `unclean` (it did not, so this task waited `fence_delay` first) |
| `the previous holder did not release the slot cleanly; waiting fence_delay before writing` | an unclean takeover: this task holds the slot and waits `fence_delay` before its first write |
| `released the leader record` (`epoch`, `confirmed_lsn`) | a clean SIGTERM handover finished; the successor starts at once |
| `leaving the leader record active; the next holder waits fence_delay` (`abandoned_writes`, `lanes`, `fenced_by_lease`) | the shutdown abandoned a write, was fenced, or its lanes had not stopped when the 3 s handover tail ran out (`lanes` is `Unfinished`) |
| `could not send the final status update; leaving the leader record active` (`failure`) | the handover's last status update failed or did not finish within the 3 s tail; the record stays `active` |
| `could not release the leader record within the handover tail` (`epoch`) | the final swap to `released` did not finish in time; the write may have landed with its response lost, so the record is either `active` or `released`. The successor reads it and waits `fence_delay` only if it is still `active` |
| `lost the slot; fenced` (`reason`) | no lease check succeeded for `lease_timeout`, or the slot was missing, free or held by another walsender; the task stopped writing and returns to standby |

**Expected gaps.** Measured under 500 events/s with the defaults ([docs/load.md](load.md#handover-criterion-5)): over five SIGTERM handovers the old task exited within 0.10 s, the new task answered `/readyz` within 0.19 s to 1.85 s (the spread is up to one 2 s standby poll), the longest pause in deliveries was 1.95 s, and nothing was lost or duplicated. After a crash, a SIGKILL or a fence, the successor waits `fence_delay` (default 16 s) after taking the slot before it writes, and replays from the last confirmed position, so consumers see duplicates (dedupe on the event `id`).

**The leader record** is `leader.json` in the state store (`s3://<bucket>/<prefix>/<pipeline>/leader.json`, or `<path>/<pipeline>/leader.json`): `{"epoch", "holder", "state": "active"|"released", "confirmed_lsn", "updated_ms"}` (the filesystem store wraps it as `{"version", "record"}`). The epoch rises by one at every takeover. Do not edit it by hand.

**Two tasks report `pgcdc.role` 1, or none does.** With telemetry on, count the `service.instance.id`s reporting `pgcdc.role == 1` over a minute; it should be 1.

- *None active.* Check `/status` on each task (`curl -s 127.0.0.1:9187/status`). If every task is a standby, check that the slot is free and not held by something else (`SELECT active, active_pid FROM pg_replication_slots WHERE slot_name = 'pgcdc_orders'`; compare `active_pid` with `pg_stat_replication`). If `/healthz` is 503, the logs say why: startup validation (a missing or lost slot, a missing publication, a refused replication login, no free WAL sender), the sink preflight or the state store check. A task that took over uncleanly waits `fence_delay` (default 16 s) before it delivers, and is not ready meanwhile.
- *Two active.* Only one walsender can hold a slot, so two active tasks of one pipeline means they use different slots, databases or pipelines, or one is a paused process that has not noticed its fence (section 12.8 of the design). Compare `slot`, `pipeline` and `holder` in each task's `/status` and stop the misconfigured one. The second task fences itself within `lease_timeout` if it does not hold the slot.

## 4. Aurora failover and slot loss

Whether an Aurora logical slot survives a writer failover is not documented, and an AWS post says slots must be recreated. Test it in staging before production (the checklist below).

**Symptoms of a lost slot:**

- `lost the slot; fenced` with `the slot is missing`, then startup validation logging `replication slot pgcdc_orders does not exist` or `replication slot pgcdc_orders is lost (wal_status lost); recreate it`;
- `/healthz` 503 on every task, `/status` `role` `standby` everywhere, no active task in `pgcdc.role`;
- `pgcdc.status_update.age` and `pgcdc.slot.retained_bytes` stop reporting.

**Recovery.** First tell a missing slot from an invalidated one, which still exists:

```sql
SELECT slot_name, active, active_pid, wal_status FROM pg_replication_slots WHERE slot_name = 'pgcdc_orders';
```

No row means the slot is missing. A row with `wal_status` `lost` means it was invalidated. Creating a slot under that name then fails with `replication slot "pgcdc_orders" already exists`, so drop the old slot first.

1. Confirm the publication still exists and lists the tables: `SELECT * FROM pg_publication_tables WHERE pubname = 'pgcdc_orders';`. Recreate it first if it is gone (section 1).
2. Record where reconciliation starts, before you drop or recreate anything. If the slot row is still visible, record `SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'pgcdc_orders';`. Otherwise take `confirmed_lsn` from the latest `confirmed position` log line of any pg-cdc task of this pipeline; the active task logs it at info at most once a minute. Every change in a transaction committed at or below that LSN was delivered or parked. A stale line, or a slot position that trails pg-cdc, only moves the start earlier, which is safe. Ignore `last_confirmed_commit_unix_ms` for this: commit times are not ordered by LSN and the line can be arbitrarily old under backpressure, so a time cannot bound the gap.
3. *Invalidated slot only:* check that it is inactive (`active` is `f`, `active_pid` empty). A pg-cdc task cannot stream from a lost slot, so an active one is held by something else; find that process from `active_pid` in `pg_stat_activity` and stop it before you go on. Then drop the slot: `SELECT pg_drop_replication_slot('pgcdc_orders');`.
4. Create the slot: `SELECT pg_create_logical_replication_slot('pgcdc_orders', 'pgoutput');`.
5. Leave the tasks running. Validation retries with backoff, then passes. One task logs `took over the slot` and `/healthz` and `/readyz` turn 200. Check that the slot query above now shows `wal_status` `reserved` and `active` `t`. Parked events in the state store are kept and drain as usual.
6. Record the gap, bounded by LSNs, not times. It runs from the start LSN recorded in step 2 through the new slot's starting position (the `lsn` that step 4's `pg_create_logical_replication_slot` returns): any transaction committed in that range may be missing, including changes committed well before the loss if delivery and parking had stalled, and pg-cdc cannot replay them. Not all of them are: an earlier transaction still in flight holds the confirmed position back, so later transactions in the range may already have been delivered or parked. Consumers can compare the range with the LSN in each event `id`. Reconcile the rows those transactions touched if you can identify them from the LSN range, for example by decoding archived WAL; otherwise, or if step 2 found no trustworthy LSN, reconcile the published tables in full, and let consumers dedupe on the event `id`. Parked events are not part of the gap: they stay in the state store and drain normally after the recreation. Dropping an invalidated slot loses nothing more, because the WAL it needed is already gone. Backfills that close the gap arrive in v1.1.

Both procedures were run against Postgres 17 with a pg-cdc task waiting. For a missing slot, the task logged `replication slot pgcdc_orders does not exist` until step 4, then `took over the slot`. For a slot invalidated by `max_slot_wal_keep_size`, it logged `replication slot pgcdc_orders is lost (wal_status lost); recreate it`. A plain create failed with `already exists`, and after the drop and create the task took over and turned ready.

**Before the loss.** A slot whose `wal_status` is `unreserved` is about to lose WAL it needs (validation logs `replication slot wal_status is unreserved: the WAL it needs may be removed at the next checkpoint`). `max_slot_wal_keep_size` invalidates a slot that falls further behind than its value, and Postgres 18's `idle_replication_slot_timeout` invalidates an idle one; leave both unset or larger than the longest outage you expect pg-cdc to sit through, and alert on `pgcdc.slot.retained_bytes` well below it.

## 5. Parked and dead events

**How events park.** An event still undelivered `delivery.park_after` (default 30 s) after its first failure is written to the state store, with every later event of its order key, so the slot keeps advancing during a sink outage. An event the sink rejects for good (SNS `SenderFault`), or one larger than the sink's limit without a payload store, parks at once as `dead` and blocks its key. Parked queues drain in order once the sink recovers.

**Find the active task.** Parked endpoints answer only on the task holding the slot. `curl -s 127.0.0.1:9187/status` shows `role`; on any other task, `pg-cdc parked` exits 1 with `pg-cdc: 409 Conflict: this task is not the active holder (active holder: <holder>)`. In ECS, run the commands in the active task (`aws ecs execute-command ... --command "/usr/local/bin/pg-cdc parked list --status dead"`).

**Inspect:**

```sh
pg-cdc parked list --status dead
pg-cdc parked show <order key id> --limit 10
```

`list` returns up to 100 queues per page (`--limit` up to 1,000; continue with `--after <next>`), each with its head event, status, attempts, first failure time and last error; `length` is capped at 1,000 (`length_capped`), and a last error longer than about 8 KiB is cut (`last_error_cut`; `show` has it whole). `show` returns the queue's events with their envelopes, up to 8 MiB of stored events per page (continue with `--after <next>`); an event larger than that, such as an oversize row parked without a payload store, is shown as `{"id", "bytes", "withheld": true}` and can be read from the state store at `parked/<order key id>/<event id>.json`.

**Redrive** after fixing the cause (a consumer, a permission, a size limit):

```sh
pg-cdc parked redrive <order key id>
```

The dead head is marked retrying and sent again; if the sink rejects it again it dies again. Redrive answers 409 if the head is not dead.

**Discard** an event that can never be delivered:

```sh
pg-cdc parked discard <order key id> --id <event id>
```

Discard removes the head only if it is still that event (409 with the current head otherwise), so you never discard an event you have not inspected. The key's next event goes out next. A discarded event is gone from the sink's point of view, but if pg-cdc crashes before the event's transaction is confirmed, the replay sends it again, after every such crash, until the transaction is confirmed.

**Answers:**

| Code | Meaning |
|---|---|
| 404 | the key has no parked events |
| 409 | this task is not the active holder (the reply names the holder), or the head is not dead (redrive), is another event (discard), or is still being parked (try again) |
| 503 | pg-cdc is starting, the key's partition is still loading (`Retry-After: 1`), the key's lane already has 16 redrive and discard requests waiting (`Retry-After: 1`; retry once earlier ones answer), or the state store is unavailable |
| 504 | no answer within 30 s, typically during an S3 brownout; the operation may still complete if the lane had started it (one it had not started is skipped), so check with `show` before repeating it |

**Oversize events.** With a `payload_store`, an event larger than the sink's limit is delivered as a pointer (`payload_ref`) to `s3://<bucket>/<prefix>/<pipeline>/payloads/<event id>.json`. Without one it parks dead; configure a payload store and redrive.

**Limits.** At `delivery.max_parked_events` (default 10,000,000) or while the state store is down, pg-cdc stops parking; events stay in flight, the reader stops at `max_inflight_bytes`, and Postgres retains WAL. Near the limit, a key parks only its oldest events that fit and keeps the rest in flight until room frees. After a start or takeover nothing parks until every partition's parked queues have been counted; a state store that keeps failing that count (`cannot load parked queue heads; retrying`) keeps parking off.

**Startup and takeover.** Reading starts at once; delivery for an order key waits until the parked queue heads of its partition (the first byte of the order key id, 256 partitions) have loaded. Replayed events below a queue's head are skipped. Store calls, including the listing during head loading, are bounded by `state_store.max_concurrency`.

**The parked count is approximate.** It is loaded from the store at startup and can over-count replayed events until the next restart, so `max_parked_events` errs towards stalling early. During a large parking backlog it also trails the store.

**Memory bound.** Memory is bounded by `delivery.max_inflight_bytes`, measured on encoded event bodies, plus one event above the bound, up to 65 raw replication messages read ahead of the bound (so the read-ahead depends on the widest row, not the transaction size), one sink batch of parked events per lane loaded for redelivery (`delivery.lanes` x 10 for SNS, x 1,024 for stdout and discard), and the parked order keys below. Measurements are in [docs/load.md](load.md#memory-criterion-6).

**Memory per parked key.** Every order key with a parked queue holds about 0.5 to 1 KB of lane memory while it is parked, outside `max_inflight_bytes`, and a task that takes over loads every parked queue's head before it delivers. `max_parked_events` is the only bound on it (each parked key holds at least one event): budget `max_parked_events` x 1 KB on top of `max_inflight_bytes`, about 10 GB at the default 10,000,000 if every parked event is on its own key, or lower `max_parked_events` to fit the task. A task that runs out of memory after a long outage over many keys crash-loops, and so does its successor, since it loads the same keys; nothing is lost while the slot holds the WAL. Raise the task's memory to recover.

**Known limits measured in v1** ([docs/load.md](load.md)):

- **Drain rate.** A parked backlog drains at the state store's request rate: about 1.1 S3 requests per drained event, within `state_store.max_concurrency` requests in flight, while every new event of a parked key also needs a PUT. Against moto with `max_concurrency: 32`, 112,000 parked events on 1,000 keys drained at 133 events/s over 30 s and still rising (21 to 31 before v1's read-ahead and batched deletes); moto's listing and its total of about 600 requests a second bound that number. At the default 256 the same probe drained nothing, because moto queues listings instead of throttling them and they waited past the client's 10 s attempt timeout. Real S3 at 20 to 50 ms a request should allow roughly 4,000 to 10,000 events/s at the default 256, unmeasured until the pre-production run. While a key's queue drains, its new events wait behind it, and a single key drains one event per GET, publish and delete. Expect delivery lag after a long outage to be the backlog divided by the drain rate, and alert on `pgcdc.parked.oldest_age`.
- **Parking rate during an S3 brownout.** Each parked event is one S3 write, with at most `state_store.max_concurrency` store calls in flight. With SNS down, S3 slowed by 500 ms and the old default of 32, parking stored 58 events/s; at 2 s, 15 events/s, against 1,000 written. The default is now 256, which allows about eight times that; it was not re-measured. The reader stalls at `max_inflight_bytes` and Postgres holds the WAL until S3 recovers; nothing is lost. Raising `state_store.max_concurrency` (up to 4,096) raises that ceiling; a segmented S3 layout is planned for v1.1.
- **Parking against moto** stored 588 events/s, under moto's own `PutObject` ceiling of 628 puts/s; real S3 is expected to be higher but is unmeasured (section 7).
- **A lane stalls while it waits on the store.** Parking, head loading, removals and admin operations run inside the lane that owns the key, and a parked key's queued events are written one after another. While a lane waits on S3 it sends nothing, for its healthy keys too: during a brownout at 500 ms a request, a key with 1,000 queued events holds its lane, one of `delivery.lanes`, for about 500 s. Nothing is lost and other lanes keep flowing; expect `pgcdc.commit_to_delivery` to rise for the keys of a stalled lane.

**S3 requests and cost while parked.** Every event of a parked key costs S3 requests: one `PutObject` to park it and one `GetObject` to read it back for redelivery. Removal is one `DeleteObjects` per delivered batch (up to 10 events per lane, at most 1,000 per request), and each queue is listed once per 1,000 events. That is about 2.1 requests per event; before v1's read-ahead and batched deletes it was about 4 (a `PutObject`, then a `ListObjectsV2`, a `GetObject` and a `DeleteObject` for each event).

- **Cost.** At S3 Standard's $0.005 per 1,000 PUT, POST and LIST requests and $0.0004 per 1,000 GETs, 1,000 events/s while parked costs about $19 an hour in PUTs and GETs, plus up to about $2 an hour for the `DeleteObjects` requests. Keys without a parked queue cost nothing in S3.
- **Request rate.** S3 supports at least 3,500 PUT, COPY, POST and DELETE and 5,500 GET and HEAD requests per second per partitioned prefix. Each order key's queue is its own prefix (`parked/<order key id>/`), so one hot key is far below that and the total spreads across keys; a new bucket may answer `503 SlowDown` while S3 scales out. pg-cdc retries those, and when they persist it backs off and retries without losing or resending events.
- **The knob is `state_store.max_concurrency`** (default 256), the number of S3 requests in flight, shared by parking, draining and head loading. At 20 to 50 ms per request, 256 allows about 5,000 to 12,800 requests/s, roughly 2,400 to 6,000 parked events/s. Raise it if parking or draining falls behind the write rate after an outage (the parked count keeps rising after the sink recovers); lower it to cap the request rate and the cost per second, at the price of a slower park and drain. Against an S3-compatible store or an emulator, lower it if store calls log attempt timeouts (`operation attempt timeout`): a store that queues requests instead of throttling them, as moto does, can hold queued requests past the client's 10 s attempt timeout, and at 256 the moto drain probe removed nothing for that reason.

## 6. Alerts

The metrics need telemetry on (`telemetry.otlp_endpoint`). Starting thresholds:

| Alert | Metric and condition |
|---|---|
| Any dead event | `pgcdc.parked.dead > 0` |
| Oldest parked event older than 10 minutes | `pgcdc.parked.oldest_age > 600` |
| Slot retained bytes above a threshold | `pgcdc.slot.retained_bytes` above 80% of `max_slot_wal_keep_size`, or of the WAL disk budget you allow pg-cdc |
| Status update age above 30 s | `pgcdc.status_update.age > 30` on active tasks (`pgcdc.role == 1`) |
| No active task, or more than one | the number of `service.instance.id`s with `pgcdc.role == 1` over 1 minute is not 1 |
| Reader falls behind | `pgcdc.slot.retained_bytes` rising for 10 minutes while the parked counts are flat, or the 99th percentile of `pgcdc.commit_to_delivery` above 60 s for 10 minutes |
| Takeovers | any `pgcdc.takeovers{kind="unclean"}` |
| SNS throttling | `rate(pgcdc.sink.throttled)` above 0 for 5 minutes; `reason="kms_throttling"` points at the topic's KMS key quota, `throttling` at the account's SNS publish quota |
| S3 throttling | `rate(pgcdc.store.throttled)` above 0 for 5 minutes; lower `state_store.max_concurrency`, or expect `503 SlowDown` to fade as S3 scales the prefixes out |
| Terminal errors | any `pgcdc.errors{kind="terminal"}` (an event parked dead, or the process stopping) |
| Store failing | `rate(pgcdc.errors{component="store",kind="unavailable"})` above 0 for 5 minutes |
| Admin refusals | any `pgcdc.admin.rejected`: a script is sending redrives or discards faster than the lane answers them |

**Throttle and error counters.** Every label value comes from a fixed set; none is a table, key, error message or request id.

| Metric | Attributes | Counts |
|---|---|---|
| `pgcdc.sink.throttled` | `reason`: `throttling`, `kms_throttling`, `service_unavailable` | messages SNS refused for throttling: a whole `PublishBatch` refused counts each of its messages, and an entry failed with a throttling code counts one. The SNS client does not retry, so every throttle is counted once |
| `pgcdc.store.throttled` | `operation`: the `pgcdc.store.duration` operations | S3 attempts answered 503 or 429, including the attempts the S3 client retries by itself (up to 3 per call) |
| `pgcdc.errors` | `component` and `kind`, below | failures pg-cdc acts on, after any client retries |
| `pgcdc.admin.rejected` | `operation`: `redrive`, `discard` | requests answered 503 because the key's lane already had 16 waiting |

| `component` | `kind` | Counted when |
|---|---|---|
| `sink` | `retryable`, `terminal` | per message the sink did not deliver; also each failed sink preflight (`retryable`) |
| `store` | `unavailable`, `not_found`, `conflict`, `malformed` | per failed state or payload store call, admin reads included; `conflict` is often a benign race that pg-cdc reloads from |
| `leader` | `unavailable`, `timeout` | per failed or timed out lease check |
| `source`, `decoder`, `leader`, `delivery` | `retryable`, `timeout`, `malformed`, `terminal` | per ended replication session, by the part that failed; `terminal` when the error stops the process, including the third decode fault at one position |

The counters overlap on purpose, so do not add them up: a throttled SNS publish counts in `pgcdc.sink.throttled` and, for the same messages, in `pgcdc.errors{component="sink",kind="retryable"}`. A throttled S3 attempt that a retry recovers counts only in `pgcdc.store.throttled`; one whose call still fails also counts once in `pgcdc.errors{component="store",kind="unavailable"}`.

Without telemetry, the same signals are in the logs and `/status`: `delivery failed; retrying` (warn, first failure per key), parking (`/status` `parked`), `lost the slot; fenced` (error), `took over the slot` with `kind`, validation errors, and `/healthz`. pg-cdc logs one `delivery failed; retrying` per key when a sink outage starts, so expect a burst of warnings with many keys.

**Error reporting (Sentry).** Optional and off unless a DSN is set, in the config or the environment (`PGCDC__TELEMETRY__SENTRY__DSN`, or the standard `SENTRY_DSN` when the config sets none). It works with or without `telemetry.otlp_endpoint`.

```yaml
telemetry:
  sentry:
    dsn: https://<public key>@<org>.ingest.sentry.io/<project>
    environment: production   # optional; Sentry's default otherwise
    sample_rate: 1.0          # share of error events sent, 0 to 1; default 1.0
```

| Key | Default |
|---|---|
| `telemetry.sentry.dsn` | unset (reporting off); an `http://` or `https://` DSN |
| `telemetry.sentry.environment` | unset |
| `telemetry.sentry.sample_rate` | `1.0` |

- **Sent:** every ERROR log line as an event (message, target, file and line, its fields), the WARN lines before it as breadcrumbs, and panics with their stack. Each event carries the release `pg-cdc@<version>`, the environment, the task's holder id as `server_name`, the tags `pipeline` and `holder`, and the OS and Rust runtime. INFO and DEBUG lines, spans and performance traces are never sent. Check the setup with a dead event or a failed startup: the error shows up within seconds.
- **Never sent:** row data. pg-cdc logs no column values, event bodies or message attributes at WARN or ERROR, and `send_default_pii` is off. Before each event leaves, fields whose names look like credentials or row data (`password`, `token`, `secret`, `dsn`, `url`, `body`, `payload`, `row`, `before`, `after`, `value`, ...) are replaced with `[redacted]`, and in every message, field and breadcrumb the user and password of a URL (`postgres://user:pass@host`) and the values of `password=`, `token=`, `secret=` and signature pairs (an RDS IAM token's `X-Amz-Security-Token` and `X-Amz-Signature`) are redacted. Error text from SNS, S3 and Postgres is forwarded after that scrubbing.
- **Shutdown:** queued events are flushed for at most 2 s at exit, and after SIGTERM only within what remains of the `2 x drain_timeout + 3 s` budget, so a 15 s `stopTimeout` still holds. A Sentry outage never blocks delivery: events go through a bounded queue on their own thread and are dropped when it is full.
- **Builds:** the `sentry` cargo feature (default on, in the published image) compiles it in. A build with `--no-default-features` ignores the DSN and logs `error reporting is configured, but this build has no sentry feature`.

## 7. Pre-production checklist

Run these once against real AWS before production. Nothing in CI runs them.

1. **Throughput against real SNS (criterion 4).** On an instance sized like production, with a topic that has no expensive subscriptions:

   ```sh
   cargo run --release -p pg-cdc-loadgen -- load sns --pg-cdc target/release/pg-cdc \
     --topic-arn <arn> --real-aws --report-dir target/load
   ```

   It starts Postgres in Docker, uses the default AWS credential chain and region for SNS and a local filesystem state store, and writes 6,000 events/s for 60 s. Criterion 4 passes only if the run delivers at least 5,000 events/s over the measured window, keeps up (lag below 16 MiB at the end) and has a p99 `pgcdc.commit_to_delivery` upper bound below 1 s (`criterion_4: "pass"` in `target/load/sns.json`). Record the result in `docs/load.md` and regenerate the summary with `loadgen report`; until then criterion 4 is unverified, and moto's ceiling of 4,364 messages/s cannot settle it.
2. **Bounded WAL during an outage (criterion 3).** The outage scenario on moto parked at moto's own `PutObject` ceiling; repeat the measurement against a real bucket (a staging deployment with SNS denied for a minute) and check `pgcdc.slot.retained_bytes` stays near `park_after` of write volume.
3. **Topic limits.** Check the `SNS topic ready` log line: `max_message_bytes` and `limit_from_topic` show the limit read from `MaximumMessageSize`. For a topic configured above 256 KiB (up to 1 MiB), publish a batch whose total exceeds 256 KiB and confirm SNS accepts it; that `PublishBatch` limit is unverified.
4. **Aurora failover.** In staging, fail the writer over while pg-cdc streams. Record whether the slot survives: if it does, pg-cdc reconnects through the cluster endpoint and resumes; if not, follow section 4 and keep that procedure ready for production.
5. **Deploy.** Run one ECS deploy under load and check the handover in the logs (`released the leader record`, then `took over the slot` with `kind` `clean`).

## 8. Shutdown, health and startup validation

**Standby.** A task that finds the slot held logs `waiting as standby`. It holds one control connection, polls `pg_replication_slots` every 2 s and takes the slot when it frees; if another standby wins the race, it keeps polling. A standby never writes to the sink or the state store.

**Shutdown on SIGTERM.** The active task stops reading, gives in-flight events `leadership.drain_timeout` (default 5 s) to deliver, parks whatever remains within at most another `drain_timeout`, stops its lanes, sends a final status update, marks the leader record `released` with the confirmed LSN, closes the replication connection and exits 0. The whole sequence takes at most `2 x drain_timeout + 3 s`, 13 s at the defaults. Events that could neither deliver nor park replay on the next start. A clean handover leaves the successor a gap of the old task's shutdown plus one standby poll. If the task was fenced, abandoned a write, could not stop its lanes within the 3 s tail or could not send the final status update, the record stays `active` and the successor waits `fence_delay` (section 3 lists the log lines).

**Health endpoints.**

- `GET /healthz` is 200 when the sink preflight, the state store check and startup validation have passed and, for an active task, the lease is valid; otherwise 503. A standby is healthy.
- `GET /readyz` is 200 only when the task is healthy, active and delivering, so a standby answers 503.
- `pg-cdc health [--addr <ip:port>]` exits 0 only when `/healthz` answers 200 within 2 s, and exits 1 with one line on stderr otherwise. It takes its address from `--addr`, else `PGCDC__ADMIN__LISTEN`, else `127.0.0.1:9187`. Use it as the container health check, since the distroless image has no curl.

**Startup validation.** Before a task goes standby or active, it checks `wal_level`, the slot (exists, `pgoutput`, this database, not `lost`) and the publication, then opens and closes a replication connection without starting replication, so a role that cannot authenticate for replication or lacks `REPLICATION` (`rds_replication` on RDS) fails here instead of after the old task stops. A failure keeps the task unhealthy and retrying; it never exits and never creates or drops anything. In a start-first deploy, a misconfigured new task never turns healthy and the old task keeps running. The check needs a spare WAL sender (section 1).

**Paused processes.** A process paused for longer than `fence_delay` (a frozen VM, a stopped container) can land writes that were already in flight when it resumes, and a request S3 already received can be applied after `fence_delay` even without a pause. The result is duplicate deliveries and, rarely, one key's events out of order, never loss; consumers should dedupe on the event `id`.

## 9. Admin API reference

The admin endpoint (`admin.listen`, default `127.0.0.1:9187`) serves health, status and the parked queues. It has no authentication, and parked events hold row data: keep it on loopback or a private network.

| Endpoint | Answer |
|---|---|
| `GET /healthz`, `GET /readyz` | `200 ok` / `200 ready`, or 503 (section 8) |
| `GET /status` | `{"pipeline", "slot", "holder", "role", "healthy", "ready", "epoch", "confirmed_lsn", "inflight": {"events", "bytes"}, "parked": {"events", "keys", "dead", "oldest_age_seconds"}, "status_update_age_seconds", "slot_retained_bytes", "tables_without_full_identity"}`; `role` is `standby`, `active` or `draining` |
| `GET /parked?status=dead\|retrying&limit=N&after=<order key id>` | `{"queues": [{"order_key", "head", "status", "length", "length_capped", "attempts", "first_failure_ms", "last_error", "last_error_cut"}], "next"}`; a listing reads at most 8 KiB of each head, never its envelope, and cuts a longer last error (`last_error_cut`) |
| `GET /parked/{order key id}?after=<event id>&limit=N` | `{"order_key", "events": [{"id", "status", "attempts", "first_failure_ms", "last_error", "attributes", "envelope"}], "next"}`; an event stored larger than 8 MiB appears as `{"id", "bytes", "withheld": true}` |
| `POST /parked/{order key id}/redrive` | `{"order_key", "id", "status": "retrying"}`: the dead head is retried, and dies again if the sink still rejects it |
| `POST /parked/{order key id}/discard?id=<event id>` | `{"order_key", "discarded"}`: removes the head only if it is still that event |

- **Pages.** `limit` is 1 to 1,000 (default 100). Pass `next` back as `after` for the next page; `next` is `null` at the end. One listing examines at most 1,000 queues, so a `status=dead` filter over many retrying queues can return fewer than `limit` with a `next` to continue from. A queue page also stops at 8 MiB of stored events. At most two listings and queue pages are read at once and others wait their turn; each keeps its turn until its reply has been sent or its connection has closed (after at most 60 s).
- **Lengths are capped.** `length` counts up to 1,000 events, and `length_capped` is `true` for a queue at least that long; `pgcdc.parked.events` and `/status` give the total. With the S3 state store each listing does work bounded by its page, whatever the backlog. The filesystem store has no ordered index: a listing reads every key directory under `parked/` and every file name in each queue it returns, and finding a head reads all of its queue's entries, so `--limit 1` over a queue of a million events still scans a million names.
- **Only the active task answers.** A standby, a task waiting `fence_delay`, a draining task and a fenced task answer every parked endpoint with `409 {"error": "this task is not the active holder", "role", "holder", "leader_state", "epoch"}`, where the last three come from the leader record. Before the state store is ready the endpoints answer `503 {"error": "pg-cdc is starting"}`. An operation runs inside the lane that owns the key, at most 4 between deliveries; one that takes longer than 30 s answers `504`, and is skipped if the lane has not started it by then. Each lane queues at most 16 redrive and discard requests; past that the endpoints answer `503` with `Retry-After: 1`.
- **Ids.** An order key id is 32 hex digits; an event id is `<commit LSN as 16 hex digits>-<ordinal as 8 hex digits>`, for example `000000001a2b3c4d-00000003`.

`pg-cdc parked list | show | redrive | discard` is a client for these endpoints (section 5 has the procedure). It takes `--addr` like `pg-cdc health`, prints a successful reply as JSON on stdout and exits 0, prints `pg-cdc: <code> <reason>: <error>` on stderr and exits 1 otherwise (adding `(active holder: <holder>)` to a 409), and exits 2 on a malformed id before sending anything. It waits up to 40 s.

```sh
pg-cdc parked list --status dead
pg-cdc parked list --limit 50 --after 6b1f0c2d9e8a7b6c5d4e3f2a1b0c9d8e
pg-cdc parked show 6b1f0c2d9e8a7b6c5d4e3f2a1b0c9d8e --limit 10
pg-cdc parked redrive 6b1f0c2d9e8a7b6c5d4e3f2a1b0c9d8e
pg-cdc parked discard 6b1f0c2d9e8a7b6c5d4e3f2a1b0c9d8e --id 000000001a2b3c4d-00000003
```

## 10. Telemetry reference

Telemetry is configured under `telemetry` ([docs/configuration.md](configuration.md#telemetry)). Metrics use the meter `pg-cdc`; durations are histograms in seconds with buckets from 1 ms to 60 s.

| Metric | Kind | Attributes |
|---|---|---|
| `pgcdc.events` | counter | `table`, `op`, `outcome` (`received`, `delivered`, `parked`, `dead`) |
| `pgcdc.commit_to_delivery` | histogram (s) | (live deliveries only, from the transaction's commit time) |
| `pgcdc.sink.duration` | histogram (s) | `outcome` (`delivered`, `retryable`, `terminal`) |
| `pgcdc.store.duration` | histogram (s) | `operation` |
| `pgcdc.takeovers` | counter | `kind` (`clean`, `unclean`; a first takeover counts as `clean`) |
| `pgcdc.decode.fallbacks` | counter | `type` |
| `pgcdc.parked.events`, `pgcdc.parked.keys`, `pgcdc.parked.dead` | gauge | |
| `pgcdc.parked.oldest_age` | gauge (s) | |
| `pgcdc.inflight.events`, `pgcdc.inflight.bytes` | gauge | |
| `pgcdc.slot.retained_bytes` | gauge | (polled every 10 s, standbys included) |
| `pgcdc.status_update.age` | gauge (s) | |
| `pgcdc.role` | gauge | 0 standby, 1 active, 2 draining |
| `pgcdc.leader.epoch` | gauge | |
| `pgcdc.tables_without_full_identity` | gauge | |

Spans: `transaction` (`commit_lsn`, `xid`, `events`), `sink_send` (`sink`, `size`, `delivered`, `retryable`, `terminal`), `store` (`operation`, `count`), `park` and `drain` (`events`) and `status_update` (`lsn`) at debug; `takeover` (`holder`, `kind`, `epoch`) and `role_change` (`from`, `to`) at info. Only pg-cdc's own spans at debug and above are exported, sampled by `trace_sample_ratio`. Logs at info and above are exported too, and while telemetry is enabled a JSON log line written inside a sampled span gains `"trace_id"` (32 hex digits) and `"span_id"` (16 hex digits).

## 11. Container image

`ghcr.io/rajp33/pg-cdc` is built for linux/amd64 and linux/arm64 on distroless (`gcr.io/distroless/cc-debian13:nonroot`) and runs as the `nonroot` user. Tags: `main` for the main branch, `sha-<short commit>`, and for release tags `<version>`, `<major>.<minor>` and, for a release tag `vX.Y.Z` without a pre-release suffix, `latest`. The `ci` workflow builds both architectures on every pull request without pushing, and publishes only after every CI job passes for that commit. Each published image carries an SBOM and build provenance and is signed with cosign (section 2 shows how to verify it).

The entrypoint is `/usr/local/bin/pg-cdc` and the default command is `run --config /etc/pg-cdc/config.yaml`. The image's `HEALTHCHECK` runs `pg-cdc health`; ECS ignores image health checks, so set the same command in the task definition (section 2).

```sh
docker run --rm -v "$PWD/pg-cdc.yaml:/etc/pg-cdc/config.yaml:ro" \
  -e PGCDC__SOURCE__CREDENTIALS__PASSWORD -p 127.0.0.1:9187:9187 \
  -e PGCDC__ADMIN__LISTEN=0.0.0.0:9187 ghcr.io/rajp33/pg-cdc:main
```

Binding the admin endpoint to `0.0.0.0` inside a container exposes it to the container's network; publish its port only to trusted hosts, since it has no authentication.
