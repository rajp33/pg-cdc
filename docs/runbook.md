# pg-cdc runbook

How to set up, deploy, watch and repair a pg-cdc pipeline. The [README](../README.md) describes the configuration; [docs/load.md](load.md) has the measured numbers quoted here. Commands assume the pipeline `orders`, the slot and publication `pgcdc_orders`, and the role `pgcdc`.

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

## 3. Clean handover and crash takeover

The replication slot is the lock: one task streams, every other task waits as a standby and polls `pg_replication_slots` every 2 s. What the logs show:

| Log message | Meaning |
|---|---|
| `waiting as standby` (`holder_pid`) | the slot is held by another task's walsender |
| `took over the slot` (`epoch`, `kind`, `holder`) | this task is active; `kind` is `first` (no record yet), `clean` (the previous holder released the record) or `unclean` (it did not, so this task waited `fence_delay` first) |
| `the previous holder did not release the slot cleanly; waiting fence_delay before writing` | an unclean takeover: this task holds the slot and waits `fence_delay` before its first write |
| `released the leader record` (`epoch`, `confirmed_lsn`) | a clean SIGTERM handover finished; the successor starts at once |
| `leaving the leader record active; the next holder waits fence_delay` (`abandoned_writes`, `lanes`, `fenced_by_lease`) | the shutdown abandoned a write, was fenced, or its lanes had not stopped when the 3 s handover tail ran out (`lanes` is `Unfinished`) |
| `lost the slot; fenced` (`reason`) | no lease check succeeded for `lease_timeout`, or the slot was missing, free or held by another walsender; the task stopped writing and returns to standby |

**Expected gaps.** Measured under 500 events/s with the defaults ([docs/load.md](load.md#handover-criterion-5)): over five SIGTERM handovers the old task exited within 0.10 s, the new task answered `/readyz` within 0.19 s to 1.85 s (the spread is up to one 2 s standby poll), the longest pause in deliveries was 1.95 s, and nothing was lost or duplicated. After a crash, a SIGKILL or a fence, the successor waits `fence_delay` (default 10 s) after taking the slot before it writes, and replays from the last confirmed position, so consumers see duplicates (dedupe on the event `id`).

**The leader record** is `leader.json` in the state store (`s3://<bucket>/<prefix>/<pipeline>/leader.json`, or `<path>/<pipeline>/leader.json`): `{"epoch", "holder", "state": "active"|"released", "confirmed_lsn", "updated_ms"}` (the filesystem store wraps it as `{"version", "record"}`). The epoch rises by one at every takeover. Do not edit it by hand.

**Two tasks report `pgcdc.role` 1, or none does.** With telemetry on, count the `service.instance.id`s reporting `pgcdc.role == 1` over a minute; it should be 1.

- *None active.* Check `/status` on each task (`curl -s 127.0.0.1:9187/status`). If every task is a standby, check that the slot is free and not held by something else (`SELECT active, active_pid FROM pg_replication_slots WHERE slot_name = 'pgcdc_orders'`; compare `active_pid` with `pg_stat_replication`). If `/healthz` is 503, the logs say why: startup validation (a missing or lost slot, a missing publication, a refused replication login, no free WAL sender), the sink preflight or the state store check. A task that took over uncleanly waits `fence_delay` (default 10 s) before it delivers, and is not ready meanwhile.
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
2. *Invalidated slot only:* check that it is inactive (`active` is `f`, `active_pid` empty). A pg-cdc task cannot stream from a lost slot, so an active one is held by something else; find that process from `active_pid` in `pg_stat_activity` and stop it before you go on. Then drop the slot: `SELECT pg_drop_replication_slot('pgcdc_orders');`.
3. Create the slot: `SELECT pg_create_logical_replication_slot('pgcdc_orders', 'pgoutput');`.
4. Leave the tasks running. Validation retries with backoff, then passes. One task logs `took over the slot` and `/healthz` and `/readyz` turn 200. Check that the slot query above now shows `wal_status` `reserved` and `active` `t`. Parked events in the state store are kept and drain as usual.
5. Record the gap. Changes committed between the loss and the recreation are not captured, and pg-cdc cannot replay them. Dropping an invalidated slot loses nothing more, because the WAL it needed is already gone. Backfills that close the gap arrive in v1.1. Until then, reconcile from the source if consumers need those changes.

Both procedures were run against Postgres 17 with a pg-cdc task waiting. For a missing slot, the task logged `replication slot pgcdc_orders does not exist` until step 3, then `took over the slot`. For a slot invalidated by `max_slot_wal_keep_size`, it logged `replication slot pgcdc_orders is lost (wal_status lost); recreate it`. A plain create failed with `already exists`, and after the drop and create the task took over and turned ready.

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

Discard removes the head only if it is still that event (409 with the current head otherwise), so you never discard an event you have not inspected. The key's next event goes out next. A discarded event is gone from the sink's point of view, but if pg-cdc crashes before the event's transaction is confirmed, the replay can send it once more.

**Answers:**

| Code | Meaning |
|---|---|
| 404 | the key has no parked events |
| 409 | this task is not the active holder (the reply names the holder), or the head is not dead (redrive), is another event (discard), or is still being parked (try again) |
| 503 | pg-cdc is starting, the key's partition is still loading (`Retry-After: 1`), or the state store is unavailable |
| 504 | no answer within 30 s, typically during an S3 brownout; the operation may still complete, so check with `show` before repeating it |

**Oversize events.** With a `payload_store`, an event larger than the sink's limit is delivered as a pointer (`payload_ref`) to `s3://<bucket>/<prefix>/<pipeline>/payloads/<event id>.json`. Without one it parks dead; configure a payload store and redrive.

**Limits.** At `delivery.max_parked_events` (default 10,000,000) or while the state store is down, pg-cdc stops parking; events stay in flight, the reader stops at `max_inflight_bytes`, and Postgres retains WAL.

**Known limits measured in v1** ([docs/load.md](load.md)):

- **Drain rate.** A parked backlog drains at the state store's request rate: about 1.1 S3 requests per drained event, within `state_store.max_concurrency` requests in flight, while every new event of a parked key also needs a PUT. Against moto, 112,000 parked events on 1,000 keys drained at 133 events/s over 30 s and still rising (21 to 31 before v1's read-ahead and batched deletes); moto's listing and its total of about 600 requests a second bound that number. Real S3 at 20 to 50 ms a request should allow roughly 4,000 to 10,000 events/s at the default 256, unmeasured until the pre-production run. While a key's queue drains, its new events wait behind it, and a single key drains one event per GET, publish and delete. Expect delivery lag after a long outage to be the backlog divided by the drain rate, and alert on `pgcdc.parked.oldest_age`.
- **Parking rate during an S3 brownout.** Each parked event is one S3 write, with at most `state_store.max_concurrency` store calls in flight. With SNS down, S3 slowed by 500 ms and the old default of 32, parking stored 58 events/s; at 2 s, 15 events/s, against 1,000 written. The default is now 256, which allows about eight times that; it was not re-measured. The reader stalls at `max_inflight_bytes` and Postgres holds the WAL until S3 recovers; nothing is lost. Raising `state_store.max_concurrency` (up to 4,096) raises that ceiling; a segmented S3 layout is planned for v1.1.
- **Parking against moto** topped out at 632 events/s, moto's own `PutObject` ceiling; real S3 is expected to be higher but is unmeasured (section 7).

**S3 requests and cost while parked.** Every event of a parked key costs S3 requests: one `PutObject` to park it and one `GetObject` to read it back for redelivery. Removal is one `DeleteObjects` per delivered batch (up to 10 events per lane, at most 1,000 per request), and each queue is listed once per 1,000 events. That is about 2.1 requests per event; before v1's read-ahead and batched deletes it was about 4 (a `PutObject`, then a `ListObjectsV2`, a `GetObject` and a `DeleteObject` for each event).

- **Cost.** At S3 Standard's $0.005 per 1,000 PUT, POST and LIST requests and $0.0004 per 1,000 GETs, 1,000 events/s while parked costs about $19 an hour in PUTs and GETs, plus up to about $2 an hour for the `DeleteObjects` requests. Keys without a parked queue cost nothing in S3.
- **Request rate.** S3 supports at least 3,500 PUT, COPY, POST and DELETE and 5,500 GET and HEAD requests per second per partitioned prefix. Each order key's queue is its own prefix (`parked/<order key id>/`), so one hot key is far below that and the total spreads across keys; a new bucket may answer `503 SlowDown` while S3 scales out. pg-cdc retries those, and when they persist it backs off and retries without losing or resending events.
- **The knob is `state_store.max_concurrency`** (default 256), the number of S3 requests in flight, shared by parking, draining and head loading. At 20 to 50 ms per request, 256 allows about 5,000 to 12,800 requests/s, roughly 2,400 to 6,000 parked events/s. Raise it if parking or draining falls behind the write rate after an outage (the parked count keeps rising after the sink recovers); lower it to cap the request rate and the cost per second, at the price of a slower park and drain. Lower it too if store calls log `operation attempt timeout`: a store that queues requests instead of throttling them, as moto does, can hold queued requests past the client's 10 s attempt timeout.

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

Without telemetry, the same signals are in the logs and `/status`: `delivery failed; retrying` (warn, first failure per key), parking (`/status` `parked`), `lost the slot; fenced` (error), `took over the slot` with `kind`, validation errors, and `/healthz`. pg-cdc logs one `delivery failed; retrying` per key when a sink outage starts, so expect a burst of warnings with many keys.

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
