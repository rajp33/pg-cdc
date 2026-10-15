# pg-cdc

pg-cdc streams row changes for the tables in one Postgres publication through a logical replication slot to a sink. Delivery is at least once, and pg-cdc never writes to the source database.

Status: milestone 4 of the [design](docs/superpowers/specs/2026-10-14-pg-cdc-design.md). Changes are delivered to SNS, stdout or a discard sink through ordered lanes with retries. An event the sink cannot take within `delivery.park_after` is parked in a state store (S3 or a local directory), so the slot keeps advancing during a sink outage, and parked events are redelivered in order once the sink recovers. Tasks coordinate through the replication slot, so a deploy can start the new task before the old one stops and hand the slot over without losing a change. The admin API for parked events arrives in milestone 5.

## Run

Create the publication and slot first (pg-cdc never creates or drops either):

```sql
CREATE PUBLICATION pgcdc_orders FOR TABLE public.orders;
ALTER TABLE public.orders REPLICA IDENTITY FULL;
SELECT pg_create_logical_replication_slot('pgcdc_orders', 'pgoutput');
```

Then write a config file and run:

```yaml
pipeline: orders
source:
  host: db.internal
  database: app
  user: pgcdc
  credentials:
    type: password
  tls:
    mode: verify-full
  slot: pgcdc_orders
  publication: pgcdc_orders
delivery:
  order_by: key
  lanes: 64
  max_inflight_bytes: 256MiB
  park_after: 30s
  max_parked_events: 10000000
sink:
  type: sns
  topic_arn: arn:aws:sns:us-east-1:123456789012:orders.fifo
  attribute_columns: [tenant_id]
state_store:
  type: s3
  bucket: pgcdc-state
  prefix: orders/
  max_concurrency: 32
payload_store:
  type: s3
  bucket: pgcdc-payloads
  prefix: orders/
```

```sh
PGCDC__SOURCE__CREDENTIALS__PASSWORD=... pg-cdc run --config pg-cdc.yaml
```

Each change is one SNS message whose body is the JSON envelope, with `schema`, `table`, `op` and each `attribute_columns` value as message attributes. A column value is a `Number` attribute when it is a plain JSON number of at most 38 digits, and a `String` otherwise, including numbers in exponent form such as `1e+300`; null and empty values are omitted. On a FIFO topic the message group is the order key id and the deduplication id is the event id. The SNS sink uses the default AWS credential chain; `sink.region` overrides the region. `sink.max_concurrency` (default 32, any sink type, from 1 to 4096) bounds concurrent sink calls. Retries are owned by pg-cdc: a failing order key backs off from 100 ms to 30 s while other keys keep flowing.

The SNS message limit comes from the topic's `MaximumMessageSize` attribute when `GetTopicAttributes` returns it, and is 256 KiB otherwise; the `SNS topic ready` log at startup shows the limit in use and where it came from. Topics above 256 KiB have not been verified against real AWS. pg-cdc keeps retrying the sink preflight and does not open the replication connection until it succeeds.

## Parking

- **When an event parks.** An event still undelivered `delivery.park_after` (default 30 s) after its first failure is written to the state store, together with every later event of its order key, and counts as settled, so the slot moves on. An event the sink rejects for good (SNS `SenderFault`) is parked at once as `dead`.
- **Parked keys stay in order.** While an order key has parked events, its new events are appended behind them and never sent ahead. pg-cdc redelivers the queue from the store, oldest first, and the key returns to the live path once its queue is empty. Other keys keep flowing throughout.
- **Dead events block their key** until an operator removes them. Until the admin API (milestone 5), stop pg-cdc, delete the dead event's object (`<prefix>/<pipeline>/parked/<order key id>/<event id>.json` in S3, or the same path under `state_store.path`), and start it again.
- **Oversize events.** An event larger than the sink's message limit is stored in the payload store, and the sink receives a pointer instead: the same envelope without `before`, `after` and `unchanged_toast`, plus `"payload_ref": {"uri": "s3://bucket/prefix/pipeline/payloads/<event id>.json", "bytes": 912345}`. Without a `payload_store`, such an event is parked as dead. A lifecycle rule should expire payloads after your consumers' retention period.
- **Startup.** Reading starts at once; delivery for an order key waits until the parked queue heads of its partition (the first byte of the order key id, 256 partitions) have loaded. Replayed events below a queue's head are skipped. Store calls are bounded by `state_store.max_concurrency` (default 32, from 1 to 4096), including listing during head loading.
- **Shutdown.** On SIGTERM, pg-cdc stops reading, gives in-flight events `leadership.drain_timeout` (default 5 s) to deliver, parks whatever remains (another `drain_timeout` at most), sends a final status update and exits 0. Events that could neither deliver nor park replay on the next start. See [Leadership and deploys](#leadership-and-deploys) for the full handover.
- **Limits.** At `delivery.max_parked_events` (default 10,000,000; `0` is unlimited), or while the state store is unavailable, events stay in flight and the reader stops at `delivery.max_inflight_bytes`. pg-cdc checks that the state store answers before it opens the replication connection.
- **State stores.** `state_store: { type: s3, bucket, prefix, region }` uses conditional writes and suits active/standby across hosts; the bucket should use default encryption, because parked objects hold row data. `state_store: { type: filesystem, path: /var/lib/pg-cdc }` keeps the same tree on local disk and suits a single host.

## Leadership and deploys

The replication slot is the lock: Postgres allows one walsender per slot, so only one task streams at a time. Every other task waits as a standby and takes over when the slot frees.

```yaml
leadership:
  lease_timeout: 6s
  fence_delay: 10s
  drain_timeout: 5s
admin:
  listen: 127.0.0.1:9187
```

- **Standby.** A task that finds the slot held logs `waiting as standby`. It holds one control connection, polls `pg_replication_slots` every 2 s, and takes the slot when it frees. If another standby wins the race, it keeps polling. A standby never writes to the sink or the state store.
- **Takeover.** The new holder reads the leader record (`leader.json` in the state store). A `released` record means the previous holder handed over cleanly, and delivery starts at once. An `active` record means the previous holder crashed or was fenced, and the successor waits `fence_delay` before it writes anything. Each takeover logs `took over the slot` with its `epoch` and its `kind` (`first`, `clean` or `unclean`); the epoch rises by one at every takeover.
- **`leadership.lease_timeout`** (default 6 s, above 2 s). Every 2 s the active task checks over the control connection that the slot is still held by its own walsender. If no check succeeds for `lease_timeout`, the task fences itself: it stops writing, drops in-flight work, closes the replication connection and returns to standby.
- **`leadership.fence_delay`** (default 10 s, must exceed `lease_timeout`). What a successor waits after a crash or a fence, so the old holder has stopped writing before the new one starts. A crashed or fenced holder therefore costs its successor `fence_delay`.
- **`leadership.drain_timeout`** (default 5 s). The window for in-flight events to deliver, and again for parking what remains, during a shutdown.
- **Shutdown on SIGTERM.** The task stops reading, drains for `drain_timeout`, parks for at most another `drain_timeout`, stops its lanes, sends a final status update, marks the leader record `released` with the confirmed LSN, closes the replication connection and exits 0. The whole sequence takes at most `2 x drain_timeout + 3 s`, 13 s at the defaults. A clean handover leaves the successor a gap of the old task's shutdown plus one poll, a few seconds, well under 10 s. If the task was fenced, had to abandon a write or could not send the final status update, the record stays `active` and the successor waits `fence_delay`.
- **Admin endpoint.** `admin.listen` is an IP address and port, default `127.0.0.1:9187`; host names are rejected, and port 0 picks a free port that is logged as `admin listening`. The endpoint has no authentication, so keep it on loopback or a private network. Two tasks that share a network namespace, as during a start-first deploy on one host, need distinct ports. A task that cannot bind the address exits 1.
- **`GET /healthz`** is 200 when the sink preflight, the state store check and startup validation have passed, and, for an active task, the lease is valid; otherwise 503. A standby is healthy. **`GET /readyz`** is 200 only when the task is healthy, active and delivering, so a standby answers 503.
- **`pg-cdc health [--addr <ip:port>]`** exits 0 only when `/healthz` answers 200 within 2 s, and exits 1 with one line on stderr otherwise. It takes its address from `--addr`, else `PGCDC__ADMIN__LISTEN`, else `127.0.0.1:9187`. Use it as the container health check, since a distroless image has no curl.
- **Startup validation.** Before a task goes standby or active, it checks `wal_level`, the slot (exists, `pgoutput`, this database, not `lost`) and the publication, then opens and closes a replication connection without starting replication, so a role that cannot authenticate for replication or lacks `REPLICATION` (`rds_replication` on RDS) fails here instead of after the old task stops. A failure keeps the task unhealthy and retrying; it never exits and never creates or drops anything. In a start-first deploy, a misconfigured new task therefore never turns healthy and the old task keeps running.
- **A spare WAL sender.** The replication login check holds a walsender for a moment, so the source needs one free beyond those in use (`max_wal_senders` minus the rows in `pg_stat_replication`) while a task starts. With none free, the check fails with SQLSTATE 53300 (`number of requested standby connections exceeds max_wal_senders`), and the task stays unhealthy and retries: a start-first deploy rolls back with the old task still streaming, and a new task validates only once a sender frees. Raising `max_wal_senders` needs a Postgres restart, so size it with one spare per task that may start at the same time.

### ECS settings

- **Deployment:** minimum healthy 100%, maximum 200%, with the deployment circuit breaker on, so the new task starts before the old one stops and a task that never turns healthy rolls the deploy back.
- **Source capacity:** at least one free WAL sender while the new task starts (see startup validation above).
- **`stopTimeout`:** at least `2 x drain_timeout + 3 s`, which is 15 s at the defaults. A SIGKILL at `stopTimeout` turns the handover into a crash takeover, which is safe but costs `fence_delay`.
- **Health check:** `pg-cdc health`, with `PGCDC__ADMIN__LISTEN` set if `admin.listen` is not the default.
- **Desired count:** 1, or 2 for a hot standby.

### Known limit

A process paused for longer than `fence_delay` (a frozen VM, a stopped container) can land writes that were already in flight when it resumes. The result is duplicate deliveries, never loss; consumers should dedupe on the event `id`.

## Limits in this milestone

- **Memory** is bounded by `delivery.max_inflight_bytes`, measured on encoded event bodies, plus one event above the bound and up to 65 raw replication messages read ahead of the bound, plus at most `delivery.lanes` x 10 parked events loaded for redelivery (one SNS batch per lane). The read-ahead is a fixed number of messages, independent of transaction size, so its size depends on the widest row.
- **The parked count is approximate.** It is loaded from the store at startup and can over-count replayed events until the next restart, so the limit errs towards stalling early.

`delivery.order_by` is `key` (default), `table`, `column:<name>` or `pipeline`. `sink: { type: stdout }` writes each change as a JSON line on stdout and logs to stderr; `sink: { type: discard }` drops every change, for load tests. With the SNS and discard sinks, logs are JSON on stdout. Logs are written from a bounded buffer of 8,192 lines on their own thread, so a stalled log reader never holds up replication or shutdown; lines that do not fit are dropped. Any key can be overridden with `PGCDC__<SECTION>__<KEY>` (lists as comma-separated values), and passwords come only from the environment. For Aurora with RDS IAM auth, use `credentials: { type: aws_rds_iam }` with the default AWS credential chain. pg-cdc runs on Linux and macOS.

## Develop

```sh
mise trust && mise install
mise run check
```

`mise run check` is the only gate. Integration tests need Docker; `PGCDC_TEST_PG_VERSION` selects Postgres 14 to 18 (default 17). See [docs/engineering.md](docs/engineering.md).

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option. Contributions are dual licensed the same way.
