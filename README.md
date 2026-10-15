# pg-cdc

pg-cdc streams row changes for the tables in one Postgres publication through a logical replication slot to a sink. Delivery is at least once, and pg-cdc never writes to the source database.

Status: every v1 milestone of the [design](docs/superpowers/specs/2026-10-14-pg-cdc-design.md) is built (milestone 5 of 5), but v1 is not complete while a success criterion is unverified. Changes are delivered to SNS, stdout or a discard sink through ordered lanes with retries. An event the sink cannot take within `delivery.park_after` is parked in a state store (S3 or a local directory), so the slot keeps advancing during a sink outage, and parked events are redelivered in order once the sink recovers. Tasks coordinate through the replication slot, so a deploy can start the new task before the old one stops and hand the slot over without losing a change. An admin API and `pg-cdc parked` list, redrive and discard parked events, and OpenTelemetry exports traces, metrics and logs.

Measured against the design's section 4 ([docs/load.md](docs/load.md)), on one laptop with Postgres and moto in Docker:

- **Passed:** nothing lost and per-key order held through a 10-minute chaos run (criteria 1 and 2); five deploy handovers under load were ready within 1.85 s with nothing lost (criterion 5); memory stayed flat from 50,000 to 500,000-row transactions, at 122 MiB peak with `max_inflight_bytes: 64MiB` and a stalled sink (criterion 6).
- **Unverified:** 5,000 events/s against SNS with p99 under 1 s (criterion 4) and bounded WAL during a sink outage (criterion 3). In both runs moto's own ceiling (4,364 SNS messages/s, 636 S3 puts/s) was below the load, so they stay unverified until the pre-production runs against a real SNS topic and S3 bucket pass ([runbook](docs/runbook.md#7-pre-production-checklist)). pg-cdc alone drains 228,772 events/s into the discard sink.
- **Not resolved:** telemetry overhead within 1% (criterion 7). Two runs gave +1.71% and -0.73%, with run-to-run spread of 8% to 14% on a shared machine.

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
- **Dead events block their key** until an operator redrives or discards them with `pg-cdc parked` (see [Admin API and parked events](#admin-api-and-parked-events)).
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
- **Health check:** `["CMD", "/usr/local/bin/pg-cdc", "health"]` in the task definition (ECS ignores the image's `HEALTHCHECK`), with `PGCDC__ADMIN__LISTEN` set if `admin.listen` is not the default.
- **Desired count:** 1, or 2 for a hot standby.

### Known limit

A process paused for longer than `fence_delay` (a frozen VM, a stopped container) can land writes that were already in flight when it resumes. The result is duplicate deliveries, never loss; consumers should dedupe on the event `id`.

## Admin API and parked events

The admin endpoint (`admin.listen`, default `127.0.0.1:9187`) serves health, status and the parked queues. **It has no authentication in v1, and parked events hold row data:** keep it on loopback or a private network.

| Endpoint | Answer |
|---|---|
| `GET /healthz`, `GET /readyz` | `200 ok` / `200 ready`, or 503 (see [Leadership and deploys](#leadership-and-deploys)) |
| `GET /status` | `{"pipeline", "slot", "holder", "role", "healthy", "ready", "epoch", "confirmed_lsn", "inflight": {"events", "bytes"}, "parked": {"events", "keys", "dead", "oldest_age_seconds"}, "status_update_age_seconds", "slot_retained_bytes", "tables_without_full_identity"}`; `role` is `standby`, `active` or `draining` |
| `GET /parked?status=dead\|retrying&limit=N&after=<order key id>` | `{"queues": [{"order_key", "head", "status", "length", "length_capped", "attempts", "first_failure_ms", "last_error"}], "next"}` |
| `GET /parked/{order key id}?after=<event id>&limit=N` | `{"order_key", "events": [{"id", "status", "attempts", "first_failure_ms", "last_error", "attributes", "envelope"}], "next"}` |
| `POST /parked/{order key id}/redrive` | `{"order_key", "id", "status": "retrying"}`: the dead head is retried, and dies again if the sink still rejects it |
| `POST /parked/{order key id}/discard?id=<event id>` | `{"order_key", "discarded"}`: removes the head only if it is still that event, so a discard never removes an event you have not seen |

- **Pages.** `limit` is 1 to 1,000 (default 100). Pass `next` back as `after` for the next page; `next` is `null` at the end. One listing examines at most 1,000 queues, so a `status=dead` filter over many retrying queues can return fewer than `limit` with a `next` to continue from.
- **Lengths are capped.** `length` counts up to 1,000 events, and `length_capped` is `true` for a queue at least that long; `pgcdc.parked.events` and `/status` give the total. Each listing does work bounded by its page, whatever the backlog.
- **Only the active task answers.** A standby, a task waiting `fence_delay`, a draining task and a fenced task answer every parked endpoint with `409 {"error": "this task is not the active holder", "role", "holder", "leader_state", "epoch"}`, where the last three come from the leader record. Before the state store is ready the endpoints answer `503 {"error": "pg-cdc is starting"}`. An operation runs inside the lane that owns the key, between its own deliveries; one that takes longer than 30 s answers `504 {"error": "no answer within 30s; the operation may still complete"}`.
- **Ids.** An order key id is 32 hex digits; an event id is `<LSN as 16 hex digits>-<ordinal as 8 hex digits>`, for example `000000001a2b3c4d-00000003`.

`pg-cdc parked` is a client for these endpoints. It takes `--addr` like `pg-cdc health` (else `PGCDC__ADMIN__LISTEN`, else `127.0.0.1:9187`), prints a successful reply as JSON on stdout and exits 0, prints `pg-cdc: <code> <reason>: <error>` on stderr and exits 1 otherwise (adding `(active holder: <holder>)` to a 409), and exits 2 on a malformed id before sending anything. It waits up to 40 s.

```sh
pg-cdc parked list --status dead
pg-cdc parked list --limit 50 --after 6b1f0c2d9e8a7b6c5d4e3f2a1b0c9d8e
pg-cdc parked show 6b1f0c2d9e8a7b6c5d4e3f2a1b0c9d8e --limit 10
pg-cdc parked redrive 6b1f0c2d9e8a7b6c5d4e3f2a1b0c9d8e
pg-cdc parked discard 6b1f0c2d9e8a7b6c5d4e3f2a1b0c9d8e --id 000000001a2b3c4d-00000003
```

Run it against the active task; `/status` shows each task's `role`, and a 409 names the holder. The [runbook](docs/runbook.md#5-parked-and-dead-events) has the full procedure.

## Telemetry

Telemetry is off unless an OTLP endpoint is configured, and then traces, metrics and logs go to it over OTLP/gRPC.

```yaml
telemetry:
  otlp_endpoint: http://otel-collector:4317
  trace_sample_ratio: 0.01
```

- **Enabling.** `telemetry.otlp_endpoint` (an `http://` or `https://` URL, unset by default) or `OTEL_EXPORTER_OTLP_ENDPOINT` turns it on; the config value wins when both are set. `https://` endpoints use rustls with the webpki roots.
- **gRPC only.** If `OTEL_EXPORTER_OTLP_PROTOCOL` is set to anything other than `grpc`, pg-cdc logs a warning and uses gRPC.
- **`OTEL_*` variables.** `OTEL_SERVICE_NAME` (default `pg-cdc`), `OTEL_RESOURCE_ATTRIBUTES` and `OTEL_METRIC_EXPORT_INTERVAL` are honored. Every signal carries `service.instance.id` (the task's holder id), `service.version` and `pgcdc.pipeline`.
- **Sampling.** Traces use `ParentBased(TraceIdRatioBased(trace_sample_ratio))`; `trace_sample_ratio` is 0 to 1, default `0.01`.
- **Never in the way.** Exports go through bounded batch queues that drop when full, and on SIGTERM the exporters get at most 1 s within the shutdown budget.
- **The `otel` feature.** OpenTelemetry is compiled in by default. `cargo build --release -p pg-cdc --no-default-features` builds without it; such a build logs `telemetry is configured, but this build has no otel feature` and exports nothing.

Metrics (meter `pg-cdc`; durations are histograms in seconds with buckets from 1 ms to 60 s):

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

Spans: `transaction` (`commit_lsn`, `xid`, `events`), `sink_send` (`sink`, `size`, `delivered`, `retryable`, `terminal`), `store` (`operation`, `count`), `park` and `drain` (`events`) and `status_update` (`lsn`) at debug; `takeover` (`holder`, `kind`, `epoch`) and `role_change` (`from`, `to`) at info. Only pg-cdc's own spans at debug and above are exported, sampled as above. Logs at info and above are exported too, and while telemetry is enabled a JSON log line written inside a sampled span gains `"trace_id"` (32 hex digits) and `"span_id"` (16 hex digits). The [runbook](docs/runbook.md#6-alerts) maps the suggested alerts to these metrics.

## Container image

`ghcr.io/rajp33/pg-cdc` is built for linux/amd64 and linux/arm64 on distroless (`gcr.io/distroless/cc-debian13:nonroot`) and runs as the `nonroot` user. Tags: `main` for the main branch, `sha-<short commit>`, and for release tags `<version>`, `<major>.<minor>` and `latest`.

```sh
docker run --rm -v "$PWD/pg-cdc.yaml:/etc/pg-cdc/config.yaml:ro" \
  -e PGCDC__SOURCE__CREDENTIALS__PASSWORD -p 127.0.0.1:9187:9187 \
  -e PGCDC__ADMIN__LISTEN=0.0.0.0:9187 ghcr.io/rajp33/pg-cdc:main
```

The entrypoint is `/usr/local/bin/pg-cdc` and the default command is `run --config /etc/pg-cdc/config.yaml`. The image's `HEALTHCHECK` runs `pg-cdc health`; ECS ignores image health checks, so set the same command in the task definition (see [ECS settings](#ecs-settings)). Binding the admin endpoint to `0.0.0.0` inside a container exposes it to the container's network; publish its port only to trusted hosts, since it has no authentication.

## v1 limits

- **Memory** is bounded by `delivery.max_inflight_bytes`, measured on encoded event bodies, plus one event above the bound and up to 65 raw replication messages read ahead of the bound, plus at most `delivery.lanes` x 10 parked events loaded for redelivery (one SNS batch per lane). The read-ahead is a fixed number of messages, independent of transaction size, so its size depends on the widest row. Measured: with `max_inflight_bytes: 64MiB` and the sink stalled, resident memory peaked at 121.8 MiB for a 50,000-row transaction and 122.3 MiB for a 500,000-row one (about 24 MiB idle); with the discard sink it stayed at 22 MiB ([docs/load.md](docs/load.md#memory-criterion-6)).
- **The parked count is approximate.** It is loaded from the store at startup and can over-count replayed events until the next restart, so the limit errs towards stalling early. During a large parking backlog it also trails the store.
- **A hot key's throughput.** One order key moves at most one event per sink round trip, roughly 20 to 50 events/s against SNS. `order_by` chooses the key.
- **Parked backlogs drain slowly.** After an outage that parked about 112,000 events on 1,000 keys, the parked count fell by about 20 events a second against moto, so draining would have taken about 90 minutes; while it drains, every parked key's new events wait behind its queue. The cause is under investigation ([docs/load.md](docs/load.md#drain-after-an-outage-a-measured-v1-limit)).
- **Parking during an S3 brownout.** Each parked event is its own S3 write, with at most `state_store.max_concurrency` (default 32) store calls in flight. With SNS down and S3 slowed by 500 ms, parking stored 58 events/s; at 2 s, 15 events/s, against 1,000 written. The reader then stalls at `max_inflight_bytes` and Postgres holds the WAL; nothing is lost. A segmented S3 layout is planned for v1.1.
- **Composite types** are sent as their Postgres text form, which differs from `to_jsonb`.
- **No backfills.** Changes made while the slot was lost are not captured; backfills arrive in v1.1.
- **The admin API has no authentication.** Keep it on loopback or a private network.
- **A discarded event can come back.** If pg-cdc crashes before the discarded event's transaction is confirmed, the replay sends it again once its key's queue has drained.
- **A paused process** can land duplicate writes (see [Known limit](#known-limit)).
- **Aurora slot survival is unverified.** Whether a logical slot survives an Aurora writer failover must be tested in staging ([runbook](docs/runbook.md#4-aurora-failover-and-slot-loss)).

`delivery.order_by` is `key` (default), `table`, `column:<name>` or `pipeline`. `sink: { type: stdout }` writes each change as a JSON line on stdout and logs to stderr; `sink: { type: discard }` drops every change, for load tests. With the SNS and discard sinks, logs are JSON on stdout. Logs are written from a bounded buffer of 8,192 lines on their own thread, so a stalled log reader never holds up replication or shutdown; lines that do not fit are dropped. Any key can be overridden with `PGCDC__<SECTION>__<KEY>` (lists as comma-separated values), and passwords come only from the environment. For Aurora with RDS IAM auth, use `credentials: { type: aws_rds_iam }` with the default AWS credential chain. pg-cdc runs on Linux and macOS.

## Develop

```sh
mise trust && mise install
mise run check
```

`mise run check` is the only gate. Integration tests need Docker; `PGCDC_TEST_PG_VERSION` selects Postgres 14 to 18 (default 17). See [docs/engineering.md](docs/engineering.md).

Outside the gate:

- `mise run chaos-smoke` runs the chaos harness for 15 s (seed 1) against two debug instances, Postgres and moto, and checks that nothing was lost and per-key order held; CI runs it as its own job.
- `mise run chaos` runs it for 10 minutes with `CHAOS_SEED` (default: the current Unix time) on a release build. Allow up to an hour for the parked backlog to drain.
- `mise run load` builds the release and `--no-default-features` binaries and runs the load scenarios into `target/load`; `LOAD_SCENARIO=ceiling|sns|overhead|memory|outage|brownout|handover` runs one. `cargo run -p pg-cdc-loadgen -- report --report-dir target/load --out summary.md` writes the summary table. Results are in [docs/load.md](docs/load.md).
- `cargo run -p pg-cdc-loadgen -- write --database <db> --user <user> --slot <slot> --rate 1000 --duration 60s` writes the chaos workload (upserts into `loadgen_items` from one connection) against any Postgres, with the password from `PGPASSWORD`; given `--slot`, it first creates the table with FULL identity, the `loadgen` publication and each slot.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option. Contributions are dual licensed the same way.
