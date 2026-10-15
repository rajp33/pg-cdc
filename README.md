# pg-cdc

pg-cdc streams row changes for the tables in one Postgres publication through a logical replication slot to a sink. Delivery is at least once, and pg-cdc never writes to the source database.

Status: milestone 3 of the [design](docs/superpowers/specs/2026-10-14-pg-cdc-design.md). Changes are delivered to SNS, stdout or a discard sink through ordered lanes with retries. An event the sink cannot take within `delivery.park_after` is parked in a state store (S3 or a local directory), so the slot keeps advancing during a sink outage, and parked events are redelivered in order once the sink recovers. Leadership and the admin API arrive in later milestones.

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
- **Shutdown.** On SIGTERM, pg-cdc stops reading, gives in-flight events 5 s to deliver, parks whatever remains (another 5 s at most), sends a final status update and exits 0. Events that could neither deliver nor park replay on the next start.
- **Limits.** At `delivery.max_parked_events` (default 10,000,000; `0` is unlimited), or while the state store is unavailable, events stay in flight and the reader stops at `delivery.max_inflight_bytes`. pg-cdc checks that the state store answers before it opens the replication connection.
- **State stores.** `state_store: { type: s3, bucket, prefix, region }` uses conditional writes and suits active/standby across hosts; the bucket should use default encryption, because parked objects hold row data. `state_store: { type: filesystem, path: /var/lib/pg-cdc }` keeps the same tree on local disk and suits a single host.

## Limits in this milestone

- **Memory** is bounded by `delivery.max_inflight_bytes`, measured on encoded event bodies, plus one event above the bound and up to 65 raw replication messages read ahead of the bound, plus at most `delivery.lanes` x 10 parked events loaded for redelivery (one SNS batch per lane). The read-ahead is a fixed number of messages, independent of transaction size, so its size depends on the widest row.
- **The parked count is approximate.** It is loaded from the store at startup and can over-count replayed events until the next restart, so the limit errs towards stalling early.

`delivery.order_by` is `key` (default), `table`, `column:<name>` or `pipeline`. `sink: { type: stdout }` writes each change as a JSON line on stdout and logs to stderr; `sink: { type: discard }` drops every change, for load tests. With the SNS and discard sinks, logs are JSON on stdout. Any key can be overridden with `PGCDC__<SECTION>__<KEY>` (lists as comma-separated values), and passwords come only from the environment. For Aurora with RDS IAM auth, use `credentials: { type: aws_rds_iam }` with the default AWS credential chain. pg-cdc runs on Linux and macOS.

## Develop

```sh
mise trust && mise install
mise run check
```

`mise run check` is the only gate. Integration tests need Docker; `PGCDC_TEST_PG_VERSION` selects Postgres 14 to 18 (default 17). See [docs/engineering.md](docs/engineering.md).

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option. Contributions are dual licensed the same way.
