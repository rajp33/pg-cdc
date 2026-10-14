# pg-cdc

pg-cdc streams row changes for the tables in one Postgres publication through a logical replication slot to a sink. Delivery is at least once, and pg-cdc never writes to the source database.

Status: milestone 2 of the [design](docs/superpowers/specs/2026-10-14-pg-cdc-design.md). Changes are delivered to SNS, stdout or a discard sink through ordered lanes with retries, and the slot is acknowledged once every event of a transaction, and of every transaction before it, is delivered. Parking, leadership and the admin API arrive in later milestones.

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
sink:
  type: sns
  topic_arn: arn:aws:sns:us-east-1:123456789012:orders.fifo
  attribute_columns: [tenant_id]
```

```sh
PGCDC__SOURCE__CREDENTIALS__PASSWORD=... pg-cdc run --config pg-cdc.yaml
```

Each change is one SNS message whose body is the JSON envelope, with `schema`, `table`, `op` and each `attribute_columns` value as message attributes. A column value is a `Number` attribute when it is a plain JSON number of at most 38 digits, and a `String` otherwise, including numbers in exponent form such as `1e+300`; null and empty values are omitted. On a FIFO topic the message group is the order key id and the deduplication id is the event id. The SNS sink uses the default AWS credential chain; `sink.region` overrides the region. `sink.max_concurrency` (default 32, any sink type) bounds concurrent sink calls. Retries are owned by pg-cdc: a failing order key backs off from 100 ms to 30 s while other keys keep flowing.

The SNS message limit comes from the topic's `MaximumMessageSize` attribute when `GetTopicAttributes` returns it, and is 256 KiB otherwise; the `SNS topic ready` log at startup shows the limit in use and where it came from. Topics above 256 KiB have not been verified against real AWS. pg-cdc keeps retrying the sink preflight and does not open the replication connection until it succeeds.

## Limits in this milestone

- **A rejected event blocks its order key.** When the sink rejects an event for good (SNS `SenderFault`), or an event is larger than the sink's message limit, pg-cdc logs it at error once and never sends it again. Later events of the same order key wait behind it, other keys keep flowing, and the slot stops advancing at that event, so WAL accumulates until pg-cdc is restarted, which replays it. Nothing is lost or skipped. Milestone 3 parks such events in a state store instead.
- **Memory** is bounded by `delivery.max_inflight_bytes`, measured on encoded event bodies, plus one event above the bound and up to 65 raw replication messages read ahead of the bound. The read-ahead is a fixed number of messages, independent of transaction size, so its size depends on the widest row.

`delivery.order_by` is `key` (default), `table`, `column:<name>` or `pipeline`. `sink: { type: stdout }` writes each change as a JSON line on stdout and logs to stderr; `sink: { type: discard }` drops every change, for load tests. With the SNS and discard sinks, logs are JSON on stdout. Any key can be overridden with `PGCDC__<SECTION>__<KEY>` (lists as comma-separated values), and passwords come only from the environment. For Aurora with RDS IAM auth, use `credentials: { type: aws_rds_iam }` with the default AWS credential chain. pg-cdc runs on Linux and macOS.

## Develop

```sh
mise trust && mise install
mise run check
```

`mise run check` is the only gate. Integration tests need Docker; `PGCDC_TEST_PG_VERSION` selects Postgres 14 to 18 (default 17). See [docs/engineering.md](docs/engineering.md).

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option. Contributions are dual licensed the same way.
