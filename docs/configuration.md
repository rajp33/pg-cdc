# Configuration reference

pg-cdc reads one YAML file, passed as `pg-cdc run --config <file>`. The [runbook](runbook.md) covers setup and operations; this page lists every key.

## Loading rules

- **Environment overrides.** Any key can be set or overridden with `PGCDC__<SECTION>__<KEY>`, for example `PGCDC__DELIVERY__PARK_AFTER=60s` or `PGCDC__SINK__ATTRIBUTE_COLUMNS=tenant_id,region` (lists as comma-separated values). A `PGCDC__` variable whose path does not name a configuration key is an unknown key: it is never merged into the configuration, so it cannot create a section, change how a known key is read, or appear in an error message. A variable with an empty segment, such as `PGCDC__SOURCE____PORT`, is an error.
- **Secrets come from the environment only.** With `credentials.type: password`, the password is read from `PGCDC__SOURCE__CREDENTIALS__PASSWORD`; a password in the file is rejected.
- **Unknown keys are ignored with a warning.** A key this version does not know, in the file or from a `PGCDC__` variable, does not stop startup. pg-cdc logs one warning, `ignoring unknown configuration keys`, whose `keys` field names each one by its dotted path up to the first name this version does not know (`delivery.order_by_tables`, `future_section`), never with its value, and carries on. A key that belongs to a different `type` of the same section, such as `topic_arn` under a `stdout` sink, is reported the same way. That lets a key added by a newer version, or left behind after a rollback, through without a crash loop. The trade-off is that a misspelled key or section name is ignored too and its setting quietly keeps its default, so check the startup log for that warning after every configuration change. Startup still stops on a known key with an invalid value, on a missing required key, and on a `type` that is not a plain string naming one of the documented types.
- **Units.** Durations are whole numbers with `ms`, `s`, `m` or `h` (`30s`); sizes are whole numbers with `B`, `KiB`, `MiB` or `GiB` (`256MiB`).

## Full example

```yaml
pipeline: orders

source:
  host: app.cluster-xyz.us-east-1.rds.amazonaws.com
  port: 5432
  database: app
  user: pgcdc
  credentials:
    type: aws_rds_iam
  tls:
    mode: verify-full
    ca_file: /etc/pg-cdc/rds-global-bundle.pem
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
  max_concurrency: 32
  attribute_columns: [tenant_id]

state_store:
  type: s3
  bucket: pgcdc-state
  prefix: orders/
  max_concurrency: 256

payload_store:
  type: s3
  bucket: pgcdc-payloads
  prefix: orders/

leadership:
  lease_timeout: 6s
  fence_delay: 16s
  drain_timeout: 5s

admin:
  listen: 127.0.0.1:9187

telemetry:
  otlp_endpoint: http://otel-collector:4317
  trace_sample_ratio: 0.01
  sentry:
    dsn: https://<public key>@<org>.ingest.sentry.io/<project>
    environment: production
    sample_rate: 1.0
```

## Keys

| Key | Default | Notes |
|---|---|---|
| `pipeline` | required | 1 to 63 letters, digits, `_` or `-`. Names the pipeline in the state store, in event `source.pipeline` and in FIFO deduplication ids. |
| `source.host`, `source.database`, `source.user` | required | |
| `source.port` | `5432` | |
| `source.credentials.type` | required | `password` or `aws_rds_iam` (see [Credentials](#credentials)). |
| `source.credentials.region` | the default AWS region | `aws_rds_iam` only. |
| `source.tls.mode` | `verify-full` | `disable`, `require` (encrypt, no certificate check), `verify-ca` (check the chain, not the host name) or `verify-full`. |
| `source.tls.ca_file` | the webpki (Mozilla) roots | PEM file of trusted CAs, such as the RDS global bundle. |
| `source.slot`, `source.publication` | required | 1 to 63 lowercase letters, digits or underscores. pg-cdc never creates or drops either. |
| `source.status_interval` | `10s` | How often the confirmed position is reported to Postgres. |
| `delivery.order_by` | `key` | `key`, `table`, `column:<name>` or `pipeline` (see [Order keys](#order-keys)). |
| `delivery.lanes` | `64` | 1 to 4,096. An event goes to lane `order key id mod lanes`. |
| `delivery.max_inflight_bytes` | `256MiB` | The reader stops when in-flight events (their encoded bodies) reach this size. |
| `delivery.park_after` | `30s` | How long an event may stay undelivered after its first failure before it is parked. |
| `delivery.max_parked_events` | `10000000` | `0` is unlimited. Also bounds parked-key memory (see [runbook](runbook.md#5-parked-and-dead-events)). |
| `sink.type` | required | `sns`, `stdout` or `discard`. |
| `sink.max_concurrency` | `32` | 1 to 4,096 concurrent sink calls, for any sink type. |
| `sink.topic_arn` | required for `sns` | |
| `sink.region` | the default AWS region | `sns` only. |
| `sink.attribute_columns` | `[]` | `sns` only. Up to 7 distinct column names sent as message attributes; `schema`, `table` and `op` are reserved. |
| `sink.fair_queue_group` | unset | `sns` only, as `column:<name>`; the column must also be in `attribute_columns`. |
| `state_store.type` | required | `s3` or `filesystem`. |
| `state_store.bucket`, `state_store.prefix`, `state_store.region` | bucket required, prefix empty | `s3` only. |
| `state_store.path` | required for `filesystem` | |
| `state_store.max_concurrency` | `256` | 1 to 4,096 store calls in flight, shared by parking, draining and head loading. |
| `payload_store` | unset | Optional; `type: s3` with `bucket`, `prefix` and `region` (see [Payload store](#payload-store)). |
| `leadership.lease_timeout` | `6s` | Must exceed the 2 s lease check interval. |
| `leadership.fence_delay` | `16s` | Must exceed `lease_timeout`; with an S3 state store, at least `lease_timeout` plus the S3 client's 10 s attempt timeout. |
| `leadership.drain_timeout` | `5s` | Greater than zero. |
| `admin.listen` | `127.0.0.1:9187` | An IP address and port; host names are rejected. |
| `telemetry.otlp_endpoint` | unset (telemetry off) | An `http://` or `https://` URL. |
| `telemetry.trace_sample_ratio` | `0.01` | 0 to 1. |
| `telemetry.sentry.dsn` | unset (error reporting off) | An `http://` or `https://` Sentry DSN; `SENTRY_DSN` is used when unset. Kept secret. |
| `telemetry.sentry.environment` | unset (Sentry's default) | Tags every event. |
| `telemetry.sentry.sample_rate` | `1.0` | 0 to 1; the share of error events sent. |

Fixed internal values: standby poll and lease check every 2 s; batch linger 5 ms; sink retry backoff 100 ms to 30 s; reconnect backoff 1 s to 30 s.

## Credentials

- **`password`** reads `PGCDC__SOURCE__CREDENTIALS__PASSWORD`.
- **`aws_rds_iam`** generates an RDS IAM token with the default AWS credential chain for every connection. The role needs `rds_iam` and `rds_replication`, and the cluster needs `rds.iam_auth_for_replication = 1` ([runbook](runbook.md#1-setup)).

## Order keys

The order key decides ordering and lane placement. Events of one order key are delivered one at a time, in stream order.

| `delivery.order_by` | Order key |
|---|---|
| `key` | schema, table and key values (primary key or replica identity index); schema and table when the relation has no key |
| `table` | schema and table |
| `column:<name>` | the value of that column; schema and table when it is absent or null |
| `pipeline` | one key for everything: total order, one event in flight at a time |

A truncate has no key: its order key is its table under `key`, `table` and the `column:<name>` fallback, and the one constant key under `pipeline`. Under `key` and `column:<name>` it is therefore not ordered relative to that table's row events that use a different order key. One order key moves at most one event per sink round trip, roughly 20 to 50 events/s against SNS, so choose `order_by` so that no single key carries a high rate.

## Sinks

### SNS

Each change is one SNS message whose body is the JSON envelope (see the [design](superpowers/specs/2026-10-14-pg-cdc-design.md#71-envelope-version-1)). The sink uses `PublishBatch` with up to 10 entries and the default AWS credential chain.

- **Message attributes.** Every message carries `schema`, `table` and `op`, plus each `attribute_columns` value, taken from `after`, or from `before` when there is no `after`. A value is a `Number` attribute when it is a plain JSON number from -10^9 to 10^9 with at most 5 digits after the decimal point, the range SNS accepts, and a `String` holding the exact original text otherwise, including larger or more precise numbers and exponent forms such as `1e+300`. Null and empty values are omitted, as is a string with a character SNS rejects (with a warning).
- **Filtering on changed columns.** Updates on a table with `REPLICA IDENTITY FULL` carry `changed`, the columns the update changed, in the body (see the [envelope](superpowers/specs/2026-10-14-pg-cdc-design.md#71-envelope-version-1)). A subscription with `FilterPolicyScope` set to `MessageBody` and the filter policy `{"changed": ["revision"]}` receives only updates that changed `revision`; inserts, deletes, truncates and updates without a full old row have no `changed` and never match. Values are compared as Postgres text, so an equal value written differently also counts as changed: numeric scale (`1.50` to `1.5`), `json` key order or spacing, and float `-0` to `0`. Only columns Postgres replicates can appear, so a filter on a generated column (not replicated by Postgres 17 and earlier, and on Postgres 18 only with `publish_generated_columns = stored` or a column list naming it) or on a column outside the publication's column list never matches. `changed` is not a message attribute.
- **FIFO topics.** The message group is the order key id and the deduplication id is `<pipeline>:<event id>`, the same on every replay and takeover. SNS deduplicates across the whole topic, so pipelines sharing a FIFO topic need distinct `pipeline` names.
- **Standard topics.** No message group, unless `fair_queue_group: column:<name>` sets one from that column for SQS fair queues. It is ignored, with a warning, on a FIFO topic.
- **Message size.** The limit comes from the topic's `MaximumMessageSize` attribute when `GetTopicAttributes` returns it, and is 256 KiB otherwise; the `SNS topic ready` log at startup shows the limit and where it came from. Topics above 256 KiB have not been verified against real AWS. Larger events are stored in the payload store and sent as a pointer, provided the pointer fits, or park as dead without one (see [Payload store](#payload-store)).
- **Preflight.** pg-cdc checks the topic with `GetTopicAttributes` and keeps retrying until it succeeds before it opens the replication connection.
- **Retries.** A failing order key backs off from 100 ms to 30 s while other keys keep flowing, as long as the state store answers promptly. An entry SNS rejects with `SenderFault` is terminal and parks at once as dead.

### stdout and discard

`sink: { type: stdout }` writes each change as a JSON line on stdout and logs to stderr. `sink: { type: discard }` drops every change, for load tests.

## State store

The state store holds parked events and the leader record (`leader.json`). Layout and IAM are in the [runbook](runbook.md#1-setup).

- **`s3`** uses conditional writes and suits active/standby across hosts. Use default encryption on the bucket, because parked objects hold row data.
- **`filesystem`** keeps the same tree on local disk under `path` and suits a single host. pg-cdc fsyncs `path` and every directory above it, so the service user must be able to read each of them; one it cannot read leaves the store unavailable. It has no ordered index, so admin listings scan directory entries (see [runbook](runbook.md#9-admin-api-reference)).

pg-cdc checks that the state store answers before it opens the replication connection.

## Payload store

With a payload store, an event larger than the sink's message limit is stored at `s3://<bucket>/<prefix>/<pipeline>/payloads/<event id>.json`, and the sink receives a pointer instead: the same envelope without `before`, `after` and `unchanged_toast` (so it keeps `changed`), plus `"payload_ref": {"uri": "s3://...", "bytes": 912345}`. The pointer, with its key and message attributes, must still fit the sink's limit; a pointer that does not, or an oversize event without a payload store, parks as dead. Give the payload prefix a lifecycle rule that expires objects after your consumers' retention.

## Leadership

- **`lease_timeout`.** Every 2 s the active task checks over its control connection that the slot is still held by its own walsender. If no check succeeds for `lease_timeout`, the task fences itself: it stops writing, drops in-flight work, closes the replication connection and returns to standby.
- **`fence_delay`.** What a successor waits after a crash or a fence before it writes anything. By then the old holder has stopped issuing writes and given up on any attempt it sent. It cannot recall a request S3 already received, which S3 may still apply later: a late status rewrite is conditional on the version it read, so it is rejected if the event was removed or changed meanwhile and otherwise applies, while a late append can yield a duplicate and, rarely, one key's events out of order.
- **`drain_timeout`.** On SIGTERM, the window for in-flight events to deliver, and again for parking what remains. The whole shutdown takes at most `2 x drain_timeout + 3 s`.

The handover sequence and ECS settings are in the [runbook](runbook.md#8-shutdown-health-and-startup-validation).

## Admin endpoint

`admin.listen` serves health, status and the parked-event API. Port 0 picks a free port, logged as `admin listening`. Two tasks that share a network namespace need distinct ports, and a task that cannot bind the address exits 1. The endpoint has no authentication, so keep it on loopback or a private network. `pg-cdc health` and `pg-cdc parked` take `--addr`, else `PGCDC__ADMIN__LISTEN`, else `127.0.0.1:9187`.

## Telemetry

Telemetry is off unless an OTLP endpoint is configured; then traces, metrics and logs go to it over OTLP/gRPC. The metrics, spans and suggested alerts are in the [runbook](runbook.md#10-telemetry-reference).

- **Enabling.** `telemetry.otlp_endpoint` or `OTEL_EXPORTER_OTLP_ENDPOINT` turns it on; the config value wins when both are set. `https://` endpoints use rustls with the webpki roots.
- **gRPC only.** If `OTEL_EXPORTER_OTLP_PROTOCOL` is set to anything other than `grpc`, pg-cdc logs a warning and uses gRPC.
- **`OTEL_*` variables.** `OTEL_SERVICE_NAME` (default `pg-cdc`), `OTEL_RESOURCE_ATTRIBUTES` and `OTEL_METRIC_EXPORT_INTERVAL` are honored. Every signal carries `service.instance.id` (the task's holder id), `service.version` and `pgcdc.pipeline`.
- **Sampling.** Traces use `ParentBased(TraceIdRatioBased(trace_sample_ratio))`.
- **Never in the way.** Exports go through bounded batch queues that drop when full, and on SIGTERM the exporters get at most 1 s within the shutdown budget.
- **Error reporting.** `telemetry.sentry.dsn`, or `SENTRY_DSN` when the config sets none, sends ERROR lines and panics to Sentry, independently of OTLP. Only an allowlist leaves the process; the [runbook](runbook.md#error-reporting-sentry) lists exactly what. The `sentry` cargo feature (default on) compiles it in; without it the DSN is ignored with a warning.
- **The `otel` feature.** OpenTelemetry is compiled in by default. `cargo build --release -p pg-cdc --no-default-features` builds without it; such a build logs `telemetry is configured, but this build has no otel feature` and exports nothing.

## Logs

Logs are JSON lines on stdout with the SNS and discard sinks, and on stderr with the stdout sink. `RUST_LOG` sets the filter (default `info`). Logs are written from a bounded buffer of 8,192 lines on their own thread, so a stalled log reader never holds up replication or shutdown; lines that do not fit are dropped.

## Platforms

pg-cdc runs on Linux and macOS. The container image is described in the [runbook](runbook.md#11-container-image).
