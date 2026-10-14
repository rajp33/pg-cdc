# pg-cdc design

| | |
|---|---|
| Date | 2026-10-14 |
| Status | Draft for review |
| Repository | github.com/rajp33/pg-cdc (public) |
| License | MIT OR Apache-2.0 |

## 1. Summary

pg-cdc is a single Rust binary that streams row changes for the tables in one Postgres publication through a logical replication slot to a sink (Amazon SNS in v1). Delivery is at-least-once and ordered per order key, and pg-cdc never writes to the source database.

An event that cannot be delivered within `park_after` is parked in a durable state store (Amazon S3 in v1). The slot keeps advancing, so a slow or failing sink never makes the source accumulate WAL.

The sink, the state store, the payload store and the Postgres connection are adapters behind small traits. Each new backend is one implementation.

## 2. Goals and non-goals

### 2.1 Goals

1. Deliver every committed change in the publication at least once, in order per order key.
2. Protect the source: never write to it, and keep its WAL bounded while a sink is down.
3. Ship one binary whose runtime dependencies are Postgres, the sink and the state store.
4. Deploy without downtime: a new task waits as a standby and takes over the slot on handover.
5. Make the sink, state store, payload store and connection adapters.
6. Instrument traces, metrics and logs with OpenTelemetry, with no overhead when it is not configured.
7. Serve one production workload in v1, developed in the open from the start.

### 2.2 Non-goals for v1

- Snapshots and backfills. The `read` op is reserved for them.
- Exactly-once delivery.
- Transforms, or filtering beyond what the publication expresses (tables, row filters, column lists).
- Creating or dropping slots and publications. Operators do this with SQL or infrastructure code (§13.1).
- More than one pipeline per process.
- Any UI.

## 3. Context

### 3.1 Lessons from the references

**An earlier in-house pipeline** (Go; WAL → an audit table → SNS):

- **The slot is the durable buffer.** The intermediate table added replay history, stable ids and actor enrichment, but no durability. pg-cdc derives stable ids from the WAL instead.
- **Status updates must never wait on the sink.** Long sink calls tripped `wal_sender_timeout`, and the service crash-looped.
- **Acknowledge the keepalive position when idle.** Otherwise a quiet publication pins WAL.
- **Never recreate a missing slot automatically.** Doing so silently skips every change in between.
- **One poison transaction must not stop everything.** In the earlier pipeline it stopped everything, and skipping its LSNs by hand lost events for every consumer. pg-cdc parks the failing event and keeps going; an operator redrives or discards it.
- **Don't buffer whole transactions.** Doing so in memory caused out-of-memory crashes; pg-cdc streams events as they arrive.
- **Don't negotiate streaming.** The earlier pipeline mishandled streamed transactions that contained subtransactions.
- **Report every change.** The earlier pipeline silently dropped unchanged TOAST values and TRUNCATE; pg-cdc reports both.

**Sequin** (Elixir):

- **Adopted:**
  - `(commit_lsn, commit_idx)` as identity and idempotency key;
  - one message per group in flight, with a failure blocking only its group;
  - acknowledging the oldest unsettled position on a timer;
  - persisting failing messages so the slot can advance.
- **Avoided:**
  - Redis as a second source of truth whose cursor can drift from the slot;
  - silently dropping large messages;
  - spilled messages that also stay in memory;
  - one stuck sink stalling every sink on a slot.

**florinutz/pgcdc** (Go):

- **Adopted:** a small sink interface, with retries owned by the pipeline.
- **Avoided:**
  - acknowledging events when received rather than when delivered;
  - code paths that skip an event and still acknowledge it;
  - random event ids;
  - string-only values;
  - status updates sent only from the read loop;
  - an external checkpoint table treated as the source of truth.

**Rust ecosystem:**

- **supabase/etl is not a good base.** It is git-only, its destination trait is shaped for warehouses, and its fixed password breaks IAM tokens.
  - Its feedback rules are adopted: the checkpoint is a commit's end LSN, recorded only once the commit's events are durable, and the keepalive's `wal_end` is acknowledged when idle.
- **pg-cdc writes its own replication transport and pgoutput decoder.** The transport is a small client built on `postgres-protocol`, the message codec and SCRAM implementation behind tokio-postgres. pgwire-replication informed its design and is not vendored.
- **Status updates keep flowing under backpressure.** pgwire-replication and SpiceAI learned this the hard way: stopping feedback while a channel is full lets `wal_sender_timeout` drop the connection.
- **A keepalive's `wal_end` is safe to acknowledge when nothing is pending,** as in Materialize.

### 3.2 First deployment

pg-cdc v1 is the change reader for Theia's [THE-526](https://linear.app/theia-vision/issue/THE-526). It replaces core's `pg_notify` path: every change to the published tables (`captures` and `pairings` to start) goes to an SNS topic, and an SQS queue feeds that topic to the backend.

- **Database:**
  - Postgres on Railway today, with password auth and TLS.
  - Aurora PostgreSQL with RDS IAM auth after the move to AWS (THE-438). Both are ConnectionProvider implementations (§11.7).
- **Compute:** pg-cdc runs as its own Railway service. Deploys start the new instance first and gate on a health check, so the new instance waits as a standby and then takes over the slot (§12).
- **AWS resources:** a standard SNS topic and an S3 bucket for state.
- **Failed posts are parked, not dropped (§10).** THE-526 asks that a failed post never grow the slot and that a restart resume without loss. Parking meets both, and also loses nothing when SNS fails.
- **Telemetry:** OTLP when enabled. THE-526's alert for a reader that falls behind is built from the metrics in §17.3 as part of Theia's deployment.

## 4. Success criteria

1. **No committed change is lost.** Each is delivered at least once or held in the state store, as proven by the chaos tests (§18.3).
2. **Per-order-key order holds** at the sink.
3. **WAL stays bounded during a sink outage.** The slot keeps advancing once events park, so retained WAL is roughly `park_after` worth of write volume.
4. **Throughput:** at least 5,000 events/s sustained, with p99 commit-to-acknowledgement under 1 s against a healthy SNS topic.
5. **Handover:** a deploy handover completes in under 10 s with nothing lost.
6. **Memory** stays within `max_inflight_bytes` plus fixed overhead, whatever the transaction size.
7. **Telemetry overhead:** with telemetry not configured, throughput is within 1% of a build without the `otel` feature.

## 5. Architecture

```
Postgres writer
  │ replication connection (pgoutput, proto_version 1)        control connection
  ▼                                                            ▲  catalog, lease, slot metrics
reader ──► decoder ──► builder ──► router ──► lanes[0..N) ──► Sink (SNS)
  ▲                                              │   ▲
  │ status updates                               │   └──► StateStore (S3): parked events, leader record
ack tracker ◄──────────── settled ◄──────────────┘        PayloadStore (S3): oversize payloads
admin HTTP: /healthz /readyz /status /parked
```

| Component | Responsibility |
|---|---|
| leader | Waits as standby until the slot is free, takes it over, renews a lease, and hands over on SIGTERM (§12) |
| reader | Opens the replication connection, issues `START_REPLICATION`, and reads raw WAL messages |
| decoder | Turns pgoutput messages and Postgres text values into typed rows (crate `pg-cdc-decoder`, no I/O) |
| builder | Builds the envelope, order key and event id, and fills unchanged TOAST columns (§7) |
| router | Maps an order key to a lane |
| lanes | Each owns a subset of order keys: batching, retries, parking, and draining parked queues (§8, §10) |
| ack tracker | Computes the confirmed LSN from settled transactions and sends status updates on its own timer (§9) |
| Sink, StateStore, PayloadStore, ConnectionProvider | Adapters (§11) |
| admin | Health, status and parked-event operations over HTTP (§16) |

**Workspace**

- `crates/decoder` (package `pg-cdc-decoder`): a pure, fuzzed decoder for pgoutput messages and Postgres text values. The name avoids the unrelated `pgoutput` crate on crates.io.
- `crates/replication` (package `pg-cdc-replication`): the replication transport.
  - It covers startup, TLS, authentication (cleartext, MD5 and SCRAM-SHA-256), `START_REPLICATION`, and standby status updates on its own timer.
  - It exposes the walsender's backend PID from BackendKeyData.
- `crates/pg-cdc`: the binary and library.
- `crates/loadgen`: a load generator, not shipped.

**Runtime footprint**

- Postgres 14 or newer.
- An active task holds two connections (replication and control); a standby holds one (control).
- The sink and state store are reached over HTTPS.
- AWS clients use the default AWS configuration chain (environment, task role); each adapter takes an optional `region` override.

## 6. Replication and decoding

### 6.1 Connections

- **Replication connection.** It is opened with `replication=database`, then issues `START_REPLICATION SLOT <slot> LOGICAL 0/0 (proto_version '1', publication_names '<publication>')`. Starting at `0/0` makes the server resume from the slot's `confirmed_flush_lsn`.
- **Pinned session settings.** The startup `options` set:
  - `DateStyle=ISO, MDY`;
  - `TimeZone=UTC`;
  - `IntervalStyle=postgres`;
  - `extra_float_digits=1`;
  - `bytea_output=hex`.

  This makes value text deterministic and the same as a default `to_jsonb` session.
- **Control connection.** A regular `tokio-postgres` connection over rustls carries catalog lookups, lease checks, slot metrics and standby polling. Metadata never goes over the replication connection, because some Aurora versions reject `SHOW` there.
- **Parameters.** Both connections take their parameters from the ConnectionProvider before every attempt (§11.7).
- **Direct connections only.** Both connect directly to the database endpoint: RDS Proxy rejects replication connections, and poolers cannot mint IAM tokens.

### 6.2 Protocol scope

- **Only protocol version 1, with none of the optional features.** `binary`, `streaming`, `two_phase`, `messages` and `origin` are not negotiated, so the message set is closed: Begin, Commit, Origin, Relation, Type, Insert, Update, Delete, Truncate.
- **Origin and Type messages are accepted and ignored.** Types are resolved through the catalog instead (§6.3).
- **Whole committed transactions.** Without streaming, the server sends a transaction only after it commits, whole and in commit order. It spills large transactions to its own disk (`logical_decoding_work_mem`).
- **Events are complete on arrival.** Begin carries the commit LSN, commit timestamp and xid, so pg-cdc never buffers a whole transaction.
- **Commit gives the acknowledgement position.** It carries the transaction's end LSN, which is the position pg-cdc acknowledges.

### 6.3 Relation and type metadata

- **Relation cache.** Relation messages are cached by relation OID for the life of a connection and cleared on reconnect.
- **Primary key.** It comes from the catalog over the control connection: `pg_index.indisprimary`, in index column order.
  - Without one, the replica-identity index (`indisreplident`) is used; otherwise the relation has no key.
  - Keys are cached per relation OID and refreshed when a new Relation message for that OID arrives.
- **Non-built-in types.** Enums, domains, arrays, composites, ranges and custom types are resolved from `pg_type` over the control connection and cached for the life of the process.

### 6.4 Value mapping

JSON output matches `to_jsonb` for the same value. Integration tests use Postgres itself as the reference (§18.2).

| Postgres type | JSON |
|---|---|
| bool | `true` / `false` |
| int2, int4, int8, oid | number |
| float4, float8 | number; `NaN`, `Infinity` and `-Infinity` as strings |
| numeric | number with exact digits; `NaN` and infinities as strings |
| text, varchar, bpchar, name, char | string |
| json, jsonb | embedded JSON value |
| date, timestamp, timestamptz | string in `to_jsonb` ISO 8601 form (`2026-10-14T12:00:00.123456+00:00` for timestamptz); `infinity` and `-infinity` as strings |
| bytea | string `\x…` (hex) |
| arrays | JSON arrays nested by dimension, elements mapped recursively, `NULL` as `null` |
| enums | string |
| domains | mapped as their base type |
| composites | v1: the Postgres text form as a string, which differs from `to_jsonb` (roadmap §20) |
| all other types (uuid, inet, interval, money, ranges, geometric and custom types) | the Postgres text form as a string |

### 6.5 Tuples

- **`n` (null)** becomes `null`, and **`t` (text)** is mapped as in §6.4.
- **`u` (unchanged TOAST).**
  - If the update carries an old tuple (`REPLICA IDENTITY FULL`), the value is filled from it.
  - Otherwise the column is left out of `after` and listed in `unchanged_toast`.
- **`before` for an update:**
  - the full old row when the update carries an `O` tuple (FULL identity);
  - only the key columns when it carries a `K` tuple (DEFAULT or index identity, key changed);
  - otherwise `null`.
- **`before` for a delete:** the full old row with an `O` tuple, or only the key columns with a `K` tuple.
- **Startup warning.** pg-cdc logs a warning and sets a metric for every published table whose replica identity is not FULL.

## 7. Event model

### 7.1 Envelope (version 1)

```json
{
  "id": "000000001a2b3c4d-00000003",
  "version": 1,
  "op": "update",
  "schema": "public",
  "table": "orders",
  "key": { "id": 42 },
  "before": { "id": 42, "status": "pending" },
  "after": { "id": 42, "status": "paid" },
  "source": {
    "pipeline": "orders",
    "commit_lsn": "0/1A2B3C4D",
    "commit_ts": "2026-10-14T12:00:00.123456+00:00"
  }
}
```

| Field | Meaning |
|---|---|
| `id` | Event id (§7.2). Dedupe on it; compare ids to tell which version of a row is newer. |
| `version` | Envelope version, `1`. |
| `op` | `insert`, `update`, `delete` or `truncate`. `read` is reserved for backfills. |
| `schema`, `table` | Source relation. |
| `key` | Key columns and their values (§7.3), or `null`. |
| `before` | The old row as far as Postgres sent it (§6.5), or `null`. |
| `after` | The new row for `insert` and `update`; otherwise `null`. |
| `unchanged_toast` | Present only when non-empty: columns that exist but whose unchanged value Postgres did not send. Consumers keep the value they already have. |
| `payload_ref` | Present only on an oversize pointer event (§7.5). |
| `source.pipeline` | Pipeline name from config. |
| `source.commit_lsn` | Commit LSN in Postgres notation. Every event of one transaction shares it. |
| `source.commit_ts` | Commit time in UTC, in the same ISO 8601 form as timestamptz values (§6.4). |

The xid is left out of the envelope: it is 32 bits and wraps, and `commit_lsn` already identifies the transaction. It is recorded on telemetry spans and log lines.

### 7.2 Identity

- **`ordinal`** is the 0-based position of a row event within its transaction, in arrival order. Insert, Update and Delete each count as one; a Truncate counts one per truncated relation.
- **`id`** is the commit LSN as 16 lowercase hex digits, then `-`, then the ordinal as 8 lowercase hex digits.
- **Properties of `id`:**
  - it is unique within a pipeline;
  - it is identical on every replay of the same WAL;
  - lexicographic order equals stream order;
  - it is a valid SNS FIFO deduplication id.
- **Change publications only when caught up.** If the publication's table list changes while transactions are still unacknowledged, those transactions can get different ordinals on replay.

### 7.3 Key and order key

- **`key`** holds the primary-key columns, or the replica-identity index columns. Values come from `after`, or from `before` for deletes.
- **The order key** decides ordering and lane placement:

| `delivery.order_by` | Order key |
|---|---|
| `key` (default) | schema, table and key values; schema and table when the relation has no key |
| `table` | schema and table |
| `column:<name>` | the value of that column; schema and table when the column is absent or null |
| `pipeline` | one constant key: total order, with one event in flight at a time |

- **The canonical order key** is a compact JSON array:
  - `[schema, table, v1, v2, …]` for `key`, with values in key column order;
  - `[schema, table]` for `table`, and for any fallback to the table;
  - `[value]` for `column:<name>`;
  - `[]` for `pipeline`.
- **The order key id** is the 128-bit xxh3 hash of the canonical order key, written as 32 hex digits.
  - It names parked queues (§11.4), FIFO message groups (§11.2) and admin handles.
  - Two order keys that share a hash are only serialized together, so ordering stays correct.

### 7.4 Truncate

A truncate produces one event per truncated relation, with `key`, `before` and `after` all `null`. Its order key is the table, so under `order_by: key` it is not ordered relative to that table's row events.

### 7.5 Oversize events

When the encoded message (body plus attributes) exceeds the sink's `max_message_bytes`:

- **With a PayloadStore,** the full envelope is stored there, and the sink receives a pointer event instead. The pointer is the same envelope without `before`, `after` and `unchanged_toast`, plus `"payload_ref": { "uri": "s3://bucket/prefix/payloads/<id>.json", "bytes": 912345 }`.
- **Without a PayloadStore,** the event is terminal and is parked as `dead` (§10.4).

## 8. Ordering and lanes

- **Lane assignment.** There are `delivery.lanes` tasks (default 64), and an event goes to lane `order_key_id mod lanes`.
- **Lanes own their keys.** A lane holds the per-key queues, retry timers, parked state and batching for its order keys, so per-key state needs no locks.
- **One event per key in flight.** At most one event per order key is in flight, and a batch holds at most one event per order key.
- **Batching.** Batches are filled from ready keys up to the sink's message and byte limits, lingering up to 5 ms when not full.
- **Retries don't block the lane.** A key whose head is waiting on a retry is not ready, and the lane's other keys keep flowing.
- **Per-key throughput.** One key moves at most one event per sink round trip, roughly 20–50 events/s for SNS. `order_by: pipeline` therefore trades throughput for total order.

## 9. Delivery and acknowledgement

### 9.1 Settling

- **An event settles** when the sink reports it delivered, or when the StateStore confirms it is parked. Nothing else settles an event (invariant A1).
- **A transaction settles** when its Commit has arrived and all its events have settled. A transaction with no events settles at its Commit.

### 9.2 Confirmed LSN

- **The ack tracker** keeps transactions in commit order, each with a count of unsettled events.
- **The confirmed LSN** is the end LSN of the last transaction in the unbroken run of settled transactions at the front of that list.
- **Starting value.** When streaming starts, the confirmed LSN starts at the slot's `confirmed_flush_lsn`, so pg-cdc never reports a lower position.
- **When idle,** with no unsettled events and no open transaction, the confirmed LSN advances to the latest keepalive `wal_end`. This keeps the slot moving while the publication is quiet.

### 9.3 Status updates

- **Timing.** A dedicated task sends a standby status update every `source.status_interval` (default 10 s), and immediately whenever the server requests a reply. Each update reports the confirmed LSN as write, flush and apply.
- **Independence.** The task never waits on the reader, the sink or the store (invariant A4). AWS sets `wal_sender_timeout` to 30 s.

### 9.4 Backpressure

- **The bound.** In-flight events (received but not yet settled) are bounded by `delivery.max_inflight_bytes` (default 256 MiB), measured on encoded envelopes.
- **At the bound,** the reader stops reading the socket. Status updates continue, so the connection stays up, and the server holds the remaining WAL.

### 9.5 Replay

After any restart or takeover, the server resends from the confirmed LSN. Events that were in flight are sent again with the same ids, and parked events are not duplicated (§10.6).

## 10. Retries, parking and dead events

### 10.1 Retry

A retryable outcome puts the event's order key into exponential backoff with full jitter, from 100 ms up to 30 s. That key waits while other keys keep flowing.

### 10.2 Parking

- **When an event parks.**
  - An event still undelivered `delivery.park_after` (default 30 s) after its first failure is parked.
  - A terminal outcome parks it immediately as `dead`.
- **There is no separate outage mode.**
  - During a sink outage, events park after `park_after`, the slot keeps advancing, and the backlog moves into the state store.
  - Alerts watch the parked counts and the age of the oldest parked event (§17).
- **Cost.** Parking costs one store write per event. On S3 that is about $5 per million events in PUT requests, plus reads when draining.

### 10.3 Parked queues

- **One queue per key.** Each parked order key has a queue in the StateStore, in id order.
- **New events queue behind.** While a key has a parked queue, every new event for that key is appended to the queue and never sent ahead of it.
- **Draining.** The owning lane drains the queue alongside live traffic:
  - it reads the head and sends it;
  - it removes the head on success;
  - it waits with the backoff of §10.1 between failures.
- **Back to live.** The key returns to the live path once the store confirms its queue is empty.

### 10.4 Dead events

- **A dead event blocks its key.** It stays at the head of its queue until an operator either:
  - redrives it, marking it retrying again after fixing the cause; or
  - discards it, removing it.
- **Operator access.** Operators act through the admin API (§16).
- **Alerting.** Any dead event raises an alert.

### 10.5 Limits

Short of a halt (§10.7) or a lost slot (§13.3), only two conditions stall the pipeline:

- **The parked limit.** `delivery.max_parked_events` defaults to 10,000,000, and 0 means unlimited. At the limit no more events park, so undeliverable events stay in flight until backpressure stalls the reader.
- **The StateStore is unavailable.** Events cannot park, and the pipeline stalls the same way.

### 10.6 Startup and takeover

- **Reading starts at once.** Reading and decoding begin as soon as the slot is acquired.
- **Heads load per partition.** Parked queue heads load one partition at a time, with 256 partitions keyed by the first byte of the order key id. Store calls are bounded by `state_store.max_concurrency`.
- **Delivery waits for its partition.** Delivery for an order key starts only once its partition has loaded, so no live event can overtake a parked one.
- **Replayed events for parked keys.**
  - A replayed event with an id below its queue's head settled before the restart, so it is skipped.
  - Any other event for a parked key is appended. Appends are create-only, so an event already in the queue is not duplicated.

### 10.7 Decode faults are never parked

- **Values can't fail.** The decoder cannot fail on a value (§14.1, D2).
- **A malformed message is a transport fault.** pg-cdc drops the connection, reconnects and replays from the confirmed LSN, which lies before the fault.
- **Repeated faults halt the pipeline.** If the same LSN faults three times in a row, the pipeline halts with an error and an alert. It never skips.

## 11. Adapters

Each adapter is an object-safe async trait (`async-trait`), selected by `type` in config. A new backend is one implementation plus its config section.

### 11.1 Sink

```rust
#[async_trait]
pub trait Sink: Send + Sync {
    fn capabilities(&self) -> SinkCapabilities;
    async fn preflight(&self) -> Result<(), SinkError>;
    async fn send(&self, batch: &[Outbound]) -> Vec<Outcome>;
}

pub struct SinkCapabilities {
    pub max_batch_messages: usize,
    pub max_batch_bytes: usize,
    pub max_message_bytes: usize,
}

pub struct Outbound {
    pub id: EventId,
    pub order_key: OrderKeyId,
    pub attributes: Attributes,
    pub body: Bytes,
}

pub enum Outcome {
    Delivered,
    Retryable(SinkError),
    Terminal(SinkError),
}
```

Contract:

- **One outcome per message.** `send` returns exactly one outcome per message, in input order.
- **`Terminal` is narrow.** It is only for errors specific to that message that retrying cannot fix.
- **No sink-side retries.** The sink neither retries nor backs off; the pipeline does. SDK-level retries are disabled.
- **`preflight` gates health.** It runs at startup and must succeed before the task reports healthy. `capabilities` is valid only after it has run.
- **Encoding is shared.** The pipeline encodes envelopes and attributes, so every sink receives the same body.

v1 sinks:

- `sns`;
- `stdout`: JSON lines, for development;
- `discard`: counts events and drops them, for load tests.

### 11.2 SNS sink

- **Batching.** The sink uses `PublishBatch` with up to 10 entries, within the topic's size limit.
- **Preflight.** `preflight` calls `GetTopicAttributes`:
  - the topic must exist;
  - `FifoTopic` selects FIFO mode;
  - `MaximumMessageSize` sets `max_message_bytes` and `max_batch_bytes`. It is 256 KiB unless the topic opts in to up to 1 MiB.
- **Size accounting** follows SNS rules: the body plus attribute names, types and values. If SNS still rejects a batch as too long, the sink sends its entries one at a time and reports per-entry outcomes.
- **Outcomes:**
  - a failed entry with `SenderFault=true` is `Terminal`;
  - a failed entry with `SenderFault=false` is `Retryable`;
  - request-level failures (throttling, 5xx, timeouts, connection or credential errors, a missing topic) make every entry `Retryable`.
- **FIFO topics.**
  - `MessageGroupId` is the order key id, and `MessageDeduplicationId` is the event id.
  - SNS FIFO allows 300 messages/s per group and 3,000/s per topic by default, and more in high-throughput mode.
- **Standard topics.** There is no group id, unless `sink.fair_queue_group: column:<name>` sets one from that column for SQS fair queues. The column must also be listed in `attribute_columns`.
- **Message attributes.**
  - Every message carries `schema`, `table` and `op`, plus one attribute per column in `sink.attribute_columns`.
  - A column's value comes from `after`, or from `before` when there is no `after`. It is sent as String or Number and omitted when null.
  - SNS allows 10 attributes per message.

### 11.3 StateStore

```rust
#[async_trait]
pub trait StateStore: Send + Sync {
    async fn append_parked(&self, events: &[ParkedEvent]) -> Result<(), StoreError>;
    async fn load_parked_heads(&self, partition: u8) -> Result<Vec<ParkedHead>, StoreError>;
    async fn read_parked(&self, key: &OrderKeyId, after: Option<&EventId>, limit: usize) -> Result<Vec<ParkedEvent>, StoreError>;
    async fn set_parked_status(&self, key: &OrderKeyId, id: &EventId, status: ParkStatus) -> Result<(), StoreError>;
    async fn remove_parked(&self, events: &[(OrderKeyId, EventId)]) -> Result<(), StoreError>;
    async fn read_leader(&self) -> Result<Option<Versioned<LeaderRecord>>, StoreError>;
    async fn swap_leader(&self, next: &LeaderRecord, expected: Option<&Version>) -> Result<SwapOutcome, StoreError>;
}
```

The types:

- `ParkedEvent` holds the order key id, event id, status (`retrying` or `dead`), attempts, first failure time, last error and the encoded envelope.
- `ParkedHead` holds the order key id, head event id and queue length.
- `LeaderRecord` holds the epoch, holder, state (`active` or `released`), confirmed LSN and update time.

Contract, checked by a shared conformance suite that every adapter must pass:

1. A call that returns `Ok` is durable.
2. Reads and listings reflect every completed write.
3. `append_parked` is create-only per (order key, event id). Appending an existing event is a no-op and does not change its status.
4. `read_parked` returns events in ascending id order, and `load_parked_heads` returns each queue's lowest id.
5. `remove_parked` of a missing event succeeds.
6. `swap_leader` is an atomic compare-and-swap, and `expected: None` means create-if-absent.
7. Concurrent calls are safe. pg-cdc bounds them with `state_store.max_concurrency` (default 32) and backs off on throttling.

### 11.4 S3 StateStore

- **Parked events** live at `<prefix>/<pipeline>/parked/<order key id>/<event id>.json`. They are written with `If-None-Match: *`, so a 412 means the object already exists and counts as success.
- **`load_parked_heads(p)`** lists the `parked/<pp>` prefix, where `pp` is the partition as two hex digits. The first object under each key is its head.
- **`read_parked`** lists with `StartAfter`, then fetches each object.
- **A status change** rewrites the object; only the owning lane writes it.
- **The leader record** lives at `<prefix>/<pipeline>/leader.json`. It is changed with `If-Match: <etag>` and created with `If-None-Match: *`.
- **Throttling.** `503 SlowDown` and other 5xx responses are retried with backoff.
- **Encryption.** Parked objects hold row data, so the bucket should use default encryption (SSE-KMS).

### 11.5 Filesystem StateStore

- **Layout.** The same tree lives under `state_store.path`.
- **Append** writes a temp file, fsyncs it, then `link()`s it to the final name, which fails if that name exists and so gives create-only semantics. It then fsyncs the directory.
- **The leader compare-and-swap** runs under an exclusive lock on `leader.lock`, with the version stored in the record.
- **Single host only.** Active/standby across hosts needs S3, or the later Postgres store.

### 11.6 PayloadStore

```rust
#[async_trait]
pub trait PayloadStore: Send + Sync {
    async fn put(&self, id: &EventId, body: &[u8]) -> Result<PayloadRef, StoreError>;
}
```

- **Why it is separate** from the StateStore: what it writes must be readable by consumers.
- **v1 implementation: S3.**
  - Payloads live at `<prefix>/<pipeline>/payloads/<event id>.json`.
  - `put` returns an `s3://` URI and the payload size.
  - A lifecycle rule should expire payloads after the consumers' retention period.
- **It is optional.** Without it, oversize events are dead (§7.5).

### 11.7 ConnectionProvider

```rust
#[async_trait]
pub trait ConnectionProvider: Send + Sync {
    async fn params(&self) -> Result<ConnectParams, ConnectError>;
}

pub struct ConnectParams {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: Option<SecretString>,
    pub tls: TlsConfig,
}
```

- **Called before every connection attempt,** replication and control alike. The core knows nothing about IAM or Aurora.
- **Failover handling is generic.** A dropped connection backs off (1 s to 30 s, full jitter), calls `params()` again, re-resolves DNS and reconnects.
- **v1 providers:**
  - `password`: static, with the password taken from the environment only;
  - `aws_rds_iam`: generates an RDS IAM token on every call (`aws_sdk_rds::auth_token::AuthTokenGenerator`) and uses it as the password.
- **Aurora specifics live in config and docs:** the cluster endpoint, the CA bundle, `rds_iam` and `rds.iam_auth_for_replication`.

## 12. Leadership and handover

### 12.1 Roles

A task moves from `standby` to `active` to `draining`. The slot is the lock, because Postgres allows one walsender per slot.

### 12.2 Standby

- **Polling.** A standby holds only the control connection and polls `pg_replication_slots` every 2 s.
- **Taking the slot.** When the slot is inactive, the standby sends `START_REPLICATION`. If another task wins, it keeps polling.
- **No writes.** A standby never writes to the sink or the store.

### 12.3 Lease

- **The check.** Every 2 s, the active task checks over the control connection that the slot's `active_pid` equals its walsender's PID, taken from the replication connection's BackendKeyData.
- **Self-fencing.** If no check succeeds for `lease_timeout` (6 s), the task fences itself:
  - it stops all sink and store writes;
  - it drops in-flight work;
  - it closes the replication connection;
  - it returns to standby.

### 12.4 Takeover

1. **Acquire the slot.** Start the status task and the reader immediately.
2. **Read the leader record.**
   - If it is `released`, continue at once.
   - If it is `active`, the previous holder did not release cleanly. Wait `fence_delay` (10 s, longer than `lease_timeout`) before any sink or store write.
3. **Claim leadership.** Swap the record to `{epoch: previous + 1, holder: self, state: active}`.
   - If the swap fails, another writer is active: release the slot, return to standby and raise an alert.
4. **Start delivery.** Load the parked heads and enable delivery partition by partition (§10.6).

### 12.5 Clean handover on SIGTERM

1. Stop reading the socket.
2. Give in-flight events `drain_timeout` (5 s) to deliver, then park whatever remains.
3. Send a final status update.
4. Swap the leader record to `released`, with the confirmed LSN.
5. Close the replication connection and exit 0.

The new task's gap is the drain plus one poll, about 2–7 s. A transaction still open at handover replays to the new task with the same ids.

### 12.6 Crash takeover

- **The slot frees on its own.** Postgres releases it when the dead task's connection drops, or after `wal_sender_timeout` if its host vanished.
- **The next holder waits.** It finds the record `active` and waits `fence_delay`.
- **The wait also covers a task that is still running.** That task fenced itself within `lease_timeout` of losing the slot.

### 12.7 ECS settings

- **Deployment:** minimum healthy 100%, maximum 200%, with the deployment circuit breaker on.
- **`stopTimeout`:** at least 15 s.
- **Container health check:** `pg-cdc health`.
- **Desired count:** 1, or 2 for a hot standby.

### 12.8 Known limit

A process paused for longer than `fence_delay` can land stale writes when it resumes. The result is duplicates, never loss.

## 13. Slot and source safety

### 13.1 Operator setup

Slots and publications are created outside pg-cdc.

- **Create the publication before the slot.** pgoutput looks up the publication as of each change's position in the WAL. Changes made before the publication existed either fail to decode or are skipped, depending on the Postgres version.

```sql
CREATE PUBLICATION pgcdc_orders FOR TABLE public.orders, public.order_items;
ALTER TABLE public.orders REPLICA IDENTITY FULL;
ALTER TABLE public.order_items REPLICA IDENTITY FULL;
SELECT pg_create_logical_replication_slot('pgcdc_orders', 'pgoutput');
GRANT rds_replication TO pgcdc;
GRANT rds_iam TO pgcdc;
```

- **Self-managed Postgres:** `ALTER ROLE pgcdc REPLICATION` replaces `rds_replication`, and `rds_iam` does not apply.
- **Aurora:** it also needs `rds.logical_replication=1`, which requires a writer reboot. IAM on replication connections additionally needs `rds.iam_auth_for_replication=1`.
- **Table privileges:** streaming changes needs none.

### 13.2 Startup validation

At process start, in any role, pg-cdc checks over the control connection that:

- `wal_level` is `logical`;
- the slot exists, uses `pgoutput`, belongs to the configured database, and has a `wal_status` other than `lost`;
- the publication exists.

On failure:

- **The task stays unhealthy and retries with backoff.** A misconfigured new task never takes over, and a start-first deploy keeps the old task running.
- **A `wal_status` of `unreserved` doesn't block.** It logs an error and raises an alert.

### 13.3 Slot loss

- **What happens.** If the slot is missing or invalidated, streaming stops and the task turns unhealthy and alerts. pg-cdc never recreates a slot.
  - Invalidation comes from `max_slot_wal_keep_size`, or from `idle_replication_slot_timeout` on Postgres 18.
- **Recovery.** An operator recreates the slot (§13.1). Changes made between the loss and the recreation are not captured; backfills (§20) will close that gap.
- **Aurora.** Whether logical slots survive a writer failover is not documented, and an AWS post (2026-08-12) says they must be recreated. This must be tested in staging before production.
  - If the slot survives, pg-cdc reconnects through the cluster endpoint and resumes.
  - If it does not, the path above applies.

## 14. Invariants

Each invariant has an enforcement mechanism listed in §14.6. A runtime violation is fail-stop: the process exits before acknowledging anything past the violation, and replays after restart.

### 14.1 Decoding

- **D1. The message set is closed.** Only protocol version 1 is negotiated, with none of its optional features (§6.2), and the decoder handles every message in that set.
- **D2. Value decoding is total.** Every type OID maps to typed JSON, or to its Postgres text form as a string.
  - If a known type fails to parse, the text form is emitted, and `decode_fallback_total` and an error log record it.
  - No value is dropped.
- **D3. Malformed input is a transport fault.** Malformed bytes, an unknown message tag, or a tuple of the wrong shape triggers a reconnect and a replay from the confirmed LSN.
  - Three consecutive faults at the same LSN halt the pipeline; it never skips.
- **D4. Caches are per connection.** Relation and type caches live per connection, and Postgres sends a Relation message before its first use on each connection.

### 14.2 Identity and order

- **O1. Ids depend only on the WAL.** The same WAL yields the same ids on every replay, strictly increasing in stream order.
- **O2. Each key is delivered in order.** Events of one order key reach the sink in id order, and the next is sent only after the previous is delivered or discarded.
- **O3. Parked keys stay queued.** While an order key has parked events, its new events are appended behind them. The key returns to the live path only once the store confirms the queue is empty.

### 14.3 Acknowledgement

- **A1. Only delivery or parking settles an event.** The type that marks an event settled can only be built from those two results, so no code path can skip an event and still acknowledge it.
- **A2. The confirmed LSN is safe.** It never decreases, and never passes the end LSN of a transaction with an unsettled event.
- **A3. Keepalive positions are acknowledged only when idle,** meaning no event is unsettled and no transaction is open.
- **A4. Status updates are independent.** One is sent at least every `status_interval`, whatever the reader, sink and store are doing.

### 14.4 Slot

- **S1. pg-cdc never creates or drops a slot,** and moves it only through status updates.

### 14.5 Leadership

- **L1. Only the slot holder writes** to the sink or the store.
  - A task that loses the slot stops writing within `lease_timeout`.
  - After an unclean takeover, a new holder waits `fence_delay` before writing.
- **L2. The leader epoch strictly increases.**
- **L3. A standby never writes** to the sink or the store.

### 14.6 Enforcement

| Mechanism | Covers |
|---|---|
| Property tests on the ack tracker with random settle orders | A2, A3 |
| Model-based property tests of lanes and parking, with random failures, restarts and operator actions | O2, O3, A1 |
| Fuzzing the decoder, which must never panic | D1–D3 |
| `to_jsonb` reference tests over every built-in type | D2 |
| Chaos integration tests: SIGKILL at random points, sink and store faults, dropped connections and takeovers. A checker verifies every change was delivered at least once and per-key order held. | O1–O3, A1–A4, L1–L3 |
| A settled event can only be constructed from a delivery or a park | A1 |
| Runtime checks with fail-stop | A2, O2 |

## 15. Configuration

- **One YAML file,** passed with `--config`. Any key can be overridden by an environment variable named `PGCDC__<SECTION>__<KEY>`.
- **Secrets come from the environment only.** A password in the file is rejected.
- **Units.** Durations take units (`30s`), and sizes take binary units (`256MiB`).

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
  park_after: 30s
  max_parked_events: 10000000
  max_inflight_bytes: 256MiB

sink:
  type: sns
  topic_arn: arn:aws:sns:us-east-1:123456789012:orders.fifo
  max_concurrency: 32
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

leadership:
  lease_timeout: 6s
  fence_delay: 10s
  drain_timeout: 5s

admin:
  listen: 127.0.0.1:9187

telemetry:
  otlp_endpoint: http://localhost:4317
  trace_sample_ratio: 0.01
```

With `credentials.type: password`, the password comes from `PGCDC__SOURCE__CREDENTIALS__PASSWORD`. A filesystem store is configured as `state_store: { type: filesystem, path: /var/lib/pg-cdc }`.

| Key | Default |
|---|---|
| `source.port` | `5432` |
| `source.tls.mode` | `verify-full` |
| `source.status_interval` | `10s` |
| `delivery.order_by` | `key` |
| `delivery.lanes` | `64` |
| `delivery.park_after` | `30s` |
| `delivery.max_parked_events` | `10000000` (`0` = unlimited) |
| `delivery.max_inflight_bytes` | `256MiB` |
| `sink.max_concurrency` | `32` |
| `state_store.max_concurrency` | `32` |
| `leadership.lease_timeout` | `6s` |
| `leadership.fence_delay` | `10s` |
| `leadership.drain_timeout` | `5s` |
| `admin.listen` | `127.0.0.1:9187` |
| `telemetry.otlp_endpoint` | unset (telemetry disabled) |
| `telemetry.trace_sample_ratio` | `0.01` |

Fixed internal values:

- standby poll and lease check: every 2 s;
- batch linger: 5 ms;
- sink retry backoff: 100 ms to 30 s;
- reconnect backoff: 1 s to 30 s;
- decode fault halt: 3 consecutive faults.

`fence_delay` must be greater than `lease_timeout`; config validation enforces it.

## 16. CLI and admin API

### 16.1 CLI

| Command | Purpose |
|---|---|
| `pg-cdc run --config <file>` | Run as standby or active |
| `pg-cdc health` | Exit 0 if `/healthz` returns 200. Used as the container health check, since a distroless image has no curl. |
| `pg-cdc parked list \| show \| redrive \| discard` | Thin client for the local admin endpoint's parked API |

There are no setup, check or teardown commands: slots and publications are managed with SQL or infrastructure code (§13.1).

### 16.2 Admin HTTP

The admin API binds to `admin.listen` (default `127.0.0.1:9187`). It has no authentication in v1, so it must stay on localhost or a private network.

| Endpoint | Behavior |
|---|---|
| `GET /healthz` | 200 when startup validation (§13.2), sink preflight and the store check pass, and, for an active task, the lease is valid |
| `GET /readyz` | 200 only when active and streaming |
| `GET /status` | Role, epoch, slot, confirmed LSN, in-flight counts, parked and dead counts, and time since the last status update |
| `GET /parked` | Parked queues with head id, head status, length and last error, filterable by status |
| `GET /parked/{order_key_id}` | The events in one queue |
| `POST /parked/{order_key_id}/redrive` | Mark the dead head retrying again |
| `POST /parked/{order_key_id}/discard?id=<event id>` | Remove the head, only if it still has that id |

- **No races with delivery.** Parked operations run inside the owning lane.
- **Standbys refuse them.** A standby answers with 409 and the current holder from the leader record.

## 17. Observability

### 17.1 Enabling

- **Switching it on.** Telemetry is enabled when `telemetry.otlp_endpoint` or `OTEL_EXPORTER_OTLP_ENDPOINT` is set. Standard `OTEL_*` variables are honored.
- **When disabled:**
  - No OpenTelemetry SDK, exporter or tracing layer is installed.
  - Hot-path spans are at DEBUG level and are filtered out at their callsites.
  - Metrics go through a small internal facade that does nothing when no provider is installed.
  - Logs go to stdout as JSON at INFO.
- **The `otel` cargo feature** (default on) gates every OpenTelemetry dependency. A build without it is the baseline for criterion 7 (§4).
- **When enabled:**
  - Traces, metrics and logs export over OTLP, using gRPC by default.
  - Head sampling uses `telemetry.trace_sample_ratio`: default 0.01, and 1.0 for load tests.

### 17.2 Spans

- a received transaction (commit LSN, xid, event count);
- a batch send (sink, size, outcomes);
- a store operation (operation, count);
- park;
- drain;
- a role change;
- a status update.

### 17.3 Metrics

| Metric | Type | Attributes |
|---|---|---|
| `pgcdc.events` | counter | `table`, `op`, `outcome` (received, delivered, parked, dead) |
| `pgcdc.commit_to_delivery` | histogram (s) | |
| `pgcdc.sink.duration` | histogram (s) | `outcome` |
| `pgcdc.store.duration` | histogram (s) | `operation` |
| `pgcdc.parked.events`, `pgcdc.parked.keys`, `pgcdc.parked.dead` | gauge | |
| `pgcdc.parked.oldest_age` | gauge (s) | |
| `pgcdc.inflight.events`, `pgcdc.inflight.bytes` | gauge | |
| `pgcdc.slot.retained_bytes` | gauge | |
| `pgcdc.status_update.age` | gauge (s) | |
| `pgcdc.role` | gauge | 0 standby, 1 active, 2 draining |
| `pgcdc.leader.epoch` | gauge | |
| `pgcdc.takeovers` | counter | `kind` (clean, unclean) |
| `pgcdc.decode.fallbacks` | counter | `type` |
| `pgcdc.tables_without_full_identity` | gauge | |

Suggested alerts:

- any dead event;
- the oldest parked event older than 10 minutes;
- slot retained bytes above a threshold;
- status update age above 30 s;
- no active task, or more than one.

### 17.4 Logs and profiles

- **Logs.** `tracing` writes JSON logs with trace and span ids, and they export over OTLP when telemetry is enabled.
- **Profiles.** An optional `pprof` build feature, off by default, serves CPU profiles on the admin port for load tests.

## 18. Testing

### 18.1 Unit, property and fuzz

- **Decoder golden fixtures:** raw pgoutput bytes captured from Postgres 14–18.
- **Value parsers:** property tests and fuzz targets.
- **Envelope golden JSON.**
- **Model tests:** property tests for the ack tracker, and for lanes and parking (§14.6).

### 18.2 Integration

The integration tests use testcontainers:

- Postgres 14, 15, 16, 17 and 18 with `wal_level=logical`;
- LocalStack for SNS, SQS and S3, reading delivered messages back through an SQS subscription.

They cover:

- the `to_jsonb` reference over every built-in type and arrays of them;
- TOAST under FULL and DEFAULT identity, TRUNCATE, and tables without keys;
- an idle publication, where the slot must still advance through keepalives;
- startup validation with a missing slot, a lost slot and a missing publication;
- oversize events, with and without a PayloadStore.

### 18.3 Chaos

The harness:

1. writes a known workload;
2. SIGKILLs pg-cdc at random points;
3. injects sink and store faults through a `chaos` wrapper;
4. drops connections;
5. runs clean and crash takeovers between two instances.

A checker then asserts that every committed change was either delivered at least once or is still in the store, and that per-key order held.

### 18.4 Conformance

- **StateStore suite:** run against the filesystem store locally, and against the S3 store on LocalStack or MinIO.
- **Sink suite:** outcome classification and batch limits.

### 18.5 Load

- **Load generator.** `loadgen` drives a configurable write rate, transaction size, row width (including TOAST) and key skew.
- **Runs:**
  - against `discard`, for pg-cdc's own ceiling;
  - against SNS, for the real number;
  - outage scenarios through the `chaos` wrapper, measuring parking throughput and drain catch-up.
- **Pass/fail** against criteria 4, 6 and 7 (§4).

### 18.6 CI

GitHub Actions runs:

- `cargo fmt --check`;
- `cargo clippy -D warnings`;
- unit and property tests;
- the integration matrix;
- a short fuzz run;
- `cargo deny`.

## 19. v1 milestones

Each milestone ends in working, tested software and gets its own implementation plan.

1. **Stream.**
   - Build: the workspace, the replication transport, the ConnectionProvider (`password`, `aws_rds_iam`), the decoder and value mapping (with fixtures, fuzzing and `to_jsonb` tests), the envelope, and the `stdout` sink.
   - Outcome: changes stream end to end, and the slot is acknowledged after delivery.
2. **Deliver.** Order keys, lanes, the ack tracker, the status task, backpressure, the Sink trait, the SNS and `discard` sinks, and retries.
3. **Park.**
   - The StateStore trait and its conformance suite, plus the filesystem and S3 stores.
   - Parking, dead events, draining and partitioned head loading.
   - The PayloadStore and oversize pointers.
4. **Lead.** Standby, lease, takeover, clean handover, fencing, the leader record, startup validation, and `pg-cdc health`.
5. **Operate.**
   - The admin API and the `parked` CLI.
   - OpenTelemetry.
   - The chaos harness, `loadgen`, and load tests against §4.
   - A multi-arch Docker image.
   - The README and a runbook covering setup SQL, Aurora failover, slot loss and parked events.

## 20. Roadmap (later scope, in priority order)

### v1.1: recover and harden

1. **Backfills (the `read` op).**
   - On-demand, per-table backfills using keyset pagination and watermarks written through `pg_logical_emit_message`. This turns on the `messages` option.
   - Backfills close the slot-loss gap, and let new consumers start with existing rows.
   - Before release, review the patent landscape for watermark-based CDC (Netflix DBLog).
2. **Admin API authentication.** Required before the admin API is exposed beyond localhost.
3. **Segmented S3 parking layout.** Only if load tests or real outages show per-event objects are too slow or too costly.

### v1.2: open-source release

4. **Postgres StateStore.** Sequin-style tables, held to the same conformance suite.
5. **More sinks,** in this order: SQS, then Kafka (idempotent producer, keyed by order key), then webhooks (with Standard Webhooks signatures).
6. **Several pipelines per process,** each with its own slot.
7. **Release engineering:** GHCR images, release binaries, crates.io publishing, a docs site and a contribution guide.

### v2: richer events

8. **Composite types as JSON objects,** completing `to_jsonb` parity.
9. **Transaction annotations** through `pg_logical_emit_message`, carrying actor and request context.
10. **Static transforms:** column exclusion and masking, and per-table routing.
11. **Schema-change events.**
12. **More adapters:**
    - sinks: Kinesis, NATS, Redis Streams, Google Pub/Sub;
    - connection providers: Vault, Cloud SQL IAM, Entra.

### Not planned without evidence

- Streaming of in-progress transactions (protocol 2 and later).
- Exactly-once sinks (Kafka transactions).
- A UI.

## 21. Risks and open questions

| Risk | Mitigation |
|---|---|
| Aurora may drop logical slots on writer failover. AWS doesn't document it, and one AWS post says it does. | Test in staging before production; alert and follow the runbook (§13.3); backfills arrive in v1.1 |
| SNS topics above 256 KiB may restrict subscription types, and the `PublishBatch` aggregate limit for such topics is unverified | Verify during milestone 2; `preflight` reads limits from the topic |
| The in-house transport is protocol code pg-cdc maintains | Keep it small and behind the transport crate's API; test it against a fake server and against Postgres 14 to 18 |
| Fencing assumes no process pauses for longer than `fence_delay` | Only duplicates result; documented |
| Parking during a long outage costs about $5 per million events on S3 | `max_parked_events`; a segmented layout in v1.1 if needed |
| Primary keys are read from the current catalog, not as of the WAL position | A key change during traffic can mis-key a few events; documented |
| A hot key moves at most one event per sink round trip | Documented, along with the `order_by` choices |
| Changing a publication during a backlog can renumber replayed ordinals | Change publications only when caught up (§7.2) |

## 22. References

- Sequin: https://github.com/sequinstream/sequin
- florinutz/pgcdc: https://github.com/florinutz/pgcdc
- supabase/etl: https://github.com/supabase/etl
- pgwire-replication: https://github.com/vnvo/pgwire-replication
- Materialize Postgres source: https://github.com/MaterializeInc/materialize/blob/main/src/storage/src/source/postgres/replication.rs
- Postgres logical replication protocol: https://www.postgresql.org/docs/18/protocol-logical-replication.html
- Postgres logical replication message formats: https://www.postgresql.org/docs/18/protocol-logicalrep-message-formats.html
- SNS 1 MiB payloads: https://www.amazonaws.cn/en/new/2026/amazon-sns-supports-message-payloads-up-to-1-mib/
- SNS FIFO message grouping: https://docs.aws.amazon.com/sns/latest/dg/fifo-message-grouping.html
- S3 conditional writes: https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html
- Aurora IAM auth for replication: https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.Replication.Logical.IAM-auth.html
