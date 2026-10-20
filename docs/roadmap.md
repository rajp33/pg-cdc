# Roadmap

What pg-cdc does not do today, and what is planned or possible. The [design](superpowers/specs/2026-10-14-pg-cdc-design.md#20-roadmap-later-scope-in-priority-order) has the reasoning and priority order.

## Planned next

- **Snapshots and backfills.** Changes made while a slot was lost cannot be replayed today, and a new consumer cannot start from existing rows. On-demand, per-table backfills (the reserved `read` op) would close both gaps; until then, recovering from slot loss means reconciling the affected rows ([runbook](runbook.md#4-aurora-failover-and-slot-loss)).
- **Admin API authentication.** The admin endpoint has no authentication, so it must stay on loopback or a private network.
- **A segmented S3 parking layout, if needed.** Each parked event is its own S3 object, so parking and draining run at the store's request rate, bounded by `state_store.max_concurrency`. During an S3 brownout parking slows, the reader stalls at `max_inflight_bytes` and Postgres holds the WAL; nothing is lost. A segmented layout would cut requests per event if real outages show per-event objects are too slow or costly.
- **An optional `pprof` build feature** for CPU profiles during load tests.

## Later

- A Postgres state store, as an alternative to S3 and the filesystem.
- More sinks: SQS, then Kafka, then webhooks; later Kinesis, NATS, Redis Streams and Google Pub/Sub. Today the only production sink is SNS.
- Several pipelines per process, each with its own slot. Today one process serves one publication and one slot.
- Release engineering: release binaries, crates.io publishing and a docs site. Tagged releases ship as signed, versioned container images.
- Composite types as JSON objects. Today they are sent as their Postgres text form, which differs from `to_jsonb`.
- Transaction annotations through `pg_logical_emit_message`, carrying actor and request context.
- Static transforms: column exclusion and masking, and per-table routing. Today filtering is whatever the publication expresses (tables, row filters, column lists).
- Schema-change events.
- More connection providers: Vault, Cloud SQL IAM and Entra, beside the password and RDS IAM providers.

Not planned without evidence: streaming of in-progress transactions, exactly-once delivery, and a UI.

## Known constraints

These follow from the design and have no planned change.

- **A hot key's throughput.** One order key moves at most one event per sink round trip, roughly 20 to 50 events/s against SNS. `delivery.order_by` and `delivery.order_by_tables` choose the key.
- **A lane waits for its store calls.** Parking, head loading, removals and admin operations run inside the lane that owns the key, so while one waits on the state store the lane sends nothing, for its healthy keys too. Other lanes keep flowing.
- **Parked keys cost memory** outside `max_inflight_bytes`, about 0.5 to 1 KB each, bounded by `delivery.max_parked_events` ([runbook](runbook.md#5-parked-and-dead-events)).
- **A paused process** longer than `fence_delay` can cause duplicates and, rarely, one key's events out of order, never loss.
- **A discarded event can come back.** Discard leaves no tombstone, so the event is replayed again after each crash until its transaction is confirmed.
- **Primary keys are read from the current catalog,** not as of the WAL position, so after a key change, events decoded against the changed key definition can be mis-keyed.
- **Change a publication only when caught up.** Changing its table list while transactions are unacknowledged can renumber the ordinals of replayed events.

## Not yet verified in production

These checks run once against real AWS before production ([runbook](runbook.md#7-pre-production-checklist)); local runs against an emulator could not settle them ([docs/load.md](load.md)).

- Sustained throughput and commit-to-delivery latency against a real SNS topic.
- Bounded WAL during a sink outage with a real S3 state store.
- `PublishBatch` against SNS topics configured above 256 KiB.
- Whether an Aurora logical slot survives a writer failover.
- An ECS deploy handover under production load.
- The throughput cost of the compiled-in OpenTelemetry layer when telemetry is off, which local runs could not separate from run-to-run noise.
