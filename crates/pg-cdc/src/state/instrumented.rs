use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::time::Instant;
use tracing::Instrument;

use super::{
    LeaderRecord, ParkStatus, ParkedEvent, ParkedHead, StateStore, StoreError, SwapOutcome,
    Version, Versioned,
};
use crate::event::EventId;
use crate::lane::Stores;
use crate::metrics::{self, Measurement, StoreOperation};
use crate::order::OrderKeyId;
use crate::payload::{PayloadRef, PayloadStore};

#[derive(Debug)]
pub struct InstrumentedStateStore {
    inner: Arc<dyn StateStore>,
}

#[derive(Debug)]
pub struct InstrumentedPayloadStore {
    inner: Arc<dyn PayloadStore>,
}

impl InstrumentedStateStore {
    pub fn new(inner: Arc<dyn StateStore>) -> Self {
        Self { inner }
    }
}

impl InstrumentedPayloadStore {
    pub fn new(inner: Arc<dyn PayloadStore>) -> Self {
        Self { inner }
    }
}

pub fn instrumented(stores: &Stores) -> Stores {
    Stores {
        state: Arc::new(InstrumentedStateStore::new(Arc::clone(&stores.state))),
        payload: stores.payload.as_ref().map(|payload| {
            Arc::new(InstrumentedPayloadStore::new(Arc::clone(payload))) as Arc<dyn PayloadStore>
        }),
    }
}

async fn timed<F: Future>(operation: StoreOperation, count: usize, call: F) -> F::Output {
    let span = tracing::debug_span!(
        "store",
        operation = operation.as_str(),
        count = u64::try_from(count).unwrap_or(u64::MAX)
    );
    async move {
        let started = metrics::enabled().then(Instant::now);
        let output = call.await;
        if let Some(started) = started {
            metrics::record(Measurement::Store {
                operation,
                elapsed: started.elapsed(),
            });
        }
        output
    }
    .instrument(span)
    .await
}

#[async_trait]
impl StateStore for InstrumentedStateStore {
    async fn append_parked(&self, events: &[ParkedEvent]) -> Result<(), StoreError> {
        timed(
            StoreOperation::AppendParked,
            events.len(),
            self.inner.append_parked(events),
        )
        .await
    }

    async fn load_parked_heads(&self, partition: u8) -> Result<Vec<ParkedHead>, StoreError> {
        timed(
            StoreOperation::LoadParkedHeads,
            1,
            self.inner.load_parked_heads(partition),
        )
        .await
    }

    async fn list_parked_queues(
        &self,
        after: Option<&OrderKeyId>,
        limit: usize,
        length_cap: usize,
    ) -> Result<Vec<ParkedHead>, StoreError> {
        timed(
            StoreOperation::ListParkedQueues,
            1,
            self.inner.list_parked_queues(after, limit, length_cap),
        )
        .await
    }

    async fn read_parked(
        &self,
        key: &OrderKeyId,
        after: Option<&EventId>,
        limit: usize,
    ) -> Result<Vec<ParkedEvent>, StoreError> {
        timed(
            StoreOperation::ReadParked,
            1,
            self.inner.read_parked(key, after, limit),
        )
        .await
    }

    async fn set_parked_status(
        &self,
        key: &OrderKeyId,
        id: &EventId,
        status: ParkStatus,
    ) -> Result<(), StoreError> {
        timed(
            StoreOperation::SetParkedStatus,
            1,
            self.inner.set_parked_status(key, id, status),
        )
        .await
    }

    async fn remove_parked(&self, events: &[(OrderKeyId, EventId)]) -> Result<(), StoreError> {
        timed(
            StoreOperation::RemoveParked,
            events.len(),
            self.inner.remove_parked(events),
        )
        .await
    }

    async fn read_leader(&self) -> Result<Option<Versioned<LeaderRecord>>, StoreError> {
        timed(StoreOperation::ReadLeader, 1, self.inner.read_leader()).await
    }

    async fn swap_leader(
        &self,
        next: &LeaderRecord,
        expected: Option<&Version>,
    ) -> Result<SwapOutcome, StoreError> {
        timed(
            StoreOperation::SwapLeader,
            1,
            self.inner.swap_leader(next, expected),
        )
        .await
    }
}

#[async_trait]
impl PayloadStore for InstrumentedPayloadStore {
    async fn put(&self, id: &EventId, body: &[u8]) -> Result<PayloadRef, StoreError> {
        timed(StoreOperation::PutPayload, 1, self.inner.put(id, body)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::EventId;
    use crate::metrics::{Measurement, Recorder};
    use crate::order::OrderKeyId;
    use crate::state::memory::MemoryStateStore;
    use crate::state::{LeaderRecord, LeaderState, ParkStatus, ParkedEvent};
    use bytes::Bytes;
    use pg_cdc_decoder::Lsn;
    use std::sync::Mutex;

    #[derive(Debug)]
    struct Collecting(Arc<Mutex<Vec<String>>>);

    impl Recorder for Collecting {
        fn record(&self, measurement: Measurement<'_>) {
            if let Measurement::Store { operation, .. } = measurement {
                self.0.lock().unwrap().push(operation.as_str().to_owned());
            }
        }
    }

    #[derive(Debug)]
    struct Payloads;

    #[async_trait]
    impl PayloadStore for Payloads {
        async fn put(&self, id: &EventId, body: &[u8]) -> Result<PayloadRef, StoreError> {
            Ok(PayloadRef {
                uri: format!("memory://{id}"),
                bytes: u64::try_from(body.len()).unwrap(),
            })
        }
    }

    fn event(ordinal: u32) -> ParkedEvent {
        ParkedEvent {
            order_key: OrderKeyId::of(b"k"),
            id: EventId::new(Lsn(0x10), ordinal),
            status: ParkStatus::Retrying,
            attempts: 1,
            first_failure_ms: 1,
            last_error: "seeded".to_owned(),
            attributes: Vec::new(),
            body: Bytes::from_static(b"x"),
        }
    }

    fn leader() -> LeaderRecord {
        LeaderRecord {
            epoch: 1,
            holder: "a".to_owned(),
            state: LeaderState::Active,
            confirmed_lsn: Lsn(5),
            updated_ms: 7,
        }
    }

    #[tokio::test]
    async fn every_store_call_records_its_operation_and_duration() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        crate::metrics::install(Box::new(Collecting(Arc::clone(&seen)))).unwrap();
        let plain = MemoryStateStore::new();
        let state = InstrumentedStateStore::new(Arc::new(plain.clone()));
        let payload = InstrumentedPayloadStore::new(Arc::new(Payloads));
        let key = OrderKeyId::of(b"k");
        let id = EventId::new(Lsn(0x10), 1);

        state.append_parked(&[event(1), event(2)]).await.unwrap();
        let heads = state.load_parked_heads(key.partition()).await.unwrap();
        assert_eq!(
            heads,
            plain.load_parked_heads(key.partition()).await.unwrap()
        );
        let queues = state.list_parked_queues(None, 10, 10).await.unwrap();
        assert_eq!(
            queues,
            plain.list_parked_queues(None, 10, 10).await.unwrap()
        );
        let read = state.read_parked(&key, None, 10).await.unwrap();
        assert_eq!(read, plain.read_parked(&key, None, 10).await.unwrap());
        state
            .set_parked_status(&key, &id, ParkStatus::Dead)
            .await
            .unwrap();
        state.remove_parked(&[(key, id)]).await.unwrap();
        assert_eq!(state.read_leader().await.unwrap(), None);
        let swapped = state.swap_leader(&leader(), None).await.unwrap();
        assert!(matches!(swapped, SwapOutcome::Swapped(_)));
        let put = payload.put(&id, b"abc").await.unwrap();
        assert_eq!(put.bytes, 3);

        assert_eq!(plain.parked_events(), 1);
        assert_eq!(
            plain.read_parked(&key, None, 10).await.unwrap()[0].status,
            ParkStatus::Retrying
        );
        assert_eq!(
            *seen.lock().unwrap(),
            vec![
                "append_parked",
                "load_parked_heads",
                "list_parked_queues",
                "read_parked",
                "set_parked_status",
                "remove_parked",
                "read_leader",
                "swap_leader",
                "put_payload",
            ]
        );
    }
}
