use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::time::Instant;
use tracing::Instrument;

use super::{
    LeaderRecord, ParkStatus, ParkedEvent, ParkedHead, ParkedQueues, ParkedSlice, ParkedSummary,
    StateStore, StoreError, SwapOutcome, Version, Versioned,
};
use crate::event::EventId;
use crate::lane::Stores;
use crate::metrics::{self, ErrorComponent, ErrorKind, Measurement, StoreOperation};
use crate::order::OrderKeyId;
use crate::payload::{PayloadRef, PayloadStore};

tokio::task_local! {
    static OPERATION: Option<StoreOperation>;
}

pub(crate) fn current_operation() -> Option<StoreOperation> {
    OPERATION.try_with(|operation| *operation).ok().flatten()
}

pub(crate) fn carry_operation<F: Future>(future: F) -> impl Future<Output = F::Output> {
    OPERATION.scope(current_operation(), future)
}

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

fn error_kind(error: &StoreError) -> ErrorKind {
    match error {
        StoreError::Unavailable(_) => ErrorKind::Unavailable,
        StoreError::NotFound(_) => ErrorKind::NotFound,
        StoreError::Conflict(_) => ErrorKind::Conflict,
        StoreError::Malformed { .. } => ErrorKind::Malformed,
    }
}

async fn timed<T, F>(operation: StoreOperation, count: usize, call: F) -> Result<T, StoreError>
where
    F: Future<Output = Result<T, StoreError>>,
{
    let span = tracing::debug_span!(
        "store",
        operation = operation.as_str(),
        count = u64::try_from(count).unwrap_or(u64::MAX)
    );
    async move {
        let started = metrics::enabled().then(Instant::now);
        let output = OPERATION.scope(Some(operation), call).await;
        if let Some(started) = started {
            metrics::record(Measurement::Store {
                operation,
                elapsed: started.elapsed(),
            });
            if let Err(error) = &output {
                metrics::record(Measurement::Error {
                    component: ErrorComponent::Store,
                    kind: error_kind(error),
                    count: 1,
                });
            }
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
    ) -> Result<ParkedQueues, StoreError> {
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

    async fn read_parked_within(
        &self,
        key: &OrderKeyId,
        after: Option<&EventId>,
        limit: usize,
        max_bytes: u64,
    ) -> Result<ParkedSlice, StoreError> {
        timed(
            StoreOperation::ReadParkedWithin,
            1,
            self.inner.read_parked_within(key, after, limit, max_bytes),
        )
        .await
    }

    async fn read_parked_summary(
        &self,
        key: &OrderKeyId,
    ) -> Result<Option<ParkedSummary>, StoreError> {
        timed(
            StoreOperation::ReadParkedSummary,
            1,
            self.inner.read_parked_summary(key),
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
    use crate::metrics::{ErrorComponent, ErrorKind, Measurement, Recorder};
    use crate::order::OrderKeyId;
    use crate::state::memory::MemoryStateStore;
    use crate::state::{LeaderRecord, LeaderState, ParkStatus, ParkedEvent};
    use bytes::Bytes;
    use pg_cdc_decoder::Lsn;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    #[tokio::test]
    async fn the_operation_in_flight_is_visible_to_its_calls_and_to_the_tasks_they_carry_it_to() {
        assert_eq!(current_operation(), None);
        let seen = timed(StoreOperation::ReadParked, 1, async {
            let direct = current_operation();
            let carried = tokio::spawn(carry_operation(async { current_operation() }))
                .await
                .unwrap();
            let uncarried = tokio::spawn(async { current_operation() }).await.unwrap();
            Ok::<_, StoreError>((direct, carried, uncarried))
        })
        .await
        .unwrap();
        assert_eq!(
            seen,
            (
                Some(StoreOperation::ReadParked),
                Some(StoreOperation::ReadParked),
                None
            )
        );
        let outside = tokio::spawn(carry_operation(async { current_operation() }))
            .await
            .unwrap();
        assert_eq!(outside, None);
    }

    #[derive(Debug)]
    struct Errors(Arc<Mutex<Vec<(ErrorComponent, ErrorKind, u64)>>>);

    impl Recorder for Errors {
        fn record(&self, measurement: Measurement<'_>) {
            if let Measurement::Error {
                component,
                kind,
                count,
            } = measurement
            {
                self.0.lock().unwrap().push((component, kind, count));
            }
        }
    }

    #[tokio::test]
    async fn each_failed_store_call_counts_one_store_error_of_its_kind() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        crate::metrics::install(Box::new(Errors(Arc::clone(&seen)))).unwrap();
        let failures = [
            (
                StoreError::Unavailable("down".to_owned()),
                ErrorKind::Unavailable,
            ),
            (StoreError::NotFound("k".to_owned()), ErrorKind::NotFound),
            (StoreError::Conflict("k".to_owned()), ErrorKind::Conflict),
            (
                StoreError::Malformed {
                    key: "k".to_owned(),
                    reason: "bad".to_owned(),
                },
                ErrorKind::Malformed,
            ),
        ];
        for (error, _) in &failures {
            let failed = timed(StoreOperation::ReadParked, 1, async {
                Err::<(), _>(error.clone())
            })
            .await;
            assert_eq!(failed, Err(error.clone()));
        }
        timed(StoreOperation::ReadParked, 1, async {
            Ok::<_, StoreError>(())
        })
        .await
        .unwrap();
        let expected: Vec<(ErrorComponent, ErrorKind, u64)> = failures
            .iter()
            .map(|(_, kind)| (ErrorComponent::Store, *kind, 1))
            .collect();
        assert_eq!(*seen.lock().unwrap(), expected);
    }

    #[derive(Debug)]
    struct Collecting(Arc<Mutex<Vec<(StoreOperation, Duration)>>>);

    impl Recorder for Collecting {
        fn record(&self, measurement: Measurement<'_>) {
            if let Measurement::Store { operation, elapsed } = measurement {
                self.0.lock().unwrap().push((operation, elapsed));
            }
        }
    }

    fn recording() -> Arc<Mutex<Vec<(StoreOperation, Duration)>>> {
        let seen = Arc::new(Mutex::new(Vec::new()));
        crate::metrics::install(Box::new(Collecting(Arc::clone(&seen)))).unwrap();
        seen
    }

    #[derive(Debug, Default)]
    struct Slow {
        inner: MemoryStateStore,
        calls: AtomicU64,
        fail: bool,
    }

    impl Slow {
        async fn call<T>(
            &self,
            answer: impl Future<Output = Result<T, StoreError>>,
        ) -> Result<T, StoreError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            tokio::time::sleep(Duration::from_millis(10 * call)).await;
            if self.fail {
                return Err(StoreError::Unavailable("injected outage".to_owned()));
            }
            answer.await
        }
    }

    #[async_trait]
    impl StateStore for Slow {
        async fn append_parked(&self, events: &[ParkedEvent]) -> Result<(), StoreError> {
            self.call(self.inner.append_parked(events)).await
        }

        async fn load_parked_heads(&self, partition: u8) -> Result<Vec<ParkedHead>, StoreError> {
            self.call(self.inner.load_parked_heads(partition)).await
        }

        async fn list_parked_queues(
            &self,
            after: Option<&OrderKeyId>,
            limit: usize,
            length_cap: usize,
        ) -> Result<ParkedQueues, StoreError> {
            self.call(self.inner.list_parked_queues(after, limit, length_cap))
                .await
        }

        async fn read_parked(
            &self,
            key: &OrderKeyId,
            after: Option<&EventId>,
            limit: usize,
        ) -> Result<Vec<ParkedEvent>, StoreError> {
            self.call(self.inner.read_parked(key, after, limit)).await
        }

        async fn read_parked_within(
            &self,
            key: &OrderKeyId,
            after: Option<&EventId>,
            limit: usize,
            max_bytes: u64,
        ) -> Result<ParkedSlice, StoreError> {
            self.call(self.inner.read_parked_within(key, after, limit, max_bytes))
                .await
        }

        async fn read_parked_summary(
            &self,
            key: &OrderKeyId,
        ) -> Result<Option<ParkedSummary>, StoreError> {
            self.call(self.inner.read_parked_summary(key)).await
        }

        async fn set_parked_status(
            &self,
            key: &OrderKeyId,
            id: &EventId,
            status: ParkStatus,
        ) -> Result<(), StoreError> {
            self.call(self.inner.set_parked_status(key, id, status))
                .await
        }

        async fn remove_parked(&self, events: &[(OrderKeyId, EventId)]) -> Result<(), StoreError> {
            self.call(self.inner.remove_parked(events)).await
        }

        async fn read_leader(&self) -> Result<Option<Versioned<LeaderRecord>>, StoreError> {
            self.call(self.inner.read_leader()).await
        }

        async fn swap_leader(
            &self,
            next: &LeaderRecord,
            expected: Option<&Version>,
        ) -> Result<SwapOutcome, StoreError> {
            self.call(self.inner.swap_leader(next, expected)).await
        }
    }

    #[async_trait]
    impl PayloadStore for Slow {
        async fn put(&self, id: &EventId, body: &[u8]) -> Result<PayloadRef, StoreError> {
            self.call(Payloads.put(id, body)).await
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
        let seen = recording();
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
        let within = state
            .read_parked_within(&key, None, 10, 1 << 20)
            .await
            .unwrap();
        assert_eq!(
            within,
            plain
                .read_parked_within(&key, None, 10, 1 << 20)
                .await
                .unwrap()
        );
        let summary = state.read_parked_summary(&key).await.unwrap();
        assert_eq!(summary, plain.read_parked_summary(&key).await.unwrap());
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
        let operations: Vec<&str> = seen
            .lock()
            .unwrap()
            .iter()
            .map(|(operation, _)| operation.as_str())
            .collect();
        assert_eq!(
            operations,
            vec![
                "append_parked",
                "load_parked_heads",
                "list_parked_queues",
                "read_parked",
                "read_parked_within",
                "read_parked_summary",
                "set_parked_status",
                "remove_parked",
                "read_leader",
                "swap_leader",
                "put_payload",
            ]
        );
    }

    const ALL_OPERATIONS: [StoreOperation; 11] = [
        StoreOperation::AppendParked,
        StoreOperation::LoadParkedHeads,
        StoreOperation::ListParkedQueues,
        StoreOperation::ReadParked,
        StoreOperation::ReadParkedWithin,
        StoreOperation::ReadParkedSummary,
        StoreOperation::SetParkedStatus,
        StoreOperation::RemoveParked,
        StoreOperation::ReadLeader,
        StoreOperation::SwapLeader,
        StoreOperation::PutPayload,
    ];

    async fn call_each_operation(inner: &Arc<Slow>) -> Vec<bool> {
        let state = InstrumentedStateStore::new(inner.clone());
        let payload = InstrumentedPayloadStore::new(inner.clone());
        let key = OrderKeyId::of(b"k");
        let id = EventId::new(Lsn(0x10), 1);
        vec![
            state.append_parked(&[event(1)]).await.is_ok(),
            state.load_parked_heads(key.partition()).await.is_ok(),
            state.list_parked_queues(None, 10, 10).await.is_ok(),
            state.read_parked(&key, None, 10).await.is_ok(),
            state
                .read_parked_within(&key, None, 10, 1 << 20)
                .await
                .is_ok(),
            state.read_parked_summary(&key).await.is_ok(),
            state
                .set_parked_status(&key, &id, ParkStatus::Dead)
                .await
                .is_ok(),
            state.remove_parked(&[(key, id)]).await.is_ok(),
            state.read_leader().await.is_ok(),
            state.swap_leader(&leader(), None).await.is_ok(),
            payload.put(&id, b"abc").await.is_ok(),
        ]
    }

    fn timed_in_turn() -> Vec<(StoreOperation, Duration)> {
        ALL_OPERATIONS
            .into_iter()
            .zip(1..)
            .map(|(operation, turn)| (operation, Duration::from_millis(10 * turn)))
            .collect()
    }

    #[tokio::test(start_paused = true)]
    async fn each_store_call_records_exactly_its_own_duration() {
        let seen = recording();
        let inner = Arc::new(Slow::default());
        assert_eq!(call_each_operation(&inner).await, vec![true; 11]);
        assert_eq!(*seen.lock().unwrap(), timed_in_turn());
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_store_call_still_records_its_duration() {
        let seen = recording();
        let inner = Arc::new(Slow {
            fail: true,
            ..Slow::default()
        });
        assert_eq!(call_each_operation(&inner).await, vec![false; 11]);
        assert_eq!(*seen.lock().unwrap(), timed_in_turn());
    }
}
