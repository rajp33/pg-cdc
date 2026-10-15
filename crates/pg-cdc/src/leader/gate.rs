use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::event::EventId;
use crate::lane::Stores;
use crate::order::OrderKeyId;
use crate::payload::{PayloadRef, PayloadStore};
use crate::sink::{Outbound, Outcome, Sink, SinkCapabilities, SinkError};
use crate::state::{
    LeaderRecord, ParkStatus, ParkedEvent, ParkedHead, StateStore, StoreError, SwapOutcome,
    Version, Versioned,
};

pub const FENCED: &str = "sink and store writes are fenced";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateState {
    Closed,
    Open,
    Fenced,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("sink and store writes are fenced")]
pub struct Fenced;

#[derive(Debug)]
pub struct WriteGate {
    state: watch::Sender<GateState>,
    token: CancellationToken,
    abandoned: AtomicU64,
}

impl WriteGate {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: watch::Sender::new(GateState::Closed),
            token: CancellationToken::new(),
            abandoned: AtomicU64::new(0),
        })
    }

    pub fn state(&self) -> GateState {
        *self.state.borrow()
    }

    pub fn open(&self) {
        self.state.send_if_modified(|state| {
            let closed = *state == GateState::Closed;
            if closed {
                *state = GateState::Open;
            }
            closed
        });
    }

    pub fn fence(&self) {
        self.state.send_replace(GateState::Fenced);
        self.token.cancel();
    }

    pub fn abandoned_writes(&self) -> u64 {
        self.abandoned.load(Ordering::SeqCst)
    }

    pub async fn write<F>(&self, call: F) -> Result<F::Output, Fenced>
    where
        F: Future + Send,
    {
        self.admit().await?;
        let mut guard = AbandonGuard {
            abandoned: &self.abandoned,
            completed: false,
        };
        tokio::select! {
            biased;
            () = self.token.cancelled() => Err(Fenced),
            output = call => {
                guard.completed = true;
                Ok(output)
            }
        }
    }

    pub async fn read<F>(&self, call: F) -> Result<F::Output, Fenced>
    where
        F: Future + Send,
    {
        self.admit().await?;
        tokio::select! {
            biased;
            () = self.token.cancelled() => Err(Fenced),
            output = call => Ok(output),
        }
    }

    async fn admit(&self) -> Result<(), Fenced> {
        let mut receiver = self.state.subscribe();
        let state = receiver
            .wait_for(|state| *state != GateState::Closed)
            .await
            .map_or(GateState::Fenced, |state| *state);
        match state {
            GateState::Fenced | GateState::Closed => Err(Fenced),
            GateState::Open => Ok(()),
        }
    }
}

struct AbandonGuard<'a> {
    abandoned: &'a AtomicU64,
    completed: bool,
}

impl Drop for AbandonGuard<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.abandoned.fetch_add(1, Ordering::SeqCst);
        }
    }
}

fn fenced_outcomes(batch: &[Outbound]) -> Vec<Outcome> {
    batch
        .iter()
        .map(|_| Outcome::Retryable(SinkError(FENCED.to_owned())))
        .collect()
}

fn unavailable() -> StoreError {
    StoreError::Unavailable(FENCED.to_owned())
}

fn or_unavailable<T>(result: Result<Result<T, StoreError>, Fenced>) -> Result<T, StoreError> {
    result.unwrap_or_else(|Fenced| Err(unavailable()))
}

#[derive(Debug)]
pub struct GatedSink {
    inner: Arc<dyn Sink>,
    gate: Arc<WriteGate>,
}

impl GatedSink {
    pub fn new(inner: Arc<dyn Sink>, gate: Arc<WriteGate>) -> Self {
        Self { inner, gate }
    }
}

#[async_trait]
impl Sink for GatedSink {
    fn name(&self) -> &'static str {
        self.inner.name()
    }

    fn capabilities(&self) -> SinkCapabilities {
        self.inner.capabilities()
    }

    async fn preflight(&self) -> Result<(), SinkError> {
        self.inner.preflight().await
    }

    async fn send(&self, batch: &[Outbound]) -> Vec<Outcome> {
        match self.gate.write(self.inner.send(batch)).await {
            Ok(outcomes) => outcomes,
            Err(Fenced) => fenced_outcomes(batch),
        }
    }
}

#[derive(Debug)]
pub struct GatedStateStore {
    inner: Arc<dyn StateStore>,
    gate: Arc<WriteGate>,
}

impl GatedStateStore {
    pub fn new(inner: Arc<dyn StateStore>, gate: Arc<WriteGate>) -> Self {
        Self { inner, gate }
    }
}

#[async_trait]
impl StateStore for GatedStateStore {
    async fn append_parked(&self, events: &[ParkedEvent]) -> Result<(), StoreError> {
        or_unavailable(self.gate.write(self.inner.append_parked(events)).await)
    }

    async fn load_parked_heads(&self, partition: u8) -> Result<Vec<ParkedHead>, StoreError> {
        or_unavailable(
            self.gate
                .read(self.inner.load_parked_heads(partition))
                .await,
        )
    }

    async fn list_parked_queues(
        &self,
        after: Option<&OrderKeyId>,
        limit: usize,
        length_cap: usize,
    ) -> Result<Vec<ParkedHead>, StoreError> {
        or_unavailable(
            self.gate
                .read(self.inner.list_parked_queues(after, limit, length_cap))
                .await,
        )
    }

    async fn read_parked(
        &self,
        key: &OrderKeyId,
        after: Option<&EventId>,
        limit: usize,
    ) -> Result<Vec<ParkedEvent>, StoreError> {
        or_unavailable(
            self.gate
                .read(self.inner.read_parked(key, after, limit))
                .await,
        )
    }

    async fn set_parked_status(
        &self,
        key: &OrderKeyId,
        id: &EventId,
        status: ParkStatus,
    ) -> Result<(), StoreError> {
        or_unavailable(
            self.gate
                .write(self.inner.set_parked_status(key, id, status))
                .await,
        )
    }

    async fn remove_parked(&self, events: &[(OrderKeyId, EventId)]) -> Result<(), StoreError> {
        or_unavailable(self.gate.write(self.inner.remove_parked(events)).await)
    }

    async fn read_leader(&self) -> Result<Option<Versioned<LeaderRecord>>, StoreError> {
        or_unavailable(self.gate.read(self.inner.read_leader()).await)
    }

    async fn swap_leader(
        &self,
        next: &LeaderRecord,
        expected: Option<&Version>,
    ) -> Result<SwapOutcome, StoreError> {
        or_unavailable(
            self.gate
                .write(self.inner.swap_leader(next, expected))
                .await,
        )
    }
}

#[derive(Debug)]
pub struct GatedPayloadStore {
    inner: Arc<dyn PayloadStore>,
    gate: Arc<WriteGate>,
}

impl GatedPayloadStore {
    pub fn new(inner: Arc<dyn PayloadStore>, gate: Arc<WriteGate>) -> Self {
        Self { inner, gate }
    }
}

#[async_trait]
impl PayloadStore for GatedPayloadStore {
    async fn put(&self, id: &EventId, body: &[u8]) -> Result<PayloadRef, StoreError> {
        or_unavailable(self.gate.write(self.inner.put(id, body)).await)
    }
}

pub fn gated_sink(sink: &Arc<dyn Sink>, gate: &Arc<WriteGate>) -> Arc<dyn Sink> {
    Arc::new(GatedSink::new(Arc::clone(sink), Arc::clone(gate)))
}

pub fn gated_stores(stores: &Stores, gate: &Arc<WriteGate>) -> Stores {
    Stores {
        state: Arc::new(GatedStateStore::new(
            Arc::clone(&stores.state),
            Arc::clone(gate),
        )),
        payload: stores.payload.as_ref().map(|payload| {
            Arc::new(GatedPayloadStore::new(
                Arc::clone(payload),
                Arc::clone(gate),
            )) as Arc<dyn PayloadStore>
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::task::Poll;
    use std::time::Duration;

    use bytes::Bytes;
    use pg_cdc_decoder::Lsn;
    use tokio::task::{JoinHandle, yield_now};
    use tokio::time::{Instant, advance, sleep, timeout};

    use super::*;
    use crate::state::LeaderState;

    const LIMITS: SinkCapabilities = SinkCapabilities {
        max_batch_messages: 7,
        max_batch_bytes: 1 << 20,
        max_message_bytes: 1 << 19,
    };

    const SECOND: Duration = Duration::from_secs(1);
    const TEN_SECONDS: Duration = Duration::from_secs(10);

    fn unavailable_error() -> StoreError {
        StoreError::Unavailable(FENCED.to_owned())
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Call {
        name: &'static str,
        started: Instant,
        completed: Option<Instant>,
    }

    #[derive(Debug)]
    struct Recorder {
        delay: Duration,
        detach_appends: bool,
        log: Arc<Mutex<Vec<Call>>>,
    }

    impl Recorder {
        fn new(delay: Duration) -> Arc<Self> {
            Arc::new(Self {
                delay,
                detach_appends: false,
                log: Arc::default(),
            })
        }

        fn detached(delay: Duration) -> Arc<Self> {
            Arc::new(Self {
                delay,
                detach_appends: true,
                log: Arc::default(),
            })
        }

        fn calls(&self) -> Vec<Call> {
            self.log.lock().unwrap().clone()
        }

        fn names(&self) -> Vec<&'static str> {
            let mut names: Vec<_> = self.calls().iter().map(|call| call.name).collect();
            names.sort_unstable();
            names
        }

        async fn run(&self, name: &'static str) {
            let index = {
                let mut log = self.log.lock().unwrap();
                log.push(Call {
                    name,
                    started: Instant::now(),
                    completed: None,
                });
                log.len() - 1
            };
            let delay = self.delay;
            let log = Arc::clone(&self.log);
            let work = async move {
                sleep(delay).await;
                log.lock().unwrap()[index].completed = Some(Instant::now());
            };
            if self.detach_appends && name == "append_parked" {
                tokio::spawn(work).await.unwrap();
            } else {
                work.await;
            }
        }
    }

    #[async_trait]
    impl Sink for Recorder {
        fn capabilities(&self) -> SinkCapabilities {
            LIMITS
        }

        async fn preflight(&self) -> Result<(), SinkError> {
            self.run("preflight").await;
            Ok(())
        }

        async fn send(&self, batch: &[Outbound]) -> Vec<Outcome> {
            self.run("send").await;
            batch.iter().map(|_| Outcome::Delivered).collect()
        }
    }

    #[async_trait]
    impl StateStore for Recorder {
        async fn append_parked(&self, _events: &[ParkedEvent]) -> Result<(), StoreError> {
            self.run("append_parked").await;
            Ok(())
        }

        async fn load_parked_heads(&self, _partition: u8) -> Result<Vec<ParkedHead>, StoreError> {
            self.run("load_parked_heads").await;
            Ok(vec![ParkedHead {
                order_key: key(),
                head: id(),
                length: 3,
            }])
        }

        async fn list_parked_queues(
            &self,
            _after: Option<&OrderKeyId>,
            _limit: usize,
            _length_cap: usize,
        ) -> Result<Vec<ParkedHead>, StoreError> {
            self.run("list_parked_queues").await;
            Ok(vec![ParkedHead {
                order_key: key(),
                head: id(),
                length: 3,
            }])
        }

        async fn read_parked(
            &self,
            _key: &OrderKeyId,
            _after: Option<&EventId>,
            _limit: usize,
        ) -> Result<Vec<ParkedEvent>, StoreError> {
            self.run("read_parked").await;
            Ok(vec![parked_event()])
        }

        async fn set_parked_status(
            &self,
            _key: &OrderKeyId,
            _id: &EventId,
            _status: ParkStatus,
        ) -> Result<(), StoreError> {
            self.run("set_parked_status").await;
            Ok(())
        }

        async fn remove_parked(&self, _events: &[(OrderKeyId, EventId)]) -> Result<(), StoreError> {
            self.run("remove_parked").await;
            Ok(())
        }

        async fn read_leader(&self) -> Result<Option<Versioned<LeaderRecord>>, StoreError> {
            self.run("read_leader").await;
            Ok(Some(Versioned {
                value: leader(),
                version: Version("v1".to_owned()),
            }))
        }

        async fn swap_leader(
            &self,
            _next: &LeaderRecord,
            _expected: Option<&Version>,
        ) -> Result<SwapOutcome, StoreError> {
            self.run("swap_leader").await;
            Ok(SwapOutcome::Conflict)
        }
    }

    #[async_trait]
    impl PayloadStore for Recorder {
        async fn put(&self, _id: &EventId, body: &[u8]) -> Result<PayloadRef, StoreError> {
            self.run("put").await;
            Ok(PayloadRef {
                uri: "s3://bucket/payload".to_owned(),
                bytes: u64::try_from(body.len()).unwrap(),
            })
        }
    }

    fn key() -> OrderKeyId {
        OrderKeyId::of(b"key")
    }

    fn id() -> EventId {
        EventId::new(Lsn(0x10), 1)
    }

    fn outbound(ordinal: u32) -> Outbound {
        Outbound {
            id: EventId::new(Lsn(0x10), ordinal),
            order_key: key(),
            attributes: Vec::new(),
            body: Bytes::from_static(b"{}"),
        }
    }

    fn parked_event() -> ParkedEvent {
        ParkedEvent {
            order_key: key(),
            id: id(),
            status: ParkStatus::Retrying,
            attempts: 1,
            first_failure_ms: 1,
            last_error: "seeded".to_owned(),
            attributes: Vec::new(),
            body: Bytes::from_static(b"{}"),
        }
    }

    fn leader() -> LeaderRecord {
        LeaderRecord {
            epoch: 1,
            holder: "task".to_owned(),
            state: LeaderState::Active,
            confirmed_lsn: Lsn(0x20),
            updated_ms: 5,
        }
    }

    struct Gated {
        gate: Arc<WriteGate>,
        sink: Arc<GatedSink>,
        state: Arc<GatedStateStore>,
        payload: Arc<GatedPayloadStore>,
    }

    impl Gated {
        fn new(recorder: &Arc<Recorder>) -> Self {
            let gate = WriteGate::new();
            Self {
                sink: Arc::new(GatedSink::new(
                    Arc::clone(recorder) as Arc<dyn Sink>,
                    Arc::clone(&gate),
                )),
                state: Arc::new(GatedStateStore::new(
                    Arc::clone(recorder) as Arc<dyn StateStore>,
                    Arc::clone(&gate),
                )),
                payload: Arc::new(GatedPayloadStore::new(
                    Arc::clone(recorder) as Arc<dyn PayloadStore>,
                    Arc::clone(&gate),
                )),
                gate,
            }
        }

        fn opened(recorder: &Arc<Recorder>) -> Self {
            let gated = Self::new(recorder);
            gated.gate.open();
            gated
        }

        fn append(&self) -> JoinHandle<Result<(), StoreError>> {
            let state = Arc::clone(&self.state);
            tokio::spawn(async move { state.append_parked(&[parked_event()]).await })
        }
    }

    async fn poll_once<F: Future + Unpin>(future: &mut F) {
        std::future::poll_fn(|context| {
            let _ = Pin::new(&mut *future).poll(context);
            Poll::Ready(())
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn l3_nothing_passes_a_closed_gate() {
        let recorder = Recorder::new(SECOND);
        let gated = Gated::new(&recorder);
        let sink = Arc::clone(&gated.sink);
        let send = tokio::spawn(async move { sink.send(&[outbound(1)]).await });
        let state = Arc::clone(&gated.state);
        let append = tokio::spawn(async move { state.append_parked(&[parked_event()]).await });
        let state = Arc::clone(&gated.state);
        let remove = tokio::spawn(async move { state.remove_parked(&[(key(), id())]).await });
        let payload = Arc::clone(&gated.payload);
        let put = tokio::spawn(async move { payload.put(&id(), b"body").await });
        let state = Arc::clone(&gated.state);
        let heads = tokio::spawn(async move { state.load_parked_heads(3).await });

        advance(Duration::from_secs(3600)).await;
        yield_now().await;

        assert!(!send.is_finished());
        assert!(!append.is_finished());
        assert!(!remove.is_finished());
        assert!(!put.is_finished());
        assert!(!heads.is_finished());
        assert_eq!(recorder.names(), Vec::<&str>::new());
        assert_eq!(gated.gate.state(), GateState::Closed);

        gated.gate.open();

        assert_eq!(send.await.unwrap(), vec![Outcome::Delivered]);
        assert_eq!(append.await.unwrap(), Ok(()));
        assert_eq!(remove.await.unwrap(), Ok(()));
        assert_eq!(
            put.await.unwrap(),
            Ok(PayloadRef {
                uri: "s3://bucket/payload".to_owned(),
                bytes: 4,
            })
        );
        assert_eq!(
            heads.await.unwrap(),
            Ok(vec![ParkedHead {
                order_key: key(),
                head: id(),
                length: 3,
            }])
        );
        assert_eq!(
            recorder.names(),
            [
                "append_parked",
                "load_parked_heads",
                "put",
                "remove_parked",
                "send"
            ]
        );
        assert!(recorder.calls().iter().all(|call| call.completed.is_some()));
        assert_eq!(gated.gate.abandoned_writes(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn l1_fencing_cancels_calls_in_flight_and_refuses_new_ones() {
        let recorder = Recorder::new(TEN_SECONDS);
        let gated = Gated::opened(&recorder);
        let began = Instant::now();
        let append = gated.append();

        advance(SECOND).await;
        gated.gate.fence();

        assert_eq!(append.await.unwrap(), Err(unavailable_error()));
        assert_eq!(began.elapsed(), SECOND);
        let calls = recorder.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "append_parked");
        assert_eq!(calls[0].completed, None);
        assert_eq!(gated.gate.abandoned_writes(), 1);

        assert_eq!(
            gated.state.append_parked(&[parked_event()]).await,
            Err(unavailable_error())
        );
        assert_eq!(
            gated.sink.send(&[outbound(1), outbound(2)]).await,
            vec![Outcome::Retryable(SinkError(FENCED.to_owned())); 2]
        );
        assert_eq!(began.elapsed(), SECOND);
        assert_eq!(recorder.calls().len(), 1);
        assert_eq!(gated.gate.abandoned_writes(), 1);
        assert_eq!(gated.gate.state(), GateState::Fenced);
    }

    #[tokio::test(start_paused = true)]
    async fn a_write_that_completes_is_not_abandoned() {
        let recorder = Recorder::new(SECOND);
        let gated = Gated::opened(&recorder);

        assert_eq!(gated.state.append_parked(&[parked_event()]).await, Ok(()));
        assert_eq!(
            gated.sink.send(&[outbound(1)]).await,
            vec![Outcome::Delivered]
        );

        assert_eq!(gated.gate.abandoned_writes(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_dropped_write_counts_as_abandoned_and_a_dropped_read_does_not() {
        let recorder = Recorder::new(TEN_SECONDS);
        let gated = Gated::opened(&recorder);
        let events = [(key(), id())];
        let order_key = key();

        let mut write = Box::pin(gated.state.remove_parked(&events));
        let mut read = Box::pin(gated.state.read_parked(&order_key, None, 10));
        poll_once(&mut write).await;
        poll_once(&mut read).await;
        assert_eq!(recorder.names(), ["read_parked", "remove_parked"]);
        assert_eq!(gated.gate.abandoned_writes(), 0);

        drop(read);
        assert_eq!(gated.gate.abandoned_writes(), 0);
        drop(write);

        assert_eq!(gated.gate.abandoned_writes(), 1);
        assert!(recorder.calls().iter().all(|call| call.completed.is_none()));
    }

    #[tokio::test(start_paused = true)]
    async fn a_write_that_keeps_running_after_its_future_is_dropped_is_still_abandoned() {
        let recorder = Recorder::detached(TEN_SECONDS);
        let gated = Gated::opened(&recorder);
        let began = Instant::now();
        let append = gated.append();

        advance(SECOND).await;
        gated.gate.fence();

        assert_eq!(append.await.unwrap(), Err(unavailable_error()));
        assert_eq!(began.elapsed(), SECOND);
        assert_eq!(gated.gate.abandoned_writes(), 1);
        assert_eq!(recorder.calls()[0].completed, None);

        advance(TEN_SECONDS).await;
        yield_now().await;

        assert!(recorder.calls()[0].completed.is_some());
        assert_eq!(gated.gate.abandoned_writes(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn fencing_a_closed_gate_releases_waiting_calls_with_an_error() {
        let recorder = Recorder::new(SECOND);
        let gated = Gated::new(&recorder);
        let append = gated.append();
        let sink = Arc::clone(&gated.sink);
        let send = tokio::spawn(async move { sink.send(&[outbound(1)]).await });
        let began = Instant::now();

        advance(SECOND).await;
        assert!(!append.is_finished());
        gated.gate.fence();

        assert_eq!(append.await.unwrap(), Err(unavailable_error()));
        assert_eq!(
            send.await.unwrap(),
            vec![Outcome::Retryable(SinkError(FENCED.to_owned()))]
        );
        assert_eq!(began.elapsed(), SECOND);
        assert_eq!(recorder.names(), Vec::<&str>::new());
        assert_eq!(gated.gate.abandoned_writes(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn fencing_cancels_a_read_in_flight_without_counting_it() {
        let recorder = Recorder::new(TEN_SECONDS);
        let gated = Gated::opened(&recorder);
        let state = Arc::clone(&gated.state);
        let heads = tokio::spawn(async move { state.load_parked_heads(0).await });

        advance(SECOND).await;
        gated.gate.fence();

        assert_eq!(heads.await.unwrap(), Err(unavailable_error()));
        assert_eq!(recorder.calls().len(), 1);
        assert_eq!(recorder.calls()[0].completed, None);
        assert_eq!(gated.gate.abandoned_writes(), 0);
    }

    #[test]
    fn open_after_fence_stays_fenced() {
        let gate = WriteGate::new();
        assert_eq!(gate.state(), GateState::Closed);
        gate.open();
        assert_eq!(gate.state(), GateState::Open);
        gate.fence();
        gate.open();
        assert_eq!(gate.state(), GateState::Fenced);
    }

    #[tokio::test(start_paused = true)]
    async fn every_gated_call_is_refused_once_fenced() {
        let recorder = Recorder::new(SECOND);
        let gated = Gated::opened(&recorder);
        gated.gate.fence();
        let state = &gated.state;
        let record = leader();

        assert_eq!(
            gated.sink.send(&[outbound(1)]).await,
            vec![Outcome::Retryable(SinkError(FENCED.to_owned()))]
        );
        assert_eq!(
            state.append_parked(&[parked_event()]).await,
            Err(unavailable_error())
        );
        assert_eq!(state.load_parked_heads(0).await, Err(unavailable_error()));
        assert_eq!(
            state.list_parked_queues(None, 10, 5).await,
            Err(unavailable_error())
        );
        assert_eq!(
            state.read_parked(&key(), None, 1).await,
            Err(unavailable_error())
        );
        assert_eq!(
            state
                .set_parked_status(&key(), &id(), ParkStatus::Dead)
                .await,
            Err(unavailable_error())
        );
        assert_eq!(
            state.remove_parked(&[(key(), id())]).await,
            Err(unavailable_error())
        );
        assert_eq!(state.read_leader().await, Err(unavailable_error()));
        assert_eq!(
            state.swap_leader(&record, None).await,
            Err(unavailable_error())
        );
        assert_eq!(
            gated.payload.put(&id(), b"x").await,
            Err(unavailable_error())
        );
        assert_eq!(recorder.names(), Vec::<&str>::new());
    }

    #[tokio::test(start_paused = true)]
    async fn every_gated_call_reaches_the_inner_value_once_open() {
        let recorder = Recorder::new(SECOND);
        let gated = Gated::opened(&recorder);
        let state = &gated.state;
        let record = leader();

        assert_eq!(
            state.list_parked_queues(Some(&key()), 10, 5).await,
            Ok(vec![ParkedHead {
                order_key: key(),
                head: id(),
                length: 3,
            }])
        );
        assert_eq!(
            state.read_parked(&key(), None, 1).await,
            Ok(vec![parked_event()])
        );
        assert_eq!(
            state
                .set_parked_status(&key(), &id(), ParkStatus::Dead)
                .await,
            Ok(())
        );
        assert_eq!(
            state.read_leader().await,
            Ok(Some(Versioned {
                value: leader(),
                version: Version("v1".to_owned()),
            }))
        );
        assert_eq!(
            state.swap_leader(&record, None).await,
            Ok(SwapOutcome::Conflict)
        );
        assert_eq!(
            recorder.names(),
            [
                "list_parked_queues",
                "read_leader",
                "read_parked",
                "set_parked_status",
                "swap_leader"
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_gated_sink_reports_the_inner_capabilities_and_preflight() {
        let recorder = Recorder::new(SECOND);
        let gated = Gated::new(&recorder);

        assert_eq!(gated.sink.capabilities(), LIMITS);
        assert_eq!(
            timeout(Duration::from_secs(3600), gated.sink.preflight()).await,
            Ok(Ok(()))
        );
        assert_eq!(recorder.names(), ["preflight"]);
        assert_eq!(gated.gate.state(), GateState::Closed);
    }

    #[tokio::test(start_paused = true)]
    async fn the_helpers_wrap_the_sink_and_every_store_behind_one_gate() {
        let recorder = Recorder::new(SECOND);
        let gate = WriteGate::new();
        let sink: Arc<dyn Sink> = Arc::clone(&recorder) as Arc<dyn Sink>;
        let stores = Stores {
            state: Arc::clone(&recorder) as Arc<dyn StateStore>,
            payload: Some(Arc::clone(&recorder) as Arc<dyn PayloadStore>),
        };
        let wrapped_sink = gated_sink(&sink, &gate);
        let wrapped = gated_stores(&stores, &gate);
        let payload = wrapped.payload.clone().unwrap();
        gate.open();
        gate.fence();

        assert_eq!(
            wrapped_sink.send(&[outbound(1)]).await,
            vec![Outcome::Retryable(SinkError(FENCED.to_owned()))]
        );
        assert_eq!(
            wrapped.state.append_parked(&[parked_event()]).await,
            Err(unavailable_error())
        );
        assert_eq!(payload.put(&id(), b"x").await, Err(unavailable_error()));
        assert_eq!(recorder.names(), Vec::<&str>::new());

        let without = gated_stores(
            &Stores {
                state: Arc::clone(&recorder) as Arc<dyn StateStore>,
                payload: None,
            },
            &gate,
        );
        assert!(without.payload.is_none());
    }
}
