mod gauges;
mod model;
mod offload;
mod park;
mod stored;

use super::*;
use crate::ack::AckTracker;
use crate::payload::PayloadRef;
use crate::state::memory::MemoryStateStore;
use crate::state::{
    LeaderRecord, ParkStatus, ParkedEvent, StoreError, SwapOutcome, Version, Versioned,
};
use crate::status::ParkedSnapshot;
use async_trait::async_trait;
use bytes::Bytes;
use pg_cdc_decoder::Lsn;
use proptest::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use tokio::sync::Notify;

type Script = Box<dyn Fn(u32, u32) -> Outcome + Send + Sync>;

const LIMITS: SinkCapabilities = SinkCapabilities {
    max_batch_messages: 10,
    max_batch_bytes: 1 << 20,
    max_message_bytes: 1 << 20,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct Sent {
    ordinal: u32,
    at: Instant,
    delivered: bool,
}

struct ScriptedSink {
    limits: SinkCapabilities,
    script: Script,
    latency: Duration,
    attempts: Mutex<HashMap<u32, u32>>,
    batches: Mutex<Vec<Vec<Sent>>>,
    active: AtomicUsize,
    most_active: AtomicUsize,
    drop_outcomes_once: AtomicBool,
    bodies: Mutex<HashMap<u32, Bytes>>,
}

impl std::fmt::Debug for ScriptedSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScriptedSink").finish_non_exhaustive()
    }
}

impl ScriptedSink {
    fn new(limits: SinkCapabilities, script: Script) -> Self {
        Self {
            limits,
            script,
            latency: Duration::ZERO,
            attempts: Mutex::new(HashMap::new()),
            batches: Mutex::new(Vec::new()),
            active: AtomicUsize::new(0),
            most_active: AtomicUsize::new(0),
            drop_outcomes_once: AtomicBool::new(false),
            bodies: Mutex::new(HashMap::new()),
        }
    }

    fn delivering() -> Self {
        Self::new(LIMITS, Box::new(|_, _| Outcome::Delivered))
    }

    fn batches(&self) -> Vec<Vec<u32>> {
        self.batches
            .lock()
            .unwrap()
            .iter()
            .map(|batch| batch.iter().map(|sent| sent.ordinal).collect())
            .collect()
    }

    fn sends(&self) -> Vec<Sent> {
        self.batches.lock().unwrap().concat()
    }
}

#[async_trait]
impl Sink for ScriptedSink {
    fn capabilities(&self) -> SinkCapabilities {
        self.limits
    }

    async fn preflight(&self) -> Result<(), SinkError> {
        Ok(())
    }

    async fn send(&self, batch: &[Outbound]) -> Vec<Outcome> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.most_active.fetch_max(active, Ordering::SeqCst);
        tokio::time::sleep(self.latency).await;
        self.active.fetch_sub(1, Ordering::SeqCst);
        let at = Instant::now();
        let mut outcomes = Vec::new();
        let mut sent = Vec::new();
        for outbound in batch {
            let ordinal = outbound.id.ordinal();
            let attempt = {
                let mut attempts = self.attempts.lock().unwrap();
                let count = attempts.entry(ordinal).or_insert(0);
                *count += 1;
                *count
            };
            let outcome = (self.script)(ordinal, attempt);
            self.bodies
                .lock()
                .unwrap()
                .insert(ordinal, outbound.body.clone());
            sent.push(Sent {
                ordinal,
                at,
                delivered: outcome == Outcome::Delivered,
            });
            outcomes.push(outcome);
        }
        self.batches.lock().unwrap().push(sent);
        if self.drop_outcomes_once.swap(false, Ordering::SeqCst) {
            return Vec::new();
        }
        outcomes
    }
}

#[derive(Debug, Default)]
struct TestStore {
    inner: MemoryStateStore,
    held: Mutex<Option<u8>>,
    release: Notify,
    load_delay: Duration,
    append_delay: Duration,
    loads: AtomicUsize,
    loading: AtomicUsize,
    peak_loading: AtomicUsize,
    appends: AtomicUsize,
    fail_appends: AtomicU32,
    land_on_failure: AtomicUsize,
    fail_reads: AtomicU32,
    fail_removes: AtomicU32,
    outstanding: Mutex<HashSet<OrderKeyId>>,
    peak_outstanding: AtomicUsize,
}

fn take_failure(counter: &AtomicU32) -> bool {
    counter
        .try_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
            left.checked_sub(1)
        })
        .is_ok()
}

fn outage() -> StoreError {
    StoreError::Unavailable("injected outage".to_owned())
}

impl TestStore {
    fn hold(&self, partition: u8) {
        *self.held.lock().unwrap() = Some(partition);
    }

    async fn seed(&self, key: &[u8], ordinal: u32, status: ParkStatus) {
        self.inner
            .append_parked(&[parked(key, ordinal, status)])
            .await
            .unwrap();
    }

    async fn seed_sized(&self, key: &[u8], ordinal: u32, body: usize) {
        let mut event = parked(key, ordinal, ParkStatus::Retrying);
        event.body = Bytes::from(vec![b'x'; body]);
        self.inner.append_parked(&[event]).await.unwrap();
    }

    async fn queue(&self, key: &[u8]) -> Vec<(u32, ParkStatus)> {
        self.inner
            .read_parked(&OrderKeyId::of(key), None, usize::MAX)
            .await
            .unwrap()
            .iter()
            .map(|event| (event.id.ordinal(), event.status))
            .collect()
    }
}

#[async_trait]
impl StateStore for TestStore {
    async fn append_parked(&self, events: &[ParkedEvent]) -> Result<(), StoreError> {
        tokio::time::sleep(self.append_delay).await;
        if take_failure(&self.fail_appends) {
            let land = self.land_on_failure.load(Ordering::SeqCst);
            let mut sorted: Vec<&ParkedEvent> = events.iter().collect();
            sorted.sort_by_key(|event| (event.order_key, event.id));
            let mut landed: HashMap<OrderKeyId, usize> = HashMap::new();
            let prefix: Vec<ParkedEvent> = sorted
                .into_iter()
                .filter(|event| {
                    let count = landed.entry(event.order_key).or_default();
                    *count += 1;
                    *count <= land
                })
                .cloned()
                .collect();
            self.inner.append_parked(&prefix).await?;
            return Err(outage());
        }
        self.appends.fetch_add(events.len(), Ordering::SeqCst);
        self.inner.append_parked(events).await
    }

    async fn load_parked_heads(&self, partition: u8) -> Result<Vec<ParkedHead>, StoreError> {
        let now = self.loading.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak_loading.fetch_max(now, Ordering::SeqCst);
        tokio::time::sleep(self.load_delay).await;
        let held = *self.held.lock().unwrap() == Some(partition);
        if held {
            self.release.notified().await;
        }
        self.loading.fetch_sub(1, Ordering::SeqCst);
        self.loads.fetch_add(1, Ordering::SeqCst);
        self.inner.load_parked_heads(partition).await
    }

    async fn list_parked_queues(
        &self,
        after: Option<&OrderKeyId>,
        limit: usize,
        length_cap: usize,
    ) -> Result<Vec<ParkedHead>, StoreError> {
        self.inner
            .list_parked_queues(after, limit, length_cap)
            .await
    }

    async fn read_parked(
        &self,
        key: &OrderKeyId,
        after: Option<&EventId>,
        limit: usize,
    ) -> Result<Vec<ParkedEvent>, StoreError> {
        if take_failure(&self.fail_reads) {
            return Err(outage());
        }
        let events = self.inner.read_parked(key, after, limit).await?;
        if !events.is_empty() {
            let mut outstanding = self.outstanding.lock().unwrap();
            outstanding.insert(*key);
            self.peak_outstanding
                .fetch_max(outstanding.len(), Ordering::SeqCst);
        }
        Ok(events)
    }

    async fn set_parked_status(
        &self,
        key: &OrderKeyId,
        id: &EventId,
        status: ParkStatus,
    ) -> Result<(), StoreError> {
        self.outstanding.lock().unwrap().remove(key);
        self.inner.set_parked_status(key, id, status).await
    }

    async fn remove_parked(&self, events: &[(OrderKeyId, EventId)]) -> Result<(), StoreError> {
        if take_failure(&self.fail_removes) {
            return Err(outage());
        }
        {
            let mut outstanding = self.outstanding.lock().unwrap();
            for (key, _) in events {
                outstanding.remove(key);
            }
        }
        self.inner.remove_parked(events).await
    }

    async fn read_leader(&self) -> Result<Option<Versioned<LeaderRecord>>, StoreError> {
        self.inner.read_leader().await
    }

    async fn swap_leader(
        &self,
        next: &LeaderRecord,
        expected: Option<&Version>,
    ) -> Result<SwapOutcome, StoreError> {
        self.inner.swap_leader(next, expected).await
    }
}

#[derive(Debug, Default)]
struct TestPayloads {
    stored: Mutex<HashMap<EventId, Bytes>>,
    fail: AtomicU32,
}

#[async_trait]
impl PayloadStore for TestPayloads {
    async fn put(&self, id: &EventId, body: &[u8]) -> Result<PayloadRef, StoreError> {
        if take_failure(&self.fail) {
            return Err(outage());
        }
        self.stored
            .lock()
            .unwrap()
            .insert(*id, Bytes::copy_from_slice(body));
        Ok(PayloadRef {
            uri: format!("memory://{id}"),
            bytes: u64::try_from(body.len()).unwrap(),
        })
    }
}

fn envelope(ordinal: u32, padding: usize) -> Bytes {
    let id = EventId::new(Lsn(0x10), ordinal);
    Bytes::from(format!(
        r#"{{"id":"{id}","op":"insert","key":{{"id":1}},"before":null,"after":{{"pad":"{}"}},"source":{{"pipeline":"p"}}}}"#,
        "x".repeat(padding)
    ))
}

fn parked(key: &[u8], ordinal: u32, status: ParkStatus) -> ParkedEvent {
    ParkedEvent {
        order_key: OrderKeyId::of(key),
        id: EventId::new(Lsn(0x10), ordinal),
        status,
        attempts: 1,
        first_failure_ms: 1,
        last_error: "seeded".to_owned(),
        attributes: Vec::new(),
        body: Bytes::from_static(b"x"),
    }
}

#[derive(Debug, Clone, Copy)]
struct Setup {
    lanes: usize,
    concurrency: usize,
    park_after: Duration,
    max_parked_events: u64,
    store_concurrency: usize,
}

impl Default for Setup {
    fn default() -> Self {
        Self {
            lanes: 1,
            concurrency: 1,
            park_after: Duration::from_secs(30),
            max_parked_events: 0,
            store_concurrency: 4,
        }
    }
}

struct Harness {
    lanes: Lanes,
    reports: mpsc::UnboundedReceiver<Report>,
    ack: AckTracker,
    commits: u64,
    sink: Arc<ScriptedSink>,
    store: Arc<TestStore>,
}

impl Harness {
    async fn new(count: usize, concurrency: usize, sink: ScriptedSink) -> Self {
        let setup = Setup {
            lanes: count,
            concurrency,
            ..Setup::default()
        };
        Self::start(setup, sink, TestStore::default()).await
    }

    async fn start(setup: Setup, sink: ScriptedSink, store: TestStore) -> Self {
        Self::start_with_payloads(setup, sink, store, None).await
    }

    async fn start_with_payloads(
        setup: Setup,
        sink: ScriptedSink,
        store: TestStore,
        payloads: Option<Arc<TestPayloads>>,
    ) -> Self {
        let sink = Arc::new(sink);
        let store = Arc::new(store);
        let (reports, receiver) = mpsc::unbounded_channel();
        let dyn_sink: Arc<dyn Sink> = sink.clone();
        let settings = LaneSettings {
            lanes: setup.lanes,
            park_after: setup.park_after,
            max_parked_events: setup.max_parked_events,
            store_concurrency: setup.store_concurrency,
        };
        let stores = Stores {
            state: store.clone(),
            payload: payloads.map(|payloads| payloads as Arc<dyn PayloadStore>),
        };
        let lanes = Lanes::spawn(
            settings,
            &dyn_sink,
            &stores,
            &Arc::new(Semaphore::new(setup.concurrency)),
            &reports,
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
        Self {
            lanes,
            reports: receiver,
            ack: AckTracker::new(Lsn(0)),
            commits: 0,
            sink,
            store,
        }
    }

    fn gauges(&self) -> ParkedSnapshot {
        self.lanes.gauges().snapshot()
    }

    async fn delivered(&self, count: usize) -> Vec<u32> {
        for _ in 0..100_000 {
            let delivered: Vec<u32> = self
                .sink
                .sends()
                .iter()
                .filter(|sent| sent.delivered)
                .map(|sent| sent.ordinal)
                .collect();
            if delivered.len() >= count {
                return delivered;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the sink never delivered {count} events");
    }

    fn send(&mut self, ordinal: u32, key: &[u8], body: usize) -> Result<(), LaneStopped> {
        self.send_body(ordinal, key, Bytes::from(vec![b'x'; body]))
    }

    fn send_body(&mut self, ordinal: u32, key: &[u8], body: Bytes) -> Result<(), LaneStopped> {
        self.ack.begin().unwrap();
        let ticket = self.ack.track(u64::try_from(body.len()).unwrap()).unwrap();
        self.ack.commit(Lsn(100 + self.commits)).unwrap();
        self.commits += 1;
        self.lanes.dispatch(Pending {
            outbound: Outbound {
                id: EventId::new(Lsn(0x10), ordinal),
                order_key: OrderKeyId::of(key),
                attributes: Vec::new(),
                body,
            },
            ticket,
            arrived: Instant::now(),
        })
    }

    async fn settle(&mut self, count: usize) -> Lsn {
        for _ in 0..count {
            match self.reports.recv().await {
                Some(Report::Settled(settled)) => {
                    self.ack.settle(settled).unwrap();
                }
                other => panic!("expected a settlement, got {other:?}"),
            }
        }
        self.ack.confirmed()
    }

    async fn quiet_for(&mut self, window: Duration) -> bool {
        tokio::time::timeout(window, self.reports.recv())
            .await
            .is_err()
    }
}

fn key_in_lane(lane: usize, lanes: usize, skip: usize) -> Vec<u8> {
    (0u8..=255)
        .map(|byte| vec![byte])
        .filter(|key| OrderKeyId::of(key).lane(lanes) == lane)
        .nth(skip)
        .unwrap()
}

#[tokio::test(start_paused = true)]
async fn delivers_distinct_keys_in_one_batch_and_settles_each_event_once() {
    let mut harness = Harness::new(1, 1, ScriptedSink::delivering()).await;
    for (ordinal, key) in [(0, b"a"), (1, b"b"), (2, b"c")] {
        harness.send(ordinal, key, 10).unwrap();
    }
    assert_eq!(harness.settle(3).await, Lsn(102));
    assert_eq!(harness.sink.batches(), vec![vec![0, 1, 2]]);
    assert_eq!(harness.ack.inflight_events(), 0);
    assert!(harness.quiet_for(Duration::from_secs(1)).await);
}

#[tokio::test(start_paused = true)]
async fn o2_events_of_one_key_are_sent_one_at_a_time_in_id_order() {
    let mut harness = Harness::new(1, 1, ScriptedSink::delivering()).await;
    for ordinal in 0..3 {
        harness.send(ordinal, b"same", 10).unwrap();
    }
    assert_eq!(harness.settle(3).await, Lsn(102));
    assert_eq!(harness.sink.batches(), vec![vec![0], vec![1], vec![2]]);
}

#[tokio::test(start_paused = true)]
async fn batches_respect_the_sink_message_and_byte_limits() {
    let limits = SinkCapabilities {
        max_batch_messages: 2,
        ..LIMITS
    };
    let mut harness = Harness::new(
        1,
        1,
        ScriptedSink::new(limits, Box::new(|_, _| Outcome::Delivered)),
    )
    .await;
    for (ordinal, key) in [b"a", b"b", b"c", b"d", b"e"].into_iter().enumerate() {
        harness
            .send(u32::try_from(ordinal).unwrap(), key, 10)
            .unwrap();
    }
    harness.settle(5).await;
    assert_eq!(
        harness.sink.batches(),
        vec![vec![0, 1], vec![2, 3], vec![4]]
    );

    let limits = SinkCapabilities {
        max_batch_bytes: 100,
        ..LIMITS
    };
    let mut harness = Harness::new(
        1,
        1,
        ScriptedSink::new(limits, Box::new(|_, _| Outcome::Delivered)),
    )
    .await;
    harness.send(0, b"a", 150).unwrap();
    harness.send(1, b"b", 60).unwrap();
    harness.send(2, b"c", 40).unwrap();
    harness.send(3, b"d", 60).unwrap();
    harness.settle(4).await;
    assert_eq!(harness.sink.batches(), vec![vec![0], vec![1, 2], vec![3]]);
}

#[tokio::test(start_paused = true)]
async fn a_batch_that_is_not_full_lingers_five_milliseconds_from_its_oldest_arrival() {
    let mut harness = Harness::new(1, 1, ScriptedSink::delivering()).await;
    let start = Instant::now();
    harness.send(0, b"a", 10).unwrap();
    tokio::time::advance(Duration::from_millis(3)).await;
    harness.send(1, b"b", 10).unwrap();
    harness.settle(2).await;
    let later = Instant::now() + Duration::from_secs(1);
    tokio::time::sleep_until(later).await;
    harness.send(2, b"c", 10).unwrap();
    harness.settle(1).await;
    let sends = harness.sink.sends();
    assert_eq!(harness.sink.batches(), vec![vec![0, 1], vec![2]]);
    assert_eq!(sends[0].at, start + LINGER);
    assert_eq!(sends[2].at, later + LINGER);
}

#[tokio::test(start_paused = true)]
async fn a_full_batch_is_sent_without_lingering() {
    let limits = SinkCapabilities {
        max_batch_messages: 2,
        ..LIMITS
    };
    let mut harness = Harness::new(
        1,
        1,
        ScriptedSink::new(limits, Box::new(|_, _| Outcome::Delivered)),
    )
    .await;
    let start = Instant::now();
    harness.send(0, b"a", 10).unwrap();
    harness.send(1, b"b", 10).unwrap();
    harness.settle(2).await;
    assert_eq!(harness.sink.sends()[0].at, start);
}

#[tokio::test(start_paused = true)]
async fn a_retryable_outcome_backs_off_only_its_key() {
    let script: Script = Box::new(|ordinal, attempt| {
        if ordinal == 0 && attempt <= 2 {
            Outcome::Retryable(SinkError("throttled".to_owned()))
        } else {
            Outcome::Delivered
        }
    });
    let mut harness = Harness::new(1, 1, ScriptedSink::new(LIMITS, script)).await;
    harness.send(0, b"slow", 10).unwrap();
    harness.send(1, b"fast", 10).unwrap();
    harness.send(2, b"fast", 10).unwrap();
    harness.send(3, b"slow", 10).unwrap();
    assert_eq!(harness.settle(4).await, Lsn(103));
    let sends = harness.sink.sends();
    let order: Vec<(u32, bool)> = sends
        .iter()
        .map(|sent| (sent.ordinal, sent.delivered))
        .collect();
    assert_eq!(
        order,
        vec![
            (0, false),
            (1, true),
            (2, true),
            (0, false),
            (0, true),
            (3, true)
        ]
    );
    assert!(sends[3].at - sends[0].at <= RETRY_BASE);
    assert!(sends[4].at - sends[3].at <= RETRY_BASE * 2);
}

#[tokio::test(start_paused = true)]
async fn retries_back_off_with_full_jitter_up_to_thirty_seconds() {
    let script: Script = Box::new(|_, _| Outcome::Retryable(SinkError("down".to_owned())));
    let setup = Setup {
        park_after: Duration::from_secs(3600),
        ..Setup::default()
    };
    let mut harness = Harness::start(
        setup,
        ScriptedSink::new(LIMITS, script),
        TestStore::default(),
    )
    .await;
    harness.send(0, b"a", 10).unwrap();
    assert!(harness.quiet_for(Duration::from_secs(600)).await);
    let sends = harness.sink.sends();
    assert!(
        sends.len() > 20,
        "only {} attempts in 10 minutes",
        sends.len()
    );
    for (attempt, pair) in sends.windows(2).enumerate() {
        let cap = RETRY_BASE
            .saturating_mul(1 << attempt.min(16))
            .min(RETRY_CAP);
        assert!(pair[1].at - pair[0].at <= cap, "attempt {attempt}");
    }
    assert_eq!(harness.ack.inflight_events(), 1);
}

#[tokio::test(start_paused = true)]
async fn missing_outcomes_are_retried() {
    let sink = ScriptedSink::delivering();
    sink.drop_outcomes_once.store(true, Ordering::SeqCst);
    let mut harness = Harness::new(1, 1, sink).await;
    harness.send(0, b"a", 10).unwrap();
    harness.send(1, b"b", 10).unwrap();
    assert_eq!(harness.settle(2).await, Lsn(101));
    let batches = harness.sink.batches();
    assert_eq!(batches[0], vec![0, 1]);
    assert_eq!(batches.concat().len(), 4);
}

#[tokio::test(start_paused = true)]
async fn sink_calls_across_lanes_share_the_concurrency_bound() {
    for (concurrency, expected) in [(1, 1), (4, 4)] {
        let mut sink = ScriptedSink::delivering();
        sink.latency = Duration::from_millis(50);
        let mut harness = Harness::new(4, concurrency, sink).await;
        for lane in 0..4 {
            let key = key_in_lane(lane, 4, 0);
            harness
                .send(u32::try_from(lane).unwrap(), &key, 10)
                .unwrap();
        }
        harness.settle(4).await;
        assert_eq!(harness.sink.most_active.load(Ordering::SeqCst), expected);
    }
}

#[tokio::test(start_paused = true)]
async fn an_event_arriving_out_of_id_order_is_fatal_and_stops_the_lane() {
    let mut harness = Harness::new(1, 1, ScriptedSink::delivering()).await;
    harness.send(5, b"a", 10).unwrap();
    harness.send(3, b"b", 10).unwrap();
    match harness.reports.recv().await {
        Some(Report::Fatal(error)) => assert_eq!(
            error,
            LaneError::OutOfOrder {
                lane: 0,
                previous: EventId::new(Lsn(0x10), 5),
                id: EventId::new(Lsn(0x10), 3),
            }
        ),
        other => panic!("expected a fatal report, got {other:?}"),
    }
    tokio::task::yield_now().await;
    assert_eq!(harness.send(6, b"c", 10), Err(LaneStopped(0)));
}

#[tokio::test(start_paused = true)]
async fn dispatch_routes_each_order_key_to_its_lane() {
    let mut harness = Harness::new(4, 4, ScriptedSink::delivering()).await;
    let first = key_in_lane(2, 4, 0);
    let second = key_in_lane(2, 4, 1);
    harness.send(0, &first, 10).unwrap();
    harness.send(1, &second, 10).unwrap();
    harness.settle(2).await;
    assert_eq!(harness.sink.batches(), vec![vec![0, 1]]);
    harness.lanes.stop(Duration::ZERO).await;
}

#[tokio::test(start_paused = true)]
async fn stop_waits_for_a_send_in_flight_and_reports_a_clean_stop() {
    let mut sink = ScriptedSink::delivering();
    sink.latency = Duration::from_millis(300);
    let mut harness = Harness::new(1, 1, sink).await;
    harness.send(0, b"k", 10).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(harness.lanes.stop(Duration::from_secs(1)).await);
    assert_eq!(harness.sink.batches(), vec![vec![0]]);
    match harness.reports.recv().await {
        Some(Report::Settled(settled)) => {
            assert_eq!(harness.ack.settle(settled).unwrap(), Lsn(100));
        }
        other => panic!("expected a settlement, got {other:?}"),
    }
}

#[tokio::test(start_paused = true)]
async fn stop_aborts_a_lane_stuck_past_the_grace() {
    let mut sink = ScriptedSink::delivering();
    sink.latency = Duration::from_secs(3600);
    let mut harness = Harness::new(1, 1, sink).await;
    harness.send(0, b"k", 10).unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    let started = Instant::now();
    assert!(!harness.lanes.stop(Duration::from_secs(1)).await);
    assert_eq!(started.elapsed(), Duration::from_secs(1));
    assert_eq!(harness.sink.sends(), vec![]);
    assert!(harness.reports.recv().await.is_none());
}

#[tokio::test(start_paused = true)]
async fn stop_while_heads_are_loading_aborts_the_loader_and_lets_lanes_exit() {
    let store = TestStore::default();
    store.hold(0);
    let harness = Harness::start(
        Setup {
            lanes: 2,
            ..Setup::default()
        },
        ScriptedSink::delivering(),
        store,
    )
    .await;
    let started = Instant::now();
    assert!(harness.lanes.stop(Duration::from_secs(1)).await);
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[tokio::test(start_paused = true)]
async fn stop_with_zero_grace_aborts_at_once() {
    let mut sink = ScriptedSink::delivering();
    sink.latency = Duration::from_secs(3600);
    let mut harness = Harness::new(1, 1, sink).await;
    harness.send(0, b"k", 10).unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    let started = Instant::now();
    assert!(!harness.lanes.stop(Duration::ZERO).await);
    assert_eq!(started.elapsed(), Duration::ZERO);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn o2_a1_each_key_is_delivered_in_order_and_settles_only_on_delivery(
        keys in proptest::collection::vec(0u8..5, 1..40),
        failures in proptest::collection::vec(0u32..3, 40),
        lanes in 1usize..4,
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();
        runtime.block_on(async {
            let failures = failures.clone();
            let script: Script = Box::new(move |ordinal, attempt| {
                let index = usize::try_from(ordinal).unwrap();
                if attempt <= failures[index % failures.len()] {
                    Outcome::Retryable(SinkError("flaky".to_owned()))
                } else {
                    Outcome::Delivered
                }
            });
            let mut harness = Harness::new(lanes, 2, ScriptedSink::new(LIMITS, script)).await;
            for (ordinal, key) in keys.iter().enumerate() {
                harness.send(u32::try_from(ordinal).unwrap(), &[*key], 10).unwrap();
            }
            let last = Lsn(100 + u64::try_from(keys.len() - 1).unwrap());
            prop_assert_eq!(harness.settle(keys.len()).await, last);
            prop_assert_eq!(harness.ack.inflight_events(), 0);
            let sends = harness.sink.sends();
            let delivered = sends.iter().filter(|sent| sent.delivered).count();
            prop_assert_eq!(delivered, keys.len());
            for key in 0u8..5 {
                let per_key: Vec<&Sent> = sends
                    .iter()
                    .filter(|sent| keys[usize::try_from(sent.ordinal).unwrap()] == key)
                    .collect();
                for pair in per_key.windows(2) {
                    prop_assert!(pair[0].ordinal <= pair[1].ordinal);
                    if pair[0].ordinal != pair[1].ordinal {
                        prop_assert!(pair[0].delivered, "{} sent before {} was delivered", pair[1].ordinal, pair[0].ordinal);
                    }
                }
            }
            Ok(())
        })?;
    }
}
