use std::cmp::Reverse;
use std::collections::hash_map::Entry;
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::ack::{Settled, Ticket};
use crate::event::EventId;
use crate::order::OrderKeyId;
use crate::payload::PayloadStore;
use crate::sink::{Outbound, Outcome, Sink, SinkCapabilities, SinkError};
use crate::state::StateStore;
use crate::supervisor::Backoff;

pub const LINGER: Duration = Duration::from_millis(5);
pub const RETRY_BASE: Duration = Duration::from_millis(100);
pub const RETRY_CAP: Duration = Duration::from_secs(30);

#[derive(Debug)]
pub struct Pending {
    pub outbound: Outbound,
    pub ticket: Ticket,
    pub arrived: Instant,
}

#[derive(Debug)]
pub enum Report {
    Settled(Settled),
    Fatal(LaneError),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LaneError {
    #[error("event {id} reached lane {lane} after event {previous}")]
    OutOfOrder {
        lane: usize,
        previous: EventId,
        id: EventId,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("lane {0} has stopped")]
pub struct LaneStopped(pub usize);

#[derive(Debug, Clone)]
pub struct Stores {
    pub state: Arc<dyn StateStore>,
    pub payload: Option<Arc<dyn PayloadStore>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LaneSettings {
    pub lanes: usize,
    pub park_after: Duration,
    pub max_parked_events: u64,
    pub store_concurrency: usize,
}

#[derive(Debug)]
pub struct Lanes {
    inboxes: Vec<mpsc::UnboundedSender<Pending>>,
    tasks: JoinSet<()>,
}

impl Lanes {
    pub fn spawn(
        settings: LaneSettings,
        sink: &Arc<dyn Sink>,
        _stores: &Stores,
        permits: &Arc<Semaphore>,
        reports: &mpsc::UnboundedSender<Report>,
    ) -> Self {
        let capabilities = sink.capabilities();
        let mut tasks = JoinSet::new();
        let inboxes = (0..settings.lanes.max(1))
            .map(|index| {
                let (inbox, receiver) = mpsc::unbounded_channel();
                let lane = Lane {
                    index,
                    sink: Arc::clone(sink),
                    permits: Arc::clone(permits),
                    capabilities,
                    inbox: receiver,
                    reports: reports.clone(),
                    keys: HashMap::new(),
                    ready: VecDeque::new(),
                    retries: BinaryHeap::new(),
                    last: None,
                };
                tasks.spawn(lane.run());
                inbox
            })
            .collect();
        Self { inboxes, tasks }
    }

    pub fn dispatch(&self, pending: Pending) -> Result<(), LaneStopped> {
        let index = pending.outbound.order_key.lane(self.inboxes.len());
        let inbox = self.inboxes.get(index).ok_or(LaneStopped(index))?;
        inbox.send(pending).map_err(|_| LaneStopped(index))
    }

    pub async fn shutdown(mut self) {
        self.tasks.shutdown().await;
    }
}

#[derive(Debug)]
struct Key {
    queue: VecDeque<Pending>,
    backoff: Backoff,
    failing: bool,
}

struct Lane {
    index: usize,
    sink: Arc<dyn Sink>,
    permits: Arc<Semaphore>,
    capabilities: SinkCapabilities,
    inbox: mpsc::UnboundedReceiver<Pending>,
    reports: mpsc::UnboundedSender<Report>,
    keys: HashMap<OrderKeyId, Key>,
    ready: VecDeque<OrderKeyId>,
    retries: BinaryHeap<Reverse<(Instant, OrderKeyId)>>,
    last: Option<EventId>,
}

impl Lane {
    async fn run(mut self) {
        loop {
            while let Ok(pending) = self.inbox.try_recv() {
                if !self.accept(pending) {
                    return;
                }
            }
            self.promote_due_retries(Instant::now());
            let (count, full) = self.plan_batch();
            let linger = self.linger_deadline();
            if let Some(deadline) = linger
                && (full || Instant::now() >= deadline)
            {
                self.send_batch(count).await;
                continue;
            }
            let next_retry = self.retries.peek().map(|Reverse((due, _))| *due);
            let until = [linger, next_retry].into_iter().flatten().min();
            if !self.wait(until).await {
                return;
            }
        }
    }

    fn accept(&mut self, pending: Pending) -> bool {
        let id = pending.outbound.id;
        if let Some(previous) = self.last
            && id <= previous
        {
            let error = LaneError::OutOfOrder {
                lane: self.index,
                previous,
                id,
            };
            let _ = self.reports.send(Report::Fatal(error));
            return false;
        }
        self.last = Some(id);
        let key = pending.outbound.order_key;
        match self.keys.entry(key) {
            Entry::Occupied(mut entry) => entry.get_mut().queue.push_back(pending),
            Entry::Vacant(entry) => {
                entry.insert(Key {
                    queue: VecDeque::from([pending]),
                    backoff: Backoff::new(RETRY_BASE, RETRY_CAP),
                    failing: false,
                });
                self.make_ready(key);
            }
        }
        true
    }

    fn make_ready(&mut self, key: OrderKeyId) {
        let Some(head) = self.keys.get(&key).and_then(|state| state.queue.front()) else {
            return;
        };
        let bytes = head.outbound.encoded_len();
        if bytes > self.capabilities.max_message_bytes {
            tracing::error!(
                order_key = %key,
                id = %head.outbound.id,
                bytes,
                max_message_bytes = self.capabilities.max_message_bytes,
                "event exceeds the sink's message size; its order key is blocked"
            );
            return;
        }
        self.ready.push_back(key);
    }

    fn promote_due_retries(&mut self, now: Instant) {
        while let Some(Reverse((due, key))) = self.retries.peek().copied() {
            if due > now {
                break;
            }
            self.retries.pop();
            self.make_ready(key);
        }
    }

    fn linger_deadline(&self) -> Option<Instant> {
        let key = self.ready.front()?;
        let head = self.keys.get(key)?.queue.front()?;
        Some(head.arrived + LINGER)
    }

    fn plan_batch(&self) -> (usize, bool) {
        let limits = self.capabilities;
        let mut count = 0;
        let mut bytes = 0_usize;
        for key in &self.ready {
            let Some(head) = self.keys.get(key).and_then(|state| state.queue.front()) else {
                continue;
            };
            let size = head.outbound.encoded_len();
            if count == limits.max_batch_messages
                || (count > 0 && bytes.saturating_add(size) > limits.max_batch_bytes)
            {
                return (count, true);
            }
            count += 1;
            bytes = bytes.saturating_add(size);
        }
        (count, count >= limits.max_batch_messages)
    }

    async fn wait(&mut self, until: Option<Instant>) -> bool {
        let timer = async {
            match until {
                Some(at) => tokio::time::sleep_until(at).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            received = self.inbox.recv() => match received {
                Some(pending) => self.accept(pending),
                None => false,
            },
            () = timer => true,
        }
    }

    async fn send_batch(&mut self, count: usize) {
        let mut keys = Vec::with_capacity(count);
        let mut batch = Vec::with_capacity(count);
        for key in self.ready.drain(..count) {
            if let Some(head) = self.keys.get(&key).and_then(|state| state.queue.front()) {
                keys.push(key);
                batch.push(head.outbound.clone());
            }
        }
        let outcomes = match self.permits.acquire().await {
            Ok(_permit) => self.sink.send(&batch).await,
            Err(_) => Vec::new(),
        };
        if outcomes.len() != batch.len() {
            tracing::error!(
                sent = batch.len(),
                outcomes = outcomes.len(),
                "the sink broke its contract of one outcome per message"
            );
        }
        let mut outcomes = outcomes.into_iter();
        for key in keys {
            let outcome = outcomes.next().unwrap_or_else(|| {
                Outcome::Retryable(SinkError("the sink returned no outcome".to_owned()))
            });
            self.complete(key, &outcome);
        }
    }

    fn complete(&mut self, key: OrderKeyId, outcome: &Outcome) {
        let Some(state) = self.keys.get_mut(&key) else {
            return;
        };
        match outcome {
            Outcome::Delivered => {
                let Some(head) = state.queue.pop_front() else {
                    return;
                };
                if let Ok(settled) = head.ticket.settle(outcome) {
                    let _ = self.reports.send(Report::Settled(settled));
                }
                if state.queue.is_empty() {
                    self.keys.remove(&key);
                } else {
                    state.backoff.reset();
                    state.failing = false;
                    self.make_ready(key);
                }
            }
            Outcome::Retryable(error) => {
                let delay = state.backoff.next_delay();
                let id = state.queue.front().map(|head| head.outbound.id);
                if state.failing {
                    tracing::debug!(order_key = %key, ?id, %error, ?delay, "delivery failed again; retrying");
                } else {
                    tracing::warn!(order_key = %key, ?id, %error, ?delay, "delivery failed; retrying");
                }
                state.failing = true;
                self.retries.push(Reverse((Instant::now() + delay, key)));
            }
            Outcome::Terminal(error) => {
                let id = state.queue.front().map(|head| head.outbound.id);
                tracing::error!(
                    order_key = %key,
                    ?id,
                    %error,
                    "the sink rejected the event for good; its order key is blocked"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ack::AckTracker;
    use crate::state::memory::MemoryStateStore;
    use async_trait::async_trait;
    use bytes::Bytes;
    use pg_cdc_decoder::Lsn;
    use proptest::prelude::*;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

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

    struct Harness {
        lanes: Lanes,
        reports: mpsc::UnboundedReceiver<Report>,
        ack: AckTracker,
        commits: u64,
        sink: Arc<ScriptedSink>,
    }

    impl Harness {
        fn new(count: usize, concurrency: usize, sink: ScriptedSink) -> Self {
            let sink = Arc::new(sink);
            let (reports, receiver) = mpsc::unbounded_channel();
            let dyn_sink: Arc<dyn Sink> = sink.clone();
            let settings = LaneSettings {
                lanes: count,
                park_after: Duration::from_secs(30),
                max_parked_events: 0,
                store_concurrency: 4,
            };
            let stores = Stores {
                state: Arc::new(MemoryStateStore::new()),
                payload: None,
            };
            let lanes = Lanes::spawn(
                settings,
                &dyn_sink,
                &stores,
                &Arc::new(Semaphore::new(concurrency)),
                &reports,
            );
            Self {
                lanes,
                reports: receiver,
                ack: AckTracker::new(Lsn(0)),
                commits: 0,
                sink,
            }
        }

        fn send(&mut self, ordinal: u32, key: &[u8], body: usize) -> Result<(), LaneStopped> {
            self.ack.begin().unwrap();
            let ticket = self.ack.track(u64::try_from(body).unwrap()).unwrap();
            self.ack.commit(Lsn(100 + self.commits)).unwrap();
            self.commits += 1;
            self.lanes.dispatch(Pending {
                outbound: Outbound {
                    id: EventId::new(Lsn(0x10), ordinal),
                    order_key: OrderKeyId::of(key),
                    attributes: Vec::new(),
                    body: Bytes::from(vec![b'x'; body]),
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
        let mut harness = Harness::new(1, 1, ScriptedSink::delivering());
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
        let mut harness = Harness::new(1, 1, ScriptedSink::delivering());
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
        );
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
        );
        harness.send(0, b"a", 150).unwrap();
        harness.send(1, b"b", 60).unwrap();
        harness.send(2, b"c", 40).unwrap();
        harness.send(3, b"d", 60).unwrap();
        harness.settle(4).await;
        assert_eq!(harness.sink.batches(), vec![vec![0], vec![1, 2], vec![3]]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_batch_that_is_not_full_lingers_five_milliseconds_from_its_oldest_arrival() {
        let mut harness = Harness::new(1, 1, ScriptedSink::delivering());
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
        );
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
        let mut harness = Harness::new(1, 1, ScriptedSink::new(LIMITS, script));
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
        let mut harness = Harness::new(1, 1, ScriptedSink::new(LIMITS, script));
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
    async fn a_terminal_outcome_blocks_its_key_and_never_settles() {
        let script: Script = Box::new(|ordinal, _| {
            if ordinal == 0 {
                Outcome::Terminal(SinkError("rejected".to_owned()))
            } else {
                Outcome::Delivered
            }
        });
        let mut harness = Harness::new(1, 1, ScriptedSink::new(LIMITS, script));
        harness.send(0, b"poison", 10).unwrap();
        harness.send(1, b"poison", 10).unwrap();
        harness.send(2, b"healthy", 10).unwrap();
        assert_eq!(harness.settle(1).await, Lsn(0));
        assert!(harness.quiet_for(Duration::from_secs(600)).await);
        assert_eq!(harness.sink.batches(), vec![vec![0, 2]]);
        assert_eq!(harness.ack.inflight_events(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn an_event_larger_than_the_sink_message_limit_is_never_sent() {
        let limits = SinkCapabilities {
            max_message_bytes: 100,
            ..LIMITS
        };
        let mut harness = Harness::new(
            1,
            1,
            ScriptedSink::new(limits, Box::new(|_, _| Outcome::Delivered)),
        );
        harness.send(0, b"wide", 101).unwrap();
        harness.send(1, b"narrow", 100).unwrap();
        harness.settle(1).await;
        assert!(harness.quiet_for(Duration::from_secs(60)).await);
        assert_eq!(harness.sink.batches(), vec![vec![1]]);
    }

    #[tokio::test(start_paused = true)]
    async fn missing_outcomes_are_retried() {
        let sink = ScriptedSink::delivering();
        sink.drop_outcomes_once.store(true, Ordering::SeqCst);
        let mut harness = Harness::new(1, 1, sink);
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
            let mut harness = Harness::new(4, concurrency, sink);
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
        let mut harness = Harness::new(1, 1, ScriptedSink::delivering());
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
        let mut harness = Harness::new(4, 4, ScriptedSink::delivering());
        let first = key_in_lane(2, 4, 0);
        let second = key_in_lane(2, 4, 1);
        harness.send(0, &first, 10).unwrap();
        harness.send(1, &second, 10).unwrap();
        harness.settle(2).await;
        assert_eq!(harness.sink.batches(), vec![vec![0, 1]]);
        harness.lanes.shutdown().await;
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
                let mut harness = Harness::new(lanes, 2, ScriptedSink::new(LIMITS, script));
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

        #[test]
        fn o2_a1_a_terminal_event_blocks_only_its_key_and_never_settles(
            keys in proptest::collection::vec(0u8..5, 1..40),
            fates in proptest::collection::vec(0u32..6, 40),
            lanes in 1usize..4,
        ) {
            const TERMINAL: u32 = 5;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .start_paused(true)
                .build()
                .unwrap();
            let fate = |ordinal: usize| fates[ordinal % fates.len()];
            let mut blocked = [false; 5];
            let mut settled = Vec::new();
            let mut terminal = Vec::new();
            for (ordinal, key) in keys.iter().enumerate() {
                let key = usize::from(*key);
                if blocked[key] {
                    continue;
                }
                if fate(ordinal) == TERMINAL {
                    blocked[key] = true;
                    terminal.push(u32::try_from(ordinal).unwrap());
                } else {
                    settled.push(u32::try_from(ordinal).unwrap());
                }
            }
            let first_unsettled = (0..keys.len())
                .find(|ordinal| !settled.contains(&u32::try_from(*ordinal).unwrap()));
            let confirmed = match first_unsettled {
                None => Lsn(100 + u64::try_from(keys.len() - 1).unwrap()),
                Some(0) => Lsn(0),
                Some(ordinal) => Lsn(100 + u64::try_from(ordinal - 1).unwrap()),
            };
            runtime.block_on(async {
                let fates = fates.clone();
                let script: Script = Box::new(move |ordinal, attempt| {
                    let fate = fates[usize::try_from(ordinal).unwrap() % fates.len()];
                    if fate == TERMINAL && attempt > 1 {
                        Outcome::Terminal(SinkError("rejected".to_owned()))
                    } else if attempt <= fate {
                        Outcome::Retryable(SinkError("flaky".to_owned()))
                    } else {
                        Outcome::Delivered
                    }
                });
                let mut harness = Harness::new(lanes, 2, ScriptedSink::new(LIMITS, script));
                for (ordinal, key) in keys.iter().enumerate() {
                    harness.send(u32::try_from(ordinal).unwrap(), &[*key], 10).unwrap();
                }
                prop_assert_eq!(harness.settle(settled.len()).await, confirmed);
                prop_assert!(harness.quiet_for(Duration::from_secs(600)).await);
                prop_assert_eq!(harness.ack.confirmed(), confirmed);
                prop_assert_eq!(harness.ack.inflight_events(), u64::try_from(keys.len() - settled.len()).unwrap());
                let sends = harness.sink.sends();
                let delivered: Vec<u32> = sends
                    .iter()
                    .filter(|sent| sent.delivered)
                    .map(|sent| sent.ordinal)
                    .collect();
                let mut delivered_sorted = delivered.clone();
                delivered_sorted.sort_unstable();
                prop_assert_eq!(&delivered_sorted, &settled);
                let mut sent: Vec<u32> = sends.iter().map(|sent| sent.ordinal).collect();
                sent.sort_unstable();
                sent.dedup();
                let mut expected_sent: Vec<u32> = settled.iter().chain(&terminal).copied().collect();
                expected_sent.sort_unstable();
                prop_assert_eq!(sent, expected_sent);
                for ordinal in &terminal {
                    let attempts = sends.iter().filter(|sent| sent.ordinal == *ordinal).count();
                    prop_assert_eq!(attempts, 2, "terminal event {} was sent again", ordinal);
                }
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
}
