mod loader;
mod offload;
mod operate;
mod park;
mod stored;

#[cfg(test)]
mod tests;

use std::cmp::Reverse;
use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, BinaryHeap, HashMap, VecDeque};
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::Instrument;

use crate::ack::{Settled, Ticket};
use crate::event::EventId;
use crate::metrics::{self, EventOutcome, Measurement, SinkOutcome};
use crate::order::OrderKeyId;
use crate::payload::PayloadStore;
use crate::sink::{Outbound, Outcome, Sink, SinkCapabilities, SinkError, metric_labels};
use crate::state::{PARTITIONS, ParkedHead, StateStore, now_ms};
use crate::status::{LaneCounts, ParkedGauges, STATS_INTERVAL};
use crate::supervisor::Backoff;

use operate::Operation;
pub use operate::{OPERATION_QUEUE, OperateError, Operator};

pub const LINGER: Duration = Duration::from_millis(5);
pub const RETRY_BASE: Duration = Duration::from_millis(100);
pub const RETRY_CAP: Duration = Duration::from_secs(30);

#[derive(Debug)]
pub struct Pending {
    pub outbound: Outbound,
    pub ticket: Ticket,
    pub arrived: Instant,
    pub committed_ms: Option<u64>,
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
enum Inbound {
    Event(Pending),
    Head(ParkedHead),
    Loaded(u8),
    ParkAll,
    Operate(Operation),
}

#[derive(Debug)]
pub struct Lanes {
    inboxes: Vec<mpsc::UnboundedSender<Inbound>>,
    operation_queues: Vec<Arc<Semaphore>>,
    running: JoinSet<()>,
    loader: JoinSet<()>,
    gauges: Arc<ParkedGauges>,
}

impl Lanes {
    pub fn spawn(
        settings: LaneSettings,
        sink: &Arc<dyn Sink>,
        stores: &Stores,
        permits: &Arc<Semaphore>,
        reports: &mpsc::UnboundedSender<Report>,
    ) -> Self {
        let capabilities = sink.capabilities();
        let parked = Arc::new(AtomicU64::new(0));
        let gauges = ParkedGauges::new(settings.lanes.max(1), Arc::clone(&parked));
        let mut running = JoinSet::new();
        let inboxes: Vec<_> = (0..settings.lanes.max(1))
            .map(|index| {
                let (inbox, receiver) = mpsc::unbounded_channel();
                let lane = Lane {
                    index,
                    sink: Arc::clone(sink),
                    store: Arc::clone(&stores.state),
                    payload: stores.payload.clone(),
                    to_offload: VecDeque::new(),
                    permits: Arc::clone(permits),
                    capabilities,
                    inbox: receiver,
                    reports: reports.clone(),
                    keys: HashMap::new(),
                    ready: VecDeque::new(),
                    retries: BinaryHeap::new(),
                    last: None,
                    loaded: [false; PARTITIONS],
                    to_append: BTreeSet::new(),
                    to_load: VecDeque::new(),
                    to_remove: Vec::new(),
                    to_kill: Vec::new(),
                    heads_in_memory: 0,
                    store_turn: 0,
                    store_backoff: Backoff::new(RETRY_BASE, RETRY_CAP),
                    store_due: None,
                    append_due: None,
                    park_after: settings.park_after,
                    max_parked: settings.max_parked_events,
                    parked: Arc::clone(&parked),
                    gauges: Arc::clone(&gauges),
                    stats_dirty: false,
                    stats_due: None,
                    operations: VecDeque::new(),
                    closing: false,
                };
                running.spawn(lane.run());
                inbox
            })
            .collect();
        let mut loader = JoinSet::new();
        loader.spawn(loader::load(
            Arc::clone(&stores.state),
            inboxes.clone(),
            settings.store_concurrency,
            parked,
        ));
        Self {
            operation_queues: operate::operation_queues(inboxes.len()),
            inboxes,
            running,
            loader,
            gauges,
        }
    }

    pub fn operator(&self) -> Operator {
        Operator::new(
            self.inboxes
                .iter()
                .map(mpsc::UnboundedSender::downgrade)
                .collect(),
            self.operation_queues.clone(),
        )
    }

    pub fn gauges(&self) -> Arc<ParkedGauges> {
        Arc::clone(&self.gauges)
    }

    pub fn dispatch(&self, pending: Pending) -> Result<(), LaneStopped> {
        let index = pending.outbound.order_key.lane(self.inboxes.len());
        let inbox = self.inboxes.get(index).ok_or(LaneStopped(index))?;
        inbox
            .send(Inbound::Event(pending))
            .map_err(|_| LaneStopped(index))
    }

    pub fn park_all(&self) {
        for inbox in &self.inboxes {
            let _ = inbox.send(Inbound::ParkAll);
        }
    }

    pub async fn stop(self, grace: Duration) -> bool {
        let Self {
            inboxes,
            operation_queues: _,
            mut running,
            mut loader,
            gauges: _,
        } = self;
        loader.shutdown().await;
        drop(inboxes);
        let exited = tokio::time::timeout(grace, async {
            while running.join_next().await.is_some() {}
        })
        .await
        .is_ok();
        if !exited {
            running.shutdown().await;
        }
        exited
    }
}

#[derive(Debug)]
struct Key {
    queue: VecDeque<Pending>,
    backoff: Backoff,
    failure: Option<Failure>,
    stored: Option<Stored>,
    offloaded: Option<Outbound>,
    reserved: u64,
}

impl Key {
    fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            backoff: Backoff::new(RETRY_BASE, RETRY_CAP),
            failure: None,
            stored: None,
            offloaded: None,
            reserved: 0,
        }
    }
}

#[derive(Debug, Clone)]
struct Failure {
    since: Instant,
    since_ms: u64,
    attempts: u32,
    error: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Kill {
    key: OrderKeyId,
    id: EventId,
    labels: Option<(String, String)>,
}

impl Kill {
    fn of(key: OrderKeyId, head: &Outbound) -> Self {
        let labels = metrics::enabled().then(|| {
            let (table, op) = metric_labels(&head.attributes);
            (table.to_owned(), op.to_owned())
        });
        Self {
            key,
            id: head.id,
            labels,
        }
    }

    fn is(&self, key: OrderKeyId, id: EventId) -> bool {
        self.key == key && self.id == id
    }
}

#[derive(Debug, Default)]
struct Stored {
    floor: Option<EventId>,
    current: Option<EventId>,
    head: Option<Outbound>,
    dead: bool,
    dead_on_append: bool,
    since_ms: Option<u64>,
}

struct Lane {
    index: usize,
    sink: Arc<dyn Sink>,
    store: Arc<dyn StateStore>,
    payload: Option<Arc<dyn PayloadStore>>,
    to_offload: VecDeque<OrderKeyId>,
    permits: Arc<Semaphore>,
    capabilities: SinkCapabilities,
    inbox: mpsc::UnboundedReceiver<Inbound>,
    reports: mpsc::UnboundedSender<Report>,
    keys: HashMap<OrderKeyId, Key>,
    ready: VecDeque<OrderKeyId>,
    retries: BinaryHeap<Reverse<(Instant, OrderKeyId)>>,
    last: Option<EventId>,
    loaded: [bool; PARTITIONS],
    to_append: BTreeSet<OrderKeyId>,
    to_load: VecDeque<OrderKeyId>,
    to_remove: Vec<(OrderKeyId, EventId)>,
    to_kill: Vec<Kill>,
    heads_in_memory: usize,
    store_turn: usize,
    store_backoff: Backoff,
    store_due: Option<Instant>,
    append_due: Option<Instant>,
    park_after: Duration,
    max_parked: u64,
    parked: Arc<AtomicU64>,
    gauges: Arc<ParkedGauges>,
    stats_dirty: bool,
    stats_due: Option<Instant>,
    operations: VecDeque<Operation>,
    closing: bool,
}

impl Lane {
    async fn run(mut self) {
        loop {
            while let Ok(inbound) = self.inbox.try_recv() {
                if !self.accept(inbound) {
                    return;
                }
            }
            self.perform_operations().await;
            let now = Instant::now();
            self.publish_stats(now);
            self.promote_due_retries(now);
            let stored = self.store_work(now).await;
            if self.closing {
                if stored {
                    continue;
                }
                let until = [self.store_wake(), self.stats_wake(), self.operations_wake()]
                    .into_iter()
                    .flatten()
                    .min();
                if !self.wait(until).await {
                    return;
                }
                continue;
            }
            if !self.to_offload.is_empty() {
                self.offload().await;
                continue;
            }
            let (count, full) = self.plan_batch();
            let linger = self.linger_deadline();
            if let Some(deadline) = linger
                && (full || Instant::now() >= deadline)
            {
                self.send_batch(count).await;
                continue;
            }
            if stored {
                continue;
            }
            let next_retry = self.retries.peek().map(|Reverse((due, _))| *due);
            let until = [
                linger,
                next_retry,
                self.store_wake(),
                self.stats_wake(),
                self.operations_wake(),
            ]
            .into_iter()
            .flatten()
            .min();
            if !self.wait(until).await {
                return;
            }
        }
    }

    fn publish_stats(&mut self, now: Instant) {
        if !self.stats_dirty || self.stats_due.is_some_and(|due| due > now) {
            return;
        }
        let mut counts = LaneCounts::default();
        for stored in self.keys.values().filter_map(|state| state.stored.as_ref()) {
            counts.keys += 1;
            if stored.dead {
                counts.dead += 1;
            }
            counts.oldest_ms = match (counts.oldest_ms, stored.since_ms) {
                (Some(known), Some(since)) => Some(known.min(since)),
                (known, since) => known.or(since),
            };
        }
        self.gauges.publish(self.index, counts);
        self.stats_dirty = false;
        self.stats_due = Some(now + STATS_INTERVAL);
    }

    fn stats_wake(&self) -> Option<Instant> {
        self.stats_dirty.then_some(self.stats_due).flatten()
    }

    async fn publishing<F: Future>(&mut self, io: F) -> F::Output {
        let mut io = pin!(io);
        loop {
            self.publish_stats(Instant::now());
            let Some(due) = self.stats_wake() else {
                return io.await;
            };
            tokio::select! {
                biased;
                output = &mut io => return output,
                () = tokio::time::sleep_until(due) => {}
            }
        }
    }

    fn accept(&mut self, inbound: Inbound) -> bool {
        match inbound {
            Inbound::Event(pending) => self.accept_event(pending),
            Inbound::Head(head) => {
                self.accept_head(head);
                true
            }
            Inbound::Loaded(partition) => {
                self.partition_loaded(partition);
                true
            }
            Inbound::ParkAll => {
                self.park_all();
                true
            }
            Inbound::Operate(operation) => {
                self.operations.push_back(operation);
                true
            }
        }
    }

    fn accept_event(&mut self, pending: Pending) -> bool {
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
            Entry::Occupied(mut entry) => {
                let state = entry.get_mut();
                if let Some(stored) = &state.stored {
                    if let Some(floor) = stored.floor
                        && id < floor
                    {
                        if let Ok(settled) = pending.ticket.skip_replayed(id, floor) {
                            let _ = self.reports.send(Report::Settled(settled));
                        }
                        return true;
                    }
                    self.to_append.insert(key);
                }
                state.queue.push_back(pending);
            }
            Entry::Vacant(entry) => {
                let mut state = Key::new();
                state.queue.push_back(pending);
                entry.insert(state);
                if self.closing && self.is_loaded(key) {
                    self.start_parking(key, false);
                } else {
                    self.make_ready(key);
                }
            }
        }
        true
    }

    fn is_loaded(&self, key: OrderKeyId) -> bool {
        self.loaded
            .get(usize::from(key.partition()))
            .copied()
            .unwrap_or(false)
    }

    fn make_ready(&mut self, key: OrderKeyId) {
        if self.closing || !self.is_loaded(key) {
            return;
        }
        let Some(bytes) = self.front(&key).map(Outbound::encoded_len) else {
            return;
        };
        if bytes > self.capabilities.max_message_bytes {
            self.oversize(key, bytes);
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
            match self.keys.get(&key) {
                Some(Key {
                    stored: Some(_), ..
                }) => self.to_load.push_back(key),
                Some(state) if self.park_due(state, now) => self.start_parking(key, false),
                Some(_) => self.make_ready(key),
                None => {}
            }
        }
    }

    fn front(&self, key: &OrderKeyId) -> Option<&Outbound> {
        let state = self.keys.get(key)?;
        match &state.stored {
            Some(stored) => stored.head.as_ref(),
            None => state
                .offloaded
                .as_ref()
                .or_else(|| state.queue.front().map(|pending| &pending.outbound)),
        }
    }

    fn linger_deadline(&self) -> Option<Instant> {
        let key = self.ready.front()?;
        let state = self.keys.get(key)?;
        match &state.stored {
            Some(_) => Some(Instant::now()),
            None => Some(state.queue.front()?.arrived + LINGER),
        }
    }

    fn plan_batch(&self) -> (usize, bool) {
        let limits = self.capabilities;
        let mut count = 0;
        let mut bytes = 0_usize;
        for key in &self.ready {
            let Some(head) = self.front(key) else {
                continue;
            };
            let size = head.encoded_len();
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
                Some(inbound) => self.accept(inbound),
                None => false,
            },
            () = timer => true,
        }
    }

    async fn send_batch(&mut self, count: usize) {
        let mut keys = Vec::with_capacity(count);
        let mut batch = Vec::with_capacity(count);
        for key in self.ready.drain(..count).collect::<Vec<_>>() {
            if let Some(head) = self.front(&key) {
                batch.push(head.clone());
                keys.push(key);
            }
        }
        let (permits, sink) = (Arc::clone(&self.permits), Arc::clone(&self.sink));
        let span = tracing::debug_span!(
            "sink_send",
            sink = sink.name(),
            size = u64::try_from(batch.len()).unwrap_or(u64::MAX),
            delivered = tracing::field::Empty,
            retryable = tracing::field::Empty,
            terminal = tracing::field::Empty,
        );
        let (mut outcomes, elapsed) = self
            .publishing(async {
                match permits.acquire().await {
                    Ok(_permit) => {
                        let started = metrics::enabled().then(Instant::now);
                        let outcomes = sink.send(&batch).instrument(span.clone()).await;
                        (outcomes, started.map(|started| started.elapsed()))
                    }
                    Err(_) => (Vec::new(), None),
                }
            })
            .await;
        if outcomes.len() != batch.len() {
            tracing::error!(
                sent = batch.len(),
                outcomes = outcomes.len(),
                "the sink broke its contract of one outcome per message"
            );
        }
        outcomes.resize_with(keys.len(), || {
            Outcome::Retryable(SinkError("the sink returned no outcome".to_owned()))
        });
        record_sink_call(&span, &outcomes, elapsed);
        for (key, outcome) in keys.into_iter().zip(&outcomes) {
            self.complete(key, outcome);
        }
    }

    fn complete(&mut self, key: OrderKeyId, outcome: &Outcome) {
        let Some(state) = self.keys.get_mut(&key) else {
            return;
        };
        if state.stored.is_some() {
            self.complete_stored(key, outcome);
            return;
        }
        match outcome {
            Outcome::Delivered => {
                state.offloaded = None;
                let Some(head) = state.queue.pop_front() else {
                    return;
                };
                record_delivered(&head.outbound, head.committed_ms);
                if let Ok(settled) = head.ticket.settle(outcome) {
                    let _ = self.reports.send(Report::Settled(settled));
                }
                if state.queue.is_empty() {
                    self.keys.remove(&key);
                } else {
                    state.backoff.reset();
                    state.failure = None;
                    self.make_ready(key);
                }
            }
            Outcome::Retryable(error) => self.retry_live(key, error),
            Outcome::Terminal(error) => {
                let id = state.queue.front().map(|head| head.outbound.id);
                tracing::error!(order_key = %key, ?id, %error, "the sink rejected the event for good; parking it dead");
                state.record_failure(error);
                self.start_parking(key, true);
            }
        }
    }
}

fn record_delivered(outbound: &Outbound, committed_ms: Option<u64>) {
    if !metrics::enabled() {
        return;
    }
    let (table, op) = metric_labels(&outbound.attributes);
    metrics::record(Measurement::Event {
        table,
        op,
        outcome: EventOutcome::Delivered,
        count: 1,
    });
    if let Some(committed_ms) = committed_ms {
        metrics::record(Measurement::CommitToDelivery(Duration::from_millis(
            now_ms().saturating_sub(committed_ms),
        )));
    }
}

fn record_sink_call(span: &tracing::Span, outcomes: &[Outcome], elapsed: Option<Duration>) {
    if span.is_disabled() && elapsed.is_none() {
        return;
    }
    let (mut delivered, mut retryable, mut terminal) = (0_u64, 0_u64, 0_u64);
    for outcome in outcomes {
        match outcome {
            Outcome::Delivered => delivered += 1,
            Outcome::Retryable(_) => retryable += 1,
            Outcome::Terminal(_) => terminal += 1,
        }
    }
    span.record("delivered", delivered);
    span.record("retryable", retryable);
    span.record("terminal", terminal);
    if let Some(elapsed) = elapsed {
        let outcome = if terminal > 0 {
            SinkOutcome::Terminal
        } else if retryable > 0 {
            SinkOutcome::Retryable
        } else {
            SinkOutcome::Delivered
        };
        metrics::record(Measurement::Sink { outcome, elapsed });
    }
}
