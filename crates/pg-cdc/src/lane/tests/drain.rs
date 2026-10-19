use super::*;
use crate::config::{DEFAULT_LANES, DEFAULT_SINK_CONCURRENCY, DEFAULT_STORE_CONCURRENCY};

const REQUEST: Duration = Duration::from_millis(20);
const KEYS: u32 = 1_000;
const EVENTS_PER_KEY: u32 = 20;

#[derive(Debug)]
struct ModelS3 {
    inner: MemoryStateStore,
    permits: Arc<Semaphore>,
}

impl ModelS3 {
    fn new(concurrency: usize) -> Self {
        Self {
            inner: MemoryStateStore::new(),
            permits: Arc::new(Semaphore::new(concurrency)),
        }
    }

    async fn requests(&self, count: usize) {
        let mut running = JoinSet::new();
        for _ in 0..count {
            let permits = Arc::clone(&self.permits);
            running.spawn(async move {
                let _permit = permits.acquire_owned().await;
                tokio::time::sleep(REQUEST).await;
            });
        }
        while running.join_next().await.is_some() {}
    }

    async fn serial_requests(&self, count: usize) {
        for _ in 0..count {
            self.requests(1).await;
        }
    }
}

#[async_trait]
impl StateStore for ModelS3 {
    async fn append_parked(&self, events: &[ParkedEvent]) -> Result<(), StoreError> {
        let mut per_key: HashMap<OrderKeyId, usize> = HashMap::new();
        for event in events {
            *per_key.entry(event.order_key).or_default() += 1;
        }
        let longest = per_key.values().copied().max().unwrap_or(0);
        for round in 0..longest {
            let writes = per_key.values().filter(|count| **count > round).count();
            self.requests(writes).await;
        }
        self.inner.append_parked(events).await
    }

    async fn load_parked_heads(&self, partition: u8) -> Result<Vec<ParkedHead>, StoreError> {
        self.requests(1).await;
        self.inner.load_parked_heads(partition).await
    }

    async fn list_parked_queues(
        &self,
        after: Option<&OrderKeyId>,
        limit: usize,
        length_cap: usize,
    ) -> Result<ParkedQueues, StoreError> {
        self.requests(1).await;
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
        self.requests(1).await;
        let events = self.inner.read_parked(key, after, limit).await?;
        self.requests(events.len()).await;
        Ok(events)
    }

    async fn read_parked_within(
        &self,
        key: &OrderKeyId,
        after: Option<&EventId>,
        limit: usize,
        max_bytes: u64,
    ) -> Result<ParkedSlice, StoreError> {
        self.requests(1).await;
        self.inner
            .read_parked_within(key, after, limit, max_bytes)
            .await
    }

    async fn read_parked_summary(
        &self,
        key: &OrderKeyId,
    ) -> Result<Option<ParkedSummary>, StoreError> {
        self.requests(1).await;
        self.inner.read_parked_summary(key).await
    }

    async fn set_parked_status(
        &self,
        key: &OrderKeyId,
        id: &EventId,
        status: ParkStatus,
    ) -> Result<(), StoreError> {
        self.serial_requests(2).await;
        self.inner.set_parked_status(key, id, status).await
    }

    async fn remove_parked(&self, events: &[(OrderKeyId, EventId)]) -> Result<(), StoreError> {
        self.requests(events.len()).await;
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

fn backlog_key(n: u32) -> Vec<u8> {
    format!("backlog-{n}").into_bytes()
}

struct Backlog {
    lanes: Lanes,
    store: Arc<ModelS3>,
    ack: AckTracker,
    _reports: mpsc::UnboundedReceiver<Report>,
    next: u32,
}

impl Backlog {
    async fn start(store_concurrency: usize) -> Self {
        let store = ModelS3::new(store_concurrency);
        for key in 0..KEYS {
            let events: Vec<ParkedEvent> = (0..EVENTS_PER_KEY)
                .map(|n| {
                    parked(
                        &backlog_key(key),
                        key * EVENTS_PER_KEY + n,
                        ParkStatus::Retrying,
                    )
                })
                .collect();
            store.inner.append_parked(&events).await.unwrap();
        }
        let store = Arc::new(store);
        let mut sink = ScriptedSink::delivering();
        sink.latency = REQUEST;
        let sink: Arc<dyn Sink> = Arc::new(sink);
        let (reports, receiver) = mpsc::unbounded_channel();
        let lanes = Lanes::spawn(
            LaneSettings {
                lanes: DEFAULT_LANES,
                park_after: Duration::from_secs(30),
                max_parked_events: 0,
                store_concurrency,
            },
            &sink,
            &Stores {
                state: store.clone(),
                payload: None,
            },
            &Arc::new(Semaphore::new(DEFAULT_SINK_CONCURRENCY)),
            &reports,
        );
        Self {
            lanes,
            store,
            ack: AckTracker::new(Lsn(0)),
            _reports: receiver,
            next: KEYS * EVENTS_PER_KEY,
        }
    }

    fn parked(&self) -> usize {
        self.store.inner.parked_events()
    }

    fn write(&mut self, key: u32) {
        self.ack.begin().unwrap();
        let ticket = self.ack.track(10).unwrap();
        self.ack.commit(Lsn(u64::from(self.next))).unwrap();
        self.lanes
            .dispatch(Pending {
                outbound: Outbound {
                    id: EventId::new(Lsn(0x10), self.next),
                    order_key: OrderKeyId::of(&backlog_key(key)),
                    attributes: Vec::new(),
                    body: Bytes::from_static(b"{}"),
                },
                ticket,
                arrived: Instant::now(),
                committed_ms: None,
            })
            .unwrap();
        self.next += 1;
    }
}

async fn drain_time(store_concurrency: usize, limit: Duration) -> Option<Duration> {
    let backlog = Backlog::start(store_concurrency).await;
    let started = Instant::now();
    tokio::time::timeout(limit, async {
        while backlog.parked() > 0 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .ok()
    .map(|()| started.elapsed())
}

async fn parked_after_writing(store_concurrency: usize, seconds: u32) -> usize {
    let mut backlog = Backlog::start(store_concurrency).await;
    let mut key = 0;
    for _ in 0..seconds * 100 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        for _ in 0..10 {
            backlog.write(key);
            key = (key + 1) % KEYS;
        }
    }
    backlog.parked()
}

#[tokio::test(start_paused = true)]
async fn the_default_store_concurrency_drains_a_parked_backlog_within_thirty_seconds() {
    let limit = Duration::from_secs(30);
    let drained = drain_time(DEFAULT_STORE_CONCURRENCY, limit).await;
    assert!(
        drained.is_some(),
        "20,000 parked events took over {limit:?}"
    );
    assert_eq!(
        drain_time(32, limit).await,
        None,
        "the model is not store-bound"
    );
}

#[tokio::test(start_paused = true)]
async fn the_parked_backlog_falls_while_new_events_keep_arriving() {
    let start = usize::try_from(KEYS * EVENTS_PER_KEY).unwrap();
    let parked = parked_after_writing(DEFAULT_STORE_CONCURRENCY, 30).await;
    assert!(parked < start / 4, "{parked} of {start} still parked");
    let parked = parked_after_writing(32, 30).await;
    assert!(
        parked >= start,
        "{parked} parked at 32: the model is not store-bound"
    );
}
