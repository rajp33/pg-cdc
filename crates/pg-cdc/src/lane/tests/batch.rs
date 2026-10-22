use super::*;
use crate::lane::batch::{Batcher, BatchingSink};
use tokio::task::JoinHandle;

fn entry(ordinal: u32, key: &[u8], body: usize) -> Outbound {
    Outbound {
        id: EventId::new(Lsn(0x10), ordinal),
        order_key: OrderKeyId::of(key),
        attributes: Vec::new(),
        body: Bytes::from(vec![b'x'; body]),
    }
}

struct Rig {
    sink: Arc<ScriptedSink>,
    batching: Arc<BatchingSink>,
    _batcher: JoinSet<()>,
}

impl Rig {
    fn new(limits: SinkCapabilities, permits: usize) -> Self {
        Self::with(
            ScriptedSink::new(limits, Box::new(|_, _| Outcome::Delivered)),
            permits,
        )
    }

    fn with(sink: ScriptedSink, permits: usize) -> Self {
        let sink = Arc::new(sink);
        let inner: Arc<dyn Sink> = sink.clone();
        let (batching, batcher): (Arc<BatchingSink>, Batcher) =
            BatchingSink::new(inner, Arc::new(Semaphore::new(permits)));
        let mut running = JoinSet::new();
        running.spawn(batcher.run());
        Self {
            sink,
            batching,
            _batcher: running,
        }
    }

    fn submit(&self, entries: Vec<Outbound>) -> JoinHandle<Vec<Outcome>> {
        let batching = Arc::clone(&self.batching);
        tokio::spawn(async move { batching.send(&entries).await })
    }
}

#[derive(Debug)]
struct NamedSink;

#[async_trait]
impl Sink for NamedSink {
    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities {
            max_batch_messages: 3,
            max_batch_bytes: 7,
            max_message_bytes: 5,
        }
    }

    fn name(&self) -> &'static str {
        "named"
    }

    async fn preflight(&self) -> Result<(), SinkError> {
        Err(SinkError("preflight".to_owned()))
    }

    async fn send(&self, batch: &[Outbound]) -> Vec<Outcome> {
        vec![Outcome::Delivered; batch.len()]
    }
}

#[tokio::test(start_paused = true)]
async fn submissions_from_two_submitters_within_the_linger_share_one_call() {
    let rig = Rig::new(LIMITS, 1);
    let start = Instant::now();
    let first = rig.submit(vec![entry(0, b"a", 10)]);
    tokio::time::sleep(Duration::from_millis(2)).await;
    let second = rig.submit(vec![entry(1, b"b", 10)]);
    assert_eq!(first.await.unwrap(), vec![Outcome::Delivered]);
    assert_eq!(second.await.unwrap(), vec![Outcome::Delivered]);
    assert_eq!(rig.sink.batches(), vec![vec![0, 1]]);
    assert_eq!(rig.sink.sends()[0].at, start + LINGER);
}

#[tokio::test(start_paused = true)]
async fn a_full_batch_flushes_at_once_without_lingering() {
    let limits = SinkCapabilities {
        max_batch_messages: 2,
        ..LIMITS
    };
    let rig = Rig::new(limits, 1);
    let start = Instant::now();
    let first = rig.submit(vec![entry(0, b"a", 10)]);
    let second = rig.submit(vec![entry(1, b"b", 10)]);
    first.await.unwrap();
    second.await.unwrap();
    assert_eq!(rig.sink.batches(), vec![vec![0, 1]]);
    assert_eq!(rig.sink.sends()[0].at, start);
}

#[tokio::test(start_paused = true)]
async fn batches_respect_the_message_and_byte_limits_and_an_entry_over_the_byte_limit_goes_alone() {
    let limits = SinkCapabilities {
        max_batch_messages: 2,
        ..LIMITS
    };
    let rig = Rig::new(limits, 1);
    let entries = [b"a", b"b", b"c", b"d", b"e"]
        .into_iter()
        .enumerate()
        .map(|(ordinal, key)| entry(u32::try_from(ordinal).unwrap(), key, 10))
        .collect();
    rig.submit(entries).await.unwrap();
    assert_eq!(rig.sink.batches(), vec![vec![0, 1], vec![2, 3], vec![4]]);

    let limits = SinkCapabilities {
        max_batch_bytes: 100,
        ..LIMITS
    };
    let rig = Rig::new(limits, 1);
    let entries = vec![
        entry(0, b"a", 150),
        entry(1, b"b", 60),
        entry(2, b"c", 40),
        entry(3, b"d", 60),
    ];
    rig.submit(entries).await.unwrap();
    assert_eq!(rig.sink.batches(), vec![vec![0], vec![1, 2], vec![3]]);
}

#[tokio::test(start_paused = true)]
async fn a_partial_batch_flushes_when_its_oldest_entry_has_waited_the_linger() {
    let rig = Rig::new(LIMITS, 1);
    let start = Instant::now();
    let first = rig.submit(vec![entry(0, b"a", 10)]);
    tokio::time::sleep(Duration::from_millis(3)).await;
    let second = rig.submit(vec![entry(1, b"b", 10)]);
    first.await.unwrap();
    second.await.unwrap();
    let later = Instant::now() + Duration::from_secs(1);
    tokio::time::sleep_until(later).await;
    rig.submit(vec![entry(2, b"c", 10)]).await.unwrap();
    let sends = rig.sink.sends();
    assert_eq!(rig.sink.batches(), vec![vec![0, 1], vec![2]]);
    assert_eq!(sends[0].at, start + LINGER);
    assert_eq!(sends[2].at, later + LINGER);
}

#[tokio::test(start_paused = true)]
async fn a_submission_split_across_two_calls_gets_one_reply_in_submitted_order_after_both_calls() {
    let script: Script = Box::new(|ordinal, _| {
        if ordinal % 2 == 1 {
            Outcome::Retryable(SinkError(ordinal.to_string()))
        } else {
            Outcome::Delivered
        }
    });
    let limits = SinkCapabilities {
        max_batch_messages: 2,
        ..LIMITS
    };
    let mut sink = ScriptedSink::new(limits, script);
    sink.latency = Duration::from_millis(10);
    let rig = Rig::with(sink, 2);
    let start = Instant::now();
    let reply = rig
        .submit(vec![
            entry(0, b"a", 10),
            entry(1, b"b", 10),
            entry(2, b"c", 10),
        ])
        .await
        .unwrap();
    assert_eq!(Instant::now() - start, LINGER + Duration::from_millis(10));
    assert_eq!(
        reply,
        vec![
            Outcome::Delivered,
            Outcome::Retryable(SinkError("1".to_owned())),
            Outcome::Delivered,
        ]
    );
    assert_eq!(rig.sink.batches(), vec![vec![0, 1], vec![2]]);
}

#[tokio::test(start_paused = true)]
async fn entries_go_out_in_the_order_they_were_submitted_across_submitters() {
    let rig = Rig::new(LIMITS, 1);
    let first = rig.submit(vec![entry(0, b"a", 10), entry(1, b"b", 10)]);
    tokio::time::sleep(Duration::from_millis(1)).await;
    let second = rig.submit(vec![entry(2, b"c", 10)]);
    tokio::time::sleep(Duration::from_millis(1)).await;
    let third = rig.submit(vec![entry(3, b"d", 10), entry(4, b"e", 10)]);
    first.await.unwrap();
    second.await.unwrap();
    third.await.unwrap();
    assert_eq!(rig.sink.batches(), vec![vec![0, 1, 2, 3, 4]]);
}

#[tokio::test(start_paused = true)]
async fn an_empty_send_returns_no_outcomes_and_makes_no_call() {
    let rig = Rig::new(LIMITS, 1);
    assert_eq!(rig.batching.send(&[]).await, vec![]);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(rig.sink.batches(), Vec::<Vec<u32>>::new());
}

#[tokio::test(start_paused = true)]
async fn name_capabilities_and_preflight_come_from_the_inner_sink() {
    let inner: Arc<dyn Sink> = Arc::new(NamedSink);
    let (batching, _batcher) = BatchingSink::new(Arc::clone(&inner), Arc::new(Semaphore::new(1)));
    assert_eq!(batching.name(), "named");
    assert_eq!(batching.capabilities(), inner.capabilities());
    assert_eq!(
        batching.preflight().await,
        Err(SinkError("preflight".to_owned()))
    );
}
