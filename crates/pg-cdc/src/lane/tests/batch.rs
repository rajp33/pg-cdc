use super::metrics::{Seen, recording, sink_points};
use super::*;
use crate::capture::Capture;
use crate::lane::batch::{Batcher, BatchingSink, record_sink_call};
use crate::leader::gate::{GatedSink, WriteGate};
use crate::metrics::{ErrorComponent, ErrorKind, SinkOutcome};
use tokio::task::JoinHandle;
use tracing::Level;
use tracing::level_filters::LevelFilter;

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
    batcher: JoinSet<()>,
}

impl Rig {
    fn new(limits: SinkCapabilities, permits: usize) -> Self {
        Self::with(
            ScriptedSink::new(limits, Box::new(|_, _| Outcome::Delivered)),
            Arc::new(Semaphore::new(permits)),
        )
    }

    fn with(sink: ScriptedSink, permits: Arc<Semaphore>) -> Self {
        let sink = Arc::new(sink);
        let inner: Arc<dyn Sink> = sink.clone();
        let (batching, batcher): (Arc<BatchingSink>, Batcher) = BatchingSink::new(inner, permits);
        let mut running = JoinSet::new();
        running.spawn(batcher.run());
        Self {
            sink,
            batching,
            batcher: running,
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
    let rig = Rig::with(sink, Arc::new(Semaphore::new(2)));
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

fn retryable(message: &str) -> Outcome {
    Outcome::Retryable(SinkError(message.to_owned()))
}

#[derive(Debug)]
struct FixedSink(Vec<Outcome>);

#[async_trait]
impl Sink for FixedSink {
    fn capabilities(&self) -> SinkCapabilities {
        LIMITS
    }

    async fn preflight(&self) -> Result<(), SinkError> {
        Ok(())
    }

    async fn send(&self, _batch: &[Outbound]) -> Vec<Outcome> {
        self.0.clone()
    }
}

#[tokio::test(start_paused = true)]
async fn concurrent_calls_never_exceed_the_permits() {
    let limits = SinkCapabilities {
        max_batch_messages: 1,
        ..LIMITS
    };
    for (permits, expected) in [(1, 1), (3, 3)] {
        let mut sink = ScriptedSink::new(limits, Box::new(|_, _| Outcome::Delivered));
        sink.latency = Duration::from_millis(50);
        let rig = Rig::with(sink, Arc::new(Semaphore::new(permits)));
        let submitted: Vec<_> = (0..6)
            .map(|ordinal| rig.submit(vec![entry(ordinal, b"k", 10)]))
            .collect();
        for submission in submitted {
            submission.await.unwrap();
        }
        assert_eq!(rig.sink.batches().len(), 6);
        assert_eq!(rig.sink.most_active.load(Ordering::SeqCst), expected);
    }
}

#[tokio::test(start_paused = true)]
async fn a_due_batch_keeps_filling_while_it_waits_for_a_permit() {
    let mut sink = ScriptedSink::delivering();
    sink.latency = Duration::from_millis(50);
    let rig = Rig::with(sink, Arc::new(Semaphore::new(1)));
    let start = Instant::now();
    let first = rig.submit(vec![entry(0, b"a", 10)]);
    tokio::time::sleep(Duration::from_millis(10)).await;
    let second = rig.submit(vec![entry(1, b"b", 10)]);
    tokio::time::sleep(Duration::from_millis(20)).await;
    let third = rig.submit(vec![entry(2, b"c", 10)]);
    tokio::time::sleep(Duration::from_millis(20)).await;
    let fourth = rig.submit(vec![entry(3, b"d", 10)]);
    for submission in [first, second, third, fourth] {
        submission.await.unwrap();
    }
    assert_eq!(rig.sink.batches(), vec![vec![0], vec![1, 2, 3]]);
    assert_eq!(
        rig.sink.sends()[1].at,
        start + LINGER + Duration::from_millis(100)
    );
}

#[tokio::test(start_paused = true)]
async fn a_closed_semaphore_answers_retryable_without_calling_the_sink() {
    let (capture, _guard) = Capture::install(LevelFilter::ERROR);
    let permits = Arc::new(Semaphore::new(1));
    permits.close();
    let rig = Rig::with(ScriptedSink::delivering(), permits);
    let reply = rig
        .submit(vec![entry(0, b"a", 10), entry(1, b"b", 10)])
        .await
        .unwrap();
    assert_eq!(
        reply,
        vec![
            retryable("the sink is closed"),
            retryable("the sink is closed")
        ]
    );
    assert_eq!(rig.sink.batches(), Vec::<Vec<u32>>::new());
    assert_eq!(capture.events(), vec![]);
}

#[tokio::test(start_paused = true)]
async fn unsent_entries_of_a_dropped_submission_never_reach_the_sink() {
    let mut sink = ScriptedSink::delivering();
    sink.latency = Duration::from_millis(50);
    let rig = Rig::with(sink, Arc::new(Semaphore::new(1)));
    let first = rig.submit(vec![entry(0, b"a", 10)]);
    tokio::time::sleep(Duration::from_millis(10)).await;
    let dropped = rig.submit(vec![entry(1, b"b", 10), entry(2, b"c", 10)]);
    tokio::time::sleep(Duration::from_millis(10)).await;
    dropped.abort();
    first.await.unwrap();
    let last = rig.submit(vec![entry(3, b"d", 10)]);
    last.await.unwrap();
    assert_eq!(rig.sink.batches(), vec![vec![0], vec![3]]);
}

#[tokio::test(start_paused = true)]
async fn a_dropped_submitter_does_not_disturb_the_outcomes_of_others_in_its_call() {
    let script: Script = Box::new(|ordinal, _| {
        if ordinal == 1 {
            retryable("throttled")
        } else {
            Outcome::Delivered
        }
    });
    let mut sink = ScriptedSink::new(LIMITS, script);
    sink.latency = Duration::from_millis(50);
    let rig = Rig::with(sink, Arc::new(Semaphore::new(1)));
    let dropped = rig.submit(vec![entry(0, b"a", 10)]);
    let kept = rig.submit(vec![entry(1, b"b", 10), entry(2, b"c", 10)]);
    tokio::time::sleep(Duration::from_millis(20)).await;
    dropped.abort();
    assert_eq!(
        kept.await.unwrap(),
        vec![retryable("throttled"), Outcome::Delivered]
    );
    assert_eq!(rig.sink.batches(), vec![vec![0, 1, 2]]);
}

#[tokio::test(start_paused = true)]
async fn missing_outcomes_from_the_inner_sink_become_retryable_and_extras_are_ignored() {
    let (capture, _guard) = Capture::install(LevelFilter::ERROR);
    let entries = || vec![entry(0, b"a", 10), entry(1, b"b", 10), entry(2, b"c", 10)];
    let short: Arc<dyn Sink> = Arc::new(FixedSink(vec![Outcome::Delivered]));
    let (batching, batcher) = BatchingSink::new(short, Arc::new(Semaphore::new(1)));
    let mut running = JoinSet::new();
    running.spawn(batcher.run());
    assert_eq!(
        batching.send(&entries()).await,
        vec![
            Outcome::Delivered,
            retryable("the sink returned no outcome"),
            retryable("the sink returned no outcome"),
        ]
    );
    let long: Arc<dyn Sink> = Arc::new(FixedSink(vec![Outcome::Delivered; 5]));
    let (batching, batcher) = BatchingSink::new(long, Arc::new(Semaphore::new(1)));
    running.spawn(batcher.run());
    assert_eq!(batching.send(&entries()).await, vec![Outcome::Delivered; 3]);
    let messages: Vec<String> = capture
        .events()
        .into_iter()
        .map(|event| event.message)
        .collect();
    assert_eq!(
        messages,
        vec!["the sink broke its contract of one outcome per message"; 2]
    );
}

#[tokio::test(start_paused = true)]
async fn a_send_after_the_batcher_has_stopped_returns_retryable_for_every_entry() {
    let mut sink = ScriptedSink::delivering();
    sink.latency = Duration::from_secs(3600);
    let mut rig = Rig::with(sink, Arc::new(Semaphore::new(1)));
    let waiting = rig.submit(vec![entry(0, b"a", 10)]);
    tokio::time::sleep(Duration::from_millis(10)).await;
    rig.batcher.shutdown().await;
    let stopped = || retryable("the sink batcher has stopped");
    assert_eq!(waiting.await.unwrap(), vec![stopped()]);
    assert_eq!(
        rig.batching
            .send(&[entry(1, b"b", 10), entry(2, b"c", 10)])
            .await,
        vec![stopped(), stopped()]
    );
}

#[tokio::test(start_paused = true)]
async fn aborting_the_batcher_drops_its_calls_in_flight_and_counts_them_as_abandoned_writes() {
    let mut slow = ScriptedSink::delivering();
    slow.latency = Duration::from_secs(3600);
    let gate = WriteGate::new();
    gate.open();
    let inner: Arc<dyn Sink> = Arc::new(GatedSink::new(Arc::new(slow), Arc::clone(&gate)));
    let (batching, batcher) = BatchingSink::new(inner, Arc::new(Semaphore::new(1)));
    let mut running = JoinSet::new();
    running.spawn(batcher.run());
    let submission = tokio::spawn(async move { batching.send(&[entry(0, b"a", 10)]).await });
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(gate.abandoned_writes(), 0);
    running.shutdown().await;
    assert_eq!(gate.abandoned_writes(), 1);
    assert_eq!(
        submission.await.unwrap(),
        vec![retryable("the sink batcher has stopped")]
    );
}

#[test]
fn the_worst_outcome_wins_terminal_over_retryable_over_delivered() {
    let seen = recording();
    let delivered = || Outcome::Delivered;
    let retryable = || Outcome::Retryable(SinkError("throttled".to_owned()));
    let terminal = || Outcome::Terminal(SinkError("rejected".to_owned()));
    let elapsed = Duration::from_millis(7);
    let cases = [
        (vec![delivered(), delivered()], SinkOutcome::Delivered),
        (vec![delivered(), retryable()], SinkOutcome::Retryable),
        (vec![retryable(), delivered()], SinkOutcome::Retryable),
        (vec![delivered(), terminal()], SinkOutcome::Terminal),
        (vec![terminal(), retryable()], SinkOutcome::Terminal),
        (
            vec![retryable(), terminal(), delivered()],
            SinkOutcome::Terminal,
        ),
    ];
    for (outcomes, _) in &cases {
        record_sink_call(&tracing::Span::none(), outcomes, Some(elapsed));
    }
    record_sink_call(&tracing::Span::none(), &[terminal()], None);
    let expected: Vec<(SinkOutcome, Duration)> =
        cases.iter().map(|(_, worst)| (*worst, elapsed)).collect();
    assert_eq!(sink_points(&seen), expected);
}

#[test]
fn failed_messages_count_as_sink_errors_of_their_kind() {
    let seen = recording();
    let retryable = || Outcome::Retryable(SinkError("throttled".to_owned()));
    let terminal = || Outcome::Terminal(SinkError("rejected".to_owned()));
    let elapsed = Some(Duration::from_millis(3));
    record_sink_call(
        &tracing::Span::none(),
        &[Outcome::Delivered, retryable(), terminal(), retryable()],
        elapsed,
    );
    record_sink_call(&tracing::Span::none(), &[Outcome::Delivered], elapsed);
    record_sink_call(&tracing::Span::none(), &[terminal()], elapsed);
    let errors: Vec<Seen> = seen
        .lock()
        .unwrap()
        .iter()
        .filter(|entry| matches!(entry, Seen::Error(..)))
        .cloned()
        .collect();
    assert_eq!(
        errors,
        vec![
            Seen::Error(ErrorComponent::Sink, ErrorKind::Retryable, 2),
            Seen::Error(ErrorComponent::Sink, ErrorKind::Terminal, 1),
            Seen::Error(ErrorComponent::Sink, ErrorKind::Terminal, 1),
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn each_inner_call_records_one_point_timed_over_the_call_not_the_linger_or_permit_wait() {
    let seen = recording();
    let mut sink = ScriptedSink::delivering();
    sink.latency = Duration::from_millis(30);
    let rig = Rig::with(sink, Arc::new(Semaphore::new(1)));
    let first = rig.submit(vec![entry(0, b"a", 1)]);
    tokio::time::sleep(Duration::from_millis(10)).await;
    let second = rig.submit(vec![entry(1, b"b", 1)]);
    first.await.unwrap();
    second.await.unwrap();
    assert_eq!(rig.sink.batches(), vec![vec![0], vec![1]]);
    assert_eq!(
        sink_points(&seen),
        vec![(SinkOutcome::Delivered, Duration::from_millis(30)); 2]
    );
}

#[tokio::test(start_paused = true)]
async fn the_sink_send_span_size_counts_the_entries_of_one_inner_call_across_submitters() {
    let (capture, _guard) = Capture::install(LevelFilter::DEBUG);
    let rig = Rig::new(LIMITS, 1);
    let first = rig.submit(vec![entry(0, b"a", 1), entry(1, b"b", 1)]);
    let second = rig.submit(vec![entry(2, b"c", 1)]);
    first.await.unwrap();
    second.await.unwrap();
    let spans: Vec<_> = capture
        .spans()
        .into_iter()
        .filter(|span| span.name == "sink_send")
        .collect();
    assert_eq!(spans.len(), 1, "{spans:?}");
    assert_eq!(spans[0].level, Level::DEBUG);
    let fields: Vec<(&str, &str)> = spans[0]
        .fields
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect();
    assert_eq!(
        fields,
        vec![
            ("delivered", "3"),
            ("retryable", "0"),
            ("sink", "custom"),
            ("size", "3"),
            ("terminal", "0"),
        ]
    );
}
