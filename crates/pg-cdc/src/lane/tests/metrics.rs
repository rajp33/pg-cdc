use super::*;
use crate::capture::{Capture, SpanSeen};
use crate::metrics::{ErrorComponent, ErrorKind, EventOutcome, Measurement, Recorder, SinkOutcome};
use crate::sink::AttributeValue;
use tracing::Level;
use tracing::level_filters::LevelFilter;

#[derive(Debug, Clone, PartialEq)]
pub(super) enum Seen {
    Event {
        table: String,
        op: String,
        outcome: EventOutcome,
        count: u64,
    },
    CommitToDelivery(Duration),
    Sink(SinkOutcome, Duration),
    Error(ErrorComponent, ErrorKind, u64),
    Other,
}

#[derive(Debug)]
struct Collecting(Arc<Mutex<Vec<Seen>>>);

impl Recorder for Collecting {
    fn record(&self, measurement: Measurement<'_>) {
        let seen = match measurement {
            Measurement::Event {
                table,
                op,
                outcome,
                count,
            } => Seen::Event {
                table: table.to_owned(),
                op: op.to_owned(),
                outcome,
                count,
            },
            Measurement::CommitToDelivery(elapsed) => Seen::CommitToDelivery(elapsed),
            Measurement::Sink { outcome, elapsed } => Seen::Sink(outcome, elapsed),
            Measurement::Error {
                component,
                kind,
                count,
            } => Seen::Error(component, kind, count),
            _ => Seen::Other,
        };
        self.0.lock().unwrap().push(seen);
    }
}

pub(super) fn recording() -> Arc<Mutex<Vec<Seen>>> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    crate::metrics::install(Box::new(Collecting(Arc::clone(&seen)))).unwrap();
    seen
}

fn counted(seen: &Mutex<Vec<Seen>>, table: &str, outcome: EventOutcome) -> u64 {
    seen.lock()
        .unwrap()
        .iter()
        .map(|entry| match entry {
            Seen::Event {
                table: seen_table,
                outcome: seen_outcome,
                count,
                ..
            } if seen_table == table && *seen_outcome == outcome => *count,
            _ => 0,
        })
        .sum()
}

pub(super) fn sink_points(seen: &Mutex<Vec<Seen>>) -> Vec<(SinkOutcome, Duration)> {
    seen.lock()
        .unwrap()
        .iter()
        .filter_map(|entry| match entry {
            Seen::Sink(outcome, elapsed) => Some((*outcome, *elapsed)),
            _ => None,
        })
        .collect()
}

fn label(name: &str, value: &str) -> (String, AttributeValue) {
    (name.to_owned(), AttributeValue::String(value.to_owned()))
}

fn send_labelled(
    harness: &mut Harness,
    ordinal: u32,
    key: &[u8],
    table: &str,
    committed_ms: Option<u64>,
) {
    harness.ack.begin().unwrap();
    let ticket = harness.ack.track(1).unwrap();
    harness.ack.commit(Lsn(100 + harness.commits)).unwrap();
    harness.commits += 1;
    harness
        .lanes
        .dispatch(Pending {
            outbound: Outbound {
                id: EventId::new(Lsn(0x10), ordinal),
                order_key: OrderKeyId::of(key),
                attributes: vec![label("table", table), label("op", "insert")],
                body: Bytes::from_static(b"x"),
            },
            ticket,
            committed_ms,
        })
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn delivery_records_events_sink_calls_and_commit_latency() {
    let seen = recording();
    let mut harness = Harness::new(1, 1, ScriptedSink::delivering()).await;
    let committed = crate::state::now_ms() - 50;
    for (ordinal, key) in [(0, b"a"), (1, b"a"), (2, b"b")] {
        send_labelled(&mut harness, ordinal, key, "orders", Some(committed));
    }
    harness.settle(3).await;
    harness.delivered(3).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let seen = seen.lock().unwrap().clone();
    let delivered = Seen::Event {
        table: "orders".to_owned(),
        op: "insert".to_owned(),
        outcome: EventOutcome::Delivered,
        count: 1,
    };
    assert_eq!(seen.iter().filter(|entry| **entry == delivered).count(), 3);
    assert!(
        seen.iter()
            .any(|entry| matches!(entry, Seen::Sink(SinkOutcome::Delivered, _)))
    );
    let latencies: Vec<Duration> = seen
        .iter()
        .filter_map(|entry| match entry {
            Seen::CommitToDelivery(elapsed) => Some(*elapsed),
            _ => None,
        })
        .collect();
    assert_eq!(latencies.len(), 3);
    assert!(
        latencies
            .iter()
            .all(|elapsed| *elapsed >= Duration::from_millis(50)),
        "{latencies:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn parking_and_dead_events_are_counted() {
    let seen = recording();
    let script: Script = Box::new(|ordinal, _| match ordinal {
        0 | 1 => Outcome::Retryable(SinkError("throttled".to_owned())),
        _ => Outcome::Terminal(SinkError("rejected".to_owned())),
    });
    let setup = Setup {
        park_after: Duration::from_secs(1),
        ..Setup::default()
    };
    let mut harness = Harness::start(
        setup,
        ScriptedSink::new(LIMITS, script),
        TestStore::default(),
    )
    .await;
    send_labelled(&mut harness, 0, b"a", "orders", None);
    send_labelled(&mut harness, 1, b"a", "orders", None);
    send_labelled(&mut harness, 2, b"b", "payments", None);
    harness.settle(3).await;
    assert_eq!(counted(&seen, "orders", EventOutcome::Parked), 2);
    assert_eq!(counted(&seen, "orders", EventOutcome::Dead), 0);
    assert_eq!(counted(&seen, "payments", EventOutcome::Parked), 1);
    assert_eq!(counted(&seen, "payments", EventOutcome::Dead), 1);
    assert!(
        sink_points(&seen)
            .iter()
            .any(|(outcome, _)| *outcome == SinkOutcome::Retryable)
    );
}

#[tokio::test(start_paused = true)]
async fn drained_events_count_as_delivered_without_a_latency() {
    let seen = recording();
    let store = TestStore::default();
    store.seed(b"k", 1, ParkStatus::Retrying).await;
    let harness = Harness::start(Setup::default(), ScriptedSink::delivering(), store).await;
    harness.delivered(1).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(counted(&seen, "unknown", EventOutcome::Delivered), 1);
    assert!(
        !seen
            .lock()
            .unwrap()
            .iter()
            .any(|entry| matches!(entry, Seen::CommitToDelivery(_)))
    );
}

async fn spans_of_a_delivered_event(level: LevelFilter) -> Vec<SpanSeen> {
    let (capture, _guard) = Capture::install(level);
    let mut harness = Harness::new(1, 1, ScriptedSink::delivering()).await;
    harness.send(0, b"k", 1).unwrap();
    harness.settle(1).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    capture.spans()
}

#[tokio::test(start_paused = true)]
async fn hot_path_spans_are_debug_and_disabled_at_info() {
    let at_info = spans_of_a_delivered_event(LevelFilter::INFO).await;
    assert!(
        at_info.iter().all(|span| span.name != "sink_send"),
        "{at_info:?}"
    );
    let at_debug = spans_of_a_delivered_event(LevelFilter::DEBUG).await;
    let sends: Vec<&SpanSeen> = at_debug
        .iter()
        .filter(|span| span.name == "sink_send")
        .collect();
    assert_eq!(sends.len(), 1, "{at_debug:?}");
    assert_eq!(sends[0].level, Level::DEBUG);
    let fields: Vec<(&str, &str)> = sends[0]
        .fields
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect();
    assert_eq!(
        fields,
        vec![
            ("delivered", "1"),
            ("retryable", "0"),
            ("sink", "custom"),
            ("size", "1"),
            ("terminal", "0"),
        ]
    );
}

fn labelled(key: &[u8], ordinal: u32, body: usize) -> ParkedEvent {
    let mut event = parked(key, ordinal, ParkStatus::Retrying);
    event.attributes = vec![label("table", "orders"), label("op", "update")];
    event.body = Bytes::from(vec![b'x'; body]);
    event
}

async fn wait_for_dead(store: &TestStore, count: usize) {
    for _ in 0..10_000 {
        if store.dead_heads().await.len() >= count {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("{count} heads never turned dead");
}

fn dead_events(seen: &Mutex<Vec<Seen>>) -> Vec<(String, String, u64)> {
    seen.lock()
        .unwrap()
        .iter()
        .filter_map(|entry| match entry {
            Seen::Event {
                table,
                op,
                outcome: EventOutcome::Dead,
                count,
            } => Some((table.clone(), op.clone(), *count)),
            _ => None,
        })
        .collect()
}

#[tokio::test(start_paused = true)]
async fn a_parked_head_rejected_for_good_counts_as_dead_under_its_labels() {
    let seen = recording();
    let store = TestStore::default();
    store
        .inner
        .append_parked(&[labelled(b"k", 0, 1)])
        .await
        .unwrap();
    store.fail_status_changes.store(2, Ordering::SeqCst);
    let terminal: Script = Box::new(|_, _| Outcome::Terminal(SinkError("rejected".to_owned())));
    let harness =
        Harness::start(Setup::default(), ScriptedSink::new(LIMITS, terminal), store).await;
    wait_for_dead(&harness.store, 1).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(harness.store.status_changes.load(Ordering::SeqCst), 3);
    assert_eq!(
        dead_events(&seen),
        vec![("orders".to_owned(), "update".to_owned(), 1)]
    );
}

#[tokio::test(start_paused = true)]
async fn an_oversized_parked_head_counts_as_dead_under_its_labels() {
    let seen = recording();
    let store = TestStore::default();
    store
        .inner
        .append_parked(&[labelled(b"k", 0, 500)])
        .await
        .unwrap();
    let limits = SinkCapabilities {
        max_message_bytes: 100,
        ..LIMITS
    };
    let harness = Harness::start(
        Setup::default(),
        ScriptedSink::new(limits, Box::new(|_, _| Outcome::Delivered)),
        store,
    )
    .await;
    wait_for_dead(&harness.store, 1).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        dead_events(&seen),
        vec![("orders".to_owned(), "update".to_owned(), 1)]
    );
}

fn sink_errors(seen: &Mutex<Vec<Seen>>) -> Vec<Seen> {
    seen.lock()
        .unwrap()
        .iter()
        .filter(|entry| matches!(entry, Seen::Error(ErrorComponent::Sink, ..)))
        .cloned()
        .collect()
}

#[tokio::test(start_paused = true)]
async fn an_event_too_large_for_the_sink_counts_one_terminal_error() {
    let seen = recording();
    let limits = SinkCapabilities {
        max_message_bytes: 100,
        ..LIMITS
    };
    let mut harness = Harness::start(
        Setup::default(),
        ScriptedSink::new(limits, Box::new(|_, _| Outcome::Delivered)),
        TestStore::default(),
    )
    .await;
    harness.send(0, b"big", 500).unwrap();
    harness.settle(1).await;
    wait_for_dead(&harness.store, 1).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(harness.sink.batches(), Vec::<Vec<u32>>::new());
    assert_eq!(
        sink_errors(&seen),
        vec![Seen::Error(ErrorComponent::Sink, ErrorKind::Terminal, 1)]
    );
}

#[tokio::test(start_paused = true)]
async fn an_oversized_parked_head_counts_one_terminal_error() {
    let seen = recording();
    let store = TestStore::default();
    store
        .inner
        .append_parked(&[labelled(b"k", 0, 500)])
        .await
        .unwrap();
    let limits = SinkCapabilities {
        max_message_bytes: 100,
        ..LIMITS
    };
    let harness = Harness::start(
        Setup::default(),
        ScriptedSink::new(limits, Box::new(|_, _| Outcome::Delivered)),
        store,
    )
    .await;
    wait_for_dead(&harness.store, 1).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        sink_errors(&seen),
        vec![Seen::Error(ErrorComponent::Sink, ErrorKind::Terminal, 1)]
    );
}

#[tokio::test(start_paused = true)]
async fn each_sink_call_records_one_point_timed_over_the_call() {
    let seen = recording();
    let mut sink = ScriptedSink::delivering();
    sink.latency = Duration::from_millis(30);
    let mut harness = Harness::new(1, 1, sink).await;
    for (ordinal, key) in [(0, b"a"), (1, b"a"), (2, b"b")] {
        harness.send(ordinal, key, 1).unwrap();
    }
    harness.settle(3).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let calls = harness.sink.batches().len();
    assert!(calls >= 2, "{:?}", harness.sink.batches());
    assert_eq!(
        sink_points(&seen),
        vec![(SinkOutcome::Delivered, Duration::from_millis(30)); calls]
    );
}

#[tokio::test(start_paused = true)]
async fn a_mixed_batch_records_its_worst_outcome() {
    let seen = recording();
    let script: Script = Box::new(|ordinal, attempt| match (ordinal, attempt) {
        (1, 1) => Outcome::Retryable(SinkError("throttled".to_owned())),
        (2, _) => Outcome::Terminal(SinkError("rejected".to_owned())),
        _ => Outcome::Delivered,
    });
    let mut harness = Harness::new(1, 1, ScriptedSink::new(LIMITS, script)).await;
    for (ordinal, key) in [(0, b"a"), (1, b"b"), (2, b"c")] {
        harness.send(ordinal, key, 1).unwrap();
    }
    harness.settle(3).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(harness.sink.batches()[0], vec![0, 1, 2]);
    let outcomes: Vec<SinkOutcome> = sink_points(&seen)
        .into_iter()
        .map(|(outcome, _)| outcome)
        .collect();
    assert_eq!(outcomes.first(), Some(&SinkOutcome::Terminal));
    assert_eq!(outcomes.len(), harness.sink.batches().len());
}

const HOT_PATH_SPANS: [&str; 4] = ["sink_send", "park", "drain", "store"];

async fn spans_of_parking_and_draining(level: LevelFilter) -> Vec<SpanSeen> {
    let (capture, _guard) = Capture::install(level);
    let store = TestStore::default();
    store.seed(b"drained", 1, ParkStatus::Retrying).await;
    let script: Script = Box::new(|ordinal, _| match ordinal {
        2 => Outcome::Terminal(SinkError("rejected".to_owned())),
        _ => Outcome::Delivered,
    });
    let setup = Setup {
        instrumented: true,
        ..Setup::default()
    };
    let mut harness = Harness::start(setup, ScriptedSink::new(LIMITS, script), store).await;
    harness.delivered(1).await;
    harness.send(2, b"rejected", 1).unwrap();
    harness.settle(1).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(harness.store.queue(b"drained").await, vec![]);
    assert_eq!(
        harness.store.queue(b"rejected").await,
        vec![(2, ParkStatus::Dead)]
    );
    capture.spans()
}

fn fields_of<'a>(spans: &'a [SpanSeen], name: &str) -> Vec<Vec<(&'a str, &'a str)>> {
    spans
        .iter()
        .filter(|span| span.name == name)
        .inspect(|span| assert_eq!(span.level, Level::DEBUG, "{span:?}"))
        .map(|span| {
            span.fields
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_str()))
                .collect()
        })
        .collect()
}

#[tokio::test(start_paused = true)]
async fn park_drain_and_store_spans_are_debug_and_disabled_at_info() {
    let at_info = spans_of_parking_and_draining(LevelFilter::INFO).await;
    assert!(
        at_info
            .iter()
            .all(|span| !HOT_PATH_SPANS.contains(&span.name.as_str())),
        "{at_info:?}"
    );
    let at_debug = spans_of_parking_and_draining(LevelFilter::DEBUG).await;
    assert_eq!(fields_of(&at_debug, "park"), vec![vec![("events", "1")]]);
    assert_eq!(fields_of(&at_debug, "drain"), vec![vec![("events", "1")]]);
    let stores = fields_of(&at_debug, "store");
    for operation in [
        "load_parked_heads",
        "read_parked",
        "remove_parked",
        "append_parked",
    ] {
        assert!(
            stores.contains(&vec![("count", "1"), ("operation", operation)]),
            "{operation}: {stores:?}"
        );
    }
    assert!(
        stores
            .iter()
            .all(|fields| fields.len() == 2 && fields[0].0 == "count"),
        "{stores:?}"
    );
    assert_eq!(fields_of(&at_debug, "sink_send").len(), 2, "{at_debug:?}");
}
