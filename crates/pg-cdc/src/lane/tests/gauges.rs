use super::*;
use crate::state::now_ms;
use crate::status::{ParkedSnapshot, STATS_INTERVAL};

fn parking_after(park_after: Duration) -> Setup {
    Setup {
        park_after,
        ..Setup::default()
    }
}

#[tokio::test(start_paused = true)]
async fn gauges_count_parked_keys_dead_heads_and_parked_events() {
    let script: Script = Box::new(|ordinal, _| match ordinal {
        0 | 1 => Outcome::Retryable(SinkError("throttled".to_owned())),
        _ => Outcome::Terminal(SinkError("rejected".to_owned())),
    });
    let setup = parking_after(Duration::from_secs(1));
    let mut harness = Harness::start(
        setup,
        ScriptedSink::new(LIMITS, script),
        TestStore::default(),
    )
    .await;
    harness.send(0, b"a", 10).unwrap();
    harness.send(1, b"a", 10).unwrap();
    harness.send(2, b"b", 10).unwrap();
    harness.settle(3).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let gauges = harness.gauges();
    assert_eq!(
        (gauges.events, gauges.keys, gauges.dead),
        (3, 2, 1),
        "{gauges:?}"
    );
    let oldest = gauges.oldest_ms.expect("a failure time is known");
    assert!(oldest <= now_ms());
}

#[tokio::test(start_paused = true)]
async fn gauges_return_to_zero_once_the_queues_drain() {
    let down = Arc::new(AtomicBool::new(true));
    let script: Script = {
        let down = Arc::clone(&down);
        Box::new(move |_, _| {
            if down.load(Ordering::SeqCst) {
                Outcome::Retryable(SinkError("throttled".to_owned()))
            } else {
                Outcome::Delivered
            }
        })
    };
    let setup = parking_after(Duration::from_secs(1));
    let mut harness = Harness::start(
        setup,
        ScriptedSink::new(LIMITS, script),
        TestStore::default(),
    )
    .await;
    harness.send(0, b"a", 10).unwrap();
    harness.send(1, b"a", 10).unwrap();
    harness.settle(2).await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(harness.gauges().keys, 1);
    down.store(false, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(40)).await;
    assert_eq!(
        harness.gauges(),
        ParkedSnapshot {
            events: 0,
            keys: 0,
            dead: 0,
            oldest_ms: None,
        }
    );
}

#[tokio::test(start_paused = true)]
async fn a_loaded_dead_head_is_counted_once_its_head_is_read() {
    let store = TestStore::default();
    store.seed(b"k", 0, ParkStatus::Dead).await;
    let harness = Harness::start(Setup::default(), ScriptedSink::delivering(), store).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        harness.gauges(),
        ParkedSnapshot {
            events: 1,
            keys: 1,
            dead: 1,
            oldest_ms: Some(1),
        }
    );
}

#[tokio::test(start_paused = true)]
async fn a_change_is_published_within_the_stats_interval_even_when_idle() {
    let script: Script = Box::new(|_, _| Outcome::Terminal(SinkError("rejected".to_owned())));
    let mut harness = Harness::new(1, 1, ScriptedSink::new(LIMITS, script)).await;
    harness.send(0, b"k", 10).unwrap();
    harness.settle(1).await;
    tokio::time::sleep(STATS_INTERVAL + Duration::from_millis(1)).await;
    assert_eq!(harness.gauges().dead, 1);
}

#[tokio::test(start_paused = true)]
async fn a_change_is_published_within_the_stats_interval_while_a_sink_call_is_held() {
    let script: Script = Box::new(|ordinal, _| match ordinal {
        0 => Outcome::Terminal(SinkError("rejected".to_owned())),
        _ => Outcome::Delivered,
    });
    let mut sink = ScriptedSink::new(LIMITS, script);
    sink.latency = Duration::from_secs(10);
    let mut harness = Harness::new(1, 1, sink).await;
    let start = Instant::now();
    harness.send(0, b"a", 10).unwrap();
    tokio::time::sleep_until(start + Duration::from_secs(9)).await;
    harness.send(1, b"b", 10).unwrap();
    tokio::time::sleep_until(start + Duration::from_millis(10_500)).await;
    let gauges = harness.gauges();
    assert_eq!((gauges.keys, gauges.dead), (1, 0), "{gauges:?}");
    tokio::time::sleep_until(start + Duration::from_millis(11_100)).await;
    assert_eq!(harness.sink.batches(), vec![vec![0]]);
    let gauges = harness.gauges();
    assert_eq!((gauges.keys, gauges.dead), (1, 1), "{gauges:?}");
}

#[tokio::test(start_paused = true)]
async fn a_change_is_published_within_the_stats_interval_while_a_store_call_is_held() {
    let store = TestStore {
        status_delay: Duration::from_secs(5),
        ..TestStore::default()
    };
    let keys = three_keys_in_one_partition();
    store.seed(&keys[0], 0, ParkStatus::Retrying).await;
    store.seed(&keys[1], 1, ParkStatus::Retrying).await;
    let script: Script = Box::new(|_, _| Outcome::Terminal(SinkError("rejected".to_owned())));
    let harness = Harness::start(Setup::default(), ScriptedSink::new(LIMITS, script), store).await;
    let start = Instant::now();
    tokio::time::sleep_until(start + Duration::from_secs(1)).await;
    let gauges = harness.gauges();
    assert_eq!((gauges.keys, gauges.dead), (2, 0), "{gauges:?}");
    tokio::time::sleep_until(start + Duration::from_secs(7)).await;
    assert_eq!(harness.store.status_changes.load(Ordering::SeqCst), 2);
    assert_eq!(harness.store.dead_heads().await.len(), 1);
    let gauges = harness.gauges();
    assert_eq!((gauges.keys, gauges.dead), (2, 1), "{gauges:?}");
    tokio::time::sleep_until(start + Duration::from_secs(12)).await;
    assert_eq!(harness.gauges().dead, 2);
}

#[tokio::test(start_paused = true)]
async fn a_change_is_published_within_the_stats_interval_while_a_queued_operation_is_held() {
    let store = TestStore {
        remove_delay: Duration::from_secs(30),
        ..TestStore::default()
    };
    store.seed(b"a", 0, ParkStatus::Dead).await;
    store.seed(b"b", 1, ParkStatus::Dead).await;
    let harness = Harness::start(Setup::default(), ScriptedSink::delivering(), store).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let gauges = harness.gauges();
    assert_eq!((gauges.keys, gauges.dead), (2, 2), "{gauges:?}");
    let operator = harness.operator();
    let start = Instant::now();
    let (redriven, discarded, during) = tokio::join!(
        operator.redrive(OrderKeyId::of(b"a")),
        operator.discard(OrderKeyId::of(b"b"), EventId::new(Lsn(0x10), 1)),
        async {
            tokio::time::sleep(STATS_INTERVAL + Duration::from_millis(100)).await;
            harness.gauges()
        },
    );
    assert_eq!(redriven, Ok(EventId::new(Lsn(0x10), 0)));
    assert_eq!(discarded, Ok(EventId::new(Lsn(0x10), 1)));
    assert!(start.elapsed() >= Duration::from_secs(30));
    assert_eq!((during.keys, during.dead), (2, 1), "{during:?}");
}
