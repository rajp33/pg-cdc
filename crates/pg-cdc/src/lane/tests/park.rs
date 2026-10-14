use super::*;

fn outage(down: &Arc<AtomicBool>, failing: u32) -> Script {
    let down = Arc::clone(down);
    Box::new(move |ordinal, _| {
        if ordinal == failing && down.load(Ordering::SeqCst) {
            Outcome::Retryable(SinkError("throttled".to_owned()))
        } else {
            Outcome::Delivered
        }
    })
}

fn parking_after(park_after: Duration) -> Setup {
    Setup {
        park_after,
        ..Setup::default()
    }
}

#[tokio::test(start_paused = true)]
async fn a_key_failing_for_park_after_parks_its_events_and_settles_them() {
    let down = Arc::new(AtomicBool::new(true));
    let setup = parking_after(Duration::from_secs(1));
    let sink = ScriptedSink::new(LIMITS, outage(&down, 0));
    let mut harness = Harness::start(setup, sink, TestStore::default()).await;
    let start = Instant::now();
    harness.send(0, b"k", 10).unwrap();
    harness.send(1, b"k", 10).unwrap();
    harness.send(2, b"other", 10).unwrap();
    assert_eq!(harness.settle(3).await, Lsn(102));
    let parked_after = Instant::now() - start;
    assert!(
        parked_after >= Duration::from_secs(1) && parked_after <= Duration::from_millis(1100),
        "parked after {parked_after:?}"
    );
    let queue = harness
        .store
        .inner
        .read_parked(&OrderKeyId::of(b"k"), None, 10)
        .await
        .unwrap();
    let summary: Vec<(u32, ParkStatus, u32, &str)> = queue
        .iter()
        .map(|event| {
            (
                event.id.ordinal(),
                event.status,
                event.attempts,
                event.last_error.as_str(),
            )
        })
        .collect();
    assert_eq!(summary.len(), 2);
    assert_eq!((summary[0].0, summary[0].1), (0, ParkStatus::Retrying));
    assert!(summary[0].2 >= 1);
    assert_eq!(summary[0].3, "throttled");
    assert_eq!(
        summary[1],
        (1, ParkStatus::Retrying, 0, "queued behind a parked event")
    );
    let sent: Vec<u32> = harness
        .sink
        .sends()
        .iter()
        .map(|sent| sent.ordinal)
        .collect();
    assert!(!sent.contains(&1), "{sent:?}");
}

#[tokio::test(start_paused = true)]
async fn a_parked_key_drains_in_order_once_the_sink_recovers() {
    let down = Arc::new(AtomicBool::new(true));
    let setup = parking_after(Duration::from_secs(1));
    let sink = ScriptedSink::new(LIMITS, outage(&down, 0));
    let mut harness = Harness::start(setup, sink, TestStore::default()).await;
    harness.send(0, b"k", 10).unwrap();
    harness.send(1, b"k", 10).unwrap();
    assert_eq!(harness.settle(2).await, Lsn(101));
    harness.send(2, b"k", 10).unwrap();
    assert_eq!(harness.settle(1).await, Lsn(102));
    down.store(false, Ordering::SeqCst);
    assert_eq!(harness.delivered(3).await, vec![0, 1, 2]);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(harness.store.queue(b"k").await, vec![]);
    assert_eq!(harness.store.inner.parked_events(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_terminal_outcome_parks_the_event_dead_and_blocks_its_key() {
    let script: Script = Box::new(|ordinal, _| {
        if ordinal == 0 {
            Outcome::Terminal(SinkError("rejected".to_owned()))
        } else {
            Outcome::Delivered
        }
    });
    let mut harness = Harness::new(1, 1, ScriptedSink::new(LIMITS, script)).await;
    harness.send(0, b"poison", 10).unwrap();
    harness.send(1, b"poison", 10).unwrap();
    harness.send(2, b"healthy", 10).unwrap();
    assert_eq!(harness.settle(3).await, Lsn(102));
    assert!(harness.quiet_for(Duration::from_secs(600)).await);
    assert_eq!(harness.sink.batches(), vec![vec![0, 2]]);
    assert_eq!(
        harness.store.queue(b"poison").await,
        vec![(0, ParkStatus::Dead), (1, ParkStatus::Retrying)]
    );
    harness.send(3, b"poison", 10).unwrap();
    assert_eq!(harness.settle(1).await, Lsn(103));
    assert_eq!(harness.store.queue(b"poison").await.len(), 3);
    assert_eq!(harness.sink.batches(), vec![vec![0, 2]]);
    let stored = harness
        .store
        .inner
        .read_parked(&OrderKeyId::of(b"poison"), None, 10)
        .await
        .unwrap();
    let metadata: Vec<(u32, u32, &str)> = stored
        .iter()
        .map(|event| {
            (
                event.id.ordinal(),
                event.attempts,
                event.last_error.as_str(),
            )
        })
        .collect();
    assert_eq!(
        metadata,
        vec![
            (0, 1, "rejected"),
            (1, 0, "queued behind a parked event"),
            (3, 0, "queued behind a parked event"),
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn an_event_above_the_sink_limit_is_parked_dead_without_a_payload_store() {
    let limits = SinkCapabilities {
        max_message_bytes: 100,
        ..LIMITS
    };
    let mut harness = Harness::new(
        1,
        1,
        ScriptedSink::new(limits, Box::new(|_, _| Outcome::Delivered)),
    )
    .await;
    harness.send(0, b"wide", 101).unwrap();
    harness.send(1, b"narrow", 100).unwrap();
    assert_eq!(harness.settle(2).await, Lsn(101));
    assert!(harness.quiet_for(Duration::from_secs(60)).await);
    assert_eq!(harness.sink.batches(), vec![vec![1]]);
    let queue = harness
        .store
        .inner
        .read_parked(&OrderKeyId::of(b"wide"), None, 10)
        .await
        .unwrap();
    assert_eq!(queue.len(), 1);
    assert_eq!(queue[0].status, ParkStatus::Dead);
    assert_eq!(
        queue[0].last_error,
        "the event is 101 bytes, above the sink's limit of 100"
    );
}

#[tokio::test(start_paused = true)]
async fn a_store_outage_keeps_parking_events_in_flight_until_it_ends() {
    let down = Arc::new(AtomicBool::new(true));
    let store = TestStore::default();
    store.fail_appends.store(u32::MAX, Ordering::SeqCst);
    let setup = parking_after(Duration::from_secs(1));
    let sink = ScriptedSink::new(LIMITS, outage(&down, 0));
    let mut harness = Harness::start(setup, sink, store).await;
    harness.send(0, b"k", 10).unwrap();
    assert!(harness.quiet_for(Duration::from_secs(120)).await);
    assert_eq!(harness.ack.inflight_events(), 1);
    assert_eq!(harness.store.queue(b"k").await, vec![]);
    harness.store.fail_appends.store(0, Ordering::SeqCst);
    assert_eq!(harness.settle(1).await, Lsn(100));
    assert_eq!(
        harness.store.queue(b"k").await,
        vec![(0, ParkStatus::Retrying)]
    );
}

#[tokio::test(start_paused = true)]
async fn the_parked_limit_holds_events_in_flight_until_queues_drain() {
    let down = Arc::new(AtomicBool::new(true));
    let store = TestStore::default();
    store.seed(b"a", 0, ParkStatus::Retrying).await;
    store.seed(b"a", 1, ParkStatus::Retrying).await;
    let setup = Setup {
        park_after: Duration::from_secs(1),
        max_parked_events: 2,
        ..Setup::default()
    };
    let down_for = Arc::clone(&down);
    let script: Script = Box::new(move |_, _| {
        if down_for.load(Ordering::SeqCst) {
            Outcome::Retryable(SinkError("throttled".to_owned()))
        } else {
            Outcome::Delivered
        }
    });
    let mut harness = Harness::start(setup, ScriptedSink::new(LIMITS, script), store).await;
    harness.send(5, b"b", 10).unwrap();
    assert!(harness.quiet_for(Duration::from_secs(60)).await);
    assert_eq!(harness.store.queue(b"b").await, vec![]);
    assert_eq!(harness.ack.inflight_events(), 1);
    down.store(false, Ordering::SeqCst);
    assert_eq!(harness.settle(1).await, Lsn(100));
    let mut delivered = harness.delivered(3).await;
    delivered.sort_unstable();
    assert_eq!(delivered, vec![0, 1, 5]);
}

#[tokio::test(start_paused = true)]
async fn park_all_parks_every_queued_event_and_stops_sending() {
    let script: Script = Box::new(|_, _| Outcome::Retryable(SinkError("down".to_owned())));
    let mut harness = Harness::new(2, 2, ScriptedSink::new(LIMITS, script)).await;
    harness.send(0, b"k", 10).unwrap();
    harness.send(1, b"k", 10).unwrap();
    harness.send(2, b"other", 10).unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    harness.lanes.park_all();
    assert_eq!(harness.settle(3).await, Lsn(102));
    let sent = harness.sink.sends().len();
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert_eq!(harness.sink.sends().len(), sent);
    assert_eq!(
        harness.store.queue(b"k").await,
        vec![(0, ParkStatus::Retrying), (1, ParkStatus::Retrying)]
    );
    assert_eq!(
        harness.store.queue(b"other").await,
        vec![(2, ParkStatus::Retrying)]
    );
}

#[tokio::test(start_paused = true)]
async fn park_all_leaves_events_of_unloaded_partitions_to_the_replay_skip() {
    let store = TestStore::default();
    let held = OrderKeyId::of(b"k").partition();
    store.hold(held);
    store.seed(b"k", 6, ParkStatus::Retrying).await;
    let mut harness = Harness::start(Setup::default(), ScriptedSink::delivering(), store).await;
    harness.lanes.park_all();
    tokio::time::sleep(Duration::from_millis(500)).await;
    harness.send(5, b"k", 10).unwrap();
    assert!(harness.quiet_for(Duration::from_secs(60)).await);
    assert_eq!(
        harness.store.queue(b"k").await,
        vec![(6, ParkStatus::Retrying)]
    );
    harness.store.release.notify_one();
    assert_eq!(harness.settle(1).await, Lsn(100));
    assert_eq!(
        harness.store.queue(b"k").await,
        vec![(6, ParkStatus::Retrying)]
    );
    assert_eq!(harness.sink.batches(), Vec::<Vec<u32>>::new());
}
