use super::*;

fn key_outside_partition(partition: u8) -> Vec<u8> {
    (0u8..=255)
        .map(|byte| vec![b'k', byte])
        .find(|key| OrderKeyId::of(key).partition() != partition)
        .unwrap()
}

#[tokio::test(start_paused = true)]
async fn o3_a_partition_delivers_nothing_until_its_heads_load() {
    let store = TestStore::default();
    let held = OrderKeyId::of(b"held").partition();
    store.hold(held);
    let free = key_outside_partition(held);
    let mut harness = Harness::start(Setup::default(), ScriptedSink::delivering(), store).await;
    harness.send(0, b"held", 10).unwrap();
    harness.send(1, &free, 10).unwrap();
    harness.settle(1).await;
    assert!(harness.quiet_for(Duration::from_secs(60)).await);
    assert_eq!(harness.sink.batches(), vec![vec![1]]);
    harness.store.release.notify_one();
    assert_eq!(harness.settle(1).await, Lsn(101));
    assert_eq!(harness.sink.batches(), vec![vec![1], vec![0]]);
}

#[tokio::test(start_paused = true)]
async fn parked_queues_drain_in_id_order_then_the_key_returns_to_live() {
    let store = TestStore::default();
    for ordinal in 1..=3 {
        store.seed(b"k", ordinal, ParkStatus::Retrying).await;
    }
    let mut harness = Harness::start(Setup::default(), ScriptedSink::delivering(), store).await;
    harness.send(4, b"k", 10).unwrap();
    harness.send(5, b"k", 10).unwrap();
    assert_eq!(harness.settle(2).await, Lsn(101));
    assert_eq!(harness.delivered(5).await, vec![1, 2, 3, 4, 5]);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(harness.store.queue(b"k").await, vec![]);
    let appended = harness.store.appends.load(Ordering::SeqCst);
    harness.send(6, b"k", 10).unwrap();
    assert_eq!(harness.settle(1).await, Lsn(102));
    assert_eq!(harness.delivered(6).await, vec![1, 2, 3, 4, 5, 6]);
    assert_eq!(harness.store.appends.load(Ordering::SeqCst), appended);
}

#[tokio::test(start_paused = true)]
async fn o3_new_events_queue_behind_a_failing_parked_head() {
    let store = TestStore::default();
    store.seed(b"k", 1, ParkStatus::Retrying).await;
    let script: Script = Box::new(|ordinal, attempt| {
        if ordinal == 1 && attempt <= 3 {
            Outcome::Retryable(SinkError("throttled".to_owned()))
        } else {
            Outcome::Delivered
        }
    });
    let mut harness =
        Harness::start(Setup::default(), ScriptedSink::new(LIMITS, script), store).await;
    harness.send(2, b"k", 10).unwrap();
    harness.send(3, b"k", 10).unwrap();
    assert_eq!(harness.settle(2).await, Lsn(101));
    assert_eq!(harness.delivered(3).await, vec![1, 2, 3]);
    let order: Vec<u32> = harness
        .sink
        .sends()
        .iter()
        .map(|sent| sent.ordinal)
        .collect();
    assert_eq!(order, vec![1, 1, 1, 1, 2, 3]);
}

#[tokio::test(start_paused = true)]
async fn replayed_events_below_the_head_settle_without_being_sent() {
    let store = TestStore::default();
    store.seed(b"k", 5, ParkStatus::Retrying).await;
    store.seed(b"k", 7, ParkStatus::Retrying).await;
    store.hold(OrderKeyId::of(b"k").partition());
    let mut harness = Harness::start(Setup::default(), ScriptedSink::delivering(), store).await;
    for ordinal in [3, 5, 7, 9] {
        harness.send(ordinal, b"k", 10).unwrap();
    }
    harness.store.release.notify_one();
    assert_eq!(harness.settle(4).await, Lsn(103));
    assert_eq!(harness.delivered(3).await, vec![5, 7, 9]);
    assert!(harness.quiet_for(Duration::from_secs(60)).await);
    assert_eq!(harness.sink.sends().len(), 3);
    assert_eq!(harness.store.queue(b"k").await, vec![]);
}

#[tokio::test(start_paused = true)]
async fn a_dead_head_blocks_only_its_key() {
    let store = TestStore::default();
    store.seed(b"k", 1, ParkStatus::Dead).await;
    store.seed(b"k", 2, ParkStatus::Retrying).await;
    let mut harness = Harness::start(Setup::default(), ScriptedSink::delivering(), store).await;
    harness.send(3, b"k", 10).unwrap();
    harness.send(4, b"other", 10).unwrap();
    assert_eq!(harness.settle(2).await, Lsn(101));
    assert!(harness.quiet_for(Duration::from_secs(600)).await);
    assert_eq!(harness.sink.batches(), vec![vec![4]]);
    assert_eq!(
        harness.store.queue(b"k").await,
        vec![
            (1, ParkStatus::Dead),
            (2, ParkStatus::Retrying),
            (3, ParkStatus::Retrying)
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_terminal_outcome_on_a_parked_head_marks_it_dead() {
    let store = TestStore::default();
    store.seed(b"k", 1, ParkStatus::Retrying).await;
    store.seed(b"k", 2, ParkStatus::Retrying).await;
    let script: Script = Box::new(|ordinal, _| {
        if ordinal == 1 {
            Outcome::Terminal(SinkError("rejected".to_owned()))
        } else {
            Outcome::Delivered
        }
    });
    let mut harness =
        Harness::start(Setup::default(), ScriptedSink::new(LIMITS, script), store).await;
    assert!(harness.quiet_for(Duration::from_secs(600)).await);
    assert_eq!(harness.sink.batches(), vec![vec![1]]);
    assert_eq!(
        harness.store.queue(b"k").await,
        vec![(1, ParkStatus::Dead), (2, ParkStatus::Retrying)]
    );
}

#[tokio::test(start_paused = true)]
async fn a_store_outage_while_draining_resends_nothing_and_resumes() {
    let store = TestStore::default();
    store.seed(b"k", 1, ParkStatus::Retrying).await;
    store.seed(b"k", 2, ParkStatus::Retrying).await;
    store.fail_removes.store(3, Ordering::SeqCst);
    store.fail_reads.store(1, Ordering::SeqCst);
    let harness = Harness::start(Setup::default(), ScriptedSink::delivering(), store).await;
    assert_eq!(harness.delivered(2).await, vec![1, 2]);
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert_eq!(harness.sink.sends().len(), 2);
    assert_eq!(harness.store.queue(b"k").await, vec![]);
}

#[tokio::test(start_paused = true)]
async fn startup_loading_is_bounded_by_the_store_concurrency() {
    let store = TestStore {
        load_delay: Duration::from_millis(10),
        ..TestStore::default()
    };
    let setup = Setup {
        store_concurrency: 3,
        ..Setup::default()
    };
    let harness = Harness::start(setup, ScriptedSink::delivering(), store).await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(harness.store.loads.load(Ordering::SeqCst), 256);
    assert_eq!(harness.store.peak_loading.load(Ordering::SeqCst), 3);
}

#[tokio::test(start_paused = true)]
async fn parked_heads_in_memory_never_exceed_one_batch() {
    let store = TestStore::default();
    for key in 0u8..40 {
        store
            .seed(&[key], u32::from(key), ParkStatus::Retrying)
            .await;
    }
    let limits = SinkCapabilities {
        max_batch_messages: 4,
        ..LIMITS
    };
    let mut sink = ScriptedSink::new(limits, Box::new(|_, _| Outcome::Delivered));
    sink.latency = Duration::from_millis(10);
    let harness = Harness::start(Setup::default(), sink, store).await;
    let mut delivered = harness.delivered(40).await;
    delivered.sort_unstable();
    assert_eq!(delivered, (0..40).collect::<Vec<u32>>());
    assert!(harness.store.peak_outstanding.load(Ordering::SeqCst) <= 4);
    assert!(harness.sink.batches().iter().all(|batch| batch.len() <= 4));
}
