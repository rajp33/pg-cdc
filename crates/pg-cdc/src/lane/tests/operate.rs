use super::*;

fn k() -> OrderKeyId {
    OrderKeyId::of(b"k")
}

fn id(ordinal: u32) -> EventId {
    EventId::new(Lsn(0x10), ordinal)
}

fn rejects_the_first_attempt_of_ordinal_zero() -> Script {
    Box::new(|ordinal, attempt| {
        if ordinal == 0 && attempt == 1 {
            Outcome::Terminal(SinkError("rejected".to_owned()))
        } else {
            Outcome::Delivered
        }
    })
}

fn always_retryable() -> Script {
    Box::new(|_, _| Outcome::Retryable(SinkError("down".to_owned())))
}

fn always_terminal() -> Script {
    Box::new(|_, _| Outcome::Terminal(SinkError("rejected".to_owned())))
}

fn parking_after(park_after: Duration) -> Setup {
    Setup {
        park_after,
        ..Setup::default()
    }
}

async fn dead_head_behind(ordinals: u32, store: TestStore) -> Harness {
    let mut sink = ScriptedSink::new(LIMITS, rejects_the_first_attempt_of_ordinal_zero());
    sink.latency = Duration::from_millis(100);
    let mut harness = Harness::start(Setup::default(), sink, store).await;
    for ordinal in 0..ordinals {
        harness.send(ordinal, b"k", 10).unwrap();
    }
    harness.settle(usize::try_from(ordinals).unwrap()).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    harness
}

#[tokio::test(start_paused = true)]
async fn redrive_marks_a_dead_head_retrying_and_delivers_it_then_the_queue() {
    let harness = dead_head_behind(3, TestStore::default()).await;
    assert_eq!(
        harness.store.queue(b"k").await,
        vec![
            (0, ParkStatus::Dead),
            (1, ParkStatus::Retrying),
            (2, ParkStatus::Retrying)
        ]
    );
    assert_eq!(harness.sink.batches(), vec![vec![0]]);
    assert_eq!(harness.gauges().dead, 1);

    assert_eq!(harness.operator().redrive(k()).await, Ok(id(0)));
    assert_eq!(
        harness.store.queue(b"k").await.first(),
        Some(&(0, ParkStatus::Retrying))
    );
    assert_eq!(harness.delivered(3).await, vec![0, 1, 2]);
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(harness.store.queue(b"k").await, vec![]);
    assert_eq!(harness.gauges().dead, 0);
    let order: Vec<u32> = harness
        .sink
        .sends()
        .iter()
        .map(|sent| sent.ordinal)
        .collect();
    assert_eq!(order, vec![0, 0, 1, 2]);
}

#[tokio::test(start_paused = true)]
async fn redrive_of_a_head_that_is_not_dead_is_refused() {
    let mut harness = Harness::start(
        parking_after(Duration::from_secs(1)),
        ScriptedSink::new(LIMITS, always_retryable()),
        TestStore::default(),
    )
    .await;
    harness.send(0, b"k", 10).unwrap();
    harness.settle(1).await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(
        harness.store.queue(b"k").await,
        vec![(0, ParkStatus::Retrying)]
    );
    let changes = harness.store.status_changes.load(Ordering::SeqCst);
    assert_eq!(
        harness.operator().redrive(k()).await,
        Err(OperateError::NotDead {
            key: k(),
            head: id(0)
        })
    );
    assert_eq!(harness.store.status_changes.load(Ordering::SeqCst), changes);
}

#[tokio::test(start_paused = true)]
async fn operations_on_keys_without_parked_events_are_refused() {
    let mut harness = Harness::new(1, 1, ScriptedSink::new(LIMITS, always_retryable())).await;
    harness.send(0, b"live", 10).unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(harness.sink.sends().len() > 1);
    let operator = harness.operator();
    for name in [&b"ghost"[..], &b"live"[..]] {
        let key = OrderKeyId::of(name);
        assert_eq!(
            operator.redrive(key).await,
            Err(OperateError::NotParked(key))
        );
        assert_eq!(
            operator.discard(key, id(0)).await,
            Err(OperateError::NotParked(key))
        );
    }
}

#[tokio::test(start_paused = true)]
async fn operations_wait_for_the_partition_to_load() {
    let store = TestStore::default();
    store.seed(b"k", 0, ParkStatus::Dead).await;
    store.hold(k().partition());
    let harness = Harness::start(Setup::default(), ScriptedSink::delivering(), store).await;
    let operator = harness.operator();
    assert_eq!(operator.redrive(k()).await, Err(OperateError::Loading(k())));
    assert_eq!(harness.store.status_changes.load(Ordering::SeqCst), 0);
    harness.store.release.notify_one();
    tokio::time::sleep(Duration::from_millis(1)).await;
    assert_eq!(operator.redrive(k()).await, Ok(id(0)));
}

#[tokio::test(start_paused = true)]
async fn discard_removes_the_head_only_if_it_still_has_that_id() {
    let mut harness = dead_head_behind(2, TestStore::default()).await;
    let operator = harness.operator();
    assert_eq!(
        operator.discard(k(), id(1)).await,
        Err(OperateError::HeadMismatch {
            key: k(),
            expected: id(1),
            head: id(0)
        })
    );
    assert_eq!(
        harness.store.queue(b"k").await,
        vec![(0, ParkStatus::Dead), (1, ParkStatus::Retrying)]
    );

    assert_eq!(operator.discard(k(), id(0)).await, Ok(id(0)));
    assert!(
        harness
            .store
            .queue(b"k")
            .await
            .iter()
            .all(|(ordinal, _)| *ordinal != 0)
    );
    assert_eq!(harness.delivered(1).await, vec![1]);
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(harness.store.queue(b"k").await, vec![]);
    let order: Vec<u32> = harness
        .sink
        .sends()
        .iter()
        .map(|sent| sent.ordinal)
        .collect();
    assert_eq!(order, vec![0, 1]);

    let appended = harness.store.appends.load(Ordering::SeqCst);
    harness.send(2, b"k", 10).unwrap();
    harness.settle(1).await;
    assert_eq!(harness.delivered(2).await, vec![1, 2]);
    assert_eq!(harness.store.appends.load(Ordering::SeqCst), appended);
}

#[tokio::test(start_paused = true)]
async fn discarding_a_retrying_head_in_memory_drops_it_and_sends_the_next() {
    let until = Instant::now() + Duration::from_secs(60);
    let script: Script = Box::new(move |ordinal, _| {
        if ordinal == 0 && Instant::now() < until {
            Outcome::Retryable(SinkError("throttled".to_owned()))
        } else {
            Outcome::Delivered
        }
    });
    let mut harness = Harness::start(
        parking_after(Duration::from_secs(1)),
        ScriptedSink::new(LIMITS, script),
        TestStore::default(),
    )
    .await;
    harness.send(0, b"k", 10).unwrap();
    harness.send(1, b"k", 10).unwrap();
    harness.settle(2).await;
    tokio::time::sleep(Duration::from_secs(10)).await;
    assert_eq!(
        harness.store.queue(b"k").await,
        vec![(0, ParkStatus::Retrying), (1, ParkStatus::Retrying)]
    );
    let attempts = |harness: &Harness| {
        harness
            .sink
            .sends()
            .iter()
            .filter(|sent| sent.ordinal == 0)
            .count()
    };
    let before = attempts(&harness);

    assert_eq!(harness.operator().discard(k(), id(0)).await, Ok(id(0)));
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(harness.delivered(1).await, vec![1]);
    tokio::time::sleep(Duration::from_secs(120)).await;
    assert_eq!(attempts(&harness), before);
    assert_eq!(harness.store.queue(b"k").await, vec![]);
    assert_eq!(harness.gauges().events, 0);
}

#[tokio::test(start_paused = true)]
async fn a_discard_leaves_no_stale_retry_to_ready_the_key_twice() {
    let script: Script = Box::new(|ordinal, _| {
        if ordinal == 0 {
            Outcome::Retryable(SinkError("throttled".to_owned()))
        } else {
            Outcome::Delivered
        }
    });
    let mut sink = ScriptedSink::new(LIMITS, script);
    sink.latency = Duration::from_secs(1);
    let mut harness = Harness::start(
        parking_after(Duration::from_secs(1)),
        sink,
        TestStore::default(),
    )
    .await;
    harness.send(0, b"k", 10).unwrap();
    harness.settle(1).await;
    tokio::time::sleep(Duration::from_secs(40)).await;

    assert_eq!(harness.operator().discard(k(), id(0)).await, Ok(id(0)));
    tokio::time::sleep(Duration::from_millis(1)).await;
    for ordinal in 1..=60 {
        harness.send(ordinal, b"k", 10).unwrap();
    }
    harness.settle(60).await;
    let delivered: Vec<u32> = harness
        .sink
        .sends()
        .iter()
        .filter(|sent| sent.delivered)
        .map(|sent| sent.ordinal)
        .collect();
    assert_eq!(delivered, (1..=60).collect::<Vec<u32>>());
}

fn three_keys_in_one_partition() -> Vec<Vec<u8>> {
    let mut by_partition: HashMap<u8, Vec<Vec<u8>>> = HashMap::new();
    for number in 0..4000 {
        let key = format!("key-{number}").into_bytes();
        let keys = by_partition
            .entry(OrderKeyId::of(&key).partition())
            .or_default();
        keys.push(key);
        if keys.len() == 3 {
            return keys.clone();
        }
    }
    panic!("no three keys share a partition");
}

#[tokio::test(start_paused = true)]
async fn a_discard_takes_the_key_out_of_the_ready_queue() {
    let limits = SinkCapabilities {
        max_batch_bytes: 2,
        ..LIMITS
    };
    let mut sink = ScriptedSink::new(limits, Box::new(|_, _| Outcome::Delivered));
    sink.latency = Duration::from_millis(100);
    let keys = three_keys_in_one_partition();
    let store = TestStore::default();
    for (index, key) in keys.iter().enumerate() {
        let ordinal = u32::try_from(index).unwrap();
        store.seed(key, ordinal, ParkStatus::Retrying).await;
        store.seed(key, ordinal + 3, ParkStatus::Retrying).await;
    }
    let harness = Harness::start(Setup::default(), sink, store).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let operator = harness.operator();
    let (first, second, third) = tokio::join!(
        operator.discard(OrderKeyId::of(&keys[0]), id(0)),
        operator.discard(OrderKeyId::of(&keys[1]), id(1)),
        operator.discard(OrderKeyId::of(&keys[2]), id(2))
    );
    assert_eq!((first, second, third), (Ok(id(0)), Ok(id(1)), Ok(id(2))));
    tokio::time::sleep(Duration::from_secs(5)).await;
    let batches = harness.sink.batches();
    assert!(batches.iter().all(|batch| !batch.is_empty()), "{batches:?}");
    let mut sent: Vec<u32> = batches.concat();
    sent.retain(|ordinal| *ordinal >= 3);
    sent.sort_unstable();
    assert_eq!(sent, vec![3, 4, 5]);
    for key in &keys {
        assert_eq!(harness.store.queue(key).await, vec![]);
    }
}

#[tokio::test(start_paused = true)]
async fn discarding_a_delivered_head_before_its_removal_is_flushed_removes_it_once() {
    let mut sink = ScriptedSink::delivering();
    sink.latency = Duration::from_millis(100);
    let store = TestStore::default();
    for ordinal in 0..3 {
        store.seed(b"a", ordinal, ParkStatus::Retrying).await;
    }
    store.seed(b"b", 3, ParkStatus::Dead).await;
    let harness = Harness::start(Setup::default(), sink, store).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        harness
            .operator()
            .discard(OrderKeyId::of(b"a"), id(0))
            .await,
        Ok(id(0))
    );
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(harness.sink.batches(), vec![vec![0], vec![1], vec![2]]);
    assert_eq!(harness.store.queue(b"a").await, vec![]);
    assert_eq!(harness.gauges().events, 1);
}

#[tokio::test(start_paused = true)]
async fn discarding_heads_held_in_memory_frees_room_for_the_next_heads() {
    let limits = SinkCapabilities {
        max_batch_messages: 2,
        max_batch_bytes: 1,
        ..LIMITS
    };
    let mut sink = ScriptedSink::new(limits, Box::new(|_, _| Outcome::Delivered));
    sink.latency = Duration::from_millis(100);
    let keys = three_keys_in_one_partition();
    let store = TestStore::default();
    for round in 0..4 {
        for (index, key) in keys.iter().take(2).enumerate() {
            store
                .seed(
                    key,
                    2 * round + u32::try_from(index).unwrap(),
                    ParkStatus::Retrying,
                )
                .await;
        }
    }
    let harness = Harness::start(Setup::default(), sink, store).await;
    let operator = harness.operator();
    for round in 0..4 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let (first, second) = tokio::join!(
            operator.discard(OrderKeyId::of(&keys[0]), id(2 * round)),
            operator.discard(OrderKeyId::of(&keys[1]), id(2 * round + 1))
        );
        assert_eq!(
            (first, second),
            (Ok(id(2 * round)), Ok(id(2 * round + 1))),
            "round {round}"
        );
    }
    tokio::time::sleep(Duration::from_secs(5)).await;
    for key in keys.iter().take(2) {
        assert_eq!(harness.store.queue(key).await, vec![]);
    }
    assert_eq!(harness.gauges().events, 0);
}

#[tokio::test(start_paused = true)]
async fn a_redriven_head_retries_from_the_start_of_the_backoff() {
    let script: Script = Box::new(|_, attempt| match attempt {
        1..=10 | 12 => Outcome::Retryable(SinkError("throttled".to_owned())),
        11 => Outcome::Terminal(SinkError("rejected".to_owned())),
        _ => Outcome::Delivered,
    });
    let store = TestStore::default();
    store.seed(b"k", 0, ParkStatus::Retrying).await;
    let harness = Harness::start(Setup::default(), ScriptedSink::new(LIMITS, script), store).await;
    tokio::time::sleep(Duration::from_secs(600)).await;
    assert_eq!(harness.store.queue(b"k").await, vec![(0, ParkStatus::Dead)]);
    let redriven_at = Instant::now();
    assert_eq!(harness.operator().redrive(k()).await, Ok(id(0)));
    tokio::time::sleep(Duration::from_secs(60)).await;
    let sends = harness.sink.sends();
    let last = sends.last().unwrap();
    assert!(last.delivered);
    assert_eq!(sends.len(), 13);
    assert!(
        last.at - redriven_at <= RETRY_BASE,
        "{:?}",
        last.at - redriven_at
    );
}

#[tokio::test(start_paused = true)]
async fn redrive_and_discard_of_a_head_that_is_still_being_parked_are_refused() {
    let store = TestStore::default();
    store.fail_appends.store(u32::MAX, Ordering::SeqCst);
    let mut harness = Harness::start(
        Setup::default(),
        ScriptedSink::new(LIMITS, always_terminal()),
        store,
    )
    .await;
    harness.send(0, b"k", 10).unwrap();
    tokio::time::sleep(Duration::from_secs(5)).await;
    let operator = harness.operator();
    assert_eq!(
        operator.redrive(k()).await,
        Err(OperateError::Parking { key: k() })
    );
    assert_eq!(
        operator.discard(k(), id(0)).await,
        Err(OperateError::Parking { key: k() })
    );
    assert_eq!(harness.store.status_changes.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn discard_while_the_head_is_still_being_parked_is_refused() {
    let store = TestStore::default();
    store.fail_appends.store(u32::MAX, Ordering::SeqCst);
    store.land_on_failure.store(1, Ordering::SeqCst);
    let mut harness = Harness::start(
        parking_after(Duration::from_secs(1)),
        ScriptedSink::new(LIMITS, always_retryable()),
        store,
    )
    .await;
    harness.send(0, b"k", 10).unwrap();
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(
        harness.store.queue(b"k").await,
        vec![(0, ParkStatus::Retrying)]
    );
    assert_eq!(
        harness.operator().discard(k(), id(0)).await,
        Err(OperateError::Parking { key: k() })
    );
    assert_eq!(
        harness.store.queue(b"k").await,
        vec![(0, ParkStatus::Retrying)]
    );
}

#[tokio::test(start_paused = true)]
async fn discard_cancels_a_pending_mark_dead() {
    let store = TestStore::default();
    store.seed(b"k", 0, ParkStatus::Retrying).await;
    store.fail_status_changes.store(3, Ordering::SeqCst);
    let harness = Harness::start(
        Setup::default(),
        ScriptedSink::new(LIMITS, always_terminal()),
        store,
    )
    .await;
    for _ in 0..1000 {
        if harness.store.status_changes.load(Ordering::SeqCst) > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(harness.operator().discard(k(), id(0)).await, Ok(id(0)));
    let changes = harness.store.status_changes.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert_eq!(harness.store.status_changes.load(Ordering::SeqCst), changes);
    assert_eq!(harness.store.queue(b"k").await, vec![]);
}

#[tokio::test(start_paused = true)]
async fn a_failed_status_change_reports_the_error_and_changes_nothing() {
    let store = TestStore::default();
    store.fail_status_changes.store(1, Ordering::SeqCst);
    let harness = dead_head_behind(2, store).await;
    let operator = harness.operator();
    assert!(matches!(
        operator.redrive(k()).await,
        Err(OperateError::Store(StoreError::Unavailable(_)))
    ));
    assert_eq!(
        harness.store.queue(b"k").await,
        vec![(0, ParkStatus::Dead), (1, ParkStatus::Retrying)]
    );
    assert_eq!(harness.gauges().dead, 1);
    assert_eq!(operator.redrive(k()).await, Ok(id(0)));
    assert_eq!(harness.delivered(2).await, vec![0, 1]);
}

#[tokio::test(start_paused = true)]
async fn a_failed_removal_reports_the_error_and_changes_nothing() {
    let store = TestStore::default();
    store.fail_removes.store(1, Ordering::SeqCst);
    let harness = dead_head_behind(2, store).await;
    let operator = harness.operator();
    assert!(matches!(
        operator.discard(k(), id(0)).await,
        Err(OperateError::Store(StoreError::Unavailable(_)))
    ));
    assert_eq!(
        harness.store.queue(b"k").await,
        vec![(0, ParkStatus::Dead), (1, ParkStatus::Retrying)]
    );
    assert_eq!(harness.gauges().events, 2);
    assert_eq!(operator.discard(k(), id(0)).await, Ok(id(0)));
    assert_eq!(harness.delivered(1).await, vec![1]);
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(harness.gauges().events, 0);
}

#[tokio::test(start_paused = true)]
async fn operations_after_the_lanes_stop_report_stopped() {
    let harness = Harness::new(2, 1, ScriptedSink::delivering()).await;
    let operator = harness.operator();
    harness.lanes.stop(Duration::ZERO).await;
    assert_eq!(operator.redrive(k()).await, Err(OperateError::Stopped));
    assert_eq!(
        operator.discard(k(), id(0)).await,
        Err(OperateError::Stopped)
    );
}

#[tokio::test(start_paused = true)]
async fn a_held_operator_never_delays_a_clean_stop() {
    let mut sink = ScriptedSink::delivering();
    sink.latency = Duration::from_millis(300);
    let mut harness = Harness::new(1, 1, sink).await;
    let operator = harness.operator();
    harness.send(0, b"k", 10).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let started = Instant::now();
    assert!(harness.lanes.stop(Duration::from_secs(1)).await);
    assert!(started.elapsed() <= Duration::from_millis(300));
    assert_eq!(operator.redrive(k()).await, Err(OperateError::Stopped));
}

#[tokio::test(start_paused = true)]
async fn a_lane_that_is_parking_everything_for_shutdown_refuses_operations() {
    let store = TestStore::default();
    store.seed(b"k", 0, ParkStatus::Dead).await;
    let harness = Harness::start(Setup::default(), ScriptedSink::delivering(), store).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    harness.lanes.park_all();
    let operator = harness.operator();
    assert_eq!(operator.redrive(k()).await, Err(OperateError::Stopped));
    assert_eq!(
        operator.discard(k(), id(0)).await,
        Err(OperateError::Stopped)
    );
    assert_eq!(harness.store.status_changes.load(Ordering::SeqCst), 0);
}
