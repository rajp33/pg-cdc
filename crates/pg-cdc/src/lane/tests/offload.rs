use super::*;

fn limited(max_message_bytes: usize) -> ScriptedSink {
    let limits = SinkCapabilities {
        max_message_bytes,
        ..LIMITS
    };
    ScriptedSink::new(limits, Box::new(|_, _| Outcome::Delivered))
}

fn pointer(ordinal: u32, bytes: usize) -> String {
    let id = EventId::new(Lsn(0x10), ordinal);
    format!(
        r#"{{"id":"{id}","op":"insert","key":{{"id":1}},"payload_ref":{{"uri":"memory://{id}","bytes":{bytes}}},"source":{{"pipeline":"p"}}}}"#
    )
}

#[tokio::test(start_paused = true)]
async fn an_oversize_event_is_sent_as_a_pointer_to_its_stored_payload() {
    let payloads = Arc::new(TestPayloads::default());
    let mut harness = Harness::start_with_payloads(
        Setup::default(),
        limited(300),
        TestStore::default(),
        Some(Arc::clone(&payloads)),
    )
    .await;
    let full = envelope(0, 400);
    harness.send_body(0, b"k", full.clone()).unwrap();
    harness.send(1, b"small", 10).unwrap();
    assert_eq!(harness.settle(2).await, Lsn(101));
    let id = EventId::new(Lsn(0x10), 0);
    assert_eq!(payloads.stored.lock().unwrap().get(&id), Some(&full));
    let bodies = harness.sink.bodies.lock().unwrap().clone();
    assert_eq!(
        std::str::from_utf8(&bodies[&0]).unwrap(),
        pointer(0, full.len())
    );
    assert_eq!(bodies[&1], Bytes::from(vec![b'x'; 10]));
    assert_eq!(harness.store.inner.parked_events(), 0);
}

#[tokio::test(start_paused = true)]
async fn an_event_pushed_over_the_limit_by_changed_becomes_a_pointer_that_keeps_it() {
    let mut update = crate::event::sample_envelope(
        crate::event::Op::Update,
        Some(serde_json::json!({"id": 42})),
        Some(serde_json::json!({"id": 42, "status": "x".repeat(200)})),
        Some(serde_json::json!({"id": 42, "status": "y".repeat(200)})),
    );
    let without = Bytes::from(update.encode().unwrap());
    update.changed = Some(vec!["status".to_owned()]);
    let with = Bytes::from(update.encode().unwrap());
    let payloads = Arc::new(TestPayloads::default());
    let mut harness = Harness::start_with_payloads(
        Setup::default(),
        limited(without.len()),
        TestStore::default(),
        Some(Arc::clone(&payloads)),
    )
    .await;
    harness.send_body(0, b"with", with.clone()).unwrap();
    harness.send_body(1, b"without", without.clone()).unwrap();
    assert_eq!(harness.settle(2).await, Lsn(101));
    let stored = payloads.stored.lock().unwrap().clone();
    assert_eq!(
        stored.into_iter().collect::<Vec<_>>(),
        [(EventId::new(Lsn(0x10), 0), with)]
    );
    let bodies = harness.sink.bodies.lock().unwrap().clone();
    let pointer = std::str::from_utf8(&bodies[&0]).unwrap();
    assert!(
        pointer.contains(r#""changed":["status"],"payload_ref":{"#),
        "{pointer}"
    );
    assert_eq!(bodies[&1], without);
}

#[tokio::test(start_paused = true)]
async fn a_payload_store_outage_retries_and_then_parks_the_full_event() {
    let payloads = Arc::new(TestPayloads::default());
    payloads.fail.store(u32::MAX, Ordering::SeqCst);
    let setup = Setup {
        park_after: Duration::from_secs(1),
        ..Setup::default()
    };
    let mut harness =
        Harness::start_with_payloads(setup, limited(300), TestStore::default(), Some(payloads))
            .await;
    let full = envelope(0, 400);
    harness.send_body(0, b"k", full.clone()).unwrap();
    assert_eq!(harness.settle(1).await, Lsn(100));
    let queue = harness
        .store
        .inner
        .read_parked(&OrderKeyId::of(b"k"), None, 10)
        .await
        .unwrap();
    assert_eq!(queue.len(), 1);
    assert_eq!(queue[0].status, ParkStatus::Retrying);
    assert_eq!(queue[0].body, full);
    assert!(
        queue[0]
            .last_error
            .starts_with("cannot store the oversize payload"),
        "{}",
        queue[0].last_error
    );
    assert_eq!(harness.sink.sends(), vec![]);
}

#[tokio::test(start_paused = true)]
async fn a_pointer_still_above_the_limit_is_parked_dead() {
    let mut harness = Harness::start_with_payloads(
        Setup::default(),
        limited(120),
        TestStore::default(),
        Some(Arc::new(TestPayloads::default())),
    )
    .await;
    harness.send_body(0, b"k", envelope(0, 400)).unwrap();
    assert_eq!(harness.settle(1).await, Lsn(100));
    let queue = harness
        .store
        .inner
        .read_parked(&OrderKeyId::of(b"k"), None, 10)
        .await
        .unwrap();
    assert_eq!(queue[0].status, ParkStatus::Dead);
    assert!(
        queue[0].last_error.starts_with("the pointer event is"),
        "{}",
        queue[0].last_error
    );
    assert_eq!(harness.sink.sends(), vec![]);
}

#[tokio::test(start_paused = true)]
async fn a_parked_oversize_event_drains_as_a_pointer() {
    let store = TestStore::default();
    let full = envelope(3, 400);
    store
        .inner
        .append_parked(&[ParkedEvent {
            body: full.clone(),
            ..parked(b"k", 3, ParkStatus::Retrying)
        }])
        .await
        .unwrap();
    let payloads = Arc::new(TestPayloads::default());
    let harness = Harness::start_with_payloads(
        Setup::default(),
        limited(300),
        store,
        Some(Arc::clone(&payloads)),
    )
    .await;
    assert_eq!(harness.delivered(1).await, vec![3]);
    let bodies = harness.sink.bodies.lock().unwrap().clone();
    assert_eq!(
        std::str::from_utf8(&bodies[&3]).unwrap(),
        pointer(3, full.len())
    );
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(harness.store.queue(b"k").await, vec![]);
    assert_eq!(payloads.stored.lock().unwrap().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn oversize_parked_heads_waiting_for_offload_stay_within_one_batch() {
    let store = TestStore::default();
    for key in 0u8..24 {
        let ordinal = u32::from(key);
        store
            .inner
            .append_parked(&[ParkedEvent {
                body: envelope(ordinal, 400),
                ..parked(&[key], ordinal, ParkStatus::Retrying)
            }])
            .await
            .unwrap();
    }
    let limits = SinkCapabilities {
        max_batch_messages: 4,
        max_message_bytes: 300,
        ..LIMITS
    };
    let mut sink = ScriptedSink::new(limits, Box::new(|_, _| Outcome::Delivered));
    sink.latency = Duration::from_millis(10);
    let harness = Harness::start_with_payloads(
        Setup::default(),
        sink,
        store,
        Some(Arc::new(TestPayloads::default())),
    )
    .await;
    let mut delivered = harness.delivered(24).await;
    delivered.sort_unstable();
    assert_eq!(delivered, (0..24).collect::<Vec<u32>>());
    assert!(harness.store.peak_outstanding.load(Ordering::SeqCst) <= 4);
}

#[tokio::test(start_paused = true)]
async fn a_live_pointer_that_cannot_be_delivered_parks_the_full_event() {
    let payloads = Arc::new(TestPayloads::default());
    let limits = SinkCapabilities {
        max_message_bytes: 300,
        ..LIMITS
    };
    let script: Script = Box::new(|_, _| Outcome::Retryable(SinkError("down".to_owned())));
    let setup = Setup {
        park_after: Duration::from_secs(1),
        ..Setup::default()
    };
    let mut harness = Harness::start_with_payloads(
        setup,
        ScriptedSink::new(limits, script),
        TestStore::default(),
        Some(Arc::clone(&payloads)),
    )
    .await;
    let full = envelope(0, 400);
    harness.send_body(0, b"k", full.clone()).unwrap();
    assert_eq!(harness.settle(1).await, Lsn(100));
    let queue = harness
        .store
        .inner
        .read_parked(&OrderKeyId::of(b"k"), None, 10)
        .await
        .unwrap();
    assert_eq!(queue.len(), 1);
    assert_eq!(queue[0].body, full);
    let bodies = harness.sink.bodies.lock().unwrap().clone();
    assert_eq!(
        std::str::from_utf8(&bodies[&0]).unwrap(),
        pointer(0, full.len())
    );
}
