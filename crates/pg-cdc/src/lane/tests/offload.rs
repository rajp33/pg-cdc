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
