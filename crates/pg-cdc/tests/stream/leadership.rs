use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use pg_cdc::config::DEFAULT_MAX_PARKED_EVENTS;
use pg_cdc::health::{Health, Role};
use pg_cdc::leader::record::TakeoverError;
use pg_cdc::order::{OrderBy, OrderKeyId};
use pg_cdc::session::{SessionConfig, SessionError};
use pg_cdc::state::memory::MemoryStateStore;
use pg_cdc::state::{LeaderRecord, LeaderState, StateStore, SwapOutcome, Versioned, now_ms};
use pg_cdc::validate::Invalid;
use pg_cdc_decoder::Lsn;
use pg_cdc_testkit::Postgres;
use serde_json::Value;

use crate::support::{self, LIMITS, Mode, RecordingStore, SLOT, TestResult, WAIT};

const FAST: Duration = Duration::from_millis(200);
const BUFFER: usize = 64;
const TWO_POLLS: Duration = Duration::from_millis(4500);
const TAKEOVER: Duration = Duration::from_secs(4);

async fn orders(pg: &Postgres) -> TestResult {
    support::prepare(
        pg,
        &["CREATE TABLE orders (id int PRIMARY KEY, status text)"],
        &["orders"],
    )
    .await
}

async fn standby_ready(health: &Health) -> TestResult {
    tokio::time::timeout(
        WAIT,
        health
            .subscribe()
            .wait_for(|snapshot| snapshot.validated && snapshot.role == Role::Standby),
    )
    .await??;
    Ok(())
}

fn memory() -> Arc<dyn StateStore> {
    Arc::new(MemoryStateStore::new())
}

async fn leader(store: &Arc<dyn StateStore>) -> TestResult<Versioned<LeaderRecord>> {
    Ok(store
        .read_leader()
        .await?
        .ok_or("the leader record is missing")?)
}

fn keyed() -> SessionConfig {
    let mut config = support::config(FAST, BUFFER);
    config.order_by = OrderBy::Key;
    config.lanes = 4;
    config.park_after = Duration::from_millis(500);
    config.max_parked_events = DEFAULT_MAX_PARKED_EVENTS;
    config
}

fn row(event: &Value) -> i64 {
    event
        .pointer("/after/id")
        .and_then(Value::as_i64)
        .unwrap_or(-1)
}

fn id(event: &Value) -> String {
    event
        .pointer("/id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

async fn active(health: &Health, within: Duration) -> TestResult {
    tokio::time::timeout(
        within,
        health
            .subscribe()
            .wait_for(|snapshot| snapshot.role == Role::Active),
    )
    .await??;
    Ok(())
}

#[tokio::test]
async fn l3_a_standby_never_writes_while_another_session_holds_the_slot() -> TestResult {
    let pg = Postgres::start().await?;
    orders(&pg).await?;
    let shared = memory();
    let store_a = RecordingStore::over(Arc::clone(&shared));
    let store_b = RecordingStore::over(Arc::clone(&shared));
    let mut a = support::spawn_in_context(
        support::params(&pg),
        support::config(FAST, BUFFER),
        store_a.stores(),
        LIMITS,
        support::context_for("holder-a"),
    );
    for id in 1..=3 {
        pg.execute(&format!("INSERT INTO orders VALUES ({id}, 'a')"))
            .await?;
    }
    let first = a.next_events(3).await?;
    assert_eq!(first.len(), 3);
    assert!(store_a.calls().contains(&"load_parked_heads"));

    let mut b = support::spawn_in_context(
        support::params(&pg),
        support::config(FAST, BUFFER),
        store_b.stores(),
        LIMITS,
        support::context_for("holder-b"),
    );
    let health = b.health();
    standby_ready(&health).await?;
    assert!(b.no_event_within(TWO_POLLS).await?);
    assert_eq!(store_b.calls(), Vec::<&str>::new());
    let snapshot = health.snapshot();
    assert!(snapshot.healthy(), "{snapshot:?}");
    assert!(!snapshot.ready(), "{snapshot:?}");
    assert_eq!(snapshot.role, Role::Standby);

    a.stop().await?;
    active(&health, TAKEOVER).await?;
    pg.execute("INSERT INTO orders VALUES (4, 'b')").await?;
    loop {
        let event = b.next_event().await?;
        if event["after"]["id"] == 4 {
            break;
        }
    }
    let record = leader(&shared).await?.value;
    assert_eq!(record.epoch, 2);
    assert_eq!(record.holder, "holder-b");
    assert_eq!(record.state, LeaderState::Active);
    b.stop().await
}

#[tokio::test]
async fn a_missing_publication_fails_validation_without_taking_the_slot() -> TestResult {
    let pg = Postgres::start().await?;
    pg.execute("CREATE TABLE orders (id int PRIMARY KEY, status text)")
        .await?;
    pg.create_slot(SLOT).await?;
    let session = support::spawn_with_stores(
        support::params(&pg),
        support::config(FAST, BUFFER),
        support::memory_stores(),
    );
    let health = session.health();

    let result = session.finished(Duration::from_secs(5)).await?;

    assert!(
        matches!(
            &result,
            Err(SessionError::Invalid(Invalid::PublicationMissing(_)))
        ),
        "{result:?}"
    );
    assert!(!health.snapshot().validated);
    assert!(!health.snapshot().healthy());
    let slot = pg.slot_state(SLOT).await?.ok_or("the slot disappeared")?;
    assert!(!slot.active);
    Ok(())
}

#[tokio::test]
async fn a_dropped_slot_fails_validation_until_it_is_recreated() -> TestResult {
    let pg = Postgres::start().await?;
    orders(&pg).await?;
    let context = support::context();
    let mut first = support::spawn_in_context(
        support::params(&pg),
        support::config(FAST, BUFFER),
        support::memory_stores(),
        LIMITS,
        context.clone(),
    );
    pg.execute("INSERT INTO orders VALUES (1, 'before')")
        .await?;
    assert_eq!(first.next_event().await?["after"]["id"], 1);
    assert!(context.health.snapshot().validated);

    assert!(pg.terminate_walsender(SLOT).await?);
    pg.wait_for_slot(SLOT, WAIT, |state| !state.active).await?;
    pg.execute(&format!("SELECT pg_drop_replication_slot('{SLOT}')"))
        .await?;
    assert!(first.finished(WAIT).await?.is_err());

    let second = support::spawn_in_context(
        support::params(&pg),
        support::config(FAST, BUFFER),
        support::memory_stores(),
        LIMITS,
        context.clone(),
    );
    let result = second.finished(Duration::from_secs(5)).await?;
    assert!(
        matches!(&result, Err(SessionError::Invalid(Invalid::SlotMissing(slot))) if slot == SLOT),
        "{result:?}"
    );
    assert!(!context.health.snapshot().validated);
    assert!(pg.slot_state(SLOT).await?.is_none());

    pg.create_slot(SLOT).await?;
    let mut third = support::spawn_in_context(
        support::params(&pg),
        support::config(FAST, BUFFER),
        support::memory_stores(),
        LIMITS,
        context.clone(),
    );
    pg.execute("INSERT INTO orders VALUES (2, 'after')").await?;
    assert_eq!(third.next_event().await?["after"]["id"], 2);
    assert!(context.health.snapshot().validated);
    third.stop().await
}

#[tokio::test]
async fn l1_an_unclean_takeover_waits_fence_delay_before_delivering() -> TestResult {
    let pg = Postgres::start().await?;
    orders(&pg).await?;
    let shared = memory();
    let mut a = support::spawn_in_context(
        support::params(&pg),
        support::config(FAST, BUFFER),
        RecordingStore::over(Arc::clone(&shared)).stores(),
        LIMITS,
        support::context_for("holder-a"),
    );
    pg.execute("INSERT INTO orders VALUES (1, 'a')").await?;
    assert_eq!(a.next_event().await?["after"]["id"], 1);
    a.abort().await?;
    let record = leader(&shared).await?.value;
    assert_eq!((record.epoch, record.state), (1, LeaderState::Active));
    pg.execute("INSERT INTO orders VALUES (2, 'b')").await?;

    let t0 = Instant::now();
    let store_b = RecordingStore::over(Arc::clone(&shared));
    let mut b = support::spawn_in_context(
        support::params(&pg),
        support::config(FAST, BUFFER),
        store_b.stores(),
        LIMITS,
        support::context_for("holder-b"),
    );
    tokio::time::sleep_until((t0 + Duration::from_secs(3)).into()).await;
    assert!(
        store_b.calls().iter().all(|call| *call == "read_leader"),
        "{:?}",
        store_b.calls()
    );
    b.next_event().await?;
    assert!(t0.elapsed() >= TWO_POLLS, "{:?}", t0.elapsed());
    let calls = store_b.calls();
    for write in ["append_parked", "remove_parked", "set_parked_status"] {
        assert!(!calls.contains(&write), "{calls:?}");
    }

    let record = leader(&shared).await?.value;
    assert_eq!(record.epoch, 2);
    assert_eq!(record.holder, "holder-b");
    assert_eq!(record.state, LeaderState::Active);
    b.stop().await
}

#[tokio::test]
async fn a_conflicting_leader_record_releases_the_slot_and_ends_the_attempt() -> TestResult {
    let pg = Postgres::start().await?;
    orders(&pg).await?;
    let shared = memory();
    let mut a = support::spawn_in_context(
        support::params(&pg),
        support::config(FAST, BUFFER),
        RecordingStore::over(Arc::clone(&shared)).stores(),
        LIMITS,
        support::context_for("holder-a"),
    );
    pg.execute("INSERT INTO orders VALUES (1, 'a')").await?;
    a.next_event().await?;
    a.abort().await?;

    let started = Instant::now();
    let store_b = RecordingStore::over(Arc::clone(&shared));
    let b = support::spawn_in_context(
        support::params(&pg),
        support::config(FAST, BUFFER),
        store_b.stores(),
        LIMITS,
        support::context_for("holder-b"),
    );
    active(&b.health(), WAIT).await?;
    store_b.wait_for_call("read_leader").await?;
    let current = leader(&shared).await?;
    let intruder = LeaderRecord {
        epoch: 7,
        holder: "intruder".to_owned(),
        state: LeaderState::Active,
        confirmed_lsn: Lsn(0),
        updated_ms: now_ms(),
    };
    let swapped = shared
        .swap_leader(&intruder, Some(&current.version))
        .await?;
    assert!(matches!(swapped, SwapOutcome::Swapped(_)));

    let budget = TWO_POLLS + TAKEOVER;
    let remaining = budget.saturating_sub(started.elapsed());
    let (result, delivered) = b.finished_with_output(remaining).await?;

    assert!(
        matches!(result, Err(SessionError::Takeover(TakeoverError::Conflict))),
        "{result:?}"
    );
    assert_eq!(delivered, 0);
    pg.wait_for_slot(SLOT, WAIT, |state| !state.active).await?;
    let record = leader(&shared).await?.value;
    assert_eq!(record.epoch, 7);
    assert_eq!(record.holder, "intruder");
    Ok(())
}

#[tokio::test]
async fn a_session_on_a_fresh_store_claims_epoch_one_at_once() -> TestResult {
    let pg = Postgres::start().await?;
    orders(&pg).await?;
    let shared = memory();
    let mut session = support::spawn_in_context(
        support::params(&pg),
        support::config(FAST, BUFFER),
        RecordingStore::over(Arc::clone(&shared)).stores(),
        LIMITS,
        support::context_for("holder-a"),
    );
    let health = session.health();
    active(&health, WAIT).await?;
    pg.execute("INSERT INTO orders VALUES (1, 'a')").await?;

    let event = tokio::time::timeout(Duration::from_secs(3), session.next_event()).await??;

    assert_eq!(event["after"]["id"], 1);
    let record = leader(&shared).await?.value;
    assert_eq!(record.epoch, 1);
    assert_eq!(record.holder, "holder-a");
    assert_eq!(record.state, LeaderState::Active);
    assert!(health.snapshot().ready(), "{:?}", health.snapshot());
    session.stop().await
}

async fn late_parked(
    shared: &Arc<dyn StateStore>,
) -> TestResult<BTreeMap<i64, Vec<pg_cdc::event::EventId>>> {
    let mut parked = BTreeMap::new();
    for row in 1..=3 {
        let key = OrderKeyId::of(&support::order_key(row));
        let events = shared.read_parked(&key, None, 100).await?;
        if !events.is_empty() {
            parked.insert(row, events.iter().map(|event| event.id).collect());
        }
    }
    Ok(parked)
}

fn assert_ids_never_decrease(events: &[Value]) {
    let mut last: BTreeMap<i64, String> = BTreeMap::new();
    for event in events {
        let current = id(event);
        if let Some(previous) = last.insert(row(event), current.clone()) {
            assert!(previous <= current, "{previous} then {current}");
        }
    }
}

#[tokio::test]
async fn a_stale_park_that_lands_after_the_takeover_yields_duplicates_not_loss() -> TestResult {
    let pg = Postgres::start().await?;
    orders(&pg).await?;
    let shared = memory();
    let store_a = RecordingStore::over(Arc::clone(&shared));
    store_a.detach_appends(Duration::from_secs(10));
    let a = support::spawn_in_context(
        support::params(&pg),
        keyed(),
        store_a.stores(),
        LIMITS,
        support::context_for("holder-a"),
    );
    a.set_mode(Mode::Fail);
    let health_a = a.health();
    tokio::time::timeout(
        WAIT,
        health_a
            .subscribe()
            .wait_for(|snapshot| snapshot.delivering),
    )
    .await??;
    for row in 1..=3 {
        pg.execute(&format!("INSERT INTO orders VALUES ({row}, 'a')"))
            .await?;
    }
    store_a.wait_for_call("append_parked").await?;
    let started_appends = store_a
        .calls()
        .iter()
        .filter(|call| **call == "append_parked")
        .count();
    let t_abort = Instant::now();
    a.abort().await?;

    let store_b = RecordingStore::over(Arc::clone(&shared));
    let mut b = support::spawn_in_context(
        support::params(&pg),
        keyed(),
        store_b.stores(),
        LIMITS,
        support::context_for("holder-b"),
    );
    let mut from_b = Vec::new();
    let mut rows_b = BTreeSet::new();
    while rows_b.len() < 3 {
        let event = b.next_event().await?;
        if from_b.is_empty() {
            assert!(t_abort.elapsed() >= TWO_POLLS, "{:?}", t_abort.elapsed());
        }
        rows_b.insert(row(&event));
        from_b.push(event);
    }
    assert_eq!(rows_b, BTreeSet::from([1, 2, 3]));
    assert_eq!(leader(&shared).await?.value.epoch, 2);
    assert!(late_parked(&shared).await?.is_empty());

    store_a.wait_for_landed(started_appends).await?;
    let late = late_parked(&shared).await?;
    assert!(!late.is_empty());
    pg.execute("INSERT INTO orders VALUES (4, 'b')").await?;
    loop {
        let event = b.next_event().await?;
        let done = row(&event) == 4;
        from_b.push(event);
        if done {
            break;
        }
    }
    assert_ids_never_decrease(&from_b);
    b.stop().await?;

    let store_c = RecordingStore::over(Arc::clone(&shared));
    let mut c = support::spawn_in_context(
        support::params(&pg),
        keyed(),
        store_c.stores(),
        LIMITS,
        support::context_for("holder-c"),
    );
    let wanted: BTreeSet<String> = late.values().flatten().map(ToString::to_string).collect();
    let mut from_c = Vec::new();
    let mut seen = BTreeSet::new();
    while !wanted.is_subset(&seen) {
        let event = c.next_event().await?;
        seen.insert(id(&event));
        from_c.push(event);
    }
    assert_ids_never_decrease(&from_c);
    tokio::time::timeout(WAIT, async {
        while !late_parked(&shared).await?.is_empty() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        TestResult::Ok(())
    })
    .await??;
    c.stop().await?;

    let arrived: BTreeSet<i64> = from_b.iter().chain(&from_c).map(row).collect();
    assert_eq!(arrived, BTreeSet::from([1, 2, 3, 4]));
    Ok(())
}

#[tokio::test]
async fn a_shutdown_during_the_fence_wait_leaves_the_record_untouched() -> TestResult {
    let pg = Postgres::start().await?;
    orders(&pg).await?;
    let shared = memory();
    let mut a = support::spawn_in_context(
        support::params(&pg),
        support::config(FAST, BUFFER),
        RecordingStore::over(Arc::clone(&shared)).stores(),
        LIMITS,
        support::context_for("holder-a"),
    );
    pg.execute("INSERT INTO orders VALUES (1, 'a')").await?;
    a.next_event().await?;
    a.abort().await?;
    let before = leader(&shared).await?;

    let store_b = RecordingStore::over(Arc::clone(&shared));
    let b = support::spawn_in_context(
        support::params(&pg),
        support::config(FAST, BUFFER),
        store_b.stores(),
        LIMITS,
        support::context_for("holder-b"),
    );
    active(&b.health(), WAIT).await?;
    store_b.wait_for_call("read_leader").await?;
    let calls = store_b.calls();
    let started = Instant::now();
    b.stop().await?;

    assert!(started.elapsed() < TWO_POLLS, "{:?}", started.elapsed());
    assert!(!calls.contains(&"swap_leader"), "{calls:?}");
    assert_eq!(leader(&shared).await?, before);
    pg.wait_for_slot(SLOT, WAIT, |state| !state.active).await?;
    Ok(())
}
