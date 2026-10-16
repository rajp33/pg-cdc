use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use pg_cdc::config::{DEFAULT_MAX_PARKED_EVENTS, LeadershipConfig};
use pg_cdc::connection::{ConnectError, ConnectParams, ConnectionProvider};
use pg_cdc::health::{Health, Role};
use pg_cdc::leader::POLL_INTERVAL;
use pg_cdc::leader::lease::FenceReason;
use pg_cdc::leader::record::TakeoverError;
use pg_cdc::order::{OrderBy, OrderKeyId};
use pg_cdc::session::{CONTROL_QUERY_TIMEOUT, HANDOVER_TAIL, SessionConfig, SessionError};
use pg_cdc::state::memory::MemoryStateStore;
use pg_cdc::state::{LeaderRecord, LeaderState, StateStore, SwapOutcome, Versioned, now_ms};
use pg_cdc::validate::Invalid;
use pg_cdc_decoder::Lsn;
use pg_cdc_testkit::Postgres;
use secrecy::SecretString;
use serde_json::Value;

use crate::support::{self, ControlProxy, LIMITS, Mode, RecordingStore, SLOT, TestResult, WAIT};

const FAST: Duration = Duration::from_millis(200);
const BUFFER: usize = 64;
const TWO_POLLS: Duration = Duration::from_millis(4500);
const TAKEOVER: Duration = Duration::from_secs(4);
const TAKEOVER_AFTER_HANDOVER: Duration = Duration::from_secs(3);
const HANDOVER_DRAIN: Duration = Duration::from_secs(1);

fn handover() -> SessionConfig {
    let mut config = support::config(FAST, BUFFER);
    config.leadership = LeadershipConfig {
        drain_timeout: HANDOVER_DRAIN,
        ..support::TEST_LEADERSHIP
    };
    config
}

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
    let stopped = Instant::now();
    pg.execute("INSERT INTO orders VALUES (4, 'b')").await?;
    active(&health, TAKEOVER).await?;
    loop {
        let event = b.next_event().await?;
        if event["after"]["id"] == 4 {
            break;
        }
    }
    assert!(
        stopped.elapsed() < TAKEOVER_AFTER_HANDOVER,
        "{:?}",
        stopped.elapsed()
    );
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

async fn without_replication(pg: &Postgres) -> TestResult<ConnectParams> {
    pg.execute("CREATE ROLE no_replication LOGIN PASSWORD 'no_replication'")
        .await?;
    Ok(ConnectParams {
        user: "no_replication".to_owned(),
        password: Some(SecretString::from("no_replication".to_owned())),
        ..support::params(pg)
    })
}

fn replication_denied(result: &Result<(), SessionError>) -> bool {
    matches!(
        result,
        Err(SessionError::Invalid(Invalid::ReplicationDenied(reason))) if reason.contains("42501")
    )
}

#[tokio::test]
async fn a_role_without_replication_fails_validation_without_taking_a_free_slot() -> TestResult {
    let pg = Postgres::start().await?;
    orders(&pg).await?;
    let params = without_replication(&pg).await?;
    let context = support::context();
    for _ in 0..2 {
        let session = support::spawn_in_context(
            params.clone(),
            support::config(FAST, BUFFER),
            support::memory_stores(),
            LIMITS,
            context.clone(),
        );
        let result = session.finished(Duration::from_secs(5)).await?;
        assert!(replication_denied(&result), "{result:?}");
        let snapshot = context.health.snapshot();
        assert!(!snapshot.validated, "{snapshot:?}");
        assert!(!snapshot.healthy(), "{snapshot:?}");
        let slot = pg.slot_state(SLOT).await?.ok_or("the slot disappeared")?;
        assert!(!slot.active);
    }
    Ok(())
}

#[tokio::test]
async fn a_role_without_replication_never_turns_healthy_beside_the_holder() -> TestResult {
    let pg = Postgres::start().await?;
    orders(&pg).await?;
    let mut holder = support::spawn_in_context(
        support::params(&pg),
        support::config(FAST, BUFFER),
        support::memory_stores(),
        LIMITS,
        support::context_for("holder-a"),
    );
    pg.execute("INSERT INTO orders VALUES (1, 'a')").await?;
    assert_eq!(holder.next_event().await?["after"]["id"], 1);
    let walsender = pg
        .slot_state(SLOT)
        .await?
        .and_then(|state| state.active_pid);

    let context = support::context_for("holder-b");
    let replacement = support::spawn_in_context(
        without_replication(&pg).await?,
        support::config(FAST, BUFFER),
        support::memory_stores(),
        LIMITS,
        context.clone(),
    );
    let result = replacement.finished(Duration::from_secs(5)).await?;

    assert!(replication_denied(&result), "{result:?}");
    let snapshot = context.health.snapshot();
    assert!(!snapshot.validated, "{snapshot:?}");
    assert!(!snapshot.healthy(), "{snapshot:?}");
    assert_eq!(snapshot.role, Role::Standby);
    assert_eq!(
        pg.slot_state(SLOT)
            .await?
            .and_then(|state| state.active_pid),
        walsender
    );
    pg.execute("INSERT INTO orders VALUES (2, 'a')").await?;
    assert_eq!(holder.next_event().await?["after"]["id"], 2);
    holder.stop().await
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

const LEASE_BOUND: Duration = Duration::from_millis(6500);

#[tokio::test]
async fn l1_a_session_whose_control_connection_dies_fences_within_lease_timeout() -> TestResult {
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
    pg.execute("INSERT INTO orders VALUES (1, 'a')").await?;
    assert_eq!(session.next_event().await?["after"]["id"], 1);
    assert_eq!(leader(&shared).await?.value.epoch, 1);

    pg.execute(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
         WHERE application_name = 'pg-cdc' AND backend_type = 'client backend'",
    )
    .await?;
    let t_kill = Instant::now();
    let result = session.finished(LEASE_BOUND).await?;

    assert!(
        matches!(result, Err(SessionError::Fenced(FenceReason::Expired))),
        "{result:?}"
    );
    assert!(t_kill.elapsed() <= LEASE_BOUND, "{:?}", t_kill.elapsed());
    pg.wait_for_slot(SLOT, Duration::from_secs(2), |state| !state.active)
        .await?;
    let record = leader(&shared).await?.value;
    assert_eq!((record.epoch, record.state), (1, LeaderState::Active));
    let snapshot = health.snapshot();
    assert!(!snapshot.lease_valid, "{snapshot:?}");
    assert_eq!(snapshot.role, Role::Standby);
    Ok(())
}

#[tokio::test]
async fn l1_a_session_stuck_in_a_catalog_lookup_fences_and_frees_the_slot() -> TestResult {
    let pg = Postgres::start().await?;
    support::prepare(
        &pg,
        &["CREATE TABLE stuck (id int PRIMARY KEY)"],
        &["stuck"],
    )
    .await?;
    let proxy = ControlProxy::start(&pg).await?;
    let shared = memory();
    let mut a = support::spawn_in_context(
        support::params_via(&proxy, &pg),
        support::config(FAST, BUFFER),
        RecordingStore::over(Arc::clone(&shared)).stores(),
        LIMITS,
        support::context_for("holder-a"),
    );
    pg.execute("INSERT INTO stuck VALUES (1)").await?;
    assert_eq!(a.next_event().await?["after"]["id"], 1);
    assert_eq!(leader(&shared).await?.value.epoch, 1);
    let walsender = pg
        .slot_state(SLOT)
        .await?
        .and_then(|state| state.active_pid)
        .ok_or("the slot has no walsender")?;

    let mut b = support::spawn_in_context(
        support::params(&pg),
        support::config(FAST, BUFFER),
        RecordingStore::over(Arc::clone(&shared)).stores(),
        LIMITS,
        support::context_for("holder-b"),
    );
    let health_b = b.health();
    standby_ready(&health_b).await?;

    proxy.freeze(0)?;
    let t_freeze = Instant::now();
    pg.execute("ALTER TABLE stuck ADD COLUMN note text").await?;
    pg.execute("INSERT INTO stuck VALUES (2, 'two')").await?;

    let (result, delivered_after_freeze) = a.finished_with_output(LEASE_BOUND).await?;
    assert!(
        matches!(result, Err(SessionError::Fenced(FenceReason::Expired))),
        "{result:?}"
    );
    assert!(
        t_freeze.elapsed() <= LEASE_BOUND,
        "{:?}",
        t_freeze.elapsed()
    );
    assert_eq!(delivered_after_freeze, 0);
    pg.wait_for_slot(SLOT, Duration::from_secs(2), |state| {
        state.active_pid != Some(walsender)
    })
    .await?;

    active(&health_b, Duration::from_secs(2)).await?;
    let budget = LEASE_BOUND + Duration::from_secs(2) + TWO_POLLS + Duration::from_secs(3);
    let remaining = budget.saturating_sub(t_freeze.elapsed());
    let event = tokio::time::timeout(remaining, b.next_event()).await??;
    assert_eq!(event["after"]["id"], 2);
    let record = leader(&shared).await?.value;
    assert_eq!(record.epoch, 2);
    assert_eq!(record.holder, "holder-b");
    b.stop().await
}

#[tokio::test]
async fn a_clean_handover_releases_the_record_at_the_confirmed_lsn() -> TestResult {
    let pg = Postgres::start().await?;
    orders(&pg).await?;
    let shared = memory();
    let mut a = support::spawn_in_context(
        support::params(&pg),
        handover(),
        RecordingStore::over(Arc::clone(&shared)).stores(),
        LIMITS,
        support::context_for("holder-a"),
    );
    for id in 1..=3 {
        pg.execute(&format!("INSERT INTO orders VALUES ({id}, 'a')"))
            .await?;
    }
    a.next_events(3).await?;

    a.request_stop();
    let (result, _) = a
        .finished_with_output(HANDOVER_DRAIN * 2 + HANDOVER_TAIL)
        .await?;

    assert!(result.is_ok(), "{result:?}");
    let slot = pg.slot_state(SLOT).await?.ok_or("the slot disappeared")?;
    let record = leader(&shared).await?.value;
    assert_eq!((record.epoch, record.state), (1, LeaderState::Released));
    assert_eq!(record.holder, "holder-a");
    assert_eq!(
        Some(record.confirmed_lsn.0),
        slot.confirmed_flush_lsn,
        "{record:?} {slot:?}"
    );

    let started = Instant::now();
    let mut b = support::spawn_in_context(
        support::params(&pg),
        handover(),
        RecordingStore::over(Arc::clone(&shared)).stores(),
        LIMITS,
        support::context_for("holder-b"),
    );
    pg.execute("INSERT INTO orders VALUES (4, 'b')").await?;
    let event = tokio::time::timeout(TAKEOVER_AFTER_HANDOVER, b.next_event()).await??;
    assert_eq!(event["after"]["id"], 4);
    assert!(
        started.elapsed() < support::TEST_LEADERSHIP.fence_delay,
        "{:?}",
        started.elapsed()
    );
    let record = leader(&shared).await?.value;
    assert_eq!(record.epoch, 2);
    assert_eq!(record.holder, "holder-b");
    assert_eq!(record.state, LeaderState::Active);
    b.stop().await
}

#[tokio::test]
async fn a_handover_with_a_stuck_sink_keeps_the_record_active_and_ends_within_the_budget()
-> TestResult {
    let pg = Postgres::start().await?;
    orders(&pg).await?;
    let shared = memory();
    let mut a = support::spawn_in_context(
        support::params(&pg),
        handover(),
        RecordingStore::over(Arc::clone(&shared)).stores(),
        LIMITS,
        support::context_for("holder-a"),
    );
    a.set_mode(Mode::Hold);
    active(&a.health(), WAIT).await?;
    pg.execute("INSERT INTO orders VALUES (1, 'a')").await?;
    a.wait_for_sink_call().await?;

    let stopping = Instant::now();
    a.request_stop();
    let budget = HANDOVER_DRAIN * 2 + HANDOVER_TAIL + Duration::from_secs(1);
    let (result, delivered) = a.finished_with_output(budget).await?;

    assert!(result.is_ok(), "{result:?}");
    assert_eq!(delivered, 0);
    assert!(stopping.elapsed() <= budget, "{:?}", stopping.elapsed());
    let record = leader(&shared).await?.value;
    assert_eq!((record.epoch, record.state), (1, LeaderState::Active));
    assert_eq!(record.holder, "holder-a");

    let started = Instant::now();
    let mut b = support::spawn_in_context(
        support::params(&pg),
        handover(),
        RecordingStore::over(Arc::clone(&shared)).stores(),
        LIMITS,
        support::context_for("holder-b"),
    );
    let event = b.next_event().await?;
    assert_eq!(event["after"]["id"], 1);
    assert!(
        started.elapsed() >= support::TEST_LEADERSHIP.fence_delay,
        "{:?}",
        started.elapsed()
    );
    assert_eq!(leader(&shared).await?.value.epoch, 2);
    b.stop().await
}

#[tokio::test]
async fn shutdown_before_the_claim_leaves_the_record_untouched() -> TestResult {
    let pg = Postgres::start().await?;
    orders(&pg).await?;
    let shared = memory();
    let mut a = support::spawn_in_context(
        support::params(&pg),
        handover(),
        RecordingStore::over(Arc::clone(&shared)).stores(),
        LIMITS,
        support::context_for("holder-a"),
    );
    pg.execute("INSERT INTO orders VALUES (1, 'a')").await?;
    a.next_event().await?;
    a.abort().await?;
    let before = leader(&shared).await?;
    assert_eq!(before.value.state, LeaderState::Active);

    let b = support::spawn_in_context(
        support::params(&pg),
        handover(),
        RecordingStore::over(Arc::clone(&shared)).stores(),
        LIMITS,
        support::context_for("holder-b"),
    );
    active(&b.health(), WAIT).await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    b.request_stop();
    let (result, delivered) = b
        .finished_with_output(HANDOVER_TAIL + Duration::from_secs(1))
        .await?;

    assert!(result.is_ok(), "{result:?}");
    assert_eq!(delivered, 0);
    assert_eq!(leader(&shared).await?, before);
    pg.wait_for_slot(SLOT, WAIT, |state| !state.active).await?;
    Ok(())
}

#[tokio::test]
async fn a_release_that_cannot_reach_the_store_still_exits_within_the_budget() -> TestResult {
    let pg = Postgres::start().await?;
    orders(&pg).await?;
    let shared = memory();
    let store = RecordingStore::over(Arc::clone(&shared));
    let a = support::spawn_in_context(
        support::params(&pg),
        handover(),
        store.stores(),
        LIMITS,
        support::context_for("holder-a"),
    );
    tokio::time::timeout(
        WAIT,
        a.health()
            .subscribe()
            .wait_for(|snapshot| snapshot.delivering),
    )
    .await??;
    store.stall_leader_swaps();

    let stopping = Instant::now();
    a.request_stop();
    let budget = HANDOVER_TAIL + Duration::from_secs(1);
    let (result, _) = a.finished_with_output(budget).await?;

    assert!(result.is_ok(), "{result:?}");
    assert!(stopping.elapsed() <= budget, "{:?}", stopping.elapsed());
    let swaps = store
        .calls()
        .iter()
        .filter(|call| **call == "swap_leader")
        .count();
    assert_eq!(swaps, 2, "{:?}", store.calls());
    let record = leader(&shared).await?.value;
    assert_eq!((record.epoch, record.state), (1, LeaderState::Active));
    pg.wait_for_slot(SLOT, WAIT, |state| !state.active).await?;
    Ok(())
}

const STOP_PROMPTLY: Duration = Duration::from_secs(1);

#[tokio::test]
async fn a_shutdown_during_validation_ends_the_attempt_at_once() -> TestResult {
    let pg = Postgres::start().await?;
    orders(&pg).await?;
    let proxy = ControlProxy::start(&pg).await?;
    proxy.freeze_next_once_ready();
    let store = RecordingStore::over(memory());
    let session = support::spawn_in_context(
        support::params_via(&proxy, &pg),
        handover(),
        store.stores(),
        LIMITS,
        support::context_for("holder-a"),
    );
    let health = session.health();
    proxy.wait_until_frozen(0).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let stopping = Instant::now();
    session.request_stop();
    let (result, delivered) = session.finished_with_output(STOP_PROMPTLY).await?;

    assert!(result.is_ok(), "{result:?}");
    assert!(
        stopping.elapsed() <= STOP_PROMPTLY,
        "{:?}",
        stopping.elapsed()
    );
    assert_eq!(delivered, 0);
    assert!(store.calls().is_empty(), "{:?}", store.calls());
    assert!(!health.snapshot().validated);
    let slot = pg.slot_state(SLOT).await?.ok_or("the slot disappeared")?;
    assert!(!slot.active, "{slot:?}");
    Ok(())
}

#[tokio::test]
async fn a_validation_query_that_never_answers_ends_the_attempt() -> TestResult {
    let pg = Postgres::start().await?;
    orders(&pg).await?;
    let proxy = ControlProxy::start(&pg).await?;
    proxy.freeze_next_once_ready();
    let session = support::spawn_in_context(
        support::params_via(&proxy, &pg),
        handover(),
        support::memory_stores(),
        LIMITS,
        support::context_for("holder-a"),
    );
    let health = session.health();
    proxy.wait_until_frozen(0).await?;
    let frozen = Instant::now();

    let result = session
        .finished(CONTROL_QUERY_TIMEOUT + Duration::from_secs(2))
        .await?;

    assert!(
        matches!(result, Err(SessionError::ControlStalled(_))),
        "{result:?}"
    );
    assert!(
        frozen.elapsed() + Duration::from_millis(500) >= CONTROL_QUERY_TIMEOUT,
        "{:?}",
        frozen.elapsed()
    );
    assert!(result.is_err_and(|error| !error.is_fatal()));
    assert!(!health.snapshot().validated);
    Ok(())
}

struct RotatingTokens {
    pg: Arc<Postgres>,
    issued: AtomicUsize,
}

impl RotatingTokens {
    fn issued(&self) -> usize {
        self.issued.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ConnectionProvider for RotatingTokens {
    async fn params(&self) -> Result<ConnectParams, ConnectError> {
        let token = format!("token-{}", self.issued.fetch_add(1, Ordering::SeqCst) + 1);
        self.pg
            .execute(&format!("ALTER ROLE rotating PASSWORD '{token}'"))
            .await
            .map_err(|error| ConnectError::Token(error.to_string()))?;
        Ok(ConnectParams {
            user: "rotating".to_owned(),
            password: Some(SecretString::from(token)),
            ..support::params(&self.pg)
        })
    }
}

#[tokio::test]
async fn a_standby_takes_over_with_credentials_issued_for_that_connection() -> TestResult {
    let pg = Arc::new(Postgres::start().await?);
    orders(&pg).await?;
    pg.execute("CREATE ROLE rotating LOGIN SUPERUSER REPLICATION")
        .await?;
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

    let tokens = Arc::new(RotatingTokens {
        pg: Arc::clone(&pg),
        issued: AtomicUsize::new(0),
    });
    let mut b = support::spawn_provided(
        Arc::clone(&tokens) as Arc<dyn ConnectionProvider>,
        support::config(FAST, BUFFER),
        RecordingStore::over(Arc::clone(&shared)).stores(),
        LIMITS,
        support::context_for("holder-b"),
    );
    let health = b.health();
    standby_ready(&health).await?;
    let issued_before_takeover = tokens.issued();
    pg.execute("ALTER ROLE rotating PASSWORD 'expired'").await?;

    a.stop().await?;
    pg.execute("INSERT INTO orders VALUES (2, 'b')").await?;
    active(&health, WAIT).await?;
    loop {
        if row(&b.next_event().await?) == 2 {
            break;
        }
    }
    assert!(tokens.issued() > issued_before_takeover);
    b.stop().await
}

#[tokio::test]
async fn a_standby_whose_control_connection_stalls_ends_the_attempt() -> TestResult {
    let pg = Postgres::start().await?;
    orders(&pg).await?;
    let shared = memory();
    let a = support::spawn_in_context(
        support::params(&pg),
        handover(),
        RecordingStore::over(Arc::clone(&shared)).stores(),
        LIMITS,
        support::context_for("holder-a"),
    );
    active(&a.health(), WAIT).await?;
    let proxy = ControlProxy::start(&pg).await?;
    let b = support::spawn_in_context(
        support::params_via(&proxy, &pg),
        handover(),
        RecordingStore::over(Arc::clone(&shared)).stores(),
        LIMITS,
        support::context_for("holder-b"),
    );
    let health_b = b.health();
    standby_ready(&health_b).await?;
    tokio::time::sleep(POLL_INTERVAL / 2).await;

    proxy.freeze(0)?;
    let frozen = Instant::now();
    let budget = CONTROL_QUERY_TIMEOUT + POLL_INTERVAL + Duration::from_secs(2);
    let result = b.finished(budget).await?;

    assert!(matches!(result, Err(SessionError::Probe(_))), "{result:?}");
    assert!(frozen.elapsed() <= budget, "{:?}", frozen.elapsed());
    assert!(health_b.snapshot().validated);
    a.stop().await
}

async fn delivering(health: &Health) -> TestResult {
    tokio::time::timeout(
        WAIT,
        health.subscribe().wait_for(|snapshot| snapshot.delivering),
    )
    .await??;
    Ok(())
}

async fn wait_for_swaps(store: &RecordingStore, count: usize) -> TestResult {
    tokio::time::timeout(WAIT, async {
        while store
            .calls()
            .iter()
            .filter(|call| **call == "swap_leader")
            .count()
            < count
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn the_final_status_update_reaches_the_slot_before_the_release() -> TestResult {
    let pg = Postgres::start().await?;
    orders(&pg).await?;
    let shared = memory();
    let store = RecordingStore::over(Arc::clone(&shared));
    let mut config = handover();
    config.status_interval = Duration::from_secs(60);
    let mut a = support::spawn_in_context(
        support::params(&pg),
        config,
        store.stores(),
        LIMITS,
        support::context_for("holder-a"),
    );
    delivering(&a.health()).await?;
    store.stall_leader_swaps();
    pg.execute("INSERT INTO orders VALUES (1, 'a')").await?;
    let commit = support::commit_lsn(&a.next_event().await?)?;
    let before = pg.slot_state(SLOT).await?.ok_or("the slot disappeared")?;
    assert!(
        before.confirmed_flush_lsn < Some(commit),
        "{before:?} {commit:#x}"
    );

    a.request_stop();
    wait_for_swaps(&store, 2).await?;
    pg.wait_for_slot(SLOT, Duration::from_secs(1), |state| {
        state.confirmed_flush_lsn > Some(commit)
    })
    .await?;
    let record = leader(&shared).await?.value;
    assert_eq!((record.epoch, record.state), (1, LeaderState::Active));

    let (result, _) = a
        .finished_with_output(HANDOVER_TAIL + Duration::from_secs(1))
        .await?;
    assert!(result.is_ok(), "{result:?}");
    Ok(())
}

const CONTROL: usize = 0;
const STREAMING: usize = 2;

#[tokio::test]
async fn a_final_status_update_that_cannot_be_sent_keeps_the_record_active() -> TestResult {
    let pg = Postgres::start().await?;
    orders(&pg).await?;
    let proxy = ControlProxy::start(&pg).await?;
    let shared = memory();
    let mut a = support::spawn_in_context(
        support::params_via(&proxy, &pg),
        handover(),
        RecordingStore::over(Arc::clone(&shared)).stores(),
        LIMITS,
        support::context_for("holder-a"),
    );
    delivering(&a.health()).await?;
    a.set_mode(Mode::Hold);
    pg.execute("INSERT INTO orders VALUES (1, 'a')").await?;
    a.wait_for_sink_call().await?;

    a.request_stop();
    proxy.freeze(CONTROL)?;
    proxy.sever(STREAMING)?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    a.set_mode(Mode::Deliver);
    let budget = HANDOVER_DRAIN * 2 + HANDOVER_TAIL + Duration::from_secs(1);
    let (result, delivered) = a.finished_with_output(budget).await?;

    assert!(result.is_ok(), "{result:?}");
    assert_eq!(delivered, 1);
    let record = leader(&shared).await?.value;
    assert_eq!((record.epoch, record.state), (1, LeaderState::Active));
    assert_eq!(record.holder, "holder-a");
    Ok(())
}

async fn reply_time_ms(pg: &Postgres) -> TestResult<i64> {
    let row = pg
        .client()
        .await?
        .query_one(
            "SELECT (extract(epoch FROM r.reply_time) * 1000)::bigint FROM pg_stat_replication r \
             JOIN pg_replication_slots s ON s.active_pid = r.pid WHERE s.slot_name = $1",
            &[&SLOT],
        )
        .await?;
    Ok(row.try_get(0)?)
}

#[tokio::test]
async fn l1_a_closed_gate_reads_within_the_inflight_bound_and_confirms_no_commit() -> TestResult {
    const ROWS: i64 = 30_000;
    let pg = Postgres::start().await?;
    orders(&pg).await?;
    let shared = memory();
    let store = RecordingStore::over(Arc::clone(&shared));
    store.stall_leader_swaps();
    let mut config = support::config(FAST, BUFFER);
    config.max_inflight_bytes = 2048;
    let mut a = support::spawn_in_context(
        support::params(&pg),
        config,
        store.stores(),
        LIMITS,
        support::context_for("holder-a"),
    );
    active(&a.health(), WAIT).await?;
    wait_for_swaps(&store, 1).await?;
    let start = pg
        .slot_state(SLOT)
        .await?
        .and_then(|state| state.confirmed_flush_lsn)
        .ok_or("the slot has no confirmed position")?;

    pg.execute(&format!(
        "INSERT INTO orders SELECT g, repeat('x', 1000) FROM generate_series(1, {ROWS}) g"
    ))
    .await?;
    let end = pg.current_wal_lsn().await?;
    let first_reply = reply_time_ms(&pg).await?;
    assert!(a.no_event_within(Duration::from_secs(2)).await?);

    let sent = pg.sent_lsn(SLOT).await?.ok_or("no walsender")?;
    assert!(sent > start, "the session never read: {sent:#x}");
    assert!(
        sent < end,
        "nothing held the walsender back: {sent:#x} {end:#x}"
    );
    let slot = pg.slot_state(SLOT).await?.ok_or("the slot disappeared")?;
    let confirmed = slot
        .confirmed_flush_lsn
        .ok_or("the slot has no confirmed position")?;
    assert!(start <= confirmed && confirmed <= end, "{slot:?}");
    assert!(reply_time_ms(&pg).await? > first_reply);
    assert!(
        store
            .calls()
            .iter()
            .all(|call| matches!(*call, "read_leader" | "swap_leader")),
        "{:?}",
        store.calls()
    );
    assert!(a.health().snapshot().lease_valid);
    a.abort().await?;

    let mut b = support::spawn_in_context(
        support::params(&pg),
        support::config(FAST, BUFFER),
        RecordingStore::over(Arc::clone(&shared)).stores(),
        LIMITS,
        support::context_for("holder-b"),
    );
    let mut replayed = BTreeSet::new();
    while replayed.len() < usize::try_from(ROWS)? {
        replayed.insert(row(&b.next_event().await?));
    }
    assert_eq!(replayed, (1..=ROWS).collect::<BTreeSet<_>>());
    let record = leader(&shared).await?.value;
    assert_eq!((record.epoch, record.holder.as_str()), (1, "holder-b"));
    b.stop().await
}
