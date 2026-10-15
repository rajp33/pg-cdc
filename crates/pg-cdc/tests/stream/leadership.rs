use std::sync::Arc;
use std::time::Duration;

use pg_cdc::health::{Health, Role};
use pg_cdc::session::SessionError;
use pg_cdc::state::StateStore;
use pg_cdc::state::memory::MemoryStateStore;
use pg_cdc::validate::Invalid;
use pg_cdc_testkit::Postgres;

use crate::support::{self, LIMITS, RecordingStore, SLOT, TestResult, WAIT};

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
    let shared: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
    let store_a = RecordingStore::over(Arc::clone(&shared));
    let store_b = RecordingStore::over(shared);
    let mut a = support::spawn_with_stores(
        support::params(&pg),
        support::config(FAST, BUFFER),
        store_a.stores(),
    );
    for id in 1..=3 {
        pg.execute(&format!("INSERT INTO orders VALUES ({id}, 'a')"))
            .await?;
    }
    let first = a.next_events(3).await?;
    assert_eq!(first.len(), 3);
    assert!(store_a.calls().contains(&"load_parked_heads"));

    let mut b = support::spawn_with_stores(
        support::params(&pg),
        support::config(FAST, BUFFER),
        store_b.stores(),
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
