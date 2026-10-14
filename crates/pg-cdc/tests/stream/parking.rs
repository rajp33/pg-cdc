use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use pg_cdc::event::EventId;
use pg_cdc::lane::Stores;
use pg_cdc::order::{OrderBy, OrderKeyId};
use pg_cdc::session::SessionConfig;
use pg_cdc::state::memory::MemoryStateStore;
use pg_cdc::state::{ParkStatus, ParkedEvent, StateStore};
use pg_cdc_decoder::Lsn;
use pg_cdc_testkit::Postgres;
use serde_json::Value;

use crate::support::{self, Mode, SLOT, TestResult, WAIT};

const FAST: Duration = Duration::from_millis(200);
const BUFFER: usize = 1 << 16;

async fn orders(pg: &Postgres) -> TestResult {
    support::prepare(
        pg,
        &["CREATE TABLE orders (id int PRIMARY KEY, status text)"],
        &["orders"],
    )
    .await
}

fn keyed(park_after: Duration) -> SessionConfig {
    let mut config = support::config(FAST, BUFFER);
    config.order_by = OrderBy::Key;
    config.lanes = 4;
    config.park_after = park_after;
    config
}

async fn wait_for_queue(
    stores: &Stores,
    id: i64,
    done: impl Fn(&[ParkedEvent]) -> bool,
) -> TestResult<Vec<ParkedEvent>> {
    tokio::time::timeout(WAIT, async {
        loop {
            let parked = support::parked(stores, &support::order_key(id)).await?;
            if done(&parked) {
                return Ok(parked);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await?
}

#[tokio::test]
async fn o3_a_parked_queue_drains_before_new_events_for_its_key() -> TestResult {
    let pg = Postgres::start().await?;
    orders(&pg).await?;
    let store = MemoryStateStore::new();
    store
        .append_parked(&[ParkedEvent {
            order_key: OrderKeyId::of(&support::order_key(1)),
            id: EventId::new(Lsn(1), 0),
            status: ParkStatus::Retrying,
            attempts: 3,
            first_failure_ms: 1,
            last_error: "throttled".to_owned(),
            attributes: Vec::new(),
            body: Bytes::from_static(br#"{"seeded":true}"#),
        }])
        .await?;
    let stores = Stores {
        state: Arc::new(store.clone()),
        payload: None,
    };
    let mut session =
        support::spawn_with_stores(support::params(&pg), keyed(Duration::from_secs(30)), stores);
    pg.execute("INSERT INTO orders VALUES (1, 'new')").await?;
    let first = session.next_event().await?;
    assert_eq!(first, serde_json::json!({"seeded": true}));
    let second = session.next_event().await?;
    assert_eq!(second["after"]["status"], "new");
    assert_eq!(store.parked_events(), 0);
    session.stop().await
}

#[tokio::test]
async fn parking_lets_the_slot_advance_while_the_sink_fails_and_drains_in_order() -> TestResult {
    let pg = Postgres::start().await?;
    orders(&pg).await?;
    let dir = tempfile::tempdir()?;
    let stores = support::filesystem_stores(dir.path());
    let mut session = support::spawn_with_stores(
        support::params(&pg),
        keyed(Duration::from_secs(1)),
        stores.clone(),
    );
    session.set_mode(Mode::Fail);
    pg.execute("INSERT INTO orders VALUES (1, 'new')").await?;
    pg.execute("UPDATE orders SET status = 'paid' WHERE id = 1")
        .await?;
    pg.execute("UPDATE orders SET status = 'shipped' WHERE id = 1")
        .await?;
    let written = pg.current_wal_lsn().await?;
    let parked = wait_for_queue(&stores, 1, |queue| queue.len() == 3).await?;
    assert!(
        parked
            .iter()
            .all(|event| event.status == ParkStatus::Retrying)
    );
    pg.wait_for_slot(SLOT, WAIT, |state| {
        state.confirmed_flush_lsn.is_some_and(|lsn| lsn >= written)
    })
    .await?;
    session.set_mode(Mode::Deliver);
    let events = session.next_events(3).await?;
    let statuses: Vec<&Value> = events
        .iter()
        .map(|event| &event["after"]["status"])
        .collect();
    assert_eq!(statuses, ["new", "paid", "shipped"]);
    wait_for_queue(&stores, 1, <[ParkedEvent]>::is_empty).await?;
    session.stop().await
}

#[tokio::test]
async fn a_rejected_event_is_parked_dead_and_blocks_only_its_key() -> TestResult {
    let pg = Postgres::start().await?;
    orders(&pg).await?;
    let dir = tempfile::tempdir()?;
    let stores = support::filesystem_stores(dir.path());
    let mut session = support::spawn_with_stores(
        support::params(&pg),
        keyed(Duration::from_secs(30)),
        stores.clone(),
    );
    session.set_mode(Mode::RejectContaining("poison".to_owned()));
    pg.execute("INSERT INTO orders VALUES (1, 'poison')")
        .await?;
    pg.execute("UPDATE orders SET status = 'later' WHERE id = 1")
        .await?;
    pg.execute("INSERT INTO orders VALUES (2, 'fine')").await?;
    let written = pg.current_wal_lsn().await?;
    let delivered = session.next_event().await?;
    assert_eq!(delivered["after"]["id"], 2);
    pg.wait_for_slot(SLOT, WAIT, |state| {
        state.confirmed_flush_lsn.is_some_and(|lsn| lsn >= written)
    })
    .await?;
    let parked = wait_for_queue(&stores, 1, |queue| queue.len() == 2).await?;
    let statuses: Vec<ParkStatus> = parked.iter().map(|event| event.status).collect();
    assert_eq!(statuses, [ParkStatus::Dead, ParkStatus::Retrying]);
    assert_eq!(parked[0].last_error, "test sink rejects the event");
    assert!(session.no_event_within(Duration::from_secs(1)).await?);
    session.stop().await
}
