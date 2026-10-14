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

use crate::support::{self, TestResult};

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
