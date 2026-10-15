use std::sync::Arc;
use std::time::Duration;

use pg_cdc::event::EventId;
use pg_cdc::health::{Health, Role};
use pg_cdc::lane::{OperateError, Stores};
use pg_cdc::order::{OrderBy, OrderKeyId};
use pg_cdc::session::SessionConfig;
use pg_cdc::state::StateStore;
use pg_cdc::state::memory::MemoryStateStore;
use pg_cdc::status::{ParkedSnapshot, Status, StatusSnapshot};
use pg_cdc_decoder::Lsn;
use pg_cdc_testkit::Postgres;
use serde_json::Value;

use crate::support::{self, LIMITS, Mode, Running, TestResult, WAIT};

const BUFFER: usize = 64;
const PUBLISH_WITHIN: Duration = Duration::from_secs(5);
const DRAINED_WITHIN: Duration = Duration::from_secs(3);

fn keyed() -> SessionConfig {
    let mut config = support::config(Duration::from_secs(1), BUFFER);
    config.order_by = OrderBy::Key;
    config.lanes = 2;
    config
}

async fn wait_for_status(
    status: &Status,
    within: Duration,
    ready: impl Fn(&StatusSnapshot) -> bool,
) -> TestResult<StatusSnapshot> {
    tokio::time::timeout(within, async {
        loop {
            let snapshot = status.snapshot();
            if ready(&snapshot) {
                return snapshot;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .map_err(|_| format!("the status never matched; last {:?}", status.snapshot()).into())
}

async fn draining(health: &Health) -> TestResult {
    tokio::time::timeout(
        WAIT,
        health
            .subscribe()
            .wait_for(|snapshot| snapshot.role == Role::Draining),
    )
    .await??;
    Ok(())
}

fn field<'a>(event: &'a Value, pointer: &str) -> &'a Value {
    event.pointer(pointer).unwrap_or(&Value::Null)
}

async fn session_publishes_while_active(
    active: &Status,
    standby: &Status,
    row_two_commit: Lsn,
) -> TestResult {
    let published = wait_for_status(active, PUBLISH_WITHIN, |snapshot| {
        snapshot.epoch == Some(1)
            && snapshot
                .confirmed_lsn
                .is_some_and(|confirmed| confirmed >= row_two_commit)
            && snapshot.inflight_events == 0
            && snapshot
                .parked
                .is_some_and(|parked| (parked.events, parked.keys, parked.dead) == (2, 1, 1))
            && snapshot.slot_retained_bytes.is_some()
    })
    .await?;
    assert_eq!(published.inflight_bytes, 0);
    assert!(
        published
            .status_update_age
            .is_some_and(|age| age < Duration::from_secs(3)),
        "{published:?}"
    );
    assert!(active.parked_access().is_some());

    let waiting = wait_for_status(standby, PUBLISH_WITHIN, |snapshot| {
        snapshot.slot_retained_bytes.is_some()
    })
    .await?;
    assert_eq!(waiting.epoch, None);
    assert_eq!(waiting.parked, None);
    assert!(standby.parked_access().is_none());
    Ok(())
}

async fn operator_redrives_the_poisoned_head(
    a: &mut Running,
    stores: &Stores,
    row_two_id: EventId,
) -> TestResult {
    let access = a
        .status()
        .parked_access()
        .ok_or("the active session published no parked access")?;
    let key = OrderKeyId::of(&support::order_key(1));
    let poisoned = support::parked(stores, &support::order_key(1)).await?;
    let poison_id = poisoned.first().ok_or("row 1 was not parked")?.id;
    let refused = access.operator.discard(key, row_two_id).await;
    assert!(
        matches!(
            &refused,
            Err(OperateError::HeadMismatch { head, .. }) if *head == poison_id
        ),
        "{refused:?}"
    );

    a.set_mode(Mode::Deliver);
    assert_eq!(access.operator.redrive(key).await?, poison_id);
    let redelivered = a.next_events(2).await?;
    let statuses: Vec<_> = redelivered
        .iter()
        .map(|event| field(event, "/after/status").clone())
        .collect();
    assert_eq!(statuses, ["poison", "next"]);
    wait_for_status(&a.status(), DRAINED_WITHIN, |snapshot| {
        snapshot.parked
            == Some(ParkedSnapshot {
                events: 0,
                keys: 0,
                dead: 0,
                oldest_ms: None,
            })
    })
    .await?;
    Ok(())
}

async fn draining_withdraws_access(pg: &Postgres, a: &mut Running) -> TestResult {
    a.set_mode(Mode::Hold);
    pg.execute("INSERT INTO orders VALUES (3, 'held')").await?;
    a.wait_for_next_sink_call().await?;
    a.request_stop();
    draining(&a.health()).await?;
    assert!(a.status().parked_access().is_none());
    a.set_mode(Mode::Deliver);
    Ok(())
}

#[tokio::test]
async fn an_active_session_publishes_its_status_and_operator_and_a_standby_publishes_neither()
-> TestResult {
    let pg = Postgres::start().await?;
    support::prepare(
        &pg,
        &["CREATE TABLE orders (id int PRIMARY KEY, status text)"],
        &["orders"],
    )
    .await?;
    let shared: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
    let stores = Stores {
        state: Arc::clone(&shared),
        payload: None,
    };

    let mut a = support::spawn_in_context(
        support::params(&pg),
        keyed(),
        stores.clone(),
        LIMITS,
        support::context_for("holder-a"),
    );
    a.set_mode(Mode::RejectContaining("poison".to_owned()));
    tokio::time::timeout(
        WAIT,
        a.health()
            .subscribe()
            .wait_for(|snapshot| snapshot.role == Role::Active),
    )
    .await??;
    let mut b = support::spawn_in_context(
        support::params(&pg),
        keyed(),
        stores.clone(),
        LIMITS,
        support::context_for("holder-b"),
    );

    pg.execute("INSERT INTO orders VALUES (1, 'poison')")
        .await?;
    pg.execute("UPDATE orders SET status = 'next' WHERE id = 1")
        .await?;
    pg.execute("INSERT INTO orders VALUES (2, 'ok')").await?;
    let row_two = a.next_event().await?;
    assert_eq!(field(&row_two, "/after/id"), 2);
    let row_two_id: EventId = field(&row_two, "/id")
        .as_str()
        .ok_or("row 2 has no event id")?
        .parse()?;
    let row_two_commit = Lsn(support::commit_lsn(&row_two)?);

    session_publishes_while_active(&a.status(), &b.status(), row_two_commit).await?;
    operator_redrives_the_poisoned_head(&mut a, &stores, row_two_id).await?;
    draining_withdraws_access(&pg, &mut a).await?;
    let ended = a.status();
    a.finished(WAIT).await??;
    let snapshot = ended.snapshot();
    assert_eq!((snapshot.epoch, snapshot.parked), (None, None));

    pg.execute("INSERT INTO orders VALUES (4, 'after')").await?;
    while field(&b.next_event().await?, "/after/id") != 4 {}
    let takeover = wait_for_status(&b.status(), PUBLISH_WITHIN, |snapshot| {
        snapshot.epoch == Some(2)
    })
    .await?;
    assert!(takeover.parked.is_some());
    assert!(b.status().parked_access().is_some());
    b.stop().await
}
