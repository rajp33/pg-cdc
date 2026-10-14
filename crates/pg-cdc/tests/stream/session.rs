use std::time::Duration;

use pg_cdc::config::TlsMode;
use pg_cdc::connection::TlsConfig;
use pg_cdc_testkit::{Postgres, PostgresOptions};
use secrecy::SecretString;
use serde_json::{Value, json};

use pg_cdc::order::OrderBy;
use pg_cdc::session::DRAIN_TIMEOUT;

use crate::support::{self, Mode, SLOT, TestResult, WAIT};

const FAST: Duration = Duration::from_millis(200);
const BUFFER: usize = 1 << 16;

#[tokio::test]
async fn streams_insert_update_delete_end_to_end() -> TestResult {
    let pg = Postgres::start().await?;
    support::prepare(
        &pg,
        &[
            "CREATE TABLE orders (id int PRIMARY KEY, status text)",
            "ALTER TABLE orders REPLICA IDENTITY FULL",
        ],
        &["orders"],
    )
    .await?;
    let mut session = support::spawn_session(support::params(&pg), FAST, BUFFER);
    pg.execute("INSERT INTO orders VALUES (1, 'pending')")
        .await?;
    pg.execute("UPDATE orders SET status = 'paid' WHERE id = 1")
        .await?;
    pg.execute("DELETE FROM orders WHERE id = 1").await?;
    let insert = session.next_event().await?;
    let update = session.next_event().await?;
    let delete = session.next_event().await?;
    assert_eq!(insert["op"], "insert");
    assert_eq!(insert["key"], json!({"id": 1}));
    assert_eq!(insert["before"], Value::Null);
    assert_eq!(insert["after"], json!({"id": 1, "status": "pending"}));
    assert_eq!(update["op"], "update");
    assert_eq!(update["before"], json!({"id": 1, "status": "pending"}));
    assert_eq!(update["after"], json!({"id": 1, "status": "paid"}));
    assert_eq!(delete["op"], "delete");
    assert_eq!(delete["key"], json!({"id": 1}));
    assert_eq!(delete["before"], json!({"id": 1, "status": "paid"}));
    assert_eq!(delete["after"], Value::Null);
    let mut ids = Vec::new();
    for event in [&insert, &update, &delete] {
        assert_eq!(event["version"], 1);
        assert_eq!(event["schema"], "public");
        assert_eq!(event["table"], "orders");
        assert_eq!(event["source"]["pipeline"], support::PIPELINE);
        assert!(event.get("unchanged_toast").is_none());
        let id = event["id"].as_str().unwrap().to_owned();
        let commit = format!("{:016x}", support::commit_lsn(event)?);
        assert!(
            id.starts_with(&commit),
            "{id} does not start with the commit LSN {commit}"
        );
        ids.push(id);
    }
    assert!(ids.windows(2).all(|pair| pair[0] < pair[1]), "{ids:?}");
    session.stop().await
}

#[tokio::test]
async fn a2_slot_not_acknowledged_before_delivery() -> TestResult {
    let pg = Postgres::start().await?;
    support::prepare(
        &pg,
        &["CREATE TABLE orders (id int PRIMARY KEY, status text)"],
        &["orders"],
    )
    .await?;
    let mut session = support::spawn_session(support::params(&pg), FAST, 64);
    session.set_mode(Mode::Hold);
    pg.execute("INSERT INTO orders VALUES (1, 'blocked')")
        .await?;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let blocked = pg
        .slot_state(SLOT)
        .await?
        .ok_or("slot missing")?
        .confirmed_flush_lsn
        .ok_or("no position")?;
    session.set_mode(Mode::Deliver);
    let event = session.next_event().await?;
    let commit = support::commit_lsn(&event)?;
    assert!(
        blocked <= commit,
        "slot acknowledged {blocked:X} past the undelivered commit at {commit:X}"
    );
    pg.wait_for_slot(SLOT, WAIT, |state| {
        state.confirmed_flush_lsn.is_some_and(|lsn| lsn > commit)
    })
    .await?;
    session.stop().await
}

#[tokio::test]
async fn a3_idle_publication_advances_slot() -> TestResult {
    let pg = Postgres::start().await?;
    support::prepare(
        &pg,
        &[
            "CREATE TABLE orders (id int PRIMARY KEY)",
            "CREATE TABLE noise (id int)",
        ],
        &["orders"],
    )
    .await?;
    let mut session = support::spawn_session(support::params(&pg), FAST, BUFFER);
    pg.execute("INSERT INTO noise VALUES (1)").await?;
    let target = pg.current_wal_lsn().await?;
    pg.execute("INSERT INTO noise VALUES (2)").await?;
    pg.wait_for_slot(SLOT, WAIT, |state| {
        state.confirmed_flush_lsn.is_some_and(|lsn| lsn >= target)
    })
    .await?;
    assert!(session.no_event_within(Duration::from_millis(500)).await?);
    session.stop().await
}

#[tokio::test]
async fn o1_replayed_transaction_keeps_its_ids() -> TestResult {
    let pg = Postgres::start().await?;
    support::prepare(
        &pg,
        &["CREATE TABLE orders (id int PRIMARY KEY)"],
        &["orders"],
    )
    .await?;
    let mut first = support::spawn_session(support::params(&pg), Duration::from_secs(60), BUFFER);
    pg.execute("INSERT INTO orders VALUES (1), (2)").await?;
    let a = first.next_event().await?;
    let b = first.next_event().await?;
    let commit = support::commit_lsn(&a)?;
    first.abort().await?;
    let state = pg.wait_for_slot(SLOT, WAIT, |state| !state.active).await?;
    assert!(
        state.confirmed_flush_lsn.is_some_and(|lsn| lsn < commit),
        "the first session acknowledged too early"
    );
    let mut second = support::spawn_session(support::params(&pg), FAST, BUFFER);
    let replayed_a = second.next_event().await?;
    let replayed_b = second.next_event().await?;
    assert_eq!((&replayed_a["id"], &replayed_b["id"]), (&a["id"], &b["id"]));
    assert_eq!(replayed_a, a);
    second.stop().await
}

#[tokio::test]
async fn pinned_session_settings_override_database_defaults() -> TestResult {
    let pg = Postgres::start().await?;
    for setting in [
        "ALTER DATABASE postgres SET timezone = 'America/New_York'",
        "ALTER DATABASE postgres SET datestyle = 'SQL, DMY'",
        "ALTER DATABASE postgres SET intervalstyle = 'iso_8601'",
        "ALTER DATABASE postgres SET bytea_output = 'escape'",
        "ALTER DATABASE postgres SET extra_float_digits = 0",
    ] {
        pg.execute(setting).await?;
    }
    support::prepare(
        &pg,
        &["CREATE TABLE probe (id int PRIMARY KEY, at timestamptz, day date, span interval, raw bytea, ratio float8)"],
        &["probe"],
    )
    .await?;
    let mut session = support::spawn_session(support::params(&pg), FAST, BUFFER);
    pg.execute(
        r"INSERT INTO probe VALUES (1, '2026-10-14 12:00:00+00', '2026-10-14', '1 day 02:00:00', '\x0102', 1.1::float8 + 2.2::float8)",
    )
    .await?;
    let event = session.next_event().await?;
    assert_eq!(event["after"]["at"], "2026-10-14T12:00:00+00:00");
    assert_eq!(event["after"]["day"], "2026-10-14");
    assert_eq!(event["after"]["span"], "1 day 02:00:00");
    assert_eq!(event["after"]["raw"], "\\x0102");
    assert_eq!(event["after"]["ratio"].to_string(), "3.3000000000000003");
    assert_eq!(
        event["source"]["commit_ts"]
            .as_str()
            .map(|ts| ts.ends_with("+00:00")),
        Some(true)
    );
    session.stop().await
}

#[tokio::test]
async fn tls_require_streams_over_an_encrypted_replication_connection() -> TestResult {
    let pg = Postgres::start_with(PostgresOptions { tls: true }).await?;
    support::prepare(
        &pg,
        &["CREATE TABLE orders (id int PRIMARY KEY)"],
        &["orders"],
    )
    .await?;
    let mut params = support::params(&pg);
    params.tls = TlsConfig {
        mode: TlsMode::Require,
        ca_file: None,
    };
    let mut session = support::spawn_session(params, FAST, BUFFER);
    pg.execute("INSERT INTO orders VALUES (7)").await?;
    assert_eq!(session.next_event().await?["after"]["id"], 7);
    let encrypted: i64 = pg
        .client()
        .await?
        .query_one(
            "SELECT count(*) FROM pg_stat_ssl s JOIN pg_stat_replication r ON r.pid = s.pid WHERE s.ssl",
            &[],
        )
        .await?
        .try_get(0)?;
    assert_eq!(encrypted, 1);
    session.stop().await
}

#[tokio::test]
async fn special_character_password_streams() -> TestResult {
    let pg = Postgres::start().await?;
    support::prepare(
        &pg,
        &[
            "CREATE TABLE orders (id int PRIMARY KEY)",
            r#"CREATE ROLE "pg cdc" LOGIN REPLICATION PASSWORD 'p@ss w:rd/''"&=%'"#,
        ],
        &["orders"],
    )
    .await?;
    let mut params = support::params(&pg);
    params.user = "pg cdc".to_owned();
    params.password = Some(SecretString::from(r#"p@ss w:rd/'"&=%"#.to_owned()));
    let mut session = support::spawn_session(params, FAST, BUFFER);
    pg.execute("INSERT INTO orders VALUES (3)").await?;
    assert_eq!(session.next_event().await?["after"]["id"], 3);
    session.stop().await
}

#[tokio::test]
async fn shutdown_during_a_wide_event_write_leaves_only_whole_lines() -> TestResult {
    let pg = Postgres::start().await?;
    support::prepare(
        &pg,
        &["CREATE TABLE docs (id int PRIMARY KEY, body text)"],
        &["docs"],
    )
    .await?;
    let session = support::spawn_stdout_session(support::params(&pg), FAST, 1024);
    pg.execute("INSERT INTO docs VALUES (1, repeat('x', 65536))")
        .await?;
    let output = session.stop_once_output_starts().await?;
    assert_eq!(output.last(), Some(&b'\n'), "output ends mid-line");
    let lines: Vec<Value> = output
        .split_inclusive(|&byte| byte == b'\n')
        .map(serde_json::from_slice)
        .collect::<Result<_, _>>()?;
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0]["after"]["id"], 1);
    assert_eq!(
        lines[0]["after"]["body"].as_str().map(str::len),
        Some(65536)
    );
    Ok(())
}

#[tokio::test]
async fn shutdown_with_a_failing_sink_stops_cleanly_and_replays_the_undelivered_event() -> TestResult
{
    let pg = Postgres::start().await?;
    support::prepare(
        &pg,
        &["CREATE TABLE orders (id int PRIMARY KEY)"],
        &["orders"],
    )
    .await?;
    let mut first = support::spawn_session(support::params(&pg), Duration::from_secs(3600), BUFFER);
    pg.execute("INSERT INTO orders VALUES (1)").await?;
    let delivered = first.next_event().await?;
    let first_commit = support::commit_lsn(&delivered)?;
    first.set_mode(Mode::Fail);
    pg.execute("INSERT INTO orders VALUES (2)").await?;
    first.wait_for_failures(1).await?;
    let stopping = tokio::time::Instant::now();
    first.stop().await?;
    assert!(
        stopping.elapsed() < DRAIN_TIMEOUT + Duration::from_secs(5),
        "shutdown took {:?}",
        stopping.elapsed()
    );
    let state = pg.wait_for_slot(SLOT, WAIT, |state| !state.active).await?;
    let confirmed = state.confirmed_flush_lsn.ok_or("no position")?;
    assert!(
        confirmed > first_commit,
        "the final status update did not confirm the delivered transaction"
    );
    let mut second = support::spawn_session(support::params(&pg), FAST, BUFFER);
    let replayed = second.next_event().await?;
    assert_eq!(replayed["after"]["id"], 2);
    assert!(
        confirmed <= support::commit_lsn(&replayed)?,
        "the undelivered transaction was confirmed"
    );
    second.stop().await
}

#[tokio::test]
async fn a2_the_slot_waits_for_an_earlier_transaction_held_by_a_retrying_key() -> TestResult {
    let pg = Postgres::start().await?;
    support::prepare(
        &pg,
        &["CREATE TABLE orders (id int PRIMARY KEY, status text)"],
        &["orders"],
    )
    .await?;
    let mut config = support::config(FAST, BUFFER);
    config.order_by = OrderBy::Key;
    config.lanes = 4;
    let mut session = support::spawn_with(support::params(&pg), config);
    session.set_mode(Mode::FailContaining("stuck".to_owned()));
    let before = pg.current_wal_lsn().await?;
    pg.execute("INSERT INTO orders VALUES (1, 'stuck')").await?;
    pg.execute("INSERT INTO orders VALUES (2, 'fine')").await?;
    let later = session.next_event().await?;
    assert_eq!(later["after"]["id"], 2);
    session.wait_for_failures(2).await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let held = pg
        .slot_state(SLOT)
        .await?
        .ok_or("slot missing")?
        .confirmed_flush_lsn
        .ok_or("no position")?;
    assert!(
        held <= before,
        "slot confirmed {held:X} past the unsettled transaction after {before:X}"
    );
    session.set_mode(Mode::Deliver);
    let earlier = session.next_event().await?;
    assert_eq!(earlier["after"]["id"], 1);
    let last = support::commit_lsn(&later)?;
    pg.wait_for_slot(SLOT, WAIT, |state| {
        state.confirmed_flush_lsn.is_some_and(|lsn| lsn > last)
    })
    .await?;
    session.stop().await
}

#[tokio::test]
async fn o2_each_key_reaches_the_sink_in_id_order_across_lanes() -> TestResult {
    let pg = Postgres::start().await?;
    support::prepare(
        &pg,
        &["CREATE TABLE counters (id int PRIMARY KEY, n int)"],
        &["counters"],
    )
    .await?;
    let mut config = support::config(FAST, BUFFER);
    config.order_by = OrderBy::Key;
    config.lanes = 4;
    let mut session = support::spawn_with(support::params(&pg), config);
    pg.execute("INSERT INTO counters SELECT g, 0 FROM generate_series(1, 8) g")
        .await?;
    for _ in 0..10 {
        pg.execute("UPDATE counters SET n = n + 1").await?;
        pg.execute("UPDATE counters SET n = n + 1 WHERE id % 2 = 0")
            .await?;
    }
    let events = session.next_events(8 + 10 * 12).await?;
    for key in 1..=8 {
        let per_key: Vec<&Value> = events
            .iter()
            .filter(|event| event["key"]["id"] == key)
            .collect();
        let ids: Vec<&str> = per_key
            .iter()
            .map(|event| event["id"].as_str().unwrap_or_default())
            .collect();
        assert!(
            ids.windows(2).all(|pair| pair[0] < pair[1]),
            "key {key}: {ids:?}"
        );
        let counts: Vec<i64> = per_key
            .iter()
            .filter_map(|event| event["after"]["n"].as_i64())
            .collect();
        let expected: Vec<i64> = (0..=if key % 2 == 0 { 20 } else { 10 }).collect();
        assert_eq!(counts, expected, "key {key}");
    }
    assert!(session.no_event_within(Duration::from_millis(500)).await?);
    session.stop().await
}
