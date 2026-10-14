use std::time::Duration;

use pg_cdc_testkit::Postgres;
use serde_json::{Value, json};

use crate::support::{self, SLOT, TestResult, WAIT};

const FAST: Duration = Duration::from_millis(200);
const BUFFER: usize = 1 << 16;

#[tokio::test]
async fn toast_unchanged_under_default_identity_is_listed() -> TestResult {
    let pg = Postgres::start().await?;
    support::prepare(
        &pg,
        &[
            "CREATE TABLE docs (id int PRIMARY KEY, title text, body text)",
            "ALTER TABLE docs ALTER COLUMN body SET STORAGE EXTERNAL",
        ],
        &["docs"],
    )
    .await?;
    let mut session = support::spawn_session(support::params(&pg), FAST, BUFFER);
    pg.execute("INSERT INTO docs VALUES (1, 'a', repeat('x', 5000))")
        .await?;
    pg.execute("UPDATE docs SET title = 'b' WHERE id = 1")
        .await?;
    pg.execute("UPDATE docs SET id = 2 WHERE id = 1").await?;
    let insert = session.next_event().await?;
    assert_eq!(insert["after"]["body"].as_str().map(str::len), Some(5000));
    let same_key = session.next_event().await?;
    assert_eq!(same_key["before"], Value::Null);
    assert_eq!(same_key["after"], json!({"id": 1, "title": "b"}));
    assert_eq!(same_key["unchanged_toast"], json!(["body"]));
    let new_key = session.next_event().await?;
    assert_eq!(new_key["before"], json!({"id": 1}));
    assert_eq!(new_key["after"], json!({"id": 2, "title": "b"}));
    assert_eq!(new_key["key"], json!({"id": 2}));
    assert_eq!(new_key["unchanged_toast"], json!(["body"]));
    session.stop().await
}

#[tokio::test]
async fn toast_filled_under_full_identity() -> TestResult {
    let pg = Postgres::start().await?;
    support::prepare(
        &pg,
        &[
            "CREATE TABLE docs (id int PRIMARY KEY, title text, body text)",
            "ALTER TABLE docs ALTER COLUMN body SET STORAGE EXTERNAL",
            "ALTER TABLE docs REPLICA IDENTITY FULL",
        ],
        &["docs"],
    )
    .await?;
    let mut session = support::spawn_session(support::params(&pg), FAST, BUFFER);
    pg.execute("INSERT INTO docs VALUES (1, 'a', repeat('y', 5000))")
        .await?;
    pg.execute("UPDATE docs SET title = 'b' WHERE id = 1")
        .await?;
    session.next_event().await?;
    let update = session.next_event().await?;
    let body = "y".repeat(5000);
    assert_eq!(
        update["before"],
        json!({"id": 1, "title": "a", "body": body})
    );
    assert_eq!(
        update["after"],
        json!({"id": 1, "title": "b", "body": body})
    );
    assert!(update.get("unchanged_toast").is_none());
    session.stop().await
}

#[tokio::test]
async fn truncate_emits_one_event_per_relation() -> TestResult {
    let pg = Postgres::start().await?;
    support::prepare(
        &pg,
        &[
            "CREATE TABLE a (id int PRIMARY KEY)",
            "CREATE TABLE b (id int PRIMARY KEY)",
        ],
        &["a", "b"],
    )
    .await?;
    let mut session = support::spawn_session(support::params(&pg), FAST, BUFFER);
    pg.execute("TRUNCATE a, b").await?;
    let first = session.next_event().await?;
    let second = session.next_event().await?;
    let mut tables = Vec::new();
    for (ordinal, event) in [&first, &second].into_iter().enumerate() {
        assert_eq!(event["op"], "truncate");
        assert_eq!(
            (&event["key"], &event["before"], &event["after"]),
            (&Value::Null, &Value::Null, &Value::Null)
        );
        assert!(
            event["id"]
                .as_str()
                .unwrap()
                .ends_with(&format!("-{ordinal:08x}"))
        );
        tables.push(event["table"].as_str().unwrap().to_owned());
    }
    tables.sort();
    assert_eq!(tables, ["a", "b"]);
    assert_eq!(support::commit_lsn(&first)?, support::commit_lsn(&second)?);
    session.stop().await
}

#[tokio::test]
async fn table_without_key_has_null_key() -> TestResult {
    let pg = Postgres::start().await?;
    support::prepare(
        &pg,
        &[
            "CREATE TABLE nokey (a int, b text)",
            "ALTER TABLE nokey REPLICA IDENTITY FULL",
        ],
        &["nokey"],
    )
    .await?;
    let mut session = support::spawn_session(support::params(&pg), FAST, BUFFER);
    pg.execute("INSERT INTO nokey VALUES (1, 'x')").await?;
    pg.execute("DELETE FROM nokey").await?;
    let insert = session.next_event().await?;
    let delete = session.next_event().await?;
    assert_eq!(insert["key"], Value::Null);
    assert_eq!(delete["key"], Value::Null);
    assert_eq!(delete["before"], json!({"a": 1, "b": "x"}));
    session.stop().await
}

#[tokio::test]
async fn schema_change_mid_stream_updates_relation() -> TestResult {
    let pg = Postgres::start().await?;
    support::prepare(
        &pg,
        &["CREATE TABLE orders (id int PRIMARY KEY, status text)"],
        &["orders"],
    )
    .await?;
    let mut session = support::spawn_session(support::params(&pg), FAST, BUFFER);
    pg.execute("INSERT INTO orders VALUES (1, 'a')").await?;
    pg.execute("ALTER TABLE orders ADD COLUMN extra int DEFAULT 7")
        .await?;
    pg.execute("INSERT INTO orders VALUES (2, 'b')").await?;
    pg.execute("ALTER TABLE orders DROP COLUMN status").await?;
    pg.execute("INSERT INTO orders VALUES (3)").await?;
    assert_eq!(
        session.next_event().await?["after"],
        json!({"id": 1, "status": "a"})
    );
    assert_eq!(
        session.next_event().await?["after"],
        json!({"id": 2, "status": "b", "extra": 7})
    );
    assert_eq!(
        session.next_event().await?["after"],
        json!({"id": 3, "extra": 7})
    );
    session.stop().await
}

#[tokio::test]
async fn quoted_identifiers_stream_with_exact_names() -> TestResult {
    let pg = Postgres::start().await?;
    support::prepare(
        &pg,
        &[
            r#"CREATE SCHEMA "Weird Schema""#,
            r#"CREATE TABLE "Weird Schema"."Order Items" ("Order ID" int PRIMARY KEY, "naïve" text)"#,
        ],
        &[r#""Weird Schema"."Order Items""#],
    )
    .await?;
    let mut session = support::spawn_session(support::params(&pg), FAST, BUFFER);
    pg.execute(r#"INSERT INTO "Weird Schema"."Order Items" VALUES (1, 'ok')"#)
        .await?;
    pg.execute(r#"UPDATE "Weird Schema"."Order Items" SET "Order ID" = 2"#)
        .await?;
    let insert = session.next_event().await?;
    assert_eq!(insert["schema"], "Weird Schema");
    assert_eq!(insert["table"], "Order Items");
    assert_eq!(insert["key"], json!({"Order ID": 1}));
    assert_eq!(insert["after"], json!({"Order ID": 1, "naïve": "ok"}));
    let update = session.next_event().await?;
    assert_eq!(update["before"], json!({"Order ID": 1}));
    assert_eq!(update["key"], json!({"Order ID": 2}));
    session.stop().await
}

#[tokio::test]
async fn o2_large_transaction_streams_in_stream_order() -> TestResult {
    const ROWS: i64 = 20_000;
    let pg = Postgres::start().await?;
    support::prepare(
        &pg,
        &["CREATE TABLE items (id int PRIMARY KEY, label text)"],
        &["items"],
    )
    .await?;
    let mut session = support::spawn_session(support::params(&pg), FAST, BUFFER);
    pg.execute(&format!(
        "INSERT INTO items SELECT g, 'row ' || g FROM generate_series(1, {ROWS}) g"
    ))
    .await?;
    pg.execute(&format!("INSERT INTO items VALUES ({}, 'after')", ROWS + 1))
        .await?;
    let mut previous = String::new();
    let mut first_commit = None;
    let mut last_commit = 0;
    for expected in 1..=ROWS + 1 {
        let event = session.next_event().await?;
        assert_eq!(event["after"]["id"], expected);
        let id = event["id"].as_str().unwrap().to_owned();
        assert!(previous < id, "{previous} then {id}");
        let commit = support::commit_lsn(&event)?;
        if expected <= ROWS {
            assert_eq!(*first_commit.get_or_insert(commit), commit);
            assert!(id.ends_with(&format!("-{:08x}", expected - 1)), "{id}");
        } else {
            assert!(Some(commit) > first_commit);
        }
        last_commit = commit;
        previous = id;
    }
    pg.wait_for_slot(SLOT, WAIT, |state| {
        state
            .confirmed_flush_lsn
            .is_some_and(|lsn| lsn > last_commit)
    })
    .await?;
    session.stop().await
}
