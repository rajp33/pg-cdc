use std::collections::BTreeMap;
use std::time::Duration;

use pg_cdc::order::{OrderBy, OrderKeyId, TableName};
use pg_cdc::session::{SessionConfig, SessionError};
use pg_cdc::validate::Invalid;
use pg_cdc_testkit::Postgres;
use serde_json::{Value, json};

use crate::support::{self, TestResult};

const FAST: Duration = Duration::from_millis(200);
const BUFFER: usize = 1 << 16;

fn table(name: &str) -> TableName {
    TableName {
        schema: "public".to_owned(),
        table: name.to_owned(),
    }
}

fn summary(event: &Value) -> (String, String, Value) {
    let row = if event["after"].is_null() {
        &event["before"]
    } else {
        &event["after"]
    };
    (
        event["table"].as_str().unwrap_or_default().to_owned(),
        event["op"].as_str().unwrap_or_default().to_owned(),
        row.clone(),
    )
}

#[tokio::test]
async fn table_overrides_deliver_a_parent_and_its_children_on_one_order_key_in_commit_order()
-> TestResult {
    let pg = Postgres::start().await?;
    support::prepare(
        &pg,
        &[
            "CREATE TABLE orders (id int PRIMARY KEY, status text)",
            "CREATE TABLE order_items (id int PRIMARY KEY, order_id int, sku text)",
            "ALTER TABLE order_items REPLICA IDENTITY FULL",
            "CREATE TABLE notes (id int PRIMARY KEY, order_id int)",
        ],
        &["orders", "order_items", "notes"],
    )
    .await?;
    let config = SessionConfig {
        order_by: OrderBy::Key,
        order_by_tables: BTreeMap::from([
            (table("orders"), OrderBy::Column("id".to_owned())),
            (table("order_items"), OrderBy::Column("order_id".to_owned())),
        ]),
        lanes: 16,
        ..support::config(FAST, BUFFER)
    };
    let mut session = support::spawn_with(support::params(&pg), config);
    pg.execute(
        "BEGIN;
         INSERT INTO orders VALUES (42, 'new');
         INSERT INTO order_items VALUES (1, 42, 'a');
         INSERT INTO notes VALUES (1, 42);
         INSERT INTO order_items VALUES (2, 42, 'b');
         UPDATE orders SET status = 'paid' WHERE id = 42;
         COMMIT;
         BEGIN;
         DELETE FROM order_items WHERE id = 1;
         UPDATE orders SET status = 'shipped' WHERE id = 42;
         COMMIT;",
    )
    .await?;
    let mut grouped = Vec::new();
    let mut others = Vec::new();
    for _ in 0..7 {
        let (key, event) = session.next_keyed_event().await?;
        if key == OrderKeyId::of(b"[42]") {
            grouped.push(event);
        } else {
            others.push((key, event));
        }
    }
    assert_eq!(
        grouped.iter().map(summary).collect::<Vec<_>>(),
        [
            ("orders", "insert", json!({"id": 42, "status": "new"})),
            (
                "order_items",
                "insert",
                json!({"id": 1, "order_id": 42, "sku": "a"})
            ),
            (
                "order_items",
                "insert",
                json!({"id": 2, "order_id": 42, "sku": "b"})
            ),
            ("orders", "update", json!({"id": 42, "status": "paid"})),
            (
                "order_items",
                "delete",
                json!({"id": 1, "order_id": 42, "sku": "a"})
            ),
            ("orders", "update", json!({"id": 42, "status": "shipped"})),
        ]
        .map(|(table, op, row)| (table.to_owned(), op.to_owned(), row))
    );
    let ids: Vec<&str> = grouped
        .iter()
        .filter_map(|event| event["id"].as_str())
        .collect();
    assert!(ids.is_sorted(), "{ids:?}");
    assert_eq!(others.len(), 1);
    assert_eq!(
        others.first().map(|(key, event)| (*key, summary(event).0)),
        Some((
            OrderKeyId::of(br#"["public","notes",1]"#),
            "notes".to_owned()
        ))
    );
    session.stop().await
}

#[tokio::test]
async fn an_override_on_a_column_the_publication_does_not_replicate_fails_startup() -> TestResult {
    let pg = Postgres::start().await?;
    support::prepare(
        &pg,
        &["CREATE TABLE order_items (id int PRIMARY KEY, order_id int, sku text)"],
        &["order_items"],
    )
    .await?;
    let config = SessionConfig {
        order_by_tables: BTreeMap::from([(
            table("order_items"),
            OrderBy::Column("order_ref".to_owned()),
        )]),
        ..support::config(FAST, BUFFER)
    };
    let session = support::spawn_with(support::params(&pg), config);
    let health = session.health();
    let result = session.finished(support::WAIT).await?;
    assert!(
        matches!(
            &result,
            Err(SessionError::Invalid(Invalid::OrderColumnUnpublished { table, column }))
                if table == "public.order_items" && column == "order_ref"
        ),
        "{result:?}"
    );
    assert!(!health.snapshot().validated, "{:?}", health.snapshot());
    Ok(())
}

#[tokio::test]
async fn an_unchanged_toasted_order_column_keeps_its_key_only_under_full_identity() -> TestResult {
    let pg = Postgres::start().await?;
    let mut statements = Vec::new();
    for (table, identity) in [("full_items", "FULL"), ("plain_items", "DEFAULT")] {
        statements.push(format!(
            "CREATE TABLE {table} (id int PRIMARY KEY, order_ref text, note text)"
        ));
        statements.push(format!(
            "ALTER TABLE {table} ALTER COLUMN order_ref SET STORAGE EXTERNAL"
        ));
        statements.push(format!("ALTER TABLE {table} REPLICA IDENTITY {identity}"));
    }
    let statements: Vec<&str> = statements.iter().map(String::as_str).collect();
    support::prepare(&pg, &statements, &["full_items", "plain_items"]).await?;
    let by_ref = OrderBy::Column("order_ref".to_owned());
    let config = SessionConfig {
        order_by_tables: BTreeMap::from([
            (table("full_items"), by_ref.clone()),
            (table("plain_items"), by_ref),
        ]),
        ..support::config(FAST, BUFFER)
    };
    let mut session = support::spawn_with(support::params(&pg), config);
    for table in ["full_items", "plain_items"] {
        pg.execute(&format!(
            "INSERT INTO {table} VALUES (1, repeat('k', 5000), 'a');
             UPDATE {table} SET note = 'b' WHERE id = 1;"
        ))
        .await?;
    }
    let by_value = OrderKeyId::of(json!([&"k".repeat(5000)]).to_string().as_bytes());
    let mut keys = Vec::new();
    for _ in 0..4 {
        let (key, event) = session.next_keyed_event().await?;
        let (table, op, _) = summary(&event);
        let id = event["id"].as_str().unwrap_or_default().to_owned();
        keys.push((id, (table, op, key, event["unchanged_toast"].clone())));
    }
    keys.sort_by(|left, right| left.0.cmp(&right.0));
    let keys: Vec<_> = keys.into_iter().map(|(_, keyed)| keyed).collect();
    assert_eq!(
        keys,
        [
            ("full_items", "insert", by_value, Value::Null),
            ("full_items", "update", by_value, Value::Null),
            ("plain_items", "insert", by_value, Value::Null),
            (
                "plain_items",
                "update",
                OrderKeyId::of(br#"["public","plain_items"]"#),
                json!(["order_ref"])
            ),
        ]
        .map(|(table, op, key, toast)| (table.to_owned(), op.to_owned(), key, toast))
    );
    session.stop().await
}
