use std::collections::BTreeSet;
use std::path::PathBuf;

use pg_cdc_decoder::message::{
    LogicalMessage, OldTuple, Relation, ReplicaIdentity, Tuple, TupleValue, Update, decode,
};
use pg_cdc_testkit::{Postgres, read_fixture, write_fixture};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const VERSIONS: [u8; 5] = [14, 15, 16, 17, 18];

const SETUP: &[&str] = &[
    "CREATE TYPE fx_mood AS ENUM ('happy', 'sad')",
    "CREATE TABLE fx_items (id int PRIMARY KEY, name text, mood fx_mood)",
    "CREATE TABLE fx_docs (id int PRIMARY KEY, body text)",
    "ALTER TABLE fx_docs ALTER COLUMN body SET STORAGE EXTERNAL",
    "CREATE TABLE fx_full (id int PRIMARY KEY, body text)",
    "ALTER TABLE fx_full ALTER COLUMN body SET STORAGE EXTERNAL",
    "ALTER TABLE fx_full REPLICA IDENTITY FULL",
    "CREATE PUBLICATION fx_pub FOR TABLE fx_items, fx_docs, fx_full",
    "SELECT pg_replication_origin_create('fx_origin')",
];

const SCENARIOS: &[(&str, &[&str])] = &[
    ("insert", &["INSERT INTO fx_items VALUES (1, 'a', 'happy')"]),
    (
        "update",
        &[
            "UPDATE fx_items SET name = 'b' WHERE id = 1",
            "UPDATE fx_items SET id = 2 WHERE id = 1",
        ],
    ),
    ("delete", &["DELETE FROM fx_items WHERE id = 2"]),
    (
        "toast",
        &[
            "INSERT INTO fx_docs VALUES (1, repeat('x', 3000))",
            "UPDATE fx_docs SET id = 2 WHERE id = 1",
            "INSERT INTO fx_full VALUES (1, repeat('y', 3000))",
            "UPDATE fx_full SET id = 2 WHERE id = 1",
        ],
    ),
    ("truncate", &["TRUNCATE fx_items, fx_docs RESTART IDENTITY"]),
    (
        "origin",
        &[
            "SELECT pg_replication_origin_session_setup('fx_origin'); INSERT INTO fx_items VALUES (9, 'o', NULL)",
        ],
    ),
];

fn fixture_path(version: u8, scenario: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(format!("pg{version}"))
        .join(format!("{scenario}.bin"))
}

fn load(version: u8, scenario: &str) -> TestResult<Vec<LogicalMessage>> {
    let mut messages = Vec::new();
    for bytes in read_fixture(&fixture_path(version, scenario))? {
        messages.push(decode(&bytes)?);
    }
    Ok(messages)
}

fn tags(messages: &[LogicalMessage]) -> String {
    messages
        .iter()
        .map(|message| char::from(message.tag()))
        .collect()
}

fn text(value: &str) -> TupleValue {
    TupleValue::Text(value.to_owned())
}

fn updates_in(messages: &[LogicalMessage]) -> Vec<&Update> {
    messages
        .iter()
        .filter_map(|message| match message {
            LogicalMessage::Update(update) => Some(update),
            _ => None,
        })
        .collect()
}

fn relations_in(messages: &[LogicalMessage]) -> Vec<&Relation> {
    messages
        .iter()
        .filter_map(|message| match message {
            LogicalMessage::Relation(relation) => Some(relation),
            _ => None,
        })
        .collect()
}

#[tokio::test]
#[ignore = "regenerates golden fixtures; run `mise run fixtures`"]
async fn capture_golden_fixtures() -> TestResult {
    let pg = Postgres::start().await?;
    for statement in SETUP {
        pg.execute(statement).await?;
    }
    pg.create_slot("fx_slot").await?;
    for (scenario, statements) in SCENARIOS {
        for statement in *statements {
            pg.execute(statement).await?;
        }
        let messages = pg.capture_pgoutput("fx_slot", "fx_pub").await?;
        write_fixture(&fixture_path(pg.version(), scenario), &messages)?;
    }
    Ok(())
}

#[test]
fn golden_insert_decodes_on_every_version() -> TestResult {
    for version in VERSIONS {
        let messages = load(version, "insert")?;
        assert_eq!(tags(&messages), "BYRIC", "pg{version}");
        let [
            LogicalMessage::Begin(begin),
            LogicalMessage::Type(mood),
            LogicalMessage::Relation(relation),
            LogicalMessage::Insert(insert),
            LogicalMessage::Commit(commit),
        ] = messages.as_slice()
        else {
            panic!("pg{version}: unexpected message shape");
        };
        assert_eq!(
            (mood.namespace.as_str(), mood.name.as_str()),
            ("public", "fx_mood")
        );
        assert_eq!(
            (relation.namespace.as_str(), relation.name.as_str()),
            ("public", "fx_items")
        );
        assert_eq!(relation.replica_identity, ReplicaIdentity::Default);
        let columns: Vec<(&str, bool, u32)> = relation
            .columns
            .iter()
            .map(|column| (column.name.as_str(), column.is_key, column.type_oid))
            .collect();
        assert_eq!(
            columns,
            vec![
                ("id", true, 23),
                ("name", false, 25),
                ("mood", false, mood.oid)
            ]
        );
        assert_eq!(insert.relation_oid, relation.oid);
        assert_eq!(insert.new, Tuple(vec![text("1"), text("a"), text("happy")]));
        assert_eq!(begin.final_lsn, commit.commit_lsn);
        assert!(commit.end_lsn > commit.commit_lsn);
    }
    Ok(())
}

#[test]
fn golden_update_and_delete_decode_on_every_version() -> TestResult {
    for version in VERSIONS {
        let update = load(version, "update")?;
        assert_eq!(tags(&update), "BYRUCBUC", "pg{version}");
        let updates = updates_in(&update);
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0].old, None);
        assert_eq!(
            updates[0].new,
            Tuple(vec![text("1"), text("b"), text("happy")])
        );
        assert_eq!(
            updates[1].old,
            Some(OldTuple::Key(Tuple(vec![
                text("1"),
                TupleValue::Null,
                TupleValue::Null
            ])))
        );
        assert_eq!(
            updates[1].new,
            Tuple(vec![text("2"), text("b"), text("happy")])
        );

        let delete = load(version, "delete")?;
        assert_eq!(tags(&delete), "BYRDC", "pg{version}");
        let Some(LogicalMessage::Delete(delete)) = delete.get(3) else {
            panic!("pg{version}: no delete");
        };
        assert_eq!(
            delete.old,
            OldTuple::Key(Tuple(vec![text("2"), TupleValue::Null, TupleValue::Null]))
        );
    }
    Ok(())
}

#[test]
fn golden_toast_decodes_on_every_version() -> TestResult {
    let docs_body = "x".repeat(3000);
    let full_body = "y".repeat(3000);
    for version in VERSIONS {
        let messages = load(version, "toast")?;
        assert_eq!(tags(&messages), "BRICBUCBRICBUC", "pg{version}");
        let relations = relations_in(&messages);
        assert_eq!(relations[0].replica_identity, ReplicaIdentity::Default);
        assert_eq!(relations[1].replica_identity, ReplicaIdentity::Full);
        assert!(relations[1].columns.iter().all(|column| column.is_key));
        let updates = updates_in(&messages);
        assert_eq!(
            updates[0].old,
            Some(OldTuple::Key(Tuple(vec![text("1"), TupleValue::Null])))
        );
        assert_eq!(
            updates[0].new,
            Tuple(vec![text("2"), TupleValue::UnchangedToast])
        );
        assert_eq!(
            updates[1].old,
            Some(OldTuple::Full(Tuple(vec![text("1"), text(&full_body)])))
        );
        assert_eq!(
            updates[1].new,
            Tuple(vec![text("2"), TupleValue::UnchangedToast])
        );
        let Some(LogicalMessage::Insert(first)) = messages.get(2) else {
            panic!("pg{version}: no insert");
        };
        assert_eq!(first.new, Tuple(vec![text("1"), text(&docs_body)]));
    }
    Ok(())
}

#[test]
fn golden_truncate_and_origin_decode_on_every_version() -> TestResult {
    for version in VERSIONS {
        let messages = load(version, "truncate")?;
        let truncate_tags = tags(&messages);
        assert!(
            truncate_tags.starts_with('B') && truncate_tags.ends_with("TC"),
            "pg{version}: {truncate_tags}"
        );
        assert_eq!(
            truncate_tags.matches('R').count(),
            2,
            "pg{version}: {truncate_tags}"
        );
        let relation_oids: BTreeSet<u32> = relations_in(&messages)
            .iter()
            .map(|relation| relation.oid)
            .collect();
        let Some(LogicalMessage::Truncate(truncate)) = messages.iter().rev().nth(1) else {
            panic!("pg{version}: no truncate");
        };
        assert!(!truncate.cascade);
        assert!(truncate.restart_identity);
        assert_eq!(
            truncate
                .relation_oids
                .iter()
                .copied()
                .collect::<BTreeSet<u32>>(),
            relation_oids
        );
        assert_eq!(relation_oids.len(), 2);

        let origin = load(version, "origin")?;
        assert_eq!(tags(&origin), "BOYRIC", "pg{version}");
        let Some(LogicalMessage::Origin(origin)) = origin.get(1) else {
            panic!("pg{version}: no origin");
        };
        assert_eq!(origin.name, "fx_origin");
    }
    Ok(())
}
