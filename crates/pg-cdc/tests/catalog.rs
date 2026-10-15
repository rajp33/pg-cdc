use std::time::Duration;

use pg_cdc::catalog::{
    SlotActivity, TypeResolver, key_columns, slot_activity, slot_confirmed_flush,
    tables_without_full_identity,
};
use pg_cdc_decoder::Lsn;
use pg_cdc_decoder::value::TypeKind;
use pg_cdc_replication::{ReplicationConfig, ReplicationStream};
use pg_cdc_testkit::{Postgres, SESSION_OPTIONS};
use secrecy::SecretString;
use tokio_postgres::Client;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

async fn oid_of(
    client: &Client,
    expression: &str,
) -> Result<u32, Box<dyn std::error::Error + Send + Sync>> {
    Ok(client
        .query_one(&format!("SELECT ({expression})::oid"), &[])
        .await?
        .try_get(0)?)
}

fn array(element: TypeKind, delimiter: char) -> TypeKind {
    TypeKind::Array {
        element: Box::new(element),
        delimiter,
    }
}

#[tokio::test]
async fn resolves_builtin_and_user_defined_types() -> TestResult {
    let pg = Postgres::start().await?;
    pg.execute(
        "CREATE TYPE mood AS ENUM ('happy', 'sad');
         CREATE DOMAIN posint AS int CHECK (VALUE > 0);
         CREATE DOMAIN posints AS int[];
         CREATE TYPE pair AS (a int, b text);",
    )
    .await?;
    let client = pg.client().await?;
    let mut resolver = TypeResolver::new();
    for (expression, expected) in [
        ("'mood'::regtype", TypeKind::Text),
        ("'mood[]'::regtype", array(TypeKind::Text, ',')),
        ("'posint'::regtype", TypeKind::Integer),
        ("'posint[]'::regtype", array(TypeKind::Integer, ',')),
        ("'posints'::regtype", array(TypeKind::Integer, ',')),
        ("'pair'::regtype", TypeKind::Text),
        ("'pair[]'::regtype", array(TypeKind::Text, ',')),
        ("'int4range'::regtype", TypeKind::Text),
        ("'box[]'::regtype", array(TypeKind::Text, ';')),
        ("'uuid[]'::regtype", array(TypeKind::Text, ',')),
        ("'timestamptz'::regtype", TypeKind::Timestamptz),
        ("'jsonb[]'::regtype", array(TypeKind::Json, ',')),
    ] {
        let oid = oid_of(&client, expression).await?;
        assert_eq!(
            resolver.resolve(&client, oid).await?,
            expected,
            "{expression}"
        );
        assert_eq!(
            resolver.resolve(&client, oid).await?,
            expected,
            "{expression} from cache"
        );
    }
    assert_eq!(
        resolver.resolve(&client, 4_000_000_000).await?,
        TypeKind::Text
    );
    Ok(())
}

#[tokio::test]
async fn key_columns_follow_the_primary_key_then_the_replica_identity_index() -> TestResult {
    let pg = Postgres::start().await?;
    pg.execute(
        "CREATE TABLE pk (b int, a int, c text, PRIMARY KEY (b, a));
         CREATE TABLE ri (a int, c text NOT NULL);
         CREATE UNIQUE INDEX ri_c ON ri (c);
         ALTER TABLE ri REPLICA IDENTITY USING INDEX ri_c;
         CREATE TABLE nokey (a int);
         CREATE TABLE inc (a int, b int, PRIMARY KEY (a) INCLUDE (b));
         CREATE TABLE ri_inc (a int, c text NOT NULL, d int);
         CREATE UNIQUE INDEX ri_inc_c ON ri_inc (c) INCLUDE (d);
         ALTER TABLE ri_inc REPLICA IDENTITY USING INDEX ri_inc_c;",
    )
    .await?;
    let client = pg.client().await?;
    assert_eq!(
        key_columns(&client, oid_of(&client, "'pk'::regclass").await?).await?,
        ["b", "a"]
    );
    assert_eq!(
        key_columns(&client, oid_of(&client, "'ri'::regclass").await?).await?,
        ["c"]
    );
    assert_eq!(
        key_columns(&client, oid_of(&client, "'inc'::regclass").await?).await?,
        ["a"]
    );
    assert_eq!(
        key_columns(&client, oid_of(&client, "'ri_inc'::regclass").await?).await?,
        ["c"]
    );
    assert_eq!(
        key_columns(&client, oid_of(&client, "'nokey'::regclass").await?).await?,
        Vec::<String>::new()
    );
    Ok(())
}

#[tokio::test]
async fn key_columns_handle_quoted_identifiers() -> TestResult {
    let pg = Postgres::start().await?;
    pg.execute(
        r#"CREATE SCHEMA "Weird Schema";
           CREATE TABLE "Weird Schema"."Order Items" ("Order ID" int PRIMARY KEY, "naïve" text);"#,
    )
    .await?;
    let client = pg.client().await?;
    let oid = oid_of(&client, r#"'"Weird Schema"."Order Items"'::regclass"#).await?;
    assert_eq!(key_columns(&client, oid).await?, ["Order ID"]);
    Ok(())
}

#[tokio::test]
async fn reads_slot_position_and_identity_warnings() -> TestResult {
    let pg = Postgres::start().await?;
    pg.execute(
        "CREATE TABLE full_t (id int PRIMARY KEY);
         ALTER TABLE full_t REPLICA IDENTITY FULL;
         CREATE TABLE default_t (id int PRIMARY KEY);
         CREATE TABLE nothing_t (id int);
         ALTER TABLE nothing_t REPLICA IDENTITY NOTHING;",
    )
    .await?;
    pg.create_publication("cat_pub", &["full_t", "default_t", "nothing_t"])
        .await?;
    let created = pg.create_slot("cat_slot").await?;
    let client = pg.client().await?;
    assert_eq!(
        slot_confirmed_flush(&client, "cat_slot").await?,
        Some(Lsn(created))
    );
    assert_eq!(slot_confirmed_flush(&client, "missing_slot").await?, None);
    assert_eq!(
        tables_without_full_identity(&client, "cat_pub").await?,
        vec![
            ("public".to_owned(), "default_t".to_owned()),
            ("public".to_owned(), "nothing_t".to_owned())
        ]
    );
    Ok(())
}

#[tokio::test]
async fn slot_activity_reports_the_walsender_holding_the_slot() -> TestResult {
    let pg = Postgres::start().await?;
    pg.execute("CREATE TABLE held (id int PRIMARY KEY)").await?;
    pg.create_publication("it_pub", &["held"]).await?;
    let start = pg.create_slot("it_slot").await?;
    let client = pg.client().await?;
    assert_eq!(
        slot_activity(&client, "it_slot").await?,
        Some(SlotActivity { active_pid: None })
    );

    let stream = ReplicationStream::start(ReplicationConfig {
        host: pg.host().to_owned(),
        port: pg.port(),
        user: pg.user().to_owned(),
        password: Some(SecretString::from(pg.password().to_owned())),
        database: pg.database().to_owned(),
        options: SESSION_OPTIONS.to_owned(),
        application_name: "pg-cdc-catalog-test".to_owned(),
        tls: None,
        slot: "it_slot".to_owned(),
        publication: "it_pub".to_owned(),
        start_lsn: Lsn(start),
        status_interval: Duration::from_millis(200),
        connect_timeout: Duration::from_secs(10),
        buffer: 8,
    })
    .await?;
    assert_eq!(
        slot_activity(&client, "it_slot").await?,
        Some(SlotActivity {
            active_pid: Some(stream.backend_pid())
        })
    );

    stream.stop().await?;
    pg.wait_for_slot("it_slot", Duration::from_secs(10), |state| !state.active)
        .await?;
    assert_eq!(
        slot_activity(&client, "it_slot").await?,
        Some(SlotActivity { active_pid: None })
    );
    Ok(())
}

#[tokio::test]
async fn slot_activity_is_none_for_a_missing_slot() -> TestResult {
    let pg = Postgres::start().await?;
    let client = pg.client().await?;
    assert_eq!(slot_activity(&client, "nope").await?, None);
    Ok(())
}
