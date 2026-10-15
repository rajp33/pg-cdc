use pg_cdc::validate::{Invalid, ValidationError, Verdict, validate};
use std::time::Duration;

use pg_cdc_testkit::{Postgres, PostgresOptions, WalLevel};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const SLOT: &str = "it_slot";
const PUBLICATION: &str = "it_pub";

async fn slot_names(
    pg: &Postgres,
) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
    let client = pg.client().await?;
    let rows = client
        .query(
            "SELECT slot_name::text FROM pg_replication_slots ORDER BY 1",
            &[],
        )
        .await?;
    let mut names = Vec::new();
    for row in &rows {
        names.push(row.try_get::<_, String>(0)?);
    }
    Ok(names)
}

#[tokio::test]
async fn a_server_without_logical_wal_level_fails_validation() -> TestResult {
    let pg = Postgres::start_with(PostgresOptions {
        wal_level: WalLevel::Replica,
        ..PostgresOptions::default()
    })
    .await?;
    let client = pg.client().await?;
    let result = validate(&client, SLOT, PUBLICATION).await;
    assert!(
        matches!(
            &result,
            Err(ValidationError::Invalid(Invalid::WalLevel(level))) if level == "replica"
        ),
        "{result:?}"
    );
    Ok(())
}

#[tokio::test]
async fn a_missing_slot_or_publication_fails_validation_until_created() -> TestResult {
    let pg = Postgres::start().await?;
    let client = pg.client().await?;
    let result = validate(&client, SLOT, PUBLICATION).await;
    assert!(
        matches!(
            &result,
            Err(ValidationError::Invalid(Invalid::SlotMissing(slot))) if slot == SLOT
        ),
        "{result:?}"
    );
    assert_eq!(slot_names(&pg).await?, Vec::<String>::new());

    pg.create_slot(SLOT).await?;
    let result = validate(&client, SLOT, PUBLICATION).await;
    assert!(
        matches!(
            &result,
            Err(ValidationError::Invalid(Invalid::PublicationMissing(publication)))
                if publication == PUBLICATION
        ),
        "{result:?}"
    );

    pg.execute("CREATE TABLE orders (id int PRIMARY KEY)")
        .await?;
    pg.create_publication(PUBLICATION, &["orders"]).await?;
    let verdict = validate(&client, SLOT, PUBLICATION).await?;
    assert_eq!(verdict, Verdict::Valid);
    assert_eq!(slot_names(&pg).await?, [SLOT]);
    Ok(())
}

#[tokio::test]
async fn a_slot_with_the_wrong_plugin_or_database_fails_validation() -> TestResult {
    let pg = Postgres::start().await?;
    pg.execute("CREATE TABLE orders (id int PRIMARY KEY)")
        .await?;
    pg.create_publication(PUBLICATION, &["orders"]).await?;
    let client = pg.client().await?;

    client
        .execute(
            "SELECT pg_create_logical_replication_slot('decoding_slot', 'test_decoding')",
            &[],
        )
        .await?;
    let result = validate(&client, "decoding_slot", PUBLICATION).await;
    assert!(
        matches!(
            &result,
            Err(ValidationError::Invalid(Invalid::SlotPlugin { plugin: Some(plugin), .. }))
                if plugin == "test_decoding"
        ),
        "{result:?}"
    );

    client.batch_execute("CREATE DATABASE other").await?;
    let mut config = tokio_postgres::Config::new();
    config
        .host(pg.host())
        .port(pg.port())
        .user(pg.user())
        .password(pg.password())
        .dbname("other");
    let (other, connection) = config.connect(tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    other
        .execute(
            "SELECT pg_create_logical_replication_slot('other_slot', 'pgoutput')",
            &[],
        )
        .await?;
    let result = validate(&client, "other_slot", PUBLICATION).await;
    assert!(
        matches!(
            &result,
            Err(ValidationError::Invalid(Invalid::SlotDatabase { found: Some(found), expected, .. }))
                if found == "other" && expected == "postgres"
        ),
        "{result:?}"
    );
    Ok(())
}

#[tokio::test]
async fn a_lost_slot_fails_validation_and_an_unreserved_slot_only_warns() -> TestResult {
    let pg = Postgres::start().await?;
    pg.execute("CREATE TABLE filler (x int)").await?;
    pg.create_publication(PUBLICATION, &["filler"]).await?;
    pg.create_slot("lost_slot").await?;
    let client = pg.client().await?;
    client
        .batch_execute("ALTER SYSTEM SET max_slot_wal_keep_size = '1MB'")
        .await?;
    client.batch_execute("SELECT pg_reload_conf()").await?;

    for _ in 0..2 {
        client
            .batch_execute("INSERT INTO filler VALUES (1)")
            .await?;
        client.batch_execute("SELECT pg_switch_wal()").await?;
    }
    pg.wait_for_slot("lost_slot", Duration::from_secs(10), |state| {
        state.wal_status.as_deref() == Some("unreserved")
    })
    .await?;
    assert_eq!(
        validate(&client, "lost_slot", PUBLICATION).await?,
        Verdict::Unreserved
    );

    let mut lost = false;
    for _ in 0..5 {
        client
            .batch_execute("INSERT INTO filler VALUES (1)")
            .await?;
        client.batch_execute("SELECT pg_switch_wal()").await?;
        client.batch_execute("CHECKPOINT").await?;
        let state = pg.slot_state("lost_slot").await?;
        if state.and_then(|state| state.wal_status).as_deref() == Some("lost") {
            lost = true;
            break;
        }
    }
    assert!(lost, "the slot never reached wal_status lost");
    let result = validate(&client, "lost_slot", PUBLICATION).await;
    assert!(
        matches!(
            &result,
            Err(ValidationError::Invalid(Invalid::SlotLost(slot))) if slot == "lost_slot"
        ),
        "{result:?}"
    );
    Ok(())
}
