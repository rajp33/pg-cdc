use pg_cdc::validate::{Invalid, ValidationError, Verdict, validate};
use std::fmt::Debug;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use pg_cdc_testkit::{Postgres, PostgresOptions, WalLevel};
use tokio_postgres::Client;
use tracing::field::{Field, Visit};
use tracing::instrument::WithSubscriber;
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const SLOT: &str = "it_slot";
const PUBLICATION: &str = "it_pub";

const UNRESERVED_MESSAGE: &str = "replication slot wal_status is unreserved: the WAL it needs may be removed at the next checkpoint";

#[derive(Debug, Clone, PartialEq, Eq)]
struct Captured {
    level: Level,
    message: String,
    slot: Option<String>,
}

#[derive(Debug, Default)]
struct Fields {
    message: String,
    slot: Option<String>,
}

impl Visit for Fields {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "slot" {
            self.slot = Some(value.to_owned());
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        }
    }
}

#[derive(Debug, Clone, Default)]
struct Capture(Arc<Mutex<Vec<Captured>>>);

impl<S: Subscriber> Layer<S> for Capture {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        let mut fields = Fields::default();
        event.record(&mut fields);
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Captured {
                level: *event.metadata().level(),
                message: fields.message,
                slot: fields.slot,
            });
    }
}

async fn validate_capturing(
    client: &Client,
    slot: &str,
) -> (Result<Verdict, ValidationError>, Vec<Captured>) {
    let capture = Capture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    let result = validate(client, slot, PUBLICATION)
        .with_subscriber(subscriber)
        .await;
    let events = capture
        .0
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    (result, events)
}

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
    let (result, events) = validate_capturing(&client, SLOT).await;
    assert_eq!(result?, Verdict::Valid);
    assert_eq!(events, []);
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
    let (result, events) = validate_capturing(&client, "lost_slot").await;
    assert_eq!(result?, Verdict::Unreserved);
    assert_eq!(
        events,
        [Captured {
            level: Level::ERROR,
            message: UNRESERVED_MESSAGE.to_owned(),
            slot: Some("lost_slot".to_owned()),
        }]
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
