use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use pg_cdc::leader::record::TakeoverKind;
use pg_cdc::metrics::{EventOutcome, Measurement, Recorder, StoreOperation};
use pg_cdc_testkit::Postgres;
use tracing::field::{Field, Visit};
use tracing::level_filters::LevelFilter;
use tracing::span::{Attributes, Id, Record};
use tracing::{Level, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

use crate::support::{self, TestResult, WAIT};

const FAST: Duration = Duration::from_millis(200);
const BUFFER: usize = 1 << 16;
const BETWEEN_TRANSACTIONS: Duration = Duration::from_secs(3);
const BACKLOG: Duration = Duration::from_secs(3);
const JSON_OID: u32 = 114;

#[derive(Debug, Clone, PartialEq)]
enum Seen {
    Event {
        table: String,
        op: String,
        outcome: EventOutcome,
        count: u64,
    },
    CommitToDelivery(Duration),
    Store(StoreOperation),
    Takeover(TakeoverKind),
    DecodeFallback(u32),
    Sink,
}

#[derive(Debug)]
struct Collecting(Arc<Mutex<Vec<Seen>>>);

impl Recorder for Collecting {
    fn record(&self, measurement: Measurement<'_>) {
        let seen = match measurement {
            Measurement::Event {
                table,
                op,
                outcome,
                count,
            } => Seen::Event {
                table: table.to_owned(),
                op: op.to_owned(),
                outcome,
                count,
            },
            Measurement::CommitToDelivery(elapsed) => Seen::CommitToDelivery(elapsed),
            Measurement::Store { operation, .. } => Seen::Store(operation),
            Measurement::Takeover(kind) => Seen::Takeover(kind),
            Measurement::DecodeFallback { type_oid } => Seen::DecodeFallback(type_oid),
            Measurement::Sink { .. } => Seen::Sink,
        };
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(seen);
    }
}

#[derive(Debug, Clone)]
struct SpanSeen {
    name: String,
    level: Level,
    fields: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default)]
struct Spans(Arc<Mutex<Vec<SpanSeen>>>);

struct Fields<'a>(&'a mut BTreeMap<String, String>);

impl Visit for Fields<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }
}

impl<S> Layer<S> for Spans
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(&self, attributes: &Attributes<'_>, id: &Id, context: Context<'_, S>) {
        let mut fields = BTreeMap::new();
        attributes.record(&mut Fields(&mut fields));
        let mut spans = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(span) = context.span(id) {
            span.extensions_mut().insert(spans.len());
        }
        spans.push(SpanSeen {
            name: attributes.metadata().name().to_owned(),
            level: *attributes.metadata().level(),
            fields,
        });
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, context: Context<'_, S>) {
        let Some(index) = context
            .span(id)
            .and_then(|span| span.extensions().get::<usize>().copied())
        else {
            return;
        };
        if let Some(seen) = self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_mut(index)
        {
            values.record(&mut Fields(&mut seen.fields));
        }
    }
}

fn event(table: &str, op: &str, outcome: EventOutcome) -> Seen {
    Seen::Event {
        table: table.to_owned(),
        op: op.to_owned(),
        outcome,
        count: 1,
    }
}

fn count(seen: &[Seen], wanted: &Seen) -> usize {
    seen.iter().filter(|entry| *entry == wanted).count()
}

async fn delivering(session: &support::Running) -> TestResult {
    tokio::time::timeout(
        WAIT,
        session
            .health()
            .subscribe()
            .wait_for(|snapshot| snapshot.delivering),
    )
    .await??;
    Ok(())
}

#[tokio::test]
async fn a_session_records_what_it_receives_its_commit_latency_fallbacks_and_its_takeover()
-> TestResult {
    let recorded = Arc::new(Mutex::new(Vec::new()));
    pg_cdc::metrics::install(Box::new(Collecting(Arc::clone(&recorded))))?;
    let spans = Spans::default();
    let _guard = tracing::subscriber::set_default(
        tracing_subscriber::registry().with(spans.clone().with_filter(LevelFilter::DEBUG)),
    );
    let pg = Postgres::start().await?;
    support::prepare(
        &pg,
        &["CREATE TABLE orders (id int PRIMARY KEY, doc json)"],
        &["orders"],
    )
    .await?;
    pg.execute(r#"INSERT INTO orders VALUES (0, '{"a": 0}')"#)
        .await?;
    tokio::time::sleep(BACKLOG).await;
    let mut session = support::spawn_session(support::params(&pg), FAST, BUFFER);
    delivering(&session).await?;
    session.skip_events(1).await?;
    pg.execute(r#"INSERT INTO orders VALUES (1, '{"a": 1}'), (2, '{"a": 2}')"#)
        .await?;
    session.skip_events(2).await?;
    tokio::time::sleep(BETWEEN_TRANSACTIONS).await;
    pg.execute(r#"INSERT INTO orders VALUES (3, '{"a": 1e2000000}')"#)
        .await?;
    session.skip_events(1).await?;
    tokio::time::sleep(BETWEEN_TRANSACTIONS).await;
    pg.execute(r#"UPDATE orders SET doc = '{"a": 4}' WHERE id = 1"#)
        .await?;
    session.skip_events(1).await?;
    session.stop().await?;

    let seen = recorded.lock().unwrap().clone();
    for outcome in [EventOutcome::Received, EventOutcome::Delivered] {
        assert_eq!(
            count(&seen, &event("orders", "insert", outcome)),
            4,
            "{seen:?}"
        );
        assert_eq!(
            count(&seen, &event("orders", "update", outcome)),
            1,
            "{seen:?}"
        );
    }
    let latencies: Vec<Duration> = seen
        .iter()
        .filter_map(|entry| match entry {
            Seen::CommitToDelivery(elapsed) => Some(*elapsed),
            _ => None,
        })
        .collect();
    let Some((backlogged, fresh)) = latencies.split_first() else {
        panic!("no commit latency recorded: {seen:?}");
    };
    assert!(*backlogged >= BACKLOG, "{latencies:?}");
    assert_eq!(fresh.len(), 4, "{seen:?}");
    assert!(
        fresh.iter().all(|elapsed| *elapsed < BACKLOG),
        "{latencies:?}"
    );
    let fallbacks: Vec<&Seen> = seen
        .iter()
        .filter(|entry| matches!(entry, Seen::DecodeFallback(_)))
        .collect();
    assert_eq!(fallbacks, vec![&Seen::DecodeFallback(JSON_OID)]);
    assert_eq!(count(&seen, &Seen::Takeover(TakeoverKind::First)), 1);
    assert!(seen.contains(&Seen::Store(StoreOperation::SwapLeader)));

    let transactions: Vec<SpanSeen> = spans
        .0
        .lock()
        .unwrap()
        .iter()
        .filter(|span| span.name == "transaction")
        .cloned()
        .collect();
    assert_eq!(transactions.len(), 4, "{transactions:?}");
    let counted: Vec<&str> = transactions
        .iter()
        .map(|span| span.fields.get("events").map_or("", String::as_str))
        .collect();
    assert_eq!(counted, vec!["1", "2", "1", "1"]);
    for span in &transactions {
        assert_eq!(span.level, Level::DEBUG);
        assert!(span.fields.contains_key("commit_lsn"), "{span:?}");
        assert!(span.fields.contains_key("xid"), "{span:?}");
    }
    Ok(())
}

#[tokio::test]
async fn the_transaction_span_is_filtered_out_at_info() -> TestResult {
    let spans = Spans::default();
    let _guard = tracing::subscriber::set_default(
        tracing_subscriber::registry().with(spans.clone().with_filter(LevelFilter::INFO)),
    );
    let pg = Postgres::start().await?;
    support::prepare(
        &pg,
        &["CREATE TABLE orders (id int PRIMARY KEY)"],
        &["orders"],
    )
    .await?;
    let mut session = support::spawn_session(support::params(&pg), FAST, BUFFER);
    pg.execute("INSERT INTO orders VALUES (1)").await?;
    session.skip_events(1).await?;
    session.stop().await?;
    let names: Vec<String> = spans
        .0
        .lock()
        .unwrap()
        .iter()
        .map(|span| span.name.clone())
        .collect();
    assert!(names.contains(&"takeover".to_owned()), "{names:?}");
    for hot in ["transaction", "sink_send", "store", "status_update"] {
        assert!(!names.contains(&hot.to_owned()), "{hot}: {names:?}");
    }
    Ok(())
}
