use std::io::Write as _;
use std::ops::RangeInclusive;
use std::pin::Pin;
use std::task::Poll;
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, Statement};
use tokio_util::sync::CancellationToken;

pub const TABLE: &str = "loadgen_items";

const PIPELINE_WINDOW: usize = 128;
const MAX_CONSECUTIVE_FAILURES: u32 = 5;

#[derive(Debug, Clone, PartialEq)]
pub struct WorkloadConfig {
    pub rate: u32,
    pub tx_size: RangeInclusive<u32>,
    pub row_bytes: usize,
    pub toast_every: u32,
    pub oversize_every: u32,
    pub keys: u32,
    pub skew: f64,
    pub seed: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub struct Change {
    pub seq: i64,
    pub key: i32,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Ledger {
    pub committed: Vec<Change>,
    pub uncertain: Vec<Change>,
    pub transactions: u64,
    pub oversize: u64,
    pub last_lsn: Option<String>,
}

#[derive(Debug, Clone)]
pub struct KeySampler {
    cumulative: Vec<f64>,
}

impl KeySampler {
    pub fn new(keys: u32, skew: f64) -> Self {
        let mut total = 0.0;
        let cumulative = (1..=keys.max(1))
            .map(|rank| {
                total += f64::from(rank).powf(-skew);
                total
            })
            .collect();
        Self { cumulative }
    }

    pub fn sample(&self, rng: &mut fastrand::Rng) -> i32 {
        let total = self.cumulative.last().copied().unwrap_or(1.0);
        let draw = rng.f64() * total;
        let index = self.cumulative.partition_point(|weight| *weight <= draw);
        i32::try_from(index.min(self.cumulative.len().saturating_sub(1)) + 1).unwrap_or(i32::MAX)
    }
}

#[derive(Debug)]
pub struct Pacer {
    rate: u32,
    start: Instant,
    sent: u64,
}

impl Pacer {
    pub fn new(rate: u32, start: Instant) -> Self {
        Self {
            rate,
            start,
            sent: 0,
        }
    }

    pub fn delay(&mut self, events: u32, now: Instant) -> Duration {
        if self.rate == 0 {
            return Duration::ZERO;
        }
        self.sent += u64::from(events);
        let due_nanos = u128::from(self.sent) * 1_000_000_000 / u128::from(self.rate);
        let due = Duration::from_nanos(u64::try_from(due_nanos).unwrap_or(u64::MAX));
        self.start
            .checked_add(due)
            .map_or(Duration::ZERO, |due_at| {
                due_at.saturating_duration_since(now)
            })
    }
}

const TOAST_BYTES: usize = 16 * 1024;
const OVERSIZE_BYTES: usize = 300 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Row {
    change: Change,
    payload: String,
    replace_payload: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Plan {
    rows: Vec<Row>,
    oversize: bool,
}

#[derive(Debug)]
struct Planner {
    config: WorkloadConfig,
    sampler: KeySampler,
    rng: fastrand::Rng,
    next_seq: i64,
    statements: u64,
    transactions: u64,
}

impl Planner {
    fn new(config: &WorkloadConfig) -> Self {
        Self {
            config: config.clone(),
            sampler: KeySampler::new(config.keys, config.skew),
            rng: fastrand::Rng::with_seed(config.seed),
            next_seq: 1,
            statements: 0,
            transactions: 0,
        }
    }

    fn next_transaction(&mut self) -> Plan {
        self.transactions += 1;
        let low = *self.config.tx_size.start();
        let high = *self.config.tx_size.end();
        let size = self.rng.u32(low.min(high)..=low.max(high)).max(1);
        let mut rows: Vec<Row> = (0..size).map(|_| self.row(false)).collect();
        let oversize = every(self.config.oversize_every, self.transactions);
        if oversize {
            rows.push(self.row(true));
        }
        Plan { rows, oversize }
    }

    fn row(&mut self, oversize: bool) -> Row {
        self.statements += 1;
        let key = self.sampler.sample(&mut self.rng);
        let seq = self.next_seq;
        self.next_seq += 1;
        let bytes = if oversize {
            OVERSIZE_BYTES
        } else if every(self.config.toast_every, self.statements) {
            TOAST_BYTES
        } else {
            self.config.row_bytes
        };
        Row {
            change: Change { seq, key },
            payload: (0..bytes).map(|_| self.rng.alphanumeric()).collect(),
            replace_payload: oversize || self.rng.bool(),
        }
    }
}

fn every(period: u32, count: u64) -> bool {
    period > 0 && count.is_multiple_of(u64::from(period))
}

fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn setup_statements(publication: &str) -> Vec<String> {
    vec![
        format!(
            "CREATE TABLE {TABLE} (id int PRIMARY KEY, seq bigint NOT NULL, payload text NOT NULL)"
        ),
        format!("ALTER TABLE {TABLE} REPLICA IDENTITY FULL"),
        format!(
            "CREATE PUBLICATION {} FOR TABLE {TABLE}",
            quote_identifier(publication)
        ),
    ]
}

pub async fn setup(client: &Client, publication: &str, slots: &[&str]) -> anyhow::Result<()> {
    for statement in setup_statements(publication) {
        client.batch_execute(&statement).await?;
    }
    for slot in slots {
        client
            .query(
                "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
                &[slot],
            )
            .await?;
    }
    Ok(())
}

fn log(message: &str) {
    let _ = writeln!(std::io::stderr(), "loadgen: {message}");
}

async fn pipeline<F>(futures: Vec<F>) -> Result<(), tokio_postgres::Error>
where
    F: Future<Output = Result<u64, tokio_postgres::Error>>,
{
    let mut pending: Vec<Pin<Box<F>>> = futures.into_iter().map(Box::pin).collect();
    std::future::poll_fn(|context| {
        let mut index = 0;
        while let Some(future) = pending.get_mut(index) {
            match future.as_mut().poll(context) {
                Poll::Ready(Ok(_)) => {
                    pending.remove(index);
                }
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => index += 1,
            }
        }
        if pending.is_empty() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    })
    .await
}

async fn run_transaction(
    client: &Client,
    upsert: &Statement,
    rows: &[Row],
) -> Result<(), tokio_postgres::Error> {
    client.batch_execute("BEGIN").await?;
    for window in rows.chunks(PIPELINE_WINDOW) {
        let parameters: Vec<[&(dyn ToSql + Sync); 4]> = window
            .iter()
            .map(|row| {
                [
                    &row.change.key as &(dyn ToSql + Sync),
                    &row.change.seq,
                    &row.payload,
                    &row.replace_payload,
                ]
            })
            .collect();
        let statements = parameters
            .iter()
            .map(|parameters| client.execute(upsert, parameters))
            .collect();
        if let Err(error) = pipeline(statements).await {
            let _ = client.batch_execute("ROLLBACK").await;
            return Err(error);
        }
    }
    client.batch_execute("COMMIT").await
}

fn limit_reached(max_events: Option<u64>, written: usize) -> bool {
    max_events.is_some_and(|max| u64::try_from(written).map_or(true, |written| written >= max))
}

pub async fn write(
    client: &Client,
    config: &WorkloadConfig,
    stop: CancellationToken,
) -> anyhow::Result<Ledger> {
    write_events(client, config, stop, None).await
}

pub async fn write_events(
    client: &Client,
    config: &WorkloadConfig,
    stop: CancellationToken,
    max_events: Option<u64>,
) -> anyhow::Result<Ledger> {
    let upsert = client
        .prepare(&format!(
            "INSERT INTO {TABLE} (id, seq, payload) VALUES ($1, $2, $3) \
             ON CONFLICT (id) DO UPDATE SET seq = EXCLUDED.seq, \
             payload = CASE WHEN $4 THEN EXCLUDED.payload ELSE {TABLE}.payload END"
        ))
        .await?;
    let mut planner = Planner::new(config);
    let mut pacer = Pacer::new(config.rate, Instant::now());
    let mut ledger = Ledger::default();
    let mut failures = 0;
    while !stop.is_cancelled() && !limit_reached(max_events, ledger.committed.len()) {
        let plan = planner.next_transaction();
        let changes: Vec<Change> = plan.rows.iter().map(|row| row.change).collect();
        match run_transaction(client, &upsert, &plan.rows).await {
            Ok(()) => {
                failures = 0;
                ledger.transactions += 1;
                ledger.oversize += u64::from(plan.oversize);
                ledger.committed.extend(&changes);
            }
            Err(error) => {
                failures += 1;
                ledger.uncertain.extend(&changes);
                log(&format!("transaction failed: {error}"));
                anyhow::ensure!(
                    !client.is_closed() && failures < MAX_CONSECUTIVE_FAILURES,
                    "the workload gave up after {failures} consecutive failures: {error}"
                );
            }
        }
        let events = u32::try_from(changes.len()).unwrap_or(u32::MAX);
        let delay = pacer.delay(events, Instant::now());
        if !delay.is_zero() {
            tokio::select! {
                () = stop.cancelled() => {}
                () = tokio::time::sleep(delay) => {}
            }
        }
    }
    let lsn: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await?
        .get(0);
    ledger.last_lsn = Some(lsn);
    Ok(ledger)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_uniform_without_skew_and_concentrated_with_it() {
        let mut rng = fastrand::Rng::with_seed(7);
        let count = |skew: f64, rng: &mut fastrand::Rng| {
            let sampler = KeySampler::new(50, skew);
            let mut counts = [0u32; 51];
            for _ in 0..100_000 {
                let key = usize::try_from(sampler.sample(rng)).unwrap();
                counts[key] += 1;
            }
            counts
        };
        let uniform = count(0.0, &mut rng);
        assert_eq!(uniform[0], 0);
        for (key, drawn) in uniform.iter().enumerate().skip(1) {
            assert!((1_500..=2_500).contains(drawn), "key {key} drew {drawn}");
        }
        let skewed = count(1.2, &mut rng);
        assert!(
            skewed[1] > skewed[50] * 10,
            "key 1 drew {}, key 50 drew {}",
            skewed[1],
            skewed[50]
        );
    }

    #[test]
    fn the_pacer_spreads_events_at_the_rate() {
        let start = Instant::now();
        let mut pacer = Pacer::new(1_000, start);
        assert_eq!(pacer.delay(100, start), Duration::from_millis(100));
        assert_eq!(
            pacer.delay(100, start + Duration::from_millis(50)),
            Duration::from_millis(150)
        );
        let mut unpaced = Pacer::new(0, start);
        assert_eq!(unpaced.delay(100, start), Duration::ZERO);
        assert_eq!(
            unpaced.delay(100, start + Duration::from_secs(1)),
            Duration::ZERO
        );
    }

    fn config() -> WorkloadConfig {
        WorkloadConfig {
            rate: 0,
            tx_size: 2..=4,
            row_bytes: 64,
            toast_every: 5,
            oversize_every: 3,
            keys: 20,
            skew: 1.0,
            seed: 11,
        }
    }

    fn plans(config: &WorkloadConfig, count: usize) -> Vec<Plan> {
        let mut planner = Planner::new(config);
        (0..count).map(|_| planner.next_transaction()).collect()
    }

    #[test]
    fn planned_transactions_number_changes_in_order_and_size_rows_by_cadence() {
        let plans = plans(&config(), 9);

        let rows: Vec<&Row> = plans.iter().flat_map(|plan| &plan.rows).collect();
        let seqs: Vec<i64> = rows.iter().map(|row| row.change.seq).collect();
        assert_eq!(
            seqs,
            (1..=i64::try_from(rows.len()).unwrap()).collect::<Vec<_>>()
        );
        assert!(rows.iter().all(|row| (1..=20).contains(&row.change.key)));

        for (index, plan) in plans.iter().enumerate() {
            let oversize_turn = (index + 1) % 3 == 0;
            assert_eq!(plan.oversize, oversize_turn, "transaction {}", index + 1);
            let regular = plan.rows.len() - usize::from(oversize_turn);
            assert!((2..=4).contains(&regular), "transaction {}", index + 1);
            if oversize_turn {
                assert_eq!(plan.rows.last().unwrap().payload.len(), 300 * 1024);
            }
        }

        let mut statement = 0;
        for plan in &plans {
            for row in &plan.rows {
                statement += 1;
                let expected = if row.payload.len() == 300 * 1024 {
                    continue;
                } else if statement % 5 == 0 {
                    16 * 1024
                } else {
                    64
                };
                assert_eq!(row.payload.len(), expected, "statement {statement}");
            }
        }
        assert!(
            rows.iter()
                .all(|row| row.payload.chars().all(|c| c.is_ascii_alphanumeric()))
        );
    }

    #[test]
    fn cadences_of_zero_never_fire_and_the_same_seed_plans_the_same_work() {
        let quiet = WorkloadConfig {
            toast_every: 0,
            oversize_every: 0,
            ..config()
        };
        let first = plans(&quiet, 30);
        assert!(first.iter().all(|plan| !plan.oversize));
        assert!(
            first
                .iter()
                .flat_map(|plan| &plan.rows)
                .all(|row| row.payload.len() == 64)
        );
        assert_eq!(first, plans(&quiet, 30));
        assert_ne!(first, plans(&WorkloadConfig { seed: 12, ..quiet }, 30));
    }

    #[test]
    fn an_event_limit_stops_after_the_transaction_that_reaches_it() {
        assert!(!limit_reached(None, 1_000_000));
        assert!(!limit_reached(Some(100), 99));
        assert!(limit_reached(Some(100), 100));
        assert!(limit_reached(Some(100), 150));
        assert!(limit_reached(Some(0), 0));
    }

    #[test]
    fn about_half_of_the_statements_replace_the_payload() {
        let many = WorkloadConfig {
            tx_size: 10..=10,
            toast_every: 0,
            oversize_every: 0,
            ..config()
        };
        let rows: Vec<Row> = plans(&many, 200)
            .into_iter()
            .flat_map(|plan| plan.rows)
            .collect();
        assert_eq!(rows.len(), 2_000);
        let replaced = rows.iter().filter(|row| row.replace_payload).count();
        assert!((900..=1_100).contains(&replaced), "{replaced} replaced");
    }

    #[test]
    fn setup_creates_the_table_then_full_identity_then_the_publication() {
        assert_eq!(
            setup_statements("loadgen_pub"),
            [
                "CREATE TABLE loadgen_items (id int PRIMARY KEY, seq bigint NOT NULL, payload text NOT NULL)",
                "ALTER TABLE loadgen_items REPLICA IDENTITY FULL",
                "CREATE PUBLICATION \"loadgen_pub\" FOR TABLE loadgen_items",
            ]
        );
        assert_eq!(
            setup_statements("odd\"name")[2],
            "CREATE PUBLICATION \"odd\"\"name\" FOR TABLE loadgen_items"
        );
    }
}
