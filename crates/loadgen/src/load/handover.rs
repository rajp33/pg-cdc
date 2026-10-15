use std::collections::BTreeSet;
use std::io::Write as _;
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use anyhow::Context as _;
use pg_cdc_testkit::AwsEmulator;
use serde::Serialize;
use tokio_util::sync::CancellationToken;

use super::rig::{self, Bench, ConfigSpec, SinkSpec, StoreSpec};
use super::{LoadOptions, Verdict, write_report};
use crate::chaos::evidence::{Observations, published_messages, read_stored};
use crate::chaos::outstanding;
use crate::checker::{self, Evidence};
use crate::cluster::PgCdc;
use crate::measure::Machine;
use crate::proxy::{FaultProxy, Tap, Upstream};
use crate::workload::{self, Ledger};

const SLOT: &str = "handover_slot";
const BUCKET: &str = "handover-state";
const TOPIC: &str = "handover";
const PARKED_PREFIX: &str = "load/load/parked/";
const WRITE_RATE: u32 = 500;
const ROUNDS: usize = 5;
const WAIT_BEFORE: Duration = Duration::from_secs(20);
const STANDBY_POLL: Duration = Duration::from_secs(2);
const EXIT_LIMIT: Duration = Duration::from_secs(60);
const READY_LIMIT: Duration = Duration::from_secs(60);
const READY_POLL: Duration = Duration::from_millis(50);
const TAIL: Duration = Duration::from_secs(1);
const SETTLE_POLL: Duration = Duration::from_secs(1);
const SETTLE_STALL: Duration = Duration::from_secs(60);
const SETTLE_STALL_FACTOR: u32 = 10;
const STOP_LIMIT: Duration = Duration::from_secs(60);
const HANDOVER_LIMIT_S: f64 = 10.0;

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
struct Round {
    #[serde(rename = "exit_s")]
    exit: f64,
    #[serde(rename = "ready_s")]
    ready: f64,
    #[serde(rename = "gap_s")]
    gap: f64,
}

fn wait_before(rng: &mut fastrand::Rng) -> Duration {
    let jitter = u64::try_from(STANDBY_POLL.as_millis()).unwrap_or(0);
    WAIT_BEFORE + Duration::from_millis(rng.u64(0..jitter.max(1)))
}

fn largest_gap(deliveries: &[Instant], term: Instant, until: Instant, now: Instant) -> Duration {
    if deliveries.is_empty() {
        return now.saturating_duration_since(term);
    }
    let ends = deliveries.iter().skip(1).chain(std::iter::once(&now));
    deliveries
        .iter()
        .zip(ends)
        .filter(|(start, end)| **end > term && **start <= until)
        .map(|(start, end)| end.saturating_duration_since(*start))
        .max()
        .unwrap_or(Duration::ZERO)
}

fn criterion_five(rounds: &[Round], checker_passed: bool) -> Verdict {
    let within = |seconds: f64| seconds < HANDOVER_LIMIT_S;
    let quick = rounds
        .iter()
        .all(|round| within(round.ready) && within(round.gap));
    if checker_passed && !rounds.is_empty() && quick {
        Verdict::Pass
    } else {
        Verdict::Fail
    }
}

#[derive(Debug, Default)]
struct Deliveries {
    observations: Observations,
    times: Mutex<Vec<Instant>>,
}

impl Deliveries {
    fn record(&self, form: &[u8]) {
        let messages = published_messages(form);
        if messages.is_empty() {
            return;
        }
        self.times
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Instant::now());
        for message in &messages {
            self.observations.record(message);
        }
    }

    fn times(&self) -> Vec<Instant> {
        self.times
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

#[derive(Debug)]
struct Spawner<'a> {
    bench: &'a Bench,
    binary: &'a Path,
    spec: ConfigSpec,
    env: Vec<(String, String)>,
    spawned: u32,
}

impl Spawner<'_> {
    async fn spawn(&mut self) -> anyhow::Result<(PgCdc, String)> {
        self.spawned += 1;
        let name = format!("instance-{}", self.spawned);
        self.bench
            .spawn(self.binary, &self.spec, &self.env, &name)
            .await
    }
}

async fn ready_after(process: &PgCdc, term: Instant) -> Option<Duration> {
    loop {
        let status = process.status().await.ok();
        let ready = status
            .as_ref()
            .and_then(|status| status.get("ready"))
            .and_then(serde_json::Value::as_bool);
        if ready == Some(true) {
            return Some(term.elapsed());
        }
        if term.elapsed() >= READY_LIMIT {
            return None;
        }
        tokio::time::sleep(READY_POLL).await;
    }
}

async fn active_index(instances: &[PgCdc]) -> Option<usize> {
    for (index, process) in instances.iter().enumerate() {
        let status = process.status().await.ok()?;
        if status.get("role").and_then(serde_json::Value::as_str) == Some("active") {
            return Some(index);
        }
    }
    None
}

async fn handover(
    instances: &mut Vec<PgCdc>,
    spawner: &mut Spawner<'_>,
    deliveries: &Deliveries,
    rng: &mut fastrand::Rng,
    problems: &mut Vec<String>,
) -> anyhow::Result<Round> {
    tokio::time::sleep(wait_before(rng)).await;
    let active = active_index(instances)
        .await
        .context("no instance reported itself active")?;
    let mut old = instances.remove(active);
    let new = instances
        .first()
        .context("there is no standby to take over")?;
    let term = Instant::now();
    old.sigterm().await?;
    let (exit, ready) = tokio::join!(
        async {
            let status = old.wait(EXIT_LIMIT).await;
            (status, term.elapsed())
        },
        ready_after(new, term),
    );
    let (status, exited) = exit;
    match status? {
        Some(status) if status.success() => {}
        Some(status) => problems.push(format!("the old instance exited with {status}")),
        None => {
            old.sigkill().await?;
            problems.push(format!(
                "the old instance was still running {EXIT_LIMIT:?} after SIGTERM"
            ));
        }
    }
    tokio::time::sleep(TAIL).await;
    let now = Instant::now();
    let ready = ready.unwrap_or(READY_LIMIT);
    let gap = largest_gap(&deliveries.times(), term, term + ready, now);
    instances.push(spawner.spawn().await?.0);
    Ok(Round {
        exit: exited.as_secs_f64(),
        ready: ready.as_secs_f64(),
        gap: gap.as_secs_f64(),
    })
}

async fn settle(ledger: &Ledger, deliveries: &Deliveries) {
    let started = Instant::now();
    let cap = started + SETTLE_STALL * SETTLE_STALL_FACTOR;
    let mut deadline = started + SETTLE_STALL;
    let mut best = usize::MAX;
    loop {
        tokio::time::sleep(SETTLE_POLL).await;
        let pending = outstanding(
            &ledger.committed,
            &deliveries.observations.snapshot(),
            &BTreeSet::new(),
        );
        let now = Instant::now();
        if pending < best {
            best = pending;
            deadline = now + SETTLE_STALL;
        }
        if pending == 0 || now >= deadline.min(cap) {
            return;
        }
    }
}

async fn stop_all(instances: &mut [PgCdc], problems: &mut Vec<String>) -> anyhow::Result<()> {
    for process in instances.iter() {
        process.sigterm().await?;
    }
    for process in instances.iter_mut() {
        match process.wait(STOP_LIMIT).await? {
            Some(status) if status.success() => {}
            Some(status) => problems.push(format!("an instance exited with {status} at the end")),
            None => {
                process.sigkill().await?;
                problems.push(format!(
                    "an instance was still running {STOP_LIMIT:?} after SIGTERM"
                ));
            }
        }
    }
    Ok(())
}

#[derive(Debug, Serialize)]
struct Report {
    scenario: &'static str,
    machine: Machine,
    config: String,
    workload: String,
    write_rate: u32,
    handovers: usize,
    rounds: Vec<Round>,
    max_exit_s: f64,
    max_ready_s: f64,
    max_gap_s: f64,
    publish_requests: usize,
    problems: Vec<String>,
    checker: checker::Verdict,
    checker_passed: bool,
    criterion_5: Verdict,
}

fn longest(rounds: &[Round], field: fn(&Round) -> f64) -> f64 {
    rounds.iter().map(field).fold(0.0, f64::max)
}

#[derive(Debug)]
struct Harness<'a> {
    aws: AwsEmulator,
    deliveries: Arc<Deliveries>,
    sns_proxy: FaultProxy,
    spawner: Spawner<'a>,
    config: String,
    instances: Vec<PgCdc>,
    schedule: fastrand::Rng,
}

async fn prepare<'a>(bench: &'a Bench, options: &'a LoadOptions) -> anyhow::Result<Harness<'a>> {
    let aws = AwsEmulator::start().await?;
    rig::create_bucket(&aws, BUCKET).await?;
    let topic_arn = rig::create_topic(&aws, TOPIC, true).await?;
    let upstream = aws.endpoint().trim_start_matches("http://").parse()?;
    let deliveries = Arc::new(Deliveries::default());
    let tap = Tap::new({
        let deliveries = Arc::clone(&deliveries);
        move |form| deliveries.record(form)
    });
    let sns_proxy = FaultProxy::start_with_tap(upstream, Upstream::Sns, 1, Some(tap)).await?;
    let mut spawner = Spawner {
        bench,
        binary: &options.binary,
        spec: ConfigSpec {
            slot: SLOT.to_owned(),
            sink: SinkSpec::Sns { topic_arn },
            store: StoreSpec::S3 {
                bucket: BUCKET.to_owned(),
            },
            max_inflight_bytes: None,
            park_after: None,
            otlp_endpoint: None,
        },
        env: rig::aws_env(&sns_proxy.endpoint(), aws.endpoint()),
        spawned: 0,
    };
    let (first, config) = spawner.spawn().await?;
    rig::wait_ready(&first).await?;
    let standby = spawner.spawn().await?.0;
    Ok(Harness {
        aws,
        deliveries,
        sns_proxy,
        spawner,
        config,
        instances: vec![first, standby],
        schedule: fastrand::Rng::with_seed(1),
    })
}

#[derive(Debug)]
struct Driven {
    ledger: Ledger,
    rounds: Vec<Round>,
    problems: Vec<String>,
}

async fn drive(bench: &Bench, harness: &mut Harness<'_>) -> anyhow::Result<Driven> {
    let writer_client = bench.postgres.client().await?;
    let stop = CancellationToken::new();
    let writer = tokio::spawn({
        let stop = stop.clone();
        async move { workload::write(&writer_client, &rig::workload(WRITE_RATE), stop).await }
    });
    let mut rounds = Vec::new();
    let mut problems = Vec::new();
    for _ in 0..ROUNDS {
        anyhow::ensure!(
            !writer.is_finished(),
            "the workload ended early: {:?}",
            writer.await
        );
        let round = handover(
            &mut harness.instances,
            &mut harness.spawner,
            &harness.deliveries,
            &mut harness.schedule,
            &mut problems,
        )
        .await?;
        let stuck = round.ready >= READY_LIMIT.as_secs_f64();
        rounds.push(round);
        if stuck {
            break;
        }
    }
    stop.cancel();
    let ledger = writer.await.context("the workload task panicked")??;
    settle(&ledger, &harness.deliveries).await;
    stop_all(&mut harness.instances, &mut problems).await?;
    Ok(Driven {
        ledger,
        rounds,
        problems,
    })
}

fn print_outcome(
    rounds: &[Round],
    checker: &checker::Verdict,
    problems: &[String],
    criterion_5: Verdict,
) -> anyhow::Result<()> {
    for (number, round) in rounds.iter().enumerate() {
        writeln!(
            std::io::stdout(),
            "load handover: round {}: exit {:.2} s, ready {:.2} s, delivery gap {:.2} s",
            number + 1,
            round.exit,
            round.ready,
            round.gap,
        )?;
    }
    writeln!(
        std::io::stdout(),
        "load handover: committed {} delivered {} duplicates {} missing {} phantoms {} out of order {}, problems {problems:?}, criterion 5 {}",
        checker.committed,
        checker.delivered,
        checker.duplicates,
        checker.missing.len(),
        checker.phantoms.len(),
        checker.out_of_order.len(),
        serde_json::to_string(&criterion_5)?,
    )?;
    Ok(())
}

pub(super) async fn run(options: &LoadOptions) -> anyhow::Result<()> {
    let bench = Bench::start(&[SLOT]).await?;
    let mut harness = prepare(&bench, options).await?;
    let Driven {
        ledger,
        rounds,
        mut problems,
    } = drive(&bench, &mut harness).await?;
    let stored = read_stored(&harness.aws.s3().await, BUCKET, PARKED_PREFIX).await?;
    let unrecognized = harness.deliveries.observations.unrecognized_count();
    if unrecognized > 0 {
        problems.push(format!(
            "{unrecognized} published messages were not understood"
        ));
    }
    let checker = checker::check(&Evidence {
        committed: ledger.committed,
        uncertain: ledger.uncertain,
        observed: harness.deliveries.observations.snapshot(),
        stored,
        discards: BTreeSet::new(),
    });
    let checker_passed = checker.passed();
    let criterion_5 = criterion_five(&rounds, checker_passed);
    print_outcome(&rounds, &checker, &problems, criterion_5)?;
    let report = Report {
        scenario: "handover",
        machine: bench.machine.clone(),
        config: harness.config.clone(),
        workload: format!("{:?}", rig::workload(WRITE_RATE)),
        write_rate: WRITE_RATE,
        handovers: rounds.len(),
        max_exit_s: longest(&rounds, |round| round.exit),
        max_ready_s: longest(&rounds, |round| round.ready),
        max_gap_s: longest(&rounds, |round| round.gap),
        rounds,
        publish_requests: harness.deliveries.times().len(),
        problems,
        checker,
        checker_passed,
        criterion_5,
    };
    drop(harness.sns_proxy);
    write_report(options, "handover", &report).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn after(base: Instant, millis: u64) -> Instant {
        base + Duration::from_millis(millis)
    }

    fn deliveries(base: Instant, millis: &[u64]) -> Vec<Instant> {
        millis.iter().map(|millis| after(base, *millis)).collect()
    }

    fn steady(from: u64, to: u64, every: u64) -> Vec<u64> {
        (from..=to)
            .step_by(usize::try_from(every).unwrap())
            .collect()
    }

    #[test]
    fn the_gap_is_the_longest_silence_that_ends_after_sigterm_and_starts_before_readiness() {
        let base = Instant::now();
        let mut millis = steady(0, 20_000, 200);
        millis.extend(steady(26_500, 40_000, 200));
        let times = deliveries(base, &millis);
        let term = after(base, 20_100);
        let until = after(base, 27_000);
        let now = after(base, 40_000);
        assert_eq!(
            largest_gap(&times, term, until, now),
            Duration::from_millis(6_500)
        );
    }

    #[test]
    fn steady_deliveries_leave_a_gap_of_one_interval() {
        let base = Instant::now();
        let times = deliveries(base, &steady(0, 30_000, 200));
        let term = after(base, 10_000);
        let until = after(base, 15_000);
        let now = after(base, 30_000);
        assert_eq!(
            largest_gap(&times, term, until, now),
            Duration::from_millis(200)
        );
    }

    #[test]
    fn silence_since_the_last_delivery_counts_up_to_now() {
        let base = Instant::now();
        let times = deliveries(base, &steady(0, 10_000, 200));
        let term = after(base, 10_100);
        let until = after(base, 12_000);
        let now = after(base, 18_000);
        assert_eq!(
            largest_gap(&times, term, until, now),
            Duration::from_millis(8_000)
        );
        assert_eq!(
            largest_gap(&[], term, until, now),
            Duration::from_millis(7_900)
        );
    }

    #[test]
    fn gaps_before_sigterm_and_after_readiness_are_not_the_handovers() {
        let base = Instant::now();
        let mut millis = steady(0, 4_000, 200);
        millis.extend(steady(12_000, 19_000, 200));
        millis.extend(steady(19_000, 30_000, 200));
        millis.sort_unstable();
        millis.dedup();
        millis.retain(|millis| *millis < 21_000 || *millis > 23_000);
        let times = deliveries(base, &millis);
        let term = after(base, 14_000);
        let until = after(base, 16_000);
        let now = after(base, 30_000);
        assert_eq!(
            largest_gap(&times, term, until, now),
            Duration::from_millis(200)
        );
    }

    #[test]
    fn the_wait_before_a_handover_is_twenty_seconds_plus_up_to_one_standby_poll() {
        let mut first = fastrand::Rng::with_seed(3);
        let mut second = fastrand::Rng::with_seed(3);
        let waits: Vec<Duration> = (0..200).map(|_| wait_before(&mut first)).collect();
        let again: Vec<Duration> = (0..200).map(|_| wait_before(&mut second)).collect();
        assert_eq!(waits, again);
        assert!(
            waits.iter().all(|wait| {
                *wait >= Duration::from_secs(20) && *wait < Duration::from_secs(22)
            })
        );
        assert!(
            waits
                .iter()
                .any(|wait| *wait < Duration::from_millis(20_500))
        );
        assert!(
            waits
                .iter()
                .any(|wait| *wait > Duration::from_millis(21_500))
        );
    }

    fn round(exit: f64, ready: f64, gap: f64) -> Round {
        Round { exit, ready, gap }
    }

    #[test]
    fn criterion_five_needs_every_handover_under_ten_seconds_and_intact_evidence() {
        let good = [round(1.0, 6.5, 6.75), round(0.5, 9.9, 9.99)];
        assert_eq!(criterion_five(&good, true), Verdict::Pass);
        assert_eq!(criterion_five(&good, false), Verdict::Fail);
        assert_eq!(
            criterion_five(&[round(1.0, 10.0, 6.0)], true),
            Verdict::Fail
        );
        assert_eq!(
            criterion_five(&[round(1.0, 6.0, 10.0)], true),
            Verdict::Fail
        );
        assert_eq!(
            criterion_five(&[round(30.0, 6.0, 6.0)], true),
            Verdict::Pass
        );
        assert_eq!(criterion_five(&[], true), Verdict::Fail);
    }
}
