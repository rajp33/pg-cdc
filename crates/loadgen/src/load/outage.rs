use std::io::Write as _;
use std::time::Duration;

use anyhow::Context as _;
use aws_sdk_s3::primitives::ByteStream;
use pg_cdc_testkit::{AwsEmulator, parse_lsn};
use serde::Serialize;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_postgres::Client;
use tokio_util::sync::CancellationToken;

use super::rig::{self, Bench, ConfigSpec, LAG_LIMIT_BYTES, SinkSpec, StoreSpec};
use super::{LoadOptions, Verdict, write_report};
use crate::cluster::PgCdc;
use crate::measure::Machine;
use crate::proxy::{FaultProxy, Faults, Mode, ProxyCounts, Upstream};
use crate::workload::{self, Ledger};

const SLOT: &str = "outage_slot";
const BUCKET: &str = "outage-state";
const TOPIC: &str = "outage";
const WRITE_RATE: u32 = 1_000;
const WRITE_WINDOW: Duration = Duration::from_secs(150);
const OUTAGE_START: Duration = Duration::from_secs(10);
const OUTAGE_END: Duration = Duration::from_secs(70);
const BROWNOUT_SPLIT: Duration = Duration::from_secs(40);
const BROWNOUT_FIRST: Duration = Duration::from_millis(500);
const BROWNOUT_SECOND: Duration = Duration::from_secs(2);
const PARK_AFTER: Duration = Duration::from_secs(5);
const PARK_AFTER_CONFIG: &str = "5s";
const MAX_INFLIGHT: &str = "64MiB";
const MAX_INFLIGHT_BYTES: u64 = 64 * 1024 * 1024;
const SAMPLE_PERIOD: Duration = Duration::from_secs(1);
#[cfg(test)]
const DEFAULT_DRAIN_LIMIT: Duration = Duration::from_mins(3);
const PARKED_PREFIX: &str = "load/load/parked/";
const LIST_PROBE_PERIOD: Duration = Duration::from_secs(30);
const CEILING_BUCKET: &str = "outage-ceiling";
const CEILING_WORKERS: usize = 32;
const CEILING_WINDOW: Duration = Duration::from_secs(10);
const CEILING_BODY_BYTES: usize = 700;
const RETAINED_FACTOR: u64 = 3;
const KEEPS_UP_SHARE: f64 = 0.95;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Outage,
    Brownout,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Self::Outage => "outage",
            Self::Brownout => "brownout",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct Step {
    at: Duration,
    sns: Mode,
    s3: Mode,
}

#[derive(Debug, Clone, PartialEq)]
struct Phase {
    label: String,
    from_s: f64,
    to_s: f64,
    s3_latency_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct Sample {
    t_s: f64,
    parked_puts: u64,
    parked_stored: u64,
    parked_events: u64,
    parked_keys: u64,
    inflight_bytes: u64,
    slot_retained_bytes: Option<u64>,
    lag_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
struct Peaks {
    parked_events: u64,
    parked_keys: u64,
    inflight_bytes: u64,
    lag_bytes: u64,
    slot_retained_bytes: u64,
}

fn schedule(kind: Kind) -> Vec<Step> {
    let down = |at, s3| Step {
        at,
        sns: Mode::Outage,
        s3,
    };
    let healthy = Step {
        at: OUTAGE_END,
        sns: Mode::Healthy,
        s3: Mode::Healthy,
    };
    match kind {
        Kind::Outage => vec![down(OUTAGE_START, Mode::Healthy), healthy],
        Kind::Brownout => vec![
            down(OUTAGE_START, Mode::Brownout(BROWNOUT_FIRST)),
            down(BROWNOUT_SPLIT, Mode::Brownout(BROWNOUT_SECOND)),
            healthy,
        ],
    }
}

fn phases(steps: &[Step], park_after: Duration) -> Vec<Phase> {
    let mut first = true;
    let mut found = Vec::new();
    for pair in steps.windows(2) {
        let [now, next] = pair else {
            continue;
        };
        if now.sns != Mode::Outage {
            continue;
        }
        let start = if first { now.at + park_after } else { now.at };
        first = false;
        let (label, s3_latency_ms) = match now.s3 {
            Mode::Brownout(latency) => {
                let millis = u64::try_from(latency.as_millis()).unwrap_or(u64::MAX);
                (
                    format!("sink outage, store brownout {millis} ms"),
                    Some(millis),
                )
            }
            Mode::Healthy | Mode::Outage => ("sink outage".to_owned(), None),
        };
        found.push(Phase {
            label,
            from_s: start.as_secs_f64(),
            to_s: next.at.as_secs_f64(),
            s3_latency_ms,
        });
    }
    found
}

fn sample(
    elapsed: Duration,
    status: &serde_json::Value,
    (parked_puts, parked_stored): (u64, u64),
    wal: u64,
) -> Option<Sample> {
    let inflight_bytes = rig::inflight_bytes(status)?;
    let parked = status.get("parked").filter(|parked| !parked.is_null());
    let count = |field: &str| {
        parked
            .and_then(|parked| parked.get(field))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    };
    Some(Sample {
        t_s: elapsed.as_secs_f64(),
        parked_puts,
        parked_stored,
        parked_events: count("events"),
        parked_keys: count("keys"),
        inflight_bytes,
        slot_retained_bytes: status
            .get("slot_retained_bytes")
            .and_then(serde_json::Value::as_u64),
        lag_bytes: wal.saturating_sub(rig::confirmed(status).unwrap_or(0)),
    })
}

fn rate_per_second(
    samples: &[Sample],
    from_s: f64,
    to_s: f64,
    series: fn(&Sample) -> u64,
) -> Option<f64> {
    let first = samples.iter().find(|sample| sample.t_s >= from_s)?;
    let last = samples.iter().rev().find(|sample| sample.t_s <= to_s)?;
    let span = last.t_s - first.t_s;
    (span > 0.0).then(|| rig::count_f64(series(last).saturating_sub(series(first))) / span)
}

fn peaks(samples: &[Sample]) -> Peaks {
    samples
        .iter()
        .fold(Peaks::default(), |peaks, sample| Peaks {
            parked_events: peaks.parked_events.max(sample.parked_events),
            parked_keys: peaks.parked_keys.max(sample.parked_keys),
            inflight_bytes: peaks.inflight_bytes.max(sample.inflight_bytes),
            lag_bytes: peaks.lag_bytes.max(sample.lag_bytes),
            slot_retained_bytes: peaks
                .slot_retained_bytes
                .max(sample.slot_retained_bytes.unwrap_or(0)),
        })
}

fn caught_up(sample: &Sample) -> bool {
    sample.parked_keys == 0 && sample.lag_bytes < LAG_LIMIT_BYTES
}

fn catch_up_seconds(samples: &[Sample], outage_end_s: f64) -> Option<f64> {
    samples
        .iter()
        .find(|sample| sample.t_s >= outage_end_s && caught_up(sample))
        .map(|sample| sample.t_s - outage_end_s)
}

fn expected_retained(wal_bytes: u64, window: Duration) -> u64 {
    let scaled = u128::from(wal_bytes) * PARK_AFTER.as_millis() / window.as_millis().max(1);
    u64::try_from(scaled).unwrap_or(u64::MAX)
}

fn drain_rate(samples: &[Sample], from_s: f64) -> Option<f64> {
    let first = samples.iter().find(|sample| sample.t_s >= from_s)?;
    let last = samples.last()?;
    let span = last.t_s - first.t_s;
    (span > 0.0)
        .then(|| (rig::count_f64(first.parked_events) - rig::count_f64(last.parked_events)) / span)
}

fn criterion_three(
    peak_retained_bytes: u64,
    expected_retained_bytes: u64,
    write_rate: f64,
    moto_ceiling: Option<f64>,
) -> Verdict {
    if moto_ceiling.is_some_and(|ceiling| ceiling < write_rate) {
        return Verdict::Unverified;
    }
    if expected_retained_bytes > 0
        && peak_retained_bytes <= expected_retained_bytes.saturating_mul(RETAINED_FACTOR)
    {
        Verdict::Pass
    } else {
        Verdict::Fail
    }
}

#[derive(Debug)]
struct Fixture {
    bench: Bench,
    aws: AwsEmulator,
    s3: aws_sdk_s3::Client,
    sns_proxy: FaultProxy,
    s3_proxy: FaultProxy,
    process: PgCdc,
    config: String,
}

impl Fixture {
    fn apply(&self, step: &Step) {
        self.sns_proxy.set(Faults {
            mode: step.sns,
            ..Faults::HEALTHY
        });
        self.s3_proxy.set(Faults {
            mode: step.s3,
            ..Faults::HEALTHY
        });
    }
}

async fn start(options: &LoadOptions, kind: Kind) -> anyhow::Result<Fixture> {
    let bench = Bench::start(&[SLOT]).await?;
    let aws = AwsEmulator::start().await?;
    rig::create_bucket(&aws, BUCKET).await?;
    let topic_arn = rig::create_topic(&aws, TOPIC, false).await?;
    let upstream = aws.endpoint().trim_start_matches("http://").parse()?;
    let sns_proxy = FaultProxy::start(upstream, Upstream::Sns, 1).await?;
    let s3_proxy = FaultProxy::start(upstream, Upstream::S3, 2).await?;
    let spec = ConfigSpec {
        slot: SLOT.to_owned(),
        sink: SinkSpec::Sns { topic_arn },
        store: StoreSpec::S3 {
            bucket: BUCKET.to_owned(),
        },
        max_inflight_bytes: Some(MAX_INFLIGHT),
        park_after: Some(PARK_AFTER_CONFIG),
        otlp_endpoint: None,
    };
    let env = rig::aws_env(&sns_proxy.endpoint(), &s3_proxy.endpoint());
    let (process, config) = bench
        .spawn(&options.binary, &spec, &env, kind.name())
        .await?;
    rig::wait_ready(&process).await?;
    Ok(Fixture {
        bench,
        s3: aws.s3().await,
        aws,
        sns_proxy,
        s3_proxy,
        process,
        config,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
struct ListProbe {
    t_s: f64,
    parked_events: u64,
    list_ms: f64,
}

async fn list_latency(s3: &aws_sdk_s3::Client) -> Option<Duration> {
    let started = Instant::now();
    s3.list_objects_v2()
        .bucket(BUCKET)
        .prefix(PARKED_PREFIX)
        .max_keys(1)
        .send()
        .await
        .ok()?;
    Some(started.elapsed())
}

#[derive(Debug, Serialize)]
struct PutCeiling {
    workers: usize,
    body_bytes: usize,
    window_s: f64,
    puts: u64,
    puts_per_s: f64,
}

async fn moto_put_ceiling(fixture: &Fixture) -> anyhow::Result<PutCeiling> {
    rig::create_bucket(&fixture.aws, CEILING_BUCKET).await?;
    let started = Instant::now();
    let deadline = started + CEILING_WINDOW;
    let mut workers = JoinSet::new();
    for worker in 0..CEILING_WORKERS {
        let client = fixture.s3.clone();
        workers.spawn(async move {
            let mut puts: u64 = 0;
            while Instant::now() < deadline {
                client
                    .put_object()
                    .bucket(CEILING_BUCKET)
                    .key(format!("{worker}/{puts}.json"))
                    .if_none_match("*")
                    .body(ByteStream::from(vec![b'x'; CEILING_BODY_BYTES]))
                    .send()
                    .await
                    .map_err(rig::aws_error)?;
                puts += 1;
            }
            anyhow::Ok(puts)
        });
    }
    let mut puts = 0;
    while let Some(joined) = workers.join_next().await {
        puts += joined.context("a PutObject worker panicked")??;
    }
    let window = started.elapsed();
    Ok(PutCeiling {
        workers: CEILING_WORKERS,
        body_bytes: CEILING_BODY_BYTES,
        window_s: window.as_secs_f64(),
        puts,
        puts_per_s: rig::per_second(puts, window),
    })
}

async fn wal_position(probe: &Client) -> anyhow::Result<u64> {
    let text: String = probe
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await?
        .get(0);
    Ok(parse_lsn(&text)?)
}

#[derive(Debug)]
struct Driven {
    samples: Vec<Sample>,
    list_probes: Vec<ListProbe>,
    missed_samples: u32,
    ledger: Ledger,
    write_window: Duration,
    wal_bytes: u64,
    proxies: [ProxyCounts; 2],
}

fn finished(
    samples: &[Sample],
    write_window: Option<Duration>,
    writer_done: bool,
    elapsed: Duration,
    limit: Duration,
) -> bool {
    let Some(stopped) = write_window else {
        return false;
    };
    let caught_up = writer_done
        && samples
            .last()
            .is_some_and(|last| last.t_s >= OUTAGE_END.as_secs_f64() && caught_up(last));
    caught_up || elapsed >= stopped + limit
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
struct Drain {
    deadline_s: f64,
    from_s: f64,
    to_s: f64,
    remaining_events: u64,
    remaining_keys: u64,
    events_per_s: Option<f64>,
    projected_s: Option<f64>,
    complete: bool,
}

fn drain(samples: &[Sample], from_s: f64, limit: Duration) -> Drain {
    let last = samples.last().filter(|last| last.t_s >= from_s);
    let remaining_events = last.map_or(0, |last| last.parked_events);
    let events_per_s = drain_rate(samples, from_s);
    let projected_s = match events_per_s {
        _ if last.is_some() && remaining_events == 0 => Some(0.0),
        Some(rate) if rate > 0.0 => Some(rig::count_f64(remaining_events) / rate),
        _ => None,
    };
    Drain {
        deadline_s: limit.as_secs_f64(),
        from_s,
        to_s: last.map_or(from_s, |last| last.t_s),
        remaining_events,
        remaining_keys: last.map_or(0, |last| last.parked_keys),
        events_per_s,
        projected_s,
        complete: last.is_some_and(caught_up),
    }
}

async fn drive(fixture: &Fixture, kind: Kind, limit: Duration) -> anyhow::Result<Driven> {
    let writer_client = fixture.bench.postgres.client().await?;
    let probe = fixture.bench.postgres.client().await?;
    let stop = CancellationToken::new();
    let wal_start = wal_position(&probe).await?;
    let started = Instant::now();
    let writer: JoinHandle<anyhow::Result<Ledger>> = tokio::spawn({
        let stop = stop.clone();
        async move { workload::write(&writer_client, &rig::workload(WRITE_RATE), stop).await }
    });
    let mut steps = schedule(kind).into_iter().peekable();
    let mut ticker = tokio::time::interval(SAMPLE_PERIOD);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut samples = Vec::new();
    let mut list_probes = Vec::new();
    let mut next_probe = Duration::ZERO;
    let mut missed_samples = 0;
    let mut write_window = None;
    loop {
        ticker.tick().await;
        let elapsed = started.elapsed();
        while let Some(step) = steps.next_if(|step| step.at <= elapsed) {
            fixture.apply(&step);
        }
        if write_window.is_none() && elapsed >= WRITE_WINDOW {
            stop.cancel();
            write_window = Some(elapsed);
        }
        anyhow::ensure!(
            write_window.is_some() || !writer.is_finished(),
            "the workload ended early: {:?}",
            writer.await
        );
        let wal = wal_position(&probe).await?;
        let counts = fixture.s3_proxy.counts();
        let taken = match fixture.process.status().await {
            Ok(status) => sample(
                elapsed,
                &status,
                (counts.parked_puts, counts.parked_stored),
                wal,
            ),
            Err(_) => None,
        };
        match taken {
            Some(taken) => samples.push(taken),
            None => missed_samples += 1,
        }
        if elapsed >= next_probe {
            next_probe = elapsed + LIST_PROBE_PERIOD;
            if let (Some(latency), Some(last)) = (list_latency(&fixture.s3).await, samples.last()) {
                list_probes.push(ListProbe {
                    t_s: elapsed.as_secs_f64(),
                    parked_events: last.parked_events,
                    list_ms: latency.as_secs_f64() * 1_000.0,
                });
            }
        }
        if finished(&samples, write_window, writer.is_finished(), elapsed, limit) {
            break;
        }
    }
    stop.cancel();
    let ledger = writer.await.context("the workload task panicked")??;
    let wal_bytes = rig::last_lsn(&ledger)?.saturating_sub(wal_start);
    Ok(Driven {
        samples,
        list_probes,
        missed_samples,
        ledger,
        write_window: write_window.unwrap_or(WRITE_WINDOW),
        wal_bytes,
        proxies: [fixture.sns_proxy.counts(), fixture.s3_proxy.counts()],
    })
}

#[derive(Debug, Serialize)]
struct PhaseReport {
    label: String,
    from_s: f64,
    to_s: f64,
    s3_latency_ms: Option<u64>,
    parked_puts_per_s: Option<f64>,
    parked_stored_per_s: Option<f64>,
    keeps_up_with_writes: Option<bool>,
    parked_events_at_end: u64,
}

#[derive(Debug, Serialize)]
struct Report {
    scenario: &'static str,
    machine: Machine,
    config: String,
    workload: String,
    write_rate: u32,
    park_after_s: f64,
    max_inflight_bytes: u64,
    outage_start_s: f64,
    outage_end_s: f64,
    window_s: f64,
    written_events: u64,
    written_rate: f64,
    wal_bytes_per_s: f64,
    expected_retained_bytes: u64,
    peak_retained_bytes: u64,
    peaks: Peaks,
    phases: Vec<PhaseReport>,
    parked_puts_per_s: Option<f64>,
    parked_stored_per_s: Option<f64>,
    drain: Drain,
    list_probes: Vec<ListProbe>,
    moto_put_ceiling: PutCeiling,
    reader_stalled: bool,
    catch_up_s: Option<f64>,
    drained: bool,
    missed_samples: u32,
    proxy: [ProxyCounts; 2],
    samples: Vec<Sample>,
    #[serde(skip_serializing_if = "Option::is_none")]
    criterion_3: Option<Verdict>,
}

fn phase_reports(samples: &[Sample], phases: &[Phase]) -> Vec<PhaseReport> {
    let offered = f64::from(WRITE_RATE);
    phases
        .iter()
        .map(|phase| {
            let puts = rate_per_second(samples, phase.from_s, phase.to_s, |s| s.parked_puts);
            let rate = rate_per_second(samples, phase.from_s, phase.to_s, |s| s.parked_stored);
            PhaseReport {
                label: phase.label.clone(),
                from_s: phase.from_s,
                to_s: phase.to_s,
                s3_latency_ms: phase.s3_latency_ms,
                parked_puts_per_s: puts,
                parked_stored_per_s: rate,
                keeps_up_with_writes: rate.map(|rate| rate >= offered * KEEPS_UP_SHARE),
                parked_events_at_end: samples
                    .iter()
                    .rev()
                    .find(|sample| sample.t_s <= phase.to_s)
                    .map_or(0, |sample| sample.parked_events),
            }
        })
        .collect()
}

fn report(
    kind: Kind,
    fixture: &Fixture,
    driven: Driven,
    moto_put_ceiling: PutCeiling,
    limit: Duration,
) -> anyhow::Result<Report> {
    let steps = schedule(kind);
    let phases = phases(&steps, PARK_AFTER);
    let written_events = u64::try_from(driven.ledger.committed.len())?;
    let window = driven.write_window;
    let wal_per_s = rig::per_second(driven.wal_bytes, window);
    let expected_retained_bytes = expected_retained(driven.wal_bytes, window);
    let peaks = peaks(&driven.samples);
    let peak_retained_bytes = peaks.slot_retained_bytes;
    let first = phases.first().map_or(0.0, |phase| phase.from_s);
    let last = phases.last().map_or(0.0, |phase| phase.to_s);
    let outage_end_s = OUTAGE_END.as_secs_f64();
    let catch_up_s = catch_up_seconds(&driven.samples, outage_end_s);
    Ok(Report {
        scenario: kind.name(),
        machine: fixture.bench.machine.clone(),
        config: fixture.config.clone(),
        workload: format!("{:?}", rig::workload(WRITE_RATE)),
        write_rate: WRITE_RATE,
        park_after_s: PARK_AFTER.as_secs_f64(),
        max_inflight_bytes: MAX_INFLIGHT_BYTES,
        outage_start_s: OUTAGE_START.as_secs_f64(),
        outage_end_s,
        window_s: window.as_secs_f64(),
        written_events,
        written_rate: rig::per_second(written_events, window),
        wal_bytes_per_s: wal_per_s,
        expected_retained_bytes,
        peak_retained_bytes,
        phases: phase_reports(&driven.samples, &phases),
        parked_puts_per_s: rate_per_second(&driven.samples, first, last, |s| s.parked_puts),
        parked_stored_per_s: rate_per_second(&driven.samples, first, last, |s| s.parked_stored),
        reader_stalled: peaks.inflight_bytes >= MAX_INFLIGHT_BYTES,
        catch_up_s,
        drained: catch_up_s.is_some(),
        missed_samples: driven.missed_samples,
        proxy: driven.proxies,
        criterion_3: (kind == Kind::Outage).then(|| {
            criterion_three(
                peak_retained_bytes,
                expected_retained_bytes,
                f64::from(WRITE_RATE),
                Some(moto_put_ceiling.puts_per_s),
            )
        }),
        drain: drain(&driven.samples, window.as_secs_f64(), limit),
        list_probes: driven.list_probes,
        moto_put_ceiling,
        peaks,
        samples: driven.samples,
    })
}

async fn run_kind(kind: Kind, options: &LoadOptions) -> anyhow::Result<()> {
    let mut fixture = start(options, kind).await?;
    let driven = drive(&fixture, kind, options.drain_limit).await?;
    let stopped = rig::stop(&mut fixture.process).await?;
    let ceiling = moto_put_ceiling(&fixture).await?;
    let report = report(kind, &fixture, driven, ceiling, options.drain_limit)?;
    for phase in &report.phases {
        writeln!(
            std::io::stdout(),
            "load {}: {} stored {:?} events/s in {:?} put attempts/s (keeps up with writes: {:?}), {} events parked at its end",
            kind.name(),
            phase.label,
            phase.parked_stored_per_s.map(f64::round),
            phase.parked_puts_per_s.map(f64::round),
            phase.keeps_up_with_writes,
            phase.parked_events_at_end,
        )?;
    }
    writeln!(
        std::io::stdout(),
        "load {}: peak retained {} KiB against {} KiB expected, peak in-flight {} KiB (stalled: {}), catch-up {:?} s, pg-cdc stop: {stopped}, criterion 3 {}",
        kind.name(),
        report.peak_retained_bytes >> 10,
        report.expected_retained_bytes >> 10,
        report.peaks.inflight_bytes >> 10,
        report.reader_stalled,
        report.catch_up_s,
        serde_json::to_string(&report.criterion_3)?,
    )?;
    writeln!(
        std::io::stdout(),
        "load {}: drain {} at the {:.0} s limit: {} events on {} keys still parked {:.0} s after the writer stopped, parked events falling at {:?} events/s (negative while still rising), projected {:?} s more to drain",
        kind.name(),
        if report.drain.complete {
            "complete"
        } else {
            "incomplete"
        },
        report.drain.deadline_s,
        report.drain.remaining_events,
        report.drain.remaining_keys,
        report.drain.to_s - report.drain.from_s,
        report
            .drain
            .events_per_s
            .map(|rate| (rate * 10.0).round() / 10.0),
        report.drain.projected_s.map(f64::round),
    )?;
    write_report(options, kind.name(), &report).await
}

pub(super) async fn run_outage(options: &LoadOptions) -> anyhow::Result<()> {
    Box::pin(run_kind(Kind::Outage, options)).await
}

pub(super) async fn run_brownout(options: &LoadOptions) -> anyhow::Result<()> {
    Box::pin(run_kind(Kind::Brownout, options)).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const SECOND: Duration = Duration::from_secs(1);
    const PARK_AFTER: Duration = Duration::from_secs(5);

    fn at(seconds: u64) -> Duration {
        Duration::from_secs(seconds)
    }

    fn step(seconds: u64, sns: Mode, s3: Mode) -> Step {
        Step {
            at: at(seconds),
            sns,
            s3,
        }
    }

    #[test]
    fn an_outage_takes_the_sink_down_for_sixty_seconds_and_leaves_the_store_alone() {
        assert_eq!(
            schedule(Kind::Outage),
            [
                step(10, Mode::Outage, Mode::Healthy),
                step(70, Mode::Healthy, Mode::Healthy),
            ]
        );
    }

    #[test]
    fn a_brownout_slows_the_store_by_half_a_second_then_two_during_the_sink_outage() {
        assert_eq!(
            schedule(Kind::Brownout),
            [
                step(10, Mode::Outage, Mode::Brownout(Duration::from_millis(500))),
                step(40, Mode::Outage, Mode::Brownout(Duration::from_secs(2))),
                step(70, Mode::Healthy, Mode::Healthy),
            ]
        );
    }

    #[test]
    fn parking_starts_park_after_into_the_outage_and_each_store_phase_is_its_own_window() {
        let outage = phases(&schedule(Kind::Outage), PARK_AFTER);
        assert_eq!(
            outage,
            [Phase {
                label: "sink outage".to_owned(),
                from_s: 15.0,
                to_s: 70.0,
                s3_latency_ms: None,
            }]
        );
        let brownout = phases(&schedule(Kind::Brownout), PARK_AFTER);
        assert_eq!(
            brownout,
            [
                Phase {
                    label: "sink outage, store brownout 500 ms".to_owned(),
                    from_s: 15.0,
                    to_s: 40.0,
                    s3_latency_ms: Some(500),
                },
                Phase {
                    label: "sink outage, store brownout 2000 ms".to_owned(),
                    from_s: 40.0,
                    to_s: 70.0,
                    s3_latency_ms: Some(2_000),
                },
            ]
        );
    }

    #[test]
    fn a_sample_reads_the_status_the_proxy_count_and_the_wal_position() {
        let status = json!({
            "confirmed_lsn": "0/1000",
            "inflight": { "events": 3, "bytes": 4_096 },
            "parked": { "events": 70, "keys": 12, "dead": 0, "oldest_age_seconds": 2.0 },
            "slot_retained_bytes": 8_192,
        });
        assert_eq!(
            sample(
                at(7) + Duration::from_millis(250),
                &status,
                (40, 31),
                0x1000 + 600
            ),
            Some(Sample {
                t_s: 7.25,
                parked_puts: 40,
                parked_stored: 31,
                parked_events: 70,
                parked_keys: 12,
                inflight_bytes: 4_096,
                slot_retained_bytes: Some(8_192),
                lag_bytes: 600,
            })
        );

        let starting = json!({
            "confirmed_lsn": null,
            "inflight": { "events": 0, "bytes": 0 },
            "parked": null,
            "slot_retained_bytes": null,
        });
        assert_eq!(
            sample(at(1), &starting, (0, 0), 5_000),
            Some(Sample {
                t_s: 1.0,
                parked_puts: 0,
                parked_stored: 0,
                parked_events: 0,
                parked_keys: 0,
                inflight_bytes: 0,
                slot_retained_bytes: None,
                lag_bytes: 5_000,
            })
        );
        assert_eq!(sample(at(1), &json!({}), (0, 0), 0), None);
    }

    fn at_second(t: u32, puts: u64, events: u64, keys: u64, lag: u64) -> Sample {
        Sample {
            t_s: SECOND.as_secs_f64() * f64::from(t),
            parked_puts: puts,
            parked_stored: puts / 4,
            parked_events: events,
            parked_keys: keys,
            inflight_bytes: u64::from(t) * 1_000,
            slot_retained_bytes: Some(u64::from(t) * 10),
            lag_bytes: lag,
        }
    }

    fn samples() -> Vec<Sample> {
        vec![
            at_second(14, 0, 0, 0, 500),
            at_second(15, 0, 0, 0, 900),
            at_second(25, 6_000, 6_000, 400, 3_000),
            at_second(35, 14_000, 14_000, 900, 5_000),
            at_second(70, 40_000, 40_000, 1_000, 8_000),
            at_second(71, 40_100, 39_800, 990, 20_000_000),
            at_second(80, 41_000, 20_000, 700, 9_000_000),
            at_second(90, 41_500, 0, 0, 20_000_000),
            at_second(91, 41_500, 0, 0, 1_000),
        ]
    }

    #[test]
    fn a_rate_is_the_growth_between_the_first_and_last_samples_of_its_window() {
        let samples = samples();
        let puts = |sample: &Sample| sample.parked_puts;
        let stored = |sample: &Sample| sample.parked_stored;
        let rate = rate_per_second(&samples, 15.0, 35.0, puts).unwrap();
        assert!((rate - 700.0).abs() < 1e-9, "{rate}");
        let kept = rate_per_second(&samples, 15.0, 35.0, stored).unwrap();
        assert!((kept - 175.0).abs() < 1e-9, "{kept}");
        let whole = rate_per_second(&samples, 15.0, 70.0, puts).unwrap();
        assert!((whole - 40_000.0 / 55.0).abs() < 1e-9, "{whole}");
        assert_eq!(rate_per_second(&samples, 200.0, 300.0, puts), None);
        assert_eq!(rate_per_second(&samples, 25.0, 25.0, puts), None);
        assert_eq!(rate_per_second(&[], 0.0, 10.0, puts), None);
    }

    #[test]
    fn peaks_are_the_largest_value_seen_in_each_series() {
        assert_eq!(
            peaks(&samples()),
            Peaks {
                parked_events: 40_000,
                parked_keys: 1_000,
                inflight_bytes: 91_000,
                lag_bytes: 20_000_000,
                slot_retained_bytes: 910,
            }
        );
        assert_eq!(peaks(&[]), Peaks::default());
    }

    #[test]
    fn catch_up_runs_from_the_outage_end_to_no_parked_keys_and_lag_under_16_mib() {
        let samples = samples();
        assert_eq!(catch_up_seconds(&samples, 70.0), Some(21.0));
        assert_eq!(catch_up_seconds(&samples, 90.5), Some(0.5));
        assert_eq!(catch_up_seconds(&samples, 92.0), None);
        assert_eq!(catch_up_seconds(&[], 0.0), None);
    }

    #[test]
    fn the_expected_retention_is_park_afters_share_of_the_wal_written() {
        assert_eq!(expected_retained(150_000_000, at(150)), 5_000_000);
        assert_eq!(expected_retained(1_000, at(0)), 5_000_000);
        assert_eq!(expected_retained(0, at(150)), 0);
    }

    #[test]
    fn criterion_three_allows_three_times_park_afters_worth_of_writes() {
        let verdict = |peak, expected| criterion_three(peak, expected, 1_000.0, None);
        assert_eq!(verdict(3_000_000, 1_000_000), Verdict::Pass);
        assert_eq!(verdict(3_000_001, 1_000_000), Verdict::Fail);
        assert_eq!(verdict(0, 1_000_000), Verdict::Pass);
        assert_eq!(verdict(0, 0), Verdict::Fail);
    }

    #[test]
    fn a_store_that_cannot_take_the_write_rate_in_puts_leaves_criterion_three_unverified() {
        let verdict = |ceiling| criterion_three(90_000_000, 1_000_000, 1_000.0, Some(ceiling));
        assert_eq!(verdict(400.0), Verdict::Unverified);
        assert_eq!(verdict(999.9), Verdict::Unverified);
        assert_eq!(verdict(1_000.0), Verdict::Fail);
        assert_eq!(
            criterion_three(2_000_000, 1_000_000, 1_000.0, Some(1_500.0)),
            Verdict::Pass
        );
    }

    #[test]
    fn the_drain_rate_is_the_fall_in_parked_events_after_the_writer_stops() {
        let samples = samples();
        let rate = drain_rate(&samples, 71.0).unwrap();
        assert!((rate - 39_800.0 / 20.0).abs() < 1e-9, "{rate}");
        let after_writes = drain_rate(&samples, 80.0).unwrap();
        assert!(
            (after_writes - 20_000.0 / 11.0).abs() < 1e-9,
            "{after_writes}"
        );
        assert_eq!(drain_rate(&samples, 91.0), None);
        assert_eq!(drain_rate(&[], 0.0), None);
    }

    #[test]
    fn the_run_ends_at_the_drain_limit_after_the_writer_stops_or_once_caught_up() {
        let limit = DEFAULT_DRAIN_LIMIT;
        let backlog = [at_second(150, 0, 100_000, 1_000, 0)];
        assert!(!finished(&backlog, None, false, at(400), limit));
        assert!(!finished(&backlog, Some(at(150)), true, at(329), limit));
        assert!(finished(&backlog, Some(at(150)), true, at(330), limit));
        assert!(finished(&backlog, Some(at(150)), false, at(330), limit));
        let drained = [at_second(160, 0, 0, 0, 1_000)];
        assert!(finished(&drained, Some(at(150)), true, at(160), limit));
        assert!(!finished(&drained, Some(at(150)), false, at(160), limit));
        assert!(!finished(&drained, None, true, at(160), limit));

        let hours = at(7_200);
        assert!(!finished(&backlog, Some(at(150)), true, at(330), hours));
        assert!(!finished(&backlog, Some(at(150)), true, at(7_349), hours));
        assert!(finished(&backlog, Some(at(150)), true, at(7_350), hours));
    }

    #[test]
    fn a_bounded_drain_reports_what_remains_its_rate_and_the_time_to_drain_the_rest() {
        let samples = [
            at_second(149, 0, 99_000, 1_000, 0),
            at_second(150, 0, 100_000, 1_000, 0),
            at_second(240, 0, 98_200, 1_000, 0),
            at_second(330, 0, 96_400, 990, 0),
        ];
        assert_eq!(
            drain(&samples, 150.0, DEFAULT_DRAIN_LIMIT),
            Drain {
                deadline_s: 180.0,
                from_s: 150.0,
                to_s: 330.0,
                remaining_events: 96_400,
                remaining_keys: 990,
                events_per_s: Some(20.0),
                projected_s: Some(4_820.0),
                complete: false,
            }
        );
        assert_eq!(drain(&samples, 150.0, at(3_600)).deadline_s, 3_600.0);

        let growing = [at_second(150, 0, 100, 10, 0), at_second(160, 0, 200, 10, 0)];
        let grew = drain(&growing, 150.0, DEFAULT_DRAIN_LIMIT);
        assert_eq!(grew.events_per_s, Some(-10.0));
        assert_eq!(grew.projected_s, None);

        assert!(!grew.complete);

        let emptied = [at_second(150, 0, 100, 10, 0), at_second(160, 0, 0, 0, 0)];
        let caught_up = drain(&emptied, 150.0, DEFAULT_DRAIN_LIMIT);
        assert_eq!(caught_up.projected_s, Some(0.0));
        assert!(caught_up.complete);
        let lagging = [
            at_second(150, 0, 100, 10, 0),
            at_second(160, 0, 0, 0, 20_000_000),
        ];
        assert!(!drain(&lagging, 150.0, DEFAULT_DRAIN_LIMIT).complete);
        assert_eq!(
            drain(&[], 150.0, DEFAULT_DRAIN_LIMIT),
            Drain {
                deadline_s: 180.0,
                from_s: 150.0,
                to_s: 150.0,
                remaining_events: 0,
                remaining_keys: 0,
                events_per_s: None,
                projected_s: None,
                complete: false,
            }
        );
    }
}
