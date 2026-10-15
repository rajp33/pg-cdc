use std::io::Write as _;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Context as _;
use pg_cdc_testkit::AwsEmulator;
use serde::Serialize;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::rig::{self, Bench, ConfigSpec, DRAIN_LIMIT, SinkSpec, StoreSpec, inflight_bytes};
use super::{LoadOptions, Verdict, write_report};
use crate::cluster::PgCdc;
use crate::measure::{self, Machine};
use crate::proxy::{FaultProxy, Faults, Mode, Upstream};
use crate::workload::{self, WorkloadConfig};

const MAX_INFLIGHT: &str = "64MiB";
const MAX_INFLIGHT_BYTES: u64 = 64 * 1024 * 1024;
const SMALL_ROWS: u32 = 50_000;
const LARGE_ROWS: u32 = 500_000;
const ROW_BYTES: usize = 1_024;
const KEYS: u32 = 1_000;
const IDLE_WAIT: Duration = Duration::from_secs(5);
const RSS_PERIOD: Duration = Duration::from_millis(100);
const BOUND_WAIT: Duration = Duration::from_secs(300);
const BOUND_OBSERVATION: Duration = Duration::from_secs(30);
const STATUS_PERIOD: Duration = Duration::from_millis(500);
const READY_WAIT: Duration = Duration::from_secs(60);
const TOPIC: &str = "memory";
const GROWTH_PERCENT: u128 = 110;
const FIXED_OVERHEAD_FACTOR: u64 = 2;

#[derive(Debug, Clone, Serialize)]
struct SizeRun {
    rows: u32,
    idle_rss_bytes: u64,
    peak_rss_bytes: u64,
    max_inflight_bytes_seen: Option<u64>,
    stop: Option<String>,
}

#[derive(Debug, Serialize)]
struct ModeReport {
    mode: &'static str,
    config: String,
    runs: Vec<SizeRun>,
    verdict: Verdict,
}

#[derive(Debug, Serialize)]
struct Report {
    scenario: &'static str,
    machine: Machine,
    workload: String,
    max_inflight_bytes: u64,
    modes: Vec<ModeReport>,
    criterion_6: Verdict,
}

fn memory_verdict(small: (u64, u64), large: (u64, u64), bound: u64) -> Verdict {
    let inside = |(idle, peak): (u64, u64)| {
        peak <= idle.saturating_add(bound.saturating_mul(FIXED_OVERHEAD_FACTOR))
    };
    let flat = u128::from(large.1) * 100 <= u128::from(small.1) * GROWTH_PERCENT;
    if inside(small) && inside(large) && flat {
        Verdict::Pass
    } else {
        Verdict::Fail
    }
}

fn workload_for(rows: u32) -> WorkloadConfig {
    WorkloadConfig {
        rate: 0,
        tx_size: rows..=rows,
        row_bytes: ROW_BYTES,
        toast_every: 0,
        oversize_every: 0,
        keys: KEYS,
        skew: 0.0,
        seed: 1,
    }
}

#[derive(Debug)]
struct PeakRss {
    peak: Arc<AtomicU64>,
    task: JoinHandle<()>,
}

impl PeakRss {
    fn start(pid: u32) -> Self {
        let peak = Arc::new(AtomicU64::new(0));
        let task = tokio::spawn({
            let peak = Arc::clone(&peak);
            async move {
                loop {
                    if let Ok(sample) = measure::sample(pid).await {
                        peak.fetch_max(sample.rss_bytes, Ordering::Relaxed);
                    }
                    tokio::time::sleep(RSS_PERIOD).await;
                }
            }
        });
        Self { peak, task }
    }

    fn take(&self) -> u64 {
        self.peak.swap(0, Ordering::Relaxed)
    }
}

impl Drop for PeakRss {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn spec(
    slot: &str,
    sink: SinkSpec,
    store: StoreSpec,
    park_after: Option<&'static str>,
) -> ConfigSpec {
    ConfigSpec {
        slot: slot.to_owned(),
        sink,
        store,
        max_inflight_bytes: Some(MAX_INFLIGHT),
        park_after,
        otlp_endpoint: None,
    }
}

async fn write_transaction(bench: &Bench, rows: u32) -> anyhow::Result<u64> {
    let ledger = workload::write_events(
        &bench.client,
        &workload_for(rows),
        CancellationToken::new(),
        Some(u64::from(rows)),
    )
    .await?;
    rig::last_lsn(&ledger)
}

async fn idle_rss(process: &PgCdc) -> anyhow::Result<u64> {
    tokio::time::sleep(IDLE_WAIT).await;
    let pid = process.pid().context("pg-cdc is not running")?;
    Ok(measure::sample(pid).await?.rss_bytes)
}

async fn discard_mode(bench: &Bench, options: &LoadOptions) -> anyhow::Result<ModeReport> {
    let slot = "memory_discard";
    bench.postgres.create_slot(slot).await?;
    let store = StoreSpec::Filesystem(bench.scratch("discard-store").await?);
    let (mut process, config) = bench
        .spawn(
            &options.binary,
            &spec(slot, SinkSpec::Discard, store, None),
            &[],
            "discard",
        )
        .await?;
    let idle = idle_rss(&process).await?;
    let sampler = PeakRss::start(process.pid().context("pg-cdc is not running")?);
    let mut runs = Vec::new();
    for rows in [SMALL_ROWS, LARGE_ROWS] {
        let target = write_transaction(bench, rows).await?;
        rig::drain(&process, target, Instant::now(), DRAIN_LIMIT).await?;
        runs.push(SizeRun {
            rows,
            idle_rss_bytes: idle,
            peak_rss_bytes: sampler.take(),
            max_inflight_bytes_seen: None,
            stop: None,
        });
    }
    rig::stop(&mut process).await?;
    Ok(finish("discard", config, runs))
}

fn finish(mode: &'static str, config: String, runs: Vec<SizeRun>) -> ModeReport {
    let verdict = match runs.as_slice() {
        [small, large] => memory_verdict(
            (small.idle_rss_bytes, small.peak_rss_bytes),
            (large.idle_rss_bytes, large.peak_rss_bytes),
            MAX_INFLIGHT_BYTES,
        ),
        _ => Verdict::Fail,
    };
    ModeReport {
        mode,
        config,
        runs,
        verdict,
    }
}

async fn wait_ready(process: &PgCdc) -> anyhow::Result<()> {
    let deadline = Instant::now() + READY_WAIT;
    loop {
        let status = process.status().await?;
        if status.get("ready").and_then(serde_json::Value::as_bool) == Some(true) {
            return Ok(());
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "pg-cdc never became ready: {status}"
        );
        tokio::time::sleep(STATUS_PERIOD).await;
    }
}

async fn observe_bound(process: &PgCdc) -> anyhow::Result<u64> {
    let deadline = Instant::now() + BOUND_WAIT;
    loop {
        let status = process.status().await?;
        if inflight_bytes(&status).is_some_and(|bytes| bytes >= MAX_INFLIGHT_BYTES) {
            break;
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "in-flight bytes never reached the bound: {status}"
        );
        tokio::time::sleep(STATUS_PERIOD).await;
    }
    let end = Instant::now() + BOUND_OBSERVATION;
    let mut seen = 0;
    while Instant::now() < end {
        let status = process.status().await?;
        seen = seen.max(inflight_bytes(&status).unwrap_or(0));
        tokio::time::sleep(STATUS_PERIOD).await;
    }
    Ok(seen)
}

async fn stalled_run(
    bench: &Bench,
    aws: &AwsEmulator,
    topic_arn: &str,
    binary: &Path,
    rows: u32,
) -> anyhow::Result<(SizeRun, String)> {
    let slot = format!("memory_stalled_{rows}");
    bench.postgres.create_slot(&slot).await?;
    let upstream = aws.endpoint().trim_start_matches("http://").parse()?;
    let proxy = FaultProxy::start(upstream, Upstream::Sns, u64::from(rows)).await?;
    let store = StoreSpec::Filesystem(bench.scratch(&format!("stalled-{rows}-store")).await?);
    let sink = SinkSpec::Sns {
        topic_arn: topic_arn.to_owned(),
    };
    let env = [
        ("AWS_ENDPOINT_URL_SNS".to_owned(), proxy.endpoint()),
        ("AWS_ACCESS_KEY_ID".to_owned(), "test".to_owned()),
        ("AWS_SECRET_ACCESS_KEY".to_owned(), "test".to_owned()),
        ("AWS_REGION".to_owned(), "us-east-1".to_owned()),
        ("AWS_EC2_METADATA_DISABLED".to_owned(), "true".to_owned()),
    ];
    let (mut process, config) = bench
        .spawn(
            binary,
            &spec(&slot, sink, store, Some("1h")),
            &env,
            &format!("stalled-{rows}"),
        )
        .await?;
    wait_ready(&process).await?;
    let idle = idle_rss(&process).await?;
    proxy.set(Faults {
        mode: Mode::Outage,
        ..Faults::HEALTHY
    });
    let sampler = PeakRss::start(process.pid().context("pg-cdc is not running")?);
    write_transaction(bench, rows).await?;
    let seen = observe_bound(&process).await?;
    let peak = sampler.take();
    let stop = rig::stop(&mut process).await?;
    let run = SizeRun {
        rows,
        idle_rss_bytes: idle,
        peak_rss_bytes: peak,
        max_inflight_bytes_seen: Some(seen),
        stop: Some(stop),
    };
    Ok((run, config))
}

async fn stalled_mode(bench: &Bench, options: &LoadOptions) -> anyhow::Result<ModeReport> {
    let aws = AwsEmulator::start().await?;
    let topic_arn = aws
        .sns()
        .await
        .create_topic()
        .name(TOPIC)
        .send()
        .await
        .map_err(|error| anyhow::anyhow!("{}", aws_sdk_sns::error::DisplayErrorContext(error)))?
        .topic_arn()
        .context("CreateTopic returned no ARN")?
        .to_owned();
    let mut runs = Vec::new();
    let mut config = String::new();
    for rows in [SMALL_ROWS, LARGE_ROWS] {
        let (run, text) = stalled_run(bench, &aws, &topic_arn, &options.binary, rows).await?;
        runs.push(run);
        config = text;
    }
    Ok(finish("stalled", config, runs))
}

pub(super) async fn run(options: &LoadOptions) -> anyhow::Result<()> {
    let bench = Bench::start(&[]).await?;
    let modes = vec![
        discard_mode(&bench, options).await?,
        stalled_mode(&bench, options).await?,
    ];
    let criterion_6 = if modes.iter().all(|mode| mode.verdict == Verdict::Pass) {
        Verdict::Pass
    } else {
        Verdict::Fail
    };
    for mode in &modes {
        for run in &mode.runs {
            writeln!(
                std::io::stdout(),
                "load memory: {} {} rows idle {} MiB peak {} MiB",
                mode.mode,
                run.rows,
                run.idle_rss_bytes >> 20,
                run.peak_rss_bytes >> 20,
            )?;
        }
    }
    writeln!(
        std::io::stdout(),
        "load memory: criterion 6 {}",
        serde_json::to_string(&criterion_6)?
    )?;
    let report = Report {
        scenario: "memory",
        machine: bench.machine.clone(),
        workload: format!("{:?}", workload_for(SMALL_ROWS)),
        max_inflight_bytes: MAX_INFLIGHT_BYTES,
        modes,
        criterion_6,
    };
    write_report(options, "memory", &report).await
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1024 * 1024;

    #[test]
    fn memory_passes_when_it_stays_flat_and_inside_the_allowance() {
        let bound = 64 * MIB;
        let verdict = |small_peak, large_peak| {
            memory_verdict(
                (20 * MIB, small_peak * MIB),
                (20 * MIB, large_peak * MIB),
                bound,
            )
        };
        assert_eq!(verdict(100, 110), Verdict::Pass);
        assert_eq!(verdict(100, 111), Verdict::Fail);
        assert_eq!(verdict(100, 90), Verdict::Pass);
        assert_eq!(verdict(148, 148), Verdict::Pass);
        assert_eq!(verdict(140, 149), Verdict::Fail);
        assert_eq!(verdict(149, 149), Verdict::Fail);
    }

    #[test]
    fn each_run_is_held_to_its_own_idle_footprint() {
        let bound = 64 * MIB;
        assert_eq!(
            memory_verdict((10 * MIB, 100 * MIB), (30 * MIB, 105 * MIB), bound),
            Verdict::Pass
        );
        assert_eq!(
            memory_verdict((10 * MIB, 100 * MIB), (10 * MIB, 105 * MIB), bound),
            Verdict::Pass
        );
        assert_eq!(
            memory_verdict((10 * MIB, 139 * MIB), (10 * MIB, 139 * MIB), bound),
            Verdict::Fail
        );
    }
}
