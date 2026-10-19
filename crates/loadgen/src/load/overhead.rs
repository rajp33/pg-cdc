use std::io::Write as _;
use std::path::Path;

use anyhow::Context as _;
use serde::Serialize;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::rig::{self, Bench, ConfigSpec, DRAIN_LIMIT, SinkSpec, StoreSpec};
use super::{LoadOptions, Verdict, write_report};
use crate::measure::{self, Machine};
use crate::workload;

const SLOTS: usize = 18;
const BACKLOG_EVENTS: u64 = 1_000_000;
const FIRST_ROUNDS: usize = 5;
const EXTRA_ROUNDS: usize = 4;
const SPREAD_LIMIT: f64 = 1.01;
const OVERHEAD_LIMIT_PERCENT: f64 = 1.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
enum Build {
    #[serde(rename = "default")]
    Default,
    #[serde(rename = "baseline")]
    Baseline,
}

#[derive(Debug, Clone, Serialize)]
struct RunRecord {
    build: Build,
    slot: String,
    wall_s: f64,
    cpu_s: f64,
    cpu_s_per_million_events: f64,
    events_per_s: f64,
}

#[derive(Debug, Clone, Serialize)]
struct Stats {
    runs: usize,
    median_cpu_s_per_million_events: Option<f64>,
    cpu_spread: Option<f64>,
    median_events_per_s: Option<f64>,
    events_spread: Option<f64>,
}

#[derive(Debug, Serialize)]
struct Report {
    scenario: &'static str,
    machine: Machine,
    workload: String,
    config: String,
    backlog_events: u64,
    runs: Vec<RunRecord>,
    default: Stats,
    baseline: Stats,
    extra_runs_taken: bool,
    overhead_percent: Option<f64>,
    events_per_s_difference_percent: Option<f64>,
    criterion_7: Verdict,
}

pub(super) fn overhead_percent(default_median: f64, baseline_median: f64) -> Option<f64> {
    (baseline_median > 0.0).then(|| (default_median / baseline_median - 1.0) * 100.0)
}

fn needs_more_runs(default_spread: Option<f64>, baseline_spread: Option<f64>) -> bool {
    [default_spread, baseline_spread]
        .into_iter()
        .flatten()
        .any(|spread| spread > SPREAD_LIMIT)
}

fn criterion_seven(overhead_percent: Option<f64>) -> Verdict {
    if overhead_percent.is_some_and(|percent| percent <= OVERHEAD_LIMIT_PERCENT) {
        Verdict::Pass
    } else {
        Verdict::Fail
    }
}

fn stats(runs: &[RunRecord], build: Build) -> Stats {
    let mut cpu: Vec<f64> = Vec::new();
    let mut rate: Vec<f64> = Vec::new();
    for run in runs.iter().filter(|run| run.build == build) {
        cpu.push(run.cpu_s_per_million_events);
        rate.push(run.events_per_s);
    }
    Stats {
        runs: cpu.len(),
        cpu_spread: measure::spread(&cpu),
        events_spread: measure::spread(&rate),
        median_cpu_s_per_million_events: measure::median(&mut cpu),
        median_events_per_s: measure::median(&mut rate),
    }
}

async fn one_run(
    bench: &Bench,
    binary: &Path,
    build: Build,
    index: usize,
    target: u64,
) -> anyhow::Result<(RunRecord, String)> {
    let slot = format!("overhead_{index}");
    let name = format!("{build:?}-{index}").to_lowercase();
    let spec = ConfigSpec {
        slot: slot.clone(),
        sink: SinkSpec::Discard,
        store: StoreSpec::Filesystem(bench.scratch(&format!("{name}-store")).await?),
        max_inflight_bytes: None,
        park_after: None,
        otlp_endpoint: None,
    };
    let started = Instant::now();
    let (mut process, config) = bench.spawn(binary, &spec, &[], &name).await?;
    let drained = rig::drain(&process, target, started, DRAIN_LIMIT).await?;
    rig::stop(&mut process).await?;
    let millions = rig::count_f64(BACKLOG_EVENTS) / 1_000_000.0;
    let record = RunRecord {
        build,
        slot,
        wall_s: drained.wall.as_secs_f64(),
        cpu_s: drained.cpu.as_secs_f64(),
        cpu_s_per_million_events: drained.cpu.as_secs_f64() / millions,
        events_per_s: rig::per_second(BACKLOG_EVENTS, drained.wall),
    };
    Ok((record, config))
}

async fn rounds(
    bench: &Bench,
    options: &LoadOptions,
    baseline: &Path,
    count: usize,
    target: u64,
    runs: &mut Vec<RunRecord>,
) -> anyhow::Result<String> {
    let mut config = String::new();
    for _ in 0..count {
        for (build, binary) in [
            (Build::Default, options.binary.as_path()),
            (Build::Baseline, baseline),
        ] {
            let index = runs.len() + 1;
            let (record, text) = one_run(bench, binary, build, index, target).await?;
            runs.push(record);
            config = text;
        }
    }
    Ok(config)
}

pub(super) async fn run(options: &LoadOptions) -> anyhow::Result<()> {
    let baseline = options
        .baseline
        .as_deref()
        .context("the overhead scenario needs --baseline, a build with --no-default-features")?;
    let slots: Vec<String> = (1..=SLOTS)
        .map(|index| format!("overhead_{index}"))
        .collect();
    let slot_names: Vec<&str> = slots.iter().map(String::as_str).collect();
    let bench = Bench::start(&slot_names).await?;
    let ledger = workload::write_events(
        &bench.client,
        &rig::workload(0),
        CancellationToken::new(),
        Some(BACKLOG_EVENTS),
    )
    .await?;
    let target = rig::last_lsn(&ledger)?;
    let mut runs = Vec::new();
    let mut config = rounds(&bench, options, baseline, FIRST_ROUNDS, target, &mut runs).await?;
    let mut extra_runs_taken = false;
    if needs_more_runs(
        stats(&runs, Build::Default).cpu_spread,
        stats(&runs, Build::Baseline).cpu_spread,
    ) {
        extra_runs_taken = true;
        config = rounds(&bench, options, baseline, EXTRA_ROUNDS, target, &mut runs).await?;
    }
    let default = stats(&runs, Build::Default);
    let base = stats(&runs, Build::Baseline);
    let overhead = default
        .median_cpu_s_per_million_events
        .zip(base.median_cpu_s_per_million_events)
        .and_then(|(default, base)| overhead_percent(default, base));
    let throughput = default
        .median_events_per_s
        .zip(base.median_events_per_s)
        .and_then(|(default, base)| overhead_percent(base, default));
    let criterion_7 = criterion_seven(overhead);
    writeln!(
        std::io::stdout(),
        "load overhead: {} runs, overhead {overhead:?} %, criterion 7 {}",
        runs.len(),
        serde_json::to_string(&criterion_7)?,
    )?;
    let report = Report {
        scenario: "overhead",
        machine: bench.machine.clone(),
        workload: format!("{:?}", rig::workload(0)),
        config,
        backlog_events: BACKLOG_EVENTS,
        runs,
        default,
        baseline: base,
        extra_runs_taken,
        overhead_percent: overhead,
        events_per_s_difference_percent: throughput,
        criterion_7,
    };
    write_report(options, "overhead", &report).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overhead_is_the_default_builds_extra_cost_in_percent() {
        assert!(overhead_percent(10.0, 10.0).is_some_and(|value| value.abs() < 1e-9));
        assert!(overhead_percent(10.5, 10.0).is_some_and(|value| (value - 5.0).abs() < 1e-9));
        assert!(overhead_percent(9.9, 10.0).is_some_and(|value| (value + 1.0).abs() < 1e-9));
        assert_eq!(overhead_percent(10.0, 0.0), None);
    }

    #[test]
    fn a_spread_above_one_percent_in_either_build_asks_for_more_runs() {
        assert!(!needs_more_runs(Some(1.01), Some(1.0)));
        assert!(needs_more_runs(Some(1.011), Some(1.0)));
        assert!(needs_more_runs(Some(1.0), Some(1.2)));
        assert!(!needs_more_runs(None, None));
    }

    #[test]
    fn criterion_seven_passes_at_one_percent_or_less() {
        assert_eq!(criterion_seven(Some(0.4)), Verdict::Pass);
        assert_eq!(criterion_seven(Some(-2.0)), Verdict::Pass);
        assert_eq!(criterion_seven(Some(1.0)), Verdict::Pass);
        assert_eq!(criterion_seven(Some(1.01)), Verdict::Fail);
        assert_eq!(criterion_seven(None), Verdict::Fail);
    }

    fn record(build: Build, cpu: f64, rate: f64) -> RunRecord {
        RunRecord {
            build,
            slot: "overhead_1".to_owned(),
            wall_s: 1.0,
            cpu_s: cpu,
            cpu_s_per_million_events: cpu,
            events_per_s: rate,
        }
    }

    #[test]
    fn stats_group_runs_by_build() {
        let runs = [
            record(Build::Default, 10.0, 100.0),
            record(Build::Baseline, 20.0, 200.0),
            record(Build::Default, 12.0, 120.0),
            record(Build::Default, 11.0, 110.0),
        ];
        let default = stats(&runs, Build::Default);
        assert_eq!(default.runs, 3);
        assert_eq!(default.median_cpu_s_per_million_events, Some(11.0));
        assert_eq!(default.cpu_spread, Some(1.2));
        assert_eq!(default.median_events_per_s, Some(110.0));
        let baseline = stats(&runs, Build::Baseline);
        assert_eq!(baseline.runs, 1);
        assert_eq!(baseline.cpu_spread, Some(1.0));
        assert_eq!(stats(&[], Build::Default).median_events_per_s, None);
    }
}
