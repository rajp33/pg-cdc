use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context as _;
use pg_cdc_testkit::{CREATED_LABEL, parse_lsn};
use serde::Serialize;
use tokio::process::Command;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::overhead::overhead_percent;
use super::rig::{self, Bench, ConfigSpec, SinkSpec, Source, StoreSpec};
use super::{LoadOptions, Verdict, write_report};
use crate::measure::{self, Machine};
use crate::workload;

const SCENARIO: &str = "overhead-instructions";
const IMAGE: &str = "pg-cdc-cachegrind:local";
const DOCKERFILE: &str = "crates/loadgen/cachegrind.Dockerfile";
const BACKLOG_EVENTS: u64 = 50_000;
const ROUNDS: usize = 5;
const TRIM_FROM: usize = 5;
const OVERHEAD_LIMIT_PERCENT: f64 = 1.0;
const POSTGRES_PORT: u16 = 5432;
const CONFIG_PATH: &str = "/tmp/pg-cdc.yaml";
const STATE_PATH: &str = "/tmp/pg-cdc-state";
const OUT_PATH: &str = "/tmp/cachegrind.out";
const VALGRIND: [&str; 3] = ["valgrind", "--tool=cachegrind", "--cache-sim=no"];
const ENVIRONMENT: [&str; 3] = [
    "TOKIO_WORKER_THREADS=1",
    "LD_PRELOAD=/usr/local/lib/libjemalloc.so.2",
    "MALLOC_CONF=dirty_decay_ms:-1,muzzy_decay_ms:-1",
];
const POLL: Duration = Duration::from_secs(1);
const STARTUP_SETTLE: Duration = Duration::from_secs(2);
const RUN_LIMIT: Duration = Duration::from_secs(300);
const EXIT_LIMIT: Duration = Duration::from_secs(120);
const LOG_TAIL: &str = "40";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
enum Build {
    #[serde(rename = "default")]
    Default,
    #[serde(rename = "baseline")]
    Baseline,
}

impl Build {
    fn binary(self) -> &'static str {
        match self {
            Self::Default => "/usr/local/bin/pg-cdc-default",
            Self::Baseline => "/usr/local/bin/pg-cdc-baseline",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
enum Kind {
    #[serde(rename = "startup")]
    Startup,
    #[serde(rename = "backlog")]
    Backlog,
}

#[derive(Debug, Clone, Serialize)]
struct RunRecord {
    build: Build,
    kind: Kind,
    slot: String,
    instructions: u64,
    wall_s: f64,
}

#[derive(Debug, Clone, Serialize)]
struct Stats {
    startup_runs: usize,
    backlog_runs: usize,
    median_startup_instructions: Option<f64>,
    startup_spread: Option<f64>,
    median_instructions_per_event: Option<f64>,
    spread: Option<f64>,
    middle_spread: Option<f64>,
    median_gross_instructions_per_event: Option<f64>,
    gross_spread: Option<f64>,
}

#[derive(Debug, Serialize)]
struct Report {
    scenario: &'static str,
    machine: Machine,
    image: &'static str,
    image_id: String,
    source_commit: String,
    source_dirty: bool,
    valgrind: String,
    container_kernel: String,
    command: String,
    workload: String,
    config: String,
    backlog_events: u64,
    runs: Vec<RunRecord>,
    default: Stats,
    baseline: Stats,
    environment: [&'static str; 3],
    overhead_percent: Option<f64>,
    gross_overhead_percent: Option<f64>,
    startup_overhead_percent: Option<f64>,
    noise_percent: Option<f64>,
    noise_rule_verdict: Verdict,
}

fn total_instructions(cachegrind_out: &str) -> Option<u64> {
    let fields = |prefix: &str| {
        cachegrind_out
            .lines()
            .find_map(|line| line.strip_prefix(prefix))
            .map(|rest| rest.split_whitespace().collect::<Vec<_>>())
    };
    let position = fields("events:")?.iter().position(|event| *event == "Ir")?;
    fields("summary:")?.get(position)?.parse().ok()
}

fn exact_f64(count: u64) -> f64 {
    let high = u32::try_from(count >> 32).unwrap_or(u32::MAX);
    let low = u32::try_from(count & u64::from(u32::MAX)).unwrap_or(u32::MAX);
    f64::from(high) * f64::from(u32::MAX) + f64::from(high) + f64::from(low)
}

fn stats(runs: &[RunRecord], build: Build, events: u64) -> Stats {
    let of_kind = |kind: Kind| -> Vec<f64> {
        runs.iter()
            .filter(|run| run.build == build && run.kind == kind)
            .map(|run| exact_f64(run.instructions))
            .collect()
    };
    let mut startup = of_kind(Kind::Startup);
    let backlog = of_kind(Kind::Backlog);
    let startup_spread = measure::spread(&startup);
    let median_startup = measure::median(&mut startup);
    let per_event = |fixed: f64| -> Vec<f64> {
        backlog
            .iter()
            .map(|total| (total - fixed) / exact_f64(events))
            .collect()
    };
    let mut gross = per_event(0.0);
    let mut net = median_startup.map(per_event).unwrap_or_default();
    Stats {
        startup_runs: startup.len(),
        backlog_runs: backlog.len(),
        median_startup_instructions: median_startup,
        startup_spread,
        spread: measure::spread(&net),
        middle_spread: middle_spread(&net),
        gross_spread: measure::spread(&gross),
        median_instructions_per_event: measure::median(&mut net),
        median_gross_instructions_per_event: measure::median(&mut gross),
    }
}

fn middle_spread(values: &[f64]) -> Option<f64> {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let middle = match sorted.as_slice() {
        [_, middle @ .., _] if sorted.len() >= TRIM_FROM => middle,
        all => all,
    };
    measure::spread(middle)
}

fn noise_percent(spreads: [Option<f64>; 2]) -> Option<f64> {
    spreads.into_iter().try_fold(0.0_f64, |widest, spread| {
        Some(widest.max((spread? - 1.0) * 100.0))
    })
}

fn noise_rule(overhead_percent: Option<f64>, noise_percent: Option<f64>) -> Verdict {
    match overhead_percent.zip(noise_percent) {
        Some((overhead, noise)) if overhead + noise < OVERHEAD_LIMIT_PERCENT => Verdict::Pass,
        Some((overhead, noise)) if overhead - noise >= OVERHEAD_LIMIT_PERCENT => Verdict::Fail,
        _ => Verdict::NotResolved,
    }
}

fn round_order(round: usize) -> [Build; 2] {
    if round.is_multiple_of(2) {
        [Build::Default, Build::Baseline]
    } else {
        [Build::Baseline, Build::Default]
    }
}

fn compare(default: Option<f64>, baseline: Option<f64>) -> Option<f64> {
    default
        .zip(baseline)
        .and_then(|(default, baseline)| overhead_percent(default, baseline))
}

async fn docker(args: &[&str]) -> anyhow::Result<String> {
    let output = Command::new("docker")
        .args(args)
        .output()
        .await
        .context("cannot run docker")?;
    anyhow::ensure!(
        output.status.success(),
        "docker {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr).trim()
    );
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

async fn build_image() -> anyhow::Result<String> {
    let root = workspace_root();
    let scratch = tempfile::tempdir()?;
    let iidfile = scratch.path().join("image-id");
    let status = Command::new("docker")
        .arg("build")
        .arg("--file")
        .arg(root.join(DOCKERFILE))
        .args(["--tag", IMAGE])
        .arg("--iidfile")
        .arg(&iidfile)
        .arg(&root)
        .status()
        .await
        .context("cannot run docker build")?;
    anyhow::ensure!(status.success(), "docker build of {IMAGE} failed");
    let id = tokio::fs::read_to_string(&iidfile).await?.trim().to_owned();
    anyhow::ensure!(!id.is_empty(), "docker build wrote no image ID");
    Ok(id)
}

async fn git(args: &[&str]) -> anyhow::Result<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(workspace_root())
        .args(args)
        .output()
        .await
        .context("cannot run git")?;
    anyhow::ensure!(output.status.success(), "git {args:?} failed");
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

#[derive(Debug)]
struct Container {
    id: String,
}

impl Drop for Container {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["rm", "--force", &self.id])
            .output();
    }
}

impl Container {
    async fn create(bench: &Bench, image: &str, build: Build) -> anyhow::Result<Self> {
        let created = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| since.as_secs());
        let network = format!("container:{}", bench.postgres.container_id());
        let label = format!("{CREATED_LABEL}={created}");
        let password = format!(
            "PGCDC__SOURCE__CREDENTIALS__PASSWORD={}",
            bench.postgres.password()
        );
        let mut args = vec![
            "create",
            "--init",
            "--network",
            &network,
            "--label",
            &label,
            "--env",
            &password,
        ];
        for variable in ENVIRONMENT {
            args.extend(["--env", variable]);
        }
        let out_file = format!("--cachegrind-out-file={OUT_PATH}");
        args.push(image);
        args.extend(VALGRIND);
        args.push(&out_file);
        args.extend([build.binary(), "run", "--config", CONFIG_PATH]);
        let id = docker(&args).await?;
        Ok(Self { id })
    }

    async fn running(&self) -> anyhow::Result<bool> {
        Ok(docker(&["inspect", "--format", "{{.State.Running}}", &self.id]).await? == "true")
    }

    async fn failure(&self, problem: &str) -> anyhow::Error {
        let logs = Command::new("docker")
            .args(["logs", "--tail", LOG_TAIL, &self.id])
            .output()
            .await
            .map(|output| {
                format!(
                    "{}{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                )
            })
            .unwrap_or_default();
        anyhow::anyhow!("{problem}; last log lines:\n{logs}")
    }

    async fn stop(&self) -> anyhow::Result<()> {
        docker(&["kill", "--signal", "TERM", &self.id]).await?;
        let exit = match tokio::time::timeout(EXIT_LIMIT, docker(&["wait", &self.id])).await {
            Ok(exit) => exit?,
            Err(_) => {
                return Err(self
                    .failure(&format!(
                        "pg-cdc did not exit within {EXIT_LIMIT:?} of SIGTERM"
                    ))
                    .await);
            }
        };
        if exit != "0" {
            return Err(self
                .failure(&format!("pg-cdc exited with {exit} after SIGTERM"))
                .await);
        }
        Ok(())
    }
}

fn source(bench: &Bench) -> Source {
    Source {
        host: "127.0.0.1".to_owned(),
        port: POSTGRES_PORT,
        database: bench.postgres.database().to_owned(),
        user: bench.postgres.user().to_owned(),
    }
}

async fn wait(
    bench: &Bench,
    container: &Container,
    slot: &str,
    kind: Kind,
    target: u64,
) -> anyhow::Result<()> {
    let started = Instant::now();
    loop {
        if !container.running().await? {
            return Err(container.failure("pg-cdc exited early").await);
        }
        let row = bench
            .client
            .query_one(
                "SELECT active, confirmed_flush_lsn::text FROM pg_replication_slots \
                 WHERE slot_name = $1",
                &[&slot],
            )
            .await?;
        let done = match kind {
            Kind::Backlog => row
                .try_get::<_, Option<&str>>(1)?
                .map(parse_lsn)
                .transpose()?
                .is_some_and(|lsn| lsn >= target),
            Kind::Startup => row.try_get(0)?,
        };
        if done {
            if kind == Kind::Startup {
                tokio::time::sleep(STARTUP_SETTLE).await;
            }
            return Ok(());
        }
        if started.elapsed() > RUN_LIMIT {
            return Err(container
                .failure(&format!("slot {slot} not done within {RUN_LIMIT:?}"))
                .await);
        }
        tokio::time::sleep(POLL).await;
    }
}

async fn one_run(
    bench: &Bench,
    image: &str,
    build: Build,
    kind: Kind,
    slot: &str,
    target: u64,
) -> anyhow::Result<(RunRecord, String)> {
    let name = format!("{build:?}-{kind:?}-{slot}").to_lowercase();
    let spec = ConfigSpec {
        slot: slot.to_owned(),
        sink: SinkSpec::Discard,
        store: StoreSpec::Filesystem(PathBuf::from(STATE_PATH)),
        max_inflight_bytes: None,
        park_after: None,
        otlp_endpoint: None,
    };
    let text = rig::render_config(&source(bench), &spec);
    let dir = bench.scratch(&name).await?;
    let config = dir.join("config.yaml");
    tokio::fs::write(&config, &text).await?;
    let container = Container::create(bench, image, build).await?;
    let config = config.to_string_lossy();
    docker(&["cp", &config, &format!("{}:{CONFIG_PATH}", container.id)]).await?;
    let started = Instant::now();
    docker(&["start", &container.id]).await?;
    wait(bench, &container, slot, kind, target).await?;
    let wall = started.elapsed();
    container.stop().await?;
    let out = dir.join("cachegrind.out");
    docker(&[
        "cp",
        &format!("{}:{OUT_PATH}", container.id),
        &out.to_string_lossy(),
    ])
    .await?;
    let summary = tokio::fs::read_to_string(&out).await?;
    let instructions = total_instructions(&summary)
        .with_context(|| format!("no Ir summary in {}", out.display()))?;
    writeln!(
        std::io::stdout(),
        "load {SCENARIO}: {name}: {instructions} instructions in {:.1} s",
        wall.as_secs_f64()
    )?;
    let record = RunRecord {
        build,
        kind,
        slot: slot.to_owned(),
        instructions,
        wall_s: wall.as_secs_f64(),
    };
    Ok((record, text))
}

pub(super) async fn run(options: &LoadOptions) -> anyhow::Result<()> {
    let image_id = build_image().await?;
    let source_commit = git(&["rev-parse", "HEAD"]).await?;
    let source_dirty = !git(&["status", "--porcelain", "--untracked-files=no"])
        .await?
        .is_empty();
    let valgrind = docker(&["run", "--rm", &image_id, "valgrind", "--version"]).await?;
    let container_kernel = docker(&["run", "--rm", &image_id, "uname", "-srm"]).await?;
    let per_round = round_order(0).len();
    let slot = |kind: &str, index: usize| format!("instructions_{kind}_{index}");
    let backlog_slots: Vec<String> = (1..=ROUNDS * per_round)
        .map(|index| slot("backlog", index))
        .collect();
    let slot_names: Vec<&str> = backlog_slots.iter().map(String::as_str).collect();
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
    let mut config = String::new();
    for round in 0..ROUNDS {
        for (offset, build) in round_order(round).into_iter().enumerate() {
            let index = round * per_round + offset + 1;
            let startup = slot("startup", index);
            bench.postgres.create_slot(&startup).await?;
            let (record, _) =
                one_run(&bench, &image_id, build, Kind::Startup, &startup, target).await?;
            runs.push(record);
            bench
                .client
                .execute("SELECT pg_drop_replication_slot($1)", &[&startup])
                .await?;
            let backlog = slot("backlog", index);
            let (record, text) =
                one_run(&bench, &image_id, build, Kind::Backlog, &backlog, target).await?;
            runs.push(record);
            config = text;
        }
    }
    let default = stats(&runs, Build::Default, BACKLOG_EVENTS);
    let baseline = stats(&runs, Build::Baseline, BACKLOG_EVENTS);
    let overhead = compare(
        default.median_instructions_per_event,
        baseline.median_instructions_per_event,
    );
    let noise = noise_percent([default.middle_spread, baseline.middle_spread]);
    let noise_rule_verdict = noise_rule(overhead, noise);
    writeln!(
        std::io::stdout(),
        "load {SCENARIO}: {} runs, overhead {overhead:?} %, noise {noise:?} %, noise rule {}",
        runs.len(),
        serde_json::to_string(&noise_rule_verdict)?,
    )?;
    let report = Report {
        scenario: SCENARIO,
        machine: bench.machine.clone(),
        image: IMAGE,
        image_id,
        source_commit,
        source_dirty,
        valgrind,
        container_kernel,
        command: format!(
            "{} --cachegrind-out-file={OUT_PATH} <pg-cdc> run --config {CONFIG_PATH}",
            VALGRIND.join(" ")
        ),
        workload: format!("{:?}", rig::workload(0)),
        config,
        backlog_events: BACKLOG_EVENTS,
        environment: ENVIRONMENT,
        noise_percent: noise,
        gross_overhead_percent: compare(
            default.median_gross_instructions_per_event,
            baseline.median_gross_instructions_per_event,
        ),
        startup_overhead_percent: compare(
            default.median_startup_instructions,
            baseline.median_startup_instructions,
        ),
        overhead_percent: overhead,
        runs,
        default,
        baseline,
        noise_rule_verdict,
    };
    write_report(options, SCENARIO, &report).await
}

#[cfg(test)]
mod tests {
    use super::*;

    const CACHEGRIND_OUT: &str = "\
# callgrind format
version: 1
creator: valgrind-3.24.0
pid: 7
cmd: /usr/local/bin/pg-cdc-default run --config /tmp/pg-cdc.yaml
part: 1

positions: line
events: Ir
fl=???
fn=0x0000000000108c40
0 5
summary: 1234567890
totals: 1234567890
";

    #[test]
    fn the_total_is_the_ir_column_of_the_summary_line() {
        assert_eq!(total_instructions(CACHEGRIND_OUT), Some(1_234_567_890));
        assert_eq!(
            total_instructions("events: Dr Ir Dw\nsummary: 1 22 333\n"),
            Some(22)
        );
        assert_eq!(total_instructions("events: Ir\n"), None);
        assert_eq!(total_instructions("summary: 5\n"), None);
        assert_eq!(total_instructions("events: Dr\nsummary: 5\n"), None);
        assert_eq!(total_instructions("events: Ir\nsummary: lots\n"), None);
        assert_eq!(total_instructions(""), None);
    }

    #[test]
    fn instruction_counts_convert_to_f64_exactly() {
        assert!((exact_f64(0) - 0.0).abs() < f64::EPSILON);
        assert!((exact_f64(1_234_567_890) - 1_234_567_890.0).abs() < f64::EPSILON);
        assert!((exact_f64(98_765_432_101) - 98_765_432_101.0).abs() < f64::EPSILON);
        assert!((exact_f64(1 << 40) - 1_099_511_627_776.0).abs() < f64::EPSILON);
    }

    fn record(build: Build, kind: Kind, instructions: u64) -> RunRecord {
        RunRecord {
            build,
            kind,
            slot: "instructions_backlog_1".to_owned(),
            instructions,
            wall_s: 1.0,
        }
    }

    #[test]
    fn per_event_cost_subtracts_the_median_startup_of_the_same_build() {
        let runs = [
            record(Build::Default, Kind::Startup, 1_000_000),
            record(Build::Default, Kind::Backlog, 11_000_000),
            record(Build::Baseline, Kind::Startup, 500_000),
            record(Build::Baseline, Kind::Backlog, 9_500_000),
            record(Build::Default, Kind::Startup, 1_200_000),
            record(Build::Default, Kind::Backlog, 12_100_000),
            record(Build::Default, Kind::Startup, 1_100_000),
            record(Build::Default, Kind::Backlog, 11_500_000),
        ];
        let default = stats(&runs, Build::Default, 1_000);
        assert_eq!(default.startup_runs, 3);
        assert_eq!(default.backlog_runs, 3);
        assert_eq!(default.median_startup_instructions, Some(1_100_000.0));
        assert_eq!(default.startup_spread, Some(1.2));
        assert_eq!(default.median_instructions_per_event, Some(10_400.0));
        assert_eq!(default.spread, Some(11_000.0 / 9_900.0));
        assert_eq!(default.middle_spread, Some(11_000.0 / 9_900.0));
        assert_eq!(default.median_gross_instructions_per_event, Some(11_500.0));
        assert_eq!(default.gross_spread, Some(1.1));
        let baseline = stats(&runs, Build::Baseline, 1_000);
        assert_eq!(baseline.median_instructions_per_event, Some(9_000.0));
        assert_eq!(baseline.spread, Some(1.0));
    }

    #[test]
    fn a_build_without_startup_runs_has_no_net_cost() {
        let runs = [record(Build::Default, Kind::Backlog, 5_000)];
        let default = stats(&runs, Build::Default, 10);
        assert_eq!(default.median_gross_instructions_per_event, Some(500.0));
        assert_eq!(default.median_instructions_per_event, None);
        assert_eq!(default.spread, None);
    }

    #[test]
    fn five_or_more_runs_drop_their_highest_and_lowest_before_the_spread() {
        assert_eq!(
            middle_spread(&[105.0, 100.0, 120.0, 101.0, 80.0]),
            Some(1.05)
        );
        assert_eq!(
            middle_spread(&[100.0, 90.0, 102.0, 101.0, 150.0, 99.0]),
            Some(102.0 / 99.0)
        );
        assert_eq!(middle_spread(&[100.0, 110.0, 120.0, 130.0]), Some(1.3));
        assert_eq!(middle_spread(&[100.0]), Some(1.0));
        assert_eq!(middle_spread(&[]), None);
    }

    #[test]
    fn the_noise_is_the_wider_spread_of_the_two_builds_in_percent() {
        let noise = noise_percent([Some(1.002), Some(1.007)]).unwrap();
        assert!((noise - 0.7).abs() < 1e-9, "{noise}");
        assert_eq!(noise_percent([Some(1.0), Some(1.0)]), Some(0.0));
        assert_eq!(noise_percent([None, Some(1.001)]), None);
    }

    #[test]
    fn the_noise_rule_decides_only_when_the_noise_cannot_cross_one_percent() {
        assert_eq!(noise_rule(Some(0.2), Some(0.3)), Verdict::Pass);
        assert_eq!(noise_rule(Some(-0.5), Some(0.9)), Verdict::Pass);
        assert_eq!(noise_rule(Some(0.8), Some(0.3)), Verdict::NotResolved);
        assert_eq!(noise_rule(Some(0.9), Some(0.05)), Verdict::Pass);
        assert_eq!(noise_rule(Some(1.2), Some(0.3)), Verdict::NotResolved);
        assert_eq!(noise_rule(Some(1.4), Some(0.3)), Verdict::Fail);
        assert_eq!(noise_rule(Some(1.0), Some(0.0)), Verdict::Fail);
        assert_eq!(noise_rule(None, Some(0.1)), Verdict::NotResolved);
        assert_eq!(noise_rule(Some(0.1), None), Verdict::NotResolved);
    }

    #[test]
    fn rounds_alternate_which_build_runs_first() {
        assert_eq!(round_order(0), [Build::Default, Build::Baseline]);
        assert_eq!(round_order(1), [Build::Baseline, Build::Default]);
        assert_eq!(round_order(4), [Build::Default, Build::Baseline]);
    }

    #[test]
    fn the_overhead_compares_medians_of_both_builds() {
        let overhead = compare(Some(10_100.0), Some(10_000.0)).unwrap();
        assert!((overhead - 1.0).abs() < 1e-9, "{overhead}");
        assert_eq!(compare(None, Some(1.0)), None);
        assert_eq!(compare(Some(1.0), Some(0.0)), None);
    }
}
