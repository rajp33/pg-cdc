use std::io::Write as _;
use std::ops::RangeInclusive;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use anyhow::Context as _;
use clap::{Args, Parser, Subcommand};
use pg_cdc_loadgen::chaos::{self, ChaosOptions};
use pg_cdc_loadgen::load::{self, LoadOptions, Scenario};
use pg_cdc_loadgen::workload::{self, WorkloadConfig};
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Parser)]
#[command(
    name = "loadgen",
    about = "Workload, chaos and load tooling for pg-cdc"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Write(WriteArgs),
    Chaos(ChaosArgs),
    Load(LoadArgs),
}

#[derive(Debug, Args)]
struct LoadArgs {
    #[arg(value_enum)]
    scenario: Scenario,
    #[arg(long = "pg-cdc")]
    binary: PathBuf,
    #[arg(long)]
    baseline: Option<PathBuf>,
    #[arg(long, default_value = "target/load")]
    report_dir: PathBuf,
    #[arg(long, requires = "real_aws")]
    topic_arn: Option<String>,
    #[arg(long, requires = "topic_arn")]
    real_aws: bool,
}

#[derive(Debug, Args)]
struct ChaosArgs {
    #[arg(long = "pg-cdc")]
    binary: PathBuf,
    #[arg(long, default_value_t = 1)]
    seed: u64,
    #[arg(long, default_value = "15s", value_parser = parse_duration)]
    duration: Duration,
    #[arg(long, default_value = "30s", value_parser = parse_duration)]
    settle: Duration,
    #[arg(long)]
    report: PathBuf,
}

#[derive(Debug, Args)]
struct WriteArgs {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    #[arg(long, default_value_t = 5432)]
    port: u16,
    #[arg(long)]
    database: String,
    #[arg(long)]
    user: String,
    #[arg(long, default_value = "loadgen")]
    publication: String,
    #[arg(long)]
    slot: Vec<String>,
    #[arg(long, default_value_t = 1_000)]
    rate: u32,
    #[arg(long, default_value = "1..10", value_parser = parse_tx_size)]
    tx_size: RangeInclusive<u32>,
    #[arg(long, default_value_t = 200)]
    row_bytes: usize,
    #[arg(long, default_value_t = 0)]
    toast_every: u32,
    #[arg(long, default_value_t = 0)]
    oversize_every: u32,
    #[arg(long, default_value_t = 1_000)]
    keys: u32,
    #[arg(long, default_value_t = 0.0)]
    skew: f64,
    #[arg(long, default_value = "10s", value_parser = parse_duration)]
    duration: Duration,
    #[arg(long, default_value_t = 1)]
    seed: u64,
}

fn parse_tx_size(text: &str) -> Result<RangeInclusive<u32>, String> {
    let (low, high) = text
        .split_once("..")
        .ok_or_else(|| format!("expected <min>..<max>, got {text:?}"))?;
    let low: u32 = low
        .parse()
        .map_err(|_| format!("bad minimum in {text:?}"))?;
    let high: u32 = high
        .parse()
        .map_err(|_| format!("bad maximum in {text:?}"))?;
    if low == 0 || low > high {
        return Err(format!("{text:?} must satisfy 1 <= min <= max"));
    }
    Ok(low..=high)
}

fn parse_duration(text: &str) -> Result<Duration, String> {
    let (digits, unit) = text
        .find(|c: char| !c.is_ascii_digit())
        .map_or((text, "s"), |split| text.split_at(split));
    let value: u64 = digits
        .parse()
        .map_err(|_| format!("expected a duration like 90s, 10m or 2h, got {text:?}"))?;
    let seconds = match unit {
        "s" => 1,
        "m" => 60,
        "h" => 3_600,
        _ => return Err(format!("unknown duration unit in {text:?}")),
    };
    Ok(Duration::from_secs(value * seconds))
}

#[tokio::main]
async fn main() -> anyhow::Result<ExitCode> {
    match Cli::parse().command {
        Command::Write(args) => write(args).await.map(|()| ExitCode::SUCCESS),
        Command::Load(args) => {
            load::run(
                args.scenario,
                &LoadOptions {
                    binary: args.binary,
                    baseline: args.baseline,
                    report_dir: args.report_dir,
                    topic_arn: args.topic_arn,
                    real_aws: args.real_aws,
                },
            )
            .await?;
            Ok(ExitCode::SUCCESS)
        }
        Command::Chaos(args) => {
            let report = Box::pin(chaos::run(&ChaosOptions {
                binary: args.binary,
                seed: args.seed,
                duration: args.duration,
                settle: args.settle,
                report: args.report,
            }))
            .await?;
            Ok(if report.verdict.passed() {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            })
        }
    }
}

async fn write(args: WriteArgs) -> anyhow::Result<()> {
    let mut config = tokio_postgres::Config::new();
    config
        .host(&args.host)
        .port(args.port)
        .dbname(&args.database)
        .user(&args.user);
    if let Ok(password) = std::env::var("PGPASSWORD") {
        config.password(password);
    }
    let (client, connection) = config
        .connect(NoTls)
        .await
        .with_context(|| format!("cannot connect to {}:{}", args.host, args.port))?;
    tokio::spawn(async move {
        let _ = connection.await;
    });

    if !args.slot.is_empty() {
        let slots: Vec<&str> = args.slot.iter().map(String::as_str).collect();
        workload::setup(&client, &args.publication, &slots).await?;
    }

    let stop = CancellationToken::new();
    let timer = stop.clone();
    let duration = args.duration;
    tokio::spawn(async move {
        tokio::time::sleep(duration).await;
        timer.cancel();
    });
    let ledger = workload::write(
        &client,
        &WorkloadConfig {
            rate: args.rate,
            tx_size: args.tx_size,
            row_bytes: args.row_bytes,
            toast_every: args.toast_every,
            oversize_every: args.oversize_every,
            keys: args.keys,
            skew: args.skew,
            seed: args.seed,
        },
        stop,
    )
    .await?;

    let summary = serde_json::json!({
        "committed": ledger.committed.len(),
        "uncertain": ledger.uncertain.len(),
        "transactions": ledger.transactions,
        "oversize": ledger.oversize,
        "last_lsn": ledger.last_lsn,
    });
    writeln!(std::io::stdout(), "{summary}")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transaction_sizes_are_a_min_and_max_pair() {
        assert_eq!(parse_tx_size("1..10"), Ok(1..=10));
        assert_eq!(parse_tx_size("5..5"), Ok(5..=5));
        assert!(parse_tx_size("10").is_err());
        assert!(parse_tx_size("0..3").is_err());
        assert!(parse_tx_size("4..3").is_err());
        assert!(parse_tx_size("a..3").is_err());
    }

    #[test]
    fn durations_default_to_seconds_and_accept_minutes_and_hours() {
        assert_eq!(parse_duration("30"), Ok(Duration::from_secs(30)));
        assert_eq!(parse_duration("90s"), Ok(Duration::from_secs(90)));
        assert_eq!(parse_duration("10m"), Ok(Duration::from_secs(600)));
        assert_eq!(parse_duration("2h"), Ok(Duration::from_secs(7_200)));
        assert!(parse_duration("fast").is_err());
        assert!(parse_duration("5d").is_err());
    }
}
