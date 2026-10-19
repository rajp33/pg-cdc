use std::io::Write as _;
use std::path::PathBuf;

use anyhow::Context as _;
use serde::Serialize;

mod ceiling;
mod handover;
mod memory;
mod outage;
mod overhead;
mod rig;
mod sns;

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Scenario {
    Ceiling,
    Sns,
    Overhead,
    Memory,
    Outage,
    Brownout,
    Handover,
    All,
}

#[derive(Debug, Clone)]
pub struct LoadOptions {
    pub binary: PathBuf,
    pub baseline: Option<PathBuf>,
    pub report_dir: PathBuf,
    pub topic_arn: Option<String>,
    pub real_aws: bool,
    pub drain_limit: std::time::Duration,
}

const EVERY_SCENARIO: [Scenario; 7] = [
    Scenario::Ceiling,
    Scenario::Sns,
    Scenario::Overhead,
    Scenario::Memory,
    Scenario::Outage,
    Scenario::Brownout,
    Scenario::Handover,
];

pub async fn run(scenario: Scenario, options: &LoadOptions) -> anyhow::Result<()> {
    match scenario {
        Scenario::Ceiling => Box::pin(ceiling::run(options)).await,
        Scenario::Sns => Box::pin(sns::run(options)).await,
        Scenario::Overhead => Box::pin(overhead::run(options)).await,
        Scenario::Memory => Box::pin(memory::run(options)).await,
        Scenario::Outage => Box::pin(outage::run_outage(options)).await,
        Scenario::Brownout => Box::pin(outage::run_brownout(options)).await,
        Scenario::Handover => Box::pin(handover::run(options)).await,
        Scenario::All => {
            for each in EVERY_SCENARIO {
                Box::pin(run(each, options)).await?;
            }
            Ok(())
        }
    }
}

async fn write_report(
    options: &LoadOptions,
    scenario: &str,
    report: &impl Serialize,
) -> anyhow::Result<()> {
    tokio::fs::create_dir_all(&options.report_dir).await?;
    let path = options.report_dir.join(format!("{scenario}.json"));
    tokio::fs::write(&path, serde_json::to_vec_pretty(report)?)
        .await
        .with_context(|| format!("cannot write {}", path.display()))?;
    writeln!(
        std::io::stdout(),
        "load {scenario}: report written to {}",
        path.display()
    )?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Verdict {
    #[serde(rename = "pass")]
    Pass,
    #[serde(rename = "fail")]
    Fail,
    #[serde(rename = "unverified (emulator-bound)")]
    Unverified,
}

pub const CRITERION_FOUR_RATE: f64 = 5_000.0;
pub const CRITERION_FOUR_P99_SECONDS: f64 = 1.0;

pub fn criterion_four(
    delivered_rate: f64,
    kept_up: bool,
    p99_upper_bound: Option<f64>,
    moto_ceiling: Option<f64>,
) -> Verdict {
    if moto_ceiling.is_some_and(|ceiling| ceiling < CRITERION_FOUR_RATE) {
        return Verdict::Unverified;
    }
    let fast_enough = p99_upper_bound.is_some_and(|p99| p99 < CRITERION_FOUR_P99_SECONDS);
    if delivered_rate >= CRITERION_FOUR_RATE && kept_up && fast_enough {
        Verdict::Pass
    } else {
        Verdict::Fail
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn criterion_four_passes_only_from_a_fast_enough_sns_run() {
        assert_eq!(
            criterion_four(5_200.0, true, Some(0.75), None),
            Verdict::Pass
        );
        assert_eq!(
            criterion_four(4_900.0, true, Some(0.5), None),
            Verdict::Fail
        );
        assert_eq!(
            criterion_four(5_200.0, true, Some(1.0), None),
            Verdict::Fail
        );
        assert_eq!(
            criterion_four(5_200.0, false, Some(0.5), None),
            Verdict::Fail
        );
        assert_eq!(criterion_four(5_200.0, true, None, None), Verdict::Fail);
        assert_eq!(
            criterion_four(1_800.0, false, Some(2.5), Some(2_000.0)),
            Verdict::Unverified
        );
        assert_eq!(
            criterion_four(5_200.0, true, Some(0.5), Some(4_999.0)),
            Verdict::Unverified
        );
        assert_eq!(
            criterion_four(5_200.0, true, Some(0.5), Some(9_000.0)),
            Verdict::Pass
        );
    }

    #[test]
    fn verdicts_serialize_as_their_report_text() {
        assert_eq!(serde_json::to_string(&Verdict::Pass).unwrap(), "\"pass\"");
        assert_eq!(
            serde_json::to_string(&Verdict::Unverified).unwrap(),
            "\"unverified (emulator-bound)\""
        );
    }
}
