use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::io::Write as _;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use pg_cdc_testkit::{AwsEmulator, Postgres};
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout};

use super::evidence::{Observations, message_groups, read_stored};
use super::fleet::{Fleet, Seen, artifact_path};
use super::operator::Operator;
use super::{
    Action, ChaosOptions, ChaosReport, choose, missing_takeovers, outstanding, resolve_unresolved,
    settle_over,
};
use crate::checker::{Evidence, check};
use crate::proxy::{FaultProxy, Faults, Mode, Tap, Upstream};
use crate::workload::{self, Ledger, WorkloadConfig};

const PIPELINE: &str = "chaos";
const PUBLICATION: &str = "chaos_pub";
const SLOT: &str = "chaos_slot";
const BUCKET: &str = "chaos-state";
const STATE_PREFIX: &str = "chaos";
const TOPIC: &str = "chaos";
const TICK: Duration = Duration::from_millis(250);
const ACTION_PERIOD: Duration = Duration::from_secs(1);
const SKIPPED: &str = "Skipped";
const CHAOS_OPERATOR_BUDGET: Duration = Duration::from_secs(5);
const SETTLE_OPERATOR_BUDGET: Duration = Duration::from_secs(45);
const OPENING_TIMEOUT: Duration = Duration::from_secs(60);
const OPENING_RESTART: Duration = Duration::from_millis(500);

#[derive(Debug, Clone, Copy)]
enum Opening {
    Clean,
    Crash,
}

#[derive(Debug, Default)]
struct Windows {
    sns: Option<Instant>,
    s3: Option<Instant>,
}

#[derive(Debug)]
pub(super) struct Outcome {
    pub report: ChaosReport,
    pub problems: Vec<String>,
}

#[derive(Debug)]
pub(super) struct Harness {
    postgres: Postgres,
    _aws: AwsEmulator,
    s3: aws_sdk_s3::Client,
    sns_proxy: FaultProxy,
    s3_proxy: FaultProxy,
    fleet: Fleet,
    operator: Operator,
    observations: Arc<Observations>,
    schedule: fastrand::Rng,
    parameters: fastrand::Rng,
    actions: BTreeMap<String, u64>,
    windows: Windows,
}

fn config_text(postgres: &Postgres, topic_arn: &str) -> String {
    render_config(
        postgres.host(),
        postgres.port(),
        postgres.database(),
        postgres.user(),
        topic_arn,
    )
}

fn render_config(host: &str, port: u16, database: &str, user: &str, topic_arn: &str) -> String {
    format!(
        "pipeline: {PIPELINE}
source:
  host: {host}
  port: {port}
  database: {database}
  user: {user}
  credentials:
    type: password
  tls:
    mode: disable
  slot: {SLOT}
  publication: {PUBLICATION}
  status_interval: 1s
delivery:
  order_by: key
  lanes: 8
  park_after: 2s
  max_inflight_bytes: 16MiB
sink:
  type: sns
  topic_arn: {topic_arn}
state_store:
  type: s3
  bucket: {BUCKET}
  prefix: {STATE_PREFIX}
leadership:
  lease_timeout: 4s
  fence_delay: 4500ms
  drain_timeout: 1s
admin:
  listen: 127.0.0.1:0
",
    )
}

fn parked_prefix() -> String {
    format!("{STATE_PREFIX}/{PIPELINE}/parked/")
}

pub(super) fn workload_config(seed: u64) -> WorkloadConfig {
    WorkloadConfig {
        rate: 300,
        tx_size: 1..=20,
        row_bytes: 200,
        toast_every: 50,
        oversize_every: 400,
        keys: 50,
        skew: 1.0,
        seed,
    }
}

fn aws_error(error: impl std::error::Error) -> anyhow::Error {
    anyhow::anyhow!("{}", aws_sdk_s3::error::DisplayErrorContext(error))
}

impl Harness {
    pub(super) async fn start(options: &ChaosOptions) -> anyhow::Result<Self> {
        let postgres = Postgres::start().await?;
        let aws = AwsEmulator::start().await?;
        workload::setup(&postgres.client().await?, PUBLICATION, &[SLOT]).await?;
        let s3 = aws.s3().await;
        s3.create_bucket()
            .bucket(BUCKET)
            .send()
            .await
            .map_err(aws_error)?;
        let topic_arn = aws
            .sns()
            .await
            .create_topic()
            .name(format!("{TOPIC}.fifo"))
            .attributes("FifoTopic", "true")
            .send()
            .await
            .map_err(aws_error)?
            .topic_arn()
            .context("CreateTopic returned no ARN")?
            .to_owned();
        let upstream: SocketAddr = aws.endpoint().trim_start_matches("http://").parse()?;
        let observations = Arc::new(Observations::default());
        let tap = Tap::new({
            let observations = Arc::clone(&observations);
            move |request, response| {
                observations.record_publish(request, response);
            }
        })
        .ordered_by(message_groups);
        let sns_proxy = FaultProxy::start_with_tap(
            upstream,
            Upstream::Sns,
            options.seed.wrapping_add(1),
            Some(tap),
        )
        .await?;
        let s3_proxy =
            FaultProxy::start(upstream, Upstream::S3, options.seed.wrapping_add(2)).await?;

        let artifacts = options.report.with_extension("");
        let config = artifact_path(&artifacts, "config.yaml");
        if let Some(parent) = config.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&config, config_text(&postgres, &topic_arn)).await?;
        let env = vec![
            ("AWS_ENDPOINT_URL_SNS".to_owned(), sns_proxy.endpoint()),
            ("AWS_ENDPOINT_URL_S3".to_owned(), s3_proxy.endpoint()),
            ("AWS_ACCESS_KEY_ID".to_owned(), "test".to_owned()),
            ("AWS_SECRET_ACCESS_KEY".to_owned(), "test".to_owned()),
            ("AWS_REGION".to_owned(), "us-east-1".to_owned()),
            ("AWS_EC2_METADATA_DISABLED".to_owned(), "true".to_owned()),
            (
                "PGCDC__SOURCE__CREDENTIALS__PASSWORD".to_owned(),
                postgres.password().to_owned(),
            ),
        ];
        let mut fleet = Fleet::new(options.binary.clone(), config, env, artifacts);
        fleet.start().await?;
        Ok(Self {
            postgres,
            _aws: aws,
            s3,
            sns_proxy,
            s3_proxy,
            fleet,
            operator: Operator::new(options.binary.clone()),
            observations,
            schedule: fastrand::Rng::with_seed(options.seed),
            parameters: fastrand::Rng::with_seed(options.seed.wrapping_add(3)),
            actions: BTreeMap::new(),
            windows: Windows::default(),
        })
    }

    pub(super) async fn writer_client(&self) -> anyhow::Result<tokio_postgres::Client> {
        Ok(self.postgres.client().await?)
    }

    pub(super) async fn opening(
        &mut self,
        writer: &mut JoinHandle<anyhow::Result<Ledger>>,
    ) -> anyhow::Result<()> {
        self.opening_takeover(writer, Opening::Clean).await?;
        self.opening_takeover(writer, Opening::Crash).await
    }

    async fn opening_takeover(
        &mut self,
        writer: &mut JoinHandle<anyhow::Result<Ledger>>,
        opening: Opening,
    ) -> anyhow::Result<()> {
        let deadline = Instant::now() + OPENING_TIMEOUT;
        let before = self.fleet.takeovers();
        let mut sent = false;
        loop {
            if writer.is_finished() {
                anyhow::bail!("the workload ended early: {:?}", writer.await);
            }
            anyhow::ensure!(
                Instant::now() < deadline,
                "the opening {opening:?} takeover did not finish within {OPENING_TIMEOUT:?}"
            );
            tokio::time::sleep(TICK).await;
            self.fleet.advance(true).await?;
            let seen = self.fleet.poll().await;
            if !sent {
                sent = match opening {
                    Opening::Clean => self.fleet.term_active(&seen).await?,
                    Opening::Crash => self.fleet.kill_active(&seen, OPENING_RESTART).await?,
                };
            } else if self.fleet.takeovers() > before && !self.fleet.busy() {
                *self
                    .actions
                    .entry(format!("Opening{opening:?}Takeover"))
                    .or_default() += 1;
                return Ok(());
            }
        }
    }

    pub(super) async fn drive(
        &mut self,
        duration: Duration,
        writer: &mut JoinHandle<anyhow::Result<Ledger>>,
    ) -> anyhow::Result<()> {
        let end = Instant::now() + duration;
        let mut next_action = Instant::now() + ACTION_PERIOD;
        while Instant::now() < end {
            if writer.is_finished() {
                anyhow::bail!("the workload ended early: {:?}", writer.await);
            }
            tokio::time::sleep(TICK).await;
            self.fleet.advance(true).await?;
            let seen = self.fleet.poll().await;
            self.expire_windows(Instant::now());
            if Instant::now() >= next_action {
                next_action += ACTION_PERIOD;
                self.act(&seen).await?;
                self.operate(&seen, CHAOS_OPERATOR_BUDGET).await;
            }
        }
        Ok(())
    }

    fn expire_windows(&mut self, now: Instant) {
        if self.windows.sns.is_some_and(|until| now >= until) {
            self.windows.sns = None;
            self.sns_proxy.set(Faults::HEALTHY);
        }
        if self.windows.s3.is_some_and(|until| now >= until) {
            self.windows.s3 = None;
            self.s3_proxy.set(Faults::HEALTHY);
        }
    }

    fn millis(&mut self, low: u64, high: u64) -> Duration {
        Duration::from_millis(self.parameters.u64(low..=high))
    }

    async fn act(&mut self, seen: &[Option<Seen>]) -> anyhow::Result<()> {
        let action = choose(&mut self.schedule);
        let applied = match action {
            Action::KillActive => {
                let delay = self.millis(500, 2_000);
                self.fleet.kill_active(seen, delay).await?
            }
            Action::TermActive => self.fleet.term_active(seen).await?,
            Action::DropConnections => {
                self.postgres
                    .client()
                    .await?
                    .query(
                        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                         WHERE application_name = 'pg-cdc'",
                        &[],
                    )
                    .await?;
                true
            }
            Action::SinkOutage => {
                let until = Instant::now() + self.millis(3_000, 10_000);
                self.sns_proxy.set(Faults {
                    mode: Mode::Outage,
                    ..Faults::HEALTHY
                });
                self.windows.sns = Some(until);
                true
            }
            Action::StoreBrownout => {
                let latency = self.millis(200, 1_500);
                let until = Instant::now() + self.millis(3_000, 10_000);
                self.s3_proxy.set(Faults {
                    mode: Mode::Brownout(latency),
                    ..Faults::HEALTHY
                });
                self.windows.s3 = Some(until);
                true
            }
            Action::ErrorBurst => {
                let until = Instant::now() + Duration::from_secs(5);
                self.sns_proxy.set(Faults {
                    error_rate: 0.2,
                    error_after_forward_rate: 0.05,
                    ..Faults::HEALTHY
                });
                self.s3_proxy.set(Faults {
                    error_rate: 0.1,
                    error_after_forward_rate: 0.05,
                    ..Faults::HEALTHY
                });
                self.windows.sns = Some(until);
                self.windows.s3 = Some(until);
                true
            }
            Action::Nothing => true,
        };
        let name = if applied {
            format!("{action:?}")
        } else {
            SKIPPED.to_owned()
        };
        *self.actions.entry(name).or_default() += 1;
        Ok(())
    }

    async fn operate(&mut self, seen: &[Option<Seen>], budget: Duration) {
        let Some(active) = self.fleet.active(seen) else {
            return;
        };
        let observations = &self.observations;
        let pass = self
            .operator
            .pass(active, |token: u64, key: i32, seq: i64| {
                observations.record_marker(token, key, seq);
            });
        let _ = timeout(budget, pass).await;
    }

    pub(super) async fn settle(&mut self, ledger: &Ledger, limit: Duration) -> anyhow::Result<()> {
        self.sns_proxy.set(Faults::HEALTHY);
        self.s3_proxy.set(Faults::HEALTHY);
        self.windows = Windows::default();
        self.fleet.ensure_running().await?;
        let started = Instant::now();
        loop {
            tokio::time::sleep(ACTION_PERIOD).await;
            self.fleet.advance(true).await?;
            let seen = self.fleet.poll().await;
            self.operate(&seen, SETTLE_OPERATOR_BUDGET).await;
            let pending = outstanding(
                &ledger.committed,
                &self.observations.snapshot(),
                &self.operator.discards,
            );
            if settle_over(pending, started.elapsed(), limit) {
                if pending > 0 {
                    writeln!(
                        std::io::stdout(),
                        "chaos settle limit {limit:?} reached with {pending} changes neither delivered nor discarded; the checker looks for them in the state store"
                    )?;
                }
                return Ok(());
            }
        }
    }

    pub(super) async fn finish(
        mut self,
        ledger: Ledger,
        options: &ChaosOptions,
    ) -> anyhow::Result<Outcome> {
        self.fleet.stop_all().await?;
        let stored = read_stored(&self.s3, BUCKET, &parked_prefix()).await?;
        let mut problems: Vec<String> = self.fleet.failures().to_vec();
        let takeover_kinds = self.fleet.takeover_kinds();
        problems.extend(missing_takeovers(&takeover_kinds));
        let unrecognized = self.observations.unrecognized_count();
        if unrecognized > 0 {
            problems.push(format!(
                "{unrecognized} queue messages were not understood, for example {:?}",
                self.observations.unrecognized()
            ));
        }
        let misgrouped = self.observations.misgrouped_count();
        if misgrouped > 0 {
            problems.push(format!(
                "{misgrouped} deliveries were published under a second message group for their key, for example {:?}",
                self.observations.misgrouped()
            ));
        }
        let observed = self.observations.snapshot();
        let stored_seqs: HashSet<i64> = stored.iter().copied().collect();
        let resolution = resolve_unresolved(&self.operator.unresolved, &stored_seqs, &observed);
        let mut discards: BTreeSet<u64> = self.operator.discards.clone();
        discards.extend(&resolution.joined);
        let verdict = check(&Evidence {
            committed: ledger.committed,
            uncertain: ledger.uncertain,
            observed,
            stored,
            discards: discards.clone(),
        });
        let report = ChaosReport {
            seed: options.seed,
            duration_s: options.duration.as_secs_f64(),
            actions: self.actions,
            takeovers: self.fleet.takeovers(),
            takeover_kinds,
            redrives: self.operator.redrives,
            discards: u64::try_from(discards.len()).unwrap_or(u64::MAX),
            discards_refused: self.operator.refused + resolution.dropped,
            discards_resolved_late: u64::try_from(resolution.joined.len()).unwrap_or(u64::MAX),
            proxy: [self.sns_proxy.counts(), self.s3_proxy.counts()],
            ledger_transactions: ledger.transactions,
            verdict,
        };
        Ok(Outcome { report, problems })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_instances_share_one_config_with_the_plans_timings_and_no_payload_store() {
        let config = render_config("127.0.0.1", 5432, "postgres", "postgres", "arn:topic");
        for line in [
            "pipeline: chaos",
            "  slot: chaos_slot",
            "  publication: chaos_pub",
            "  status_interval: 1s",
            "  order_by: key",
            "  lanes: 8",
            "  park_after: 2s",
            "  max_inflight_bytes: 16MiB",
            "  type: sns",
            "  topic_arn: arn:topic",
            "  type: s3",
            "  bucket: chaos-state",
            "  prefix: chaos",
            "  lease_timeout: 4s",
            "  fence_delay: 4500ms",
            "  drain_timeout: 1s",
            "  listen: 127.0.0.1:0",
            "    mode: disable",
        ] {
            assert!(
                config.lines().any(|candidate| candidate == line),
                "missing {line:?} in\n{config}"
            );
        }
        assert!(!config.contains("payload_store"));
        assert!(!config.contains("password:"));
    }

    #[test]
    fn the_workload_matches_the_chaos_plan() {
        let config = workload_config(9);
        assert_eq!(config.rate, 300);
        assert_eq!(config.tx_size, 1..=20);
        assert_eq!(config.row_bytes, 200);
        assert_eq!(config.toast_every, 50);
        assert_eq!(config.oversize_every, 400);
        assert_eq!(config.keys, 50);
        assert!((config.skew - 1.0).abs() < f64::EPSILON);
        assert_eq!(config.seed, 9);
    }
}
