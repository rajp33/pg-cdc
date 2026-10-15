use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Context as _;
use pg_cdc_testkit::{OtlpReceiver, Postgres, parse_lsn};
use tempfile::TempDir;
use tokio::time::Instant;
use tokio_postgres::Client;
use tokio_util::sync::CancellationToken;

use crate::cluster::PgCdc;
use crate::measure::{self, Machine};
use crate::workload::{self, Ledger, WorkloadConfig};

pub(super) const PUBLICATION: &str = "load_pub";
pub(super) const LAG_LIMIT_BYTES: u64 = 16 * 1024 * 1024;
pub(super) const METRIC_EXPORT_INTERVAL: (&str, &str) = ("OTEL_METRIC_EXPORT_INTERVAL", "1000");
pub(super) const OFFERED_RATE: u32 = 6_000;
pub(super) const SUSTAINED_WINDOW: Duration = Duration::from_secs(60);
pub(super) const DRAIN_LIMIT: Duration = Duration::from_secs(600);

const POLL: Duration = Duration::from_millis(100);
const LAG_PERIOD: Duration = Duration::from_secs(1);
const STOP_TIMEOUT: Duration = Duration::from_secs(60);
const EXPORT_WAIT: Duration = Duration::from_secs(10);
const LATENCY_METRIC: &str = "pgcdc.commit_to_delivery";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Source {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum SinkSpec {
    Discard,
    Sns { topic_arn: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum StoreSpec {
    Filesystem(PathBuf),
    S3 { bucket: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ConfigSpec {
    pub slot: String,
    pub sink: SinkSpec,
    pub store: StoreSpec,
    pub max_inflight_bytes: Option<&'static str>,
    pub park_after: Option<&'static str>,
    pub otlp_endpoint: Option<String>,
}

pub(super) fn render_config(source: &Source, spec: &ConfigSpec) -> String {
    let mut lines = vec![
        "pipeline: load".to_owned(),
        "source:".to_owned(),
        format!("  host: {}", source.host),
        format!("  port: {}", source.port),
        format!("  database: {}", source.database),
        format!("  user: {}", source.user),
        "  credentials:".to_owned(),
        "    type: password".to_owned(),
        "  tls:".to_owned(),
        "    mode: disable".to_owned(),
        format!("  slot: {}", spec.slot),
        format!("  publication: {PUBLICATION}"),
        "  status_interval: 1s".to_owned(),
        "delivery:".to_owned(),
        "  order_by: key".to_owned(),
        "  lanes: 64".to_owned(),
    ];
    if let Some(bytes) = spec.max_inflight_bytes {
        lines.push(format!("  max_inflight_bytes: {bytes}"));
    }
    if let Some(after) = spec.park_after {
        lines.push(format!("  park_after: {after}"));
    }
    lines.push("sink:".to_owned());
    match &spec.sink {
        SinkSpec::Discard => lines.push("  type: discard".to_owned()),
        SinkSpec::Sns { topic_arn } => {
            lines.push("  type: sns".to_owned());
            lines.push(format!("  topic_arn: {topic_arn}"));
        }
    }
    lines.push("state_store:".to_owned());
    match &spec.store {
        StoreSpec::Filesystem(path) => {
            lines.push("  type: filesystem".to_owned());
            lines.push(format!("  path: {}", path.display()));
        }
        StoreSpec::S3 { bucket } => {
            lines.push("  type: s3".to_owned());
            lines.push(format!("  bucket: {bucket}"));
            lines.push("  prefix: load".to_owned());
        }
    }
    if let Some(endpoint) = &spec.otlp_endpoint {
        lines.push("telemetry:".to_owned());
        lines.push(format!("  otlp_endpoint: {endpoint}"));
        lines.push("  trace_sample_ratio: 1.0".to_owned());
    }
    lines.push("admin:".to_owned());
    lines.push("  listen: 127.0.0.1:0".to_owned());
    lines.push(String::new());
    lines.join("\n")
}

pub(super) fn count_f64(count: u64) -> f64 {
    u32::try_from(count).map_or(f64::MAX, f64::from)
}

pub(super) fn per_second(count: u64, window: Duration) -> f64 {
    count_f64(count) / window.as_secs_f64().max(f64::EPSILON)
}

pub(super) fn workload(rate: u32) -> WorkloadConfig {
    WorkloadConfig {
        rate,
        tx_size: 100..=100,
        row_bytes: 200,
        toast_every: 0,
        oversize_every: 0,
        keys: 1_000,
        skew: 0.0,
        seed: 1,
    }
}

pub(super) fn confirmed(status: &serde_json::Value) -> Option<u64> {
    parse_lsn(status.get("confirmed_lsn")?.as_str()?).ok()
}

pub(super) fn inflight_bytes(status: &serde_json::Value) -> Option<u64> {
    status.get("inflight")?.get("bytes")?.as_u64()
}

pub(super) fn last_lsn(ledger: &Ledger) -> anyhow::Result<u64> {
    let text = ledger
        .last_lsn
        .as_deref()
        .context("the workload recorded no end LSN")?;
    Ok(parse_lsn(text)?)
}

#[derive(Debug)]
pub(super) struct Bench {
    pub postgres: Postgres,
    pub client: Client,
    pub machine: Machine,
    dir: TempDir,
}

impl Bench {
    pub(super) async fn start(slots: &[&str]) -> anyhow::Result<Self> {
        let postgres = Postgres::start().await?;
        let client = postgres.client().await?;
        workload::setup(&client, PUBLICATION, slots).await?;
        let machine = measure::machine(&postgres).await?;
        Ok(Self {
            postgres,
            client,
            machine,
            dir: TempDir::new()?,
        })
    }

    pub(super) fn source(&self) -> Source {
        Source {
            host: self.postgres.host().to_owned(),
            port: self.postgres.port(),
            database: self.postgres.database().to_owned(),
            user: self.postgres.user().to_owned(),
        }
    }

    pub(super) async fn scratch(&self, name: &str) -> anyhow::Result<PathBuf> {
        let path = self.dir.path().join(name);
        tokio::fs::create_dir_all(&path).await?;
        Ok(path)
    }

    pub(super) async fn spawn(
        &self,
        binary: &Path,
        spec: &ConfigSpec,
        env: &[(String, String)],
        name: &str,
    ) -> anyhow::Result<(PgCdc, String)> {
        let text = render_config(&self.source(), spec);
        let config = self
            .scratch(&format!("{name}-config"))
            .await?
            .join("config.yaml");
        tokio::fs::write(&config, &text).await?;
        let mut env = env.to_vec();
        env.push((
            "PGCDC__SOURCE__CREDENTIALS__PASSWORD".to_owned(),
            self.postgres.password().to_owned(),
        ));
        let process = PgCdc::spawn(binary, &config, &env, name).await?;
        Ok((process, text))
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct Drained {
    pub wall: Duration,
    pub cpu: Duration,
    pub peak_rss_bytes: u64,
}

pub(super) async fn drain(
    process: &PgCdc,
    target: u64,
    started: Instant,
    within: Duration,
) -> anyhow::Result<Drained> {
    let pid = process.pid().context("pg-cdc is not running")?;
    let deadline = started + within;
    let mut peak_rss_bytes = 0;
    loop {
        let sample = measure::sample(pid).await?;
        peak_rss_bytes = peak_rss_bytes.max(sample.rss_bytes);
        let status = process.status().await?;
        if confirmed(&status).is_some_and(|lsn| lsn >= target) {
            return Ok(Drained {
                wall: started.elapsed(),
                cpu: sample.cpu,
                peak_rss_bytes,
            });
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "pg-cdc did not confirm LSN {target:#X} within {within:?}; last status: {status}"
        );
        tokio::time::sleep(POLL).await;
    }
}

pub(super) async fn stop(process: &mut PgCdc) -> anyhow::Result<String> {
    process.sigterm().await?;
    if let Some(status) = process.wait(STOP_TIMEOUT).await? {
        return Ok(status.to_string());
    }
    process.sigkill().await?;
    Ok(format!("killed after {STOP_TIMEOUT:?} without exiting"))
}

pub(super) async fn lag_bytes(probe: &Client, process: &PgCdc) -> anyhow::Result<u64> {
    let current: String = probe
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await?
        .get(0);
    let status = process.status().await?;
    Ok(parse_lsn(&current)?.saturating_sub(confirmed(&status).unwrap_or(0)))
}

#[derive(Debug)]
pub(super) struct Sustained {
    pub ledger: Ledger,
    pub elapsed: Duration,
    pub max_lag_bytes: u64,
    pub final_lag_bytes: u64,
}

impl Sustained {
    pub(super) fn kept_up(&self) -> bool {
        self.final_lag_bytes < LAG_LIMIT_BYTES
    }

    pub(super) fn written(&self) -> u64 {
        u64::try_from(self.ledger.committed.len()).unwrap_or(u64::MAX)
    }

    pub(super) fn achieved_rate(&self) -> f64 {
        per_second(self.written(), self.elapsed)
    }
}

pub(super) async fn sustain(
    bench: &Bench,
    process: &PgCdc,
    config: WorkloadConfig,
    window: Duration,
) -> anyhow::Result<Sustained> {
    let writer_client = bench.postgres.client().await?;
    let probe = bench.postgres.client().await?;
    let stop = CancellationToken::new();
    let started = Instant::now();
    let writer = tokio::spawn({
        let stop = stop.clone();
        async move { workload::write(&writer_client, &config, stop).await }
    });
    let mut max_lag_bytes = 0;
    while started.elapsed() < window {
        anyhow::ensure!(
            !writer.is_finished(),
            "the workload ended early: {:?}",
            writer.await
        );
        tokio::time::sleep(LAG_PERIOD).await;
        max_lag_bytes = max_lag_bytes.max(lag_bytes(&probe, process).await?);
    }
    stop.cancel();
    let ledger = writer.await.context("the workload task panicked")??;
    let elapsed = started.elapsed();
    let final_lag_bytes = lag_bytes(&probe, process).await?;
    Ok(Sustained {
        ledger,
        elapsed,
        max_lag_bytes: max_lag_bytes.max(final_lag_bytes),
        final_lag_bytes,
    })
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct Latency {
    pub p99_upper_bound_s: Option<f64>,
    pub observed: u64,
}

pub(super) async fn latency(receiver: &OtlpReceiver, expected: u64) -> Latency {
    let exported = receiver
        .wait_for(EXPORT_WAIT, |seen| {
            seen.histogram(LATENCY_METRIC)
                .is_some_and(|(_, _, count)| count >= expected)
        })
        .await
        .unwrap_or_else(|_| receiver.received());
    exported.histogram(LATENCY_METRIC).map_or(
        Latency {
            p99_upper_bound_s: None,
            observed: 0,
        },
        |(bounds, counts, observed)| Latency {
            p99_upper_bound_s: measure::p99_upper_bound(&bounds, &counts),
            observed,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source() -> Source {
        Source {
            host: "127.0.0.1".to_owned(),
            port: 54_321,
            database: "postgres".to_owned(),
            user: "postgres".to_owned(),
        }
    }

    fn spec() -> ConfigSpec {
        ConfigSpec {
            slot: "load_slot".to_owned(),
            sink: SinkSpec::Discard,
            store: StoreSpec::Filesystem(PathBuf::from("/tmp/state")),
            max_inflight_bytes: None,
            park_after: None,
            otlp_endpoint: None,
        }
    }

    #[test]
    fn a_discard_config_uses_a_filesystem_store_and_no_telemetry() {
        let text = render_config(&source(), &spec());
        for line in [
            "  host: 127.0.0.1",
            "  port: 54321",
            "  slot: load_slot",
            "  publication: load_pub",
            "  status_interval: 1s",
            "  order_by: key",
            "  lanes: 64",
            "  type: discard",
            "  type: filesystem",
            "  path: /tmp/state",
            "  listen: 127.0.0.1:0",
        ] {
            assert!(text.lines().any(|found| found == line), "{line} in {text}");
        }
        for absent in ["telemetry:", "max_inflight_bytes", "park_after", "sns"] {
            assert!(!text.contains(absent), "{absent} in {text}");
        }
        assert!(!text.contains("password:"));
    }

    #[test]
    fn an_sns_config_with_s3_telemetry_and_limits_carries_each_setting() {
        let text = render_config(
            &source(),
            &ConfigSpec {
                sink: SinkSpec::Sns {
                    topic_arn: "arn:aws:sns:us-east-1:123456789012:load".to_owned(),
                },
                store: StoreSpec::S3 {
                    bucket: "load-state".to_owned(),
                },
                max_inflight_bytes: Some("64MiB"),
                park_after: Some("1h"),
                otlp_endpoint: Some("http://127.0.0.1:4317".to_owned()),
                ..spec()
            },
        );
        for line in [
            "  max_inflight_bytes: 64MiB",
            "  park_after: 1h",
            "  type: sns",
            "  topic_arn: arn:aws:sns:us-east-1:123456789012:load",
            "  type: s3",
            "  bucket: load-state",
            "telemetry:",
            "  otlp_endpoint: http://127.0.0.1:4317",
            "  trace_sample_ratio: 1.0",
        ] {
            assert!(text.lines().any(|found| found == line), "{line} in {text}");
        }
    }

    #[test]
    fn status_fields_are_read_from_the_admin_json() {
        let status = serde_json::json!({
            "confirmed_lsn": "0/16B3740",
            "inflight": { "events": 3, "bytes": 4096 },
        });
        assert_eq!(confirmed(&status), Some(0x16B_3740));
        assert_eq!(inflight_bytes(&status), Some(4_096));
        let starting = serde_json::json!({ "confirmed_lsn": null });
        assert_eq!(confirmed(&starting), None);
        assert_eq!(inflight_bytes(&starting), None);
    }

    #[test]
    fn rates_divide_counts_by_the_window() {
        assert!((per_second(6_000, Duration::from_secs(2)) - 3_000.0).abs() < 1e-9);
        assert!((count_f64(5_000_000_000) - f64::MAX).abs() < f64::EPSILON);
    }
}
