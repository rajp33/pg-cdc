#![cfg(feature = "otel")]

use std::io::Write as _;
use std::path::Path;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pg_cdc_testkit::{OtlpReceiver, Postgres, Received, blackhole};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::process::{Child, Command};
use tokio::time::{sleep, timeout};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const WAIT: Duration = Duration::from_secs(30);
const EXPORTED_WITHIN: Duration = Duration::from_secs(15);
const POLL: Duration = Duration::from_millis(25);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(1);
const EXIT_SLACK: Duration = Duration::from_secs(2);
const SLOT: &str = "telemetry_slot";
const PUBLICATION: &str = "telemetry_pub";
const TOOK_OVER: &str = "took over the slot";
const LEADERSHIP: &str = "leadership:\n  lease_timeout: 4s\n  fence_delay: 4500ms\n  drain_timeout: 1s\nadmin:\n  listen: 127.0.0.1:0\n";

#[derive(Clone, Default)]
struct Lines(Arc<Mutex<Vec<String>>>);

impl Lines {
    fn collect<R: AsyncRead + Send + Unpin + 'static>(reader: R) -> Self {
        let lines = Self::default();
        let sink = lines.clone();
        tokio::spawn(async move {
            let mut reader = BufReader::new(reader).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                if let Ok(mut lines) = sink.0.lock() {
                    lines.push(line);
                }
            }
        });
        lines
    }

    fn all(&self) -> Vec<String> {
        self.0.lock().map(|lines| lines.clone()).unwrap_or_default()
    }

    fn json(&self) -> Vec<Value> {
        self.all()
            .iter()
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect()
    }

    fn contains(&self, needle: &str) -> bool {
        self.all().iter().any(|line| line.contains(needle))
    }

    async fn wait_for(&self, needle: &str) -> TestResult {
        let deadline = Instant::now() + WAIT;
        while !self.contains(needle) {
            if Instant::now() >= deadline {
                return Err(format!("no {needle:?} within {WAIT:?}").into());
            }
            sleep(POLL).await;
        }
        Ok(())
    }

    async fn wait_for_count(&self, count: usize) -> TestResult {
        let deadline = Instant::now() + WAIT;
        while self.all().len() < count {
            if Instant::now() >= deadline {
                return Err(format!("fewer than {count} lines within {WAIT:?}").into());
            }
            sleep(POLL).await;
        }
        Ok(())
    }
}

struct Task {
    child: Child,
    stdout: Lines,
    stderr: Lines,
}

impl Task {
    fn spawn(config: &Path, password: &str, env: &[(&str, &str)]) -> TestResult<Self> {
        let mut child = Command::new(env!("CARGO_BIN_EXE_pg-cdc"))
            .arg("run")
            .arg("--config")
            .arg(config)
            .env("PGCDC__SOURCE__CREDENTIALS__PASSWORD", password)
            .env_remove("PGCDC__ADMIN__LISTEN")
            .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
            .envs(env.iter().copied())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let stdout = Lines::collect(child.stdout.take().ok_or("no stdout")?);
        let stderr = Lines::collect(child.stderr.take().ok_or("no stderr")?);
        Ok(Self {
            child,
            stdout,
            stderr,
        })
    }

    async fn sigterm(&self) -> TestResult {
        let pid = self.child.id().ok_or("process already exited")?;
        let status = Command::new("kill")
            .arg("-TERM")
            .arg(pid.to_string())
            .status()
            .await?;
        if status.success() {
            Ok(())
        } else {
            Err("kill -TERM failed".into())
        }
    }
}

fn write_config(
    source: &str,
    telemetry: &str,
    state: &Path,
) -> TestResult<tempfile::NamedTempFile> {
    let mut file = tempfile::NamedTempFile::new()?;
    write!(
        file,
        "pipeline: telemetry\nsource:\n{source}  credentials:\n    type: password\n  tls:\n    mode: disable\n  slot: {SLOT}\n  publication: {PUBLICATION}\n  status_interval: 500ms\ndelivery:\n  order_by: key\n  lanes: 2\nsink:\n  type: stdout\nstate_store:\n  type: filesystem\n  path: {path}\n{LEADERSHIP}telemetry:\n{telemetry}",
        path = state.display(),
    )?;
    Ok(file)
}

#[tokio::test]
async fn sigterm_with_an_unreachable_exporter_exits_within_the_budget() -> TestResult {
    let (endpoint, server) = blackhole().await?;
    let state = tempfile::tempdir()?;
    let config = write_config(
        "  host: 127.0.0.1\n  port: 1\n  database: postgres\n  user: postgres\n",
        &format!("  otlp_endpoint: {endpoint}\n"),
        state.path(),
    )?;
    let mut task = Task::spawn(
        config.path(),
        "unused",
        &[("OTEL_METRIC_EXPORT_INTERVAL", "200")],
    )?;
    task.stderr.wait_for("admin listening").await?;
    sleep(Duration::from_secs(1)).await;

    task.sigterm().await?;
    let signalled = Instant::now();
    let budget = 2 * DRAIN_TIMEOUT + Duration::from_secs(3) + EXIT_SLACK;
    let status = timeout(budget, task.child.wait()).await??;

    assert!(status.success(), "{status}");
    assert!(
        signalled.elapsed() < budget,
        "exited after {:?}",
        signalled.elapsed()
    );
    task.stderr.wait_for("pg-cdc stopped cleanly").await?;
    assert!(task.stderr.contains("shutdown requested"));
    server.abort();
    Ok(())
}

fn exported(exports: &Received) -> bool {
    let received = [("outcome", "received")];
    let delivered = [
        ("outcome", "delivered"),
        ("table", "orders"),
        ("op", "insert"),
    ];
    let spans = exports.span_names();
    exports
        .sum("pgcdc.events", &received)
        .is_some_and(|count| count >= 3.0)
        && exports
            .sum("pgcdc.events", &delivered)
            .is_some_and(|count| count >= 3.0)
        && exports.gauge("pgcdc.role") == Some(1.0)
        && exports.gauge("pgcdc.leader.epoch") == Some(1.0)
        && exports.sum("pgcdc.takeovers", &[("kind", "clean")]) == Some(1.0)
        && exports
            .histogram("pgcdc.commit_to_delivery")
            .is_some_and(|(_, _, count)| count >= 3)
        && exports.gauge("pgcdc.status_update.age").is_some()
        && [
            "transaction",
            "sink_send",
            "role_change",
            "takeover",
            "status_update",
        ]
        .iter()
        .all(|name| spans.contains(*name))
        && exports
            .log_bodies()
            .iter()
            .any(|body| body.contains(TOOK_OVER))
}

#[tokio::test]
async fn a_running_pipeline_exports_metrics_traces_and_logs() -> TestResult {
    let pg = Postgres::start().await?;
    pg.execute("CREATE TABLE orders (id int PRIMARY KEY, status text)")
        .await?;
    pg.create_publication(PUBLICATION, &["orders"]).await?;
    pg.create_slot(SLOT).await?;
    let receiver = OtlpReceiver::start().await?;
    let state = tempfile::tempdir()?;
    let config = write_config(
        &format!(
            "  host: {}\n  port: {}\n  database: {}\n  user: {}\n",
            pg.host(),
            pg.port(),
            pg.database(),
            pg.user()
        ),
        &format!(
            "  otlp_endpoint: {}\n  trace_sample_ratio: 1.0\n",
            receiver.endpoint()
        ),
        state.path(),
    )?;
    let mut task = Task::spawn(
        config.path(),
        pg.password(),
        &[("OTEL_METRIC_EXPORT_INTERVAL", "500")],
    )?;
    task.stderr.wait_for(TOOK_OVER).await?;
    pg.execute("INSERT INTO orders VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await?;
    task.stdout.wait_for_count(3).await?;

    let exports = match receiver.wait_for(EXPORTED_WITHIN, exported).await {
        Ok(exports) => exports,
        Err(_) => receiver.received(),
    };

    for outcome in ["received", "delivered"] {
        let count = exports.sum("pgcdc.events", &[("outcome", outcome)]);
        assert!(
            count.is_some_and(|count| count >= 3.0),
            "{outcome}: {count:?}"
        );
    }
    assert!(
        exports
            .sum(
                "pgcdc.events",
                &[
                    ("outcome", "delivered"),
                    ("table", "orders"),
                    ("op", "insert")
                ]
            )
            .is_some_and(|count| count >= 3.0)
    );
    assert_eq!(exports.gauge("pgcdc.role"), Some(1.0));
    assert_eq!(exports.gauge("pgcdc.leader.epoch"), Some(1.0));
    assert_eq!(
        exports.sum("pgcdc.takeovers", &[("kind", "clean")]),
        Some(1.0)
    );
    let histogram = exports.histogram("pgcdc.commit_to_delivery");
    assert!(
        histogram.as_ref().is_some_and(|(_, _, count)| *count >= 3),
        "{histogram:?}"
    );
    assert!(exports.gauge("pgcdc.status_update.age").is_some());
    let spans = exports.span_names();
    for name in [
        "transaction",
        "sink_send",
        "role_change",
        "takeover",
        "status_update",
    ] {
        assert!(spans.contains(name), "no {name:?} span in {spans:?}");
    }
    assert!(
        exports
            .log_bodies()
            .iter()
            .any(|body| body.contains(TOOK_OVER)),
        "{:?}",
        exports.log_bodies()
    );
    assert!(
        task.stderr
            .json()
            .iter()
            .any(|line| line.get("trace_id").is_some()),
        "no stderr line carries a trace_id"
    );

    task.sigterm().await?;
    let status = timeout(
        2 * DRAIN_TIMEOUT + Duration::from_secs(3) + EXIT_SLACK,
        task.child.wait(),
    )
    .await??;
    assert!(status.success(), "{status}");
    Ok(())
}
