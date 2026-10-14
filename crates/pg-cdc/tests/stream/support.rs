use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use pg_cdc::config::{DEFAULT_MAX_INFLIGHT_BYTES, TlsMode};
use pg_cdc::connection::{ConnectParams, PasswordProvider, TlsConfig};
use pg_cdc::order::OrderBy;
use pg_cdc::session::{SessionConfig, SessionError, run_session};
use pg_cdc::sink::stdout::StdoutSink;
use pg_cdc::sink::{Outbound, Outcome, Sink, SinkCapabilities, SinkError};
use pg_cdc_testkit::Postgres;
use secrecy::SecretString;
use serde_json::Value;
use tokio::io::{AsyncReadExt, DuplexStream};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub(crate) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub(crate) const SLOT: &str = "it_slot";
pub(crate) const PUBLICATION: &str = "it_pub";
pub(crate) const PIPELINE: &str = "it";
pub(crate) const WAIT: Duration = Duration::from_secs(30);

pub(crate) async fn prepare(pg: &Postgres, statements: &[&str], tables: &[&str]) -> TestResult {
    for statement in statements {
        pg.execute(statement).await?;
    }
    pg.create_publication(PUBLICATION, tables).await?;
    pg.create_slot(SLOT).await?;
    Ok(())
}

pub(crate) fn params(pg: &Postgres) -> ConnectParams {
    ConnectParams {
        host: pg.host().to_owned(),
        port: pg.port(),
        database: pg.database().to_owned(),
        user: pg.user().to_owned(),
        password: Some(SecretString::from(pg.password().to_owned())),
        tls: TlsConfig {
            mode: TlsMode::Disable,
            ca_file: None,
        },
    }
}

pub(crate) fn config(status_interval: Duration, buffer: usize) -> SessionConfig {
    SessionConfig {
        pipeline: PIPELINE.to_owned(),
        slot: SLOT.to_owned(),
        publication: PUBLICATION.to_owned(),
        status_interval,
        buffer,
        order_by: OrderBy::Pipeline,
        lanes: 1,
        sink_concurrency: 4,
        attribute_columns: Vec::new(),
        max_inflight_bytes: DEFAULT_MAX_INFLIGHT_BYTES,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Mode {
    Deliver,
    Hold,
    Fail,
    FailContaining(String),
}

#[derive(Debug)]
pub(crate) struct TestSink {
    mode: watch::Receiver<Mode>,
    failures: watch::Sender<u64>,
    delivered: mpsc::UnboundedSender<Bytes>,
}

#[async_trait]
impl Sink for TestSink {
    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities {
            max_batch_messages: 10,
            max_batch_bytes: 1 << 20,
            max_message_bytes: 1 << 20,
        }
    }

    async fn preflight(&self) -> Result<(), SinkError> {
        Ok(())
    }

    async fn send(&self, batch: &[Outbound]) -> Vec<Outcome> {
        let mut mode = self.mode.clone();
        let Ok(current) = mode
            .wait_for(|mode| *mode != Mode::Hold)
            .await
            .map(|mode| mode.clone())
        else {
            return vec![Outcome::Retryable(SinkError("test sink closed".to_owned())); batch.len()];
        };
        batch
            .iter()
            .map(|outbound| {
                let fail = match &current {
                    Mode::Fail => true,
                    Mode::FailContaining(marker) => outbound
                        .body
                        .windows(marker.len())
                        .any(|window| window == marker.as_bytes()),
                    Mode::Deliver | Mode::Hold => false,
                };
                if fail {
                    self.failures.send_modify(|count| *count += 1);
                    Outcome::Retryable(SinkError("test sink is failing".to_owned()))
                } else {
                    let _ = self.delivered.send(outbound.body.clone());
                    Outcome::Delivered
                }
            })
            .collect()
    }
}

pub(crate) struct Running {
    shutdown: CancellationToken,
    task: JoinHandle<Result<(), SessionError>>,
    events: mpsc::UnboundedReceiver<Bytes>,
    mode: watch::Sender<Mode>,
    failures: watch::Receiver<u64>,
}

pub(crate) fn spawn_session(
    params: ConnectParams,
    status_interval: Duration,
    buffer: usize,
) -> Running {
    spawn_with(params, config(status_interval, buffer))
}

pub(crate) fn spawn_with(params: ConnectParams, config: SessionConfig) -> Running {
    let (mode, mode_receiver) = watch::channel(Mode::Deliver);
    let (failures_sender, failures) = watch::channel(0);
    let (delivered, events) = mpsc::unbounded_channel();
    let sink: Arc<dyn Sink> = Arc::new(TestSink {
        mode: mode_receiver,
        failures: failures_sender,
        delivered,
    });
    let shutdown = CancellationToken::new();
    let task = spawn_run(params, config, sink, shutdown.clone());
    Running {
        shutdown,
        task,
        events,
        mode,
        failures,
    }
}

fn spawn_run(
    params: ConnectParams,
    config: SessionConfig,
    sink: Arc<dyn Sink>,
    shutdown: CancellationToken,
) -> JoinHandle<Result<(), SessionError>> {
    tokio::spawn(async move {
        let provider = PasswordProvider::new(params);
        run_session(&config, &provider, &sink, &shutdown).await
    })
}

impl Running {
    pub(crate) async fn next_event(&mut self) -> TestResult<Value> {
        let body = tokio::time::timeout(WAIT, self.events.recv())
            .await?
            .ok_or("session output closed")?;
        Ok(serde_json::from_slice(&body)?)
    }

    pub(crate) async fn next_events(&mut self, count: usize) -> TestResult<Vec<Value>> {
        let mut events = Vec::with_capacity(count);
        for _ in 0..count {
            events.push(self.next_event().await?);
        }
        Ok(events)
    }

    pub(crate) async fn skip_events(&mut self, count: usize) -> TestResult {
        for _ in 0..count {
            self.next_event().await?;
        }
        Ok(())
    }

    pub(crate) async fn no_event_within(&mut self, window: Duration) -> TestResult<bool> {
        Ok(tokio::time::timeout(window, self.events.recv())
            .await
            .is_err())
    }

    pub(crate) fn set_mode(&self, mode: Mode) {
        self.mode.send_replace(mode);
    }

    pub(crate) async fn wait_for_failures(&mut self, count: u64) -> TestResult {
        tokio::time::timeout(WAIT, self.failures.wait_for(|failed| *failed >= count)).await??;
        Ok(())
    }

    pub(crate) async fn stop(self) -> TestResult {
        self.shutdown.cancel();
        self.task.await??;
        Ok(())
    }

    pub(crate) async fn abort(self) -> TestResult {
        self.task.abort();
        match self.task.await {
            Err(error) if error.is_cancelled() => Ok(()),
            Err(error) => Err(error.into()),
            Ok(result) => Ok(result?),
        }
    }
}

pub(crate) struct StdoutRunning {
    shutdown: CancellationToken,
    task: JoinHandle<Result<(), SessionError>>,
    reader: DuplexStream,
}

pub(crate) fn spawn_stdout_session(
    params: ConnectParams,
    status_interval: Duration,
    pipe: usize,
) -> StdoutRunning {
    let (writer, reader) = tokio::io::duplex(pipe);
    let shutdown = CancellationToken::new();
    let sink: Arc<dyn Sink> = Arc::new(StdoutSink::new(writer));
    let task = spawn_run(params, config(status_interval, 64), sink, shutdown.clone());
    StdoutRunning {
        shutdown,
        task,
        reader,
    }
}

impl StdoutRunning {
    pub(crate) async fn stop_once_output_starts(mut self) -> TestResult<Vec<u8>> {
        let mut output = vec![0u8];
        tokio::time::timeout(WAIT, self.reader.read_exact(&mut output)).await??;
        self.shutdown.cancel();
        tokio::time::timeout(WAIT, self.reader.read_to_end(&mut output)).await??;
        self.task.await??;
        Ok(output)
    }
}

pub(crate) fn commit_lsn(event: &Value) -> TestResult<u64> {
    let text = event
        .pointer("/source/commit_lsn")
        .and_then(Value::as_str)
        .ok_or("missing commit_lsn")?;
    Ok(pg_cdc_testkit::parse_lsn(text)?)
}
