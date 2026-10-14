use std::time::Duration;

use pg_cdc::config::TlsMode;
use pg_cdc::connection::{ConnectParams, PasswordProvider, TlsConfig};
use pg_cdc::session::{SessionConfig, SessionError, run_session};
use pg_cdc::sink::StdoutSink;
use pg_cdc_testkit::Postgres;
use secrecy::SecretString;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader, DuplexStream, Lines};
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

pub(crate) struct Running {
    shutdown: CancellationToken,
    task: JoinHandle<Result<(), SessionError>>,
    lines: Lines<BufReader<DuplexStream>>,
}

pub(crate) fn spawn_session(
    params: ConnectParams,
    status_interval: Duration,
    buffer: usize,
) -> Running {
    spawn_after_abandoned_writes(params, status_interval, buffer, 0)
}

pub(crate) fn spawn_after_abandoned_writes(
    params: ConnectParams,
    status_interval: Duration,
    buffer: usize,
    abandoned: usize,
) -> Running {
    let (writer, reader) = tokio::io::duplex(buffer);
    let shutdown = CancellationToken::new();
    let token = shutdown.clone();
    let config = SessionConfig {
        pipeline: PIPELINE.to_owned(),
        slot: SLOT.to_owned(),
        publication: PUBLICATION.to_owned(),
        status_interval,
        buffer,
    };
    let task = tokio::spawn(async move {
        let provider = PasswordProvider::new(params);
        let mut sink = StdoutSink::new(writer);
        for n in 0..abandoned {
            let line = format!("{{\"abandoned\":{n}}}");
            sink.write(line.as_bytes())
                .await
                .map_err(SessionError::Sink)?;
        }
        run_session(&config, &provider, &mut sink, &token).await
    });
    Running {
        shutdown,
        task,
        lines: BufReader::new(reader).lines(),
    }
}

impl Running {
    pub(crate) async fn next_event(&mut self) -> TestResult<Value> {
        let line = tokio::time::timeout(WAIT, self.lines.next_line())
            .await??
            .ok_or("session output closed")?;
        Ok(serde_json::from_str(&line)?)
    }

    pub(crate) async fn no_event_within(&mut self, window: Duration) -> TestResult<bool> {
        Ok(tokio::time::timeout(window, self.lines.next_line())
            .await
            .is_err())
    }

    pub(crate) async fn stop(self) -> TestResult {
        self.shutdown.cancel();
        self.task.await??;
        Ok(())
    }

    pub(crate) async fn stop_once_output_starts(self) -> TestResult<Vec<u8>> {
        let mut reader = self.lines.into_inner();
        let mut output = vec![0u8];
        tokio::time::timeout(WAIT, reader.read_exact(&mut output)).await??;
        self.shutdown.cancel();
        tokio::time::timeout(WAIT, reader.read_to_end(&mut output)).await??;
        self.task.await??;
        Ok(output)
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

pub(crate) fn commit_lsn(event: &Value) -> TestResult<u64> {
    let text = event
        .pointer("/source/commit_lsn")
        .and_then(Value::as_str)
        .ok_or("missing commit_lsn")?;
    Ok(pg_cdc_testkit::parse_lsn(text)?)
}
