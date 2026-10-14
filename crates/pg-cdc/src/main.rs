use std::io::Write as _;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use anyhow::Context as _;
use clap::{Parser, Subcommand};
use pg_cdc::config::{Config, Credentials, SinkConfig, SourceConfig};
use pg_cdc::connection::{
    ConnectParams, ConnectionProvider, PasswordProvider, RdsIamProvider, TlsConfig,
};
use pg_cdc::session::{REPLICATION_BUFFER, SessionConfig};
use pg_cdc::sink::Sink;
use pg_cdc::sink::discard::DiscardSink;
use pg_cdc::sink::sns::SnsSink;
use pg_cdc::sink::stdout::StdoutSink;
use pg_cdc::supervisor::{LiveSession, preflight, supervise};
use tokio::signal::unix::{Signal, SignalKind, signal};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    name = "pg-cdc",
    version,
    about = "Stream Postgres logical replication changes to a sink"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Run {
        #[arg(long)]
        config: PathBuf,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    let Command::Run { config } = Cli::parse().command;
    let env = std::env::vars_os()
        .filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?)));
    let config = match pg_cdc::config::load(&config, env) {
        Ok(config) => config,
        Err(error) => {
            let _ = writeln!(std::io::stderr(), "pg-cdc: invalid configuration: {error}");
            return ExitCode::FAILURE;
        }
    };
    init_logging(&config.sink);
    match run(config).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!("pg-cdc stopped: {error:#}");
            ExitCode::FAILURE
        }
    }
}

fn init_logging(sink: &SinkConfig) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt().json().with_env_filter(filter);
    match sink {
        SinkConfig::Stdout => builder.with_writer(std::io::stderr).init(),
        SinkConfig::Discard | SinkConfig::Sns(_) => builder.with_writer(std::io::stdout).init(),
    }
}

async fn run(config: Config) -> anyhow::Result<()> {
    let shutdown = CancellationToken::new();
    let terminate = signal(SignalKind::terminate()).context("cannot listen for SIGTERM")?;
    let interrupt = signal(SignalKind::interrupt()).context("cannot listen for SIGINT")?;
    tokio::spawn(watch_signals(terminate, interrupt, shutdown.clone()));
    tracing::info!(pipeline = %config.pipeline, slot = %config.source.slot, "pg-cdc starting");
    let Some(provider) = shutdown
        .run_until_cancelled(Box::pin(provider(&config.source)))
        .await
    else {
        tracing::info!("pg-cdc stopped cleanly");
        return Ok(());
    };
    let Some(sink) = shutdown
        .run_until_cancelled(Box::pin(sink(&config.sink)))
        .await
    else {
        tracing::info!("pg-cdc stopped cleanly");
        return Ok(());
    };
    if !preflight(sink.as_ref(), &shutdown).await {
        tracing::info!("pg-cdc stopped cleanly");
        return Ok(());
    }
    let session = SessionConfig {
        pipeline: config.pipeline.clone(),
        slot: config.source.slot.clone(),
        publication: config.source.publication.clone(),
        status_interval: config.source.status_interval,
        buffer: REPLICATION_BUFFER,
        order_by: config.delivery.order_by.clone(),
        lanes: config.delivery.lanes,
        sink_concurrency: config.sink.max_concurrency(),
        attribute_columns: config.sink.attribute_columns().to_vec(),
        max_inflight_bytes: config.delivery.max_inflight_bytes,
    };
    let mut live = LiveSession {
        config: &session,
        provider: provider.as_ref(),
        sink: &sink,
        shutdown: &shutdown,
    };
    supervise(&mut live, &shutdown)
        .await
        .context("replication stopped")?;
    tracing::info!("pg-cdc stopped cleanly");
    Ok(())
}

async fn sink(config: &SinkConfig) -> Arc<dyn Sink> {
    match config {
        SinkConfig::Stdout => Arc::new(StdoutSink::stdout()),
        SinkConfig::Discard => Arc::new(DiscardSink::new()),
        SinkConfig::Sns(sns) => Arc::new(
            SnsSink::from_environment(
                sns.topic_arn.clone(),
                sns.region.clone(),
                sns.fair_queue_group.clone(),
            )
            .await,
        ),
    }
}

async fn provider(source: &SourceConfig) -> Box<dyn ConnectionProvider> {
    let base = ConnectParams {
        host: source.host.clone(),
        port: source.port,
        database: source.database.clone(),
        user: source.user.clone(),
        password: None,
        tls: TlsConfig {
            mode: source.tls.mode,
            ca_file: source.tls.ca_file.clone(),
        },
    };
    match &source.credentials {
        Credentials::Password { password } => Box::new(PasswordProvider::new(ConnectParams {
            password: password.clone(),
            ..base
        })),
        Credentials::AwsRdsIam { region } => {
            Box::new(RdsIamProvider::from_environment(base, region.clone()).await)
        }
    }
}

async fn watch_signals(mut terminate: Signal, mut interrupt: Signal, shutdown: CancellationToken) {
    tokio::select! {
        _ = terminate.recv() => {}
        _ = interrupt.recv() => {}
    }
    tracing::info!("shutdown requested");
    shutdown.cancel();
}
