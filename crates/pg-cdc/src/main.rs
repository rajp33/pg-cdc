use std::io::Write as _;
use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::Context as _;
use clap::{Parser, Subcommand};
use pg_cdc::catalog::TypeResolver;
use pg_cdc::config::{Config, Credentials, SinkConfig, SourceConfig};
use pg_cdc::connection::{
    ConnectParams, ConnectionProvider, PasswordProvider, RdsIamProvider, TlsConfig,
};
use pg_cdc::session::{REPLICATION_BUFFER, SessionConfig};
use pg_cdc::sink::StdoutSink;
use pg_cdc::supervisor::{LiveSession, supervise};
use tokio::signal::unix::{SignalKind, signal};
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
    }
}

async fn run(config: Config) -> anyhow::Result<()> {
    let provider = provider(&config.source).await;
    let shutdown = CancellationToken::new();
    tokio::spawn(watch_signals(shutdown.clone()));
    let session = SessionConfig {
        pipeline: config.pipeline.clone(),
        slot: config.source.slot.clone(),
        publication: config.source.publication.clone(),
        status_interval: config.source.status_interval,
        buffer: REPLICATION_BUFFER,
    };
    let mut types = TypeResolver::new();
    let mut sink = StdoutSink::stdout();
    let mut live = LiveSession {
        config: &session,
        provider: provider.as_ref(),
        types: &mut types,
        sink: &mut sink,
        shutdown: &shutdown,
    };
    tracing::info!(pipeline = %config.pipeline, slot = %config.source.slot, "pg-cdc starting");
    supervise(&mut live, &shutdown)
        .await
        .context("replication stopped")?;
    tracing::info!("pg-cdc stopped cleanly");
    Ok(())
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

async fn watch_signals(shutdown: CancellationToken) {
    let mut terminate = match signal(SignalKind::terminate()) {
        Ok(terminate) => terminate,
        Err(error) => {
            tracing::error!(%error, "cannot listen for SIGTERM");
            return;
        }
    };
    tokio::select! {
        _ = terminate.recv() => {}
        result = tokio::signal::ctrl_c() => {
            if let Err(error) = result {
                tracing::error!(%error, "cannot listen for Ctrl-C");
                return;
            }
        }
    }
    tracing::info!("shutdown requested");
    shutdown.cancel();
}
