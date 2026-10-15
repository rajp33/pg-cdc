use std::io::Write as _;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use anyhow::Context as _;
use clap::{Parser, Subcommand};
use pg_cdc::admin::{self, HEALTH_TIMEOUT};
use pg_cdc::config::{
    Config, Credentials, DEFAULT_ADMIN_LISTEN, PayloadStoreConfig, SinkConfig, SourceConfig,
    StateStoreConfig,
};
use pg_cdc::connection::{
    ConnectParams, ConnectionProvider, PasswordProvider, RdsIamProvider, TlsConfig,
};
use pg_cdc::health::Health;
use pg_cdc::lane::Stores;
use pg_cdc::leader::record::holder_id;
use pg_cdc::payload::{PayloadStore, S3PayloadStore};
use pg_cdc::session::{REPLICATION_BUFFER, SessionConfig, SessionContext};
use pg_cdc::sink::Sink;
use pg_cdc::sink::discard::DiscardSink;
use pg_cdc::sink::sns::SnsSink;
use pg_cdc::sink::stdout::StdoutSink;
use pg_cdc::state::StateStore;
use pg_cdc::state::filesystem::FilesystemStateStore;
use pg_cdc::state::s3::{S3StateStore, s3_client};
use pg_cdc::supervisor::{LiveSession, preflight, store_ready, supervise};
use tokio::net::TcpListener;
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
    Health {
        #[arg(long)]
        addr: Option<SocketAddr>,
    },
}

const ADMIN_LISTEN_VARIABLE: &str = "PGCDC__ADMIN__LISTEN";

fn main() -> ExitCode {
    let cli = Cli::parse();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            let _ = writeln!(
                std::io::stderr(),
                "pg-cdc: cannot start the runtime: {error}"
            );
            return ExitCode::FAILURE;
        }
    };
    let code = runtime.block_on(async {
        match cli.command {
            Command::Run { config } => run_command(&config).await,
            Command::Health { addr } => health_command(addr).await,
        }
    });
    // Dropping the runtime waits for blocking-pool work, and a write to a full stdout pipe never returns.
    runtime.shutdown_background();
    code
}

async fn health_command(flag: Option<SocketAddr>) -> ExitCode {
    let env = std::env::var(ADMIN_LISTEN_VARIABLE).ok();
    let failure = match health_addr(flag, env.as_deref()) {
        Err(error) => format!("health check failed: {error}"),
        Ok(addr) => match admin::check_health(addr, HEALTH_TIMEOUT).await {
            Ok(200) => return ExitCode::SUCCESS,
            Ok(status) => format!("unhealthy: /healthz returned {status}"),
            Err(error) => format!("health check failed: {error}"),
        },
    };
    let _ = writeln!(std::io::stderr(), "pg-cdc: {failure}");
    ExitCode::FAILURE
}

fn health_addr(flag: Option<SocketAddr>, env: Option<&str>) -> Result<SocketAddr, String> {
    if let Some(addr) = flag {
        return Ok(addr);
    }
    match env.filter(|value| !value.is_empty()) {
        Some(value) => value.parse().map_err(|error| {
            format!("{ADMIN_LISTEN_VARIABLE} {value:?} is not an address: {error}")
        }),
        None => Ok(DEFAULT_ADMIN_LISTEN),
    }
}

async fn run_command(config: &Path) -> ExitCode {
    let env = std::env::vars_os()
        .filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?)));
    let config = match pg_cdc::config::load(config, env) {
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
        SinkConfig::Stdout(_) => builder.with_writer(std::io::stderr).init(),
        SinkConfig::Discard(_) | SinkConfig::Sns(_) => builder.with_writer(std::io::stdout).init(),
    }
}

async fn run(config: Config) -> anyhow::Result<()> {
    let shutdown = CancellationToken::new();
    let terminate = signal(SignalKind::terminate()).context("cannot listen for SIGTERM")?;
    let interrupt = signal(SignalKind::interrupt()).context("cannot listen for SIGINT")?;
    tokio::spawn(watch_signals(terminate, interrupt, shutdown.clone()));
    let holder = holder_id();
    tracing::info!(pipeline = %config.pipeline, slot = %config.source.slot, %holder, "pg-cdc starting");
    let health = start_admin(config.admin.listen).await?;
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
    let Some(stores) = shutdown
        .run_until_cancelled(Box::pin(stores(&config)))
        .await
    else {
        tracing::info!("pg-cdc stopped cleanly");
        return Ok(());
    };
    if !preflight(sink.as_ref(), &shutdown).await {
        tracing::info!("pg-cdc stopped cleanly");
        return Ok(());
    }
    health.set_sink_ready(true);
    if !store_ready(stores.state.as_ref(), &shutdown).await {
        tracing::info!("pg-cdc stopped cleanly");
        return Ok(());
    }
    health.set_store_ready(true);
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
        park_after: config.delivery.park_after,
        max_parked_events: config.delivery.max_parked_events,
        store_concurrency: config.state_store.max_concurrency(),
        leadership: config.leadership,
    };
    let context = SessionContext { holder, health };
    let mut live = LiveSession {
        config: &session,
        context: &context,
        provider: provider.as_ref(),
        sink: &sink,
        stores: &stores,
        shutdown: &shutdown,
    };
    supervise(&mut live, &shutdown)
        .await
        .context("replication stopped")?;
    tracing::info!("pg-cdc stopped cleanly");
    Ok(())
}

async fn start_admin(listen: SocketAddr) -> anyhow::Result<Health> {
    let listener = TcpListener::bind(listen)
        .await
        .with_context(|| format!("cannot bind admin.listen {listen}"))?;
    let addr = listener
        .local_addr()
        .context("cannot read the admin address")?;
    tracing::info!(%addr, "admin listening");
    let health = Health::new();
    tokio::spawn(admin::serve(listener, health.clone()));
    Ok(health)
}

async fn sink(config: &SinkConfig) -> Arc<dyn Sink> {
    match config {
        SinkConfig::Stdout(_) => Arc::new(StdoutSink::stdout()),
        SinkConfig::Discard(_) => Arc::new(DiscardSink::new()),
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

async fn stores(config: &Config) -> Stores {
    let state: Arc<dyn StateStore> = match &config.state_store {
        StateStoreConfig::S3(s3) => Arc::new(S3StateStore::new(
            s3_client(s3.region.clone()).await,
            s3.bucket.clone(),
            &s3.prefix,
            &config.pipeline,
            s3.max_concurrency,
        )),
        StateStoreConfig::Filesystem(filesystem) => Arc::new(FilesystemStateStore::new(
            &filesystem.path,
            &config.pipeline,
            filesystem.max_concurrency,
        )),
    };
    let payload: Option<Arc<dyn PayloadStore>> = match &config.payload_store {
        Some(PayloadStoreConfig::S3(s3)) => Some(Arc::new(S3PayloadStore::new(
            s3_client(s3.region.clone()).await,
            s3.bucket.clone(),
            &s3.prefix,
            &config.pipeline,
        ))),
        None => None,
    };
    Stores { state, payload }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_address_comes_from_the_flag_then_the_environment_then_the_default() {
        let flag: SocketAddr = "10.0.0.1:1".parse().unwrap();
        let env: SocketAddr = "10.0.0.2:2".parse().unwrap();
        assert_eq!(health_addr(Some(flag), Some("10.0.0.2:2")), Ok(flag));
        assert_eq!(health_addr(None, Some("10.0.0.2:2")), Ok(env));
        assert_eq!(health_addr(None, None), Ok(DEFAULT_ADMIN_LISTEN));
        assert_eq!(health_addr(None, Some("")), Ok(DEFAULT_ADMIN_LISTEN));
        let error = health_addr(None, Some("not-an-address")).unwrap_err();
        assert!(error.contains("PGCDC__ADMIN__LISTEN"), "{error}");
        assert_eq!(health_addr(Some(flag), Some("not-an-address")), Ok(flag));
    }
}
