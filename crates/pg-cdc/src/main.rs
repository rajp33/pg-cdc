use std::io::Write as _;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use anyhow::Context as _;
use clap::{Parser, Subcommand};
use hyper::{Method, StatusCode};
use pg_cdc::admin::{self, Admin, HEALTH_TIMEOUT, Identity, PARKED_TIMEOUT, Reply};
use pg_cdc::config::{
    Config, Credentials, DEFAULT_ADMIN_LISTEN, PayloadStoreConfig, SinkConfig, SourceConfig,
    StateStoreConfig,
};
use pg_cdc::connection::{
    ConnectParams, ConnectionProvider, PasswordProvider, RdsIamProvider, TlsConfig,
};
use pg_cdc::event::EventId;
use pg_cdc::health::Health;
use pg_cdc::lane::Stores;
use pg_cdc::leader::record::holder_id;
use pg_cdc::logs::{self, LOG_BUFFER_LINES, LogFlush};
use pg_cdc::order::OrderKeyId;
use pg_cdc::payload::{PayloadStore, S3PayloadStore};
use pg_cdc::session::{REPLICATION_BUFFER, SessionConfig, SessionContext, shutdown_budget};
use pg_cdc::sink::Sink;
use pg_cdc::sink::discard::DiscardSink;
use pg_cdc::sink::sns::SnsSink;
use pg_cdc::sink::stdout::StdoutSink;
use pg_cdc::state::StateStore;
use pg_cdc::state::filesystem::FilesystemStateStore;
use pg_cdc::state::instrumented::InstrumentedStateStore;
use pg_cdc::state::s3::{S3StateStore, s3_client};
use pg_cdc::status::Status;
use pg_cdc::supervisor::{LiveSession, preflight, store_ready, supervise};
use pg_cdc::telemetry::{
    ErrorReporting, OTLP_ENDPOINT_VARIABLE, REPORTING_FLUSH_TIMEOUT, SENTRY_DSN_VARIABLE,
    Telemetry, TelemetryIdentity, TelemetryLayers, TelemetryPlan, plan, reporting_plan, subscriber,
};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio::signal::unix::{Signal, SignalKind, signal};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::util::SubscriberInitExt as _;

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
    Parked {
        #[arg(long, global = true)]
        addr: Option<SocketAddr>,
        #[command(subcommand)]
        action: ParkedCommand,
    },
}

#[derive(Debug, Subcommand)]
enum ParkedCommand {
    List {
        #[arg(long)]
        status: Option<ParkedStatusArg>,
        #[arg(long)]
        limit: Option<u16>,
        #[arg(long)]
        after: Option<OrderKeyId>,
    },
    Show {
        order_key: OrderKeyId,
        #[arg(long)]
        after: Option<EventId>,
        #[arg(long)]
        limit: Option<u16>,
    },
    Redrive {
        order_key: OrderKeyId,
    },
    Discard {
        order_key: OrderKeyId,
        #[arg(long)]
        id: EventId,
    },
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum ParkedStatusArg {
    Dead,
    Retrying,
}

const ADMIN_LISTEN_VARIABLE: &str = "PGCDC__ADMIN__LISTEN";
const LOG_FLUSH_TIMEOUT: Duration = Duration::from_secs(1);
const OTLP_PROTOCOL_VARIABLE: &str = "OTEL_EXPORTER_OTLP_PROTOCOL";

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
    let (code, logs) = match cli.command {
        Command::Run { config } => run_command(&runtime, &config),
        Command::Health { addr } => (runtime.block_on(health_command(addr)), None),
        Command::Parked { addr, action } => (runtime.block_on(parked_command(addr, &action)), None),
    };
    // Dropping the runtime waits for blocking-pool work, and a write to a full stdout pipe never returns.
    runtime.shutdown_background();
    if let Some(logs) = logs {
        let window = flush_window(logs.deadline, Instant::now());
        if !window.is_zero() {
            logs.flush.flush(window);
        }
    }
    code
}

#[derive(Debug)]
struct PendingLogs {
    flush: LogFlush,
    deadline: Option<Instant>,
}

fn flush_deadline(signalled: Option<Instant>, drain_timeout: Duration) -> Option<Instant> {
    signalled?.checked_add(shutdown_budget(drain_timeout))
}

fn flush_window(deadline: Option<Instant>, now: Instant) -> Duration {
    within_budget(LOG_FLUSH_TIMEOUT, deadline, now)
}

fn reporting_window(deadline: Option<Instant>, now: Instant) -> Duration {
    within_budget(REPORTING_FLUSH_TIMEOUT, deadline, now)
}

fn within_budget(limit: Duration, deadline: Option<Instant>, now: Instant) -> Duration {
    deadline.map_or(limit, |deadline| {
        limit.min(deadline.saturating_duration_since(now))
    })
}

async fn health_command(flag: Option<SocketAddr>) -> ExitCode {
    let env = std::env::var(ADMIN_LISTEN_VARIABLE).ok();
    let failure = match admin_addr(flag, env.as_deref()) {
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

fn parked_request(action: &ParkedCommand) -> (Method, String) {
    match action {
        ParkedCommand::List {
            status,
            limit,
            after,
        } => {
            let status = status.map(|status| match status {
                ParkedStatusArg::Dead => "dead".to_owned(),
                ParkedStatusArg::Retrying => "retrying".to_owned(),
            });
            let query = [
                ("status", status),
                ("limit", limit.map(|limit| limit.to_string())),
                ("after", after.map(|after| after.to_string())),
            ];
            (Method::GET, with_query("/parked".to_owned(), &query))
        }
        ParkedCommand::Show {
            order_key,
            after,
            limit,
        } => {
            let query = [
                ("after", after.map(|after| after.to_string())),
                ("limit", limit.map(|limit| limit.to_string())),
            ];
            (
                Method::GET,
                with_query(format!("/parked/{order_key}"), &query),
            )
        }
        ParkedCommand::Redrive { order_key } => {
            (Method::POST, format!("/parked/{order_key}/redrive"))
        }
        ParkedCommand::Discard { order_key, id } => {
            (Method::POST, format!("/parked/{order_key}/discard?id={id}"))
        }
    }
}

fn with_query(path: String, query: &[(&str, Option<String>)]) -> String {
    let pairs: Vec<String> = query
        .iter()
        .filter_map(|(name, value)| Some(format!("{name}={}", value.as_ref()?)))
        .collect();
    if pairs.is_empty() {
        path
    } else {
        format!("{path}?{}", pairs.join("&"))
    }
}

fn parked_output(reply: &Reply) -> Result<Vec<u8>, String> {
    let json = std::str::from_utf8(&reply.body)
        .ok()
        .and_then(pg_cdc_decoder::value::parse_json);
    if (200..300).contains(&reply.status) {
        return Ok(
            match json.and_then(|json| serde_json::to_string_pretty(&json).ok()) {
                Some(mut pretty) => {
                    pretty.push('\n');
                    pretty.into_bytes()
                }
                None => reply.body.to_vec(),
            },
        );
    }
    let field = |name: &str| {
        json.as_ref()
            .and_then(|json| json.get(name))
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
    };
    let detail = field("error")
        .unwrap_or_else(|| String::from_utf8_lossy(&reply.body).trim_end().to_owned());
    let holder = field("holder")
        .filter(|_| reply.status == StatusCode::CONFLICT.as_u16())
        .map(|holder| format!(" (active holder: {holder})"))
        .unwrap_or_default();
    Err(
        match StatusCode::from_u16(reply.status)
            .ok()
            .and_then(|status| status.canonical_reason())
        {
            Some(reason) => format!("{} {reason}: {detail}{holder}", reply.status),
            None => format!("{}: {detail}{holder}", reply.status),
        },
    )
}

async fn parked_command(flag: Option<SocketAddr>, action: &ParkedCommand) -> ExitCode {
    let env = std::env::var(ADMIN_LISTEN_VARIABLE).ok();
    let (method, target) = parked_request(action);
    let reply = match admin_addr(flag, env.as_deref()) {
        Err(error) => Err(error),
        Ok(addr) => admin::request(addr, method, &target, PARKED_TIMEOUT)
            .await
            .map_err(|error| error.to_string()),
    };
    let failure = match reply {
        Err(error) => format!("parked request failed: {error}"),
        Ok(reply) => match parked_output(&reply) {
            Ok(output) => match std::io::stdout().write_all(&output) {
                Ok(()) => return ExitCode::SUCCESS,
                Err(error) => format!("cannot write the reply: {error}"),
            },
            Err(refusal) => refusal,
        },
    };
    let _ = writeln!(std::io::stderr(), "pg-cdc: {failure}");
    ExitCode::FAILURE
}

fn admin_addr(flag: Option<SocketAddr>, env: Option<&str>) -> Result<SocketAddr, String> {
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

fn run_command(runtime: &Runtime, config: &Path) -> (ExitCode, Option<PendingLogs>) {
    let env = std::env::vars_os()
        .filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?)));
    let config = match pg_cdc::config::load(config, env) {
        Ok(config) => config,
        Err(error) => {
            let _ = writeln!(std::io::stderr(), "pg-cdc: invalid configuration: {error}");
            return (ExitCode::FAILURE, None);
        }
    };
    let holder = holder_id();
    let status = Status::new();
    let health = Health::new();
    let plan = plan(
        &config.telemetry,
        std::env::var(OTLP_ENDPOINT_VARIABLE).ok().as_deref(),
    );
    let _entered = runtime.enter();
    let identity = TelemetryIdentity {
        pipeline: config.pipeline.clone(),
        holder: holder.clone(),
    };
    let (telemetry, layers) = match Telemetry::install(&plan, &identity, &status, &health) {
        Ok(installed) => installed,
        Err(error) => {
            let _ = writeln!(std::io::stderr(), "pg-cdc: cannot start telemetry: {error}");
            return (ExitCode::FAILURE, None);
        }
    };
    let reporting_plan = reporting_plan(
        &config.telemetry.sentry,
        std::env::var(SENTRY_DSN_VARIABLE).ok().as_deref(),
    );
    let (reporting, reporting_layers) =
        match ErrorReporting::install(reporting_plan.as_ref(), &identity) {
            Ok(installed) => installed,
            Err(error) => {
                let _ = writeln!(std::io::stderr(), "pg-cdc: cannot start telemetry: {error}");
                return (ExitCode::FAILURE, None);
            }
        };
    let layers = TelemetryLayers::combine(layers, reporting_layers);
    let logs = match init_logging(&config.sink, layers) {
        Ok(logs) => logs,
        Err(error) => {
            let _ = writeln!(std::io::stderr(), "pg-cdc: cannot start logging: {error}");
            return (ExitCode::FAILURE, None);
        }
    };
    warn_when_telemetry_is_degraded(&plan, &telemetry);
    if reporting_plan.is_some() && !reporting.is_enabled() {
        tracing::warn!("error reporting is configured, but this build has no sentry feature");
    }
    let drain_timeout = config.leadership.drain_timeout;
    let signalled = Arc::new(OnceLock::new());
    let board = Board {
        holder,
        status,
        health,
    };
    let code = runtime.block_on(async {
        match run(config, Arc::clone(&signalled), board).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                tracing::error!(error = format!("{error:#}"), "pg-cdc stopped");
                ExitCode::FAILURE
            }
        }
    });
    let deadline = flush_deadline(signalled.get().copied(), drain_timeout);
    if !telemetry.shutdown(flush_window(deadline, Instant::now())) {
        tracing::warn!("telemetry did not finish exporting within the shutdown budget");
    }
    if !reporting.shutdown(reporting_window(deadline, Instant::now())) {
        tracing::warn!("error reports did not finish sending within the shutdown budget");
    }
    (
        code,
        Some(PendingLogs {
            flush: logs,
            deadline,
        }),
    )
}

fn warn_when_telemetry_is_degraded(plan: &TelemetryPlan, telemetry: &Telemetry) {
    if matches!(plan, TelemetryPlan::Enabled { .. }) && !telemetry.is_enabled() {
        tracing::warn!("telemetry is configured, but this build has no otel feature");
    }
    if telemetry.is_enabled()
        && let Ok(protocol) = std::env::var(OTLP_PROTOCOL_VARIABLE)
        && protocol != "grpc"
    {
        tracing::warn!(
            %protocol,
            "OTEL_EXPORTER_OTLP_PROTOCOL is not grpc; only OTLP over gRPC is supported, so grpc is used"
        );
    }
}

fn init_logging(sink: &SinkConfig, layers: Option<TelemetryLayers>) -> std::io::Result<LogFlush> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let (writer, flush) = match sink {
        SinkConfig::Stdout(_) => logs::non_blocking(std::io::stderr(), LOG_BUFFER_LINES)?,
        SinkConfig::Discard(_) | SinkConfig::Sns(_) => {
            logs::non_blocking(std::io::stdout(), LOG_BUFFER_LINES)?
        }
    };
    subscriber(writer, filter, layers)
        .try_init()
        .map_err(std::io::Error::other)?;
    Ok(flush)
}

#[derive(Debug)]
struct Board {
    holder: String,
    status: Status,
    health: Health,
}

async fn run(
    config: Config,
    signalled: Arc<OnceLock<Instant>>,
    board: Board,
) -> anyhow::Result<()> {
    let shutdown = CancellationToken::new();
    let terminate = signal(SignalKind::terminate()).context("cannot listen for SIGTERM")?;
    let interrupt = signal(SignalKind::interrupt()).context("cannot listen for SIGINT")?;
    tokio::spawn(watch_signals(
        terminate,
        interrupt,
        signalled,
        shutdown.clone(),
    ));
    let Board {
        holder,
        status,
        health,
    } = board;
    tracing::info!(pipeline = %config.pipeline, slot = %config.source.slot, %holder, "pg-cdc starting");
    let admin = Admin::new(
        health.clone(),
        status.clone(),
        Identity {
            pipeline: config.pipeline.clone(),
            slot: config.source.slot.clone(),
            holder: holder.clone(),
        },
    );
    start_admin(config.admin.listen, admin.clone()).await?;
    let Some(provider) = shutdown
        .run_until_cancelled(Box::pin(provider(&config.source)))
        .await
    else {
        tracing::info!("pg-cdc stopped cleanly");
        return Ok(());
    };
    let Some(sink) = shutdown
        .run_until_cancelled(Box::pin(sink(&config.pipeline, &config.sink)))
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
    admin.set_state_store(Arc::new(InstrumentedStateStore::new(Arc::clone(
        &stores.state,
    ))));
    if !preflight(sink.as_ref(), &shutdown).await {
        tracing::info!("pg-cdc stopped cleanly");
        return Ok(());
    }
    health.set_sink_ready(true);
    if !store_ready(&stores.state, &shutdown).await {
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
    let context = SessionContext {
        holder,
        health,
        status,
    };
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

async fn start_admin(listen: SocketAddr, admin: Admin) -> anyhow::Result<()> {
    let listener = TcpListener::bind(listen)
        .await
        .with_context(|| format!("cannot bind admin.listen {listen}"))?;
    let addr = listener
        .local_addr()
        .context("cannot read the admin address")?;
    tracing::info!(%addr, "admin listening");
    tokio::spawn(admin::serve(listener, admin));
    Ok(())
}

async fn sink(pipeline: &str, config: &SinkConfig) -> Arc<dyn Sink> {
    match config {
        SinkConfig::Stdout(_) => Arc::new(StdoutSink::stdout()),
        SinkConfig::Discard(_) => Arc::new(DiscardSink::new()),
        SinkConfig::Sns(sns) => Arc::new(
            SnsSink::from_environment(
                pipeline,
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

async fn watch_signals(
    mut terminate: Signal,
    mut interrupt: Signal,
    signalled: Arc<OnceLock<Instant>>,
    shutdown: CancellationToken,
) {
    tokio::select! {
        _ = terminate.recv() => {}
        _ = interrupt.recv() => {}
    }
    let _ = signalled.set(Instant::now());
    shutdown.cancel();
    tracing::info!("shutdown requested");
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn logs_flush_for_a_second_unless_the_sigterm_budget_has_less_left() {
        let now = Instant::now();
        assert_eq!(flush_window(None, now), LOG_FLUSH_TIMEOUT);
        assert_eq!(
            flush_window(Some(now + Duration::from_secs(10)), now),
            LOG_FLUSH_TIMEOUT
        );
        assert_eq!(
            flush_window(Some(now + Duration::from_millis(300)), now),
            Duration::from_millis(300)
        );
        assert_eq!(flush_window(Some(now), now), Duration::ZERO);
        assert_eq!(
            flush_window(Some(now), now + Duration::from_secs(1)),
            Duration::ZERO
        );
    }

    #[test]
    fn error_reports_flush_for_two_seconds_unless_the_sigterm_budget_has_less_left() {
        let now = Instant::now();
        assert_eq!(REPORTING_FLUSH_TIMEOUT, Duration::from_secs(2));
        assert_eq!(reporting_window(None, now), REPORTING_FLUSH_TIMEOUT);
        assert_eq!(
            reporting_window(Some(now + Duration::from_secs(10)), now),
            REPORTING_FLUSH_TIMEOUT
        );
        assert_eq!(
            reporting_window(Some(now + Duration::from_millis(1500)), now),
            Duration::from_millis(1500)
        );
        assert_eq!(
            reporting_window(Some(now), now + Duration::from_secs(1)),
            Duration::ZERO
        );
    }

    #[test]
    fn the_flush_deadline_is_the_shutdown_budget_after_the_signal() {
        let signalled = Instant::now();
        let drain = Duration::from_secs(1);
        assert_eq!(flush_deadline(None, drain), None);
        assert_eq!(
            flush_deadline(Some(signalled), drain),
            Some(signalled + Duration::from_secs(5))
        );
        assert_eq!(flush_deadline(Some(signalled), Duration::MAX), None);
    }

    #[test]
    fn admin_address_comes_from_the_flag_then_the_environment_then_the_default() {
        let flag: SocketAddr = "10.0.0.1:1".parse().unwrap();
        let env: SocketAddr = "10.0.0.2:2".parse().unwrap();
        assert_eq!(admin_addr(Some(flag), Some("10.0.0.2:2")), Ok(flag));
        assert_eq!(admin_addr(None, Some("10.0.0.2:2")), Ok(env));
        assert_eq!(admin_addr(None, None), Ok(DEFAULT_ADMIN_LISTEN));
        assert_eq!(admin_addr(None, Some("")), Ok(DEFAULT_ADMIN_LISTEN));
        let error = admin_addr(None, Some("not-an-address")).unwrap_err();
        assert!(error.contains("PGCDC__ADMIN__LISTEN"), "{error}");
        assert_eq!(admin_addr(Some(flag), Some("not-an-address")), Ok(flag));
    }

    const KEY: &str = "0123456789abcdef0123456789abcdef";
    const EVENT: &str = "000000001a2b3c4d-00000003";

    fn mapped(arguments: &[&str]) -> (Method, String) {
        let cli = Cli::try_parse_from(
            ["pg-cdc", "parked"]
                .into_iter()
                .chain(arguments.iter().copied()),
        )
        .unwrap();
        let Command::Parked { action, .. } = cli.command else {
            panic!("not a parked command");
        };
        parked_request(&action)
    }

    #[test]
    fn parked_commands_map_to_their_admin_requests() {
        assert_eq!(mapped(&["list"]), (Method::GET, "/parked".to_owned()));
        assert_eq!(
            mapped(&["list", "--status", "dead", "--limit", "5", "--after", KEY]),
            (
                Method::GET,
                format!("/parked?status=dead&limit=5&after={KEY}")
            )
        );
        assert_eq!(
            mapped(&["list", "--status", "retrying"]),
            (Method::GET, "/parked?status=retrying".to_owned())
        );
        assert_eq!(
            mapped(&["show", KEY]),
            (Method::GET, format!("/parked/{KEY}"))
        );
        assert_eq!(
            mapped(&["show", KEY, "--after", EVENT, "--limit", "2"]),
            (Method::GET, format!("/parked/{KEY}?after={EVENT}&limit=2"))
        );
        assert_eq!(
            mapped(&["redrive", KEY]),
            (Method::POST, format!("/parked/{KEY}/redrive"))
        );
        assert_eq!(
            mapped(&["discard", KEY, "--id", EVENT]),
            (Method::POST, format!("/parked/{KEY}/discard?id={EVENT}"))
        );
    }

    #[test]
    fn the_address_flag_may_follow_the_parked_action() {
        let cli =
            Cli::try_parse_from(["pg-cdc", "parked", "list", "--addr", "10.0.0.1:1"]).unwrap();
        let Command::Parked { addr, .. } = cli.command else {
            panic!("not a parked command");
        };
        assert_eq!(addr, Some("10.0.0.1:1".parse().unwrap()));
    }

    #[test]
    fn malformed_ids_are_rejected_by_the_parser() {
        for arguments in [
            vec!["show", "nothex"],
            vec!["redrive", "nothex"],
            vec!["discard", KEY, "--id", "bad"],
            vec!["list", "--after", "bad"],
            vec!["list", "--status", "alive"],
            vec!["show", KEY, "--after", "bad"],
        ] {
            let error =
                Cli::try_parse_from(["pg-cdc", "parked"].into_iter().chain(arguments)).unwrap_err();
            assert_eq!(error.exit_code(), 2, "{error}");
        }
    }

    fn reply(status: u16, body: &str) -> Reply {
        Reply {
            status,
            body: Bytes::copy_from_slice(body.as_bytes()),
        }
    }

    #[test]
    fn a_successful_reply_is_pretty_printed_and_a_non_json_body_is_kept() {
        assert_eq!(
            parked_output(&reply(200, r#"{"queues":[],"next":null}"#)),
            Ok(b"{\n  \"queues\": [],\n  \"next\": null\n}\n".to_vec())
        );
        assert_eq!(
            parked_output(&reply(202, "accepted\n")),
            Ok(b"accepted\n".to_vec())
        );
    }

    #[test]
    fn a_pretty_printed_event_keeps_objects_named_like_serde_json_internals() {
        let body = r#"{"events":[{"after":{"a":{"$serde_json::private::Number":"123"},"b":[{"$serde_json::private::RawValue":"[1]"}],"n":1.50}}]}"#;
        let printed = parked_output(&reply(200, body)).unwrap();
        let printed = String::from_utf8(printed).unwrap();
        let compact: String = printed.split_whitespace().collect();
        assert_eq!(compact, body);
    }

    #[test]
    fn a_refusal_names_the_status_the_error_and_the_active_holder() {
        assert_eq!(
            parked_output(&reply(
                409,
                r#"{"error":"this task is not the active holder","holder":"other-1"}"#
            )),
            Err(
                "409 Conflict: this task is not the active holder (active holder: other-1)"
                    .to_owned()
            )
        );
        assert_eq!(
            parked_output(&reply(400, r#"{"error":"limit must be a number"}"#)),
            Err("400 Bad Request: limit must be a number".to_owned())
        );
        assert_eq!(
            parked_output(&reply(503, "unavailable\n")),
            Err("503 Service Unavailable: unavailable".to_owned())
        );
        assert_eq!(
            parked_output(&reply(
                503,
                r#"{"error":"too many redrive and discard requests are waiting on the lane of order key 6b; try again shortly"}"#
            )),
            Err("503 Service Unavailable: too many redrive and discard requests are waiting on the lane of order key 6b; try again shortly".to_owned())
        );
        assert_eq!(
            parked_output(&reply(500, r#"{"holder":"other-1"}"#)),
            Err(r#"500 Internal Server Error: {"holder":"other-1"}"#.to_owned())
        );
    }
}
