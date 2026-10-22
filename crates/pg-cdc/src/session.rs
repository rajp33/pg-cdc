use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use pg_cdc_decoder::Lsn;
use pg_cdc_decoder::message::{DecodeError, LogicalMessage, Relation, decode};
use pg_cdc_replication::{
    ReplicationConfig, ReplicationError, ReplicationEvent, ReplicationStream, verify_login,
};
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinError;
use tokio::time::{Instant, MissedTickBehavior, interval, timeout, timeout_at};
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;
use tracing::{Instrument, Span};

use crate::ack::{AckError, AckTracker};
use crate::catalog::{self, CatalogError, TypeResolver};
use crate::config::LeadershipConfig;
use crate::connection::{
    CONNECT_TIMEOUT, ConnectError, ConnectParams, ConnectionProvider, ControlConnection,
    SESSION_OPTIONS, connect_control, replication_tls,
};
use crate::event::{BuildError, Builder, Built, ColumnInfo, RelationInfo};
use crate::health::{Health, Role};
use crate::lane::{LaneError, LaneSettings, LaneStopped, Lanes, Pending, Report, Stores};
use crate::leader::gate::{WriteGate, gated_sink, gated_stores};
use crate::leader::lease::{FenceReason, hold_lease};
use crate::leader::record::{Claim, Release, TakeoverError, release, take_over};
use crate::leader::standby::{
    AcquireError, OBJECT_IN_USE, ProbeError, SlotHolder, SlotProbe, StartError, acquire,
};
use crate::metrics::{self, ErrorComponent, ErrorKind, EventOutcome, Measurement};
use crate::order::{
    OrderBy, OrderKeyId, TableName, missing_order_column, order_key, table_order_by, table_override,
};
use crate::sink::{Outbound, Sink, metric_labels};
use crate::state::StateStore;
use crate::state::instrumented::instrumented;
use crate::status::{ParkedAccess, Status};
use crate::validate::{Invalid, ValidationError, judge_order_overrides, validate};

pub const REPLICATION_BUFFER: usize = 64;
pub const HANDOVER_TAIL: Duration = Duration::from_secs(3);
pub const LANE_STOP_GRACE: Duration = Duration::from_secs(1);
pub const CONTROL_QUERY_TIMEOUT: Duration = Duration::from_secs(10);
pub const SLOT_METRICS_INTERVAL: Duration = Duration::from_secs(10);

pub const POSITION_LOG_INTERVAL: Duration = Duration::from_mins(1);
const POSTGRES_EPOCH_UNIX_MS: i64 = 946_684_800_000;

const INSUFFICIENT_PRIVILEGE: &str = "42501";

#[must_use]
pub fn shutdown_budget(drain_timeout: Duration) -> Duration {
    drain_timeout
        .saturating_mul(2)
        .saturating_add(HANDOVER_TAIL)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionConfig {
    pub pipeline: String,
    pub slot: String,
    pub publication: String,
    pub status_interval: Duration,
    pub buffer: usize,
    pub order_by: OrderBy,
    pub order_by_tables: BTreeMap<TableName, OrderBy>,
    pub lanes: usize,
    pub sink_concurrency: usize,
    pub attribute_columns: Vec<String>,
    pub max_inflight_bytes: u64,
    pub park_after: Duration,
    pub max_parked_events: u64,
    pub store_concurrency: usize,
    pub leadership: LeadershipConfig,
}

#[derive(Debug, Clone)]
pub struct SessionContext {
    pub holder: String,
    pub health: Health,
    pub status: Status,
}

impl SessionConfig {
    pub fn lane_settings(&self) -> LaneSettings {
        LaneSettings {
            lanes: self.lanes,
            park_after: self.park_after,
            max_parked_events: self.max_parked_events,
            store_concurrency: self.store_concurrency,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error(transparent)]
    Connect(#[from] ConnectError),
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error(transparent)]
    Invalid(#[from] Invalid),
    #[error(transparent)]
    Probe(#[from] ProbeError),
    #[error("the control connection did not answer within {0:?}")]
    ControlStalled(Duration),
    #[error("cannot take over the slot: {0}")]
    Takeover(#[from] TakeoverError),
    #[error("lost the slot and fenced: {0}")]
    Fenced(FenceReason),
    #[error("replication stream failed: {0}")]
    Replication(#[from] ReplicationError),
    #[error("replication stream ended")]
    StreamEnded,
    #[error("malformed replication stream after {at}: {source}")]
    Malformed { at: Lsn, source: ReplicationError },
    #[error("malformed message at {wal_start}: {source}")]
    Decode { wal_start: Lsn, source: DecodeError },
    #[error("cannot build an event at {wal_start}: {source}")]
    Build { wal_start: Lsn, source: BuildError },
    #[error("relation {oid} at {wal_start} has no Relation message on this connection")]
    UnknownRelation { wal_start: Lsn, oid: u32 },
    #[error("acknowledgement invariant violated: {0}")]
    Ack(#[from] AckError),
    #[error("ordering invariant violated: {0}")]
    Lane(#[from] LaneError),
    #[error(transparent)]
    LaneStopped(#[from] LaneStopped),
    #[error("every delivery lane has stopped")]
    LanesStopped,
    #[error("cannot encode an event: {0}")]
    Encode(#[from] serde_json::Error),
}

impl SessionError {
    pub fn fault_lsn(&self) -> Option<Lsn> {
        match self {
            Self::Decode { wal_start, .. }
            | Self::Build { wal_start, .. }
            | Self::UnknownRelation { wal_start, .. } => Some(*wal_start),
            Self::Malformed { at, .. } => Some(*at),
            _ => None,
        }
    }

    pub fn component(&self) -> ErrorComponent {
        match self {
            Self::Connect(_)
            | Self::Catalog(_)
            | Self::Invalid(_)
            | Self::ControlStalled(_)
            | Self::Replication(_)
            | Self::StreamEnded => ErrorComponent::Source,
            Self::Malformed { .. }
            | Self::Decode { .. }
            | Self::Build { .. }
            | Self::UnknownRelation { .. }
            | Self::Encode(_) => ErrorComponent::Decoder,
            Self::Probe(_) | Self::Takeover(_) | Self::Fenced(_) => ErrorComponent::Leader,
            Self::Ack(_) | Self::Lane(_) | Self::LaneStopped(_) | Self::LanesStopped => {
                ErrorComponent::Delivery
            }
        }
    }

    pub fn kind(&self) -> ErrorKind {
        if self.is_fatal() {
            return ErrorKind::Terminal;
        }
        match self {
            Self::ControlStalled(_) => ErrorKind::Timeout,
            Self::Malformed { .. }
            | Self::Decode { .. }
            | Self::Build { .. }
            | Self::UnknownRelation { .. } => ErrorKind::Malformed,
            _ => ErrorKind::Retryable,
        }
    }

    pub fn is_fatal(&self) -> bool {
        matches!(
            self,
            Self::Ack(_)
                | Self::Takeover(TakeoverError::EpochExhausted)
                | Self::Encode(_)
                | Self::Lane(_)
                | Self::LaneStopped(_)
                | Self::LanesStopped
        )
    }
}

pub async fn run_session(
    config: &SessionConfig,
    provider: &dyn ConnectionProvider,
    sink: &Arc<dyn Sink>,
    stores: &Stores,
    context: &SessionContext,
    shutdown: &CancellationToken,
) -> Result<(), SessionError> {
    let result = attempt(config, provider, sink, stores, context, shutdown).await;
    clear_status(&context.status);
    context.health.set_delivering(false);
    context.health.set_lease_valid(false);
    context.health.set_role(Role::Standby);
    result
}

fn clear_status(status: &Status) {
    status.set_parked_access(None);
    status.set_epoch(None);
    status.set_confirmed(None);
    status.set_inflight(0, 0);
    status.set_status_clock(None);
    status.set_parked(None);
    status.set_slot_retained_bytes(None);
}

fn spawn_slot_poller(
    control: &Arc<ControlConnection>,
    slot: &str,
    status: &Status,
) -> AbortOnDropHandle<()> {
    let control = Arc::clone(control);
    let slot = slot.to_owned();
    let status = status.clone();
    AbortOnDropHandle::new(tokio::spawn(async move {
        let mut ticker = interval(SLOT_METRICS_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let retained = match timeout(
                CONTROL_QUERY_TIMEOUT,
                catalog::slot_retained_bytes(control.client(), &slot),
            )
            .await
            {
                Ok(Ok(bytes)) => bytes,
                Ok(Err(error)) => {
                    tracing::debug!(%error, "could not read the slot's retained WAL");
                    None
                }
                Err(_) => {
                    tracing::debug!("the slot's retained WAL query timed out");
                    None
                }
            };
            status.set_slot_retained_bytes(retained);
        }
    }))
}

async fn attempt(
    config: &SessionConfig,
    provider: &dyn ConnectionProvider,
    sink: &Arc<dyn Sink>,
    stores: &Stores,
    context: &SessionContext,
    shutdown: &CancellationToken,
) -> Result<(), SessionError> {
    let Some(prepared) = shutdown
        .run_until_cancelled(prepare(provider, config, &context.health, &context.status))
        .await
    else {
        return Ok(());
    };
    let control = prepared?;
    let stores = instrumented(stores);
    context.health.set_role(Role::Standby);
    let poller = spawn_slot_poller(&control, &config.slot, &context.status);
    let result = serve(config, provider, sink, &stores, context, shutdown, &control).await;
    poller.abort();
    let _ = poller.await;
    result
}

async fn serve(
    config: &SessionConfig,
    provider: &dyn ConnectionProvider,
    sink: &Arc<dyn Sink>,
    stores: &Stores,
    context: &SessionContext,
    shutdown: &CancellationToken,
    control: &Arc<ControlConnection>,
) -> Result<(), SessionError> {
    let probe = Arc::new(ControlProbe {
        control: Arc::clone(control),
        slot: config.slot.clone(),
    });
    let Some((stream, start, acquired)) =
        acquire_slot(probe.as_ref(), control, provider, config, context, shutdown).await?
    else {
        return Ok(());
    };
    let gate = WriteGate::new();
    let lease = spawn_lease(&probe, &stream, config, acquired, &gate, &context.health);
    context.health.set_role(Role::Active);
    context.health.set_lease_valid(true);
    tracing::info!(backend_pid = stream.backend_pid(), start_lsn = %start, "acquired the slot");
    let takeover = spawn_takeover(config, stores, context, start);
    let (reports, mut settled) = mpsc::unbounded_channel();
    let permits = Arc::new(Semaphore::new(
        config.sink_concurrency.clamp(1, Semaphore::MAX_PERMITS),
    ));
    let gated = gated_stores(stores, &gate);
    let lanes = Lanes::spawn(
        config.lane_settings(),
        &gated_sink(sink, &gate),
        &gated,
        &permits,
        &reports,
    );
    let status = &context.status;
    status.set_status_clock(Some(stream.status_clock()));
    status.set_confirmed(Some(start));
    status.set_inflight(0, 0);
    status.set_parked(Some(lanes.gauges()));
    let session = Session {
        control,
        types: TypeResolver::new(),
        relations: HashMap::new(),
        builder: Builder::new(config.pipeline.clone()),
        ack: AckTracker::new(start),
        lanes,
        parked_store: gated.state,
        status,
        order_by: &config.order_by,
        order_by_tables: &config.order_by_tables,
        attribute_columns: &config.attribute_columns,
        max_inflight_bytes: config.max_inflight_bytes,
        slot: &config.slot,
        health: &context.health,
        leadership: Leadership {
            gate: Arc::clone(&gate),
            state: Arc::clone(&stores.state),
            pending: Some(takeover),
            claim: None,
        },
        drain_timeout: config.leadership.drain_timeout,
        committed_ms: None,
        position_log_at: Instant::now(),
        transaction: Span::none(),
        transaction_events: 0,
    };
    drop(reports);
    let result = session
        .run(stream, &mut settled, shutdown, start, lease)
        .await;
    gate.fence();
    result
}

type PendingClaim = AbortOnDropHandle<Result<Claim, TakeoverError>>;
type Lease = AbortOnDropHandle<FenceReason>;

fn spawn_lease(
    probe: &Arc<ControlProbe>,
    stream: &ReplicationStream,
    config: &SessionConfig,
    acquired: Instant,
    gate: &Arc<WriteGate>,
    health: &Health,
) -> Lease {
    let probe: Arc<dyn SlotProbe> = Arc::clone(probe) as Arc<dyn SlotProbe>;
    AbortOnDropHandle::new(tokio::spawn(hold_lease(
        probe,
        stream.backend_pid(),
        config.leadership.lease_timeout,
        acquired,
        Arc::clone(gate),
        health.clone(),
    )))
}

fn spawn_takeover(
    config: &SessionConfig,
    stores: &Stores,
    context: &SessionContext,
    start: Lsn,
) -> PendingClaim {
    let state = Arc::clone(&stores.state);
    let holder = context.holder.clone();
    let fence_delay = config.leadership.fence_delay;
    let token = CancellationToken::new();
    AbortOnDropHandle::new(tokio::spawn(claim_in_span(
        state,
        holder,
        start,
        fence_delay,
        token,
    )))
}

async fn claim_in_span(
    state: Arc<dyn StateStore>,
    holder: String,
    start: Lsn,
    fence_delay: Duration,
    token: CancellationToken,
) -> Result<Claim, TakeoverError> {
    let span = tracing::info_span!(
        "takeover",
        holder = %holder,
        kind = tracing::field::Empty,
        epoch = tracing::field::Empty,
    );
    let recorded = span.clone();
    async move {
        let claim = take_over(state.as_ref(), &holder, start, fence_delay, &token).await?;
        recorded.record("kind", tracing::field::display(claim.kind));
        recorded.record("epoch", claim.record.epoch);
        Ok(claim)
    }
    .instrument(span)
    .await
}

fn position_log_due(next: &mut Instant, now: Instant) -> bool {
    if now < *next {
        return false;
    }
    *next = now + POSITION_LOG_INTERVAL;
    true
}

fn commit_unix_ms(commit_ts: i64) -> Option<u64> {
    let unix_ms = commit_ts
        .div_euclid(1000)
        .checked_add(POSTGRES_EPOCH_UNIX_MS)?;
    u64::try_from(unix_ms).ok()
}

async fn stop_by(stream: ReplicationStream, deadline: Instant) {
    match timeout_at(deadline, stream.stop()).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::warn!(%error, "replication stream did not stop cleanly"),
        Err(_) => tracing::warn!("replication stream did not stop within the handover tail"),
    }
}

async fn status_by(stream: &ReplicationStream, deadline: Instant) -> bool {
    let failure = match timeout_at(deadline, stream.send_status()).await {
        Ok(Ok(())) => return true,
        Ok(Err(error)) => error.to_string(),
        Err(_) => "no time left in the handover tail".to_owned(),
    };
    tracing::warn!(
        %failure,
        "could not send the final status update; leaving the leader record active"
    );
    false
}

async fn release_by(state: &dyn StateStore, claim: &Claim, confirmed_lsn: Lsn, deadline: Instant) {
    match timeout_at(deadline, release(state, claim, confirmed_lsn)).await {
        Ok(Release::Released) => tracing::info!(
            epoch = claim.record.epoch,
            %confirmed_lsn,
            "released the leader record"
        ),
        Ok(Release::Conflict) => tracing::error!(
            epoch = claim.record.epoch,
            "another writer changed the leader record; leaving it as it is"
        ),
        Err(_) => tracing::warn!(
            epoch = claim.record.epoch,
            "could not release the leader record within the handover tail"
        ),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LaneStop {
    Exited,
    Aborted,
    Unfinished,
}

async fn stop_lanes_by(stop: impl Future<Output = bool>, deadline: Instant) -> LaneStop {
    match timeout_at(deadline, stop).await {
        Ok(true) => LaneStop::Exited,
        Ok(false) => LaneStop::Aborted,
        Err(_) => LaneStop::Unfinished,
    }
}

fn keeps_record_active(lanes: LaneStop, fenced_by_lease: bool, abandoned_writes: u64) -> bool {
    fenced_by_lease || abandoned_writes > 0 || lanes == LaneStop::Unfinished
}

async fn finished_claim(
    pending: PendingClaim,
) -> Option<Result<Result<Claim, TakeoverError>, JoinError>> {
    if pending.is_finished() {
        Some(pending.await)
    } else {
        None
    }
}

async fn claimed(
    pending: &mut Option<PendingClaim>,
) -> Result<Result<Claim, TakeoverError>, JoinError> {
    match pending {
        Some(handle) => handle.await,
        None => std::future::pending().await,
    }
}

async fn prepare(
    provider: &dyn ConnectionProvider,
    config: &SessionConfig,
    health: &Health,
    status: &Status,
) -> Result<Arc<ControlConnection>, SessionError> {
    let control = Arc::new(connect_control(&provider.params().await?).await?);
    timeout(
        CONTROL_QUERY_TIMEOUT,
        check_source(&control, config, health, status),
    )
    .await
    .map_err(|_| SessionError::ControlStalled(CONTROL_QUERY_TIMEOUT))??;
    check_login(provider, config, health).await?;
    health.set_validated(true);
    Ok(control)
}

async fn check_login(
    provider: &dyn ConnectionProvider,
    config: &SessionConfig,
    health: &Health,
) -> Result<(), SessionError> {
    let replication = replication_config(&provider.params().await?, config, Lsn(0)).await?;
    let Err(error) = verify_login(&replication).await else {
        return Ok(());
    };
    let error = login_fault(error);
    if matches!(error, SessionError::Invalid(_)) {
        health.set_validated(false);
    }
    Err(error)
}

fn refused(error: &ReplicationError) -> bool {
    match error {
        ReplicationError::Server { sqlstate, .. } => {
            sqlstate.starts_with("28") || sqlstate == INSUFFICIENT_PRIVILEGE
        }
        ReplicationError::PasswordRequired
        | ReplicationError::UnsupportedAuthentication(_)
        | ReplicationError::UnsupportedSasl(_) => true,
        _ => false,
    }
}

fn login_fault(error: ReplicationError) -> SessionError {
    if refused(&error) {
        Invalid::ReplicationDenied(error.to_string()).into()
    } else {
        SessionError::Replication(error)
    }
}

async fn check_source(
    control: &ControlConnection,
    config: &SessionConfig,
    health: &Health,
    status: &Status,
) -> Result<(), SessionError> {
    match validate(control.client(), &config.slot, &config.publication).await {
        Ok(_) => {}
        Err(ValidationError::Invalid(invalid)) => {
            health.set_validated(false);
            return Err(invalid.into());
        }
        Err(ValidationError::Catalog(error)) => return Err(error.into()),
    }
    let unsafe_tables =
        catalog::tables_without_full_identity(control.client(), &config.publication).await?;
    for (schema, table) in &unsafe_tables {
        tracing::warn!(%schema, %table, "published table does not use REPLICA IDENTITY FULL");
    }
    status.set_tables_without_full_identity(u64::try_from(unsafe_tables.len()).unwrap_or(u64::MAX));
    if !config.order_by_tables.is_empty() {
        let published = catalog::published_columns(control.client(), &config.publication).await?;
        match judge_order_overrides(&config.order_by_tables, &published) {
            Ok(unpublished) => {
                for table in unpublished {
                    tracing::warn!(%table, "delivery.order_by_tables names a table that is not in the publication");
                }
            }
            Err(invalid) => {
                health.set_validated(false);
                return Err(invalid.into());
            }
        }
    }
    Ok(())
}

async fn acquire_slot(
    probe: &ControlProbe,
    control: &ControlConnection,
    provider: &dyn ConnectionProvider,
    config: &SessionConfig,
    context: &SessionContext,
    shutdown: &CancellationToken,
) -> Result<Option<(ReplicationStream, Lsn, Instant)>, SessionError> {
    let start = || async {
        let start = timeout(
            CONTROL_QUERY_TIMEOUT,
            catalog::slot_confirmed_flush(control.client(), &config.slot),
        )
        .await
        .map_err(|_| StartError::Failed(SessionError::ControlStalled(CONTROL_QUERY_TIMEOUT)))?
        .map_err(|error| StartError::Failed(error.into()))?
        .ok_or_else(|| StartError::Failed(Invalid::SlotMissing(config.slot.clone()).into()))?;
        let params = provider
            .params()
            .await
            .map_err(|error| StartError::Failed(error.into()))?;
        let replication = replication_config(&params, config, start)
            .await
            .map_err(|error| StartError::Failed(error.into()))?;
        let acquired = Instant::now();
        let stream = ReplicationStream::start(replication)
            .await
            .map_err(|error| start_error(error, start))?;
        Ok((stream, start, acquired))
    };
    match acquire(probe, start, shutdown).await {
        Ok(acquired) => Ok(acquired),
        Err(AcquireError::SlotMissing) => {
            context.health.set_validated(false);
            Err(Invalid::SlotMissing(config.slot.clone()).into())
        }
        Err(AcquireError::Probe(error)) => Err(error.into()),
        Err(AcquireError::Start(error)) => {
            if matches!(error, SessionError::Invalid(_)) {
                context.health.set_validated(false);
            }
            Err(error)
        }
    }
}

#[derive(Debug)]
struct ControlProbe {
    control: Arc<ControlConnection>,
    slot: String,
}

#[async_trait]
impl SlotProbe for ControlProbe {
    async fn holder(&self) -> Result<SlotHolder, ProbeError> {
        let activity = timeout(
            CONTROL_QUERY_TIMEOUT,
            catalog::slot_activity(self.control.client(), &self.slot),
        )
        .await
        .map_err(|_| ProbeError(format!("no answer within {CONTROL_QUERY_TIMEOUT:?}")))?
        .map_err(|error| ProbeError(error.to_string()))?;
        Ok(match activity {
            None => SlotHolder::Missing,
            Some(activity) => activity
                .active_pid
                .map_or(SlotHolder::Free, SlotHolder::Held),
        })
    }
}

pub fn start_error(error: ReplicationError, at: Lsn) -> StartError<SessionError> {
    if error.sqlstate() == Some(OBJECT_IN_USE) {
        StartError::InUse
    } else if refused(&error) {
        StartError::Failed(login_fault(error))
    } else {
        StartError::Failed(stream_fault(error, at))
    }
}

fn next_event(
    received: Result<Option<ReplicationEvent>, ReplicationError>,
    position: &mut Lsn,
) -> Result<ReplicationEvent, SessionError> {
    let event = received
        .map_err(|error| stream_fault(error, *position))?
        .ok_or(SessionError::StreamEnded)?;
    if let ReplicationEvent::XLogData { wal_start, .. } = &event {
        *position = *wal_start;
    }
    Ok(event)
}

fn stream_fault(error: ReplicationError, at: Lsn) -> SessionError {
    match error {
        ReplicationError::Protocol(_) => SessionError::Malformed { at, source: error },
        other => SessionError::Replication(other),
    }
}

async fn replication_config(
    params: &ConnectParams,
    config: &SessionConfig,
    start: Lsn,
) -> Result<ReplicationConfig, ConnectError> {
    Ok(ReplicationConfig {
        host: params.host.clone(),
        port: params.port,
        user: params.user.clone(),
        password: params.password.clone(),
        database: params.database.clone(),
        options: SESSION_OPTIONS.to_owned(),
        application_name: "pg-cdc".to_owned(),
        tls: replication_tls(params).await?,
        slot: config.slot.clone(),
        publication: config.publication.clone(),
        start_lsn: start,
        status_interval: config.status_interval,
        connect_timeout: CONNECT_TIMEOUT,
        buffer: config.buffer,
    })
}

struct Leadership {
    gate: Arc<WriteGate>,
    state: Arc<dyn StateStore>,
    pending: Option<PendingClaim>,
    claim: Option<Claim>,
}

struct Session<'a> {
    control: &'a ControlConnection,
    types: TypeResolver,
    relations: HashMap<u32, KnownRelation>,
    builder: Builder,
    ack: AckTracker,
    lanes: Lanes,
    parked_store: Arc<dyn StateStore>,
    status: &'a Status,
    order_by: &'a OrderBy,
    order_by_tables: &'a BTreeMap<TableName, OrderBy>,
    attribute_columns: &'a [String],
    max_inflight_bytes: u64,
    slot: &'a str,
    health: &'a Health,
    leadership: Leadership,
    drain_timeout: Duration,
    committed_ms: Option<u64>,
    position_log_at: Instant,
    transaction: Span,
    transaction_events: u64,
}

enum Ended {
    Fenced(FenceReason),
    Body(Result<(), SessionError>),
}

impl Session<'_> {
    async fn run(
        mut self,
        mut stream: ReplicationStream,
        settled: &mut mpsc::UnboundedReceiver<Report>,
        shutdown: &CancellationToken,
        start: Lsn,
        mut lease: Lease,
    ) -> Result<(), SessionError> {
        let ended = tokio::select! {
            biased;
            joined = &mut lease => Ended::Fenced(joined.unwrap_or(FenceReason::Expired)),
            result = self.body(&mut stream, settled, shutdown, start) => Ended::Body(result),
        };
        let result = match ended {
            Ended::Fenced(reason) => return self.fenced(stream, reason).await,
            Ended::Body(result) => result,
        };
        self.status.set_parked_access(None);
        match result {
            Err(error @ SessionError::Takeover(_)) => {
                drop(lease);
                tracing::error!(slot = self.slot, %error, "cannot take over the slot; releasing it");
                self.leadership.gate.fence();
                self.lanes.stop(Duration::ZERO).await;
                stop_by(stream, Instant::now() + HANDOVER_TAIL).await;
                Err(error)
            }
            Err(error) => {
                drop(lease);
                self.leadership.gate.fence();
                Err(error)
            }
            Ok(()) => {
                self.hand_over(stream, lease).await;
                Ok(())
            }
        }
    }

    async fn hand_over(mut self, stream: ReplicationStream, lease: Lease) {
        let deadline = Instant::now() + HANDOVER_TAIL;
        if let Some(pending) = self.leadership.pending.take()
            && let Some(Ok(Ok(claim))) = finished_claim(pending).await
        {
            self.leadership.claim = Some(claim);
        }
        let claim = self.leadership.claim.take();
        let grace = if claim.is_some() {
            LANE_STOP_GRACE.min(deadline.saturating_duration_since(Instant::now()))
        } else {
            Duration::ZERO
        };
        let lanes = stop_lanes_by(self.lanes.stop(grace), deadline).await;
        let fenced_by_lease = lease.is_finished();
        drop(lease);
        self.leadership.gate.fence();
        if let Some(claim) = claim {
            let abandoned_writes = self.leadership.gate.abandoned_writes();
            if keeps_record_active(lanes, fenced_by_lease, abandoned_writes) {
                tracing::warn!(
                    abandoned_writes,
                    ?lanes,
                    fenced_by_lease,
                    "leaving the leader record active; the next holder waits fence_delay"
                );
            } else if status_by(&stream, deadline).await {
                let confirmed = self.ack.confirmed();
                release_by(self.leadership.state.as_ref(), &claim, confirmed, deadline).await;
            }
        }
        stop_by(stream, deadline).await;
    }

    async fn body(
        &mut self,
        stream: &mut ReplicationStream,
        settled: &mut mpsc::UnboundedReceiver<Report>,
        shutdown: &CancellationToken,
        start: Lsn,
    ) -> Result<(), SessionError> {
        let streamed = self.stream(stream, settled, shutdown, start).await;
        self.status.set_parked_access(None);
        streamed?;
        if self.leadership.claim.is_some() {
            self.health.set_role(Role::Draining);
            self.health.set_delivering(false);
            self.drain(stream, settled).await?;
        }
        Ok(())
    }

    async fn fenced(
        mut self,
        stream: ReplicationStream,
        reason: FenceReason,
    ) -> Result<(), SessionError> {
        self.status.set_parked_access(None);
        self.leadership.pending = None;
        self.leadership.gate.fence();
        self.lanes.stop(Duration::ZERO).await;
        drop(stream);
        Err(SessionError::Fenced(reason))
    }

    fn claimed(
        &mut self,
        outcome: Result<Result<Claim, TakeoverError>, JoinError>,
    ) -> Result<(), SessionError> {
        self.leadership.pending = None;
        let claim = outcome.unwrap_or(Err(TakeoverError::Conflict))?;
        metrics::record(Measurement::Takeover(claim.kind));
        self.leadership.gate.open();
        self.health.set_delivering(true);
        self.status.set_epoch(Some(claim.record.epoch));
        self.status.set_parked_access(Some(ParkedAccess {
            operator: self.lanes.operator(),
            store: Arc::clone(&self.parked_store),
        }));
        tracing::info!(
            epoch = claim.record.epoch,
            kind = %claim.kind,
            holder = %claim.record.holder,
            "took over the slot"
        );
        self.leadership.claim = Some(claim);
        Ok(())
    }

    async fn stream(
        &mut self,
        stream: &mut ReplicationStream,
        settled: &mut mpsc::UnboundedReceiver<Report>,
        shutdown: &CancellationToken,
        start: Lsn,
    ) -> Result<(), SessionError> {
        let mut position = start;
        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => return Ok(()),
                report = settled.recv() => self.report(report, stream)?,
                outcome = claimed(&mut self.leadership.pending) => self.claimed(outcome)?,
                received = stream.recv(), if self.ack.has_room(self.max_inflight_bytes) => {
                    let event = next_event(received, &mut position)?;
                    let Some(handled) = shutdown.run_until_cancelled(self.handle(event)).await else {
                        return Ok(());
                    };
                    if let Some(lsn) = handled? {
                        self.confirm(stream, lsn);
                    }
                }
            }
        }
    }

    async fn drain(
        &mut self,
        stream: &ReplicationStream,
        settled: &mut mpsc::UnboundedReceiver<Report>,
    ) -> Result<(), SessionError> {
        if self.settle_within(stream, settled).await? {
            return Ok(());
        }
        tracing::warn!(
            unsettled = self.ack.inflight_events(),
            timeout_ms = self.drain_timeout.as_millis(),
            "parking the events still in flight after the drain timeout"
        );
        self.lanes.park_all();
        if self.settle_within(stream, settled).await? {
            return Ok(());
        }
        tracing::warn!(
            unsettled = self.ack.inflight_events(),
            "events neither delivered nor parked will replay"
        );
        Ok(())
    }

    async fn settle_within(
        &mut self,
        stream: &ReplicationStream,
        settled: &mut mpsc::UnboundedReceiver<Report>,
    ) -> Result<bool, SessionError> {
        let drain_timeout = self.drain_timeout;
        let settle_all = async {
            while self.ack.inflight_events() > 0 {
                self.report(settled.recv().await, stream)?;
            }
            Ok(())
        };
        match tokio::time::timeout(drain_timeout, settle_all).await {
            Ok(result) => result.map(|()| true),
            Err(_) => Ok(false),
        }
    }

    fn report(
        &mut self,
        report: Option<Report>,
        stream: &ReplicationStream,
    ) -> Result<(), SessionError> {
        match report {
            Some(Report::Settled(settled)) => {
                let lsn = self.ack.settle(settled)?;
                self.confirm(stream, lsn);
                self.publish_inflight();
                Ok(())
            }
            Some(Report::Fatal(error)) => Err(error.into()),
            None => Err(SessionError::LanesStopped),
        }
    }

    fn confirm(&mut self, stream: &ReplicationStream, lsn: Lsn) {
        stream.confirm(lsn);
        let confirmed = self.ack.confirmed();
        self.status.set_confirmed(Some(confirmed));
        if position_log_due(&mut self.position_log_at, Instant::now()) {
            tracing::info!(
                confirmed_lsn = %confirmed,
                last_confirmed_commit_unix_ms = self.ack.confirmed_commit_ms(),
                "confirmed position"
            );
        }
    }

    fn publish_inflight(&self) {
        self.status
            .set_inflight(self.ack.inflight_events(), self.ack.inflight_bytes());
    }

    async fn handle(&mut self, event: ReplicationEvent) -> Result<Option<Lsn>, SessionError> {
        match event {
            ReplicationEvent::XLogData {
                wal_start, data, ..
            } => self.message(wal_start, &data).await,
            ReplicationEvent::KeepAlive { wal_end } => Ok(Some(self.ack.keepalive(wal_end))),
        }
    }

    async fn message(&mut self, wal_start: Lsn, data: &[u8]) -> Result<Option<Lsn>, SessionError> {
        let message = decode(data).map_err(|source| SessionError::Decode { wal_start, source })?;
        let build_error = |source| SessionError::Build { wal_start, source };
        let (key, built) = match message {
            LogicalMessage::Begin(begin) => {
                self.builder
                    .begin(begin.final_lsn, begin.commit_ts)
                    .map_err(build_error)?;
                self.ack.begin()?;
                self.committed_ms = commit_unix_ms(begin.commit_ts);
                self.ack.stamp(self.committed_ms);
                self.transaction_events = 0;
                self.transaction = tracing::debug_span!(
                    "transaction",
                    commit_lsn = %begin.final_lsn,
                    xid = begin.xid,
                    events = tracing::field::Empty,
                );
                return Ok(None);
            }
            LogicalMessage::Commit(commit) => {
                self.builder.commit().map_err(build_error)?;
                self.transaction.record("events", self.transaction_events);
                self.transaction = Span::none();
                return Ok(Some(self.ack.commit(commit.end_lsn)?));
            }
            LogicalMessage::Relation(relation) => {
                self.relation(relation).await?;
                return Ok(None);
            }
            LogicalMessage::Type(_) | LogicalMessage::Origin(_) => return Ok(None),
            LogicalMessage::Insert(insert) => {
                let relation = known(&self.relations, wal_start, insert.relation_oid)?;
                let built = self
                    .builder
                    .insert(&relation.info, &insert)
                    .map_err(build_error)?;
                relation.keyed(built)
            }
            LogicalMessage::Update(update) => {
                let relation = known(&self.relations, wal_start, update.relation_oid)?;
                let built = self
                    .builder
                    .update(&relation.info, &update)
                    .map_err(build_error)?;
                relation.keyed(built)
            }
            LogicalMessage::Delete(delete) => {
                let relation = known(&self.relations, wal_start, delete.relation_oid)?;
                let built = self
                    .builder
                    .delete(&relation.info, &delete)
                    .map_err(build_error)?;
                relation.keyed(built)
            }
            LogicalMessage::Truncate(truncate) => {
                for oid in truncate.relation_oids {
                    let relation = known(&self.relations, wal_start, oid)?;
                    let built = self.builder.truncate(&relation.info).map_err(build_error)?;
                    let (key, built) = relation.keyed(built);
                    self.emit(&built, key)?;
                }
                return Ok(None);
            }
        };
        self.emit(&built, key)?;
        Ok(None)
    }

    async fn relation(&mut self, relation: Relation) -> Result<(), SessionError> {
        let client = self.control.client();
        let mut columns = Vec::with_capacity(relation.columns.len());
        for column in relation.columns {
            let kind = self.types.resolve(client, column.type_oid).await?;
            columns.push(ColumnInfo {
                name: column.name,
                type_oid: column.type_oid,
                kind,
                is_identity: column.is_key,
            });
        }
        let key_columns = catalog::key_columns(client, relation.oid).await?;
        let schema = if relation.namespace.is_empty() {
            "pg_catalog".to_owned()
        } else {
            relation.namespace
        };
        if let Some(column) = table_override(self.order_by_tables, &schema, &relation.name)
            .and_then(|order_by| {
                let names: Vec<&str> = columns.iter().map(|column| column.name.as_str()).collect();
                missing_order_column(order_by, &names)
            })
        {
            tracing::warn!(
                %schema,
                table = %relation.name,
                %column,
                "the replication stream no longer carries this table's delivery.order_by_tables column; its events order by table"
            );
        }
        let order_by =
            table_order_by(self.order_by, self.order_by_tables, &schema, &relation.name).clone();
        let info = RelationInfo {
            oid: relation.oid,
            schema,
            table: relation.name,
            columns,
            key_columns,
        };
        self.relations
            .insert(relation.oid, KnownRelation { info, order_by });
        Ok(())
    }

    fn emit(&mut self, built: &Built, key: OrderKeyId) -> Result<(), SessionError> {
        for fallback in &built.fallbacks {
            metrics::record(Measurement::DecodeFallback {
                type_oid: fallback.type_oid,
            });
            tracing::error!(
                id = %built.envelope.id,
                column = %fallback.column,
                type_oid = fallback.type_oid,
                "value fell back to its text form"
            );
        }
        let outbound = Outbound::encode(&built.envelope, key, self.attribute_columns)?;
        let bytes = u64::try_from(outbound.encoded_len()).unwrap_or(u64::MAX);
        let ticket = self.ack.track(bytes)?;
        if metrics::enabled() {
            let (table, op) = metric_labels(&outbound.attributes);
            metrics::record(Measurement::Event {
                table,
                op,
                outcome: EventOutcome::Received,
                count: 1,
            });
        }
        self.transaction_events += 1;
        self.lanes.dispatch(Pending {
            outbound,
            ticket,
            arrived: Instant::now(),
            committed_ms: self.committed_ms,
        })?;
        self.publish_inflight();
        Ok(())
    }
}

struct KnownRelation {
    info: RelationInfo,
    order_by: OrderBy,
}

impl KnownRelation {
    fn keyed(&self, built: Built) -> (OrderKeyId, Built) {
        (order_key(&self.order_by, &built.envelope), built)
    }
}

fn known(
    relations: &HashMap<u32, KnownRelation>,
    wal_start: Lsn,
    oid: u32,
) -> Result<&KnownRelation, SessionError> {
    relations
        .get(&oid)
        .ok_or(SessionError::UnknownRelation { wal_start, oid })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capture::Capture;
    use crate::leader::standby::StartError;
    use crate::state::memory::MemoryStateStore;
    use tracing::Level;
    use tracing::level_filters::LevelFilter;

    #[test]
    fn the_confirmed_position_is_logged_at_most_once_a_minute() {
        let start = Instant::now();
        let mut next = start;
        assert!(position_log_due(&mut next, start));
        assert!(!position_log_due(
            &mut next,
            start + Duration::from_secs(59)
        ));
        assert!(position_log_due(&mut next, start + Duration::from_secs(60)));
        assert!(!position_log_due(
            &mut next,
            start + Duration::from_secs(61)
        ));
    }

    #[test]
    fn commit_timestamps_convert_to_unix_milliseconds() {
        assert_eq!(commit_unix_ms(0), Some(946_684_800_000));
        assert_eq!(
            commit_unix_ms(845_285_183_069_961),
            Some(946_684_800_000 + 845_285_183_069)
        );
        assert_eq!(commit_unix_ms(-946_684_800_000_001), None);
        assert!(commit_unix_ms(i64::MAX).is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn a_takeover_runs_in_an_info_span_that_records_its_kind_and_epoch() {
        let (capture, _guard) = Capture::install(LevelFilter::INFO);
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let claim = claim_in_span(
            store,
            "pod-a".to_owned(),
            Lsn(0x10),
            Duration::ZERO,
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(claim.record.epoch, 1);
        let spans = capture.spans();
        assert_eq!(spans.len(), 1, "{spans:?}");
        assert_eq!(spans[0].name, "takeover");
        assert_eq!(spans[0].level, Level::INFO);
        let fields: Vec<(&str, &str)> = spans[0]
            .fields
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect();
        assert_eq!(
            fields,
            vec![("epoch", "1"), ("holder", "pod-a"), ("kind", "first")]
        );
    }

    #[tokio::test]
    async fn a_claim_that_finished_before_shutdown_is_still_adopted() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let pending = AbortOnDropHandle::new(tokio::spawn(claim_in_span(
            store,
            "pod-a".to_owned(),
            Lsn(0x10),
            Duration::ZERO,
            CancellationToken::new(),
        )));
        while !pending.is_finished() {
            tokio::task::yield_now().await;
        }
        let claim = finished_claim(pending).await.unwrap().unwrap().unwrap();
        assert_eq!(claim.record.epoch, 1);
        assert_eq!(claim.record.holder, "pod-a");
    }

    #[tokio::test]
    async fn a_claim_still_waiting_out_the_fence_is_not_adopted() {
        let pending = AbortOnDropHandle::new(tokio::spawn(std::future::pending()));
        assert!(finished_claim(pending).await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn lanes_that_outlive_the_handover_tail_leave_the_record_active() {
        let deadline = Instant::now() + HANDOVER_TAIL;
        let lanes = stop_lanes_by(std::future::pending(), deadline).await;
        assert_eq!(lanes, LaneStop::Unfinished);
        assert_eq!(Instant::now(), deadline);
        assert!(keeps_record_active(lanes, false, 0));
    }

    #[tokio::test(start_paused = true)]
    async fn lanes_that_stop_in_time_release_the_record_unless_a_write_was_abandoned() {
        let deadline = Instant::now() + HANDOVER_TAIL;
        let exited = stop_lanes_by(async { true }, deadline).await;
        let aborted = stop_lanes_by(async { false }, deadline).await;
        assert_eq!((exited, aborted), (LaneStop::Exited, LaneStop::Aborted));
        assert!(!keeps_record_active(exited, false, 0));
        assert!(!keeps_record_active(aborted, false, 0));
        assert!(keeps_record_active(aborted, false, 1));
        assert!(keeps_record_active(exited, true, 0));
    }

    #[test]
    fn the_shutdown_budget_is_two_drain_windows_and_the_tail() {
        assert_eq!(
            shutdown_budget(Duration::from_secs(5)),
            Duration::from_secs(13)
        );
        assert_eq!(shutdown_budget(Duration::ZERO), HANDOVER_TAIL);
        assert_eq!(shutdown_budget(Duration::MAX), Duration::MAX);
    }

    fn xlog(wal_start: u64) -> ReplicationEvent {
        ReplicationEvent::XLogData {
            wal_start: Lsn(wal_start),
            wal_end: Lsn(wal_start + 0x100),
            data: Vec::from(*b"B").into(),
        }
    }

    #[test]
    fn d3_malformed_framing_faults_at_the_last_wal_position_received() {
        let mut position = Lsn(0x10);
        let protocol = || Err(ReplicationError::Protocol("bad frame".to_owned()));
        let error = next_event(protocol(), &mut position).unwrap_err();
        assert!(
            matches!(error, SessionError::Malformed { at: Lsn(0x10), .. }),
            "{error:?}"
        );
        next_event(Ok(Some(xlog(0x40))), &mut position).unwrap();
        next_event(
            Ok(Some(ReplicationEvent::KeepAlive { wal_end: Lsn(0x90) })),
            &mut position,
        )
        .unwrap();
        let error = next_event(protocol(), &mut position).unwrap_err();
        assert_eq!(error.fault_lsn(), Some(Lsn(0x40)));
        assert!(!error.is_fatal());
    }

    #[test]
    fn transport_failures_and_a_closed_stream_carry_no_fault_position() {
        let mut position = Lsn(0x10);
        for received in [
            Err(ReplicationError::Closed),
            Err(ReplicationError::Io(
                std::io::ErrorKind::ConnectionReset.into(),
            )),
            Ok(None),
        ] {
            let error = next_event(received, &mut position).unwrap_err();
            assert_eq!(error.fault_lsn(), None, "{error:?}");
        }
        assert!(matches!(
            stream_fault(ReplicationError::Protocol("startup".to_owned()), Lsn(0x20)).fault_lsn(),
            Some(Lsn(0x20))
        ));
    }

    #[test]
    fn a_refused_replication_login_is_a_misconfiguration() {
        let server = |sqlstate: &str| ReplicationError::Server {
            sqlstate: sqlstate.to_owned(),
            message: "refused".to_owned(),
        };
        let refusals = || {
            [
                server("42501"),
                server("28000"),
                server("28P01"),
                ReplicationError::PasswordRequired,
                ReplicationError::UnsupportedAuthentication(7),
                ReplicationError::UnsupportedSasl("SCRAM-SHA-256-PLUS".to_owned()),
            ]
        };
        for error in refusals() {
            let text = error.to_string();
            assert!(
                matches!(
                    start_error(error, Lsn(0x10)),
                    StartError::Failed(SessionError::Invalid(Invalid::ReplicationDenied(ref reason)))
                        if *reason == text
                ),
                "{text}"
            );
        }
        for error in refusals() {
            let text = error.to_string();
            assert!(
                matches!(
                    login_fault(error),
                    SessionError::Invalid(Invalid::ReplicationDenied(ref reason)) if *reason == text
                ),
                "{text}"
            );
        }
        for error in [server("53300"), server("57P03"), ReplicationError::Closed] {
            let text = error.to_string();
            assert!(
                matches!(login_fault(error), SessionError::Replication(_)),
                "{text}"
            );
        }
    }

    #[test]
    fn start_error_maps_55006_to_in_use() {
        let server = |sqlstate: &str| ReplicationError::Server {
            sqlstate: sqlstate.to_owned(),
            message: "replication slot is active".to_owned(),
        };
        assert!(matches!(
            start_error(server("55006"), Lsn(0x10)),
            StartError::InUse
        ));
        for error in [server("42704"), ReplicationError::Closed] {
            assert!(
                matches!(
                    start_error(error, Lsn(0x10)),
                    StartError::Failed(SessionError::Replication(_))
                ),
                "not a plain failure"
            );
        }
        assert!(matches!(
            start_error(ReplicationError::Protocol("bad".to_owned()), Lsn(0x20)),
            StartError::Failed(SessionError::Malformed { at: Lsn(0x20), .. })
        ));
    }
}
