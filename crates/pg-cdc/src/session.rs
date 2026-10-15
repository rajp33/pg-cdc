use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use pg_cdc_decoder::Lsn;
use pg_cdc_decoder::message::{DecodeError, LogicalMessage, Relation, decode};
use pg_cdc_replication::{
    ReplicationConfig, ReplicationError, ReplicationEvent, ReplicationStream,
};
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinError;
use tokio::time::{Instant, timeout, timeout_at};
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;

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
use crate::order::{OrderBy, order_key};
use crate::sink::{Outbound, Sink};
use crate::state::StateStore;
use crate::validate::{Invalid, ValidationError, validate};

pub const REPLICATION_BUFFER: usize = 64;
pub const HANDOVER_TAIL: Duration = Duration::from_secs(3);
pub const LANE_STOP_GRACE: Duration = Duration::from_secs(1);
pub const CONTROL_QUERY_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionConfig {
    pub pipeline: String,
    pub slot: String,
    pub publication: String,
    pub status_interval: Duration,
    pub buffer: usize,
    pub order_by: OrderBy,
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
    context.health.set_delivering(false);
    context.health.set_lease_valid(false);
    context.health.set_role(Role::Standby);
    result
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
        .run_until_cancelled(prepare(provider, config, &context.health))
        .await
    else {
        return Ok(());
    };
    let (params, control) = prepared?;
    context.health.set_role(Role::Standby);
    let probe = Arc::new(ControlProbe {
        control: Arc::clone(&control),
        slot: config.slot.clone(),
    });
    let Some((stream, start, acquired)) =
        acquire_slot(probe.as_ref(), &control, &params, config, context, shutdown).await?
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
    let session = Session {
        control: &control,
        types: TypeResolver::new(),
        relations: HashMap::new(),
        builder: Builder::new(config.pipeline.clone()),
        ack: AckTracker::new(start),
        lanes: Lanes::spawn(
            config.lane_settings(),
            &gated_sink(sink, &gate),
            &gated_stores(stores, &gate),
            &permits,
            &reports,
        ),
        order_by: &config.order_by,
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
    AbortOnDropHandle::new(tokio::spawn(async move {
        take_over(state.as_ref(), &holder, start, fence_delay, &token).await
    }))
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
) -> Result<(ConnectParams, Arc<ControlConnection>), SessionError> {
    let params = provider.params().await?;
    let control = Arc::new(connect_control(&params).await?);
    timeout(
        CONTROL_QUERY_TIMEOUT,
        check_source(&control, config, health),
    )
    .await
    .map_err(|_| SessionError::ControlStalled(CONTROL_QUERY_TIMEOUT))??;
    Ok((params, control))
}

async fn check_source(
    control: &ControlConnection,
    config: &SessionConfig,
    health: &Health,
) -> Result<(), SessionError> {
    match validate(control.client(), &config.slot, &config.publication).await {
        Ok(_) => health.set_validated(true),
        Err(ValidationError::Invalid(invalid)) => {
            health.set_validated(false);
            return Err(invalid.into());
        }
        Err(ValidationError::Catalog(error)) => return Err(error.into()),
    }
    for (schema, table) in
        catalog::tables_without_full_identity(control.client(), &config.publication).await?
    {
        tracing::warn!(%schema, %table, "published table does not use REPLICA IDENTITY FULL");
    }
    Ok(())
}

async fn acquire_slot(
    probe: &ControlProbe,
    control: &ControlConnection,
    params: &ConnectParams,
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
        let replication = replication_config(params, config, start)
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
    relations: HashMap<u32, RelationInfo>,
    builder: Builder,
    ack: AckTracker,
    lanes: Lanes,
    order_by: &'a OrderBy,
    attribute_columns: &'a [String],
    max_inflight_bytes: u64,
    slot: &'a str,
    health: &'a Health,
    leadership: Leadership,
    drain_timeout: Duration,
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
        self.leadership.pending = None;
        let claim = self.leadership.claim.take();
        let grace = if claim.is_some() {
            LANE_STOP_GRACE.min(deadline.saturating_duration_since(Instant::now()))
        } else {
            Duration::ZERO
        };
        let clean = timeout_at(deadline, self.lanes.stop(grace))
            .await
            .unwrap_or(false);
        let fenced_by_lease = lease.is_finished();
        drop(lease);
        self.leadership.gate.fence();
        if let Some(claim) = claim {
            let abandoned_writes = self.leadership.gate.abandoned_writes();
            if fenced_by_lease || abandoned_writes > 0 {
                tracing::warn!(
                    abandoned_writes,
                    clean,
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
        self.stream(stream, settled, shutdown, start).await?;
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
        self.leadership.gate.open();
        self.health.set_delivering(true);
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
                received = stream.recv(), if self.ack.inflight_bytes() < self.max_inflight_bytes => {
                    let event = next_event(received, &mut position)?;
                    let Some(handled) = shutdown.run_until_cancelled(self.handle(event)).await else {
                        return Ok(());
                    };
                    if let Some(lsn) = handled? {
                        stream.confirm(lsn);
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
                stream.confirm(self.ack.settle(settled)?);
                Ok(())
            }
            Some(Report::Fatal(error)) => Err(error.into()),
            None => Err(SessionError::LanesStopped),
        }
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
        let built = match message {
            LogicalMessage::Begin(begin) => {
                self.builder
                    .begin(begin.final_lsn, begin.commit_ts)
                    .map_err(build_error)?;
                self.ack.begin()?;
                tracing::debug!(xid = begin.xid, commit_lsn = %begin.final_lsn, "transaction");
                return Ok(None);
            }
            LogicalMessage::Commit(commit) => {
                self.builder.commit().map_err(build_error)?;
                return Ok(Some(self.ack.commit(commit.end_lsn)?));
            }
            LogicalMessage::Relation(relation) => {
                self.relation(relation).await?;
                return Ok(None);
            }
            LogicalMessage::Type(_) | LogicalMessage::Origin(_) => return Ok(None),
            LogicalMessage::Insert(insert) => {
                let relation = known(&self.relations, wal_start, insert.relation_oid)?;
                self.builder
                    .insert(relation, &insert)
                    .map_err(build_error)?
            }
            LogicalMessage::Update(update) => {
                let relation = known(&self.relations, wal_start, update.relation_oid)?;
                self.builder
                    .update(relation, &update)
                    .map_err(build_error)?
            }
            LogicalMessage::Delete(delete) => {
                let relation = known(&self.relations, wal_start, delete.relation_oid)?;
                self.builder
                    .delete(relation, &delete)
                    .map_err(build_error)?
            }
            LogicalMessage::Truncate(truncate) => {
                for oid in truncate.relation_oids {
                    let relation = known(&self.relations, wal_start, oid)?;
                    let event = self.builder.truncate(relation).map_err(build_error)?;
                    self.emit(&event)?;
                }
                return Ok(None);
            }
        };
        self.emit(&built)?;
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
        let info = RelationInfo {
            oid: relation.oid,
            schema,
            table: relation.name,
            columns,
            key_columns,
        };
        self.relations.insert(relation.oid, info);
        Ok(())
    }

    fn emit(&mut self, built: &Built) -> Result<(), SessionError> {
        for fallback in &built.fallbacks {
            tracing::error!(
                id = %built.envelope.id,
                column = %fallback.column,
                type_oid = fallback.type_oid,
                "value fell back to its text form"
            );
        }
        let key = order_key(self.order_by, &built.envelope);
        let outbound = Outbound::encode(&built.envelope, key, self.attribute_columns)?;
        let bytes = u64::try_from(outbound.body.len()).unwrap_or(u64::MAX);
        let ticket = self.ack.track(bytes)?;
        self.lanes.dispatch(Pending {
            outbound,
            ticket,
            arrived: Instant::now(),
        })?;
        Ok(())
    }
}

fn known(
    relations: &HashMap<u32, RelationInfo>,
    wal_start: Lsn,
    oid: u32,
) -> Result<&RelationInfo, SessionError> {
    relations
        .get(&oid)
        .ok_or(SessionError::UnknownRelation { wal_start, oid })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::leader::standby::StartError;

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
