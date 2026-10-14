use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use pg_cdc_decoder::Lsn;
use pg_cdc_decoder::message::{DecodeError, LogicalMessage, Relation, decode};
use pg_cdc_replication::{
    ReplicationConfig, ReplicationError, ReplicationEvent, ReplicationStream,
};
use tokio::sync::{Semaphore, mpsc};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::ack::{AckError, AckTracker};
use crate::catalog::{self, CatalogError, TypeResolver};
use crate::connection::{
    CONNECT_TIMEOUT, ConnectError, ConnectParams, ConnectionProvider, ControlConnection,
    SESSION_OPTIONS, connect_control, replication_tls,
};
use crate::event::{BuildError, Builder, Built, ColumnInfo, RelationInfo};
use crate::lane::{LaneError, LaneSettings, LaneStopped, Lanes, Pending, Report, Stores};
use crate::order::{OrderBy, order_key};
use crate::sink::{Outbound, Sink};

pub const REPLICATION_BUFFER: usize = 64;
pub const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

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
    #[error("replication slot {0} does not exist")]
    SlotMissing(String),
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
    shutdown: &CancellationToken,
) -> Result<(), SessionError> {
    let params = provider.params().await?;
    let control = connect_control(&params).await?;
    let start = catalog::slot_confirmed_flush(control.client(), &config.slot)
        .await?
        .ok_or_else(|| SessionError::SlotMissing(config.slot.clone()))?;
    for (schema, table) in
        catalog::tables_without_full_identity(control.client(), &config.publication).await?
    {
        tracing::warn!(%schema, %table, "published table does not use REPLICA IDENTITY FULL");
    }
    let mut stream = ReplicationStream::start(replication_config(&params, config, start).await?)
        .await
        .map_err(|error| stream_fault(error, start))?;
    let (reports, mut settled) = mpsc::unbounded_channel();
    let permits = Arc::new(Semaphore::new(config.sink_concurrency.max(1)));
    let mut session = Session {
        control: &control,
        types: TypeResolver::new(),
        relations: HashMap::new(),
        builder: Builder::new(config.pipeline.clone()),
        ack: AckTracker::new(start),
        lanes: Lanes::spawn(config.lane_settings(), sink, stores, &permits, &reports),
        order_by: &config.order_by,
        attribute_columns: &config.attribute_columns,
        max_inflight_bytes: config.max_inflight_bytes,
    };
    drop(reports);
    session
        .stream(&mut stream, &mut settled, shutdown, start)
        .await?;
    session.drain(&stream, &mut settled).await?;
    session.lanes.shutdown().await;
    if let Err(error) = stream.stop().await {
        tracing::warn!(%error, "replication stream did not stop cleanly");
    }
    Ok(())
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
}

impl Session<'_> {
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
        let settle_all = async {
            while self.ack.inflight_events() > 0 {
                self.report(settled.recv().await, stream)?;
            }
            Ok(())
        };
        if let Ok(result) = tokio::time::timeout(DRAIN_TIMEOUT, settle_all).await {
            return result;
        }
        tracing::warn!(
            unsettled = self.ack.inflight_events(),
            timeout_ms = DRAIN_TIMEOUT.as_millis(),
            "events still in flight after the drain timeout will replay"
        );
        Ok(())
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
}
