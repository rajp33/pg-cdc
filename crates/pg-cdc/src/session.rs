use std::collections::HashMap;
use std::time::Duration;

use pg_cdc_decoder::Lsn;
use pg_cdc_decoder::message::{DecodeError, LogicalMessage, Relation, decode};
use pg_cdc_replication::{
    ReplicationConfig, ReplicationError, ReplicationEvent, ReplicationStream,
};
use tokio::io::AsyncWrite;
use tokio_util::sync::CancellationToken;

use crate::ack::{AckError, AckProgress};
use crate::catalog::{self, CatalogError, TypeResolver};
use crate::connection::{
    CONNECT_TIMEOUT, ConnectError, ConnectParams, ConnectionProvider, ControlConnection,
    SESSION_OPTIONS, connect_control, replication_tls,
};
use crate::event::{BuildError, Builder, Built, ColumnInfo, RelationInfo};
use crate::sink::StdoutSink;

pub const REPLICATION_BUFFER: usize = 1024;
pub const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionConfig {
    pub pipeline: String,
    pub slot: String,
    pub publication: String,
    pub status_interval: Duration,
    pub buffer: usize,
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
    #[error("malformed message at {wal_start}: {source}")]
    Decode { wal_start: Lsn, source: DecodeError },
    #[error("cannot build an event at {wal_start}: {source}")]
    Build { wal_start: Lsn, source: BuildError },
    #[error("relation {oid} at {wal_start} has no Relation message on this connection")]
    UnknownRelation { wal_start: Lsn, oid: u32 },
    #[error("acknowledgement invariant violated: {0}")]
    Ack(#[from] AckError),
    #[error("sink write failed: {0}")]
    Sink(std::io::Error),
    #[error("cannot encode an event: {0}")]
    Encode(#[from] serde_json::Error),
}

impl SessionError {
    pub fn fault_lsn(&self) -> Option<Lsn> {
        match self {
            Self::Decode { wal_start, .. }
            | Self::Build { wal_start, .. }
            | Self::UnknownRelation { wal_start, .. } => Some(*wal_start),
            _ => None,
        }
    }

    pub fn is_fatal(&self) -> bool {
        matches!(self, Self::Ack(_) | Self::Sink(_) | Self::Encode(_))
    }
}

pub async fn run_session<W: AsyncWrite + Unpin + Send>(
    config: &SessionConfig,
    provider: &dyn ConnectionProvider,
    types: &mut TypeResolver,
    sink: &mut StdoutSink<W>,
    shutdown: &CancellationToken,
) -> Result<(), SessionError> {
    sink.reset_pending().await.map_err(SessionError::Sink)?;
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
    let mut stream = ReplicationStream::start(replication_config(&params, config, start)?).await?;
    let mut session = Session {
        control: &control,
        types,
        sink,
        ack: AckProgress::new(start),
        builder: Builder::new(config.pipeline.clone()),
        relations: HashMap::new(),
    };
    loop {
        let received = tokio::select! {
            biased;
            () = shutdown.cancelled() => break,
            received = stream.recv() => received,
        };
        let event = received?.ok_or(SessionError::StreamEnded)?;
        let Some(handled) = finish_or_abandon(session.handle(event), shutdown, DRAIN_TIMEOUT).await
        else {
            tracing::warn!(
                timeout_ms = DRAIN_TIMEOUT.as_millis(),
                "abandoned an event still being written at shutdown"
            );
            break;
        };
        if let Some(lsn) = handled? {
            stream.confirm(lsn);
        }
    }
    if let Err(error) = stream.stop().await {
        tracing::warn!(%error, "replication stream did not stop cleanly");
    }
    Ok(())
}

async fn finish_or_abandon<F: Future>(
    work: F,
    shutdown: &CancellationToken,
    grace: Duration,
) -> Option<F::Output> {
    let mut work = std::pin::pin!(work);
    tokio::select! {
        biased;
        output = &mut work => return Some(output),
        () = shutdown.cancelled() => {}
    }
    tokio::time::timeout(grace, work).await.ok()
}

fn replication_config(
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
        tls: replication_tls(params)?,
        slot: config.slot.clone(),
        publication: config.publication.clone(),
        start_lsn: start,
        status_interval: config.status_interval,
        connect_timeout: CONNECT_TIMEOUT,
        buffer: config.buffer,
    })
}

struct Session<'a, W> {
    control: &'a ControlConnection,
    types: &'a mut TypeResolver,
    sink: &'a mut StdoutSink<W>,
    ack: AckProgress,
    builder: Builder,
    relations: HashMap<u32, RelationInfo>,
}

impl<W: AsyncWrite + Unpin + Send> Session<'_, W> {
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
                let receipt = self.sink.flush().await.map_err(SessionError::Sink)?;
                self.ack.settle(receipt)?;
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
                    self.emit(event).await?;
                }
                return Ok(None);
            }
        };
        self.emit(built).await?;
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

    async fn emit(&mut self, built: Built) -> Result<(), SessionError> {
        for fallback in &built.fallbacks {
            tracing::error!(
                id = %built.envelope.id,
                column = %fallback.column,
                type_oid = fallback.type_oid,
                "value fell back to its text form"
            );
        }
        let body = built.envelope.encode()?;
        self.sink.write(&body).await.map_err(SessionError::Sink)?;
        self.ack.sent()?;
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

    #[tokio::test(start_paused = true)]
    async fn work_started_before_shutdown_finishes_within_the_grace_period() {
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let work = async {
            tokio::time::sleep(Duration::from_secs(4)).await;
            7
        };
        assert_eq!(
            finish_or_abandon(work, &shutdown, DRAIN_TIMEOUT).await,
            Some(7)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn work_still_running_after_the_grace_period_is_abandoned() {
        let shutdown = CancellationToken::new();
        let token = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            token.cancel();
        });
        let started = tokio::time::Instant::now();
        let work = tokio::time::sleep(Duration::from_secs(60));
        assert_eq!(
            finish_or_abandon(work, &shutdown, DRAIN_TIMEOUT).await,
            None
        );
        assert_eq!(started.elapsed(), Duration::from_secs(1) + DRAIN_TIMEOUT);
    }

    #[tokio::test(start_paused = true)]
    async fn work_runs_to_completion_without_shutdown() {
        let work = async {
            tokio::time::sleep(Duration::from_secs(600)).await;
            "done"
        };
        assert_eq!(
            finish_or_abandon(work, &CancellationToken::new(), DRAIN_TIMEOUT).await,
            Some("done")
        );
    }
}
