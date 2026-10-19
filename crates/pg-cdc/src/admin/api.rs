use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::Full;
use hyper::header::{ALLOW, CONTENT_TYPE, HeaderValue, RETRY_AFTER};
use hyper::{Response, StatusCode};
use serde::Serialize;
use serde_json::value::RawValue;
use serde_json::{Value, json};
use tokio::sync::OwnedSemaphorePermit;
use tokio::task::JoinSet;

use super::{
    Admin, HEAD_READ_CONCURRENCY, HOLDER_LOOKUP_TIMEOUT, OPERATION_TIMEOUT, PARKED_LENGTH_CAP,
    PARKED_LIMIT_DEFAULT, PARKED_LIMIT_MAX, PARKED_SCAN_LIMIT, PARKED_SHOW_BYTES,
};
use crate::event::EventId;
use crate::lane::OperateError;
use crate::metrics::{self, AdminOperation, Measurement};
use crate::order::OrderKeyId;
use crate::state::{
    LeaderRecord, ParkStatus, ParkedEntry, ParkedEvent, ParkedHead, ParkedSummary, StateStore,
    StoreError, now_ms,
};
use crate::status::ParkedAccess;

pub(super) struct Reply {
    status: StatusCode,
    body: Bytes,
    retry_after: bool,
}

struct HeldBody {
    body: Bytes,
    _turn: OwnedSemaphorePermit,
}

impl AsRef<[u8]> for HeldBody {
    fn as_ref(&self) -> &[u8] {
        &self.body
    }
}

impl Reply {
    pub(super) fn json(status: StatusCode, body: &impl Serialize) -> Self {
        match serde_json::to_vec(body) {
            Ok(bytes) => Self {
                status,
                body: Bytes::from(bytes),
                retry_after: false,
            },
            Err(error) => {
                tracing::error!(%error, "cannot encode an admin reply");
                Self {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    body: Bytes::from_static(br#"{"error":"cannot encode the reply"}"#),
                    retry_after: false,
                }
            }
        }
    }

    pub(super) fn error(status: StatusCode, message: impl std::fmt::Display) -> Self {
        Self::json(status, &json!({ "error": message.to_string() }))
    }

    fn with_retry_after(mut self) -> Self {
        self.retry_after = true;
        self
    }

    fn holding(mut self, turn: OwnedSemaphorePermit) -> Self {
        self.body = Bytes::from_owner(HeldBody {
            body: self.body,
            _turn: turn,
        });
        self
    }

    pub(super) fn into_response(self) -> Response<Full<Bytes>> {
        let mut response = Response::new(Full::new(self.body));
        *response.status_mut() = self.status;
        let headers = response.headers_mut();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        if self.retry_after {
            headers.insert(RETRY_AFTER, HeaderValue::from_static("1"));
        }
        response
    }
}

type Checked<T> = Result<T, Reply>;

fn bad_request(message: impl std::fmt::Display) -> Reply {
    Reply::error(StatusCode::BAD_REQUEST, message)
}

struct Query<'a> {
    pairs: Vec<(&'a str, &'a str)>,
}

impl<'a> Query<'a> {
    fn parse(query: Option<&'a str>, accepted: &[&str]) -> Checked<Self> {
        let mut pairs: Vec<(&str, &str)> = Vec::new();
        for piece in query.unwrap_or_default().split('&') {
            if piece.is_empty() {
                continue;
            }
            let (name, value) = piece.split_once('=').unwrap_or((piece, ""));
            if !accepted.contains(&name) {
                return Err(bad_request(format!("unknown query parameter {name:?}")));
            }
            if pairs.iter().any(|(seen, _)| *seen == name) {
                return Err(bad_request(format!("repeated query parameter {name:?}")));
            }
            pairs.push((name, value));
        }
        Ok(Self { pairs })
    }

    fn get(&self, name: &str) -> Option<&'a str> {
        self.pairs
            .iter()
            .find(|(seen, _)| *seen == name)
            .map(|(_, value)| *value)
    }

    fn limit(&self) -> Checked<usize> {
        let Some(text) = self.get("limit") else {
            return Ok(PARKED_LIMIT_DEFAULT);
        };
        match text.parse::<usize>() {
            Ok(limit) if (1..=PARKED_LIMIT_MAX).contains(&limit) => Ok(limit),
            _ => Err(bad_request(format!(
                "limit must be a number from 1 to {PARKED_LIMIT_MAX}, not {text:?}"
            ))),
        }
    }

    fn status(&self) -> Checked<Option<ParkStatus>> {
        match self.get("status") {
            None => Ok(None),
            Some("dead") => Ok(Some(ParkStatus::Dead)),
            Some("retrying") => Ok(Some(ParkStatus::Retrying)),
            Some(other) => Err(bad_request(format!(
                "status must be dead or retrying, not {other:?}"
            ))),
        }
    }

    fn after_key(&self) -> Checked<Option<OrderKeyId>> {
        self.get("after")
            .map(|text| text.parse().map_err(bad_request))
            .transpose()
    }

    fn after_event(&self) -> Checked<Option<EventId>> {
        self.get("after")
            .map(|text| text.parse().map_err(bad_request))
            .transpose()
    }

    fn event_id(&self) -> Checked<EventId> {
        self.get("id")
            .ok_or_else(|| bad_request("the id query parameter is required"))?
            .parse()
            .map_err(bad_request)
    }
}

fn order_key(text: &str) -> Checked<OrderKeyId> {
    text.parse().map_err(bad_request)
}

fn seconds(duration: Duration) -> f64 {
    (duration.as_secs_f64() * 1000.0).round() / 1000.0
}

#[derive(Serialize)]
struct InflightBody {
    events: u64,
    bytes: u64,
}

#[derive(Serialize)]
struct ParkedBody {
    events: u64,
    keys: u64,
    dead: u64,
    oldest_age_seconds: Option<f64>,
}

#[derive(Serialize)]
struct StatusBody<'a> {
    pipeline: &'a str,
    slot: &'a str,
    holder: &'a str,
    role: String,
    healthy: bool,
    ready: bool,
    epoch: Option<u64>,
    confirmed_lsn: Option<String>,
    inflight: InflightBody,
    parked: Option<ParkedBody>,
    status_update_age_seconds: Option<f64>,
    slot_retained_bytes: Option<u64>,
    tables_without_full_identity: u64,
}

pub(super) fn method_not_allowed(allow: &'static str) -> Response<Full<Bytes>> {
    let mut response =
        Reply::error(StatusCode::METHOD_NOT_ALLOWED, "method not allowed").into_response();
    response
        .headers_mut()
        .insert(ALLOW, HeaderValue::from_static(allow));
    response
}

pub(super) fn status(admin: &Admin) -> Reply {
    let health = admin.health.snapshot();
    let board = admin.status.snapshot();
    let now = now_ms();
    Reply::json(
        StatusCode::OK,
        &StatusBody {
            pipeline: &admin.identity.pipeline,
            slot: &admin.identity.slot,
            holder: &admin.identity.holder,
            role: health.role.to_string(),
            healthy: health.healthy(),
            ready: health.ready(),
            epoch: board.epoch,
            confirmed_lsn: board.confirmed_lsn.map(|lsn| lsn.to_string()),
            inflight: InflightBody {
                events: board.inflight_events,
                bytes: board.inflight_bytes,
            },
            parked: board.parked.map(|parked| ParkedBody {
                events: parked.events,
                keys: parked.keys,
                dead: parked.dead,
                oldest_age_seconds: parked.oldest_age(now).map(seconds),
            }),
            status_update_age_seconds: board.status_update_age.map(seconds),
            slot_retained_bytes: board.slot_retained_bytes,
            tables_without_full_identity: board.tables_without_full_identity,
        },
    )
}

async fn holder(admin: &Admin) -> Option<LeaderRecord> {
    let store = admin.store.get()?;
    tokio::time::timeout(HOLDER_LOOKUP_TIMEOUT, store.read_leader())
        .await
        .ok()?
        .ok()?
        .map(|record| record.value)
}

async fn not_active(admin: &Admin) -> Reply {
    let record = holder(admin).await;
    Reply::json(
        StatusCode::CONFLICT,
        &json!({
            "error": "this task is not the active holder",
            "role": admin.health.snapshot().role.to_string(),
            "holder": record.as_ref().map(|record| record.holder.as_str()),
            "leader_state": record.as_ref().map(|record| record.state),
            "epoch": record.as_ref().map(|record| record.epoch),
        }),
    )
}

async fn access(admin: &Admin) -> Checked<ParkedAccess> {
    if admin.store.get().is_none() {
        return Err(Reply::error(
            StatusCode::SERVICE_UNAVAILABLE,
            "pg-cdc is starting",
        ));
    }
    match admin.status.parked_access() {
        Some(access) => Ok(access),
        None => Err(not_active(admin).await),
    }
}

fn store_failure(error: &StoreError) -> Reply {
    Reply::error(StatusCode::SERVICE_UNAVAILABLE, error)
}

async fn bounded(operation: impl Future<Output = Reply>) -> Reply {
    tokio::time::timeout(OPERATION_TIMEOUT, operation)
        .await
        .unwrap_or_else(|_| {
            Reply::error(
                StatusCode::GATEWAY_TIMEOUT,
                format!(
                    "no answer within {}s; the operation may still complete",
                    OPERATION_TIMEOUT.as_secs()
                ),
            )
        })
}

async fn inspect(admin: &Admin, inspection: impl Future<Output = Reply>) -> Reply {
    let Ok(turn) = Arc::clone(&admin.inspections).acquire_owned().await else {
        return Reply::error(StatusCode::SERVICE_UNAVAILABLE, "pg-cdc is stopping");
    };
    inspection.await.holding(turn)
}

fn collapse(outcome: Checked<Reply>) -> Reply {
    outcome.unwrap_or_else(|reply| reply)
}

#[derive(Serialize)]
struct QueueBody {
    order_key: String,
    head: String,
    status: ParkStatus,
    length: u64,
    length_capped: bool,
    attempts: u32,
    first_failure_ms: u64,
    last_error: String,
    last_error_cut: bool,
}

#[derive(Serialize)]
struct QueuesBody {
    queues: Vec<QueueBody>,
    next: Option<String>,
}

struct Listing {
    wanted: Option<ParkStatus>,
    limit: usize,
}

struct Page {
    queues: Vec<QueueBody>,
    next: Option<OrderKeyId>,
}

pub(super) async fn list(admin: &Admin, query: Option<&str>) -> Reply {
    collapse(try_list(admin, query).await)
}

async fn try_list(admin: &Admin, query: Option<&str>) -> Checked<Reply> {
    let query = Query::parse(query, &["status", "limit", "after"])?;
    let listing = Listing {
        wanted: query.status()?,
        limit: query.limit()?,
    };
    let after = query.after_key()?;
    let access = access(admin).await?;
    Ok(bounded(inspect(admin, async {
        match scan(&access.store, &listing, after).await {
            Ok(page) => Reply::json(
                StatusCode::OK,
                &QueuesBody {
                    queues: page.queues,
                    next: page.next.map(|key| key.to_string()),
                },
            ),
            Err(error) => store_failure(&error),
        }
    }))
    .await)
}

async fn scan(
    store: &Arc<dyn StateStore>,
    listing: &Listing,
    mut cursor: Option<OrderKeyId>,
) -> Result<Page, StoreError> {
    let mut queues = Vec::new();
    let mut examined = 0;
    while examined < PARKED_SCAN_LIMIT {
        let chunk = listing.limit.min(PARKED_SCAN_LIMIT - examined);
        let listed = store
            .list_parked_queues(cursor.as_ref(), chunk, PARKED_LENGTH_CAP)
            .await?;
        let heads = read_heads(store, &listed.queues).await?;
        for (queue, head) in listed.queues.iter().zip(heads) {
            let Some(head) = head else { continue };
            if listing.wanted.is_some_and(|wanted| wanted != head.status) {
                continue;
            }
            queues.push(QueueBody {
                order_key: queue.order_key.to_string(),
                head: head.id.to_string(),
                status: head.status,
                length: queue.length,
                length_capped: usize::try_from(queue.length)
                    .is_ok_and(|length| length >= PARKED_LENGTH_CAP),
                attempts: head.attempts,
                first_failure_ms: head.first_failure_ms,
                last_error: head.last_error,
                last_error_cut: head.last_error_cut,
            });
            if queues.len() == listing.limit {
                let next = Some(queue.order_key);
                return Ok(Page { queues, next });
            }
        }
        let Some(next) = listed.next else {
            return Ok(Page { queues, next: None });
        };
        examined += chunk;
        cursor = Some(next);
    }
    Ok(Page {
        queues,
        next: cursor,
    })
}

async fn read_heads(
    store: &Arc<dyn StateStore>,
    queues: &[ParkedHead],
) -> Result<Vec<Option<ParkedSummary>>, StoreError> {
    let mut heads: Vec<Option<ParkedSummary>> = queues.iter().map(|_| None).collect();
    let mut pending = queues.iter().enumerate();
    let mut reading = JoinSet::new();
    loop {
        while reading.len() < HEAD_READ_CONCURRENCY
            && let Some((index, queue)) = pending.next()
        {
            let store = Arc::clone(store);
            let key = queue.order_key;
            reading.spawn(async move { (index, store.read_parked_summary(&key).await) });
        }
        let Some(joined) = reading.join_next().await else {
            return Ok(heads);
        };
        let (index, read) = joined.map_err(|error| StoreError::Unavailable(error.to_string()))?;
        if let Some(slot) = heads.get_mut(index) {
            *slot = read?;
        }
    }
}

#[derive(Serialize)]
struct EventBody<'a> {
    id: String,
    status: ParkStatus,
    attempts: u32,
    first_failure_ms: u64,
    last_error: &'a str,
    attributes: serde_json::Map<String, Value>,
    envelope: &'a RawValue,
}

#[derive(Serialize)]
#[serde(untagged)]
enum ShownBody<'a> {
    Event(EventBody<'a>),
    Withheld {
        id: String,
        bytes: u64,
        withheld: bool,
    },
}

#[derive(Serialize)]
struct EventsBody<'a> {
    order_key: String,
    events: Vec<ShownBody<'a>>,
    next: Option<String>,
}

fn shown_body(entry: &ParkedEntry) -> Result<ShownBody<'_>, StoreError> {
    match entry {
        ParkedEntry::Loaded(event) => event_body(event).map(ShownBody::Event),
        ParkedEntry::Withheld { id, bytes } => Ok(ShownBody::Withheld {
            id: id.to_string(),
            bytes: *bytes,
            withheld: true,
        }),
    }
}

fn event_body(event: &ParkedEvent) -> Result<EventBody<'_>, StoreError> {
    let envelope: &RawValue =
        serde_json::from_slice(&event.body).map_err(|error| StoreError::Malformed {
            key: crate::state::parked_name(&event.order_key, &event.id),
            reason: format!("the envelope is not JSON: {error}"),
        })?;
    Ok(EventBody {
        id: event.id.to_string(),
        status: event.status,
        attempts: event.attempts,
        first_failure_ms: event.first_failure_ms,
        last_error: &event.last_error,
        attributes: event
            .attributes
            .iter()
            .map(|(name, value)| (name.clone(), Value::from(value.as_str())))
            .collect(),
        envelope,
    })
}

pub(super) async fn show(admin: &Admin, key: &str, query: Option<&str>) -> Reply {
    collapse(try_show(admin, key, query).await)
}

async fn try_show(admin: &Admin, key: &str, query: Option<&str>) -> Checked<Reply> {
    let key = order_key(key)?;
    let query = Query::parse(query, &["after", "limit"])?;
    let limit = query.limit()?;
    let after = query.after_event()?;
    let access = access(admin).await?;
    Ok(bounded(inspect(admin, async {
        let mut slice = match access
            .store
            .read_parked_within(
                &key,
                after.as_ref(),
                limit.saturating_add(1),
                PARKED_SHOW_BYTES,
            )
            .await
        {
            Ok(slice) => slice,
            Err(error) => return store_failure(&error),
        };
        if slice.entries.is_empty() && after.is_none() {
            return Reply::error(
                StatusCode::NOT_FOUND,
                format!("order key {key} has no parked events"),
            );
        }
        let more = slice.full || slice.entries.len() > limit;
        slice.entries.truncate(limit);
        let next = slice
            .entries
            .last()
            .filter(|_| more)
            .map(|entry| entry.id().to_string());
        let bodies: Result<Vec<_>, _> = slice.entries.iter().map(shown_body).collect();
        match bodies {
            Ok(events) => Reply::json(
                StatusCode::OK,
                &EventsBody {
                    order_key: key.to_string(),
                    events,
                    next,
                },
            ),
            Err(error) => store_failure(&error),
        }
    }))
    .await)
}

pub(super) async fn redrive(admin: &Admin, key: &str, query: Option<&str>) -> Reply {
    collapse(try_redrive(admin, key, query).await)
}

async fn try_redrive(admin: &Admin, key: &str, query: Option<&str>) -> Checked<Reply> {
    let key = order_key(key)?;
    Query::parse(query, &[])?;
    let access = access(admin).await?;
    Ok(bounded(async {
        match access.operator.redrive(key).await {
            Ok(id) => Reply::json(
                StatusCode::OK,
                &json!({"order_key": key.to_string(), "id": id.to_string(), "status": "retrying"}),
            ),
            Err(error) => operate_failure(admin, AdminOperation::Redrive, error).await,
        }
    })
    .await)
}

pub(super) async fn discard(admin: &Admin, key: &str, query: Option<&str>) -> Reply {
    collapse(try_discard(admin, key, query).await)
}

async fn try_discard(admin: &Admin, key: &str, query: Option<&str>) -> Checked<Reply> {
    let key = order_key(key)?;
    let query = Query::parse(query, &["id"])?;
    let id = query.event_id()?;
    let access = access(admin).await?;
    Ok(bounded(async {
        match access.operator.discard(key, id).await {
            Ok(id) => Reply::json(
                StatusCode::OK,
                &json!({"order_key": key.to_string(), "discarded": id.to_string()}),
            ),
            Err(error) => operate_failure(admin, AdminOperation::Discard, error).await,
        }
    })
    .await)
}

async fn operate_failure(admin: &Admin, operation: AdminOperation, error: OperateError) -> Reply {
    match error {
        OperateError::NotParked(_) => Reply::error(StatusCode::NOT_FOUND, &error),
        OperateError::Busy(_) => {
            metrics::record(Measurement::AdminRejected { operation });
            Reply::error(StatusCode::SERVICE_UNAVAILABLE, &error).with_retry_after()
        }
        OperateError::Loading(_) => {
            Reply::error(StatusCode::SERVICE_UNAVAILABLE, &error).with_retry_after()
        }
        OperateError::NotDead { head, .. } | OperateError::HeadMismatch { head, .. } => {
            Reply::json(
                StatusCode::CONFLICT,
                &json!({"error": error.to_string(), "head": head.to_string()}),
            )
        }
        OperateError::Parking { .. } => Reply::error(StatusCode::CONFLICT, &error),
        OperateError::Store(ref store) => store_failure(store),
        OperateError::Stopped => not_active(admin).await,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, MutexGuard};
    use std::time::{Duration, SystemTime};

    use async_trait::async_trait;
    use bytes::Bytes;
    use http_body_util::BodyExt as _;
    use hyper::header::{ALLOW, CONTENT_TYPE, RETRY_AFTER};
    use hyper::{HeaderMap, Method, StatusCode};
    use pg_cdc_decoder::Lsn;
    use pg_cdc_replication::StatusClock;
    use serde_json::{Value, json};
    use tokio::sync::{Semaphore, mpsc};
    use tokio::time::Instant;

    use super::operate_failure;
    use crate::admin::{
        Admin, HEAD_READ_CONCURRENCY, HOLDER_LOOKUP_TIMEOUT, Identity, OPERATION_TIMEOUT,
        PARKED_LENGTH_CAP, PARKED_SCAN_LIMIT, PARKED_SHOW_BYTES, PARKED_SHOW_CONCURRENCY, route,
    };
    use crate::event::EventId;
    use crate::health::{Health, Role};
    use crate::lane::{LaneSettings, Lanes, OperateError, Stores};
    use crate::metrics::{AdminOperation, Measurement, Recorder};
    use crate::order::OrderKeyId;
    use crate::sink::{AttributeValue, Outbound, Outcome, Sink, SinkCapabilities, SinkError};
    use crate::state::PARKED_SUMMARY_BYTES;
    use crate::state::memory::MemoryStateStore;
    use crate::state::{
        LeaderRecord, LeaderState, ParkStatus, ParkedEntry, ParkedEvent, ParkedHead, ParkedQueues,
        ParkedSlice, ParkedSummary, StateStore, StoreError, SwapOutcome, Version, Versioned,
        now_ms,
    };
    use crate::status::{LaneCounts, ParkedAccess, ParkedGauges, Status};

    #[derive(Debug, Default)]
    struct Counts {
        calls: usize,
        heads: usize,
        lists: Vec<(usize, usize, usize)>,
        reads: Vec<(usize, usize)>,
        loaded_bytes: Vec<usize>,
    }

    #[derive(Debug, Default)]
    struct Races {
        drain_while_listing: HashSet<OrderKeyId>,
        deliver_before_reading: Option<OrderKeyId>,
    }

    #[derive(Debug, Default)]
    struct Reads {
        delays: HashMap<OrderKeyId, Duration>,
        running: usize,
        peak_running: usize,
    }

    #[derive(Debug)]
    struct TrackedBody {
        bytes: Bytes,
        live: Arc<AtomicUsize>,
    }

    impl TrackedBody {
        fn wrap(bytes: Bytes, live: &Arc<AtomicUsize>, peak: &AtomicUsize) -> Bytes {
            let now = live.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(now, Ordering::SeqCst);
            Bytes::from_owner(Self {
                bytes,
                live: Arc::clone(live),
            })
        }
    }

    impl AsRef<[u8]> for TrackedBody {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }

    impl Drop for TrackedBody {
        fn drop(&mut self) {
            self.live.fetch_sub(1, Ordering::SeqCst);
        }
    }

    #[derive(Debug)]
    struct CountingStore {
        inner: MemoryStateStore,
        counts: Mutex<Counts>,
        failing: AtomicBool,
        stalls_leader: AtomicBool,
        races: Mutex<Races>,
        reads: Mutex<Reads>,
        live_bodies: Arc<AtomicUsize>,
        peak_bodies: AtomicUsize,
    }

    impl CountingStore {
        fn over(inner: &MemoryStateStore) -> Arc<Self> {
            Arc::new(Self {
                inner: inner.clone(),
                counts: Mutex::new(Counts::default()),
                failing: AtomicBool::new(false),
                stalls_leader: AtomicBool::new(false),
                races: Mutex::new(Races::default()),
                reads: Mutex::new(Reads::default()),
                live_bodies: Arc::new(AtomicUsize::new(0)),
                peak_bodies: AtomicUsize::new(0),
            })
        }

        fn counts(&self) -> MutexGuard<'_, Counts> {
            self.counts.lock().unwrap()
        }

        fn races(&self) -> MutexGuard<'_, Races> {
            self.races.lock().unwrap()
        }

        fn reads(&self) -> MutexGuard<'_, Reads> {
            self.reads.lock().unwrap()
        }

        async fn drain(&self, key: &OrderKeyId) {
            let events = self.inner.read_parked(key, None, usize::MAX).await.unwrap();
            let ids: Vec<_> = events.iter().map(|event| (*key, event.id)).collect();
            self.inner.remove_parked(&ids).await.unwrap();
        }

        async fn deliver_head(&self, key: &OrderKeyId) {
            let head = self.inner.read_parked(key, None, 1).await.unwrap();
            let ids: Vec<_> = head.iter().map(|event| (*key, event.id)).collect();
            self.inner.remove_parked(&ids).await.unwrap();
        }

        fn count(&self) -> Result<(), StoreError> {
            self.counts().calls += 1;
            if self.failing.load(Ordering::SeqCst) {
                return Err(StoreError::Unavailable("down".to_owned()));
            }
            Ok(())
        }
    }

    #[async_trait]
    impl StateStore for CountingStore {
        async fn append_parked(&self, events: &[ParkedEvent]) -> Result<(), StoreError> {
            self.count()?;
            self.inner.append_parked(events).await
        }

        async fn load_parked_heads(&self, partition: u8) -> Result<Vec<ParkedHead>, StoreError> {
            self.count()?;
            self.counts().heads += 1;
            self.inner.load_parked_heads(partition).await
        }

        async fn list_parked_queues(
            &self,
            after: Option<&OrderKeyId>,
            limit: usize,
            length_cap: usize,
        ) -> Result<ParkedQueues, StoreError> {
            self.count()?;
            let mut listed = self
                .inner
                .list_parked_queues(after, limit, length_cap)
                .await?;
            let draining: Vec<OrderKeyId> = listed
                .queues
                .iter()
                .map(|queue| queue.order_key)
                .filter(|key| self.races().drain_while_listing.remove(key))
                .collect();
            for key in &draining {
                self.drain(key).await;
            }
            listed
                .queues
                .retain(|queue| !draining.contains(&queue.order_key));
            self.counts()
                .lists
                .push((limit, listed.queues.len(), length_cap));
            Ok(listed)
        }

        async fn read_parked(
            &self,
            key: &OrderKeyId,
            after: Option<&EventId>,
            limit: usize,
        ) -> Result<Vec<ParkedEvent>, StoreError> {
            self.count()?;
            if self.races().deliver_before_reading == Some(*key) {
                self.races().deliver_before_reading = None;
                self.deliver_head(key).await;
            }
            let delay = {
                let mut reads = self.reads();
                reads.running += 1;
                reads.peak_running = reads.peak_running.max(reads.running);
                reads.delays.get(key).copied()
            };
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
            let read = self.inner.read_parked(key, after, limit).await;
            self.reads().running -= 1;
            let read: Vec<_> = read?
                .into_iter()
                .map(|mut event| {
                    event.body =
                        TrackedBody::wrap(event.body, &self.live_bodies, &self.peak_bodies);
                    event
                })
                .collect();
            self.counts().reads.push((limit, read.len()));
            Ok(read)
        }

        async fn read_parked_within(
            &self,
            key: &OrderKeyId,
            after: Option<&EventId>,
            limit: usize,
            max_bytes: u64,
        ) -> Result<ParkedSlice, StoreError> {
            self.count()?;
            let delay = {
                let mut reads = self.reads();
                reads.running += 1;
                reads.peak_running = reads.peak_running.max(reads.running);
                reads.delays.get(key).copied()
            };
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
            let read = self
                .inner
                .read_parked_within(key, after, limit, max_bytes)
                .await;
            self.reads().running -= 1;
            let mut read = read?;
            let mut bytes = 0;
            for entry in &mut read.entries {
                if let ParkedEntry::Loaded(event) = entry {
                    bytes += event.body.len();
                    event.body =
                        TrackedBody::wrap(event.body.clone(), &self.live_bodies, &self.peak_bodies);
                }
            }
            let mut counts = self.counts();
            counts.reads.push((limit, read.entries.len()));
            counts.loaded_bytes.push(bytes);
            drop(counts);
            Ok(read)
        }

        async fn read_parked_summary(
            &self,
            key: &OrderKeyId,
        ) -> Result<Option<ParkedSummary>, StoreError> {
            self.count()?;
            if self.races().deliver_before_reading == Some(*key) {
                self.races().deliver_before_reading = None;
                self.deliver_head(key).await;
            }
            let delay = {
                let mut reads = self.reads();
                reads.running += 1;
                reads.peak_running = reads.peak_running.max(reads.running);
                reads.delays.get(key).copied()
            };
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
            let read = self.inner.read_parked_summary(key).await;
            self.reads().running -= 1;
            let read = read?;
            self.counts().reads.push((1, usize::from(read.is_some())));
            Ok(read)
        }

        async fn set_parked_status(
            &self,
            key: &OrderKeyId,
            id: &EventId,
            status: ParkStatus,
        ) -> Result<(), StoreError> {
            self.count()?;
            self.inner.set_parked_status(key, id, status).await
        }

        async fn remove_parked(&self, events: &[(OrderKeyId, EventId)]) -> Result<(), StoreError> {
            self.count()?;
            self.inner.remove_parked(events).await
        }

        async fn read_leader(&self) -> Result<Option<Versioned<LeaderRecord>>, StoreError> {
            self.count()?;
            if self.stalls_leader.load(Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            self.inner.read_leader().await
        }

        async fn swap_leader(
            &self,
            next: &LeaderRecord,
            expected: Option<&Version>,
        ) -> Result<SwapOutcome, StoreError> {
            self.count()?;
            self.inner.swap_leader(next, expected).await
        }
    }

    #[derive(Debug)]
    struct TestSink {
        hangs: bool,
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
            if self.hangs {
                std::future::pending::<()>().await;
            }
            batch
                .iter()
                .map(|_| Outcome::Retryable(SinkError("down".to_owned())))
                .collect()
        }
    }

    struct Fixture {
        admin: Admin,
        status: Status,
        health: Health,
        lanes: Lanes,
        _reports: mpsc::UnboundedReceiver<crate::lane::Report>,
    }

    impl Fixture {
        async fn start(
            lanes_over: &MemoryStateStore,
            access: Arc<dyn StateStore>,
            hangs: bool,
        ) -> Self {
            let health = Health::new();
            let status = Status::new();
            let admin = Admin::new(health.clone(), status.clone(), identity());
            let raw: Arc<dyn StateStore> = Arc::new(lanes_over.clone());
            admin.set_state_store(Arc::clone(&raw));
            let stores = Stores {
                state: raw,
                payload: None,
            };
            let sink: Arc<dyn Sink> = Arc::new(TestSink { hangs });
            let (reports, receiver) = mpsc::unbounded_channel();
            let lanes = Lanes::spawn(
                LaneSettings {
                    lanes: 1,
                    park_after: Duration::from_secs(3600),
                    max_parked_events: 100_000,
                    store_concurrency: 2,
                },
                &sink,
                &stores,
                &Arc::new(Semaphore::new(1)),
                &reports,
            );
            status.set_parked_access(Some(ParkedAccess {
                operator: lanes.operator(),
                store: access,
            }));
            tokio::time::sleep(Duration::from_millis(1)).await;
            Self {
                admin,
                status,
                health,
                lanes,
                _reports: receiver,
            }
        }

        async fn active(store: &MemoryStateStore) -> Self {
            Self::start(store, Arc::new(store.clone()), false).await
        }

        async fn reading(access: Arc<dyn StateStore>) -> Self {
            Self::start(&MemoryStateStore::new(), access, false).await
        }

        async fn stop(self) {
            self.lanes.stop(Duration::from_secs(1)).await;
        }
    }

    fn identity() -> Identity {
        Identity {
            pipeline: "orders".to_owned(),
            slot: "pgcdc_orders".to_owned(),
            holder: "host-1-0a1b2c3d".to_owned(),
        }
    }

    struct Answer {
        status: StatusCode,
        headers: HeaderMap,
        body: Value,
    }

    async fn call(admin: &Admin, method: Method, target: &str) -> Answer {
        let response = route(&method, &target.parse().unwrap(), admin).await;
        let (parts, body) = response.into_parts();
        let bytes = body.collect().await.unwrap().to_bytes();
        let body = serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
        Answer {
            status: parts.status,
            headers: parts.headers,
            body,
        }
    }

    async fn get(admin: &Admin, target: &str) -> Answer {
        call(admin, Method::GET, target).await
    }

    async fn post(admin: &Admin, target: &str) -> Answer {
        call(admin, Method::POST, target).await
    }

    fn event_id(lsn: u64, ordinal: u32) -> EventId {
        EventId::new(Lsn(lsn), ordinal)
    }

    fn parked(key: OrderKeyId, lsn: u64, ordinal: u32, status: ParkStatus) -> ParkedEvent {
        ParkedEvent {
            order_key: key,
            id: event_id(lsn, ordinal),
            status,
            attempts: 3,
            first_failure_ms: 1000,
            last_error: "seeded failure".to_owned(),
            attributes: vec![
                (
                    "table".to_owned(),
                    AttributeValue::String("orders".to_owned()),
                ),
                (
                    "amount".to_owned(),
                    AttributeValue::Number("12.50".to_owned()),
                ),
            ],
            body: Bytes::from(format!(r#"{{"n":{ordinal},"lsn":{lsn}}}"#)),
        }
    }

    async fn seed(store: &MemoryStateStore, events: &[ParkedEvent]) {
        store.append_parked(events).await.unwrap();
    }

    fn named(name: &str) -> OrderKeyId {
        OrderKeyId::of(name.as_bytes())
    }

    fn sorted_keys(count: usize) -> Vec<OrderKeyId> {
        let mut keys: Vec<_> = (0..count)
            .map(|index| named(&format!("key-{index}")))
            .collect();
        keys.sort();
        keys
    }

    fn id_text(lsn: u64, ordinal: u32) -> String {
        event_id(lsn, ordinal).to_string()
    }

    fn queue_keys(answer: &Answer) -> Vec<String> {
        answer.body["queues"]
            .as_array()
            .unwrap()
            .iter()
            .map(|queue| queue["order_key"].as_str().unwrap().to_owned())
            .collect()
    }

    #[tokio::test]
    async fn status_reports_the_board_and_health_as_json() {
        let health = Health::new();
        health.set_sink_ready(true);
        health.set_store_ready(true);
        health.set_validated(true);
        health.set_role(Role::Active);
        health.set_lease_valid(true);
        health.set_delivering(true);
        let status = Status::new();
        let admin = Admin::new(health, status.clone(), identity());
        status.set_epoch(Some(2));
        status.set_confirmed(Some(Lsn(0x16B_3740)));
        status.set_inflight(3, 300);
        let gauges = ParkedGauges::new(1, Arc::new(AtomicU64::new(4)));
        gauges.publish(
            0,
            LaneCounts {
                keys: 2,
                dead: 1,
                oldest_ms: Some(now_ms() - 1000),
            },
        );
        status.set_parked(Some(gauges));
        status.set_slot_retained_bytes(Some(1024));
        status.set_tables_without_full_identity(1);
        let clock = StatusClock::default();
        clock.record(SystemTime::now() - Duration::from_secs(2));
        status.set_status_clock(Some(clock));

        let answer = get(&admin, "/status").await;
        assert_eq!(answer.status, 200);
        assert_eq!(answer.headers[CONTENT_TYPE], "application/json");
        let mut body = answer.body;
        let oldest = body["parked"]["oldest_age_seconds"].as_f64().unwrap();
        assert!((1.0..60.0).contains(&oldest), "{oldest}");
        let update_age = body["status_update_age_seconds"].as_f64().unwrap();
        assert!((2.0..60.0).contains(&update_age), "{update_age}");
        body["parked"]["oldest_age_seconds"] = Value::Null;
        body["status_update_age_seconds"] = Value::Null;
        assert_eq!(
            body,
            json!({
                "pipeline": "orders",
                "slot": "pgcdc_orders",
                "holder": "host-1-0a1b2c3d",
                "role": "active",
                "healthy": true,
                "ready": true,
                "epoch": 2,
                "confirmed_lsn": "0/16B3740",
                "inflight": {"events": 3, "bytes": 300},
                "parked": {"events": 4, "keys": 2, "dead": 1, "oldest_age_seconds": null},
                "status_update_age_seconds": null,
                "slot_retained_bytes": 1024,
                "tables_without_full_identity": 1
            })
        );

        let standby = Admin::new(Health::new(), Status::new(), identity());
        let answer = get(&standby, "/status").await;
        assert_eq!(answer.status, 200);
        assert_eq!(
            answer.body,
            json!({
                "pipeline": "orders",
                "slot": "pgcdc_orders",
                "holder": "host-1-0a1b2c3d",
                "role": "standby",
                "healthy": false,
                "ready": false,
                "epoch": null,
                "confirmed_lsn": null,
                "inflight": {"events": 0, "bytes": 0},
                "parked": null,
                "status_update_age_seconds": null,
                "slot_retained_bytes": null,
                "tables_without_full_identity": 0
            })
        );
    }

    #[tokio::test]
    async fn parked_operations_answer_409_with_the_holder_when_not_active() {
        let key = named("a");
        let id = id_text(0x10, 0);
        let targets = [
            (Method::GET, "/parked".to_owned()),
            (Method::GET, format!("/parked/{key}")),
            (Method::POST, format!("/parked/{key}/redrive")),
            (Method::POST, format!("/parked/{key}/discard?id={id}")),
        ];
        let admin = Admin::new(Health::new(), Status::new(), identity());
        for (method, target) in &targets {
            let answer = call(&admin, method.clone(), target).await;
            assert_eq!(answer.status, 503, "{target}");
            assert_eq!(answer.body, json!({"error": "pg-cdc is starting"}));
        }

        let store = MemoryStateStore::new();
        admin.set_state_store(Arc::new(store.clone()));
        for (method, target) in &targets {
            let answer = call(&admin, method.clone(), target).await;
            assert_eq!(answer.status, 409, "{target}");
            assert_eq!(answer.headers[CONTENT_TYPE], "application/json");
            assert_eq!(
                answer.body,
                json!({
                    "error": "this task is not the active holder",
                    "role": "standby",
                    "holder": null,
                    "leader_state": null,
                    "epoch": null
                })
            );
        }

        let record = LeaderRecord {
            epoch: 3,
            holder: "other-1".to_owned(),
            state: LeaderState::Active,
            confirmed_lsn: Lsn(0x20),
            updated_ms: 1,
        };
        store.swap_leader(&record, None).await.unwrap();
        for (method, target) in &targets {
            let answer = call(&admin, method.clone(), target).await;
            assert_eq!(answer.status, 409, "{target}");
            assert_eq!(
                answer.body,
                json!({
                    "error": "this task is not the active holder",
                    "role": "standby",
                    "holder": "other-1",
                    "leader_state": "active",
                    "epoch": 3
                })
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn list_shows_each_queue_head_and_filters_by_status() {
        let (a, b) = (named("a"), named("b"));
        let store = MemoryStateStore::new();
        seed(
            &store,
            &[
                parked(a, 0x10, 0, ParkStatus::Dead),
                parked(a, 0x10, 1, ParkStatus::Retrying),
                parked(b, 0x20, 0, ParkStatus::Retrying),
                parked(b, 0x20, 1, ParkStatus::Retrying),
            ],
        )
        .await;
        let fixture = Fixture::reading(Arc::new(store)).await;
        let mut keys = [a, b];
        keys.sort();

        let answer = get(&fixture.admin, "/parked").await;
        assert_eq!(answer.status, 200);
        assert_eq!(answer.headers[CONTENT_TYPE], "application/json");
        assert_eq!(
            queue_keys(&answer),
            keys.map(|key| key.to_string()).to_vec()
        );
        assert_eq!(answer.body["next"], Value::Null);
        let queues = answer.body["queues"].as_array().unwrap();
        let of_a = queues
            .iter()
            .find(|queue| queue["order_key"] == a.to_string())
            .unwrap();
        assert_eq!(
            *of_a,
            json!({
                "order_key": a.to_string(),
                "head": id_text(0x10, 0),
                "status": "dead",
                "length": 2,
                "length_capped": false,
                "attempts": 3,
                "first_failure_ms": 1000,
                "last_error": "seeded failure",
                "last_error_cut": false
            })
        );
        assert!(queues.iter().all(|queue| queue["length"] == 2));

        let dead = get(&fixture.admin, "/parked?status=dead").await;
        assert_eq!(queue_keys(&dead), vec![a.to_string()]);
        let retrying = get(&fixture.admin, "/parked?status=retrying").await;
        assert_eq!(queue_keys(&retrying), vec![b.to_string()]);
        fixture.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn listing_pages_with_a_cursor_and_bounds_each_request() {
        let keys = sorted_keys(5);
        let store = MemoryStateStore::new();
        for key in &keys {
            seed(&store, &[parked(*key, 0x10, 0, ParkStatus::Retrying)]).await;
        }
        let fixture = Fixture::reading(Arc::new(store)).await;
        let first = get(&fixture.admin, "/parked?limit=2").await;
        assert_eq!(
            queue_keys(&first),
            vec![keys[0].to_string(), keys[1].to_string()]
        );
        assert_eq!(first.body["next"], keys[1].to_string());
        let second = get(
            &fixture.admin,
            &format!("/parked?limit=2&after={}", keys[1]),
        )
        .await;
        assert_eq!(
            queue_keys(&second),
            vec![keys[2].to_string(), keys[3].to_string()]
        );
        assert_eq!(second.body["next"], keys[3].to_string());
        let last = get(
            &fixture.admin,
            &format!("/parked?limit=2&after={}", keys[3]),
        )
        .await;
        assert_eq!(queue_keys(&last), vec![keys[4].to_string()]);
        assert_eq!(last.body["next"], Value::Null);
        fixture.stop().await;

        let keys = sorted_keys(PARKED_SCAN_LIMIT + 6);
        let store = MemoryStateStore::new();
        let mut events = Vec::new();
        for (index, key) in keys.iter().enumerate() {
            let status = if index == keys.len() - 1 {
                ParkStatus::Dead
            } else {
                ParkStatus::Retrying
            };
            events.push(parked(*key, 0x10, 0, status));
        }
        for ordinal in 1..5000 {
            events.push(parked(keys[0], 0x10, ordinal, ParkStatus::Retrying));
        }
        seed(&store, &events).await;
        let counting = CountingStore::over(&store);
        let fixture = Fixture::reading(counting.clone()).await;

        let dead = get(&fixture.admin, "/parked?status=dead").await;
        assert_eq!(dead.status, 200);
        assert_eq!(queue_keys(&dead), Vec::<String>::new());
        assert_eq!(dead.body["next"], keys[PARKED_SCAN_LIMIT - 1].to_string());
        {
            let counts = counting.counts();
            assert_eq!(counts.heads, 0);
            assert_eq!(
                counts
                    .lists
                    .iter()
                    .map(|(_, returned, _)| returned)
                    .sum::<usize>(),
                PARKED_SCAN_LIMIT
            );
            assert!(
                counts
                    .lists
                    .iter()
                    .all(|(asked, _, cap)| *cap == PARKED_LENGTH_CAP && *asked == 100)
            );
            assert!(counts.reads.len() <= PARKED_SCAN_LIMIT);
            assert!(counts.reads.iter().all(|(limit, _)| *limit == 1));
        }

        let after = format!("/parked?status=dead&after={}", keys[PARKED_SCAN_LIMIT - 1]);
        let rest = get(&fixture.admin, &after).await;
        assert_eq!(queue_keys(&rest), vec![keys[keys.len() - 1].to_string()]);
        assert_eq!(rest.body["next"], Value::Null);

        let hot = get(&fixture.admin, "/parked?limit=1").await;
        assert_eq!(queue_keys(&hot), vec![keys[0].to_string()]);
        assert_eq!(hot.body["queues"][0]["length"], PARKED_LENGTH_CAP);
        assert_eq!(hot.body["queues"][0]["length_capped"], true);
        assert_eq!(hot.body["next"], keys[0].to_string());
        let asked = counting.counts().lists.last().map(|(asked, _, _)| *asked);
        assert_eq!(asked, Some(1));
        fixture.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_failing_store_answers_503_and_a_drained_queue_is_skipped() {
        let (a, b) = (named("a"), named("b"));
        let store = MemoryStateStore::new();
        seed(
            &store,
            &[
                parked(a, 0x10, 0, ParkStatus::Retrying),
                parked(b, 0x20, 0, ParkStatus::Retrying),
            ],
        )
        .await;
        let counting = CountingStore::over(&store);
        let fixture = Fixture::reading(counting.clone()).await;
        counting.failing.store(true, Ordering::SeqCst);
        for target in ["/parked".to_owned(), format!("/parked/{a}")] {
            let answer = get(&fixture.admin, &target).await;
            assert_eq!(answer.status, 503, "{target}");
            assert_eq!(
                answer.body,
                json!({"error": "state store unavailable: down"})
            );
        }
        counting.failing.store(false, Ordering::SeqCst);
        store
            .remove_parked(&[(a, event_id(0x10, 0))])
            .await
            .unwrap();
        let answer = get(&fixture.admin, "/parked").await;
        assert_eq!(queue_keys(&answer), vec![b.to_string()]);
        fixture.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_queue_draining_during_the_listing_does_not_end_the_pages() {
        let keys = sorted_keys(3);
        let store = MemoryStateStore::new();
        for key in &keys {
            seed(&store, &[parked(*key, 0x10, 0, ParkStatus::Dead)]).await;
        }
        let counting = CountingStore::over(&store);
        counting.races().drain_while_listing.insert(keys[0]);
        let fixture = Fixture::reading(counting.clone()).await;

        let answer = get(&fixture.admin, "/parked?status=dead&limit=2").await;
        assert_eq!(answer.status, 200);
        assert_eq!(
            queue_keys(&answer),
            vec![keys[1].to_string(), keys[2].to_string()]
        );
        assert_eq!(answer.body["next"], keys[2].to_string());
        fixture.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn queues_draining_during_the_listing_count_against_the_scan_limit() {
        let keys = sorted_keys(PARKED_SCAN_LIMIT + 3);
        let store = MemoryStateStore::new();
        let events: Vec<_> = keys
            .iter()
            .map(|key| parked(*key, 0x10, 0, ParkStatus::Dead))
            .collect();
        seed(&store, &events).await;
        let counting = CountingStore::over(&store);
        counting
            .races()
            .drain_while_listing
            .extend(keys[..PARKED_SCAN_LIMIT + 2].iter().copied());
        let fixture = Fixture::reading(counting.clone()).await;

        let first = get(&fixture.admin, "/parked?status=dead&limit=1").await;
        assert_eq!(first.status, 200);
        assert_eq!(queue_keys(&first), Vec::<String>::new());
        assert_eq!(first.body["next"], keys[PARKED_SCAN_LIMIT - 1].to_string());
        {
            let counts = counting.counts();
            assert_eq!(counts.lists.len(), PARKED_SCAN_LIMIT);
            assert!(counts.lists.iter().all(|(asked, _, _)| *asked == 1));
            assert_eq!(counts.reads, Vec::<(usize, usize)>::new());
        }

        let after = format!(
            "/parked?status=dead&limit=1&after={}",
            keys[PARKED_SCAN_LIMIT - 1]
        );
        let second = get(&fixture.admin, &after).await;
        let live = keys[PARKED_SCAN_LIMIT + 2].to_string();
        assert_eq!(queue_keys(&second), vec![live.clone()]);
        assert_eq!(second.body["next"], live);
        assert_eq!(counting.counts().lists.len(), PARKED_SCAN_LIMIT + 3);

        let last = get(&fixture.admin, &format!("/parked?status=dead&after={live}")).await;
        assert_eq!(queue_keys(&last), Vec::<String>::new());
        assert_eq!(last.body["next"], Value::Null);
        fixture.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_head_delivered_between_listing_and_reading_is_reported_as_its_successor() {
        let (a, b) = (named("a"), named("b"));
        let successor = |key, lsn| ParkedEvent {
            attempts: 7,
            last_error: "second failure".to_owned(),
            ..parked(key, lsn, 1, ParkStatus::Retrying)
        };
        let store = MemoryStateStore::new();
        seed(
            &store,
            &[
                parked(a, 0x10, 0, ParkStatus::Dead),
                successor(a, 0x10),
                parked(b, 0x20, 0, ParkStatus::Dead),
                successor(b, 0x20),
            ],
        )
        .await;
        let counting = CountingStore::over(&store);
        let fixture = Fixture::reading(counting.clone()).await;

        counting.races().deliver_before_reading = Some(a);
        let answer = get(&fixture.admin, "/parked").await;
        let queues = answer.body["queues"].as_array().unwrap();
        let of_a = queues
            .iter()
            .find(|queue| queue["order_key"] == a.to_string())
            .unwrap();
        assert_eq!(of_a["head"], id_text(0x10, 1));
        assert_eq!(of_a["status"], "retrying");
        assert_eq!(of_a["attempts"], 7);
        assert_eq!(of_a["last_error"], "second failure");

        counting.races().deliver_before_reading = Some(b);
        let retrying = get(&fixture.admin, "/parked?status=retrying").await;
        let mut keys = [a, b];
        keys.sort();
        assert_eq!(
            queue_keys(&retrying),
            keys.map(|key| key.to_string()).to_vec()
        );
        let heads: Vec<_> = retrying.body["queues"]
            .as_array()
            .unwrap()
            .iter()
            .map(|queue| (queue["order_key"].clone(), queue["head"].clone()))
            .collect();
        assert!(heads.contains(&(json!(b.to_string()), json!(id_text(0x20, 1)))));
        let dead = get(&fixture.admin, "/parked?status=dead").await;
        assert_eq!(queue_keys(&dead), Vec::<String>::new());
        fixture.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn head_reads_run_at_most_sixteen_at_once_and_keep_key_order() {
        let keys = sorted_keys(40);
        let store = MemoryStateStore::new();
        for key in &keys {
            seed(&store, &[parked(*key, 0x10, 0, ParkStatus::Dead)]).await;
        }
        let counting = CountingStore::over(&store);
        {
            let mut reads = counting.reads();
            for (index, key) in keys.iter().enumerate() {
                let late = u64::try_from(keys.len() - index).unwrap();
                reads.delays.insert(*key, Duration::from_millis(late));
            }
        }
        let fixture = Fixture::reading(counting.clone()).await;

        let first = get(&fixture.admin, "/parked?limit=30").await;
        assert_eq!(first.status, 200);
        let expected: Vec<_> = keys.iter().take(30).map(ToString::to_string).collect();
        assert_eq!(queue_keys(&first), expected);
        assert_eq!(first.body["next"], keys[29].to_string());
        assert_eq!(counting.reads().peak_running, HEAD_READ_CONCURRENCY);

        let rest = get(&fixture.admin, &format!("/parked?after={}", keys[29])).await;
        let expected: Vec<_> = keys.iter().skip(30).map(ToString::to_string).collect();
        assert_eq!(queue_keys(&rest), expected);
        assert_eq!(rest.body["next"], Value::Null);
        assert_eq!(counting.reads().peak_running, HEAD_READ_CONCURRENCY);
        fixture.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_listing_cuts_a_last_error_too_long_to_list() {
        let (a, b) = (named("a"), named("b"));
        let long = "e".repeat(PARKED_SUMMARY_BYTES);
        let store = MemoryStateStore::new();
        seed(
            &store,
            &[
                ParkedEvent {
                    last_error: long.clone(),
                    ..parked(a, 0x10, 0, ParkStatus::Dead)
                },
                parked(b, 0x20, 0, ParkStatus::Dead),
            ],
        )
        .await;
        let fixture = Fixture::reading(Arc::new(store)).await;

        let answer = get(&fixture.admin, "/parked").await;
        assert_eq!(answer.status, 200);
        let queues = answer.body["queues"].as_array().unwrap();
        let of = |key: OrderKeyId| {
            queues
                .iter()
                .find(|queue| queue["order_key"] == key.to_string())
                .unwrap()
        };
        let cut = of(a)["last_error"].as_str().unwrap();
        assert!(long.starts_with(cut) && cut.len() < long.len(), "{cut}");
        assert_eq!(of(a)["last_error_cut"], true);
        assert_eq!(of(b)["last_error"], "seeded failure");
        assert_eq!(of(b)["last_error_cut"], false);
        fixture.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_listings_load_no_head_envelopes() {
        let keys = sorted_keys(40);
        let store = MemoryStateStore::new();
        let large = Bytes::from(format!(r#"{{"pad":"{}"}}"#, "x".repeat(256 * 1024)));
        for key in &keys {
            let event = ParkedEvent {
                body: large.clone(),
                ..parked(*key, 0x10, 0, ParkStatus::Dead)
            };
            seed(&store, &[event]).await;
        }
        let counting = CountingStore::over(&store);
        {
            let mut reads = counting.reads();
            for key in &keys {
                reads.delays.insert(*key, Duration::from_millis(10));
            }
        }
        let fixture = Fixture::reading(counting.clone()).await;

        let mut listings = tokio::task::JoinSet::new();
        for _ in 0..4 {
            let admin = fixture.admin.clone();
            listings.spawn(async move { get(&admin, "/parked?status=dead").await });
        }
        while let Some(answer) = listings.join_next().await {
            let answer = answer.unwrap();
            assert_eq!(answer.status, 200);
            assert_eq!(queue_keys(&answer).len(), keys.len());
            assert!(
                answer.body["queues"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|queue| queue["last_error"] == "seeded failure")
            );
        }
        assert_eq!(counting.counts().reads.len(), 4 * keys.len());
        assert_eq!(counting.peak_bodies.load(Ordering::SeqCst), 0);
        fixture.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_listing_waits_for_the_inspection_budget() {
        let a = named("a");
        let store = MemoryStateStore::new();
        seed(&store, &[parked(a, 0x10, 0, ParkStatus::Dead)]).await;
        let counting = CountingStore::over(&store);
        let fixture = Fixture::reading(counting.clone()).await;

        let budget = u32::try_from(PARKED_SHOW_CONCURRENCY).unwrap();
        let held = Arc::clone(&fixture.admin.inspections)
            .acquire_many_owned(budget)
            .await
            .unwrap();
        let waiting = get(&fixture.admin, "/parked").await;
        assert_eq!(waiting.status, 504);
        {
            let counts = counting.counts();
            assert!(counts.lists.is_empty(), "{:?}", counts.lists);
            assert!(counts.reads.is_empty(), "{:?}", counts.reads);
        }
        drop(held);
        let answer = get(&fixture.admin, "/parked").await;
        assert_eq!(answer.status, 200);
        assert_eq!(queue_keys(&answer), vec![a.to_string()]);
        fixture.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_leader_lookup_that_fails_or_stalls_still_answers_409_with_nulls() {
        let key = named("a");
        let id = id_text(0x10, 0);
        let targets = [
            (Method::GET, "/parked".to_owned()),
            (Method::GET, format!("/parked/{key}")),
            (Method::POST, format!("/parked/{key}/redrive")),
            (Method::POST, format!("/parked/{key}/discard?id={id}")),
        ];
        let store = MemoryStateStore::new();
        let record = LeaderRecord {
            epoch: 4,
            holder: "other-2".to_owned(),
            state: LeaderState::Released,
            confirmed_lsn: Lsn(0x30),
            updated_ms: 1,
        };
        store.swap_leader(&record, None).await.unwrap();
        let counting = CountingStore::over(&store);
        let admin = Admin::new(Health::new(), Status::new(), identity());
        admin.set_state_store(counting.clone());
        let unknown = json!({
            "error": "this task is not the active holder",
            "role": "standby",
            "holder": null,
            "leader_state": null,
            "epoch": null
        });

        for (method, target) in &targets {
            let answer = call(&admin, method.clone(), target).await;
            assert_eq!(answer.status, 409, "{target}");
            assert_eq!(
                answer.body,
                json!({
                    "error": "this task is not the active holder",
                    "role": "standby",
                    "holder": "other-2",
                    "leader_state": "released",
                    "epoch": 4
                }),
                "{target}"
            );
        }

        counting.failing.store(true, Ordering::SeqCst);
        for (method, target) in &targets {
            let answer = call(&admin, method.clone(), target).await;
            assert_eq!(answer.status, 409, "{target}");
            assert_eq!(answer.headers[CONTENT_TYPE], "application/json");
            assert_eq!(answer.body, unknown, "{target}");
        }

        counting.failing.store(false, Ordering::SeqCst);
        counting.stalls_leader.store(true, Ordering::SeqCst);
        for (method, target) in &targets {
            let started = Instant::now();
            let answer = call(&admin, method.clone(), target).await;
            let waited = started.elapsed();
            assert_eq!(answer.status, 409, "{target}");
            assert_eq!(answer.body, unknown, "{target}");
            assert!(
                (HOLDER_LOOKUP_TIMEOUT..HOLDER_LOOKUP_TIMEOUT + Duration::from_secs(1))
                    .contains(&waited),
                "{target} waited {waited:?}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn show_returns_a_queues_events_with_their_envelopes() {
        let a = named("a");
        let store = MemoryStateStore::new();
        seed(
            &store,
            &[
                parked(a, 0x10, 0, ParkStatus::Dead),
                parked(a, 0x10, 1, ParkStatus::Retrying),
            ],
        )
        .await;
        let fixture = Fixture::reading(Arc::new(store)).await;
        let attributes = json!({"table": "orders", "amount": "12.50"});

        let all = get(&fixture.admin, &format!("/parked/{a}")).await;
        assert_eq!(all.status, 200);
        assert_eq!(
            all.body,
            json!({
                "order_key": a.to_string(),
                "events": [
                    {
                        "id": id_text(0x10, 0),
                        "status": "dead",
                        "attempts": 3,
                        "first_failure_ms": 1000,
                        "last_error": "seeded failure",
                        "attributes": attributes,
                        "envelope": {"n": 0, "lsn": 16}
                    },
                    {
                        "id": id_text(0x10, 1),
                        "status": "retrying",
                        "attempts": 3,
                        "first_failure_ms": 1000,
                        "last_error": "seeded failure",
                        "attributes": attributes,
                        "envelope": {"n": 1, "lsn": 16}
                    }
                ],
                "next": null
            })
        );

        let one = get(&fixture.admin, &format!("/parked/{a}?limit=1")).await;
        assert_eq!(one.body["events"].as_array().unwrap().len(), 1);
        assert_eq!(one.body["events"][0]["id"], id_text(0x10, 0));
        assert_eq!(one.body["next"], id_text(0x10, 0));
        let after = format!("/parked/{a}?limit=1&after={}", id_text(0x10, 0));
        let second = get(&fixture.admin, &after).await;
        assert_eq!(second.body["events"][0]["id"], id_text(0x10, 1));
        assert_eq!(second.body["next"], Value::Null);

        let past = format!("/parked/{a}?after={}", id_text(0x10, 1));
        let empty = get(&fixture.admin, &past).await;
        assert_eq!(empty.status, 200);
        assert_eq!(empty.body["events"], json!([]));

        let ghost = named("ghost");
        let missing = get(&fixture.admin, &format!("/parked/{ghost}")).await;
        assert_eq!(missing.status, 404);
        assert_eq!(
            missing.body,
            json!({"error": format!("order key {ghost} has no parked events")})
        );
        fixture.stop().await;
    }

    fn padded(key: OrderKeyId, ordinal: u32, bytes: u64) -> ParkedEvent {
        let pad = "x".repeat(usize::try_from(bytes).unwrap());
        ParkedEvent {
            body: Bytes::from(format!(r#"{{"n":{ordinal},"pad":"{pad}"}}"#)),
            ..parked(key, 0x10, ordinal, ParkStatus::Dead)
        }
    }

    fn shown_ids(answer: &Answer) -> Vec<String> {
        answer.body["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|event| event["id"].as_str().unwrap().to_owned())
            .collect()
    }

    #[tokio::test(start_paused = true)]
    async fn show_pages_by_envelope_bytes_and_withholds_an_event_too_large_to_show() {
        let a = named("a");
        let store = MemoryStateStore::new();
        let third = PARKED_SHOW_BYTES / 3;
        let huge = padded(a, 3, PARKED_SHOW_BYTES + (1 << 20));
        let huge_bytes = huge.body.len();
        seed(
            &store,
            &[
                padded(a, 0, third),
                padded(a, 1, third),
                padded(a, 2, third),
                huge,
                parked(a, 0x10, 4, ParkStatus::Retrying),
            ],
        )
        .await;
        let counting = CountingStore::over(&store);
        let fixture = Fixture::reading(counting.clone()).await;

        let first = get(&fixture.admin, &format!("/parked/{a}")).await;
        assert_eq!(first.status, 200);
        assert_eq!(shown_ids(&first), [id_text(0x10, 0), id_text(0x10, 1)]);
        assert_eq!(first.body["events"][1]["envelope"]["n"], 1);
        assert_eq!(first.body["next"], id_text(0x10, 1));

        let rest = get(
            &fixture.admin,
            &format!("/parked/{a}?after={}", id_text(0x10, 1)),
        )
        .await;
        assert_eq!(rest.status, 200);
        assert_eq!(
            shown_ids(&rest),
            [id_text(0x10, 2), id_text(0x10, 3), id_text(0x10, 4)]
        );
        assert_eq!(
            rest.body["events"][1],
            json!({"id": id_text(0x10, 3), "bytes": huge_bytes, "withheld": true})
        );
        assert_eq!(
            rest.body["events"][2]["envelope"],
            json!({"n": 4, "lsn": 16})
        );
        assert_eq!(rest.body["next"], Value::Null);

        let loaded = counting.counts().loaded_bytes.clone();
        assert_eq!(loaded.len(), 2);
        for bytes in loaded {
            assert!(
                u64::try_from(bytes).unwrap() <= PARKED_SHOW_BYTES,
                "one request loaded {bytes} envelope bytes"
            );
        }
        fixture.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_shows_share_the_inspection_budget() {
        let keys = sorted_keys(6);
        let store = MemoryStateStore::new();
        for key in &keys {
            seed(&store, &[parked(*key, 0x10, 0, ParkStatus::Dead)]).await;
        }
        let counting = CountingStore::over(&store);
        {
            let mut reads = counting.reads();
            for key in &keys {
                reads.delays.insert(*key, Duration::from_millis(100));
            }
        }
        let fixture = Fixture::reading(counting.clone()).await;

        let mut shows = tokio::task::JoinSet::new();
        for key in &keys {
            let admin = fixture.admin.clone();
            let target = format!("/parked/{key}");
            shows.spawn(async move { get(&admin, &target).await.status });
        }
        while let Some(status) = shows.join_next().await {
            assert_eq!(status.unwrap(), 200);
        }
        assert_eq!(counting.reads().peak_running, PARKED_SHOW_CONCURRENCY);
        fixture.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_reply_holds_its_inspection_turn_until_its_body_is_dropped() {
        let a = named("a");
        let store = MemoryStateStore::new();
        seed(&store, &[parked(a, 0x10, 0, ParkStatus::Dead)]).await;
        let fixture = Fixture::reading(Arc::new(store)).await;
        let available = || fixture.admin.inspections.available_permits();

        for target in [format!("/parked/{a}"), "/parked".to_owned()] {
            let response = route(&Method::GET, &target.parse().unwrap(), &fixture.admin).await;
            assert_eq!(response.status(), 200, "{target}");
            assert_eq!(available(), PARKED_SHOW_CONCURRENCY - 1, "{target}");
            drop(response);
            assert_eq!(available(), PARKED_SHOW_CONCURRENCY, "{target}");
        }
        fixture.stop().await;
    }

    async fn eventually(what: &str, done: impl Fn() -> bool) {
        let waited = tokio::time::timeout(Duration::from_secs(10), async {
            while !done() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(waited.is_ok(), "{what} did not happen within 10 s");
    }

    #[tokio::test]
    async fn clients_that_stop_reading_keep_the_inspection_budget_until_they_go() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        use tokio::net::{TcpListener, TcpSocket, TcpStream};

        let a = named("a");
        let store = MemoryStateStore::new();
        let third = PARKED_SHOW_BYTES / 3 - 1024;
        seed(
            &store,
            &[
                padded(a, 0, third),
                padded(a, 1, third),
                padded(a, 2, third),
            ],
        )
        .await;
        let counting = CountingStore::over(&store);
        let fixture = Fixture::reading(counting.clone()).await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(crate::admin::serve(listener, fixture.admin.clone()));
        let request =
            format!("GET /parked/{a} HTTP/1.1\r\nHost: pg-cdc\r\nConnection: close\r\n\r\n");
        let available = || fixture.admin.inspections.available_permits();

        let mut stalled = Vec::new();
        for _ in 0..PARKED_SHOW_CONCURRENCY {
            let socket = TcpSocket::new_v4().unwrap();
            socket.set_recv_buffer_size(4096).unwrap();
            let mut stream = socket.connect(addr).await.unwrap();
            stream.write_all(request.as_bytes()).await.unwrap();
            stalled.push(stream);
        }
        eventually("both pages loading", || {
            counting.counts().loaded_bytes.len() == PARKED_SHOW_CONCURRENCY
        })
        .await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(available(), 0);

        drop(stalled.pop());
        eventually("a released turn", || available() == 1).await;
        let mut reader = TcpStream::connect(addr).await.unwrap();
        reader.write_all(request.as_bytes()).await.unwrap();
        let mut reply = Vec::new();
        reader.read_to_end(&mut reply).await.unwrap();
        assert!(reply.starts_with(b"HTTP/1.1 200 OK"));
        assert!(reply.len() > usize::try_from(3 * third).unwrap());

        drop(stalled);
        eventually("every turn released", || {
            available() == PARKED_SHOW_CONCURRENCY
        })
        .await;
        server.abort();
        fixture.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn redrive_and_discard_go_through_the_owning_lane() {
        let (a, b) = (named("a"), named("b"));
        let store = MemoryStateStore::new();
        seed(
            &store,
            &[
                parked(a, 0x10, 0, ParkStatus::Dead),
                parked(a, 0x10, 1, ParkStatus::Retrying),
                parked(b, 0x20, 0, ParkStatus::Retrying),
            ],
        )
        .await;
        let fixture = Fixture::active(&store).await;

        let redriven = post(&fixture.admin, &format!("/parked/{a}/redrive")).await;
        assert_eq!(redriven.status, 200);
        assert_eq!(
            redriven.body,
            json!({"order_key": a.to_string(), "id": id_text(0x10, 0), "status": "retrying"})
        );
        let head = store.read_parked(&a, None, 1).await.unwrap();
        assert_eq!(head[0].status, ParkStatus::Retrying);

        let refused = post(&fixture.admin, &format!("/parked/{b}/redrive")).await;
        assert_eq!(refused.status, 409);
        assert_eq!(refused.body["head"], id_text(0x20, 0));

        let missing = post(&fixture.admin, &format!("/parked/{a}/discard")).await;
        assert_eq!(missing.status, 400);
        assert!(missing.body["error"].as_str().unwrap().contains("id"));

        let wrong = format!("/parked/{a}/discard?id={}", id_text(0x20, 0));
        let mismatch = post(&fixture.admin, &wrong).await;
        assert_eq!(mismatch.status, 409);
        assert_eq!(mismatch.body["head"], id_text(0x10, 0));

        let right = format!("/parked/{a}/discard?id={}", id_text(0x10, 0));
        let discarded = post(&fixture.admin, &right).await;
        assert_eq!(discarded.status, 200);
        assert_eq!(
            discarded.body,
            json!({"order_key": a.to_string(), "discarded": id_text(0x10, 0)})
        );
        let remaining = store.read_parked(&a, None, 10).await.unwrap();
        assert_eq!(
            remaining.iter().map(|event| event.id).collect::<Vec<_>>(),
            vec![event_id(0x10, 1)]
        );

        let unknown = post(&fixture.admin, &format!("/parked/{}/redrive", named("zz"))).await;
        assert_eq!(unknown.status, 404);
        fixture.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn malformed_requests_are_rejected_before_any_store_call() {
        let a = named("a");
        let store = MemoryStateStore::new();
        seed(&store, &[parked(a, 0x10, 0, ParkStatus::Dead)]).await;
        let counting = CountingStore::over(&store);
        let fixture = Fixture::reading(counting.clone()).await;
        let id = id_text(0x10, 0);

        let bad = [
            (Method::GET, "/parked/zz".to_owned()),
            (Method::GET, "/parked?status=gone".to_owned()),
            (Method::GET, "/parked?limit=0".to_owned()),
            (Method::GET, "/parked?limit=1001".to_owned()),
            (Method::GET, "/parked?limit=x".to_owned()),
            (Method::GET, "/parked?after=nothex".to_owned()),
            (Method::GET, "/parked?verbose=1".to_owned()),
            (Method::GET, "/parked?limit=1&limit=2".to_owned()),
            (Method::GET, "/parked?status".to_owned()),
            (Method::GET, format!("/parked/{a}?after=bad")),
            (Method::GET, format!("/parked/{a}?limit=0")),
            (Method::GET, format!("/parked/{a}?status=dead")),
            (Method::POST, "/parked/zz/redrive".to_owned()),
            (Method::POST, format!("/parked/{a}/redrive?x=1")),
            (Method::POST, format!("/parked/{a}/discard?id=bad")),
            (Method::POST, format!("/parked/{a}/discard?id={id}&id={id}")),
            (Method::POST, format!("/parked/{a}/discard?id={id}&extra=1")),
        ];
        for (method, target) in &bad {
            let answer = call(&fixture.admin, method.clone(), target).await;
            assert_eq!(answer.status, 400, "{target}");
            assert_eq!(answer.headers[CONTENT_TYPE], "application/json");
            assert!(answer.body["error"].is_string(), "{target}");
        }
        let named_in_error = get(&fixture.admin, "/parked?verbose=1").await;
        assert!(
            named_in_error.body["error"]
                .as_str()
                .unwrap()
                .contains("verbose")
        );

        let wrong_method = [
            (Method::GET, format!("/parked/{a}/redrive"), "POST"),
            (Method::GET, format!("/parked/{a}/discard?id={id}"), "POST"),
            (Method::POST, "/parked".to_owned(), "GET"),
            (Method::POST, format!("/parked/{a}"), "GET"),
            (Method::POST, "/status".to_owned(), "GET"),
        ];
        for (method, target, allow) in &wrong_method {
            let answer = call(&fixture.admin, method.clone(), target).await;
            assert_eq!(answer.status, 405, "{target}");
            assert_eq!(answer.headers[ALLOW], *allow, "{target}");
            assert_eq!(answer.headers[CONTENT_TYPE], "application/json");
        }
        let other = get(&fixture.admin, &format!("/parked/{a}/other")).await;
        assert_eq!(other.status, 404);
        assert_eq!(counting.counts().calls, 0);
        fixture.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn an_operation_that_outlives_its_budget_answers_504() {
        let a = named("a");
        let store = MemoryStateStore::new();
        seed(&store, &[parked(a, 0x10, 0, ParkStatus::Retrying)]).await;
        let fixture = Fixture::start(&store, Arc::new(store.clone()), true).await;
        let started = Instant::now();
        let answer = post(&fixture.admin, &format!("/parked/{a}/redrive")).await;
        assert_eq!(answer.status, 504);
        assert!(started.elapsed() >= OPERATION_TIMEOUT);
        assert_eq!(
            answer.body,
            json!({"error": "no answer within 30s; the operation may still complete"})
        );
        fixture.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_lane_with_a_full_operation_queue_answers_503_with_retry_after() {
        let a = named("a");
        let store = MemoryStateStore::new();
        seed(&store, &[parked(a, 0x10, 0, ParkStatus::Retrying)]).await;
        let fixture = Fixture::start(&store, Arc::new(store.clone()), true).await;
        let admin = Arc::new(fixture.admin.clone());
        let target = format!("/parked/{a}/redrive");
        let mut waiting = tokio::task::JoinSet::new();
        for _ in 0..crate::lane::OPERATION_QUEUE {
            let (admin, target) = (Arc::clone(&admin), target.clone());
            waiting.spawn(async move { post(&admin, &target).await.status });
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        let started = Instant::now();
        let refused = post(&fixture.admin, &target).await;
        assert_eq!(started.elapsed(), Duration::ZERO);
        assert_eq!(refused.status, 503);
        assert_eq!(refused.headers[RETRY_AFTER], "1");
        assert!(
            refused.body["error"]
                .as_str()
                .unwrap()
                .contains("too many redrive and discard requests")
        );
        while let Some(status) = waiting.join_next().await {
            assert_eq!(status.unwrap(), 504);
        }
        fixture.stop().await;
    }

    #[tokio::test]
    async fn withdrawn_access_turns_parked_endpoints_into_409s() {
        let store = MemoryStateStore::new();
        let fixture = Fixture::active(&store).await;
        fixture.health.set_role(Role::Draining);
        assert_eq!(get(&fixture.admin, "/parked").await.status, 200);
        fixture.status.set_parked_access(None);
        let answer = get(&fixture.admin, "/parked").await;
        assert_eq!(answer.status, 409);
        assert_eq!(answer.body["role"], "draining");
        fixture.stop().await;
    }

    #[derive(Debug)]
    struct Rejections(Arc<Mutex<Vec<AdminOperation>>>);

    impl Recorder for Rejections {
        fn record(&self, measurement: Measurement<'_>) {
            if let Measurement::AdminRejected { operation } = measurement {
                self.0.lock().unwrap().push(operation);
            }
        }
    }

    #[tokio::test]
    async fn only_a_full_operation_queue_counts_as_a_rejection_of_its_operation() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        crate::metrics::install(Box::new(Rejections(Arc::clone(&seen)))).unwrap();
        let admin = Admin::new(Health::new(), Status::new(), identity());
        let key = named("a");
        for (operation, error) in [
            (AdminOperation::Discard, OperateError::Busy(key)),
            (AdminOperation::Redrive, OperateError::Loading(key)),
            (AdminOperation::Redrive, OperateError::Busy(key)),
            (AdminOperation::Redrive, OperateError::NotParked(key)),
        ] {
            operate_failure(&admin, operation, error).await;
        }
        assert_eq!(
            *seen.lock().unwrap(),
            vec![AdminOperation::Discard, AdminOperation::Redrive]
        );
    }

    #[tokio::test]
    async fn operate_errors_map_to_their_replies() {
        let admin = Admin::new(Health::new(), Status::new(), identity());
        let key = named("a");
        let head = event_id(0x10, 0);
        let cases = [
            (OperateError::NotParked(key), 404),
            (OperateError::Loading(key), 503),
            (OperateError::NotDead { key, head }, 409),
            (
                OperateError::HeadMismatch {
                    key,
                    expected: event_id(0x20, 0),
                    head,
                },
                409,
            ),
            (OperateError::Parking { key }, 409),
            (OperateError::Busy(key), 503),
            (
                OperateError::Store(StoreError::Unavailable("down".to_owned())),
                503,
            ),
            (OperateError::Stopped, 409),
        ];
        for (error, status) in cases {
            let message = error.to_string();
            let has_head = matches!(
                error,
                OperateError::NotDead { .. } | OperateError::HeadMismatch { .. }
            );
            let retry = matches!(error, OperateError::Loading(_) | OperateError::Busy(_));
            let stopped = matches!(error, OperateError::Stopped);
            let response = operate_failure(&admin, AdminOperation::Redrive, error)
                .await
                .into_response();
            assert_eq!(response.status(), status, "{message}");
            assert_eq!(response.headers()[CONTENT_TYPE], "application/json");
            assert_eq!(response.headers().get(RETRY_AFTER).is_some(), retry);
            let body: Value =
                serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                    .unwrap();
            assert_eq!(body.get("head").is_some(), has_head, "{message}");
            if stopped {
                assert_eq!(body["error"], "this task is not the active holder");
            } else {
                assert_eq!(body["error"], message);
            }
            if has_head {
                assert_eq!(body["head"], head.to_string());
            }
        }
    }
}
