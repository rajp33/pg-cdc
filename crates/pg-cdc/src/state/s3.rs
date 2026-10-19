use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use aws_config::retry::RetryConfig;
use aws_config::timeout::TimeoutConfig;
use aws_sdk_s3::Client;
use aws_sdk_s3::error::{DisplayErrorContext, ProvideErrorMetadata, SdkError};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use tokio::sync::{Semaphore, SemaphorePermit};
use tokio::task::JoinSet;

use super::{
    Admission, ByteBudget, LEADER_OBJECT, LeaderRecord, PARKED_DIR, PARKED_SUMMARY_BYTES,
    ParkStatus, ParkedEntry, ParkedEvent, ParkedHead, ParkedQueues, ParkedSlice, ParkedSummary,
    StateStore, StoreError, SwapOutcome, Version, Versioned, decode_leader, decode_parked,
    decode_parked_summary, encode_parked, parse_parked_name, store_root,
};
use crate::aws::aws_loader;
use crate::event::EventId;
use crate::order::OrderKeyId;

pub const S3_MAX_ATTEMPTS: u32 = 3;
pub const S3_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(10);
pub const S3_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
pub const S3_LIST_PAGE: i32 = 1000;
// S3 DeleteObjects accepts at most 1,000 keys per request.
pub const S3_DELETE_BATCH: usize = 1000;
pub const S3_READ_AHEAD_IDS: usize = 100_000;
const JSON: &str = "application/json";

pub async fn s3_client(region: Option<String>) -> Client {
    let loader = aws_loader(region)
        .retry_config(RetryConfig::standard().with_max_attempts(S3_MAX_ATTEMPTS))
        .timeout_config(
            TimeoutConfig::builder()
                .operation_attempt_timeout(S3_ATTEMPT_TIMEOUT)
                .operation_timeout(S3_OPERATION_TIMEOUT)
                .build(),
        );
    Client::new(&loader.load().await)
}

#[derive(Debug, Clone)]
pub struct S3StateStore {
    client: Client,
    bucket: String,
    root: String,
    permits: Arc<Semaphore>,
    read_ahead: Arc<Mutex<ReadAhead>>,
}

#[derive(Debug, Default)]
struct ReadAhead {
    generation: u64,
    cached: usize,
    keys: HashMap<OrderKeyId, VecDeque<EventId>>,
}

impl ReadAhead {
    fn take(&self, key: &OrderKeyId, limit: usize) -> Option<Vec<EventId>> {
        let ids = self.keys.get(key)?;
        (ids.len() >= limit).then(|| ids.iter().take(limit).copied().collect())
    }

    fn fill(&mut self, generation: u64, key: OrderKeyId, ids: &[EventId]) {
        if generation != self.generation {
            return;
        }
        self.forget(&key);
        let kept: VecDeque<EventId> = ids.iter().take(S3_READ_AHEAD_IDS).copied().collect();
        if kept.is_empty() {
            return;
        }
        while self.cached + kept.len() > S3_READ_AHEAD_IDS {
            let Some(evicted) = self.keys.keys().next().copied() else {
                break;
            };
            self.forget(&evicted);
        }
        self.cached += kept.len();
        self.keys.insert(key, kept);
    }

    fn forget(&mut self, key: &OrderKeyId) {
        if let Some(ids) = self.keys.remove(key) {
            self.cached = self.cached.saturating_sub(ids.len());
        }
    }

    fn removed(&mut self, key: &OrderKeyId, id: &EventId) {
        let Some(ids) = self.keys.get_mut(key) else {
            return;
        };
        let before = ids.len();
        if ids.front() == Some(id) {
            ids.pop_front();
        } else {
            ids.retain(|cached| cached != id);
        }
        self.cached = self.cached.saturating_sub(before - ids.len());
        if ids.is_empty() {
            self.keys.remove(key);
        }
    }

    fn appended(&mut self, key: &OrderKeyId, id: &EventId) {
        if self
            .keys
            .get(key)
            .and_then(VecDeque::back)
            .is_some_and(|last| id < last)
        {
            self.generation += 1;
            self.forget(key);
        }
    }

    fn clear(&mut self) {
        self.generation += 1;
        self.cached = 0;
        self.keys.clear();
    }
}

struct Page {
    objects: Vec<String>,
    sizes: Vec<u64>,
    prefixes: Vec<String>,
    next: Option<String>,
}

fn page_size(wanted: usize) -> i32 {
    i32::try_from(wanted)
        .unwrap_or(S3_LIST_PAGE)
        .min(S3_LIST_PAGE)
}

fn status<E>(error: &SdkError<E>) -> Option<u16> {
    error
        .raw_response()
        .map(|response| response.status().as_u16())
}

fn missing_key<E: ProvideErrorMetadata>(error: &SdkError<E>) -> bool {
    error
        .as_service_error()
        .and_then(ProvideErrorMetadata::code)
        == Some("NoSuchKey")
}

fn unavailable<E: std::error::Error + 'static>(error: &SdkError<E>) -> StoreError {
    StoreError::Unavailable(DisplayErrorContext(error).to_string())
}

impl S3StateStore {
    pub fn new(
        client: Client,
        bucket: String,
        prefix: &str,
        pipeline: &str,
        max_concurrency: usize,
    ) -> Self {
        Self {
            client,
            bucket,
            root: store_root(prefix, pipeline),
            permits: Arc::new(Semaphore::new(
                max_concurrency.clamp(1, Semaphore::MAX_PERMITS),
            )),
            read_ahead: Arc::default(),
        }
    }

    fn read_ahead(&self) -> MutexGuard<'_, ReadAhead> {
        self.read_ahead
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    async fn permit(&self) -> Result<SemaphorePermit<'_>, StoreError> {
        self.permits
            .acquire()
            .await
            .map_err(|_| StoreError::Unavailable("the store is closed".to_owned()))
    }

    fn parked_prefix(&self) -> String {
        format!("{}/{PARKED_DIR}/", self.root)
    }

    fn object(&self, key: &OrderKeyId, id: &EventId) -> String {
        format!("{}{}", self.parked_prefix(), super::parked_name(key, id))
    }

    fn leader_object(&self) -> String {
        format!("{}/{LEADER_OBJECT}", self.root)
    }

    async fn put_new(&self, object: String, bytes: Vec<u8>) -> Result<(), StoreError> {
        let _permit = self.permit().await?;
        let sent = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&object)
            .content_type(JSON)
            .if_none_match("*")
            .body(ByteStream::from(bytes))
            .send()
            .await;
        match sent {
            Ok(_) => Ok(()),
            Err(error) if status(&error) == Some(412) => Ok(()),
            Err(error) => Err(unavailable(&error)),
        }
    }

    async fn append_queue(
        &self,
        key: OrderKeyId,
        queue: Vec<(EventId, Vec<u8>)>,
    ) -> Result<(), StoreError> {
        // Never parallel and never past a failure: replays below a queue's lowest stored id are skipped, so a gap below a stored event loses it.
        for (id, bytes) in queue {
            self.put_new(self.object(&key, &id), bytes).await?;
        }
        Ok(())
    }

    async fn get(&self, object: &str) -> Result<Option<(Vec<u8>, Option<String>)>, StoreError> {
        let _permit = self.permit().await?;
        self.fetch(object).await
    }

    async fn fetch(&self, object: &str) -> Result<Option<(Vec<u8>, Option<String>)>, StoreError> {
        let output = match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(object)
            .send()
            .await
        {
            Ok(output) => output,
            Err(error) if missing_key(&error) => return Ok(None),
            Err(error) => return Err(unavailable(&error)),
        };
        let etag = output.e_tag().map(str::to_owned);
        let bytes = output
            .body
            .collect()
            .await
            .map_err(|error| StoreError::Unavailable(format!("{object}: {error}")))?
            .into_bytes()
            .to_vec();
        Ok(Some((bytes, etag)))
    }

    async fn fetch_prefix(&self, object: &str) -> Result<Option<Vec<u8>>, StoreError> {
        let _permit = self.permit().await?;
        let mut output = match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(object)
            .range(format!("bytes=0-{}", PARKED_SUMMARY_BYTES - 1))
            .send()
            .await
        {
            Ok(output) => output,
            Err(error) if missing_key(&error) => return Ok(None),
            Err(error) => return Err(unavailable(&error)),
        };
        let mut prefix = Vec::with_capacity(PARKED_SUMMARY_BYTES);
        while prefix.len() < PARKED_SUMMARY_BYTES
            && let Some(chunk) = output
                .body
                .try_next()
                .await
                .map_err(|error| StoreError::Unavailable(format!("{object}: {error}")))?
        {
            prefix.extend_from_slice(&chunk);
        }
        prefix.truncate(PARKED_SUMMARY_BYTES);
        Ok(Some(prefix))
    }

    async fn delete_batch(&self, objects: Vec<String>) -> Result<(), StoreError> {
        let identifiers = objects
            .into_iter()
            .map(|object| ObjectIdentifier::builder().key(object).build())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| StoreError::Unavailable(error.to_string()))?;
        let delete = Delete::builder()
            .set_objects(Some(identifiers))
            .quiet(true)
            .build()
            .map_err(|error| StoreError::Unavailable(error.to_string()))?;
        let _permit = self.permit().await?;
        let output = self
            .client
            .delete_objects()
            .bucket(&self.bucket)
            .delete(delete)
            .send()
            .await
            .map_err(|error| unavailable(&error))?;
        match output.errors().first() {
            None => Ok(()),
            Some(error) => Err(StoreError::Unavailable(format!(
                "cannot delete {}: {} {}",
                error.key().unwrap_or_default(),
                error.code().unwrap_or_default(),
                error.message().unwrap_or_default(),
            ))),
        }
    }

    async fn list(
        &self,
        prefix: &str,
        start_after: Option<String>,
        token: Option<String>,
        max_keys: i32,
    ) -> Result<(Vec<String>, Option<String>), StoreError> {
        let page = self
            .list_page(prefix, None, start_after, token, max_keys)
            .await?;
        Ok((page.objects, page.next))
    }

    async fn list_page(
        &self,
        prefix: &str,
        delimiter: Option<&str>,
        start_after: Option<String>,
        token: Option<String>,
        max_keys: i32,
    ) -> Result<Page, StoreError> {
        let _permit = self.permit().await?;
        let output = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(prefix)
            .set_delimiter(delimiter.map(str::to_owned))
            .set_start_after(start_after)
            .set_continuation_token(token)
            .max_keys(max_keys)
            .send()
            .await
            .map_err(|error| unavailable(&error))?;
        let (objects, sizes) = output
            .contents()
            .iter()
            .filter_map(|object| {
                let size = object.size().and_then(|size| u64::try_from(size).ok());
                Some((object.key()?.to_owned(), size.unwrap_or(u64::MAX)))
            })
            .unzip();
        let prefixes = output
            .common_prefixes()
            .iter()
            .filter_map(|prefix| prefix.prefix().map(str::to_owned))
            .collect();
        let next = output
            .is_truncated()
            .unwrap_or(false)
            .then(|| output.next_continuation_token().map(str::to_owned))
            .flatten();
        Ok(Page {
            objects,
            sizes,
            prefixes,
            next,
        })
    }

    async fn queue_keys(
        &self,
        after: Option<&OrderKeyId>,
        limit: usize,
    ) -> Result<(Vec<OrderKeyId>, bool), StoreError> {
        let prefix = self.parked_prefix();
        let mut start_after = after.map(|after| format!("{prefix}{after}0"));
        let mut token = None;
        let mut keys = Vec::new();
        while keys.len() < limit {
            let wanted = page_size(limit - keys.len());
            let page = self
                .list_page(&prefix, Some("/"), start_after.take(), token.take(), wanted)
                .await?;
            keys.extend(page.prefixes.iter().filter_map(|common| {
                common
                    .strip_prefix(&prefix)?
                    .strip_suffix('/')?
                    .parse::<OrderKeyId>()
                    .ok()
            }));
            match page.next {
                Some(next) => token = Some(next),
                None => break,
            }
        }
        let more = token.is_some() || keys.len() > limit;
        keys.truncate(limit);
        Ok((keys, more))
    }

    async fn queue_head(
        &self,
        key: OrderKeyId,
        length_cap: usize,
    ) -> Result<Option<ParkedHead>, StoreError> {
        let prefix = format!("{}{key}/", self.parked_prefix());
        let mut token = None;
        let mut head = None;
        let mut length = 0_usize;
        while length < length_cap {
            let wanted = page_size(length_cap - length);
            let page = self
                .list_page(&prefix, None, None, token.take(), wanted)
                .await?;
            for (_, id) in page.objects.iter().filter_map(|object| self.parse(object)) {
                head.get_or_insert(id);
                length += 1;
            }
            match page.next {
                Some(next) => token = Some(next),
                None => break,
            }
        }
        Ok(head.map(|head| ParkedHead {
            order_key: key,
            head,
            length: u64::try_from(length).unwrap_or(u64::MAX),
        }))
    }

    fn parse(&self, object: &str) -> Option<(OrderKeyId, EventId)> {
        parse_parked_name(object.strip_prefix(&self.parked_prefix())?)
    }

    async fn read_event(
        &self,
        key: OrderKeyId,
        id: EventId,
    ) -> Result<Option<ParkedEvent>, StoreError> {
        let object = self.object(&key, &id);
        let Some((bytes, _)) = self.get(&object).await? else {
            return Ok(None);
        };
        decode_parked(&super::parked_name(&key, &id), &bytes).map(Some)
    }

    async fn read_events(
        &self,
        key: OrderKeyId,
        ids: Vec<EventId>,
    ) -> Result<Vec<Option<ParkedEvent>>, StoreError> {
        let mut tasks = JoinSet::new();
        for id in ids {
            let store = self.clone();
            tasks.spawn(async move { store.read_event(key, id).await });
        }
        Self::join(tasks).await
    }

    async fn list_ahead(&self, key: OrderKeyId, limit: usize) -> Result<Vec<EventId>, StoreError> {
        let generation = self.read_ahead().generation;
        let prefix = format!("{}{key}/", self.parked_prefix());
        let (objects, _) = self.list(&prefix, None, None, S3_LIST_PAGE).await?;
        let listed: Vec<EventId> = objects
            .iter()
            .filter_map(|object| self.parse(object))
            .map(|(_, id)| id)
            .collect();
        self.read_ahead().fill(generation, key, &listed);
        Ok(listed.into_iter().take(limit).collect())
    }

    async fn read_ahead_head(
        &self,
        key: OrderKeyId,
        limit: usize,
    ) -> Result<Option<Vec<ParkedEvent>>, StoreError> {
        let cached = self.read_ahead().take(&key, limit);
        let ids = if let Some(ids) = cached {
            ids
        } else {
            self.list_ahead(key, limit).await?
        };
        let wanted = ids.len();
        let mut events: Vec<ParkedEvent> = self
            .read_events(key, ids)
            .await?
            .into_iter()
            .flatten()
            .collect();
        if events.len() < wanted {
            self.read_ahead().forget(&key);
            return Ok(None);
        }
        events.sort_unstable_by_key(|event| event.id);
        Ok(Some(events))
    }

    async fn join<T: Send + 'static>(
        mut tasks: JoinSet<Result<T, StoreError>>,
    ) -> Result<Vec<T>, StoreError> {
        let mut results = Vec::with_capacity(tasks.len());
        let mut failure = None;
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok(Ok(value)) => results.push(value),
                Ok(Err(error)) => failure = Some(error),
                Err(error) => failure = Some(StoreError::Unavailable(error.to_string())),
            }
        }
        failure.map_or(Ok(results), Err)
    }
}

#[async_trait]
impl StateStore for S3StateStore {
    async fn append_parked(&self, events: &[ParkedEvent]) -> Result<(), StoreError> {
        let mut queues: BTreeMap<OrderKeyId, Vec<(EventId, Vec<u8>)>> = BTreeMap::new();
        for event in events {
            queues
                .entry(event.order_key)
                .or_default()
                .push((event.id, encode_parked(event)?));
        }
        let mut tasks = JoinSet::new();
        for (key, mut queue) in queues {
            queue.sort_by_key(|(id, _)| *id);
            let store = self.clone();
            tasks.spawn(async move { store.append_queue(key, queue).await });
        }
        let appended = Self::join(tasks).await.map(drop);
        let mut read_ahead = self.read_ahead();
        for event in events {
            read_ahead.appended(&event.order_key, &event.id);
        }
        appended
    }

    async fn load_parked_heads(&self, partition: u8) -> Result<Vec<ParkedHead>, StoreError> {
        let prefix = format!("{}{partition:02x}", self.parked_prefix());
        let mut heads: Vec<ParkedHead> = Vec::new();
        let mut token = None;
        loop {
            let (keys, next) = self.list(&prefix, None, token, S3_LIST_PAGE).await?;
            for (order_key, id) in keys.iter().filter_map(|object| self.parse(object)) {
                match heads.last_mut() {
                    Some(last) if last.order_key == order_key => {
                        last.length = last.length.saturating_add(1);
                    }
                    _ => heads.push(ParkedHead {
                        order_key,
                        head: id,
                        length: 1,
                    }),
                }
            }
            match next {
                Some(next) => token = Some(next),
                None => return Ok(heads),
            }
        }
    }

    async fn list_parked_queues(
        &self,
        after: Option<&OrderKeyId>,
        limit: usize,
        length_cap: usize,
    ) -> Result<ParkedQueues, StoreError> {
        if limit == 0 {
            return Ok(ParkedQueues::default());
        }
        let length_cap = length_cap.max(1);
        let (keys, more) = self.queue_keys(after, limit).await?;
        let next = keys.last().copied().filter(|_| more);
        let mut tasks = JoinSet::new();
        for key in keys {
            let store = self.clone();
            tasks.spawn(async move { store.queue_head(key, length_cap).await });
        }
        let mut queues: Vec<ParkedHead> = Self::join(tasks).await?.into_iter().flatten().collect();
        queues.sort_unstable_by_key(|queue| queue.order_key);
        Ok(ParkedQueues { queues, next })
    }

    async fn read_parked(
        &self,
        key: &OrderKeyId,
        after: Option<&EventId>,
        limit: usize,
    ) -> Result<Vec<ParkedEvent>, StoreError> {
        if after.is_none()
            && i32::try_from(limit).is_ok_and(|limit| limit <= S3_LIST_PAGE)
            && let Some(events) = self.read_ahead_head(*key, limit).await?
        {
            return Ok(events);
        }
        let prefix = format!("{}{key}/", self.parked_prefix());
        let mut start_after = after.map(|after| self.object(key, after));
        let mut ids = Vec::new();
        while ids.len() < limit {
            let wanted = i32::try_from(limit - ids.len()).unwrap_or(S3_LIST_PAGE);
            let (objects, next) = self
                .list(&prefix, start_after.take(), None, wanted.min(S3_LIST_PAGE))
                .await?;
            let last = objects.last().cloned();
            ids.extend(
                objects
                    .iter()
                    .filter_map(|object| self.parse(object))
                    .map(|(_, id)| id),
            );
            match (next, last) {
                (Some(_), Some(last)) => start_after = Some(last),
                _ => break,
            }
        }
        ids.truncate(limit);
        let mut events: Vec<ParkedEvent> = self
            .read_events(*key, ids)
            .await?
            .into_iter()
            .flatten()
            .collect();
        events.sort_unstable_by_key(|event| event.id);
        Ok(events)
    }

    async fn read_parked_within(
        &self,
        key: &OrderKeyId,
        after: Option<&EventId>,
        limit: usize,
        max_bytes: u64,
    ) -> Result<ParkedSlice, StoreError> {
        let prefix = format!("{}{key}/", self.parked_prefix());
        let mut start_after = after.map(|after| self.object(key, after));
        let mut budget = ByteBudget::new(limit, max_bytes);
        let mut planned: Vec<(EventId, Option<u64>)> = Vec::new();
        let mut full = false;
        'listing: while planned.len() < limit {
            let page = self
                .list_page(
                    &prefix,
                    None,
                    start_after.take(),
                    None,
                    page_size(limit - planned.len()),
                )
                .await?;
            for (object, bytes) in page.objects.iter().zip(&page.sizes) {
                let Some((_, id)) = self.parse(object) else {
                    continue;
                };
                match budget.admit(*bytes) {
                    Admission::Load => planned.push((id, None)),
                    Admission::Withhold => planned.push((id, Some(*bytes))),
                    Admission::Full => {
                        full = true;
                        break 'listing;
                    }
                    Admission::Done => break 'listing,
                }
            }
            match (page.next, page.objects.last()) {
                (Some(_), Some(last)) => start_after = Some(last.clone()),
                _ => break,
            }
        }
        let loading = planned
            .iter()
            .filter(|(_, withheld)| withheld.is_none())
            .map(|(id, _)| *id)
            .collect();
        let mut loaded: HashMap<EventId, ParkedEvent> = self
            .read_events(*key, loading)
            .await?
            .into_iter()
            .flatten()
            .map(|event| (event.id, event))
            .collect();
        let entries = planned
            .into_iter()
            .filter_map(|(id, withheld)| match withheld {
                Some(bytes) => Some(ParkedEntry::Withheld { id, bytes }),
                None => loaded.remove(&id).map(ParkedEntry::Loaded),
            })
            .collect();
        Ok(ParkedSlice { entries, full })
    }

    async fn read_parked_summary(
        &self,
        key: &OrderKeyId,
    ) -> Result<Option<ParkedSummary>, StoreError> {
        let prefix = format!("{}{key}/", self.parked_prefix());
        let mut start_after = None;
        loop {
            let (objects, _) = self.list(&prefix, start_after.take(), None, 1).await?;
            let Some(object) = objects.into_iter().next() else {
                return Ok(None);
            };
            if let Some((_, id)) = self.parse(&object)
                && let Some(stored) = self.fetch_prefix(&object).await?
            {
                return decode_parked_summary(&super::parked_name(key, &id), &stored).map(Some);
            }
            start_after = Some(object);
        }
    }

    async fn set_parked_status(
        &self,
        key: &OrderKeyId,
        id: &EventId,
        next: ParkStatus,
    ) -> Result<(), StoreError> {
        let name = super::parked_name(key, id);
        let object = self.object(key, id);
        let (stored, etag) = self
            .get(&object)
            .await?
            .ok_or_else(|| StoreError::NotFound(name.clone()))?;
        let etag = etag.ok_or_else(|| StoreError::Malformed {
            key: name.clone(),
            reason: "the object has no ETag".to_owned(),
        })?;
        let mut event = decode_parked(&name, &stored)?;
        event.status = next;
        let bytes = encode_parked(&event)?;
        let _permit = self.permit().await?;
        let sent = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(object)
            .content_type(JSON)
            .if_match(etag)
            .body(ByteStream::from(bytes))
            .send()
            .await;
        match sent {
            Ok(_) => Ok(()),
            Err(error) if status(&error) == Some(412) => Err(StoreError::Conflict(name)),
            Err(error) if status(&error) == Some(404) || missing_key(&error) => {
                Err(StoreError::NotFound(name))
            }
            Err(error) => Err(unavailable(&error)),
        }
    }

    async fn remove_parked(&self, events: &[(OrderKeyId, EventId)]) -> Result<(), StoreError> {
        let mut tasks = JoinSet::new();
        for batch in events.chunks(S3_DELETE_BATCH) {
            let store = self.clone();
            let objects = batch.iter().map(|(key, id)| self.object(key, id)).collect();
            tasks.spawn(async move { store.delete_batch(objects).await });
        }
        Self::join(tasks).await?;
        let mut read_ahead = self.read_ahead();
        for (key, id) in events {
            read_ahead.removed(key, id);
        }
        Ok(())
    }

    async fn read_leader(&self) -> Result<Option<Versioned<LeaderRecord>>, StoreError> {
        let Some((bytes, etag)) = self.fetch(&self.leader_object()).await? else {
            return Ok(None);
        };
        let version = etag.ok_or_else(|| StoreError::Malformed {
            key: LEADER_OBJECT.to_owned(),
            reason: "the object has no ETag".to_owned(),
        })?;
        Ok(Some(Versioned {
            value: decode_leader(LEADER_OBJECT, &bytes)?,
            version: Version(version),
        }))
    }

    async fn swap_leader(
        &self,
        next: &LeaderRecord,
        expected: Option<&Version>,
    ) -> Result<SwapOutcome, StoreError> {
        let bytes = serde_json::to_vec(next).map_err(|error| StoreError::Malformed {
            key: LEADER_OBJECT.to_owned(),
            reason: error.to_string(),
        })?;
        let request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(self.leader_object())
            .content_type(JSON)
            .body(ByteStream::from(bytes));
        let request = match expected {
            Some(version) => request.if_match(&version.0),
            None => request.if_none_match("*"),
        };
        let sent = request.send().await;
        self.read_ahead().clear();
        match sent {
            Ok(output) => output
                .e_tag()
                .map(|etag| SwapOutcome::Swapped(Version(etag.to_owned())))
                .ok_or_else(|| StoreError::Malformed {
                    key: LEADER_OBJECT.to_owned(),
                    reason: "PutObject returned no ETag".to_owned(),
                }),
            Err(error) if status(&error) == Some(412) || missing_key(&error) => {
                match self.read_leader().await? {
                    Some(current) if current.value == *next => {
                        Ok(SwapOutcome::Swapped(current.version))
                    }
                    _ => Ok(SwapOutcome::Conflict),
                }
            }
            Err(error) => Err(unavailable(&error)),
        }
    }
}

#[cfg(test)]
mod tests {
    use pg_cdc_decoder::Lsn;

    use super::*;

    fn ids(lsn: u64, count: usize) -> Vec<EventId> {
        (0..u32::try_from(count).unwrap())
            .map(|ordinal| EventId::new(Lsn(lsn), ordinal))
            .collect()
    }

    fn key(n: u8) -> OrderKeyId {
        format!("{n:02x}{}", "0".repeat(30)).parse().unwrap()
    }

    #[test]
    fn read_ahead_ids_stay_within_their_bound_by_evicting_other_keys() {
        let mut read_ahead = ReadAhead::default();
        read_ahead.fill(0, key(1), &ids(1, S3_READ_AHEAD_IDS - 5));
        read_ahead.fill(0, key(2), &ids(2, 5));
        assert_eq!(read_ahead.cached, S3_READ_AHEAD_IDS);
        assert!(read_ahead.take(&key(1), 1).is_some());

        read_ahead.fill(0, key(3), &ids(3, 10));
        assert!(read_ahead.cached <= S3_READ_AHEAD_IDS);
        assert_eq!(read_ahead.take(&key(3), 10), Some(ids(3, 10)));

        read_ahead.fill(0, key(4), &ids(4, S3_READ_AHEAD_IDS + 7));
        assert_eq!(read_ahead.cached, S3_READ_AHEAD_IDS);
        assert_eq!(read_ahead.keys.len(), 1);
    }

    #[test]
    fn removals_and_stale_fills_keep_the_count_exact() {
        let mut read_ahead = ReadAhead::default();
        read_ahead.fill(0, key(1), &ids(1, 3));
        let [first, second, third] = ids(1, 3)[..] else {
            panic!("three ids");
        };
        read_ahead.removed(&key(1), &second);
        read_ahead.removed(&key(1), &first);
        assert_eq!(
            (read_ahead.cached, read_ahead.take(&key(1), 1)),
            (1, Some(vec![third]))
        );
        read_ahead.removed(&key(1), &third);
        assert_eq!(read_ahead.cached, 0);
        assert!(read_ahead.keys.is_empty());

        let generation = read_ahead.generation;
        read_ahead.clear();
        read_ahead.fill(generation, key(2), &ids(2, 4));
        assert_eq!((read_ahead.cached, read_ahead.take(&key(2), 1)), (0, None));
    }
}
