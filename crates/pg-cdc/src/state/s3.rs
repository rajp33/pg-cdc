use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use aws_config::retry::RetryConfig;
use aws_config::timeout::TimeoutConfig;
use aws_sdk_s3::Client;
use aws_sdk_s3::error::{DisplayErrorContext, ProvideErrorMetadata, SdkError};
use aws_sdk_s3::primitives::ByteStream;
use tokio::sync::{Semaphore, SemaphorePermit};
use tokio::task::JoinSet;

use super::{
    LEADER_OBJECT, LeaderRecord, PARKED_DIR, ParkStatus, ParkedEvent, ParkedHead, StateStore,
    StoreError, SwapOutcome, Version, Versioned, decode_leader, decode_parked, encode_parked,
    parse_parked_name, store_root,
};
use crate::aws::aws_loader;
use crate::event::EventId;
use crate::order::OrderKeyId;

pub const S3_MAX_ATTEMPTS: u32 = 3;
pub const S3_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(10);
pub const S3_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
pub const S3_LIST_PAGE: i32 = 1000;
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
        }
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
        // A key's events land in id order and stop at its first failure: §10.6 skips replays below the lowest stored id.
        for (id, bytes) in queue {
            self.put_new(self.object(&key, &id), bytes).await?;
        }
        Ok(())
    }

    async fn get(&self, object: &str) -> Result<Option<(Vec<u8>, Option<String>)>, StoreError> {
        let _permit = self.permit().await?;
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

    async fn delete(&self, object: String) -> Result<(), StoreError> {
        let _permit = self.permit().await?;
        match self
            .client
            .delete_object()
            .bucket(&self.bucket)
            .key(&object)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(error) => Err(unavailable(&error)),
        }
    }

    async fn list(
        &self,
        prefix: &str,
        start_after: Option<String>,
        token: Option<String>,
        max_keys: i32,
    ) -> Result<(Vec<String>, Option<String>), StoreError> {
        let _permit = self.permit().await?;
        let output = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(prefix)
            .set_start_after(start_after)
            .set_continuation_token(token)
            .max_keys(max_keys)
            .send()
            .await
            .map_err(|error| unavailable(&error))?;
        let keys = output
            .contents()
            .iter()
            .filter_map(|object| object.key().map(str::to_owned))
            .collect();
        let next = output
            .is_truncated()
            .unwrap_or(false)
            .then(|| output.next_continuation_token().map(str::to_owned))
            .flatten();
        Ok((keys, next))
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
        Self::join(tasks).await.map(drop)
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

    async fn read_parked(
        &self,
        key: &OrderKeyId,
        after: Option<&EventId>,
        limit: usize,
    ) -> Result<Vec<ParkedEvent>, StoreError> {
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
        let mut tasks = JoinSet::new();
        for id in ids {
            let (store, key) = (self.clone(), *key);
            tasks.spawn(async move { store.read_event(key, id).await });
        }
        let mut events: Vec<ParkedEvent> = Self::join(tasks).await?.into_iter().flatten().collect();
        events.sort_unstable_by_key(|event| event.id);
        Ok(events)
    }

    async fn set_parked_status(
        &self,
        key: &OrderKeyId,
        id: &EventId,
        status: ParkStatus,
    ) -> Result<(), StoreError> {
        let mut event = self
            .read_event(*key, *id)
            .await?
            .ok_or_else(|| StoreError::NotFound(super::parked_name(key, id)))?;
        event.status = status;
        let bytes = encode_parked(&event)?;
        let _permit = self.permit().await?;
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(self.object(key, id))
            .content_type(JSON)
            .body(ByteStream::from(bytes))
            .send()
            .await
            .map(drop)
            .map_err(|error| unavailable(&error))
    }

    async fn remove_parked(&self, events: &[(OrderKeyId, EventId)]) -> Result<(), StoreError> {
        let mut tasks = JoinSet::new();
        for (key, id) in events {
            let (store, object) = (self.clone(), self.object(key, id));
            tasks.spawn(async move { store.delete(object).await });
        }
        Self::join(tasks).await.map(drop)
    }

    async fn read_leader(&self) -> Result<Option<Versioned<LeaderRecord>>, StoreError> {
        let Some((bytes, etag)) = self.get(&self.leader_object()).await? else {
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
        let sent = {
            let _permit = self.permit().await?;
            request.send().await
        };
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
