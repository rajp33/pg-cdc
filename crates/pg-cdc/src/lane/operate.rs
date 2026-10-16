use std::cmp::Reverse;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::time::Instant;

use super::{Inbound, Lane, Stored};
use crate::event::EventId;
use crate::order::OrderKeyId;
use crate::state::{ParkStatus, StoreError};

pub const OPERATION_QUEUE: usize = 16;
const OPERATIONS_PER_TURN: usize = 4;

#[derive(Debug, Clone)]
pub struct Operator {
    inboxes: Vec<mpsc::WeakUnboundedSender<Inbound>>,
    queues: Vec<Arc<Semaphore>>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OperateError {
    #[error("order key {0} has no parked events")]
    NotParked(OrderKeyId),
    #[error("the parked queue heads for order key {0} are still loading")]
    Loading(OrderKeyId),
    #[error("the head of order key {key}, event {head}, is not dead")]
    NotDead { key: OrderKeyId, head: EventId },
    #[error("the head of order key {key} is event {head}, not {expected}")]
    HeadMismatch {
        key: OrderKeyId,
        expected: EventId,
        head: EventId,
    },
    #[error("the head of order key {key} is still being parked; try again")]
    Parking { key: OrderKeyId },
    #[error(
        "too many redrive and discard requests are waiting on the lane of order key {0}; try again shortly"
    )]
    Busy(OrderKeyId),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("the lanes are stopping or have stopped")]
    Stopped,
}

#[derive(Debug)]
pub(super) enum Action {
    Redrive(OrderKeyId),
    Discard(OrderKeyId, EventId),
}

#[derive(Debug)]
pub(super) struct Operation {
    action: Action,
    reply: oneshot::Sender<Result<EventId, OperateError>>,
    _queued: OwnedSemaphorePermit,
}

pub(super) fn operation_queues(lanes: usize) -> Vec<Arc<Semaphore>> {
    (0..lanes)
        .map(|_| Arc::new(Semaphore::new(OPERATION_QUEUE)))
        .collect()
}

impl Operator {
    pub(super) fn new(
        inboxes: Vec<mpsc::WeakUnboundedSender<Inbound>>,
        queues: Vec<Arc<Semaphore>>,
    ) -> Self {
        Self { inboxes, queues }
    }

    pub async fn redrive(&self, key: OrderKeyId) -> Result<EventId, OperateError> {
        self.request(key, Action::Redrive(key)).await
    }

    pub async fn discard(&self, key: OrderKeyId, id: EventId) -> Result<EventId, OperateError> {
        self.request(key, Action::Discard(key, id)).await
    }

    async fn request(&self, key: OrderKeyId, action: Action) -> Result<EventId, OperateError> {
        let (reply, answer) = oneshot::channel();
        {
            let lane = key.lane(self.inboxes.len());
            let inbox = self
                .inboxes
                .get(lane)
                .and_then(mpsc::WeakUnboundedSender::upgrade)
                .ok_or(OperateError::Stopped)?;
            let queued = self
                .queues
                .get(lane)
                .ok_or(OperateError::Stopped)?
                .clone()
                .try_acquire_owned()
                .map_err(|_| OperateError::Busy(key))?;
            inbox
                .send(Inbound::Operate(Operation {
                    action,
                    reply,
                    _queued: queued,
                }))
                .map_err(|_| OperateError::Stopped)?;
        }
        answer.await.map_err(|_| OperateError::Stopped)?
    }
}

impl Lane {
    pub(super) fn operations_wake(&self) -> Option<Instant> {
        (!self.operations.is_empty()).then(Instant::now)
    }

    pub(super) async fn perform_operations(&mut self) {
        let mut performed = 0;
        while performed < OPERATIONS_PER_TURN
            && let Some(Operation { action, reply, .. }) = self.operations.pop_front()
        {
            if reply.is_closed() {
                continue;
            }
            performed += 1;
            let result = if self.closing {
                Err(OperateError::Stopped)
            } else {
                match action {
                    Action::Redrive(key) => self.redrive(key).await,
                    Action::Discard(key, id) => self.discard(key, id).await,
                }
            };
            let _ = reply.send(result);
        }
    }

    fn operable(&self, key: OrderKeyId) -> Result<(EventId, &Stored), OperateError> {
        if !self.is_loaded(key) {
            return Err(OperateError::Loading(key));
        }
        let stored = self
            .keys
            .get(&key)
            .and_then(|state| state.stored.as_ref())
            .ok_or(OperateError::NotParked(key))?;
        let head = stored.current.ok_or(OperateError::Loading(key))?;
        Ok((head, stored))
    }

    async fn redrive(&mut self, key: OrderKeyId) -> Result<EventId, OperateError> {
        let (head, stored) = self.operable(key)?;
        if stored.dead_on_append {
            return Err(OperateError::Parking { key });
        }
        if !stored.dead {
            return Err(OperateError::NotDead { key, head });
        }
        let store = Arc::clone(&self.store);
        match self
            .publishing(store.set_parked_status(&key, &head, ParkStatus::Retrying))
            .await
        {
            Ok(()) => {}
            Err(error @ (StoreError::NotFound(_) | StoreError::Conflict(_))) => {
                tracing::warn!(order_key = %key, id = %head, %error, "the dead head changed in the store before the redrive; reloading it");
                self.to_kill.retain(|kill| !kill.is(key, head));
                self.reload(key);
                return Err(OperateError::Loading(key));
            }
            Err(error) => return Err(error.into()),
        }
        if let Some(state) = self.keys.get_mut(&key) {
            if let Some(stored) = state.stored.as_mut() {
                stored.dead = false;
            }
            state.failure = None;
            state.backoff.reset();
        }
        self.to_kill.retain(|kill| !kill.is(key, head));
        self.to_load.push_back(key);
        self.stats_dirty = true;
        tracing::info!(order_key = %key, id = %head, "redrove a dead event");
        Ok(head)
    }

    async fn discard(&mut self, key: OrderKeyId, id: EventId) -> Result<EventId, OperateError> {
        let (head, stored) = self.operable(key)?;
        if head != id {
            return Err(OperateError::HeadMismatch {
                key,
                expected: id,
                head,
            });
        }
        let still_queued = self
            .keys
            .get(&key)
            .and_then(|state| state.queue.front())
            .is_some_and(|pending| pending.outbound.id == id);
        if stored.dead_on_append || still_queued {
            return Err(OperateError::Parking { key });
        }
        let store = Arc::clone(&self.store);
        self.publishing(store.remove_parked(&[(key, id)])).await?;
        let _ = self
            .parked
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |parked| {
                Some(parked.saturating_sub(1))
            });
        if let Some(state) = self.keys.get_mut(&key) {
            state.counted = state.counted.saturating_sub(1);
            state.failure = None;
            state.backoff.reset();
            if let Some(stored) = state.stored.as_mut() {
                stored.dead = false;
                if stored.head.as_ref().is_some_and(|held| held.id == id) {
                    stored.head = None;
                    self.heads_in_memory = self.heads_in_memory.saturating_sub(1);
                }
                stored.floor = Some(id);
                stored.current = None;
                stored.since_ms = None;
            }
        }
        self.ready.retain(|ready| *ready != key);
        self.to_kill.retain(|kill| !kill.is(key, id));
        self.to_remove.retain(|entry| *entry != (key, id));
        self.retries.retain(|Reverse((_, due))| *due != key);
        self.to_load.push_back(key);
        self.stats_dirty = true;
        tracing::warn!(order_key = %key, id = %id, "discarded a parked event");
        Ok(id)
    }
}
