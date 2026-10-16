use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;

use super::{
    Admission, ByteBudget, LeaderRecord, ParkStatus, ParkedEntry, ParkedEvent, ParkedHead,
    ParkedQueues, ParkedSlice, ParkedSummary, StateStore, StoreError, SwapOutcome, Version,
    Versioned,
};
use crate::event::EventId;
use crate::order::OrderKeyId;

#[derive(Debug, Clone, Default)]
pub struct MemoryStateStore {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Debug, Default)]
struct Inner {
    parked: BTreeMap<OrderKeyId, BTreeMap<EventId, ParkedEvent>>,
    leader: Option<(LeaderRecord, u64)>,
}

impl MemoryStateStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn parked_events(&self) -> usize {
        self.lock()
            .map_or(0, |inner| inner.parked.values().map(BTreeMap::len).sum())
    }

    fn lock(&self) -> Result<MutexGuard<'_, Inner>, StoreError> {
        self.inner
            .lock()
            .map_err(|_| StoreError::Unavailable("the memory store is poisoned".to_owned()))
    }
}

#[async_trait]
impl StateStore for MemoryStateStore {
    async fn append_parked(&self, events: &[ParkedEvent]) -> Result<(), StoreError> {
        let mut inner = self.lock()?;
        for event in events {
            inner
                .parked
                .entry(event.order_key)
                .or_default()
                .entry(event.id)
                .or_insert_with(|| event.clone());
        }
        Ok(())
    }

    async fn load_parked_heads(&self, partition: u8) -> Result<Vec<ParkedHead>, StoreError> {
        let inner = self.lock()?;
        Ok(inner
            .parked
            .iter()
            .filter(|(key, _)| key.partition() == partition)
            .filter_map(|(key, queue)| {
                let head = *queue.keys().next()?;
                Some(ParkedHead {
                    order_key: *key,
                    head,
                    length: u64::try_from(queue.len()).unwrap_or(u64::MAX),
                })
            })
            .collect())
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
        let inner = self.lock()?;
        let lower = after.map_or(Bound::Unbounded, Bound::Excluded);
        let mut keys = inner.parked.range((lower, Bound::Unbounded));
        let examined: Vec<_> = keys.by_ref().take(limit).collect();
        let more = keys.next().is_some();
        Ok(ParkedQueues {
            next: examined.last().filter(|_| more).map(|(key, _)| **key),
            queues: examined
                .into_iter()
                .filter_map(|(key, queue)| {
                    let head = *queue.keys().next()?;
                    Some(ParkedHead {
                        order_key: *key,
                        head,
                        length: u64::try_from(queue.len().min(length_cap.max(1)))
                            .unwrap_or(u64::MAX),
                    })
                })
                .collect(),
        })
    }

    async fn read_parked(
        &self,
        key: &OrderKeyId,
        after: Option<&EventId>,
        limit: usize,
    ) -> Result<Vec<ParkedEvent>, StoreError> {
        let inner = self.lock()?;
        let Some(queue) = inner.parked.get(key) else {
            return Ok(Vec::new());
        };
        Ok(queue
            .values()
            .filter(|event| after.is_none_or(|after| event.id > *after))
            .take(limit)
            .cloned()
            .collect())
    }

    async fn read_parked_within(
        &self,
        key: &OrderKeyId,
        after: Option<&EventId>,
        limit: usize,
        max_bytes: u64,
    ) -> Result<ParkedSlice, StoreError> {
        let inner = self.lock()?;
        let mut slice = ParkedSlice::default();
        let Some(queue) = inner.parked.get(key) else {
            return Ok(slice);
        };
        let lower = after.map_or(Bound::Unbounded, Bound::Excluded);
        let mut budget = ByteBudget::new(limit, max_bytes);
        for (id, event) in queue.range((lower, Bound::Unbounded)) {
            let bytes = u64::try_from(event.body.len()).unwrap_or(u64::MAX);
            match budget.admit(bytes) {
                Admission::Load => slice.entries.push(ParkedEntry::Loaded(event.clone())),
                Admission::Withhold => slice.entries.push(ParkedEntry::Withheld { id: *id, bytes }),
                Admission::Full => {
                    slice.full = true;
                    break;
                }
                Admission::Done => break,
            }
        }
        Ok(slice)
    }

    async fn read_parked_summary(
        &self,
        key: &OrderKeyId,
    ) -> Result<Option<ParkedSummary>, StoreError> {
        let inner = self.lock()?;
        inner
            .parked
            .get(key)
            .and_then(|queue| queue.values().next())
            .map(ParkedSummary::of)
            .transpose()
    }

    async fn set_parked_status(
        &self,
        key: &OrderKeyId,
        id: &EventId,
        status: ParkStatus,
    ) -> Result<(), StoreError> {
        let mut inner = self.lock()?;
        let event = inner
            .parked
            .get_mut(key)
            .and_then(|queue| queue.get_mut(id))
            .ok_or_else(|| StoreError::NotFound(super::parked_name(key, id)))?;
        event.status = status;
        Ok(())
    }

    async fn remove_parked(&self, events: &[(OrderKeyId, EventId)]) -> Result<(), StoreError> {
        let mut inner = self.lock()?;
        for (key, id) in events {
            if let Some(queue) = inner.parked.get_mut(key) {
                queue.remove(id);
                if queue.is_empty() {
                    inner.parked.remove(key);
                }
            }
        }
        Ok(())
    }

    async fn read_leader(&self) -> Result<Option<Versioned<LeaderRecord>>, StoreError> {
        let inner = self.lock()?;
        Ok(inner.leader.as_ref().map(|(record, version)| Versioned {
            value: record.clone(),
            version: Version(version.to_string()),
        }))
    }

    async fn swap_leader(
        &self,
        next: &LeaderRecord,
        expected: Option<&Version>,
    ) -> Result<SwapOutcome, StoreError> {
        let mut inner = self.lock()?;
        let current = inner.leader.as_ref().map(|(_, version)| *version);
        let matches = match (current, expected) {
            (None, None) => true,
            (Some(current), Some(expected)) => expected.0 == current.to_string(),
            _ => false,
        };
        if !matches {
            return Ok(SwapOutcome::Conflict);
        }
        let version = current.map_or(1, |current| current + 1);
        inner.leader = Some((next.clone(), version));
        Ok(SwapOutcome::Swapped(Version(version.to_string())))
    }
}
