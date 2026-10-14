use std::cmp::Reverse;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio::task::JoinSet;
use tokio::time::Instant;

use super::{Key, Lane, Report, Stored};
use crate::event::EventId;
use crate::order::OrderKeyId;
use crate::sink::Outcome;
use crate::state::{ParkStatus, ParkedEvent, ParkedHead, StoreError, now_ms};

pub(super) const QUEUED_BEHIND: &str = "queued behind a parked event";

const STORE_STEPS: usize = 4;

impl Lane {
    pub(super) fn accept_head(&mut self, head: ParkedHead) {
        let key = head.order_key;
        let state = self.keys.entry(key).or_insert_with(Key::new);
        state.stored = Some(Stored {
            floor: Some(head.head),
            ..Stored::default()
        });
        let queued = std::mem::take(&mut state.queue);
        for pending in queued {
            let id = pending.outbound.id;
            if id < head.head {
                if let Ok(settled) = pending.ticket.skip_replayed(id, head.head) {
                    let _ = self.reports.send(Report::Settled(settled));
                }
            } else {
                state.queue.push_back(pending);
            }
        }
        if !state.queue.is_empty() {
            self.to_append.insert(key);
        }
    }

    pub(super) fn partition_loaded(&mut self, partition: u8) {
        if let Some(loaded) = self.loaded.get_mut(usize::from(partition)) {
            *loaded = true;
        }
        let mut keys: Vec<(Option<EventId>, OrderKeyId)> = self
            .keys
            .iter()
            .filter(|(key, _)| key.partition() == partition)
            .map(|(key, state)| (state.queue.front().map(|pending| pending.outbound.id), *key))
            .collect();
        keys.sort_unstable();
        for (_, key) in keys {
            match self.keys.get(&key) {
                Some(Key {
                    stored: Some(_), ..
                }) => self.to_load.push_back(key),
                Some(_) => self.make_ready(key),
                None => {}
            }
        }
    }

    pub(super) fn store_wake(&self) -> Option<Instant> {
        let appending = !self.to_append.is_empty();
        let other = !self.to_remove.is_empty()
            || !self.to_kill.is_empty()
            || (!self.closing
                && !self.to_load.is_empty()
                && self.heads_in_memory < self.head_capacity());
        let append_at = appending
            .then(|| {
                [self.store_due, self.append_due]
                    .into_iter()
                    .flatten()
                    .max()
            })
            .flatten();
        let other_at = self.store_due.filter(|_| other);
        [append_at, other_at].into_iter().flatten().min()
    }

    fn head_capacity(&self) -> usize {
        self.capabilities.max_batch_messages.max(1)
    }

    pub(super) async fn store_work(&mut self, now: Instant) -> bool {
        if self.store_due.is_some_and(|due| due > now) {
            return false;
        }
        for offset in 0..STORE_STEPS {
            let step = (self.store_turn + offset) % STORE_STEPS;
            let ran = match step {
                0 if !self.to_append.is_empty() && self.append_due.is_none_or(|due| due <= now) => {
                    self.flush_appends().await;
                    true
                }
                1 if !self.to_remove.is_empty() => {
                    self.flush_removes().await;
                    true
                }
                2 if !self.to_kill.is_empty() => {
                    self.flush_kills().await;
                    true
                }
                3 if !self.closing
                    && !self.to_load.is_empty()
                    && self.heads_in_memory < self.head_capacity() =>
                {
                    self.load_heads().await;
                    true
                }
                _ => false,
            };
            if ran {
                self.store_turn = (step + 1) % STORE_STEPS;
                return true;
            }
        }
        false
    }

    fn store_succeeded(&mut self) {
        self.store_backoff.reset();
        self.store_due = None;
    }

    fn store_failed(&mut self, operation: &str, error: &StoreError) {
        let delay = self.store_backoff.next_delay();
        tracing::warn!(lane = self.index, operation, %error, ?delay, "state store call failed; retrying");
        self.store_due = Some(Instant::now() + delay);
    }

    fn queued_events(&self, keys: &[OrderKeyId]) -> Vec<ParkedEvent> {
        let parked_at = now_ms();
        let mut events = Vec::new();
        for state in keys.iter().filter_map(|key| self.keys.get(key)) {
            let floor = state.stored.as_ref().and_then(|stored| stored.floor);
            let dead = state
                .stored
                .as_ref()
                .is_some_and(|stored| stored.dead_on_append);
            for (index, pending) in state.queue.iter().enumerate() {
                let failure = state
                    .failure
                    .as_ref()
                    .filter(|_| index == 0 && floor == Some(pending.outbound.id));
                events.push(ParkedEvent {
                    order_key: pending.outbound.order_key,
                    id: pending.outbound.id,
                    status: if dead && index == 0 {
                        ParkStatus::Dead
                    } else {
                        ParkStatus::Retrying
                    },
                    attempts: failure.map_or(0, |failure| failure.attempts),
                    first_failure_ms: failure.map_or(parked_at, |failure| failure.since_ms),
                    last_error: failure
                        .map_or_else(|| QUEUED_BEHIND.to_owned(), |failure| failure.error.clone()),
                    attributes: pending.outbound.attributes.clone(),
                    body: pending.outbound.body.clone(),
                });
            }
        }
        events
    }

    async fn flush_appends(&mut self) {
        let keys: Vec<OrderKeyId> = std::mem::take(&mut self.to_append).into_iter().collect();
        let events = self.queued_events(&keys);
        let count = u64::try_from(events.len()).unwrap_or(u64::MAX);
        let parked = self.parked.load(Ordering::SeqCst);
        if self.max_parked > 0 && parked.saturating_add(count) > self.max_parked {
            let delay = self.store_backoff.next_delay();
            tracing::warn!(
                lane = self.index,
                parked,
                max_parked_events = self.max_parked,
                ?delay,
                "the parked event limit is reached; events stay in flight"
            );
            self.append_due = Some(Instant::now() + delay);
            self.to_append.extend(keys);
            return;
        }
        self.append_due = None;
        let appended = self.store.append_parked(&events).await;
        if let Err(error) = &appended {
            self.store_failed("append", error);
            self.to_append.extend(keys);
            return;
        }
        self.store_succeeded();
        self.parked.fetch_add(count, Ordering::SeqCst);
        for key in keys {
            let Some(state) = self.keys.get_mut(&key) else {
                continue;
            };
            for pending in std::mem::take(&mut state.queue) {
                if let Ok(settled) = pending.ticket.park(&appended) {
                    let _ = self.reports.send(Report::Settled(settled));
                }
            }
            if let Some(stored) = state.stored.as_mut()
                && stored.dead_on_append
            {
                stored.dead_on_append = false;
                stored.dead = true;
                tracing::error!(order_key = %key, id = ?stored.floor, "a dead event is parked and blocks its order key");
            }
        }
    }

    async fn load_heads(&mut self) {
        let room = self.head_capacity().saturating_sub(self.heads_in_memory);
        let mut wanted = Vec::new();
        while wanted.len() < room {
            let Some(key) = self.to_load.pop_front() else {
                break;
            };
            let waiting = self.keys.get(&key).and_then(|state| state.stored.as_ref());
            if waiting.is_some_and(|stored| stored.head.is_none() && !stored.dead)
                && !wanted.contains(&key)
            {
                wanted.push(key);
            }
        }
        let mut reads = JoinSet::new();
        for key in &wanted {
            let (store, key) = (Arc::clone(&self.store), *key);
            reads.spawn(async move { (key, store.read_parked(&key, None, 1).await) });
        }
        let mut answered = HashSet::new();
        let mut failure = None;
        while let Some(joined) = reads.join_next().await {
            let Ok((key, read)) = joined else {
                continue;
            };
            answered.insert(key);
            match read {
                Ok(events) => self.head_read(key, events.into_iter().next()),
                Err(error) => {
                    self.to_load.push_back(key);
                    failure = Some(error);
                }
            }
        }
        for key in wanted {
            if !answered.contains(&key) {
                self.to_load.push_back(key);
            }
        }
        match failure {
            Some(error) => self.store_failed("read", &error),
            None => self.store_succeeded(),
        }
    }

    fn head_read(&mut self, key: OrderKeyId, head: Option<ParkedEvent>) {
        let Some(state) = self.keys.get_mut(&key) else {
            return;
        };
        let Some(stored) = state.stored.as_mut() else {
            return;
        };
        let Some(head) = head else {
            state.stored = None;
            state.backoff.reset();
            state.failure = None;
            self.to_append.remove(&key);
            if state.queue.is_empty() {
                self.keys.remove(&key);
            } else {
                self.make_ready(key);
            }
            return;
        };
        stored.floor = Some(head.id);
        if head.status == ParkStatus::Dead {
            stored.dead = true;
            tracing::error!(
                order_key = %key,
                id = %head.id,
                error = %head.last_error,
                "a dead event blocks its order key"
            );
            return;
        }
        let outbound = head.outbound();
        if outbound.encoded_len() <= self.capabilities.max_message_bytes {
            self.heads_in_memory += 1;
        }
        stored.head = Some(outbound);
        self.make_ready(key);
    }

    pub(super) fn complete_stored(&mut self, key: OrderKeyId, outcome: &Outcome) {
        let Some(state) = self.keys.get_mut(&key) else {
            return;
        };
        let Some(head) = state.stored.as_mut().and_then(|stored| stored.head.take()) else {
            return;
        };
        self.heads_in_memory = self.heads_in_memory.saturating_sub(1);
        match outcome {
            Outcome::Delivered => {
                state.backoff.reset();
                state.failure = None;
                self.to_remove.push((key, head.id));
            }
            Outcome::Retryable(error) => {
                let delay = state.backoff.next_delay();
                if state.failure.is_none() {
                    tracing::warn!(order_key = %key, id = %head.id, %error, ?delay, "redelivery of a parked event failed; retrying");
                }
                state.record_failure(error);
                self.retries.push(Reverse((Instant::now() + delay, key)));
            }
            Outcome::Terminal(error) => {
                tracing::error!(order_key = %key, id = %head.id, %error, "the sink rejected a parked event for good; marking it dead");
                self.to_kill.push((key, head.id));
            }
        }
    }

    async fn flush_removes(&mut self) {
        let removals = std::mem::take(&mut self.to_remove);
        if let Err(error) = self.store.remove_parked(&removals).await {
            self.store_failed("remove", &error);
            self.to_remove = removals;
            return;
        }
        self.store_succeeded();
        let removed = u64::try_from(removals.len()).unwrap_or(u64::MAX);
        let _ = self
            .parked
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |parked| {
                Some(parked.saturating_sub(removed))
            });
        for (key, id) in removals {
            if let Some(stored) = self
                .keys
                .get_mut(&key)
                .and_then(|state| state.stored.as_mut())
            {
                stored.floor = Some(id);
                self.to_load.push_back(key);
            }
        }
    }

    async fn flush_kills(&mut self) {
        while let Some((key, id)) = self.to_kill.pop() {
            if let Err(error) = self
                .store
                .set_parked_status(&key, &id, ParkStatus::Dead)
                .await
            {
                self.store_failed("mark dead", &error);
                self.to_kill.push((key, id));
                return;
            }
            if let Some(stored) = self
                .keys
                .get_mut(&key)
                .and_then(|state| state.stored.as_mut())
            {
                stored.dead = true;
            }
        }
        self.store_succeeded();
    }
}
