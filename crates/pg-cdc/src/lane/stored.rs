use std::cmp::Reverse;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::Instrument;

use super::{Key, Kill, Lane, Report, Stored, record_delivered};
use crate::event::EventId;
use crate::metrics::{self, EventOutcome, Measurement};
use crate::order::OrderKeyId;
use crate::sink::{Outcome, metric_labels};
use crate::state::{ParkStatus, ParkedEvent, ParkedHead, StoreError, now_ms};

pub(super) const QUEUED_BEHIND: &str = "queued behind a parked event";

const STORE_STEPS: usize = 4;

impl Lane {
    pub(super) fn accept_head(&mut self, head: ParkedHead) {
        let key = head.order_key;
        let state = self.keys.entry(key).or_insert_with(Key::new);
        state.counted = state.counted.saturating_add(head.length);
        state.stored = Some(Box::new(Stored {
            floor: Some(head.head),
            current: Some(head.head),
            ..Stored::default()
        }));
        self.stats_dirty = true;
        let queued = std::mem::take(&mut state.queue);
        let mut dropped = 0_u64;
        for pending in queued {
            let id = pending.outbound.id;
            if id < head.head {
                dropped += 1;
                if let Ok(settled) = pending.ticket.skip_replayed(id, head.head) {
                    let _ = self.reports.send(Report::Settled(settled));
                }
            } else {
                state.queue.push_back(pending);
            }
        }
        let freed = dropped.min(state.reserved);
        state.reserved -= freed;
        state.counted = state.counted.saturating_sub(freed);
        if !state.queue.is_empty() {
            self.to_append.insert(key);
        }
        self.release(freed);
    }

    pub(super) fn partition_loaded(&mut self, partition: u8) {
        if let Some(loaded) = self.loaded.get_mut(usize::from(partition)) {
            *loaded = true;
        }
        if !self.counted_everywhere && self.loaded.iter().all(|loaded| *loaded) {
            self.counted_everywhere = true;
            self.append_due = None;
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
                Some(_) if self.closing => self.start_parking(key, false),
                Some(_) => self.make_ready(key),
                None => {}
            }
        }
    }

    pub(super) fn store_wake(&self) -> Option<Instant> {
        let appending = self.counted_everywhere && !self.to_append.is_empty();
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
                0 if self.counted_everywhere
                    && !self.to_append.is_empty()
                    && self.append_due.is_none_or(|due| due <= now) =>
                {
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

    pub(super) fn reload(&mut self, key: OrderKeyId) {
        let Some(stored) = self
            .keys
            .get_mut(&key)
            .and_then(|state| state.stored.as_mut())
        else {
            return;
        };
        if stored.head.take().is_some() {
            self.heads_in_memory = self.heads_in_memory.saturating_sub(1);
        }
        stored.dead = false;
        stored.current = None;
        self.stats_dirty = true;
        self.to_load.push_back(key);
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

    fn queued_events(&self, key: &OrderKeyId, limit: usize) -> Vec<ParkedEvent> {
        let Some(state) = self.keys.get(key) else {
            return Vec::new();
        };
        let parked_at = now_ms();
        let floor = state.stored.as_ref().and_then(|stored| stored.floor);
        let dead = state
            .stored
            .as_ref()
            .is_some_and(|stored| stored.dead_on_append);
        state
            .queue
            .iter()
            .take(limit)
            .enumerate()
            .map(|(index, pending)| {
                let failure = state
                    .failure
                    .as_ref()
                    .filter(|_| index == 0 && floor == Some(pending.outbound.id));
                ParkedEvent {
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
                }
            })
            .collect()
    }

    fn reserve(&self, wanted: u64) -> u64 {
        let max = self.max_parked;
        let take = |parked: u64| {
            if max == 0 {
                wanted
            } else {
                wanted.min(max.saturating_sub(parked))
            }
        };
        if wanted == 0 {
            return 0;
        }
        self.parked
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |parked| {
                let granted = take(parked);
                (granted > 0).then(|| parked.saturating_add(granted))
            })
            .map_or(0, take)
    }

    fn release(&self, count: u64) {
        if count == 0 {
            return;
        }
        let _ = self
            .parked
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |parked| {
                Some(parked.saturating_sub(count))
            });
    }

    fn admit(&mut self, key: OrderKeyId) -> (u64, u64) {
        let Some(state) = self.keys.get(&key) else {
            return (0, 0);
        };
        let wanted = u64::try_from(state.queue.len()).unwrap_or(u64::MAX);
        let held = state.reserved.min(wanted);
        let granted = self.reserve(wanted - held);
        let allowed = held.saturating_add(granted);
        if let Some(state) = self.keys.get_mut(&key) {
            state.reserved = allowed;
            state.counted = state.counted.saturating_add(granted);
        }
        (wanted, allowed)
    }

    async fn flush_appends(&mut self) {
        let mut queues: Vec<(OrderKeyId, Vec<ParkedEvent>)> = Vec::new();
        let mut held_back = 0_u64;
        for key in std::mem::take(&mut self.to_append) {
            let (wanted, allowed) = self.admit(key);
            held_back = held_back.saturating_add(wanted - allowed);
            let events = self.queued_events(&key, usize::try_from(allowed).unwrap_or(usize::MAX));
            if events.is_empty() {
                if wanted > 0 {
                    self.to_append.insert(key);
                }
            } else {
                queues.push((key, events));
            }
        }
        if !queues.is_empty() {
            self.append(queues).await;
        }
        if held_back > 0 {
            let delay = self.store_backoff.next_delay();
            tracing::warn!(
                lane = self.index,
                parked = self.parked.load(Ordering::SeqCst),
                held_back,
                max_parked_events = self.max_parked,
                ?delay,
                "the parked event limit is reached; events stay in flight"
            );
            self.append_due = Some(Instant::now() + delay);
        } else {
            self.append_due = None;
        }
    }

    async fn append(&mut self, queues: Vec<(OrderKeyId, Vec<ParkedEvent>)>) {
        let count = queues
            .iter()
            .map(|(_, events)| u64::try_from(events.len()).unwrap_or(u64::MAX))
            .fold(0_u64, u64::saturating_add);
        let store = Arc::clone(&self.store);
        let span = tracing::debug_span!("park", events = count);
        let appends = queues
            .iter()
            .map(|(_, events)| store.append_parked(events))
            .collect();
        let appended = self.publishing(join_all(appends).instrument(span)).await;
        let mut failure = None;
        for ((key, events), appended) in queues.into_iter().zip(appended) {
            match appended {
                Ok(()) => self.appended(key, &events),
                Err(error) => {
                    self.to_append.insert(key);
                    failure = Some(error);
                }
            }
        }
        match failure {
            Some(error) => self.store_failed("append", &error),
            None => self.store_succeeded(),
        }
    }

    fn appended(&mut self, key: OrderKeyId, events: &[ParkedEvent]) {
        record_parked(events);
        let Some(state) = self.keys.get_mut(&key) else {
            return;
        };
        let count = u64::try_from(events.len()).unwrap_or(u64::MAX);
        state.reserved = state.reserved.saturating_sub(count);
        let last = events.last().map(|event| event.id);
        while let Some(pending) = state
            .queue
            .pop_front_if(|pending| last.is_some_and(|last| pending.outbound.id <= last))
        {
            if let Ok(settled) = pending.ticket.park(&Ok(())) {
                let _ = self.reports.send(Report::Settled(settled));
            }
        }
        if !state.queue.is_empty() {
            self.to_append.insert(key);
        }
        if let Some(stored) = state.stored.as_mut()
            && stored.dead_on_append
        {
            stored.dead_on_append = false;
            stored.dead = true;
            self.stats_dirty = true;
            tracing::error!(order_key = %key, id = ?stored.floor, "a dead event is parked and blocks its order key");
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
        while let Some(joined) = self.publishing(reads.join_next()).await {
            let (key, read) = match joined {
                Ok(answer) => answer,
                Err(error) => {
                    failure = Some(StoreError::Unavailable(error.to_string()));
                    continue;
                }
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
        self.stats_dirty = true;
        let Some(head) = head else {
            state.reserved = 0;
            let counted = std::mem::take(&mut state.counted);
            state.stored = None;
            state.backoff.reset();
            state.failure = None;
            self.to_append.remove(&key);
            if state.queue.is_empty() {
                self.keys.remove(&key);
            } else {
                self.make_ready(key);
            }
            self.release(counted);
            return;
        };
        stored.floor = Some(head.id);
        stored.current = Some(head.id);
        stored.since_ms = Some(head.first_failure_ms);
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
        self.heads_in_memory += 1;
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
                record_delivered(&head, None);
                state.backoff.reset();
                state.failure = None;
                if let Some(pending) = state
                    .queue
                    .pop_front_if(|pending| pending.outbound.id == head.id)
                {
                    state.reserved = state.reserved.saturating_sub(1);
                    if let Ok(settled) = pending.ticket.settle(outcome) {
                        let _ = self.reports.send(Report::Settled(settled));
                    }
                }
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
                self.to_kill.push(Kill::of(key, &head));
            }
        }
    }

    async fn flush_removes(&mut self) {
        let removals = std::mem::take(&mut self.to_remove);
        let store = Arc::clone(&self.store);
        let span = tracing::debug_span!(
            "drain",
            events = u64::try_from(removals.len()).unwrap_or(u64::MAX)
        );
        let removed = self
            .publishing(store.remove_parked(&removals).instrument(span))
            .await;
        if let Err(error) = removed {
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
            let Some(state) = self.keys.get_mut(&key) else {
                continue;
            };
            state.counted = state.counted.saturating_sub(1);
            if let Some(stored) = state.stored.as_mut() {
                stored.floor = Some(id);
                stored.current = None;
                stored.since_ms = None;
                self.stats_dirty = true;
                self.to_load.push_back(key);
            }
        }
    }

    async fn flush_kills(&mut self) {
        let store = Arc::clone(&self.store);
        while let Some(kill) = self.to_kill.pop() {
            match self
                .publishing(store.set_parked_status(&kill.key, &kill.id, ParkStatus::Dead))
                .await
            {
                Ok(()) => {}
                Err(error @ (StoreError::NotFound(_) | StoreError::Conflict(_))) => {
                    tracing::warn!(order_key = %kill.key, id = %kill.id, %error, "the head changed in the store before it was marked dead; reloading it");
                    self.reload(kill.key);
                    continue;
                }
                Err(error) => {
                    self.store_failed("mark dead", &error);
                    self.to_kill.push(kill);
                    return;
                }
            }
            if let Some((table, op)) = &kill.labels {
                metrics::record(Measurement::Event {
                    table,
                    op,
                    outcome: EventOutcome::Dead,
                    count: 1,
                });
            }
            if let Some(stored) = self
                .keys
                .get_mut(&kill.key)
                .and_then(|state| state.stored.as_mut())
            {
                stored.dead = true;
                self.stats_dirty = true;
            }
        }
        self.store_succeeded();
    }
}

// Store writes must stay in the lane task: a spawned write outlives an aborted lane and escapes the gate's abandoned-write count.
async fn join_all<F: Future>(futures: Vec<F>) -> Vec<F::Output> {
    let mut running: FuturesUnordered<_> = futures
        .into_iter()
        .enumerate()
        .map(|(index, future)| async move { (index, future.await) })
        .collect();
    let mut outputs: Vec<Option<F::Output>> = std::iter::repeat_with(|| None)
        .take(running.len())
        .collect();
    while let Some((index, output)) = running.next().await {
        if let Some(slot) = outputs.get_mut(index) {
            *slot = Some(output);
        }
    }
    outputs.into_iter().flatten().collect()
}

fn record_parked(events: &[ParkedEvent]) {
    if !metrics::enabled() {
        return;
    }
    let mut groups: BTreeMap<(&str, &str), u64> = BTreeMap::new();
    for event in events {
        *groups.entry(metric_labels(&event.attributes)).or_default() += 1;
    }
    for ((table, op), count) in groups {
        metrics::record(Measurement::Event {
            table,
            op,
            outcome: EventOutcome::Parked,
            count,
        });
    }
    for event in events
        .iter()
        .filter(|event| event.status == ParkStatus::Dead)
    {
        let (table, op) = metric_labels(&event.attributes);
        metrics::record(Measurement::Event {
            table,
            op,
            outcome: EventOutcome::Dead,
            count: 1,
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    use super::*;

    struct Counted<'a>(&'a AtomicUsize);

    impl Drop for Counted<'_> {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn join_all_returns_outputs_in_input_order_when_they_complete_in_reverse() {
        let count = 500_u64;
        let futures: Vec<_> = (0..count)
            .map(|index| {
                Box::pin(async move {
                    tokio::time::sleep(Duration::from_millis(count - index)).await;
                    index
                })
            })
            .collect();
        assert_eq!(join_all(futures).await, (0..count).collect::<Vec<_>>());
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_join_all_drops_its_unfinished_futures_in_place() {
        let dropped = AtomicUsize::new(0);
        let futures: Vec<_> = (0..3_u64)
            .map(|index| {
                let guard = Counted(&dropped);
                Box::pin(async move {
                    let _guard = guard;
                    if index > 0 {
                        std::future::pending::<()>().await;
                    }
                })
            })
            .collect();
        let joined = tokio::time::timeout(Duration::from_secs(1), join_all(futures)).await;
        assert!(joined.is_err());
        assert_eq!(dropped.load(Ordering::SeqCst), 3);
    }
}
