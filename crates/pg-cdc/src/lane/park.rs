use std::cmp::Reverse;

use tokio::time::Instant;

use super::{Failure, Key, Lane, Stored};
use crate::order::OrderKeyId;
use crate::sink::SinkError;
use crate::state::now_ms;

impl Key {
    pub(super) fn record_failure(&mut self, error: &SinkError) {
        let failure = self.failure.get_or_insert_with(|| Failure {
            since: Instant::now(),
            since_ms: now_ms(),
            attempts: 0,
            error: String::new(),
        });
        failure.attempts = failure.attempts.saturating_add(1);
        failure.error = error.to_string();
    }
}

impl Lane {
    pub(super) fn park_due(&self, state: &Key, now: Instant) -> bool {
        state
            .failure
            .as_ref()
            .is_some_and(|failure| now >= failure.since + self.park_after)
    }

    pub(super) fn retry_live(&mut self, key: OrderKeyId, error: &SinkError) {
        let Some(state) = self.keys.get_mut(&key) else {
            return;
        };
        let id = state.queue.front().map(|head| head.outbound.id);
        if state.failure.is_none() {
            tracing::warn!(order_key = %key, ?id, %error, "delivery failed; retrying");
        } else {
            tracing::debug!(order_key = %key, ?id, %error, "delivery failed again; retrying");
        }
        state.record_failure(error);
        let now = Instant::now();
        let Some(since) = state.failure.as_ref().map(|failure| failure.since) else {
            return;
        };
        let park_at = since + self.park_after;
        if now >= park_at {
            self.start_parking(key, false);
            return;
        }
        let due = (now + state.backoff.next_delay()).min(park_at);
        self.retries.push(Reverse((due, key)));
    }

    pub(super) fn start_parking(&mut self, key: OrderKeyId, dead: bool) {
        let Some(state) = self.keys.get_mut(&key) else {
            return;
        };
        if state.stored.is_some() || state.queue.is_empty() {
            return;
        }
        let floor = state.queue.front().map(|pending| pending.outbound.id);
        state.offloaded = None;
        let since_ms = state
            .failure
            .as_ref()
            .map_or_else(now_ms, |failure| failure.since_ms);
        state.stored = Some(Stored {
            floor,
            current: floor,
            dead_on_append: dead,
            since_ms: Some(since_ms),
            ..Stored::default()
        });
        self.stats_dirty = true;
        tracing::warn!(order_key = %key, id = ?floor, dead, "parking an order key's events");
        self.to_append.insert(key);
        if !dead && !self.closing {
            let delay = state.backoff.next_delay();
            self.retries.push(Reverse((Instant::now() + delay, key)));
        }
    }

    pub(super) fn oversize(&mut self, key: OrderKeyId, bytes: usize) {
        if self.payload.is_some() {
            if !self.to_offload.contains(&key) {
                self.to_offload.push_back(key);
            }
            return;
        }
        let error = SinkError(format!(
            "the event is {bytes} bytes, above the sink's limit of {}",
            self.capabilities.max_message_bytes
        ));
        self.give_up(key, &error);
    }

    pub(super) fn give_up(&mut self, key: OrderKeyId, error: &SinkError) {
        let Some(state) = self.keys.get_mut(&key) else {
            return;
        };
        if let Some(stored) = state.stored.as_mut() {
            if let Some(head) = stored.head.take() {
                self.heads_in_memory = self.heads_in_memory.saturating_sub(1);
                tracing::error!(order_key = %key, id = %head.id, %error, "a parked event cannot be sent; marking it dead");
                self.to_kill.push((key, head.id));
            }
        } else {
            tracing::error!(order_key = %key, %error, "an event cannot be sent; parking it dead");
            state.record_failure(error);
            self.start_parking(key, true);
        }
    }

    pub(super) fn park_all(&mut self) {
        self.closing = true;
        self.ready.clear();
        let keys: Vec<OrderKeyId> = self
            .keys
            .iter()
            .filter(|(key, state)| state.stored.is_none() && self.is_loaded(**key))
            .map(|(key, _)| *key)
            .collect();
        for key in keys {
            self.start_parking(key, false);
        }
    }
}
