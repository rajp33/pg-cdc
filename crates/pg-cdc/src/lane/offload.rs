use std::cmp::Reverse;

use tokio::time::Instant;

use super::Lane;
use crate::order::OrderKeyId;
use crate::payload::pointer_body;
use crate::sink::{Outbound, SinkError};

impl Lane {
    pub(super) async fn offload(&mut self) {
        let Some(key) = self.to_offload.pop_front() else {
            return;
        };
        let (Some(payload), Some(head)) = (self.payload.clone(), self.original(&key)) else {
            return;
        };
        let reference = match payload.put(&head.id, &head.body).await {
            Ok(reference) => reference,
            Err(error) => {
                let error = SinkError(format!("cannot store the oversize payload: {error}"));
                self.offload_failed(key, &error);
                return;
            }
        };
        let body = match pointer_body(&head.body, &reference) {
            Ok(body) => body,
            Err(error) => {
                let error = SinkError(format!("cannot build a pointer event: {error}"));
                self.give_up(key, &error);
                return;
            }
        };
        let pointer = Outbound { body, ..head };
        let bytes = pointer.encoded_len();
        if bytes > self.capabilities.max_message_bytes {
            let error = SinkError(format!(
                "the pointer event is {bytes} bytes, above the sink's limit of {}",
                self.capabilities.max_message_bytes
            ));
            self.give_up(key, &error);
            return;
        }
        tracing::info!(order_key = %key, id = %pointer.id, uri = %reference.uri, bytes = reference.bytes, "sending an oversize event as a pointer");
        let Some(state) = self.keys.get_mut(&key) else {
            return;
        };
        match state.stored.as_mut() {
            Some(stored) => stored.head = Some(pointer),
            None => state.offloaded = Some(pointer),
        }
        self.ready.push_back(key);
    }

    fn original(&self, key: &OrderKeyId) -> Option<Outbound> {
        let state = self.keys.get(key)?;
        match &state.stored {
            Some(stored) => stored.head.clone(),
            None => state.queue.front().map(|pending| pending.outbound.clone()),
        }
    }

    fn offload_failed(&mut self, key: OrderKeyId, error: &SinkError) {
        let Some(state) = self.keys.get_mut(&key) else {
            return;
        };
        if let Some(stored) = state.stored.as_mut() {
            if stored.head.take().is_some() {
                self.heads_in_memory = self.heads_in_memory.saturating_sub(1);
            }
            tracing::warn!(order_key = %key, %error, "retrying a parked oversize event");
            state.record_failure(error);
            let delay = state.backoff.next_delay();
            self.retries.push(Reverse((Instant::now() + delay, key)));
        } else {
            self.retry_live(key, error);
        }
    }
}
