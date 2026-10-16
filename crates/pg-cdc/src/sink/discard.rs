use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;

use super::{Outbound, Outcome, Sink, SinkCapabilities, SinkError};

pub const DISCARD_CAPABILITIES: SinkCapabilities = SinkCapabilities {
    max_batch_messages: 1024,
    max_batch_bytes: usize::MAX,
    max_message_bytes: usize::MAX,
};

#[derive(Debug, Default)]
pub struct DiscardSink {
    events: AtomicU64,
}

impl DiscardSink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn events(&self) -> u64 {
        self.events.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl Sink for DiscardSink {
    fn name(&self) -> &'static str {
        "discard"
    }

    fn capabilities(&self) -> SinkCapabilities {
        DISCARD_CAPABILITIES
    }

    async fn preflight(&self) -> Result<(), SinkError> {
        Ok(())
    }

    async fn send(&self, batch: &[Outbound]) -> Vec<Outcome> {
        let count = u64::try_from(batch.len()).unwrap_or(u64::MAX);
        self.events.fetch_add(count, Ordering::Relaxed);
        vec![Outcome::Delivered; batch.len()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Op, sample_envelope};
    use crate::order::OrderKeyId;

    #[tokio::test]
    async fn discard_counts_every_message_and_delivers_it() {
        let sink = DiscardSink::new();
        sink.preflight().await.unwrap();
        let outbound = Outbound::encode(
            &sample_envelope(Op::Truncate, None, None, None),
            OrderKeyId::of(b"[]"),
            &[],
        )
        .unwrap();
        let batch = vec![outbound; 3];
        assert_eq!(sink.send(&batch).await, vec![Outcome::Delivered; 3]);
        assert_eq!(sink.send(&batch[..1]).await, vec![Outcome::Delivered]);
        assert_eq!(sink.send(&[]).await, Vec::new());
        assert_eq!(sink.events(), 4);
        assert_eq!(sink.capabilities(), DISCARD_CAPABILITIES);
    }
}
