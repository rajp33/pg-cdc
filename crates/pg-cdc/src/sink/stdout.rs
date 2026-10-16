use async_trait::async_trait;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

use super::{Outbound, Outcome, Sink, SinkCapabilities, SinkError};

pub const STDOUT_CAPABILITIES: SinkCapabilities = SinkCapabilities {
    max_batch_messages: 1024,
    max_batch_bytes: 1 << 20,
    max_message_bytes: usize::MAX,
};

type WriteRequest = (Vec<u8>, oneshot::Sender<std::io::Result<()>>);

#[derive(Debug)]
pub struct StdoutSink {
    requests: mpsc::Sender<WriteRequest>,
}

impl StdoutSink {
    pub fn stdout() -> Self {
        Self::new(tokio::io::stdout())
    }

    pub fn new<W: AsyncWrite + Unpin + Send + 'static>(writer: W) -> Self {
        let (requests, receiver) = mpsc::channel(1);
        tokio::spawn(write_lines(writer, receiver));
        Self { requests }
    }

    async fn write(&self, lines: Vec<u8>) -> Result<(), SinkError> {
        let (reply, written) = oneshot::channel();
        self.requests
            .send((lines, reply))
            .await
            .map_err(|_| SinkError("the stdout writer has stopped".to_owned()))?;
        written
            .await
            .map_err(|_| SinkError("the stdout writer has stopped".to_owned()))?
            .map_err(|error| SinkError(format!("cannot write to stdout: {error}")))
    }
}

async fn write_lines<W: AsyncWrite + Unpin>(
    mut writer: W,
    mut requests: mpsc::Receiver<WriteRequest>,
) {
    while let Some((lines, reply)) = requests.recv().await {
        let written = match writer.write_all(&lines).await {
            Ok(()) => writer.flush().await,
            Err(error) => Err(error),
        };
        let _ = reply.send(written);
    }
}

#[async_trait]
impl Sink for StdoutSink {
    fn name(&self) -> &'static str {
        "stdout"
    }

    fn capabilities(&self) -> SinkCapabilities {
        STDOUT_CAPABILITIES
    }

    async fn preflight(&self) -> Result<(), SinkError> {
        Ok(())
    }

    async fn send(&self, batch: &[Outbound]) -> Vec<Outcome> {
        let size = batch.iter().map(|outbound| outbound.body.len() + 1).sum();
        let mut lines = Vec::with_capacity(size);
        for outbound in batch {
            lines.extend_from_slice(&outbound.body);
            lines.push(b'\n');
        }
        let outcome = match self.write(lines).await {
            Ok(()) => Outcome::Delivered,
            Err(error) => Outcome::Retryable(error),
        };
        vec![outcome; batch.len()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{EventId, Op, sample_envelope};
    use crate::order::OrderKeyId;
    use bytes::Bytes;
    use pg_cdc_decoder::Lsn;
    use std::sync::Arc;
    use tokio::io::AsyncReadExt;

    fn outbound(ordinal: u32, body: &str) -> Outbound {
        let mut envelope = sample_envelope(Op::Insert, None, None, None);
        envelope.id = EventId::new(Lsn(0x10), ordinal);
        Outbound {
            body: Bytes::from(body.to_owned()),
            ..Outbound::encode(&envelope, OrderKeyId::of(b"[]"), &[]).unwrap()
        }
    }

    #[tokio::test]
    async fn writes_each_batch_as_json_lines_and_delivers_it() {
        let (writer, mut reader) = tokio::io::duplex(1024);
        let sink = StdoutSink::new(writer);
        let outcomes = sink
            .send(&[outbound(0, r#"{"a":1}"#), outbound(1, r#"{"b":2}"#)])
            .await;
        assert_eq!(outcomes, vec![Outcome::Delivered, Outcome::Delivered]);
        drop(sink);
        let mut output = String::new();
        reader.read_to_string(&mut output).await.unwrap();
        assert_eq!(output, "{\"a\":1}\n{\"b\":2}\n");
    }

    #[tokio::test]
    async fn a_closed_reader_makes_every_message_retryable() {
        let (writer, reader) = tokio::io::duplex(64);
        drop(reader);
        let sink = StdoutSink::new(writer);
        let outcomes = sink.send(&[outbound(0, "{}"), outbound(1, "{}")]).await;
        assert_eq!(outcomes.len(), 2);
        for outcome in outcomes {
            assert!(
                matches!(&outcome, Outcome::Retryable(SinkError(message)) if message.starts_with("cannot write to stdout")),
                "{outcome:?}"
            );
        }
    }

    #[tokio::test]
    async fn concurrent_batches_never_interleave_their_lines() {
        let (writer, mut reader) = tokio::io::duplex(1024);
        let sink = Arc::new(StdoutSink::new(writer));
        let mut senders = Vec::new();
        for batch in 0..8u32 {
            let sink = Arc::clone(&sink);
            senders.push(tokio::spawn(async move {
                let wide = format!("{{\"batch\":{batch},\"pad\":\"{}\"}}", "x".repeat(20_000));
                let messages = [outbound(batch * 2, &wide), outbound(batch * 2 + 1, &wide)];
                sink.send(&messages).await
            }));
        }
        let reading = tokio::spawn(async move {
            let mut output = Vec::new();
            reader.read_to_end(&mut output).await.unwrap();
            output
        });
        for sender in senders {
            assert_eq!(sender.await.unwrap(), vec![Outcome::Delivered; 2]);
        }
        drop(sink);
        let output = reading.await.unwrap();
        let lines: Vec<serde_json::Value> = output
            .split(|&byte| byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect();
        assert_eq!(lines.len(), 16);
        for pair in lines.chunks(2) {
            assert_eq!(pair[0]["batch"], pair[1]["batch"]);
        }
    }

    #[tokio::test]
    async fn stdout_preflight_passes_and_reports_an_unbounded_message_limit() {
        let sink = StdoutSink::new(tokio::io::sink());
        sink.preflight().await.unwrap();
        assert_eq!(sink.capabilities(), STDOUT_CAPABILITIES);
        assert_eq!(sink.capabilities().max_message_bytes, usize::MAX);
    }
}
