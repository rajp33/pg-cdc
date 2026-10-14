use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter, Stdout};

#[derive(Debug, PartialEq, Eq)]
pub struct Delivered {
    events: u64,
}

impl Delivered {
    pub fn into_events(self) -> u64 {
        self.events
    }
}

#[derive(Debug)]
pub struct StdoutSink<W> {
    out: BufWriter<W>,
    pending: u64,
}

impl StdoutSink<Stdout> {
    pub fn stdout() -> Self {
        Self::new(tokio::io::stdout())
    }
}

impl<W: AsyncWrite + Unpin> StdoutSink<W> {
    pub fn new(writer: W) -> Self {
        Self {
            out: BufWriter::new(writer),
            pending: 0,
        }
    }

    pub async fn write(&mut self, body: &[u8]) -> std::io::Result<()> {
        self.out.write_all(body).await?;
        self.out.write_all(b"\n").await?;
        self.pending += 1;
        Ok(())
    }

    pub async fn flush(&mut self) -> std::io::Result<Delivered> {
        self.out.flush().await?;
        Ok(Delivered {
            events: std::mem::take(&mut self.pending),
        })
    }

    pub async fn reset_pending(&mut self) -> std::io::Result<()> {
        self.flush().await.map(drop)
    }

    pub fn pending(&self) -> u64 {
        self.pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn writes_json_lines_and_receipts_count_flushed_events() {
        let mut sink = StdoutSink::new(Vec::new());
        sink.write(br#"{"a":1}"#).await.unwrap();
        sink.write(br#"{"b":2}"#).await.unwrap();
        assert_eq!(sink.pending(), 2);
        let receipt = sink.flush().await.unwrap();
        assert_eq!(receipt.into_events(), 2);
        assert_eq!(sink.pending(), 0);
        assert_eq!(sink.flush().await.unwrap().into_events(), 0);
        assert_eq!(sink.out.get_ref().as_slice(), b"{\"a\":1}\n{\"b\":2}\n");
    }

    #[tokio::test]
    async fn reset_pending_emits_buffered_lines_and_clears_the_count() {
        let mut sink = StdoutSink::new(Vec::new());
        sink.write(b"{}").await.unwrap();
        sink.write(b"{}").await.unwrap();
        sink.reset_pending().await.unwrap();
        assert_eq!(sink.pending(), 0);
        assert_eq!(sink.out.get_ref().as_slice(), b"{}\n{}\n");
        assert_eq!(sink.flush().await.unwrap().into_events(), 0);
    }

    #[tokio::test]
    async fn nothing_reaches_the_writer_before_flush() {
        let (writer, mut reader) = tokio::io::duplex(1024);
        let mut sink = StdoutSink::new(writer);
        sink.write(b"{}").await.unwrap();
        let mut buffer = [0u8; 8];
        let early =
            tokio::time::timeout(Duration::from_millis(100), reader.read(&mut buffer)).await;
        assert!(early.is_err(), "bytes reached the reader before flush");
        sink.flush().await.unwrap();
        let read = reader.read(&mut buffer).await.unwrap();
        assert_eq!(&buffer[..read], b"{}\n");
    }
}
