use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter, Stdout};

const BUFFER_BYTES: usize = 8 * 1024;

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
            out: BufWriter::with_capacity(BUFFER_BYTES, writer),
            pending: 0,
        }
    }

    pub async fn write(&mut self, body: &[u8]) -> std::io::Result<()> {
        let mut line = Vec::with_capacity(body.len() + 1);
        line.extend_from_slice(body);
        line.push(b'\n');
        if line.len() < BUFFER_BYTES {
            self.out.write_all(&line).await?;
        } else {
            self.out.flush().await?;
            self.out.get_mut().write_all(&line).await?;
        }
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
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::time::Duration;
    use tokio::io::AsyncReadExt;

    #[derive(Default)]
    struct Chunks(Vec<Vec<u8>>);

    impl AsyncWrite for Chunks {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            self.0.push(buf.to_vec());
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn a_line_larger_than_the_buffer_reaches_the_writer_as_one_chunk() {
        let mut sink = StdoutSink::new(Chunks::default());
        sink.write(b"{}").await.unwrap();
        let wide = vec![b'x'; 64 * 1024];
        sink.write(&wide).await.unwrap();
        sink.flush().await.unwrap();
        let chunks = &sink.out.get_ref().0;
        let sizes: Vec<usize> = chunks.iter().map(Vec::len).collect();
        assert_eq!(sizes, [3, wide.len() + 1]);
        assert_eq!(chunks[0], b"{}\n");
        assert_eq!(chunks[1].last(), Some(&b'\n'));
    }

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
