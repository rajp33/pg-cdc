use std::io::{self, Write};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::thread;
use std::time::{Duration, Instant};

use tracing_subscriber::fmt::MakeWriter;

pub const LOG_BUFFER_LINES: usize = 8192;

const FLUSH_POLL: Duration = Duration::from_millis(10);

enum Message {
    Line(Vec<u8>),
    Flush(mpsc::Sender<()>),
}

#[derive(Debug, Clone)]
pub struct NonBlocking {
    sender: SyncSender<Message>,
}

#[derive(Debug)]
pub struct LogFlush {
    sender: SyncSender<Message>,
}

pub fn non_blocking<W: Write + Send + 'static>(
    writer: W,
    capacity: usize,
) -> io::Result<(NonBlocking, LogFlush)> {
    let (sender, receiver) = mpsc::sync_channel(capacity);
    thread::Builder::new()
        .name("pg-cdc-logs".to_owned())
        .spawn(move || drain(writer, &receiver))?;
    Ok((
        NonBlocking {
            sender: sender.clone(),
        },
        LogFlush { sender },
    ))
}

fn drain<W: Write>(mut writer: W, receiver: &Receiver<Message>) {
    for message in receiver {
        match message {
            Message::Line(line) => {
                let _ = writer.write_all(&line).and_then(|()| writer.flush());
            }
            Message::Flush(done) => {
                let _ = done.send(());
            }
        }
    }
}

impl Write for NonBlocking {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let _ = self.sender.try_send(Message::Line(buf.to_vec()));
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for NonBlocking {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

impl LogFlush {
    pub fn flush(&self, within: Duration) -> bool {
        let deadline = Instant::now() + within;
        let (done, finished) = mpsc::channel();
        let mut message = Message::Flush(done);
        loop {
            match self.sender.try_send(message) {
                Ok(()) => break,
                Err(TrySendError::Full(back)) if Instant::now() < deadline => {
                    message = back;
                    thread::sleep(FLUSH_POLL);
                }
                Err(_) => return false,
            }
        }
        finished
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .is_ok()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    type Lines = Arc<Mutex<Vec<Vec<u8>>>>;

    struct Gated {
        gate: Receiver<()>,
        lines: Lines,
    }

    impl Write for Gated {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let _ = self.gate.recv();
            self.lines.lock().unwrap().push(buf.to_vec());
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn gated(capacity: usize) -> (NonBlocking, LogFlush, mpsc::Sender<()>, Lines) {
        let (open, gate) = mpsc::channel();
        let lines = Lines::default();
        let writer = Gated {
            gate,
            lines: Arc::clone(&lines),
        };
        let (logs, flush) = non_blocking(writer, capacity).unwrap();
        (logs, flush, open, lines)
    }

    fn line(index: usize) -> Vec<u8> {
        format!("line {index}\n").into_bytes()
    }

    #[test]
    fn lines_reach_the_writer_in_order_by_the_time_flush_returns() {
        let (logs, flush, open, lines) = gated(LOG_BUFFER_LINES);
        drop(open);
        let mut writer = logs.make_writer();
        for index in 0..100 {
            writer.write_all(&line(index)).unwrap();
        }

        assert!(flush.flush(Duration::from_secs(5)));
        assert_eq!(
            *lines.lock().unwrap(),
            (0..100).map(line).collect::<Vec<_>>()
        );
    }

    #[test]
    fn a_stuck_writer_never_blocks_a_log_call_and_flush_gives_up_at_its_bound() {
        let (logs, flush, open, lines) = gated(2);
        let started = Instant::now();
        let mut writer = logs.make_writer();
        for index in 0..1000 {
            writer.write_all(&line(index)).unwrap();
        }
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "{:?}",
            started.elapsed()
        );

        let flushing = Instant::now();
        assert!(!flush.flush(Duration::from_millis(200)));
        let waited = flushing.elapsed();
        assert!(waited >= Duration::from_millis(200), "{waited:?}");
        assert!(waited < Duration::from_secs(1), "{waited:?}");
        assert!(lines.lock().unwrap().is_empty());
        drop(open);
    }

    #[test]
    fn lines_beyond_the_buffer_are_dropped_and_the_rest_keep_their_order() {
        let (logs, flush, open, lines) = gated(2);
        let mut writer = logs.make_writer();
        for index in 0..1000 {
            writer.write_all(&line(index)).unwrap();
        }
        drop(open);

        assert!(flush.flush(Duration::from_secs(5)));
        let delivered = lines.lock().unwrap().clone();
        assert!(delivered.len() <= 3, "{} lines kept", delivered.len());
        assert_eq!(delivered.first(), Some(&line(0)));
        let mut remaining = (0..1000).map(line);
        assert!(
            delivered
                .iter()
                .all(|kept| remaining.any(|sent| &sent == kept))
        );
    }
}
