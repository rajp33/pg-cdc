use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use pg_cdc_decoder::Lsn;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use tokio::io::AsyncReadExt;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tracing::Instrument;

use crate::config::ReplicationConfig;
use crate::error::ReplicationError;
use crate::protocol::{
    Backend, ReplicationEvent, parse_copy_data, pg_time, protocol, status_update,
};
use crate::startup::{self, Conn, Io};

type Delivery = Result<ReplicationEvent, ReplicationError>;
type StatusReply = oneshot::Sender<Result<(), ReplicationError>>;

#[derive(Debug, Clone, Default)]
pub struct StatusClock {
    sent_ms: Arc<AtomicU64>,
}

impl StatusClock {
    pub fn record(&self, at: SystemTime) {
        let ms = at.duration_since(UNIX_EPOCH).map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        });
        self.sent_ms.store(ms, Ordering::Release);
    }

    pub fn last_sent(&self) -> Option<SystemTime> {
        match self.sent_ms.load(Ordering::Acquire) {
            0 => None,
            ms => Some(UNIX_EPOCH + Duration::from_millis(ms)),
        }
    }
}

pub struct ReplicationStream {
    events: mpsc::Receiver<Delivery>,
    confirmed: Arc<AtomicU64>,
    stop: Option<oneshot::Sender<()>>,
    status: mpsc::Sender<StatusReply>,
    task: JoinHandle<Result<(), ReplicationError>>,
    backend_pid: u32,
    connect_timeout: Duration,
    clock: StatusClock,
}

impl ReplicationStream {
    pub async fn start(config: ReplicationConfig) -> Result<Self, ReplicationError> {
        let timeout = config.connect_timeout;
        match tokio::time::timeout(timeout, Self::connect(config)).await {
            Ok(result) => result,
            Err(_) => Err(ReplicationError::Timeout(timeout)),
        }
    }

    async fn connect(config: ReplicationConfig) -> Result<Self, ReplicationError> {
        let io = startup::open(&config).await?;
        Self::start_on(io, config).await
    }

    pub(crate) async fn start_on(
        io: Box<dyn Io>,
        config: ReplicationConfig,
    ) -> Result<Self, ReplicationError> {
        if config.buffer == 0 {
            return Err(ReplicationError::Config("buffer must be at least 1"));
        }
        if config.status_interval.is_zero() {
            return Err(ReplicationError::Config("status_interval must be positive"));
        }
        let mut conn = Conn::new(io);
        let backend_pid = startup::handshake(&mut conn, &config).await?;
        tracing::debug!(backend_pid, slot = %config.slot, start_lsn = %config.start_lsn, "replication started");
        let confirmed = Arc::new(AtomicU64::new(config.start_lsn.0));
        let (sender, events) = mpsc::channel(config.buffer);
        let (stop, stop_requested) = oneshot::channel();
        let (status, status_requests) = mpsc::channel(1);
        let clock = StatusClock::default();
        let worker = Worker {
            conn,
            confirmed: Arc::clone(&confirmed),
            status_interval: config.status_interval,
            status_requests,
            clock: clock.clone(),
        };
        let task = tokio::spawn(worker.run(sender, stop_requested));
        Ok(Self {
            events,
            confirmed,
            stop: Some(stop),
            status,
            task,
            backend_pid,
            connect_timeout: config.connect_timeout,
            clock,
        })
    }

    pub fn status_clock(&self) -> StatusClock {
        self.clock.clone()
    }

    pub fn backend_pid(&self) -> u32 {
        self.backend_pid
    }

    pub async fn recv(&mut self) -> Result<Option<ReplicationEvent>, ReplicationError> {
        self.events.recv().await.transpose()
    }

    pub fn confirm(&self, lsn: Lsn) {
        self.confirmed.fetch_max(lsn.0, Ordering::AcqRel);
    }

    pub async fn send_status(&self) -> Result<(), ReplicationError> {
        let (reply, sent) = oneshot::channel();
        self.status
            .send(reply)
            .await
            .map_err(|_| ReplicationError::Closed)?;
        sent.await.map_err(|_| ReplicationError::Closed)?
    }

    pub async fn stop(mut self) -> Result<(), ReplicationError> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        match tokio::time::timeout(self.connect_timeout, &mut self.task).await {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => Err(ReplicationError::Worker(error.to_string())),
            Err(_) => Err(ReplicationError::Timeout(self.connect_timeout)),
        }
    }
}

impl fmt::Debug for ReplicationStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplicationStream")
            .field("backend_pid", &self.backend_pid)
            .field("confirmed", &Lsn(self.confirmed.load(Ordering::Acquire)))
            .finish_non_exhaustive()
    }
}

impl Drop for ReplicationStream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Worker {
    conn: Conn,
    confirmed: Arc<AtomicU64>,
    status_interval: Duration,
    status_requests: mpsc::Receiver<StatusReply>,
    clock: StatusClock,
}

enum Ending {
    Stop,
    Done,
}

enum Step {
    Event(ReplicationEvent),
    Idle,
    Done,
}

impl Worker {
    async fn run(
        mut self,
        events: mpsc::Sender<Delivery>,
        mut stop: oneshot::Receiver<()>,
    ) -> Result<(), ReplicationError> {
        match self.stream(&events, &mut stop).await {
            Ok(Ending::Stop) => self.shutdown().await,
            Ok(Ending::Done) => {
                tracing::debug!("replication stream ended");
                Ok(())
            }
            Err(error) => {
                tracing::debug!(%error, "replication stream failed");
                self.status_requests.close();
                while let Ok(reply) = self.status_requests.try_recv() {
                    let _ = reply.send(Err(ReplicationError::Worker(error.to_string())));
                }
                let failure = ReplicationError::Worker(error.to_string());
                tokio::select! {
                    biased;
                    _ = stop => {}
                    _ = events.send(Err(error)) => {}
                }
                Err(failure)
            }
        }
    }

    async fn stream(
        &mut self,
        events: &mpsc::Sender<Delivery>,
        stop: &mut oneshot::Receiver<()>,
    ) -> Result<Ending, ReplicationError> {
        let mut ticker = tokio::time::interval(self.status_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut pending = None;
        loop {
            if pending.is_none() {
                match self.drain_buffer().await? {
                    Step::Event(event) => pending = Some(event),
                    Step::Idle => {}
                    Step::Done => return Ok(Ending::Done),
                }
            }
            tokio::select! {
                biased;
                _ = &mut *stop => return Ok(Ending::Stop),
                _ = ticker.tick() => self.send_status().await?,
                Some(reply) = self.status_requests.recv() => {
                    if let Err(error) = self.send_status().await {
                        let _ = reply.send(Err(ReplicationError::Worker(error.to_string())));
                        return Err(error);
                    }
                    let _ = reply.send(Ok(()));
                }
                permit = events.reserve(), if pending.is_some() => {
                    let Ok(permit) = permit else {
                        return Ok(Ending::Done);
                    };
                    if let Some(event) = pending.take() {
                        permit.send(Ok(event));
                    }
                }
                read = self.conn.io.read_buf(&mut self.conn.read), if pending.is_none() => {
                    if read? == 0 {
                        return Err(ReplicationError::Closed);
                    }
                }
            }
        }
    }

    async fn drain_buffer(&mut self) -> Result<Step, ReplicationError> {
        match self.conn.try_next()? {
            None => Ok(Step::Idle),
            Some(Backend::Message(Message::CopyData(body))) => {
                let (event, reply_requested) = parse_copy_data(body.into_bytes())?;
                if reply_requested {
                    self.send_status().await?;
                }
                Ok(Step::Event(event))
            }
            Some(Backend::Message(Message::CopyDone)) => Ok(Step::Done),
            Some(_) => Err(protocol("unexpected message while streaming")),
        }
    }

    async fn send_status(&mut self) -> Result<(), ReplicationError> {
        let confirmed = Lsn(self.confirmed.load(Ordering::Acquire));
        let span = tracing::debug_span!("status_update", lsn = %confirmed);
        async {
            status_update(confirmed, pg_time(SystemTime::now()), &mut self.conn.write)?;
            self.conn.flush().await?;
            self.clock.record(SystemTime::now());
            Ok(())
        }
        .instrument(span)
        .await
    }

    async fn shutdown(&mut self) -> Result<(), ReplicationError> {
        self.send_status().await?;
        frontend::copy_done(&mut self.conn.write);
        self.conn.flush().await?;
        loop {
            match self.conn.next().await {
                Ok(Backend::Message(
                    Message::CopyDone | Message::CommandComplete(_) | Message::ReadyForQuery(_),
                )) => break,
                Ok(_) => {}
                Err(ReplicationError::Closed) => return Ok(()),
                Err(error) => return Err(error),
            }
        }
        frontend::terminate(&mut self.conn.write);
        self.conn.flush().await?;
        tracing::debug!("replication stream stopped");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use tokio::time::Instant;

    use super::*;
    use crate::fake;

    #[tokio::test(start_paused = true)]
    async fn a4_status_updates_fire_while_consumer_stalls() {
        let mut config = fake::config();
        config.buffer = 1;
        let (mut stream, mut server) = fake::started(config).await;
        for n in 1..=3_u64 {
            server.xlog(n * 0x100, n * 0x100 + 0x10, b"B").await;
        }
        let stalled = Instant::now();
        for _ in 0..4 {
            let status = tokio::time::timeout(Duration::from_secs(60), server.read_status())
                .await
                .expect("no status update while the consumer stalled");
            assert_eq!(
                (status.write, status.flush, status.apply, status.reply),
                (0x10, 0x10, 0x10, 0)
            );
        }
        assert!(
            stalled.elapsed() >= Duration::from_secs(30),
            "{:?}",
            stalled.elapsed()
        );
        for n in 1..=3_u64 {
            assert_eq!(
                stream.recv().await.unwrap(),
                Some(ReplicationEvent::XLogData {
                    wal_start: Lsn(n * 0x100),
                    wal_end: Lsn(n * 0x100 + 0x10),
                    data: Bytes::from_static(b"B"),
                })
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_keepalive_requesting_a_reply_gets_an_immediate_status_update() {
        let (mut stream, mut server) = fake::started(fake::config()).await;
        server.read_status().await;
        let sent = Instant::now();
        server.keepalive(0x50, true).await;
        let status = server.read_status().await;
        assert!(
            sent.elapsed() < Duration::from_secs(10),
            "the reply waited for the timer"
        );
        assert_eq!(status.flush, 0x10);
        assert_eq!(
            stream.recv().await.unwrap(),
            Some(ReplicationEvent::KeepAlive { wal_end: Lsn(0x50) })
        );
    }

    #[derive(Clone, Default)]
    struct StatusSpans(std::sync::Arc<std::sync::Mutex<Vec<(String, tracing::Level, String)>>>);

    struct LsnField(String);

    impl tracing::field::Visit for LsnField {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
            if field.name() == "lsn" {
                self.0 = format!("{value:?}");
            }
        }
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for StatusSpans {
        fn on_new_span(
            &self,
            attributes: &tracing::span::Attributes<'_>,
            _id: &tracing::span::Id,
            _context: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut lsn = LsnField(String::new());
            attributes.record(&mut lsn);
            self.0.lock().unwrap().push((
                attributes.metadata().name().to_owned(),
                *attributes.metadata().level(),
                lsn.0,
            ));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_status_update_runs_in_a_debug_span_naming_the_confirmed_lsn() {
        use tracing_subscriber::layer::SubscriberExt;
        let spans = StatusSpans::default();
        let subscriber = tracing_subscriber::registry().with(spans.clone());
        let _guard = tracing::subscriber::set_default(subscriber);
        let (stream, mut server) = fake::started(fake::config()).await;
        server.read_status().await;
        stream.confirm(Lsn(0x40));
        stream.send_status().await.unwrap();
        server.read_status().await;
        let seen = spans.0.lock().unwrap().clone();
        let updates: Vec<&(String, tracing::Level, String)> = seen
            .iter()
            .filter(|(name, _, _)| name == "status_update")
            .collect();
        assert!(
            updates
                .iter()
                .all(|(_, level, _)| *level == tracing::Level::DEBUG)
        );
        assert_eq!(updates.last().map(|(_, _, lsn)| lsn.as_str()), Some("0/40"));
    }

    #[tokio::test(start_paused = true)]
    async fn a_status_update_span_is_filtered_out_at_info() {
        use tracing_subscriber::Layer;
        use tracing_subscriber::layer::SubscriberExt;
        let spans = StatusSpans::default();
        let subscriber = tracing_subscriber::registry().with(
            spans
                .clone()
                .with_filter(tracing::level_filters::LevelFilter::INFO),
        );
        let _guard = tracing::subscriber::set_default(subscriber);
        let (stream, mut server) = fake::started(fake::config()).await;
        server.read_status().await;
        stream.confirm(Lsn(0x40));
        stream.send_status().await.unwrap();
        assert_eq!(server.read_status().await.flush, 0x40);
        let seen = spans.0.lock().unwrap().clone();
        assert!(
            seen.iter().all(|(name, _, _)| name != "status_update"),
            "{seen:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stop_sends_the_highest_confirmed_lsn_then_copy_done() {
        let (stream, mut server) = fake::started(fake::config()).await;
        stream.confirm(Lsn(0x30));
        stream.confirm(Lsn(0x20));
        let stopping = tokio::spawn(stream.stop());
        let mut last_status = None;
        loop {
            let frame = server.read().await;
            match frame.tag {
                b'd' => last_status = Some(fake::Status::parse(&frame.body)),
                b'c' => break,
                other => panic!("unexpected frame {other:#04x}"),
            }
        }
        let last = last_status.unwrap();
        assert_eq!((last.write, last.flush, last.apply), (0x30, 0x30, 0x30));
        server.send(b'c', b"").await;
        assert_eq!(server.read().await.tag, b'X');
        stopping.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn send_status_writes_the_confirmed_lsn_without_waiting_for_the_timer() {
        let (mut stream, mut server) = fake::started(fake::config()).await;
        server.read_status().await;
        stream.confirm(Lsn(0x40));
        let sent = Instant::now();
        stream.send_status().await.unwrap();
        let status = server.read_status().await;
        assert_eq!(
            (status.write, status.flush, status.apply),
            (0x40, 0x40, 0x40)
        );
        assert_eq!(sent.elapsed(), Duration::ZERO);
        server.xlog(0x100, 0x110, b"B").await;
        assert!(matches!(
            stream.recv().await,
            Ok(Some(ReplicationEvent::XLogData { .. }))
        ));
    }

    #[tokio::test]
    async fn send_status_fails_once_the_stream_has_failed() {
        let (mut stream, mut server) = fake::started(fake::config()).await;
        server.read_status().await;
        drop(server);
        assert!(matches!(stream.recv().await, Err(ReplicationError::Closed)));
        assert!(stream.send_status().await.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn send_status_fails_at_once_when_a_failure_waits_behind_a_full_channel() {
        let mut config = fake::config();
        config.buffer = 1;
        let (stream, mut server) = fake::started(config).await;
        for n in 1..=2_u64 {
            server.xlog(n * 0x100, n * 0x100 + 0x10, b"B").await;
        }
        drop(server);
        tokio::time::sleep(Duration::from_secs(40)).await;
        let started = Instant::now();
        assert!(stream.send_status().await.is_err());
        assert_eq!(started.elapsed(), Duration::ZERO);
    }

    #[tokio::test]
    async fn server_copy_done_ends_the_stream_cleanly() {
        let (mut stream, mut server) = fake::started(fake::config()).await;
        server.xlog(0x100, 0x110, b"B").await;
        server.send(b'c', b"").await;
        assert!(matches!(
            stream.recv().await,
            Ok(Some(ReplicationEvent::XLogData { .. }))
        ));
        assert!(matches!(stream.recv().await, Ok(None)));
    }

    #[tokio::test]
    async fn streaming_errors_reach_the_consumer() {
        let (mut stream, mut server) = fake::started(fake::config()).await;
        server
            .error(
                "57P01",
                "terminating connection due to administrator command",
            )
            .await;
        assert_eq!(stream.recv().await.unwrap_err().sqlstate(), Some("57P01"));

        let (mut stream, mut server) = fake::started(fake::config()).await;
        server.send(b'd', b"x").await;
        assert!(matches!(
            stream.recv().await,
            Err(ReplicationError::Protocol(_))
        ));

        let (mut stream, mut server) = fake::started(fake::config()).await;
        server.read_status().await;
        drop(server);
        assert!(matches!(stream.recv().await, Err(ReplicationError::Closed)));
    }

    #[tokio::test]
    async fn stop_reports_a_stream_failure_the_consumer_already_saw() {
        let (mut stream, mut server) = fake::started(fake::config()).await;
        server
            .error(
                "57P01",
                "terminating connection due to administrator command",
            )
            .await;
        assert_eq!(stream.recv().await.unwrap_err().sqlstate(), Some("57P01"));
        let error = stream.stop().await.unwrap_err();
        assert!(
            matches!(&error, ReplicationError::Worker(message) if message.contains("57P01")),
            "{error:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stop_reports_a_stream_failure_stuck_behind_a_full_channel() {
        let mut config = fake::config();
        config.buffer = 1;
        let (stream, mut server) = fake::started(config).await;
        for n in 1..=2_u64 {
            server.xlog(n * 0x100, n * 0x100 + 0x10, b"B").await;
        }
        drop(server);
        tokio::time::sleep(Duration::from_secs(40)).await;
        let error = stream.stop().await.unwrap_err();
        assert!(
            matches!(&error, ReplicationError::Worker(message) if message.contains("connection failed")),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn notices_and_parameter_changes_are_skipped_while_streaming() {
        let (mut stream, mut server) = fake::started(fake::config()).await;
        server.send(b'S', b"TimeZone\0UTC\0").await;
        server.send(b'N', b"\0").await;
        server.xlog(0x100, 0x110, b"B").await;
        assert!(matches!(
            stream.recv().await,
            Ok(Some(ReplicationEvent::XLogData { .. }))
        ));
    }

    #[tokio::test]
    async fn a_zero_buffer_or_interval_is_rejected() {
        for (buffer, interval) in [(0, Duration::from_secs(1)), (1, Duration::ZERO)] {
            let mut config = fake::config();
            config.buffer = buffer;
            config.status_interval = interval;
            let (client, _server) = tokio::io::duplex(64);
            let error = ReplicationStream::start_on(Box::new(client), config)
                .await
                .unwrap_err();
            assert!(matches!(error, ReplicationError::Config(_)), "{error:?}");
        }
    }

    fn stale() -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_millis(1)
    }

    fn make_stale(clock: &StatusClock) -> SystemTime {
        clock.record(stale());
        assert_eq!(clock.last_sent(), Some(stale()));
        SystemTime::now()
    }

    fn assert_fresh(clock: &StatusClock, since: SystemTime) {
        let sent = clock.last_sent().expect("a status update was recorded");
        assert!(
            sent + Duration::from_millis(1) >= since,
            "{sent:?} is older than {since:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_status_clock_records_each_flushed_status_update() {
        let since = SystemTime::now();
        let (stream, mut server) = fake::started(fake::config()).await;
        let clock = stream.status_clock();
        let cloned = stream.status_clock();
        server.read_status().await;
        assert_fresh(&clock, since);

        let since = make_stale(&clock);
        server.read_status().await;
        assert_fresh(&clock, since);

        let since = make_stale(&clock);
        server.keepalive(0x50, true).await;
        server.read_status().await;
        assert_fresh(&clock, since);

        let since = make_stale(&clock);
        stream.send_status().await.unwrap();
        assert_fresh(&cloned, since);
        server.read_status().await;

        let since = make_stale(&clock);
        let stopping = tokio::spawn(stream.stop());
        while server.read().await.tag != b'c' {}
        assert_fresh(&clock, since);
        server.send(b'c', b"").await;
        assert_eq!(server.read().await.tag, b'X');
        stopping.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn a_failed_status_update_leaves_the_status_clock_unchanged() {
        let (stream, mut server) = fake::started(fake::config()).await;
        let clock = stream.status_clock();
        server.read_status().await;
        make_stale(&clock);
        drop(server);
        assert!(stream.send_status().await.is_err());
        assert_eq!(clock.last_sent(), Some(stale()));
    }

    #[test]
    fn a_status_clock_is_empty_until_the_first_update() {
        let clock = StatusClock::default();
        assert_eq!(clock.last_sent(), None);
        let at = SystemTime::UNIX_EPOCH + Duration::from_millis(1_500);
        clock.record(at);
        assert_eq!(clock.last_sent(), Some(at));
    }
}
