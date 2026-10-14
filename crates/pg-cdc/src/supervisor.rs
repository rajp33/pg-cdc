use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use pg_cdc_decoder::Lsn;
use tokio_util::sync::CancellationToken;

use crate::connection::ConnectionProvider;
use crate::lane::Stores;
use crate::session::{SessionConfig, SessionError, run_session};
use crate::sink::Sink;
use crate::state::StateStore;

pub const HALT_AFTER_FAULTS: u32 = 3;
pub const BACKOFF_BASE: Duration = Duration::from_secs(1);
pub const BACKOFF_CAP: Duration = Duration::from_secs(30);

pub trait Attempt {
    fn run(&mut self) -> impl Future<Output = Result<(), SessionError>> + Send;
}

pub struct LiveSession<'a> {
    pub config: &'a SessionConfig,
    pub provider: &'a dyn ConnectionProvider,
    pub sink: &'a Arc<dyn Sink>,
    pub stores: &'a Stores,
    pub shutdown: &'a CancellationToken,
}

impl fmt::Debug for LiveSession<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LiveSession")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl Attempt for LiveSession<'_> {
    fn run(&mut self) -> impl Future<Output = Result<(), SessionError>> + Send {
        run_session(
            self.config,
            self.provider,
            self.sink,
            self.stores,
            self.shutdown,
        )
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SupervisorError {
    #[error("halted after {count} consecutive faults at {lsn}: {last}")]
    Halted {
        lsn: Lsn,
        count: u32,
        last: SessionError,
    },
    #[error("stopped on a fatal error: {0}")]
    Fatal(SessionError),
}

#[derive(Debug)]
pub struct Backoff {
    base: Duration,
    cap: Duration,
    attempt: u32,
}

impl Default for Backoff {
    fn default() -> Self {
        Self::new(BACKOFF_BASE, BACKOFF_CAP)
    }
}

impl Backoff {
    pub fn new(base: Duration, cap: Duration) -> Self {
        Self {
            base,
            cap,
            attempt: 0,
        }
    }

    pub fn next_delay(&mut self) -> Duration {
        let factor = 1_u32.checked_shl(self.attempt).unwrap_or(u32::MAX);
        let cap = self.base.saturating_mul(factor).min(self.cap);
        self.attempt = self.attempt.saturating_add(1);
        let cap_ms = u64::try_from(cap.as_millis()).unwrap_or(u64::MAX);
        Duration::from_millis(fastrand::u64(0..=cap_ms))
    }

    pub fn reset(&mut self) {
        self.attempt = 0;
    }
}

#[derive(Debug, Default)]
struct FaultTracker {
    lsn: Option<Lsn>,
    count: u32,
}

impl FaultTracker {
    fn record(&mut self, lsn: Lsn) -> u32 {
        if self.lsn == Some(lsn) {
            self.count += 1;
        } else {
            self.lsn = Some(lsn);
            self.count = 1;
        }
        self.count
    }
}

pub async fn supervise<A: Attempt>(
    attempt: &mut A,
    shutdown: &CancellationToken,
) -> Result<(), SupervisorError> {
    let mut faults = FaultTracker::default();
    let mut backoff = Backoff::default();
    loop {
        let started = Instant::now();
        let error = match attempt.run().await {
            Ok(()) => return Ok(()),
            Err(error) => error,
        };
        if error.is_fatal() {
            return Err(SupervisorError::Fatal(error));
        }
        if let Some(lsn) = error.fault_lsn() {
            let count = faults.record(lsn);
            if count >= HALT_AFTER_FAULTS {
                return Err(SupervisorError::Halted {
                    lsn,
                    count,
                    last: error,
                });
            }
        }
        if started.elapsed() >= BACKOFF_CAP {
            backoff.reset();
        }
        let delay = backoff.next_delay();
        let delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX);
        tracing::warn!(error = %error, delay_ms, "replication session ended; reconnecting");
        if !sleep_unless_cancelled(delay, shutdown).await {
            return Ok(());
        }
    }
}

pub async fn preflight(sink: &dyn Sink, shutdown: &CancellationToken) -> bool {
    retry_until_ok("sink preflight failed; retrying", shutdown, || {
        sink.preflight()
    })
    .await
}

pub async fn store_ready(store: &dyn StateStore, shutdown: &CancellationToken) -> bool {
    retry_until_ok("state store check failed; retrying", shutdown, || async {
        store.read_leader().await.map(drop)
    })
    .await
}

async fn retry_until_ok<F, Fut, E>(
    failure: &'static str,
    shutdown: &CancellationToken,
    mut attempt: F,
) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(), E>>,
    E: fmt::Display,
{
    let mut backoff = Backoff::default();
    loop {
        let Some(result) = shutdown.run_until_cancelled(attempt()).await else {
            return false;
        };
        let error = match result {
            Ok(()) => return true,
            Err(error) => error,
        };
        let delay = backoff.next_delay();
        let delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX);
        tracing::warn!(%error, delay_ms, "{failure}");
        if !sleep_unless_cancelled(delay, shutdown).await {
            return false;
        }
    }
}

async fn sleep_unless_cancelled(delay: Duration, shutdown: &CancellationToken) -> bool {
    tokio::select! {
        biased;
        () = shutdown.cancelled() => false,
        () = tokio::time::sleep(delay) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ack::AckError;
    use crate::event::EventId;
    use crate::lane::{LaneError, LaneStopped};
    use crate::order::OrderKeyId;
    use crate::sink::{Outbound, Outcome, SinkCapabilities, SinkError};
    use crate::state::memory::MemoryStateStore;
    use crate::state::{
        LeaderRecord, ParkStatus, ParkedEvent, ParkedHead, StoreError, SwapOutcome, Version,
        Versioned,
    };
    use async_trait::async_trait;
    use pg_cdc_decoder::message::DecodeError;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicU32, Ordering};

    struct Scripted {
        outcomes: VecDeque<Result<(), SessionError>>,
        runs: usize,
        cancel_after_first: Option<CancellationToken>,
    }

    impl Scripted {
        fn new(outcomes: Vec<Result<(), SessionError>>) -> Self {
            Self {
                outcomes: outcomes.into(),
                runs: 0,
                cancel_after_first: None,
            }
        }
    }

    impl Attempt for Scripted {
        fn run(&mut self) -> impl Future<Output = Result<(), SessionError>> + Send {
            self.runs += 1;
            if let Some(token) = &self.cancel_after_first {
                token.cancel();
            }
            std::future::ready(self.outcomes.pop_front().unwrap_or(Ok(())))
        }
    }

    #[derive(Debug, Default)]
    struct FlakySink {
        failures_left: AtomicU32,
        calls: AtomicU32,
    }

    #[async_trait]
    impl Sink for FlakySink {
        fn capabilities(&self) -> SinkCapabilities {
            SinkCapabilities {
                max_batch_messages: 1,
                max_batch_bytes: 1,
                max_message_bytes: 1,
            }
        }

        async fn preflight(&self) -> Result<(), SinkError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let left = self.failures_left.load(Ordering::SeqCst);
            if left == 0 {
                return Ok(());
            }
            self.failures_left.store(left - 1, Ordering::SeqCst);
            Err(SinkError("topic not found".to_owned()))
        }

        async fn send(&self, batch: &[Outbound]) -> Vec<Outcome> {
            vec![Outcome::Delivered; batch.len()]
        }
    }

    #[tokio::test(start_paused = true)]
    async fn preflight_retries_with_backoff_until_the_sink_is_ready() {
        let sink = FlakySink {
            failures_left: AtomicU32::new(2),
            ..FlakySink::default()
        };
        assert!(preflight(&sink, &CancellationToken::new()).await);
        assert_eq!(sink.calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_stops_a_failing_preflight() {
        let sink = FlakySink {
            failures_left: AtomicU32::new(u32::MAX),
            ..FlakySink::default()
        };
        let shutdown = CancellationToken::new();
        let token = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(120)).await;
            token.cancel();
        });
        assert!(!preflight(&sink, &shutdown).await);
        assert!(sink.calls.load(Ordering::SeqCst) >= 3);
    }

    #[derive(Debug, Default)]
    struct FlakyStore {
        inner: MemoryStateStore,
        failures_left: AtomicU32,
        checks: AtomicU32,
    }

    #[async_trait]
    impl StateStore for FlakyStore {
        async fn append_parked(&self, events: &[ParkedEvent]) -> Result<(), StoreError> {
            self.inner.append_parked(events).await
        }

        async fn load_parked_heads(&self, partition: u8) -> Result<Vec<ParkedHead>, StoreError> {
            self.inner.load_parked_heads(partition).await
        }

        async fn read_parked(
            &self,
            key: &OrderKeyId,
            after: Option<&EventId>,
            limit: usize,
        ) -> Result<Vec<ParkedEvent>, StoreError> {
            self.inner.read_parked(key, after, limit).await
        }

        async fn set_parked_status(
            &self,
            key: &OrderKeyId,
            id: &EventId,
            status: ParkStatus,
        ) -> Result<(), StoreError> {
            self.inner.set_parked_status(key, id, status).await
        }

        async fn remove_parked(&self, events: &[(OrderKeyId, EventId)]) -> Result<(), StoreError> {
            self.inner.remove_parked(events).await
        }

        async fn read_leader(&self) -> Result<Option<Versioned<LeaderRecord>>, StoreError> {
            self.checks.fetch_add(1, Ordering::SeqCst);
            let left = self.failures_left.load(Ordering::SeqCst);
            if left > 0 {
                self.failures_left.store(left - 1, Ordering::SeqCst);
                return Err(StoreError::Unavailable("NoSuchBucket".to_owned()));
            }
            self.inner.read_leader().await
        }

        async fn swap_leader(
            &self,
            next: &LeaderRecord,
            expected: Option<&Version>,
        ) -> Result<SwapOutcome, StoreError> {
            self.inner.swap_leader(next, expected).await
        }
    }

    #[tokio::test(start_paused = true)]
    async fn the_store_check_retries_with_backoff_until_the_store_answers() {
        let store = FlakyStore {
            failures_left: AtomicU32::new(2),
            ..FlakyStore::default()
        };
        assert!(store_ready(&store, &CancellationToken::new()).await);
        assert_eq!(store.checks.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_stops_a_failing_store_check() {
        let store = FlakyStore {
            failures_left: AtomicU32::new(u32::MAX),
            ..FlakyStore::default()
        };
        let shutdown = CancellationToken::new();
        let token = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(120)).await;
            token.cancel();
        });
        assert!(!store_ready(&store, &shutdown).await);
        assert!(store.checks.load(Ordering::SeqCst) >= 3);
    }

    #[test]
    fn o2_a2_ordering_ack_and_lane_failures_are_fatal() {
        let out_of_order = SessionError::Lane(LaneError::OutOfOrder {
            lane: 0,
            previous: EventId::new(Lsn(2), 0),
            id: EventId::new(Lsn(1), 0),
        });
        for error in [
            out_of_order,
            SessionError::LaneStopped(LaneStopped(3)),
            SessionError::LanesStopped,
            SessionError::Ack(AckError::NotOpen),
        ] {
            assert!(error.is_fatal(), "{error}");
        }
        for error in [
            SessionError::StreamEnded,
            SessionError::SlotMissing("s".to_owned()),
        ] {
            assert!(!error.is_fatal(), "{error}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn o2_a_stopped_lane_stops_the_supervisor_instead_of_reconnecting() {
        for error in [
            SessionError::Lane(LaneError::OutOfOrder {
                lane: 1,
                previous: EventId::new(Lsn(9), 0),
                id: EventId::new(Lsn(8), 0),
            }),
            SessionError::LaneStopped(LaneStopped(1)),
            SessionError::LanesStopped,
        ] {
            let mut attempt = Scripted::new(vec![Err(error), Ok(())]);
            let result = supervise(&mut attempt, &CancellationToken::new()).await;
            assert!(
                matches!(result, Err(SupervisorError::Fatal(_))),
                "{result:?}"
            );
            assert_eq!(attempt.runs, 1);
        }
    }

    fn fault(lsn: u64) -> Result<(), SessionError> {
        Err(SessionError::Decode {
            wal_start: Lsn(lsn),
            source: DecodeError::Empty,
        })
    }

    #[tokio::test(start_paused = true)]
    async fn d3_three_consecutive_faults_at_same_lsn_halt() {
        let mut attempt = Scripted::new(vec![fault(0x10), fault(0x10), fault(0x10), Ok(())]);
        let result = supervise(&mut attempt, &CancellationToken::new()).await;
        assert!(
            matches!(
                result,
                Err(SupervisorError::Halted {
                    lsn: Lsn(0x10),
                    count: 3,
                    ..
                })
            ),
            "{result:?}"
        );
        assert_eq!(attempt.runs, 3);
    }

    #[tokio::test(start_paused = true)]
    async fn d3_three_malformed_streams_at_the_same_position_halt() {
        let malformed = || {
            Err(SessionError::Malformed {
                at: Lsn(0x30),
                source: pg_cdc_replication::ReplicationError::Protocol("bad frame".to_owned()),
            })
        };
        let mut attempt = Scripted::new(vec![malformed(), malformed(), malformed(), Ok(())]);
        let result = supervise(&mut attempt, &CancellationToken::new()).await;
        assert!(
            matches!(
                result,
                Err(SupervisorError::Halted {
                    lsn: Lsn(0x30),
                    count: 3,
                    last: SessionError::Malformed { .. },
                })
            ),
            "{result:?}"
        );
        assert_eq!(attempt.runs, 3);
    }

    #[tokio::test(start_paused = true)]
    async fn faults_at_different_lsns_do_not_halt() {
        let mut attempt = Scripted::new(vec![
            fault(0x10),
            fault(0x20),
            fault(0x10),
            fault(0x10),
            Ok(()),
        ]);
        assert!(
            supervise(&mut attempt, &CancellationToken::new())
                .await
                .is_ok()
        );
        assert_eq!(attempt.runs, 5);
    }

    #[tokio::test(start_paused = true)]
    async fn transport_errors_retry_until_the_session_ends_cleanly() {
        let mut outcomes: Vec<Result<(), SessionError>> =
            (0..6).map(|_| Err(SessionError::StreamEnded)).collect();
        outcomes.push(Ok(()));
        let mut attempt = Scripted::new(outcomes);
        assert!(
            supervise(&mut attempt, &CancellationToken::new())
                .await
                .is_ok()
        );
        assert_eq!(attempt.runs, 7);
    }

    #[tokio::test(start_paused = true)]
    async fn fatal_errors_stop_at_once() {
        let mut attempt = Scripted::new(vec![
            Err(SessionError::Ack(crate::ack::AckError::NotOpen)),
            Ok(()),
        ]);
        let result = supervise(&mut attempt, &CancellationToken::new()).await;
        assert!(
            matches!(result, Err(SupervisorError::Fatal(SessionError::Ack(_)))),
            "{result:?}"
        );
        assert_eq!(attempt.runs, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_during_backoff_returns_ok() {
        let shutdown = CancellationToken::new();
        let mut attempt = Scripted::new(vec![
            Err(SessionError::StreamEnded),
            Err(SessionError::StreamEnded),
        ]);
        attempt.cancel_after_first = Some(shutdown.clone());
        assert!(supervise(&mut attempt, &shutdown).await.is_ok());
        assert_eq!(attempt.runs, 1);
    }

    #[test]
    fn backoff_uses_full_jitter_capped_at_thirty_seconds() {
        let mut backoff = Backoff::default();
        let caps = [1, 2, 4, 8, 16, 30, 30, 30];
        for cap in caps {
            let delay = backoff.next_delay();
            assert!(delay <= Duration::from_secs(cap), "{delay:?} above {cap}s");
        }
        backoff.reset();
        for _ in 0..100 {
            let mut fresh = Backoff::default();
            assert!(fresh.next_delay() <= BACKOFF_BASE);
        }
        assert!(backoff.next_delay() <= BACKOFF_BASE);
    }

    #[test]
    fn backoff_doubles_from_its_base_up_to_its_cap() {
        let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_secs(30));
        let caps_ms = [
            100, 200, 400, 800, 1600, 3200, 6400, 12_800, 25_600, 30_000, 30_000,
        ];
        for cap in caps_ms {
            let delay = backoff.next_delay();
            assert!(
                delay <= Duration::from_millis(cap),
                "{delay:?} above {cap}ms"
            );
        }
        for _ in 0..40 {
            assert!(backoff.next_delay() <= Duration::from_secs(30));
        }
    }
}
