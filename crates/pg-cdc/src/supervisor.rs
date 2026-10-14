use std::fmt;
use std::time::{Duration, Instant};

use pg_cdc_decoder::Lsn;
use tokio::io::AsyncWrite;
use tokio_util::sync::CancellationToken;

use crate::catalog::TypeResolver;
use crate::connection::ConnectionProvider;
use crate::session::{SessionConfig, SessionError, run_session};
use crate::sink::StdoutSink;

pub const HALT_AFTER_FAULTS: u32 = 3;
pub const BACKOFF_BASE: Duration = Duration::from_secs(1);
pub const BACKOFF_CAP: Duration = Duration::from_secs(30);

pub trait Attempt {
    fn run(&mut self) -> impl Future<Output = Result<(), SessionError>> + Send;
}

pub struct LiveSession<'a, W> {
    pub config: &'a SessionConfig,
    pub provider: &'a dyn ConnectionProvider,
    pub types: &'a mut TypeResolver,
    pub sink: &'a mut StdoutSink<W>,
    pub shutdown: &'a CancellationToken,
}

impl<W> fmt::Debug for LiveSession<'_, W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LiveSession")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl<W: AsyncWrite + Unpin + Send> Attempt for LiveSession<'_, W> {
    fn run(&mut self) -> impl Future<Output = Result<(), SessionError>> + Send {
        run_session(
            self.config,
            self.provider,
            self.types,
            self.sink,
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

#[derive(Debug, Default)]
pub struct Backoff {
    attempt: u32,
}

impl Backoff {
    pub fn next_delay(&mut self) -> Duration {
        let cap = BACKOFF_BASE
            .saturating_mul(1 << self.attempt.min(5))
            .min(BACKOFF_CAP);
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
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return Ok(()),
            () = tokio::time::sleep(delay) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg_cdc_decoder::message::DecodeError;
    use std::collections::VecDeque;

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
}
