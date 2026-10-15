use std::fmt;
use std::future::Future;

use async_trait::async_trait;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::leader::{POLL_INTERVAL, unless_cancelled};

pub const OBJECT_IN_USE: &str = "55006";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotHolder {
    Missing,
    Free,
    Held(u32),
}

impl fmt::Display for SlotHolder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing => f.write_str("missing"),
            Self::Free => f.write_str("free"),
            Self::Held(pid) => write!(f, "held by {pid}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("cannot read the slot's status: {0}")]
pub struct ProbeError(pub String);

#[async_trait]
pub trait SlotProbe: Send + Sync + fmt::Debug {
    async fn holder(&self) -> Result<SlotHolder, ProbeError>;
}

#[derive(Debug)]
pub enum StartError<E> {
    InUse,
    Failed(E),
}

#[derive(Debug)]
pub enum AcquireError<E> {
    SlotMissing,
    Probe(ProbeError),
    Start(E),
}

async fn pause(shutdown: &CancellationToken) -> bool {
    shutdown
        .run_until_cancelled(sleep(POLL_INTERVAL))
        .await
        .is_some()
}

pub async fn acquire<T, E, F, Fut>(
    probe: &dyn SlotProbe,
    mut start: F,
    shutdown: &CancellationToken,
) -> Result<Option<T>, AcquireError<E>>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, StartError<E>>>,
{
    let mut announced = false;
    loop {
        let Some(answer) = unless_cancelled(shutdown, probe.holder()).await else {
            return Ok(None);
        };
        match answer.map_err(AcquireError::Probe)? {
            SlotHolder::Missing => return Err(AcquireError::SlotMissing),
            SlotHolder::Held(holder_pid) => {
                if announced {
                    tracing::debug!(holder_pid, "waiting as standby");
                } else {
                    announced = true;
                    tracing::info!(holder_pid, "waiting as standby");
                }
            }
            SlotHolder::Free => match unless_cancelled(shutdown, start()).await {
                None => return Ok(None),
                Some(Ok(value)) => return Ok(Some(value)),
                Some(Err(StartError::InUse)) => {
                    tracing::info!("another task took the slot first; waiting as standby");
                }
                Some(Err(StartError::Failed(error))) => return Err(AcquireError::Start(error)),
            },
        }
        if !pause(shutdown).await {
            return Ok(None);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::future::{Ready, ready};
    use std::sync::Mutex;

    use tokio::time::{Duration, Instant, sleep, timeout};

    use super::*;

    #[derive(Debug)]
    struct ScriptedProbe {
        origin: Instant,
        answers: Mutex<VecDeque<Result<SlotHolder, ProbeError>>>,
        calls: Mutex<Vec<Duration>>,
    }

    impl ScriptedProbe {
        fn new(origin: Instant, answers: Vec<Result<SlotHolder, ProbeError>>) -> Self {
            Self {
                origin,
                answers: Mutex::new(answers.into()),
                calls: Mutex::default(),
            }
        }

        fn calls(&self) -> Vec<Duration> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl SlotProbe for ScriptedProbe {
        async fn holder(&self) -> Result<SlotHolder, ProbeError> {
            self.calls.lock().unwrap().push(self.origin.elapsed());
            let mut answers = self.answers.lock().unwrap();
            if answers.len() > 1 {
                answers.pop_front().unwrap()
            } else {
                answers.front().unwrap().clone()
            }
        }
    }

    struct ScriptedStart {
        origin: Instant,
        results: Mutex<VecDeque<Result<&'static str, StartError<&'static str>>>>,
        calls: Mutex<Vec<Duration>>,
    }

    impl ScriptedStart {
        fn new(
            origin: Instant,
            results: Vec<Result<&'static str, StartError<&'static str>>>,
        ) -> Self {
            Self {
                origin,
                results: Mutex::new(results.into()),
                calls: Mutex::default(),
            }
        }

        fn call(&self) -> Ready<Result<&'static str, StartError<&'static str>>> {
            self.calls.lock().unwrap().push(self.origin.elapsed());
            ready(self.results.lock().unwrap().pop_front().unwrap())
        }

        fn calls(&self) -> Vec<Duration> {
            self.calls.lock().unwrap().clone()
        }
    }

    fn secs(values: &[u64]) -> Vec<Duration> {
        values.iter().map(|s| Duration::from_secs(*s)).collect()
    }

    #[test]
    fn slot_holders_display_their_state() {
        assert_eq!(SlotHolder::Missing.to_string(), "missing");
        assert_eq!(SlotHolder::Free.to_string(), "free");
        assert_eq!(SlotHolder::Held(42).to_string(), "held by 42");
    }

    #[test]
    fn the_object_in_use_state_is_the_postgres_code() {
        assert_eq!(OBJECT_IN_USE, "55006");
    }

    #[tokio::test(start_paused = true)]
    async fn a_free_slot_is_started_at_once() {
        let origin = Instant::now();
        let probe = ScriptedProbe::new(origin, vec![Ok(SlotHolder::Free)]);
        let start = ScriptedStart::new(origin, vec![Ok("stream")]);
        let shutdown = CancellationToken::new();

        let acquired = acquire(&probe, || start.call(), &shutdown).await;

        assert!(matches!(acquired, Ok(Some("stream"))));
        assert_eq!(probe.calls(), secs(&[0]));
        assert_eq!(start.calls(), secs(&[0]));
    }

    #[tokio::test(start_paused = true)]
    async fn a_held_slot_is_polled_every_two_seconds_until_it_frees() {
        let origin = Instant::now();
        let probe = ScriptedProbe::new(
            origin,
            vec![
                Ok(SlotHolder::Held(7)),
                Ok(SlotHolder::Held(7)),
                Ok(SlotHolder::Held(7)),
                Ok(SlotHolder::Free),
            ],
        );
        let start = ScriptedStart::new(origin, vec![Ok("stream")]);
        let shutdown = CancellationToken::new();

        let acquired = acquire(&probe, || start.call(), &shutdown).await;

        assert!(matches!(acquired, Ok(Some("stream"))));
        assert_eq!(probe.calls(), secs(&[0, 2, 4, 6]));
        assert_eq!(start.calls(), secs(&[6]));
    }

    #[tokio::test(start_paused = true)]
    async fn a_standby_that_loses_the_race_keeps_polling() {
        let origin = Instant::now();
        let probe = ScriptedProbe::new(origin, vec![Ok(SlotHolder::Free)]);
        let start = ScriptedStart::new(
            origin,
            vec![Err(StartError::InUse), Err(StartError::InUse), Ok("stream")],
        );
        let shutdown = CancellationToken::new();

        let acquired = acquire(&probe, || start.call(), &shutdown).await;

        assert!(matches!(acquired, Ok(Some("stream"))));
        assert_eq!(start.calls(), secs(&[0, 2, 4]));
        assert_eq!(probe.calls(), secs(&[0, 2, 4]));
    }

    #[tokio::test(start_paused = true)]
    async fn a_missing_slot_ends_standby() {
        let origin = Instant::now();
        let probe = ScriptedProbe::new(origin, vec![Ok(SlotHolder::Missing)]);
        let start = ScriptedStart::new(origin, vec![]);
        let shutdown = CancellationToken::new();

        let acquired = acquire(&probe, || start.call(), &shutdown).await;

        assert!(matches!(acquired, Err(AcquireError::SlotMissing)));
        assert_eq!(start.calls(), secs(&[]));
    }

    #[tokio::test(start_paused = true)]
    async fn a_probe_failure_ends_standby() {
        let origin = Instant::now();
        let probe = ScriptedProbe::new(origin, vec![Err(ProbeError("closed".to_owned()))]);
        let start = ScriptedStart::new(origin, vec![]);
        let shutdown = CancellationToken::new();

        let acquired = acquire(&probe, || start.call(), &shutdown).await;

        assert!(matches!(
            acquired,
            Err(AcquireError::Probe(ProbeError(ref reason))) if reason == "closed"
        ));
        assert_eq!(start.calls(), secs(&[]));
    }

    #[tokio::test(start_paused = true)]
    async fn other_start_failures_end_standby() {
        let origin = Instant::now();
        let probe = ScriptedProbe::new(origin, vec![Ok(SlotHolder::Free)]);
        let start = ScriptedStart::new(origin, vec![Err(StartError::Failed("boom"))]);
        let shutdown = CancellationToken::new();

        let acquired = acquire(&probe, || start.call(), &shutdown).await;

        assert!(matches!(acquired, Err(AcquireError::Start("boom"))));
        assert_eq!(probe.calls(), secs(&[0]));
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_while_waiting_returns_none_without_starting() {
        let origin = Instant::now();
        let probe = ScriptedProbe::new(origin, vec![Ok(SlotHolder::Held(7))]);
        let start = ScriptedStart::new(origin, vec![]);
        let shutdown = CancellationToken::new();
        let canceller = {
            let shutdown = shutdown.clone();
            async move {
                sleep(Duration::from_secs(5)).await;
                shutdown.cancel();
            }
        };

        let (acquired, ()) = tokio::join!(acquire(&probe, || start.call(), &shutdown), canceller);

        assert!(matches!(acquired, Ok(None)));
        assert_eq!(origin.elapsed(), Duration::from_secs(5));
        assert_eq!(start.calls(), secs(&[]));
        assert_eq!(probe.calls(), secs(&[0, 2, 4]));
    }

    #[tokio::test(start_paused = true)]
    async fn a_cancelled_shutdown_never_probes() {
        let origin = Instant::now();
        let probe = ScriptedProbe::new(origin, vec![Ok(SlotHolder::Free)]);
        let start = ScriptedStart::new(origin, vec![]);
        let shutdown = CancellationToken::new();
        shutdown.cancel();

        let acquired = acquire(&probe, || start.call(), &shutdown).await;

        assert!(matches!(acquired, Ok(None)));
        assert_eq!(probe.calls(), secs(&[]));
        assert_eq!(start.calls(), secs(&[]));
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_while_losing_the_race_returns_none() {
        let origin = Instant::now();
        let probe = ScriptedProbe::new(origin, vec![Ok(SlotHolder::Free)]);
        let start = ScriptedStart::new(origin, vec![Err(StartError::InUse)]);
        let shutdown = CancellationToken::new();
        let canceller = {
            let shutdown = shutdown.clone();
            async move {
                sleep(Duration::from_secs(1)).await;
                shutdown.cancel();
            }
        };

        let (acquired, ()) = tokio::join!(acquire(&probe, || start.call(), &shutdown), canceller);

        assert!(matches!(acquired, Ok(None)));
        assert_eq!(origin.elapsed(), Duration::from_secs(1));
        assert_eq!(start.calls(), secs(&[0]));
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_while_a_start_is_pending_returns_none() {
        let origin = Instant::now();
        let probe = ScriptedProbe::new(origin, vec![Ok(SlotHolder::Free)]);
        let starts = Mutex::new(Vec::new());
        let start = || {
            starts.lock().unwrap().push(origin.elapsed());
            std::future::pending::<Result<&'static str, StartError<&'static str>>>()
        };
        let shutdown = CancellationToken::new();
        let canceller = {
            let shutdown = shutdown.clone();
            async move {
                sleep(Duration::from_secs(3)).await;
                shutdown.cancel();
            }
        };

        let (acquired, ()) = timeout(Duration::from_secs(60), async {
            tokio::join!(acquire(&probe, start, &shutdown), canceller)
        })
        .await
        .unwrap();

        assert!(matches!(acquired, Ok(None)));
        assert_eq!(origin.elapsed(), Duration::from_secs(3));
        assert_eq!(*starts.lock().unwrap(), secs(&[0]));
        assert_eq!(probe.calls(), secs(&[0]));
    }
}
