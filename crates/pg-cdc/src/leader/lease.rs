use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::{Instant, MissedTickBehavior, interval_at, sleep_until};

use crate::health::Health;
use crate::leader::POLL_INTERVAL;
use crate::leader::gate::WriteGate;
use crate::leader::standby::{SlotHolder, SlotProbe};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FenceReason {
    Expired,
    Lost(SlotHolder),
}

impl fmt::Display for FenceReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Expired => f.write_str("no lease check succeeded for lease_timeout"),
            Self::Lost(holder) => write!(f, "the slot is {holder}"),
        }
    }
}

pub async fn hold_lease(
    probe: Arc<dyn SlotProbe>,
    own_pid: u32,
    lease_timeout: Duration,
    acquired: Instant,
    gate: Arc<WriteGate>,
    health: Health,
) -> FenceReason {
    let mut last_ok = acquired;
    let mut ticks = interval_at(acquired + POLL_INTERVAL, POLL_INTERVAL);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let reason = loop {
        let deadline = last_ok + lease_timeout;
        tokio::select! {
            biased;
            () = sleep_until(deadline) => break FenceReason::Expired,
            _ = ticks.tick() => {}
        }
        let started = Instant::now();
        let limit = (started + POLL_INTERVAL).min(deadline);
        let answer = tokio::select! {
            biased;
            () = sleep_until(limit) => None,
            answer = probe.holder() => Some(answer),
        };
        if Instant::now() >= deadline {
            break FenceReason::Expired;
        }
        match answer {
            Some(Ok(SlotHolder::Held(pid))) if pid == own_pid => {
                last_ok = last_ok.max(started);
                health.set_lease_valid(true);
                tracing::debug!(own_pid, "lease renewed");
            }
            Some(Ok(holder)) => break FenceReason::Lost(holder),
            Some(Err(error)) => tracing::warn!(%error, "lease check failed"),
            None => tracing::warn!("lease check timed out"),
        }
    };
    gate.fence();
    health.set_lease_valid(false);
    tracing::error!(
        %reason,
        own_pid,
        since_last_ok_ms = u64::try_from(last_ok.elapsed().as_millis()).unwrap_or(u64::MAX),
        "lost the slot; fenced"
    );
    reason
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use tokio::task::JoinHandle;
    use tokio::time::{advance, sleep, timeout};

    use super::*;
    use crate::leader::gate::{Fenced, GateState};
    use crate::leader::standby::ProbeError;

    const OWN: u32 = 41;

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    #[derive(Debug, Clone)]
    enum Step {
        Answer(Result<SlotHolder, ProbeError>),
        Slow(Duration, SlotHolder),
        Stalled(Duration, SlotHolder),
        Hang,
    }

    fn held(pid: u32) -> Step {
        Step::Answer(Ok(SlotHolder::Held(pid)))
    }

    fn failing() -> Step {
        Step::Answer(Err(ProbeError("closed".to_owned())))
    }

    #[derive(Debug)]
    struct ScriptedProbe {
        origin: Instant,
        rules: Vec<(Duration, Step)>,
        calls: Mutex<Vec<Duration>>,
    }

    impl ScriptedProbe {
        fn new(origin: Instant, rules: Vec<(Duration, Step)>) -> Arc<Self> {
            Arc::new(Self {
                origin,
                rules,
                calls: Mutex::default(),
            })
        }

        fn calls(&self) -> Vec<Duration> {
            self.calls.lock().unwrap().clone()
        }

        fn step(&self, elapsed: Duration) -> Step {
            self.rules
                .iter()
                .rev()
                .find(|(from, _)| *from <= elapsed)
                .map(|(_, step)| step.clone())
                .unwrap()
        }
    }

    #[async_trait]
    impl SlotProbe for ScriptedProbe {
        async fn holder(&self) -> Result<SlotHolder, ProbeError> {
            let elapsed = self.origin.elapsed();
            self.calls.lock().unwrap().push(elapsed);
            match self.step(elapsed) {
                Step::Answer(answer) => answer,
                Step::Slow(delay, holder) => {
                    sleep(delay).await;
                    Ok(holder)
                }
                Step::Stalled(stall, holder) => {
                    advance(stall).await;
                    Ok(holder)
                }
                Step::Hang => std::future::pending().await,
            }
        }
    }

    struct Lease {
        origin: Instant,
        probe: Arc<ScriptedProbe>,
        gate: Arc<WriteGate>,
        health: Health,
        task: JoinHandle<(FenceReason, Duration, GateState)>,
    }

    fn start(lease_timeout: Duration, rules: Vec<(Duration, Step)>) -> Lease {
        let origin = Instant::now();
        let probe = ScriptedProbe::new(origin, rules);
        let gate = WriteGate::new();
        gate.open();
        let health = Health::new();
        let task = {
            let probe = Arc::clone(&probe);
            let gate = Arc::clone(&gate);
            let health = health.clone();
            tokio::spawn(async move {
                let reason =
                    hold_lease(probe, OWN, lease_timeout, origin, Arc::clone(&gate), health).await;
                (reason, origin.elapsed(), gate.state())
            })
        };
        Lease {
            origin,
            probe,
            gate,
            health,
            task,
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Call {
        started: Duration,
        completed: Option<Duration>,
    }

    #[derive(Debug)]
    struct Recorder {
        origin: Instant,
        delay: Duration,
        log: Mutex<Vec<Call>>,
    }

    impl Recorder {
        fn new(origin: Instant, delay: Duration) -> Arc<Self> {
            Arc::new(Self {
                origin,
                delay,
                log: Mutex::default(),
            })
        }

        fn calls(&self) -> Vec<Call> {
            self.log.lock().unwrap().clone()
        }

        async fn append_parked(&self) {
            let index = {
                let mut log = self.log.lock().unwrap();
                log.push(Call {
                    started: self.origin.elapsed(),
                    completed: None,
                });
                log.len() - 1
            };
            sleep(self.delay).await;
            self.log.lock().unwrap()[index].completed = Some(self.origin.elapsed());
        }
    }

    #[test]
    fn fence_reasons_display_their_cause() {
        assert_eq!(
            FenceReason::Expired.to_string(),
            "no lease check succeeded for lease_timeout"
        );
        assert_eq!(
            FenceReason::Lost(SlotHolder::Held(9)).to_string(),
            "the slot is held by 9"
        );
        assert_eq!(
            FenceReason::Lost(SlotHolder::Free).to_string(),
            "the slot is free"
        );
        assert_eq!(
            FenceReason::Lost(SlotHolder::Missing).to_string(),
            "the slot is missing"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn checks_run_every_two_seconds_and_renew_the_lease() {
        let lease = start(secs(6), vec![(secs(0), held(OWN))]);

        sleep(secs(61)).await;

        assert!(!lease.task.is_finished());
        let expected: Vec<_> = (1..=30).map(|n| secs(2 * n)).collect();
        assert_eq!(lease.probe.calls(), expected);
        assert_eq!(lease.gate.state(), GateState::Open);
        assert!(lease.health.snapshot().lease_valid);
        lease.task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn l1_a_task_that_loses_the_slot_stops_writing_within_lease_timeout() {
        let lease = start(secs(6), vec![(secs(0), held(OWN)), (secs(10), failing())]);
        let recorder = Recorder::new(lease.origin, Duration::from_millis(500));
        let writer = {
            let gate = Arc::clone(&lease.gate);
            let recorder = Arc::clone(&recorder);
            let origin = lease.origin;
            tokio::spawn(async move {
                sleep(Duration::from_millis(200)).await;
                loop {
                    if gate.write(recorder.append_parked()).await.is_err() {
                        return origin.elapsed();
                    }
                }
            })
        };

        let (reason, returned_at, gate_state) = lease.task.await.unwrap();
        let writer_stopped_at = writer.await.unwrap();

        assert_eq!(reason, FenceReason::Expired);
        assert_eq!(returned_at, secs(14));
        assert_eq!(gate_state, GateState::Fenced);
        assert_eq!(writer_stopped_at, secs(14));
        assert!(!lease.health.snapshot().lease_valid);
        let calls = recorder.calls();
        assert!(calls.iter().all(|call| call.started <= secs(14)));
        assert!(
            calls
                .iter()
                .all(|call| call.completed.is_none_or(|at| at < secs(14)))
        );
        assert_eq!(calls.last().and_then(|call| call.completed), None);
        sleep(secs(10)).await;
        assert!(matches!(
            lease.gate.write(recorder.append_parked()).await,
            Err(Fenced)
        ));
        assert_eq!(recorder.calls(), calls);
    }

    #[tokio::test(start_paused = true)]
    async fn a_check_that_hangs_still_fences_at_the_deadline() {
        let lease = start(secs(6), vec![(secs(0), held(OWN)), (secs(4), Step::Hang)]);

        let (reason, returned_at, gate_state) = lease.task.await.unwrap();

        assert_eq!(reason, FenceReason::Expired);
        assert_eq!(returned_at, secs(8));
        assert_eq!(gate_state, GateState::Fenced);
        assert!(!lease.health.snapshot().lease_valid);
        assert_eq!(lease.probe.calls(), vec![secs(2), secs(4), secs(6)]);
    }

    async fn assert_fences_at_once(holder: SlotHolder) {
        let lease = start(
            secs(6),
            vec![(secs(0), held(OWN)), (secs(4), Step::Answer(Ok(holder)))],
        );

        let (reason, returned_at, gate_state) = lease.task.await.unwrap();

        assert_eq!(reason, FenceReason::Lost(holder));
        assert_eq!(returned_at, secs(4));
        assert_eq!(gate_state, GateState::Fenced);
        assert!(!lease.health.snapshot().lease_valid);
        assert_eq!(lease.probe.calls(), vec![secs(2), secs(4)]);
    }

    #[tokio::test(start_paused = true)]
    async fn another_holder_fences_at_once() {
        assert_fences_at_once(SlotHolder::Held(OWN + 1)).await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_free_slot_fences_at_once() {
        assert_fences_at_once(SlotHolder::Free).await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_missing_slot_fences_at_once() {
        assert_fences_at_once(SlotHolder::Missing).await;
    }

    #[tokio::test(start_paused = true)]
    async fn failed_checks_do_not_fence_before_the_timeout() {
        let lease = start(secs(8), vec![(secs(0), failing()), (secs(6), held(OWN))]);

        sleep(secs(30)).await;

        assert!(!lease.task.is_finished());
        assert_eq!(lease.gate.state(), GateState::Open);
        assert!(lease.health.snapshot().lease_valid);
        lease.task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn a_slow_answer_renews_the_lease_only_from_when_its_check_started() {
        let lease = start(
            secs(4),
            vec![
                (secs(0), Step::Answer(Err(ProbeError("early".to_owned())))),
                (
                    secs(2),
                    Step::Slow(Duration::from_millis(1900), SlotHolder::Held(OWN)),
                ),
                (secs(3), Step::Hang),
            ],
        );

        let (reason, returned_at, gate_state) =
            timeout(secs(60), lease.task).await.unwrap().unwrap();

        assert_eq!(reason, FenceReason::Expired);
        assert_eq!(returned_at, secs(6));
        assert_eq!(gate_state, GateState::Fenced);
        assert!(!lease.health.snapshot().lease_valid);
        assert_eq!(lease.probe.calls(), vec![secs(2), secs(4)]);
    }

    async fn assert_a_late_answer_does_not_renew(late: Step, fenced_at: Duration) {
        let lease = start(
            secs(4),
            vec![
                (secs(0), Step::Answer(Err(ProbeError("early".to_owned())))),
                (secs(2), late),
                (secs(3), Step::Hang),
            ],
        );

        let (reason, returned_at, gate_state) =
            timeout(secs(60), lease.task).await.unwrap().unwrap();

        assert_eq!(reason, FenceReason::Expired);
        assert_eq!(returned_at, fenced_at);
        assert_eq!(gate_state, GateState::Fenced);
        assert!(!lease.health.snapshot().lease_valid);
        assert_eq!(lease.probe.calls(), vec![secs(2)]);
    }

    #[tokio::test(start_paused = true)]
    async fn an_answer_ready_exactly_at_the_deadline_does_not_renew() {
        assert_a_late_answer_does_not_renew(Step::Slow(secs(2), SlotHolder::Held(OWN)), secs(4))
            .await;
    }

    #[tokio::test(start_paused = true)]
    async fn an_answer_observed_after_the_deadline_does_not_renew() {
        assert_a_late_answer_does_not_renew(
            Step::Stalled(Duration::from_millis(2_100), SlotHolder::Held(OWN)),
            Duration::from_millis(4_100),
        )
        .await;
    }
}
