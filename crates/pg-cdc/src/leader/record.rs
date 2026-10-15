use std::fmt;
use std::time::Duration;

use pg_cdc_decoder::Lsn;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::leader::{POLL_INTERVAL, unless_cancelled};
use crate::state::{
    LeaderRecord, LeaderState, StateStore, StoreError, SwapOutcome, Version, Versioned, now_ms,
};
use crate::supervisor::Backoff;

pub const LEADER_RETRY_BASE: Duration = Duration::from_millis(100);
pub const LEADER_RETRY_CAP: Duration = POLL_INTERVAL;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TakeoverKind {
    First,
    Clean,
    Unclean,
}

impl fmt::Display for TakeoverKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::First => "first",
            Self::Clean => "clean",
            Self::Unclean => "unclean",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claim {
    pub record: LeaderRecord,
    pub version: Version,
    pub kind: TakeoverKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TakeoverError {
    #[error("another writer changed the leader record")]
    Conflict,
    #[error("the leader epoch cannot grow past u64::MAX")]
    EpochExhausted,
    #[error("takeover cancelled")]
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Release {
    Released,
    Conflict,
}

pub fn holder_id() -> String {
    let host = std::env::var("HOSTNAME")
        .ok()
        .filter(|host| !host.is_empty())
        .unwrap_or_else(|| "pg-cdc".to_owned());
    format!("{host}-{}-{:08x}", std::process::id(), fastrand::u32(..))
}

fn retry_delay(backoff: &mut Backoff, action: &'static str, error: &StoreError) -> Duration {
    let delay = backoff.next_delay();
    let delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX);
    tracing::warn!(%error, action, delay_ms, "leader record call failed; retrying");
    delay
}

async fn retry_unless_cancelled<T, Fut>(
    cancel: &CancellationToken,
    action: &'static str,
    mut call: impl FnMut() -> Fut,
) -> Result<T, TakeoverError>
where
    Fut: Future<Output = Result<T, StoreError>>,
{
    let mut backoff = Backoff::new(LEADER_RETRY_BASE, LEADER_RETRY_CAP);
    loop {
        let error = match unless_cancelled(cancel, call()).await {
            None => return Err(TakeoverError::Cancelled),
            Some(Ok(value)) => return Ok(value),
            Some(Err(error)) => error,
        };
        let delay = retry_delay(&mut backoff, action, &error);
        if unless_cancelled(cancel, sleep(delay)).await.is_none() {
            return Err(TakeoverError::Cancelled);
        }
    }
}

async fn wait_out_previous_holder(
    previous: &LeaderRecord,
    fence_delay: Duration,
    cancel: &CancellationToken,
) -> Result<(), TakeoverError> {
    let wait_ms = u64::try_from(fence_delay.as_millis()).unwrap_or(u64::MAX);
    tracing::warn!(
        previous_holder = %previous.holder,
        previous_epoch = previous.epoch,
        wait_ms,
        "the previous holder did not release the slot cleanly; waiting fence_delay before writing"
    );
    unless_cancelled(cancel, sleep(fence_delay))
        .await
        .ok_or(TakeoverError::Cancelled)
}

pub async fn take_over(
    store: &dyn StateStore,
    holder: &str,
    confirmed: Lsn,
    fence_delay: Duration,
    cancel: &CancellationToken,
) -> Result<Claim, TakeoverError> {
    let current = retry_unless_cancelled(cancel, "read", || store.read_leader()).await?;
    let (kind, epoch, expected) = match current {
        None => (TakeoverKind::First, 1, None),
        Some(Versioned { value, version }) => {
            let epoch = value
                .epoch
                .checked_add(1)
                .ok_or(TakeoverError::EpochExhausted)?;
            let kind = match value.state {
                LeaderState::Released => TakeoverKind::Clean,
                LeaderState::Active => {
                    wait_out_previous_holder(&value, fence_delay, cancel).await?;
                    TakeoverKind::Unclean
                }
            };
            (kind, epoch, Some(version))
        }
    };
    let record = LeaderRecord {
        epoch,
        holder: holder.to_owned(),
        state: LeaderState::Active,
        confirmed_lsn: confirmed,
        updated_ms: now_ms(),
    };
    let outcome = retry_unless_cancelled(cancel, "claim", || {
        store.swap_leader(&record, expected.as_ref())
    })
    .await?;
    match outcome {
        SwapOutcome::Swapped(version) => Ok(Claim {
            record,
            version,
            kind,
        }),
        SwapOutcome::Conflict => Err(TakeoverError::Conflict),
    }
}

pub async fn release(store: &dyn StateStore, claim: &Claim, confirmed: Lsn) -> Release {
    let record = LeaderRecord {
        state: LeaderState::Released,
        confirmed_lsn: confirmed,
        updated_ms: now_ms(),
        ..claim.record.clone()
    };
    let mut backoff = Backoff::new(LEADER_RETRY_BASE, LEADER_RETRY_CAP);
    loop {
        match store.swap_leader(&record, Some(&claim.version)).await {
            Ok(SwapOutcome::Swapped(_)) => return Release::Released,
            Ok(SwapOutcome::Conflict) => return Release::Conflict,
            Err(error) => sleep(retry_delay(&mut backoff, "release", &error)).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use proptest::prelude::*;
    use tokio::task::{JoinHandle, JoinSet, yield_now};
    use tokio::time::{Instant, advance, sleep, timeout};

    use super::*;
    use crate::event::EventId;
    use crate::order::OrderKeyId;
    use crate::state::memory::MemoryStateStore;
    use crate::state::{
        LeaderState, ParkStatus, ParkedEvent, ParkedHead, ParkedQueues, StoreError, SwapOutcome,
        Versioned,
    };

    const FENCE: Duration = Duration::from_secs(10);

    fn record(epoch: u64, holder: &str, state: LeaderState) -> LeaderRecord {
        LeaderRecord {
            epoch,
            holder: holder.to_owned(),
            state,
            confirmed_lsn: Lsn(0x10),
            updated_ms: 1,
        }
    }

    #[derive(Debug)]
    struct FlakyLeader {
        inner: MemoryStateStore,
        read_failures: AtomicU32,
        swap_failures: AtomicU32,
        hang_reads: AtomicBool,
        hang_swaps: AtomicBool,
        attempts: Mutex<Vec<LeaderRecord>>,
        landed: Mutex<Vec<LeaderRecord>>,
    }

    impl FlakyLeader {
        fn new(read_failures: u32, swap_failures: u32) -> Arc<Self> {
            Arc::new(Self {
                inner: MemoryStateStore::new(),
                read_failures: AtomicU32::new(read_failures),
                swap_failures: AtomicU32::new(swap_failures),
                hang_reads: AtomicBool::new(false),
                hang_swaps: AtomicBool::new(false),
                attempts: Mutex::default(),
                landed: Mutex::default(),
            })
        }

        fn attempts(&self) -> Vec<LeaderRecord> {
            self.attempts.lock().unwrap().clone()
        }

        fn landed(&self) -> Vec<LeaderRecord> {
            self.landed.lock().unwrap().clone()
        }

        async fn stored(&self) -> Option<LeaderRecord> {
            self.inner.read_leader().await.unwrap().map(|v| v.value)
        }

        async fn seed(&self, next: LeaderRecord) {
            let current = self.inner.read_leader().await.unwrap();
            let expected = current.as_ref().map(|v| &v.version);
            let outcome = self.inner.swap_leader(&next, expected).await.unwrap();
            assert!(matches!(outcome, SwapOutcome::Swapped(_)));
        }

        fn consume(counter: &AtomicU32) -> bool {
            counter
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                    left.checked_sub(1)
                })
                .is_ok()
        }
    }

    #[async_trait]
    impl StateStore for FlakyLeader {
        async fn append_parked(&self, events: &[ParkedEvent]) -> Result<(), StoreError> {
            self.inner.append_parked(events).await
        }

        async fn load_parked_heads(&self, partition: u8) -> Result<Vec<ParkedHead>, StoreError> {
            self.inner.load_parked_heads(partition).await
        }

        async fn list_parked_queues(
            &self,
            after: Option<&OrderKeyId>,
            limit: usize,
            length_cap: usize,
        ) -> Result<ParkedQueues, StoreError> {
            self.inner
                .list_parked_queues(after, limit, length_cap)
                .await
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
            if self.hang_reads.load(Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            if Self::consume(&self.read_failures) {
                return Err(StoreError::Unavailable("read failed".to_owned()));
            }
            self.inner.read_leader().await
        }

        async fn swap_leader(
            &self,
            next: &LeaderRecord,
            expected: Option<&Version>,
        ) -> Result<SwapOutcome, StoreError> {
            self.attempts.lock().unwrap().push(next.clone());
            if self.hang_swaps.load(Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            if Self::consume(&self.swap_failures) {
                std::thread::sleep(Duration::from_millis(2));
                return Err(StoreError::Unavailable("swap failed".to_owned()));
            }
            let outcome = self.inner.swap_leader(next, expected).await?;
            if matches!(outcome, SwapOutcome::Swapped(_)) {
                self.landed.lock().unwrap().push(next.clone());
            }
            Ok(outcome)
        }
    }

    fn spawn_take_over(
        store: &Arc<FlakyLeader>,
        fence: Duration,
        cancel: &CancellationToken,
    ) -> JoinHandle<Result<Claim, TakeoverError>> {
        let store = Arc::clone(store);
        let cancel = cancel.clone();
        tokio::spawn(async move { take_over(&*store, "me", Lsn(0x20), fence, &cancel).await })
    }

    async fn settle() {
        for _ in 0..5 {
            yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn an_absent_record_is_claimed_at_once_with_epoch_one() {
        let store = FlakyLeader::new(0, 0);
        let started = Instant::now();
        let claim = take_over(&*store, "me", Lsn(0x20), FENCE, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(started.elapsed(), Duration::ZERO);
        assert_eq!(claim.kind, TakeoverKind::First);
        assert_eq!(claim.record.epoch, 1);
        let stored = store.stored().await.unwrap();
        assert_eq!(stored, claim.record);
        assert_eq!(stored.holder, "me");
        assert_eq!(stored.state, LeaderState::Active);
        assert_eq!(stored.confirmed_lsn, Lsn(0x20));
    }

    #[tokio::test(start_paused = true)]
    async fn a_released_record_is_claimed_at_once_with_the_next_epoch() {
        let store = FlakyLeader::new(0, 0);
        store.seed(record(4, "old", LeaderState::Released)).await;
        let started = Instant::now();
        let claim = take_over(&*store, "me", Lsn(0x20), FENCE, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(started.elapsed(), Duration::ZERO);
        assert_eq!(claim.kind, TakeoverKind::Clean);
        assert_eq!(claim.record.epoch, 5);
        assert_eq!(store.stored().await.unwrap(), claim.record);
    }

    #[tokio::test(start_paused = true)]
    async fn l1_an_active_record_is_claimed_only_after_fence_delay() {
        let store = FlakyLeader::new(0, 0);
        store.seed(record(4, "old", LeaderState::Active)).await;
        let handle = spawn_take_over(&store, FENCE, &CancellationToken::new());
        settle().await;
        advance(Duration::from_millis(9_999)).await;
        settle().await;
        assert!(!handle.is_finished());
        let stored = store.stored().await.unwrap();
        assert_eq!((stored.epoch, stored.holder.as_str()), (4, "old"));
        advance(Duration::from_millis(1)).await;
        let claim = handle.await.unwrap().unwrap();
        assert_eq!(claim.kind, TakeoverKind::Unclean);
        assert_eq!(claim.record.epoch, 5);
        assert_eq!(claim.record.holder, "me");
        assert_eq!(store.stored().await.unwrap(), claim.record);
    }

    #[tokio::test(start_paused = true)]
    async fn an_active_record_held_by_this_holder_still_waits() {
        let store = FlakyLeader::new(0, 0);
        store.seed(record(2, "me", LeaderState::Active)).await;
        let handle = spawn_take_over(&store, FENCE, &CancellationToken::new());
        settle().await;
        advance(Duration::from_millis(9_999)).await;
        settle().await;
        assert!(!handle.is_finished());
        advance(Duration::from_millis(1)).await;
        assert_eq!(handle.await.unwrap().unwrap().kind, TakeoverKind::Unclean);
    }

    #[tokio::test(start_paused = true)]
    async fn a_record_changed_during_the_wait_is_a_conflict() {
        let store = FlakyLeader::new(0, 0);
        store.seed(record(4, "old", LeaderState::Active)).await;
        let handle = spawn_take_over(&store, FENCE, &CancellationToken::new());
        settle().await;
        advance(Duration::from_secs(5)).await;
        settle().await;
        let intruder = record(9, "intruder", LeaderState::Active);
        store.seed(intruder.clone()).await;
        advance(Duration::from_secs(5)).await;
        assert_eq!(handle.await.unwrap(), Err(TakeoverError::Conflict));
        assert_eq!(store.stored().await.unwrap(), intruder);
    }

    #[tokio::test(start_paused = true)]
    async fn store_errors_during_takeover_are_retried_with_the_same_record() {
        let store = FlakyLeader::new(2, 1);
        let claim = take_over(&*store, "me", Lsn(0x20), FENCE, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(claim.record.epoch, 1);
        let attempts = store.attempts();
        assert_eq!(attempts.len(), 2);
        assert_eq!(attempts[0], attempts[1]);
        assert_eq!(store.landed(), vec![claim.record]);
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_during_the_fence_delay_cancels_without_writing() {
        let store = FlakyLeader::new(0, 0);
        let seeded = record(4, "old", LeaderState::Active);
        store.seed(seeded.clone()).await;
        let cancel = CancellationToken::new();
        let handle = spawn_take_over(&store, FENCE, &cancel);
        settle().await;
        advance(Duration::from_secs(3)).await;
        settle().await;
        assert!(!handle.is_finished());
        cancel.cancel();
        assert_eq!(handle.await.unwrap(), Err(TakeoverError::Cancelled));
        assert_eq!(store.stored().await.unwrap(), seeded);
        assert_eq!(store.attempts(), Vec::new());
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_during_a_store_retry_cancels() {
        let store = FlakyLeader::new(u32::MAX, 0);
        let cancel = CancellationToken::new();
        let handle = spawn_take_over(&store, FENCE, &cancel);
        settle().await;
        advance(Duration::from_secs(3)).await;
        settle().await;
        assert!(!handle.is_finished());
        cancel.cancel();
        assert_eq!(handle.await.unwrap(), Err(TakeoverError::Cancelled));
        assert_eq!(store.attempts(), Vec::new());
    }

    #[tokio::test(start_paused = true)]
    async fn a_cancelled_takeover_reads_and_claims_nothing() {
        let store = FlakyLeader::new(0, 0);
        store.hang_reads.store(true, Ordering::SeqCst);
        let cancel = CancellationToken::new();
        cancel.cancel();
        let result = timeout(
            Duration::from_secs(60),
            take_over(&*store, "me", Lsn(0x20), FENCE, &cancel),
        )
        .await
        .unwrap();
        assert_eq!(result, Err(TakeoverError::Cancelled));
        assert_eq!(store.attempts(), Vec::new());
        assert_eq!(store.stored().await, None);
    }

    #[tokio::test(start_paused = true)]
    async fn a_cancelled_takeover_of_an_absent_record_claims_nothing() {
        let store = FlakyLeader::new(0, 0);
        let cancel = CancellationToken::new();
        cancel.cancel();
        let result = take_over(&*store, "me", Lsn(0x20), FENCE, &cancel).await;
        assert_eq!(result, Err(TakeoverError::Cancelled));
        assert_eq!(store.attempts(), Vec::new());
        assert_eq!(store.stored().await, None);
    }

    async fn assert_cancel_during_a_hang(store: &Arc<FlakyLeader>) {
        let cancel = CancellationToken::new();
        let handle = spawn_take_over(store, FENCE, &cancel);
        settle().await;
        advance(Duration::from_secs(3)).await;
        settle().await;
        assert!(!handle.is_finished());
        cancel.cancel();
        let result = timeout(Duration::from_secs(60), handle).await.unwrap();
        assert_eq!(result.unwrap(), Err(TakeoverError::Cancelled));
        assert_eq!(store.landed(), Vec::new());
        assert_eq!(store.stored().await, None);
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_during_a_hanging_read_cancels_without_claiming() {
        let store = FlakyLeader::new(0, 0);
        store.hang_reads.store(true, Ordering::SeqCst);
        assert_cancel_during_a_hang(&store).await;
        assert_eq!(store.attempts(), Vec::new());
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_during_a_hanging_claim_cancels_without_another_attempt() {
        let store = FlakyLeader::new(0, 0);
        store.hang_swaps.store(true, Ordering::SeqCst);
        assert_cancel_during_a_hang(&store).await;
        assert_eq!(store.attempts().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn an_exhausted_epoch_is_an_error() {
        let store = FlakyLeader::new(0, 0);
        let seeded = record(u64::MAX, "old", LeaderState::Released);
        store.seed(seeded.clone()).await;
        let result = take_over(&*store, "me", Lsn(0x20), FENCE, &CancellationToken::new()).await;
        assert_eq!(result, Err(TakeoverError::EpochExhausted));
        assert_eq!(store.stored().await.unwrap(), seeded);
        assert_eq!(store.attempts(), Vec::new());
    }

    #[tokio::test(start_paused = true)]
    async fn an_exhausted_epoch_on_an_active_record_fails_before_the_wait() {
        let store = FlakyLeader::new(0, 0);
        store
            .seed(record(u64::MAX, "old", LeaderState::Active))
            .await;
        let started = Instant::now();
        let result = take_over(&*store, "me", Lsn(0x20), FENCE, &CancellationToken::new()).await;
        assert_eq!(result, Err(TakeoverError::EpochExhausted));
        assert_eq!(started.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn release_keeps_the_epoch_and_records_the_confirmed_lsn() {
        let store = FlakyLeader::new(0, 0);
        let claim = take_over(&*store, "me", Lsn(0x20), FENCE, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(release(&*store, &claim, Lsn(0x42)).await, Release::Released);
        let stored = store.stored().await.unwrap();
        assert_eq!(stored.epoch, claim.record.epoch);
        assert_eq!(stored.holder, "me");
        assert_eq!(stored.state, LeaderState::Released);
        assert_eq!(stored.confirmed_lsn, Lsn(0x42));
    }

    #[tokio::test(start_paused = true)]
    async fn release_retries_store_errors_with_the_same_record() {
        let store = FlakyLeader::new(0, 0);
        let claim = take_over(&*store, "me", Lsn(0x20), FENCE, &CancellationToken::new())
            .await
            .unwrap();
        store.swap_failures.store(2, Ordering::SeqCst);
        assert_eq!(release(&*store, &claim, Lsn(0x42)).await, Release::Released);
        let attempts = store.attempts();
        assert_eq!(attempts.len(), 4);
        assert_eq!(attempts[1], attempts[2]);
        assert_eq!(attempts[2], attempts[3]);
        assert_eq!(store.stored().await.unwrap(), attempts[3]);
    }

    #[tokio::test(start_paused = true)]
    async fn release_after_another_claim_is_a_conflict() {
        let store = FlakyLeader::new(0, 0);
        let claim = take_over(&*store, "me", Lsn(0x20), FENCE, &CancellationToken::new())
            .await
            .unwrap();
        let other = record(claim.record.epoch + 1, "other", LeaderState::Active);
        store.seed(other.clone()).await;
        assert_eq!(release(&*store, &claim, Lsn(0x42)).await, Release::Conflict);
        assert_eq!(store.stored().await.unwrap(), other);
    }

    #[test]
    fn holder_ids_differ_and_name_the_process() {
        let first = holder_id();
        let second = holder_id();
        assert_ne!(first, second);
        let pid = format!("-{}-", std::process::id());
        for id in [first, second] {
            let (head, digits) = id.split_at(id.len() - 8);
            assert!(head.ends_with(&pid), "{id}");
            assert!(
                digits.chars().all(|c| matches!(c, '0'..='9' | 'a'..='f')),
                "{id}"
            );
        }
    }

    #[test]
    fn takeover_kinds_display_their_names() {
        assert_eq!(TakeoverKind::First.to_string(), "first");
        assert_eq!(TakeoverKind::Clean.to_string(), "clean");
        assert_eq!(TakeoverKind::Unclean.to_string(), "unclean");
    }

    type Round = (u64, u64, bool);

    async fn run_holder(
        store: Arc<FlakyLeader>,
        name: String,
        rounds: Vec<Round>,
        claims: Arc<Mutex<Vec<u64>>>,
    ) {
        let cancel = CancellationToken::new();
        for (index, (fence_ms, hold_ms, releases)) in rounds.into_iter().enumerate() {
            let confirmed = Lsn(u64::try_from(index).unwrap());
            let fence = Duration::from_millis(fence_ms);
            match take_over(&*store, &name, confirmed, fence, &cancel).await {
                Ok(claim) => {
                    claims.lock().unwrap().push(claim.record.epoch);
                    sleep(Duration::from_millis(hold_ms)).await;
                    if releases {
                        release(&*store, &claim, confirmed).await;
                    }
                }
                Err(TakeoverError::Conflict) => {}
                Err(other) => panic!("unexpected takeover error: {other}"),
            }
        }
    }

    fn simulate(holders: Vec<Vec<Round>>) -> (Vec<u64>, Vec<LeaderRecord>) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();
        runtime.block_on(async {
            let store = FlakyLeader::new(0, 0);
            let claims = Arc::new(Mutex::new(Vec::new()));
            let mut tasks = JoinSet::new();
            for (index, rounds) in holders.into_iter().enumerate() {
                tasks.spawn(run_holder(
                    Arc::clone(&store),
                    format!("holder-{index}"),
                    rounds,
                    Arc::clone(&claims),
                ));
            }
            while let Some(done) = tasks.join_next().await {
                done.unwrap();
            }
            let claimed = claims.lock().unwrap().clone();
            (claimed, store.landed())
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn l2_the_leader_epoch_strictly_increases(
            holders in prop::collection::vec(
                prop::collection::vec((0u64..=3_000, 0u64..=3_000, any::<bool>()), 1..=5),
                2..=4,
            ),
        ) {
            let (claimed, landed) = simulate(holders);
            for pair in claimed.windows(2) {
                prop_assert!(pair[0] < pair[1], "claims {claimed:?}");
            }
            for pair in landed.windows(2) {
                prop_assert!(pair[0].epoch <= pair[1].epoch, "stored {landed:?}");
                if pair[1].state == LeaderState::Released {
                    prop_assert_eq!(pair[0].epoch, pair[1].epoch);
                    prop_assert_eq!(&pair[0].holder, &pair[1].holder);
                    prop_assert_eq!(pair[0].state, LeaderState::Active);
                } else {
                    prop_assert!(pair[0].epoch < pair[1].epoch, "stored {landed:?}");
                }
            }
            if let Some(first) = landed.first() {
                prop_assert_eq!(first.state, LeaderState::Active);
            }
        }
    }
}
