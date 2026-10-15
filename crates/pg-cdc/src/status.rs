use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, SystemTime};

use pg_cdc_decoder::Lsn;
use pg_cdc_replication::StatusClock;

use crate::lane::Operator;
use crate::state::StateStore;

pub const STATS_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LaneCounts {
    pub keys: u64,
    pub dead: u64,
    pub oldest_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ParkedSnapshot {
    pub events: u64,
    pub keys: u64,
    pub dead: u64,
    pub oldest_ms: Option<u64>,
}

impl ParkedSnapshot {
    pub fn oldest_age(&self, now_ms: u64) -> Option<Duration> {
        self.oldest_ms
            .map(|oldest| Duration::from_millis(now_ms.saturating_sub(oldest)))
    }
}

#[derive(Debug, Default)]
struct LaneGauge {
    keys: AtomicU64,
    dead: AtomicU64,
    oldest_ms: AtomicU64,
}

#[derive(Debug)]
pub struct ParkedGauges {
    events: Arc<AtomicU64>,
    lanes: Vec<LaneGauge>,
}

impl ParkedGauges {
    pub fn new(lanes: usize, events: Arc<AtomicU64>) -> Arc<Self> {
        Arc::new(Self {
            events,
            lanes: (0..lanes).map(|_| LaneGauge::default()).collect(),
        })
    }

    pub fn publish(&self, lane: usize, counts: LaneCounts) {
        let Some(gauge) = self.lanes.get(lane) else {
            return;
        };
        gauge.keys.store(counts.keys, Ordering::Relaxed);
        gauge.dead.store(counts.dead, Ordering::Relaxed);
        gauge
            .oldest_ms
            .store(counts.oldest_ms.unwrap_or(0), Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> ParkedSnapshot {
        let mut snapshot = ParkedSnapshot {
            events: self.events.load(Ordering::Relaxed),
            ..ParkedSnapshot::default()
        };
        for gauge in &self.lanes {
            snapshot.keys = snapshot
                .keys
                .saturating_add(gauge.keys.load(Ordering::Relaxed));
            snapshot.dead = snapshot
                .dead
                .saturating_add(gauge.dead.load(Ordering::Relaxed));
            let oldest = gauge.oldest_ms.load(Ordering::Relaxed);
            if oldest != 0 {
                snapshot.oldest_ms =
                    Some(snapshot.oldest_ms.map_or(oldest, |known| known.min(oldest)));
            }
        }
        snapshot
    }
}

#[derive(Debug, Clone)]
pub struct ParkedAccess {
    pub operator: Operator,
    pub store: Arc<dyn StateStore>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StatusSnapshot {
    pub epoch: Option<u64>,
    pub confirmed_lsn: Option<Lsn>,
    pub inflight_events: u64,
    pub inflight_bytes: u64,
    pub parked: Option<ParkedSnapshot>,
    pub slot_retained_bytes: Option<u64>,
    pub tables_without_full_identity: u64,
    pub status_update_age: Option<Duration>,
}

#[derive(Debug, Default)]
struct Board {
    epoch: AtomicU64,
    confirmed: AtomicU64,
    confirmed_set: AtomicBool,
    inflight_events: AtomicU64,
    inflight_bytes: AtomicU64,
    parked: Mutex<Option<Arc<ParkedGauges>>>,
    slot_retained_bytes: Mutex<Option<u64>>,
    tables_without_full_identity: AtomicU64,
    status_clock: Mutex<Option<StatusClock>>,
    parked_access: Mutex<Option<ParkedAccess>>,
}

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[derive(Debug, Clone, Default)]
pub struct Status {
    board: Arc<Board>,
}

impl Status {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_epoch(&self, epoch: Option<u64>) {
        self.board
            .epoch
            .store(epoch.unwrap_or(0), Ordering::Relaxed);
    }

    pub fn set_confirmed(&self, lsn: Option<Lsn>) {
        self.board
            .confirmed
            .store(lsn.map_or(0, |lsn| lsn.0), Ordering::Relaxed);
        self.board
            .confirmed_set
            .store(lsn.is_some(), Ordering::Relaxed);
    }

    pub fn set_inflight(&self, events: u64, bytes: u64) {
        self.board.inflight_events.store(events, Ordering::Relaxed);
        self.board.inflight_bytes.store(bytes, Ordering::Relaxed);
    }

    pub fn set_parked(&self, gauges: Option<Arc<ParkedGauges>>) {
        *locked(&self.board.parked) = gauges;
    }

    pub fn set_slot_retained_bytes(&self, bytes: Option<u64>) {
        *locked(&self.board.slot_retained_bytes) = bytes;
    }

    pub fn set_tables_without_full_identity(&self, count: u64) {
        self.board
            .tables_without_full_identity
            .store(count, Ordering::Relaxed);
    }

    pub fn set_status_clock(&self, clock: Option<StatusClock>) {
        *locked(&self.board.status_clock) = clock;
    }

    pub fn set_parked_access(&self, access: Option<ParkedAccess>) {
        *locked(&self.board.parked_access) = access;
    }

    pub fn parked_access(&self) -> Option<ParkedAccess> {
        locked(&self.board.parked_access).clone()
    }

    pub fn snapshot(&self) -> StatusSnapshot {
        let board = &self.board;
        let clock = locked(&board.status_clock).clone();
        let status_update_age = clock
            .and_then(|clock| clock.last_sent())
            .map(|sent| SystemTime::now().duration_since(sent).unwrap_or_default());
        let epoch = board.epoch.load(Ordering::Relaxed);
        let parked = locked(&board.parked)
            .as_ref()
            .map(|gauges| gauges.snapshot());
        StatusSnapshot {
            epoch: (epoch != 0).then_some(epoch),
            confirmed_lsn: board
                .confirmed_set
                .load(Ordering::Relaxed)
                .then(|| Lsn(board.confirmed.load(Ordering::Relaxed))),
            inflight_events: board.inflight_events.load(Ordering::Relaxed),
            inflight_bytes: board.inflight_bytes.load(Ordering::Relaxed),
            parked,
            slot_retained_bytes: *locked(&board.slot_retained_bytes),
            tables_without_full_identity: board
                .tables_without_full_identity
                .load(Ordering::Relaxed),
            status_update_age,
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::{Semaphore, mpsc};

    use super::*;
    use crate::lane::{LaneSettings, Lanes, Stores};
    use crate::sink::Sink;
    use crate::sink::discard::DiscardSink;
    use crate::state::memory::MemoryStateStore;

    #[test]
    fn parked_snapshots_sum_lanes_and_take_the_oldest_known_failure() {
        let gauges = ParkedGauges::new(2, Arc::new(AtomicU64::new(5)));
        gauges.publish(
            0,
            LaneCounts {
                keys: 1,
                dead: 0,
                oldest_ms: Some(200),
            },
        );
        gauges.publish(
            1,
            LaneCounts {
                keys: 2,
                dead: 1,
                oldest_ms: None,
            },
        );
        let snapshot = gauges.snapshot();
        assert_eq!(
            snapshot,
            ParkedSnapshot {
                events: 5,
                keys: 3,
                dead: 1,
                oldest_ms: Some(200),
            }
        );
        assert_eq!(snapshot.oldest_age(1200), Some(Duration::from_secs(1)));
        assert_eq!(snapshot.oldest_age(100), Some(Duration::ZERO));
        gauges.publish(0, LaneCounts::default());
        assert_eq!(gauges.snapshot().oldest_ms, None);
        assert_eq!(gauges.snapshot().oldest_age(1200), None);
    }

    #[test]
    fn the_oldest_failure_is_the_minimum_over_lanes_that_have_one() {
        let gauges = ParkedGauges::new(3, Arc::new(AtomicU64::new(0)));
        for (lane, oldest_ms) in [(0, Some(900)), (1, None), (2, Some(300))] {
            gauges.publish(
                lane,
                LaneCounts {
                    keys: 1,
                    dead: 0,
                    oldest_ms,
                },
            );
        }
        assert_eq!(gauges.snapshot().oldest_ms, Some(300));
    }

    #[test]
    fn publishing_to_a_lane_that_does_not_exist_does_nothing() {
        let gauges = ParkedGauges::new(1, Arc::new(AtomicU64::new(0)));
        gauges.publish(
            1,
            LaneCounts {
                keys: 4,
                dead: 4,
                oldest_ms: Some(1),
            },
        );
        assert_eq!(gauges.snapshot(), ParkedSnapshot::default());
    }

    #[test]
    fn parked_events_follow_the_shared_count() {
        let events = Arc::new(AtomicU64::new(0));
        let gauges = ParkedGauges::new(1, Arc::clone(&events));
        events.store(7, Ordering::Relaxed);
        assert_eq!(gauges.snapshot().events, 7);
    }

    #[test]
    fn a_fresh_status_board_reports_nothing_and_setters_round_trip() {
        let status = Status::new();
        assert_eq!(status.snapshot(), StatusSnapshot::default());
        let clone = status.clone();

        status.set_epoch(Some(3));
        assert_eq!(clone.snapshot().epoch, Some(3));
        status.set_epoch(None);
        assert_eq!(clone.snapshot().epoch, None);

        status.set_confirmed(Some(Lsn(0x16)));
        assert_eq!(clone.snapshot().confirmed_lsn, Some(Lsn(0x16)));
        status.set_confirmed(Some(Lsn(0)));
        assert_eq!(clone.snapshot().confirmed_lsn, Some(Lsn(0)));
        status.set_confirmed(None);
        assert_eq!(clone.snapshot().confirmed_lsn, None);

        status.set_inflight(4, 512);
        let snapshot = clone.snapshot();
        assert_eq!(
            (snapshot.inflight_events, snapshot.inflight_bytes),
            (4, 512)
        );
        status.set_inflight(0, 0);
        let snapshot = clone.snapshot();
        assert_eq!((snapshot.inflight_events, snapshot.inflight_bytes), (0, 0));

        let gauges = ParkedGauges::new(1, Arc::new(AtomicU64::new(2)));
        gauges.publish(
            0,
            LaneCounts {
                keys: 1,
                dead: 1,
                oldest_ms: Some(10),
            },
        );
        status.set_parked(Some(Arc::clone(&gauges)));
        assert_eq!(
            clone.snapshot().parked,
            Some(ParkedSnapshot {
                events: 2,
                keys: 1,
                dead: 1,
                oldest_ms: Some(10),
            })
        );
        status.set_parked(None);
        assert_eq!(clone.snapshot().parked, None);

        status.set_slot_retained_bytes(Some(0));
        assert_eq!(clone.snapshot().slot_retained_bytes, Some(0));
        status.set_slot_retained_bytes(Some(9));
        assert_eq!(clone.snapshot().slot_retained_bytes, Some(9));
        status.set_slot_retained_bytes(None);
        assert_eq!(clone.snapshot().slot_retained_bytes, None);

        status.set_tables_without_full_identity(2);
        assert_eq!(clone.snapshot().tables_without_full_identity, 2);
        status.set_tables_without_full_identity(0);
        assert_eq!(clone.snapshot(), StatusSnapshot::default());
    }

    #[tokio::test]
    async fn status_update_age_comes_from_the_clock_and_access_round_trips() {
        let status = Status::new();
        assert_eq!(status.snapshot().status_update_age, None);

        let clock = StatusClock::default();
        status.set_status_clock(Some(clock.clone()));
        assert_eq!(status.snapshot().status_update_age, None);
        clock.record(SystemTime::now() - Duration::from_secs(3));
        let age = status.snapshot().status_update_age.unwrap();
        assert!(
            age >= Duration::from_secs(3) && age < Duration::from_secs(4),
            "{age:?}"
        );
        clock.record(SystemTime::now() + Duration::from_secs(60));
        assert_eq!(status.snapshot().status_update_age, Some(Duration::ZERO));
        status.set_status_clock(None);
        assert_eq!(status.snapshot().status_update_age, None);

        assert!(status.parked_access().is_none());
        let state: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let stores = Stores {
            state: Arc::clone(&state),
            payload: None,
        };
        let sink: Arc<dyn Sink> = Arc::new(DiscardSink::new());
        let (reports, _settled) = mpsc::unbounded_channel();
        let lanes = Lanes::spawn(
            LaneSettings {
                lanes: 2,
                park_after: Duration::from_secs(1),
                max_parked_events: 10,
                store_concurrency: 2,
            },
            &sink,
            &stores,
            &Arc::new(Semaphore::new(1)),
            &reports,
        );
        let clone = status.clone();
        status.set_parked_access(Some(ParkedAccess {
            operator: lanes.operator(),
            store: state,
        }));
        assert!(clone.parked_access().is_some());
        status.set_parked_access(None);
        assert!(clone.parked_access().is_none());
        lanes.stop(Duration::from_secs(1)).await;
    }
}
