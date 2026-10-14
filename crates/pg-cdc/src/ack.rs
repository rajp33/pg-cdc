use std::collections::VecDeque;

use pg_cdc_decoder::Lsn;

use crate::sink::{Delivered, Outcome};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AckError {
    #[error("a transaction is already open")]
    AlreadyOpen,
    #[error("no transaction is open")]
    NotOpen,
    #[error("a receipt settles {settled} events but only {unsettled} are unsettled")]
    OverSettled { settled: u64, unsettled: u64 },
    #[error("commit at {end_lsn} with {unsettled} unsettled events")]
    Unsettled { end_lsn: Lsn, unsettled: u64 },
    #[error("commit at {end_lsn} follows a commit at {previous}")]
    CommitOutOfOrder { previous: Lsn, end_lsn: Lsn },
    #[error("a settlement names transaction {txn}, which has no unsettled event")]
    UnknownSettlement { txn: u64 },
}

#[derive(Debug)]
pub struct Ticket {
    txn: u64,
    bytes: u64,
}

impl Ticket {
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    pub fn settle(self, outcome: &Outcome) -> Result<Settled, Self> {
        match outcome {
            Outcome::Delivered => Ok(Settled {
                txn: self.txn,
                bytes: self.bytes,
            }),
            Outcome::Retryable(_) | Outcome::Terminal(_) => Err(self),
        }
    }
}

#[derive(Debug)]
pub struct Settled {
    txn: u64,
    bytes: u64,
}

#[derive(Debug)]
struct Txn {
    seq: u64,
    end_lsn: Option<Lsn>,
    unsettled: u64,
}

#[derive(Debug)]
pub struct AckTracker {
    confirmed: Lsn,
    txns: VecDeque<Txn>,
    next_seq: u64,
    open: bool,
    last_commit: Option<Lsn>,
    inflight_events: u64,
    inflight_bytes: u64,
}

impl AckTracker {
    pub fn new(start: Lsn) -> Self {
        Self {
            confirmed: start,
            txns: VecDeque::new(),
            next_seq: 0,
            open: false,
            last_commit: None,
            inflight_events: 0,
            inflight_bytes: 0,
        }
    }

    pub fn confirmed(&self) -> Lsn {
        self.confirmed
    }

    pub fn inflight_events(&self) -> u64 {
        self.inflight_events
    }

    pub fn inflight_bytes(&self) -> u64 {
        self.inflight_bytes
    }

    pub fn is_idle(&self) -> bool {
        !self.open && self.txns.is_empty()
    }

    pub fn begin(&mut self) -> Result<(), AckError> {
        if self.open {
            return Err(AckError::AlreadyOpen);
        }
        self.txns.push_back(Txn {
            seq: self.next_seq,
            end_lsn: None,
            unsettled: 0,
        });
        self.next_seq += 1;
        self.open = true;
        Ok(())
    }

    pub fn track(&mut self, bytes: u64) -> Result<Ticket, AckError> {
        let txn = match self.txns.back_mut() {
            Some(txn) if self.open => txn,
            _ => return Err(AckError::NotOpen),
        };
        txn.unsettled += 1;
        self.inflight_events += 1;
        self.inflight_bytes = self.inflight_bytes.saturating_add(bytes);
        Ok(Ticket {
            txn: txn.seq,
            bytes,
        })
    }

    pub fn commit(&mut self, end_lsn: Lsn) -> Result<Lsn, AckError> {
        if let Some(previous) = self.last_commit
            && end_lsn < previous
        {
            return Err(AckError::CommitOutOfOrder { previous, end_lsn });
        }
        let txn = match self.txns.back_mut() {
            Some(txn) if self.open => txn,
            _ => return Err(AckError::NotOpen),
        };
        txn.end_lsn = Some(end_lsn);
        self.open = false;
        self.last_commit = Some(end_lsn);
        self.advance();
        Ok(self.confirmed)
    }

    #[expect(
        clippy::needless_pass_by_value,
        reason = "taking the Settled by value lets each one settle only once"
    )]
    pub fn settle(&mut self, settled: Settled) -> Result<Lsn, AckError> {
        let Settled { txn, bytes } = settled;
        let unknown = AckError::UnknownSettlement { txn };
        let front = self.txns.front().map_or(u64::MAX, |txn| txn.seq);
        let index = txn
            .checked_sub(front)
            .and_then(|index| usize::try_from(index).ok())
            .ok_or(unknown.clone())?;
        let entry = self.txns.get_mut(index).ok_or(unknown.clone())?;
        entry.unsettled = entry.unsettled.checked_sub(1).ok_or(unknown)?;
        self.inflight_events -= 1;
        self.inflight_bytes = self.inflight_bytes.saturating_sub(bytes);
        self.advance();
        Ok(self.confirmed)
    }

    pub fn keepalive(&mut self, wal_end: Lsn) -> Lsn {
        if self.is_idle() {
            self.confirmed = self.confirmed.max(wal_end);
        }
        self.confirmed
    }

    fn advance(&mut self) {
        while let Some(front) = self.txns.front() {
            let Some(end_lsn) = front.end_lsn else {
                break;
            };
            if front.unsettled > 0 {
                break;
            }
            self.confirmed = self.confirmed.max(end_lsn);
            self.txns.pop_front();
        }
    }
}

#[derive(Debug)]
pub struct AckProgress {
    confirmed: Lsn,
    open: Option<u64>,
}

impl AckProgress {
    pub fn new(start: Lsn) -> Self {
        Self {
            confirmed: start,
            open: None,
        }
    }

    pub fn confirmed(&self) -> Lsn {
        self.confirmed
    }

    pub fn is_idle(&self) -> bool {
        self.open.is_none()
    }

    pub fn begin(&mut self) -> Result<(), AckError> {
        if self.open.is_some() {
            return Err(AckError::AlreadyOpen);
        }
        self.open = Some(0);
        Ok(())
    }

    pub fn sent(&mut self) -> Result<(), AckError> {
        let unsettled = self.open.as_mut().ok_or(AckError::NotOpen)?;
        *unsettled += 1;
        Ok(())
    }

    pub fn settle(&mut self, receipt: Delivered) -> Result<(), AckError> {
        let unsettled = self.open.as_mut().ok_or(AckError::NotOpen)?;
        let settled = receipt.into_events();
        *unsettled = unsettled
            .checked_sub(settled)
            .ok_or(AckError::OverSettled {
                settled,
                unsettled: *unsettled,
            })?;
        Ok(())
    }

    pub fn commit(&mut self, end_lsn: Lsn) -> Result<Lsn, AckError> {
        let unsettled = self.open.ok_or(AckError::NotOpen)?;
        if unsettled != 0 {
            return Err(AckError::Unsettled { end_lsn, unsettled });
        }
        self.open = None;
        self.confirmed = self.confirmed.max(end_lsn);
        Ok(self.confirmed)
    }

    pub fn keepalive(&mut self, wal_end: Lsn) -> Lsn {
        if self.open.is_none() {
            self.confirmed = self.confirmed.max(wal_end);
        }
        self.confirmed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::{SinkError, StdoutSink};
    use proptest::prelude::*;
    use proptest::sample::Index;

    fn delivered(ticket: Ticket) -> Settled {
        ticket.settle(&Outcome::Delivered).unwrap()
    }

    #[test]
    fn a1_only_a_delivered_outcome_settles_a_ticket() {
        let mut ack = AckTracker::new(Lsn(100));
        ack.begin().unwrap();
        let ticket = ack.track(7).unwrap();
        let retry = Outcome::Retryable(SinkError("throttled".to_owned()));
        let ticket = ticket.settle(&retry).unwrap_err();
        let terminal = Outcome::Terminal(SinkError("rejected".to_owned()));
        let ticket = ticket.settle(&terminal).unwrap_err();
        assert_eq!(ticket.bytes(), 7);
        assert_eq!(ack.commit(Lsn(200)), Ok(Lsn(100)));
        assert_eq!(ack.settle(delivered(ticket)), Ok(Lsn(200)));
    }

    #[test]
    fn a_transaction_confirms_once_committed_and_every_event_settles() {
        let mut ack = AckTracker::new(Lsn(100));
        ack.begin().unwrap();
        let first = ack.track(10).unwrap();
        let second = ack.track(20).unwrap();
        assert_eq!(ack.settle(delivered(second)), Ok(Lsn(100)));
        assert_eq!(ack.commit(Lsn(200)), Ok(Lsn(100)));
        assert_eq!(ack.settle(delivered(first)), Ok(Lsn(200)));
        assert!(ack.is_idle());
    }

    #[test]
    fn an_empty_transaction_settles_at_its_commit() {
        let mut ack = AckTracker::new(Lsn(100));
        ack.begin().unwrap();
        assert!(!ack.is_idle());
        assert_eq!(ack.commit(Lsn(150)), Ok(Lsn(150)));
        assert!(ack.is_idle());
    }

    #[test]
    fn a2_a_later_transaction_never_confirms_past_an_earlier_unsettled_one() {
        let mut ack = AckTracker::new(Lsn(100));
        ack.begin().unwrap();
        let early = ack.track(1).unwrap();
        ack.commit(Lsn(200)).unwrap();
        ack.begin().unwrap();
        let late = ack.track(1).unwrap();
        ack.commit(Lsn(300)).unwrap();
        ack.begin().unwrap();
        assert_eq!(ack.commit(Lsn(400)), Ok(Lsn(100)));
        assert_eq!(ack.settle(delivered(late)), Ok(Lsn(100)));
        assert_eq!(ack.settle(delivered(early)), Ok(Lsn(400)));
    }

    #[test]
    fn a2_the_confirmed_lsn_starts_at_the_slot_position_and_never_decreases() {
        let mut ack = AckTracker::new(Lsn(500));
        assert_eq!(ack.confirmed(), Lsn(500));
        ack.begin().unwrap();
        assert_eq!(ack.commit(Lsn(400)), Ok(Lsn(500)));
        assert_eq!(ack.keepalive(Lsn(450)), Lsn(500));
    }

    #[test]
    fn a3_keepalives_advance_the_confirmed_lsn_only_when_idle() {
        let mut ack = AckTracker::new(Lsn(100));
        assert_eq!(ack.keepalive(Lsn(150)), Lsn(150));
        ack.begin().unwrap();
        assert_eq!(ack.keepalive(Lsn(900)), Lsn(150));
        let ticket = ack.track(1).unwrap();
        assert_eq!(ack.commit(Lsn(200)), Ok(Lsn(150)));
        assert_eq!(ack.keepalive(Lsn(900)), Lsn(150));
        assert_eq!(ack.settle(delivered(ticket)), Ok(Lsn(200)));
        assert_eq!(ack.keepalive(Lsn(900)), Lsn(900));
    }

    #[test]
    fn inflight_counts_follow_tickets_until_they_settle() {
        let mut ack = AckTracker::new(Lsn(0));
        ack.begin().unwrap();
        let small = ack.track(10).unwrap();
        let large = ack.track(1000).unwrap();
        assert_eq!((ack.inflight_events(), ack.inflight_bytes()), (2, 1010));
        ack.settle(delivered(large)).unwrap();
        assert_eq!((ack.inflight_events(), ack.inflight_bytes()), (1, 10));
        ack.settle(delivered(small)).unwrap();
        assert_eq!((ack.inflight_events(), ack.inflight_bytes()), (0, 0));
    }

    #[test]
    fn a_settlement_for_an_unknown_or_settled_transaction_is_an_error() {
        let mut source = AckTracker::new(Lsn(0));
        source.begin().unwrap();
        let first = source.track(1).unwrap();
        let second = source.track(1).unwrap();
        let mut other = AckTracker::new(Lsn(0));
        assert_eq!(
            other.settle(delivered(first)),
            Err(AckError::UnknownSettlement { txn: 0 })
        );
        other.begin().unwrap();
        other.track(1).unwrap();
        let third = source.track(1).unwrap();
        other.settle(delivered(second)).unwrap();
        assert_eq!(
            other.settle(delivered(third)),
            Err(AckError::UnknownSettlement { txn: 0 })
        );
    }

    #[test]
    fn transaction_boundaries_and_commit_order_are_checked() {
        let mut ack = AckTracker::new(Lsn(0));
        assert!(matches!(ack.track(1), Err(AckError::NotOpen)));
        assert_eq!(ack.commit(Lsn(1)), Err(AckError::NotOpen));
        ack.begin().unwrap();
        assert_eq!(ack.begin(), Err(AckError::AlreadyOpen));
        ack.commit(Lsn(300)).unwrap();
        assert!(matches!(ack.track(1), Err(AckError::NotOpen)));
        ack.begin().unwrap();
        assert_eq!(
            ack.commit(Lsn(200)),
            Err(AckError::CommitOutOfOrder {
                previous: Lsn(300),
                end_lsn: Lsn(200)
            })
        );
    }

    struct Model {
        ends: Vec<u64>,
        unsettled: Vec<usize>,
        open: bool,
        floor: u64,
        previous: Lsn,
    }

    impl Model {
        fn idle(&self) -> bool {
            !self.open && self.unsettled.iter().all(|count| *count == 0)
        }

        fn expected(&self) -> u64 {
            let mut expected = self.floor;
            for (end, unsettled) in self.ends.iter().zip(&self.unsettled) {
                if *unsettled > 0 {
                    break;
                }
                expected = expected.max(*end);
            }
            expected
        }

        fn check(&mut self, ack: &AckTracker) -> Result<(), TestCaseError> {
            let confirmed = ack.confirmed();
            prop_assert!(confirmed >= self.previous, "confirmed went backwards");
            for (end, unsettled) in self.ends.iter().zip(&self.unsettled) {
                if *unsettled > 0 {
                    prop_assert!(
                        confirmed.0 < *end,
                        "confirmed {confirmed} passed unsettled end {end}"
                    );
                }
            }
            prop_assert_eq!(confirmed.0, self.expected());
            prop_assert_eq!(ack.is_idle(), self.idle());
            self.previous = confirmed;
            Ok(())
        }

        fn keepalive(&mut self, ack: &mut AckTracker, wal_end: u64) -> Result<(), TestCaseError> {
            let before = ack.confirmed();
            let after = ack.keepalive(Lsn(wal_end));
            if self.idle() {
                self.floor = self.floor.max(wal_end);
                prop_assert_eq!(after, before.max(Lsn(wal_end)));
            } else {
                prop_assert_eq!(after, before);
            }
            self.check(ack)
        }
    }

    fn settle_one(
        ack: &mut AckTracker,
        model: &mut Model,
        pool: &mut Vec<(usize, Ticket)>,
        pick: Index,
    ) -> Result<(), TestCaseError> {
        if pool.is_empty() {
            return Ok(());
        }
        let (txn, ticket) = pool.swap_remove(pick.index(pool.len()));
        ack.settle(delivered(ticket)).unwrap();
        model.unsettled[txn] -= 1;
        model.check(ack)
    }

    proptest! {
        #[test]
        fn a2_a3_random_settle_orders_keep_the_confirmed_lsn_safe(
            transactions in proptest::collection::vec((1u64..1000, 0usize..5, 0usize..4), 1..20),
            picks in proptest::collection::vec(any::<Index>(), 100),
            lags in proptest::collection::vec(0u64..2000, 40),
        ) {
            let mut ack = AckTracker::new(Lsn(1000));
            let mut model = Model { ends: Vec::new(), unsettled: Vec::new(), open: false, floor: 1000, previous: Lsn(1000) };
            let mut pool = Vec::new();
            let mut picks = picks.iter().cycle();
            let mut lags = lags.iter().cycle();
            let mut end: u64 = 1000;
            for (txn, (gap, events, settle_now)) in transactions.into_iter().enumerate() {
                ack.begin().unwrap();
                model.open = true;
                model.unsettled.push(0);
                for _ in 0..events {
                    pool.push((txn, ack.track(10).unwrap()));
                    model.unsettled[txn] += 1;
                    model.keepalive(&mut ack, end.saturating_sub(*lags.next().unwrap()))?;
                }
                end += gap;
                ack.commit(Lsn(end)).unwrap();
                model.open = false;
                model.ends.push(end);
                model.check(&ack)?;
                for _ in 0..settle_now {
                    settle_one(&mut ack, &mut model, &mut pool, *picks.next().unwrap())?;
                    model.keepalive(&mut ack, end.saturating_sub(*lags.next().unwrap()))?;
                }
            }
            while !pool.is_empty() {
                settle_one(&mut ack, &mut model, &mut pool, *picks.next().unwrap())?;
            }
            prop_assert_eq!(ack.confirmed(), Lsn(end.max(model.floor)));
            prop_assert!(ack.is_idle());
            prop_assert_eq!((ack.inflight_events(), ack.inflight_bytes()), (0, 0));
        }
    }

    async fn receipt(events: usize) -> Delivered {
        let mut sink = StdoutSink::new(Vec::new());
        for _ in 0..events {
            sink.write(b"{}").await.unwrap();
        }
        sink.flush().await.unwrap()
    }

    #[tokio::test]
    async fn a1_only_a_delivery_receipt_settles_events() {
        let mut ack = AckProgress::new(Lsn(100));
        ack.begin().unwrap();
        ack.sent().unwrap();
        ack.sent().unwrap();
        assert_eq!(
            ack.commit(Lsn(200)),
            Err(AckError::Unsettled {
                end_lsn: Lsn(200),
                unsettled: 2
            })
        );
        assert_eq!(ack.confirmed(), Lsn(100));
        ack.settle(receipt(2).await).unwrap();
        assert_eq!(ack.commit(Lsn(200)), Ok(Lsn(200)));
        assert!(ack.is_idle());
    }

    #[tokio::test]
    async fn settling_more_than_was_sent_is_an_error() {
        let mut ack = AckProgress::new(Lsn(0));
        ack.begin().unwrap();
        ack.sent().unwrap();
        assert_eq!(
            ack.settle(receipt(2).await),
            Err(AckError::OverSettled {
                settled: 2,
                unsettled: 1
            })
        );
    }

    #[tokio::test]
    async fn a_reconnect_mid_transaction_replays_against_a_fresh_progress() {
        let mut sink = StdoutSink::new(Vec::new());
        {
            let mut first = AckProgress::new(Lsn(100));
            first.begin().unwrap();
            for _ in 0..3 {
                sink.write(b"{}").await.unwrap();
                first.sent().unwrap();
            }
        }

        sink.reset_pending().await.unwrap();

        let mut second = AckProgress::new(Lsn(100));
        second.begin().unwrap();
        for _ in 0..3 {
            sink.write(b"{}").await.unwrap();
            second.sent().unwrap();
        }
        second.settle(sink.flush().await.unwrap()).unwrap();
        assert_eq!(second.commit(Lsn(200)), Ok(Lsn(200)));
    }

    #[tokio::test]
    async fn a2_confirmed_lsn_never_decreases() {
        let mut ack = AckProgress::new(Lsn(500));
        ack.begin().unwrap();
        ack.settle(receipt(0).await).unwrap();
        assert_eq!(ack.commit(Lsn(400)), Ok(Lsn(500)));
        assert_eq!(ack.keepalive(Lsn(450)), Lsn(500));
    }

    #[test]
    fn a3_keepalive_ignored_while_transaction_open() {
        let mut ack = AckProgress::new(Lsn(100));
        assert_eq!(ack.keepalive(Lsn(150)), Lsn(150));
        ack.begin().unwrap();
        assert!(!ack.is_idle());
        assert_eq!(ack.keepalive(Lsn(900)), Lsn(150));
        assert_eq!(ack.commit(Lsn(200)), Ok(Lsn(200)));
        assert_eq!(ack.keepalive(Lsn(900)), Lsn(900));
    }

    #[test]
    fn transaction_boundaries_must_pair() {
        let mut ack = AckProgress::new(Lsn(0));
        assert_eq!(ack.sent(), Err(AckError::NotOpen));
        assert_eq!(ack.commit(Lsn(1)), Err(AckError::NotOpen));
        ack.begin().unwrap();
        assert_eq!(ack.begin(), Err(AckError::AlreadyOpen));
    }

    proptest! {
        #[test]
        fn a2_random_transactions_keep_the_confirmed_lsn_safe(
            transactions in proptest::collection::vec(
                (1u64..1000, proptest::collection::vec(any::<bool>(), 0..6), 0u64..5000),
                1..20,
            )
        ) {
            let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
            runtime.block_on(async {
                let mut sink = StdoutSink::new(Vec::new());
                let mut ack = AckProgress::new(Lsn(1));
                let mut end = 1u64;
                for (gap, flush_after, keepalive) in transactions {
                    end += gap;
                    let before = ack.confirmed();
                    ack.begin().unwrap();
                    for flush_now in &flush_after {
                        sink.write(b"{}").await.unwrap();
                        ack.sent().unwrap();
                        if *flush_now {
                            ack.settle(sink.flush().await.unwrap()).unwrap();
                        }
                        assert_eq!(ack.keepalive(Lsn(end + keepalive)), before);
                    }
                    let unflushed = sink.pending();
                    if unflushed > 0 {
                        assert_eq!(ack.commit(Lsn(end)), Err(AckError::Unsettled { end_lsn: Lsn(end), unsettled: unflushed }));
                        assert_eq!(ack.confirmed(), before);
                    }
                    ack.settle(sink.flush().await.unwrap()).unwrap();
                    let confirmed = ack.commit(Lsn(end)).unwrap();
                    assert_eq!(confirmed, before.max(Lsn(end)));
                    let idle = ack.keepalive(Lsn(end.saturating_sub(keepalive)));
                    assert!(idle >= confirmed);
                }
            });
        }
    }
}
