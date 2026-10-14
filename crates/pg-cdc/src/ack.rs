use pg_cdc_decoder::Lsn;

use crate::sink::Delivered;

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
    use crate::sink::StdoutSink;
    use proptest::prelude::*;

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
