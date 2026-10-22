use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::time::Instant;
use tracing::Instrument;

use super::LINGER;
use crate::metrics::{self, ErrorComponent, ErrorKind, Measurement, SinkOutcome};
use crate::sink::{Outbound, Outcome, Sink, SinkCapabilities, SinkError};

#[derive(Debug)]
struct Submission {
    entries: Vec<Outbound>,
    reply: oneshot::Sender<Vec<Outcome>>,
}

#[derive(Debug)]
pub(crate) struct BatchingSink {
    inner: Arc<dyn Sink>,
    submissions: mpsc::UnboundedSender<Submission>,
}

impl BatchingSink {
    pub(crate) fn new(inner: Arc<dyn Sink>, permits: Arc<Semaphore>) -> (Arc<Self>, Batcher) {
        let (submissions, receiver) = mpsc::unbounded_channel();
        let mut limits = inner.capabilities();
        limits.max_batch_messages = limits.max_batch_messages.max(1);
        let batcher = Batcher {
            inner: Arc::clone(&inner),
            permits,
            limits,
            receiver,
            queue: VecDeque::new(),
            open: HashMap::new(),
            next: 0,
        };
        (Arc::new(Self { inner, submissions }), batcher)
    }
}

#[async_trait]
impl Sink for BatchingSink {
    fn capabilities(&self) -> SinkCapabilities {
        self.inner.capabilities()
    }

    fn name(&self) -> &'static str {
        self.inner.name()
    }

    async fn preflight(&self) -> Result<(), SinkError> {
        self.inner.preflight().await
    }

    async fn send(&self, batch: &[Outbound]) -> Vec<Outcome> {
        if batch.is_empty() {
            return Vec::new();
        }
        let (reply, outcomes) = oneshot::channel();
        let submission = Submission {
            entries: batch.to_vec(),
            reply,
        };
        if self.submissions.send(submission).is_err() {
            return stopped(batch.len());
        }
        outcomes.await.unwrap_or_else(|_| stopped(batch.len()))
    }
}

fn stopped(count: usize) -> Vec<Outcome> {
    vec![Outcome::Retryable(SinkError("the sink batcher has stopped".to_owned())); count]
}

#[derive(Debug)]
struct Queued {
    slot: Slot,
    bytes: usize,
    at: Instant,
    outbound: Outbound,
}

#[derive(Debug, Clone, Copy)]
struct Slot {
    submission: u64,
    index: usize,
}

#[derive(Debug)]
struct Open {
    reply: oneshot::Sender<Vec<Outcome>>,
    outcomes: Vec<Option<Outcome>>,
    pending: usize,
}

#[derive(Debug)]
struct Flushed {
    slots: Vec<Slot>,
    outcomes: Vec<Outcome>,
}

#[derive(Debug)]
pub(crate) struct Batcher {
    inner: Arc<dyn Sink>,
    permits: Arc<Semaphore>,
    limits: SinkCapabilities,
    receiver: mpsc::UnboundedReceiver<Submission>,
    queue: VecDeque<Queued>,
    open: HashMap<u64, Open>,
    next: u64,
}

impl Batcher {
    pub(crate) async fn run(mut self) {
        let mut flights = FuturesUnordered::new();
        let mut receiving = true;
        loop {
            if !receiving && self.queue.is_empty() && flights.is_empty() {
                return;
            }
            let (count, full) = self.plan();
            let deadline = self.queue.front().map(|queued| queued.at + LINGER);
            let due = full || deadline.is_some_and(|deadline| deadline <= Instant::now());
            let timer = async {
                match deadline {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                biased;
                Some(flushed) = flights.next(), if !flights.is_empty() => self.complete(flushed),
                permit = Arc::clone(&self.permits).acquire_owned(), if due => {
                    let (batch, slots) = self.take(count);
                    match permit {
                        Ok(permit) if !batch.is_empty() => {
                            flights.push(call(Arc::clone(&self.inner), permit, batch, slots));
                        }
                        Ok(_) => {}
                        Err(_) => self.complete(Flushed {
                            outcomes: vec![
                                Outcome::Retryable(SinkError("the sink is closed".to_owned()));
                                slots.len()
                            ],
                            slots,
                        }),
                    }
                }
                received = self.receiver.recv(), if receiving => match received {
                    Some(submission) => self.enqueue(submission),
                    None => receiving = false,
                },
                () = timer, if !due => {}
            }
        }
    }

    fn enqueue(&mut self, submission: Submission) {
        let Submission { entries, reply } = submission;
        let id = self.next;
        self.next += 1;
        let at = Instant::now();
        let count = entries.len();
        for (index, outbound) in entries.into_iter().enumerate() {
            self.queue.push_back(Queued {
                slot: Slot {
                    submission: id,
                    index,
                },
                bytes: outbound.encoded_len(),
                at,
                outbound,
            });
        }
        self.open.insert(
            id,
            Open {
                reply,
                outcomes: vec![None; count],
                pending: count,
            },
        );
    }

    fn plan(&self) -> (usize, bool) {
        let limits = self.limits;
        let mut count = 0;
        let mut bytes = 0_usize;
        for queued in &self.queue {
            if count == limits.max_batch_messages
                || (count > 0 && bytes.saturating_add(queued.bytes) > limits.max_batch_bytes)
            {
                return (count, true);
            }
            count += 1;
            bytes = bytes.saturating_add(queued.bytes);
        }
        (count, count >= limits.max_batch_messages)
    }

    fn take(&mut self, count: usize) -> (Vec<Outbound>, Vec<Slot>) {
        let mut batch = Vec::with_capacity(count);
        let mut slots = Vec::with_capacity(count);
        for queued in self.queue.drain(..count) {
            let submission = queued.slot.submission;
            if self
                .open
                .get(&submission)
                .is_none_or(|open| open.reply.is_closed())
            {
                self.open.remove(&submission);
                continue;
            }
            slots.push(queued.slot);
            batch.push(queued.outbound);
        }
        (batch, slots)
    }

    fn complete(&mut self, flushed: Flushed) {
        for (slot, outcome) in flushed.slots.into_iter().zip(flushed.outcomes) {
            let Entry::Occupied(mut entry) = self.open.entry(slot.submission) else {
                continue;
            };
            let open = entry.get_mut();
            if let Some(cell) = open.outcomes.get_mut(slot.index) {
                *cell = Some(outcome);
            }
            open.pending = open.pending.saturating_sub(1);
            if open.pending == 0 {
                let Open {
                    reply, outcomes, ..
                } = entry.remove();
                let _ = reply.send(outcomes.into_iter().flatten().collect());
            }
        }
    }
}

async fn call(
    inner: Arc<dyn Sink>,
    permit: OwnedSemaphorePermit,
    batch: Vec<Outbound>,
    slots: Vec<Slot>,
) -> Flushed {
    let _permit = permit;
    let span = tracing::debug_span!(
        "sink_send",
        sink = inner.name(),
        size = u64::try_from(batch.len()).unwrap_or(u64::MAX),
        delivered = tracing::field::Empty,
        retryable = tracing::field::Empty,
        terminal = tracing::field::Empty,
    );
    let started = metrics::enabled().then(Instant::now);
    let mut outcomes = inner.send(&batch).instrument(span.clone()).await;
    let elapsed = started.map(|started| started.elapsed());
    if outcomes.len() != batch.len() {
        tracing::error!(
            sent = batch.len(),
            outcomes = outcomes.len(),
            "the sink broke its contract of one outcome per message"
        );
    }
    outcomes.resize_with(batch.len(), || {
        Outcome::Retryable(SinkError("the sink returned no outcome".to_owned()))
    });
    record_sink_call(&span, &outcomes, elapsed);
    Flushed { slots, outcomes }
}

pub(super) fn record_sink_call(
    span: &tracing::Span,
    outcomes: &[Outcome],
    elapsed: Option<Duration>,
) {
    if span.is_disabled() && elapsed.is_none() {
        return;
    }
    let (mut delivered, mut retryable, mut terminal) = (0_u64, 0_u64, 0_u64);
    for outcome in outcomes {
        match outcome {
            Outcome::Delivered => delivered += 1,
            Outcome::Retryable(_) => retryable += 1,
            Outcome::Terminal(_) => terminal += 1,
        }
    }
    span.record("delivered", delivered);
    span.record("retryable", retryable);
    span.record("terminal", terminal);
    if let Some(elapsed) = elapsed {
        let outcome = if terminal > 0 {
            SinkOutcome::Terminal
        } else if retryable > 0 {
            SinkOutcome::Retryable
        } else {
            SinkOutcome::Delivered
        };
        metrics::record(Measurement::Sink { outcome, elapsed });
        for (kind, count) in [
            (ErrorKind::Retryable, retryable),
            (ErrorKind::Terminal, terminal),
        ] {
            if count > 0 {
                metrics::record(Measurement::Error {
                    component: ErrorComponent::Sink,
                    kind,
                    count,
                });
            }
        }
    }
}
