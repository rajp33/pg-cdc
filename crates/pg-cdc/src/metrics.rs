use std::fmt;
use std::sync::OnceLock;
use std::time::Duration;

use crate::leader::record::TakeoverKind;

static RECORDER: OnceLock<Box<dyn Recorder>> = OnceLock::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventOutcome {
    Received,
    Delivered,
    Parked,
    Dead,
}

impl EventOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Received => "received",
            Self::Delivered => "delivered",
            Self::Parked => "parked",
            Self::Dead => "dead",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkOutcome {
    Delivered,
    Retryable,
    Terminal,
}

impl SinkOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Delivered => "delivered",
            Self::Retryable => "retryable",
            Self::Terminal => "terminal",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreOperation {
    AppendParked,
    LoadParkedHeads,
    ListParkedQueues,
    ReadParked,
    SetParkedStatus,
    RemoveParked,
    ReadLeader,
    SwapLeader,
    PutPayload,
}

impl StoreOperation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AppendParked => "append_parked",
            Self::LoadParkedHeads => "load_parked_heads",
            Self::ListParkedQueues => "list_parked_queues",
            Self::ReadParked => "read_parked",
            Self::SetParkedStatus => "set_parked_status",
            Self::RemoveParked => "remove_parked",
            Self::ReadLeader => "read_leader",
            Self::SwapLeader => "swap_leader",
            Self::PutPayload => "put_payload",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Measurement<'a> {
    Event {
        table: &'a str,
        op: &'a str,
        outcome: EventOutcome,
        count: u64,
    },
    CommitToDelivery(Duration),
    Sink {
        outcome: SinkOutcome,
        elapsed: Duration,
    },
    Store {
        operation: StoreOperation,
        elapsed: Duration,
    },
    Takeover(TakeoverKind),
    DecodeFallback {
        type_oid: u32,
    },
}

pub trait Recorder: Send + Sync + fmt::Debug {
    fn record(&self, measurement: Measurement<'_>);
}

#[derive(Debug, thiserror::Error)]
#[error("a metrics recorder is already installed")]
pub struct AlreadyInstalled;

pub fn install(recorder: Box<dyn Recorder>) -> Result<(), AlreadyInstalled> {
    RECORDER.set(recorder).map_err(|_| AlreadyInstalled)
}

pub fn enabled() -> bool {
    RECORDER.get().is_some()
}

pub fn record(measurement: Measurement<'_>) {
    if let Some(recorder) = RECORDER.get() {
        recorder.record(measurement);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[derive(Debug)]
    struct Collecting(Arc<Mutex<Vec<String>>>);

    impl Recorder for Collecting {
        fn record(&self, measurement: Measurement<'_>) {
            self.0.lock().unwrap().push(format!("{measurement:?}"));
        }
    }

    fn every_measurement() -> Vec<Measurement<'static>> {
        vec![
            Measurement::Event {
                table: "orders",
                op: "insert",
                outcome: EventOutcome::Received,
                count: 3,
            },
            Measurement::CommitToDelivery(Duration::from_millis(12)),
            Measurement::Sink {
                outcome: SinkOutcome::Retryable,
                elapsed: Duration::from_millis(5),
            },
            Measurement::Store {
                operation: StoreOperation::PutPayload,
                elapsed: Duration::from_millis(7),
            },
            Measurement::Takeover(TakeoverKind::Unclean),
            Measurement::DecodeFallback { type_oid: 1700 },
        ]
    }

    #[test]
    fn nothing_is_recorded_without_a_recorder() {
        assert!(!enabled());
        for measurement in every_measurement() {
            record(measurement);
        }
        assert!(!enabled());
    }

    // nextest runs each test in its own process; the recorder is a process-wide singleton.
    #[test]
    fn an_installed_recorder_receives_every_measurement_and_a_second_install_is_refused() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        install(Box::new(Collecting(Arc::clone(&seen)))).unwrap();
        assert!(enabled());
        let sent = every_measurement();
        for measurement in &sent {
            record(*measurement);
        }
        let expected: Vec<String> = sent.iter().map(|m| format!("{m:?}")).collect();
        assert_eq!(*seen.lock().unwrap(), expected);
        let refused = install(Box::new(Collecting(Arc::default()))).unwrap_err();
        assert_eq!(
            refused.to_string(),
            "a metrics recorder is already installed"
        );
        record(Measurement::DecodeFallback { type_oid: 1 });
        assert_eq!(seen.lock().unwrap().len(), expected.len() + 1);
    }

    #[test]
    fn labels_match_the_metric_attribute_values() {
        let events = [
            (EventOutcome::Received, "received"),
            (EventOutcome::Delivered, "delivered"),
            (EventOutcome::Parked, "parked"),
            (EventOutcome::Dead, "dead"),
        ];
        for (outcome, label) in events {
            assert_eq!(outcome.as_str(), label);
        }
        let sinks = [
            (SinkOutcome::Delivered, "delivered"),
            (SinkOutcome::Retryable, "retryable"),
            (SinkOutcome::Terminal, "terminal"),
        ];
        for (outcome, label) in sinks {
            assert_eq!(outcome.as_str(), label);
        }
        let operations = [
            (StoreOperation::AppendParked, "append_parked"),
            (StoreOperation::LoadParkedHeads, "load_parked_heads"),
            (StoreOperation::ListParkedQueues, "list_parked_queues"),
            (StoreOperation::ReadParked, "read_parked"),
            (StoreOperation::SetParkedStatus, "set_parked_status"),
            (StoreOperation::RemoveParked, "remove_parked"),
            (StoreOperation::ReadLeader, "read_leader"),
            (StoreOperation::SwapLeader, "swap_leader"),
            (StoreOperation::PutPayload, "put_payload"),
        ];
        for (operation, label) in operations {
            assert_eq!(operation.as_str(), label);
        }
    }
}
