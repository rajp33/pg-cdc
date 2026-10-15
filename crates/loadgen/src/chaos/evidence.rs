use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};

use anyhow::Context as _;
use aws_sdk_s3::error::DisplayErrorContext;

use crate::checker::{Observed, delivery_from_envelope, observed_from_message};

pub const SAMPLE_LIMIT: usize = 5;
pub const SAMPLE_BYTES: usize = 200;

#[derive(Debug, Default)]
pub struct Observations {
    observed: Mutex<Vec<Observed>>,
    samples: Mutex<Vec<String>>,
    unrecognized: AtomicU64,
}

fn percent_decode(text: &str) -> String {
    let mut bytes = Vec::with_capacity(text.len());
    let mut input = text.bytes();
    while let Some(byte) = input.next() {
        match byte {
            b'+' => bytes.push(b' '),
            b'%' => {
                let high = input.next().and_then(hex_value);
                let low = input.next().and_then(hex_value);
                match (high, low) {
                    (Some(high), Some(low)) => bytes.push(high * 16 + low),
                    _ => bytes.push(b'%'),
                }
            }
            other => bytes.push(other),
        }
    }
    String::from_utf8_lossy(&bytes).into_owned()
}

fn hex_value(digit: u8) -> Option<u8> {
    char::from(digit)
        .to_digit(16)
        .and_then(|value| u8::try_from(value).ok())
}

pub fn published_messages(form: &[u8]) -> Vec<String> {
    String::from_utf8_lossy(form)
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .filter(|(name, _)| *name == "Message" || name.ends_with(".Message"))
        .map(|(_, value)| percent_decode(value))
        .collect()
}

impl Observations {
    pub fn record(&self, body: &str) {
        let observed = serde_json::from_str::<serde_json::Value>(body)
            .ok()
            .and_then(|value| observed_from_message(&value));
        if let Some(observed) = observed {
            self.observed
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(observed);
            return;
        }
        self.unrecognized.fetch_add(1, Ordering::Relaxed);
        let mut samples = self.samples.lock().unwrap_or_else(PoisonError::into_inner);
        if samples.len() < SAMPLE_LIMIT {
            let end = body.floor_char_boundary(SAMPLE_BYTES);
            samples.push(body.get(..end).unwrap_or(body).to_owned());
        }
    }

    pub fn record_publish(&self, form: &[u8]) {
        for message in published_messages(form) {
            self.record(&message);
        }
    }

    pub fn record_marker(&self, token: u64, key: i32, seq: i64) {
        self.observed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Observed::DiscardMarker { token, key, seq });
    }

    pub fn snapshot(&self) -> Vec<Observed> {
        self.observed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub fn unrecognized(&self) -> Vec<String> {
        self.samples
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub fn unrecognized_count(&self) -> u64 {
        self.unrecognized.load(Ordering::Relaxed)
    }
}

pub fn parked_seq(name: &str, bytes: &[u8]) -> anyhow::Result<i64> {
    let object: serde_json::Value = serde_json::from_slice(bytes)
        .with_context(|| format!("parked object {name} is not JSON"))?;
    object
        .get("envelope")
        .and_then(delivery_from_envelope)
        .map(|delivery| delivery.seq)
        .with_context(|| format!("parked object {name} has no envelope with a seq"))
}

pub async fn read_stored(
    s3: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
) -> anyhow::Result<Vec<i64>> {
    let mut stored = Vec::new();
    let mut pages = s3
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .into_paginator()
        .send();
    while let Some(page) = pages.next().await {
        let page = page.map_err(|error| anyhow::anyhow!("{}", DisplayErrorContext(error)))?;
        for name in page.contents().iter().filter_map(|object| object.key()) {
            let object = s3
                .get_object()
                .bucket(bucket)
                .key(name)
                .send()
                .await
                .map_err(|error| anyhow::anyhow!("{}", DisplayErrorContext(error)))?;
            let bytes = object.body.collect().await?.into_bytes();
            stored.push(parked_seq(name, &bytes)?);
        }
    }
    Ok(stored)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checker::Delivery;

    #[test]
    fn messages_become_observations_in_arrival_order_and_unknown_ones_are_counted() {
        let observations = Observations::default();
        observations.record(r#"{"id":"e1","key":{"id":7},"after":{"seq":3}}"#);
        observations.record(r#"{"loadgen_discard":{"token":9,"key":7,"seq":3}}"#);
        observations.record(r#"{"id":"e2","key":{"id":8},"before":{"seq":4}}"#);
        assert_eq!(
            observations.snapshot(),
            [
                Observed::Delivered(Delivery { seq: 3, key: 7 }),
                Observed::DiscardMarker {
                    token: 9,
                    key: 7,
                    seq: 3
                },
                Observed::Delivered(Delivery { seq: 4, key: 8 }),
            ]
        );
        assert_eq!(observations.unrecognized(), Vec::<String>::new());

        observations.record("not json at all");
        observations.record(r#"{"pointer":"s3://bucket/key"}"#);
        let unrecognized = observations.unrecognized();
        assert_eq!(unrecognized.len(), 2);
        assert!(
            unrecognized
                .iter()
                .any(|sample| sample.contains("not json"))
        );
        assert_eq!(observations.snapshot().len(), 3);
    }

    #[test]
    fn unrecognized_samples_are_capped_and_truncated() {
        let observations = Observations::default();
        let long = "x".repeat(1_000);
        for _ in 0..20 {
            observations.record(&long);
        }
        let samples = observations.unrecognized();
        assert_eq!(samples.len(), SAMPLE_LIMIT);
        assert!(samples.iter().all(|sample| sample.len() <= SAMPLE_BYTES));
        assert_eq!(observations.unrecognized_count(), 20);
    }

    #[test]
    fn published_messages_are_read_from_the_query_body() {
        let single = b"Action=Publish&TopicArn=arn%3Aaws%3Asns%3Aus-east-1%3A1%3Achaos.fifo&Message=%7B%22a%22%3A%22caf%C3%A9+x%22%7D&MessageGroupId=g1";
        assert_eq!(published_messages(single), [r#"{"a":"café x"}"#]);

        let batch = b"Action=PublishBatch&TopicArn=t&PublishBatchRequestEntries.member.1.Id=0&PublishBatchRequestEntries.member.1.Message=one&PublishBatchRequestEntries.member.1.MessageGroupId=g1&PublishBatchRequestEntries.member.1.MessageAttributes.entry.1.Name=table&PublishBatchRequestEntries.member.1.MessageAttributes.entry.1.Value.StringValue=Message&PublishBatchRequestEntries.member.2.Id=1&PublishBatchRequestEntries.member.2.Message=two%26three&PublishBatchRequestEntries.member.2.MessageDeduplicationId=d";
        assert_eq!(published_messages(batch), ["one", "two&three"]);
        assert_eq!(
            published_messages(b"Action=GetTopicAttributes&TopicArn=t"),
            Vec::<String>::new()
        );
        assert_eq!(published_messages(b""), Vec::<String>::new());
    }

    #[test]
    fn a_publish_request_records_its_messages_in_entry_order_and_markers_keep_their_place() {
        let observations = Observations::default();
        let message = |seq: i64| {
            format!("%7B%22key%22%3A%7B%22id%22%3A7%7D%2C%22after%22%3A%7B%22seq%22%3A{seq}%7D%7D")
        };
        let batch = format!(
            "Action=PublishBatch&PublishBatchRequestEntries.member.1.Message={}&PublishBatchRequestEntries.member.2.Message={}",
            message(1),
            message(2)
        );
        observations.record_publish(batch.as_bytes());
        observations.record_marker(5, 7, 2);
        observations.record_publish(format!("Action=Publish&Message={}", message(3)).as_bytes());
        assert_eq!(
            observations.snapshot(),
            [
                Observed::Delivered(Delivery { seq: 1, key: 7 }),
                Observed::Delivered(Delivery { seq: 2, key: 7 }),
                Observed::DiscardMarker {
                    token: 5,
                    key: 7,
                    seq: 2
                },
                Observed::Delivered(Delivery { seq: 3, key: 7 }),
            ]
        );
        assert_eq!(observations.unrecognized_count(), 0);
    }

    #[test]
    fn a_parked_object_yields_the_seq_of_its_envelope() {
        let object = br#"{"order_key":"aa","id":"e1","status":"dead","attempts":1,"first_failure_ms":1,"last_error":"","attributes":[],"envelope":{"key":{"id":7},"after":{"seq":42,"payload":"x"}}}"#;
        assert_eq!(
            parked_seq("chaos/chaos/parked/aa/e1.json", object).unwrap(),
            42
        );
        assert!(parked_seq("k", b"{}").is_err());
        assert!(parked_seq("k", b"nope").is_err());
    }
}
