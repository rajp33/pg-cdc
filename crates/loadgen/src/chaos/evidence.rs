use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
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
    groups: Mutex<HashMap<i32, String>>,
    misgrouped: AtomicU64,
    misgrouped_sample: Mutex<Option<String>>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Published {
    pub message: String,
    pub group: Option<String>,
}

const BATCH_MEMBER: &str = "PublishBatchRequestEntries.member.";

#[derive(Debug, Default)]
struct Entry {
    id: Option<String>,
    message: Option<String>,
    group: Option<String>,
}

impl Entry {
    fn set(&mut self, field: &str, value: String) {
        match field {
            "Id" => self.id = Some(value),
            "Message" => self.message = Some(value),
            "MessageGroupId" => self.group = Some(value),
            _ => {}
        }
    }

    fn published(self) -> Option<Published> {
        Some(Published {
            message: self.message?,
            group: self.group,
        })
    }
}

fn successful_ids(response: &[u8]) -> HashSet<String> {
    let text = String::from_utf8_lossy(response);
    let Some(section) = text
        .split_once("<Successful>")
        .and_then(|(_, rest)| rest.split_once("</Successful>"))
        .map(|(section, _)| section)
    else {
        return HashSet::new();
    };
    section
        .split("<Id>")
        .skip(1)
        .filter_map(|rest| rest.split_once("</Id>"))
        .map(|(id, _)| id.to_owned())
        .collect()
}

#[derive(Debug, Default)]
struct Form {
    action: Option<String>,
    single: Entry,
    batch: BTreeMap<u32, Entry>,
}

fn parse_form(form: &[u8]) -> Form {
    let text = String::from_utf8_lossy(form);
    let mut parsed = Form::default();
    for (name, value) in text.split('&').filter_map(|pair| pair.split_once('=')) {
        let value = percent_decode(value);
        if name == "Action" {
            parsed.action = Some(value);
        } else if let Some((member, field)) = name
            .strip_prefix(BATCH_MEMBER)
            .and_then(|rest| rest.split_once('.'))
        {
            if let Ok(member) = member.parse() {
                parsed.batch.entry(member).or_default().set(field, value);
            }
        } else {
            parsed.single.set(name, value);
        }
    }
    parsed
}

pub fn message_groups(form: &[u8]) -> Vec<String> {
    let parsed = parse_form(form);
    let groups: BTreeSet<String> = std::iter::once(parsed.single)
        .chain(parsed.batch.into_values())
        .filter_map(|entry| entry.group)
        .collect();
    groups.into_iter().collect()
}

pub fn accepted_messages(form: &[u8], response: &[u8]) -> Vec<Published> {
    let parsed = parse_form(form);
    match parsed.action.as_deref() {
        Some("Publish") => parsed.single.published().into_iter().collect(),
        Some("PublishBatch") => {
            let successful = successful_ids(response);
            parsed
                .batch
                .into_values()
                .filter(|entry| entry.id.as_ref().is_some_and(|id| successful.contains(id)))
                .filter_map(Entry::published)
                .collect()
        }
        _ => Vec::new(),
    }
}

impl Observations {
    pub fn record(&self, body: &str) -> Option<Observed> {
        let observed = serde_json::from_str::<serde_json::Value>(body)
            .ok()
            .and_then(|value| observed_from_message(&value));
        if let Some(observed) = observed {
            self.observed
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(observed);
            return Some(observed);
        }
        self.unrecognized.fetch_add(1, Ordering::Relaxed);
        let mut samples = self.samples.lock().unwrap_or_else(PoisonError::into_inner);
        if samples.len() < SAMPLE_LIMIT {
            let end = body.floor_char_boundary(SAMPLE_BYTES);
            samples.push(body.get(..end).unwrap_or(body).to_owned());
        }
        None
    }

    pub fn record_publish(&self, form: &[u8], response: &[u8]) -> usize {
        let accepted = accepted_messages(form, response);
        for published in &accepted {
            if let Some(observed) = self.record(&published.message)
                && let (Observed::Delivered(delivery), Some(group)) = (observed, &published.group)
            {
                self.check_group(delivery.key, group);
            }
        }
        accepted.len()
    }

    fn check_group(&self, key: i32, group: &str) {
        let mut groups = self.groups.lock().unwrap_or_else(PoisonError::into_inner);
        let first = groups.entry(key).or_insert_with(|| group.to_owned());
        if first == group {
            return;
        }
        self.misgrouped.fetch_add(1, Ordering::Relaxed);
        self.misgrouped_sample
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_or_insert_with(|| format!("key {key} in message group {group} after {first}"));
    }

    pub fn misgrouped_count(&self) -> u64 {
        self.misgrouped.load(Ordering::Relaxed)
    }

    pub fn misgrouped(&self) -> Option<String> {
        self.misgrouped_sample
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
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

    const OK: &[u8] = b"<PublishResponse><PublishResult><MessageId>m</MessageId></PublishResult></PublishResponse>";

    fn batch_response(successful: &[&str], failed: &[&str]) -> String {
        let members = |ids: &[&str], extra: &str| {
            ids.iter()
                .map(|id| ["<member><Id>", id, "</Id>", extra, "</member>"].concat())
                .collect::<String>()
        };
        format!(
            "<PublishBatchResponse xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\"><PublishBatchResult><Successful>{}</Successful><Failed>{}</Failed></PublishBatchResult></PublishBatchResponse>",
            members(
                successful,
                "<MessageId>m</MessageId><SequenceNumber>1</SequenceNumber>"
            ),
            members(
                failed,
                "<Code>InternalError</Code><SenderFault>false</SenderFault>"
            ),
        )
    }

    fn published(message: &str, group: Option<&str>) -> Published {
        Published {
            message: message.to_owned(),
            group: group.map(str::to_owned),
        }
    }

    #[test]
    fn published_messages_are_read_from_the_query_body() {
        let single = b"Action=Publish&TopicArn=arn%3Aaws%3Asns%3Aus-east-1%3A1%3Achaos.fifo&Message=%7B%22a%22%3A%22caf%C3%A9+x%22%7D&MessageGroupId=g1";
        assert_eq!(
            accepted_messages(single, OK),
            [published(r#"{"a":"café x"}"#, Some("g1"))]
        );

        let batch = b"Action=PublishBatch&TopicArn=t&PublishBatchRequestEntries.member.1.Id=0&PublishBatchRequestEntries.member.1.Message=one&PublishBatchRequestEntries.member.1.MessageGroupId=g1&PublishBatchRequestEntries.member.1.MessageAttributes.entry.1.Name=table&PublishBatchRequestEntries.member.1.MessageAttributes.entry.1.Value.StringValue=Message&PublishBatchRequestEntries.member.2.Id=1&PublishBatchRequestEntries.member.2.Message=two%26three&PublishBatchRequestEntries.member.2.MessageDeduplicationId=d";
        assert_eq!(
            accepted_messages(batch, batch_response(&["0", "1"], &[]).as_bytes()),
            [published("one", Some("g1")), published("two&three", None)]
        );
        assert_eq!(
            accepted_messages(b"Action=GetTopicAttributes&TopicArn=t", OK),
            Vec::<Published>::new()
        );
        assert_eq!(accepted_messages(b"", OK), Vec::<Published>::new());
    }

    #[test]
    fn a_publish_is_ordered_by_every_message_group_it_carries() {
        let batch = b"Action=PublishBatch&PublishBatchRequestEntries.member.1.Id=0&PublishBatchRequestEntries.member.1.Message=one&PublishBatchRequestEntries.member.1.MessageGroupId=g2&PublishBatchRequestEntries.member.2.Id=1&PublishBatchRequestEntries.member.2.Message=two&PublishBatchRequestEntries.member.2.MessageGroupId=g1&PublishBatchRequestEntries.member.3.Id=2&PublishBatchRequestEntries.member.3.Message=three&PublishBatchRequestEntries.member.3.MessageGroupId=g2";
        assert_eq!(message_groups(batch), ["g1", "g2"]);
        assert_eq!(
            message_groups(b"Action=Publish&Message=m&MessageGroupId=k%2F1"),
            ["k/1"]
        );
        assert_eq!(
            message_groups(b"Action=Publish&Message=m"),
            Vec::<String>::new()
        );
        assert_eq!(
            message_groups(b"Action=GetTopicAttributes&TopicArn=t"),
            Vec::<String>::new()
        );
    }

    #[test]
    fn only_the_entries_a_batch_response_lists_as_successful_were_delivered() {
        let entry = |member: u32, id: &str, message: &str| {
            format!(
                "PublishBatchRequestEntries.member.{member}.Id={id}&PublishBatchRequestEntries.member.{member}.Message={message}"
            )
        };
        let form = format!(
            "Action=PublishBatch&TopicArn=t&{}&{}&{}&{}&{}&{}&{}&{}&{}&{}",
            entry(1, "e1", "one"),
            entry(2, "e2", "two"),
            entry(3, "e3", "three"),
            entry(4, "e4", "four"),
            entry(5, "e5", "five"),
            entry(6, "e6", "six"),
            entry(7, "e7", "seven"),
            entry(8, "e8", "eight"),
            entry(9, "e9", "nine"),
            entry(10, "e10", "ten"),
        );
        let mixed = batch_response(
            &["e10", "e1", "e3"],
            &["e2", "e4", "e5", "e6", "e7", "e8", "e9"],
        );
        assert_eq!(
            accepted_messages(form.as_bytes(), mixed.as_bytes()),
            [
                published("one", None),
                published("three", None),
                published("ten", None)
            ]
        );
        let refused = batch_response(&[], &["e1", "e2"]);
        assert_eq!(
            accepted_messages(form.as_bytes(), refused.as_bytes()),
            Vec::<Published>::new()
        );
        assert_eq!(
            accepted_messages(form.as_bytes(), b"<PublishBatchResponse/>"),
            Vec::<Published>::new()
        );
    }

    fn envelope(key: i32, seq: i64) -> String {
        format!("%7B%22key%22%3A%7B%22id%22%3A{key}%7D%2C%22after%22%3A%7B%22seq%22%3A{seq}%7D%7D")
    }

    #[test]
    fn a_publish_request_records_its_accepted_messages_in_entry_order_and_markers_keep_their_place()
    {
        let observations = Observations::default();
        let batch = format!(
            "Action=PublishBatch&PublishBatchRequestEntries.member.1.Id=a&PublishBatchRequestEntries.member.1.Message={}&PublishBatchRequestEntries.member.2.Id=b&PublishBatchRequestEntries.member.2.Message={}&PublishBatchRequestEntries.member.3.Id=c&PublishBatchRequestEntries.member.3.Message={}",
            envelope(7, 1),
            envelope(7, 2),
            envelope(7, 9),
        );
        observations.record_publish(
            batch.as_bytes(),
            batch_response(&["a", "b"], &["c"]).as_bytes(),
        );
        observations.record_marker(5, 7, 2);
        observations.record_publish(
            format!("Action=Publish&Message={}", envelope(7, 3)).as_bytes(),
            OK,
        );
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
        assert_eq!(observations.misgrouped_count(), 0);
    }

    #[test]
    fn a_key_published_under_a_second_message_group_is_counted() {
        let observations = Observations::default();
        let publish = |key: i32, seq: i64, group: &str| {
            format!(
                "Action=Publish&Message={}&MessageGroupId={group}",
                envelope(key, seq)
            )
        };
        observations.record_publish(publish(7, 1, "g7").as_bytes(), OK);
        observations.record_publish(publish(8, 2, "g8").as_bytes(), OK);
        observations.record_publish(publish(7, 3, "g7").as_bytes(), OK);
        assert_eq!(observations.misgrouped_count(), 0);
        observations.record_publish(publish(7, 4, "g8").as_bytes(), OK);
        assert_eq!(observations.misgrouped_count(), 1);
        assert_eq!(
            observations.misgrouped(),
            Some("key 7 in message group g8 after g7".to_owned())
        );
        assert_eq!(observations.snapshot().len(), 4);
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
