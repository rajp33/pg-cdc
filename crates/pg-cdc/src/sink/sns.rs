use std::collections::HashMap;
use std::fmt;
use std::sync::OnceLock;
use std::time::Duration;

use async_trait::async_trait;
use aws_config::retry::RetryConfig;
use aws_config::timeout::TimeoutConfig;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_sns::Client;
use aws_sdk_sns::error::DisplayErrorContext;
use aws_sdk_sns::operation::publish_batch::PublishBatchError;
use aws_sdk_sns::types::{MessageAttributeValue, PublishBatchRequestEntry};
use aws_smithy_http_client::tls::{Provider, rustls_provider::CryptoMode};

use super::{AttributeValue, Outbound, Outcome, Sink, SinkCapabilities, SinkError};

pub const SNS_MAX_BATCH_MESSAGES: usize = 10;
pub const SNS_DEFAULT_MESSAGE_BYTES: usize = 256 * 1024;
pub const SNS_MAX_MESSAGE_BYTES: usize = 1024 * 1024;
pub const SNS_MAX_GROUP_ID_BYTES: usize = 128;
pub const SNS_OPERATION_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, PartialEq, Eq)]
struct Entry {
    id: String,
    message: String,
    attributes: Vec<(String, AttributeValue)>,
    group: Option<String>,
    dedupe: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EntryFailure {
    id: String,
    code: String,
    message: String,
    sender_fault: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct BatchResult {
    successful: Vec<String>,
    failed: Vec<EntryFailure>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PublishFailure {
    TooLong(String),
    Request(String),
}

#[async_trait]
trait SnsApi: Send + Sync + fmt::Debug {
    async fn topic_attributes(&self, topic_arn: &str) -> Result<HashMap<String, String>, String>;
    async fn publish_batch(
        &self,
        topic_arn: &str,
        entries: &[Entry],
    ) -> Result<BatchResult, PublishFailure>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Topic {
    fifo: bool,
    max_message_bytes: usize,
}

impl Topic {
    fn from_attributes(attributes: &HashMap<String, String>) -> Self {
        let fifo = attributes
            .get("FifoTopic")
            .is_some_and(|value| value == "true");
        let max_message_bytes = attributes
            .get("MaximumMessageSize")
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|bytes| *bytes > 0)
            .map_or(SNS_DEFAULT_MESSAGE_BYTES, |bytes| {
                bytes.min(SNS_MAX_MESSAGE_BYTES)
            });
        Self {
            fifo,
            max_message_bytes,
        }
    }
}

#[derive(Debug)]
pub struct SnsSink {
    api: Box<dyn SnsApi>,
    topic_arn: String,
    fair_queue_group: Option<String>,
    topic: OnceLock<Topic>,
}

impl SnsSink {
    pub fn new(client: Client, topic_arn: String, fair_queue_group: Option<String>) -> Self {
        Self::with_api(Box::new(AwsSns(client)), topic_arn, fair_queue_group)
    }

    pub async fn from_environment(
        topic_arn: String,
        region: Option<String>,
        fair_queue_group: Option<String>,
    ) -> Self {
        let http_client = aws_smithy_http_client::Builder::new()
            .tls_provider(Provider::Rustls(CryptoMode::Ring))
            .build_https();
        let mut loader = aws_config::defaults(BehaviorVersion::latest())
            .http_client(http_client)
            .retry_config(RetryConfig::disabled())
            .timeout_config(
                TimeoutConfig::builder()
                    .operation_timeout(SNS_OPERATION_TIMEOUT)
                    .build(),
            );
        if let Some(region) = region {
            loader = loader.region(Region::new(region));
        }
        Self::new(
            Client::new(&loader.load().await),
            topic_arn,
            fair_queue_group,
        )
    }

    fn with_api(api: Box<dyn SnsApi>, topic_arn: String, fair_queue_group: Option<String>) -> Self {
        Self {
            api,
            topic_arn,
            fair_queue_group,
            topic: OnceLock::new(),
        }
    }

    fn entry(&self, index: usize, outbound: &Outbound, topic: Topic) -> Result<Entry, SinkError> {
        let message = std::str::from_utf8(&outbound.body)
            .map_err(|error| SinkError(format!("event {} is not UTF-8: {error}", outbound.id)))?;
        let (group, dedupe) = if topic.fifo {
            (
                Some(outbound.order_key.to_string()),
                Some(outbound.id.to_string()),
            )
        } else {
            let group = self
                .fair_queue_group
                .as_deref()
                .and_then(|column| outbound.attribute(column))
                .map(AttributeValue::as_str)
                .filter(|value| valid_group_id(value))
                .map(str::to_owned);
            (group, None)
        };
        Ok(Entry {
            id: index.to_string(),
            message: message.to_owned(),
            attributes: outbound.attributes.clone(),
            group,
            dedupe,
        })
    }

    async fn publish(&self, entries: &[Entry]) -> Vec<(usize, Outcome)> {
        if entries.is_empty() {
            return Vec::new();
        }
        match self.api.publish_batch(&self.topic_arn, entries).await {
            Err(PublishFailure::TooLong(message)) if entries.len() > 1 => {
                tracing::warn!(
                    entries = entries.len(),
                    %message,
                    "SNS rejected the batch as too long; sending its entries one at a time"
                );
                let mut outcomes = Vec::with_capacity(entries.len());
                for entry in entries {
                    let single = std::slice::from_ref(entry);
                    let result = self.api.publish_batch(&self.topic_arn, single).await;
                    outcomes.extend(results(single, result));
                }
                outcomes
            }
            result => results(entries, result),
        }
    }
}

fn valid_group_id(value: &str) -> bool {
    (1..=SNS_MAX_GROUP_ID_BYTES).contains(&value.len())
        && value.bytes().all(|byte| byte.is_ascii_graphic())
}

fn results(
    entries: &[Entry],
    result: Result<BatchResult, PublishFailure>,
) -> Vec<(usize, Outcome)> {
    let index = |id: &str| id.parse::<usize>().ok();
    let every_entry = |outcome: Outcome| {
        entries
            .iter()
            .filter_map(|entry| Some((index(&entry.id)?, outcome.clone())))
            .collect()
    };
    match result {
        Ok(batch) => {
            let mut outcomes: Vec<(usize, Outcome)> = batch
                .successful
                .iter()
                .filter_map(|id| Some((index(id)?, Outcome::Delivered)))
                .collect();
            for failure in batch.failed {
                let Some(position) = index(&failure.id) else {
                    continue;
                };
                let error = SinkError(format!("{}: {}", failure.code, failure.message));
                let outcome = if failure.sender_fault {
                    Outcome::Terminal(error)
                } else {
                    Outcome::Retryable(error)
                };
                outcomes.push((position, outcome));
            }
            outcomes
        }
        Err(PublishFailure::TooLong(message)) => every_entry(Outcome::Terminal(SinkError(message))),
        Err(PublishFailure::Request(message)) => {
            every_entry(Outcome::Retryable(SinkError(message)))
        }
    }
}

#[async_trait]
impl Sink for SnsSink {
    fn capabilities(&self) -> SinkCapabilities {
        let max = self
            .topic
            .get()
            .map_or(SNS_DEFAULT_MESSAGE_BYTES, |topic| topic.max_message_bytes);
        SinkCapabilities {
            max_batch_messages: SNS_MAX_BATCH_MESSAGES,
            max_batch_bytes: max,
            max_message_bytes: max,
        }
    }

    async fn preflight(&self) -> Result<(), SinkError> {
        let attributes = self
            .api
            .topic_attributes(&self.topic_arn)
            .await
            .map_err(|error| {
                SinkError(format!("cannot read SNS topic {}: {error}", self.topic_arn))
            })?;
        let topic = Topic::from_attributes(&attributes);
        tracing::info!(
            topic_arn = %self.topic_arn,
            fifo = topic.fifo,
            max_message_bytes = topic.max_message_bytes,
            limit_from_topic = attributes.contains_key("MaximumMessageSize"),
            "SNS topic ready"
        );
        if topic.fifo && self.fair_queue_group.is_some() {
            tracing::warn!(
                "sink.fair_queue_group is ignored on a FIFO topic, where the order key is the message group"
            );
        }
        let _ = self.topic.set(topic);
        Ok(())
    }

    async fn send(&self, batch: &[Outbound]) -> Vec<Outcome> {
        let Some(topic) = self.topic.get().copied() else {
            let error = SinkError("the SNS sink has not passed preflight".to_owned());
            return vec![Outcome::Retryable(error); batch.len()];
        };
        let mut outcomes: Vec<Option<Outcome>> = vec![None; batch.len()];
        let mut entries = Vec::with_capacity(batch.len());
        for (index, outbound) in batch.iter().enumerate() {
            match self.entry(index, outbound, topic) {
                Ok(entry) => entries.push(entry),
                Err(error) => {
                    if let Some(slot) = outcomes.get_mut(index) {
                        *slot = Some(Outcome::Terminal(error));
                    }
                }
            }
        }
        for (index, outcome) in self.publish(&entries).await {
            if let Some(slot) = outcomes.get_mut(index) {
                *slot = Some(outcome);
            }
        }
        outcomes
            .into_iter()
            .map(|outcome| {
                outcome.unwrap_or_else(|| {
                    Outcome::Retryable(SinkError("SNS returned no result for the entry".to_owned()))
                })
            })
            .collect()
    }
}

#[derive(Debug)]
struct AwsSns(Client);

#[async_trait]
impl SnsApi for AwsSns {
    async fn topic_attributes(&self, topic_arn: &str) -> Result<HashMap<String, String>, String> {
        let output = self
            .0
            .get_topic_attributes()
            .topic_arn(topic_arn)
            .send()
            .await
            .map_err(|error| DisplayErrorContext(&error).to_string())?;
        Ok(output.attributes.unwrap_or_default())
    }

    async fn publish_batch(
        &self,
        topic_arn: &str,
        entries: &[Entry],
    ) -> Result<BatchResult, PublishFailure> {
        let mut request = self.0.publish_batch().topic_arn(topic_arn);
        for entry in entries {
            request = request.publish_batch_request_entries(sdk_entry(entry)?);
        }
        match request.send().await {
            Ok(output) => Ok(BatchResult {
                successful: output
                    .successful()
                    .iter()
                    .filter_map(|entry| entry.id().map(str::to_owned))
                    .collect(),
                failed: output
                    .failed()
                    .iter()
                    .map(|entry| EntryFailure {
                        id: entry.id().to_owned(),
                        code: entry.code().to_owned(),
                        message: entry.message().unwrap_or_default().to_owned(),
                        sender_fault: entry.sender_fault(),
                    })
                    .collect(),
            }),
            Err(error) => {
                let too_long = matches!(
                    error.as_service_error(),
                    Some(PublishBatchError::BatchRequestTooLongException(_))
                );
                let message = DisplayErrorContext(&error).to_string();
                Err(if too_long {
                    PublishFailure::TooLong(message)
                } else {
                    PublishFailure::Request(message)
                })
            }
        }
    }
}

fn sdk_entry(entry: &Entry) -> Result<PublishBatchRequestEntry, PublishFailure> {
    let invalid =
        |error: aws_sdk_sns::error::BuildError| PublishFailure::Request(error.to_string());
    let mut builder = PublishBatchRequestEntry::builder()
        .id(&entry.id)
        .message(&entry.message)
        .set_message_group_id(entry.group.clone())
        .set_message_deduplication_id(entry.dedupe.clone());
    for (name, value) in &entry.attributes {
        let value = MessageAttributeValue::builder()
            .data_type(value.data_type())
            .string_value(value.as_str())
            .build()
            .map_err(invalid)?;
        builder = builder.message_attributes(name, value);
    }
    builder.build().map_err(invalid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{EventId, Op, sample_envelope};
    use crate::order::OrderKeyId;
    use bytes::Bytes;
    use pg_cdc_decoder::Lsn;
    use serde_json::json;
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    #[derive(Debug, Default)]
    struct FakeSns {
        attributes: Option<HashMap<String, String>>,
        responses: Mutex<VecDeque<Result<BatchResult, PublishFailure>>>,
        calls: Mutex<Vec<Vec<Entry>>>,
    }

    #[async_trait]
    impl SnsApi for Arc<FakeSns> {
        async fn topic_attributes(
            &self,
            _topic_arn: &str,
        ) -> Result<HashMap<String, String>, String> {
            self.attributes
                .clone()
                .ok_or_else(|| "NotFound: Topic does not exist".to_owned())
        }

        async fn publish_batch(
            &self,
            _topic_arn: &str,
            entries: &[Entry],
        ) -> Result<BatchResult, PublishFailure> {
            self.calls.lock().unwrap().push(entries.to_vec());
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| {
                    Ok(BatchResult {
                        successful: entries.iter().map(|entry| entry.id.clone()).collect(),
                        failed: Vec::new(),
                    })
                })
        }
    }

    fn topic(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    async fn preflighted(fake: FakeSns, fair_queue_group: Option<&str>) -> (SnsSink, Arc<FakeSns>) {
        let fake = Arc::new(fake);
        let sink = SnsSink::with_api(
            Box::new(Arc::clone(&fake)),
            "arn:aws:sns:us-east-1:123456789012:orders".to_owned(),
            fair_queue_group.map(str::to_owned),
        );
        sink.preflight().await.unwrap();
        (sink, fake)
    }

    fn outbound(ordinal: u32, tenant: &str) -> Outbound {
        let mut envelope = sample_envelope(
            Op::Insert,
            Some(json!({"id": ordinal})),
            None,
            Some(json!({"id": ordinal, "tenant": tenant})),
        );
        envelope.id = EventId::new(Lsn(0x1A2B_3C4D), ordinal);
        Outbound::encode(&envelope, OrderKeyId::of(b"[1]"), &["tenant".to_owned()]).unwrap()
    }

    fn failure(id: &str, sender_fault: bool) -> EntryFailure {
        EntryFailure {
            id: id.to_owned(),
            code: "InvalidParameter".to_owned(),
            message: "bad".to_owned(),
            sender_fault,
        }
    }

    #[tokio::test]
    async fn preflight_reads_fifo_mode_and_the_message_size_limit() {
        for (attributes, max) in [
            (vec![], SNS_DEFAULT_MESSAGE_BYTES),
            (
                vec![("MaximumMessageSize", "1048576")],
                SNS_MAX_MESSAGE_BYTES,
            ),
            (
                vec![("MaximumMessageSize", "2000000")],
                SNS_MAX_MESSAGE_BYTES,
            ),
            (vec![("MaximumMessageSize", "0")], SNS_DEFAULT_MESSAGE_BYTES),
            (
                vec![("MaximumMessageSize", "big")],
                SNS_DEFAULT_MESSAGE_BYTES,
            ),
        ] {
            let (sink, _) = preflighted(
                FakeSns {
                    attributes: Some(topic(&attributes)),
                    ..FakeSns::default()
                },
                None,
            )
            .await;
            assert_eq!(
                sink.capabilities(),
                SinkCapabilities {
                    max_batch_messages: 10,
                    max_batch_bytes: max,
                    max_message_bytes: max,
                },
                "{attributes:?}"
            );
        }
        let (sink, _) = preflighted(
            FakeSns {
                attributes: Some(topic(&[("FifoTopic", "true")])),
                ..FakeSns::default()
            },
            None,
        )
        .await;
        assert_eq!(sink.topic.get().map(|topic| topic.fifo), Some(true));
    }

    #[tokio::test]
    async fn preflight_fails_when_the_topic_cannot_be_read() {
        let sink = SnsSink::with_api(
            Box::new(Arc::new(FakeSns::default())),
            "arn:aws:sns:us-east-1:123456789012:missing".to_owned(),
            None,
        );
        let error = sink.preflight().await.unwrap_err();
        assert!(
            error.0.contains("missing") && error.0.contains("NotFound"),
            "{error}"
        );
        let outcomes = sink.send(&[outbound(0, "acme")]).await;
        assert!(
            matches!(&outcomes[..], [Outcome::Retryable(_)]),
            "{outcomes:?}"
        );
    }

    #[tokio::test]
    async fn fifo_entries_use_the_order_key_as_group_and_the_event_id_as_dedupe_id() {
        let (sink, fake) = preflighted(
            FakeSns {
                attributes: Some(topic(&[("FifoTopic", "true")])),
                ..FakeSns::default()
            },
            Some("tenant"),
        )
        .await;
        let message = outbound(3, "acme");
        assert_eq!(
            sink.send(std::slice::from_ref(&message)).await,
            vec![Outcome::Delivered]
        );
        let calls = fake.calls.lock().unwrap();
        let entry = &calls[0][0];
        assert_eq!(entry.id, "0");
        assert_eq!(entry.message.as_bytes(), message.body.as_ref());
        assert_eq!(
            entry.group.as_deref(),
            Some(message.order_key.to_string().as_str())
        );
        assert_eq!(entry.dedupe.as_deref(), Some("000000001a2b3c4d-00000003"));
        assert_eq!(entry.attributes, message.attributes);
    }

    #[tokio::test]
    async fn standard_entries_take_a_valid_fair_queue_group_from_their_attributes() {
        let (sink, fake) = preflighted(
            FakeSns {
                attributes: Some(topic(&[])),
                ..FakeSns::default()
            },
            Some("tenant"),
        )
        .await;
        let batch = [
            outbound(0, "acme"),
            outbound(1, "has space"),
            outbound(2, &"x".repeat(129)),
            outbound(3, ""),
        ];
        assert_eq!(sink.send(&batch).await, vec![Outcome::Delivered; 4]);
        let groups: Vec<Option<String>> = fake.calls.lock().unwrap()[0]
            .iter()
            .map(|entry| entry.group.clone())
            .collect();
        assert_eq!(groups, vec![Some("acme".to_owned()), None, None, None]);
        assert!(
            fake.calls.lock().unwrap()[0]
                .iter()
                .all(|entry| entry.dedupe.is_none())
        );

        let (plain, fake) = preflighted(
            FakeSns {
                attributes: Some(topic(&[])),
                ..FakeSns::default()
            },
            None,
        )
        .await;
        plain.send(&batch[..1]).await;
        assert_eq!(fake.calls.lock().unwrap()[0][0].group, None);
    }

    #[tokio::test]
    async fn entry_results_map_by_sender_fault_and_missing_entries_are_retryable() {
        let fake = FakeSns {
            attributes: Some(topic(&[])),
            ..FakeSns::default()
        };
        fake.responses.lock().unwrap().push_back(Ok(BatchResult {
            successful: vec!["0".to_owned()],
            failed: vec![failure("1", true), failure("2", false)],
        }));
        let (sink, _) = preflighted(fake, None).await;
        let batch: Vec<Outbound> = (0..4).map(|ordinal| outbound(ordinal, "acme")).collect();
        let outcomes = sink.send(&batch).await;
        assert_eq!(outcomes[0], Outcome::Delivered);
        assert_eq!(
            outcomes[1],
            Outcome::Terminal(SinkError("InvalidParameter: bad".to_owned()))
        );
        assert_eq!(
            outcomes[2],
            Outcome::Retryable(SinkError("InvalidParameter: bad".to_owned()))
        );
        assert_eq!(
            outcomes[3],
            Outcome::Retryable(SinkError("SNS returned no result for the entry".to_owned()))
        );
    }

    #[tokio::test]
    async fn a_request_failure_makes_every_entry_retryable() {
        let fake = FakeSns {
            attributes: Some(topic(&[])),
            ..FakeSns::default()
        };
        fake.responses
            .lock()
            .unwrap()
            .push_back(Err(PublishFailure::Request(
                "Throttling: slow down".to_owned(),
            )));
        let (sink, _) = preflighted(fake, None).await;
        let outcomes = sink.send(&[outbound(0, "a"), outbound(1, "b")]).await;
        let retry = Outcome::Retryable(SinkError("Throttling: slow down".to_owned()));
        assert_eq!(outcomes, vec![retry.clone(), retry]);
    }

    #[tokio::test]
    async fn a_batch_rejected_as_too_long_is_resent_one_entry_at_a_time() {
        let fake = FakeSns {
            attributes: Some(topic(&[])),
            ..FakeSns::default()
        };
        {
            let mut responses = fake.responses.lock().unwrap();
            responses.push_back(Err(PublishFailure::TooLong(
                "BatchRequestTooLong".to_owned(),
            )));
            responses.push_back(Ok(BatchResult {
                successful: vec!["0".to_owned()],
                failed: Vec::new(),
            }));
            responses.push_back(Err(PublishFailure::TooLong(
                "BatchRequestTooLong".to_owned(),
            )));
        }
        let (sink, fake) = preflighted(fake, None).await;
        let outcomes = sink.send(&[outbound(0, "a"), outbound(1, "b")]).await;
        assert_eq!(
            outcomes,
            vec![
                Outcome::Delivered,
                Outcome::Terminal(SinkError("BatchRequestTooLong".to_owned()))
            ]
        );
        let ids: Vec<Vec<String>> = fake
            .calls
            .lock()
            .unwrap()
            .iter()
            .map(|call| call.iter().map(|entry| entry.id.clone()).collect())
            .collect();
        assert_eq!(ids, vec![vec!["0", "1"], vec!["0"], vec!["1"]]);
    }

    #[tokio::test]
    async fn a_body_that_is_not_utf8_is_terminal_and_the_rest_are_sent() {
        let (sink, fake) = preflighted(
            FakeSns {
                attributes: Some(topic(&[])),
                ..FakeSns::default()
            },
            None,
        )
        .await;
        let mut broken = outbound(0, "a");
        broken.body = Bytes::from_static(&[0xff, 0xfe]);
        let outcomes = sink.send(&[broken, outbound(1, "b")]).await;
        assert!(
            matches!(&outcomes[0], Outcome::Terminal(SinkError(message)) if message.contains("not UTF-8"))
        );
        assert_eq!(outcomes[1], Outcome::Delivered);
        assert_eq!(fake.calls.lock().unwrap()[0].len(), 1);
        assert_eq!(fake.calls.lock().unwrap()[0][0].id, "1");
    }
}
