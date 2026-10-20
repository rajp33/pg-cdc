use std::time::Duration;

use aws_sdk_sqs::types::{MessageAttributeValue, MessageSystemAttributeName, QueueAttributeName};
use pg_cdc::event::{ENVELOPE_VERSION, Envelope, EventId, Op, Source};
use pg_cdc::order::{OrderBy, order_key};
use pg_cdc::sink::sns::SnsSink;
use pg_cdc::sink::{Outbound, Outcome, Sink, SinkCapabilities};
use pg_cdc_decoder::Lsn;
use pg_cdc_testkit::{AwsEmulator, TestkitError};
use serde_json::json;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const WAIT: Duration = Duration::from_secs(30);

fn outbound(ordinal: u32, tenant: &str) -> Result<Outbound, serde_json::Error> {
    encode(&envelope(ordinal, tenant))
}

fn encode(envelope: &Envelope) -> Result<Outbound, serde_json::Error> {
    Outbound::encode(
        envelope,
        order_key(&OrderBy::Key, envelope),
        &["tenant".to_owned()],
    )
}

fn envelope(ordinal: u32, tenant: &str) -> Envelope {
    let row = json!({"id": ordinal, "tenant": tenant});
    Envelope {
        id: EventId::new(Lsn(0x1A2B_3C4D), ordinal),
        version: ENVELOPE_VERSION,
        op: Op::Insert,
        schema: "public".to_owned(),
        table: "orders".to_owned(),
        key: json!({"id": ordinal}).as_object().cloned(),
        before: None,
        after: row.as_object().cloned(),
        unchanged_toast: Vec::new(),
        changed: None,
        source: Source {
            pipeline: "orders".to_owned(),
            commit_lsn: "0/1A2B3C4D".to_owned(),
            commit_ts: "2026-10-14T12:00:00.123456+00:00".to_owned(),
        },
    }
}

#[tokio::test]
async fn preflight_reads_the_topic_and_rejects_a_missing_one() -> TestResult {
    let aws = AwsEmulator::start().await?;
    let standard = aws.topic_with_queue("orders", false).await?;
    let sink = SnsSink::new(aws.sns().await, "orders", standard.topic_arn, None);
    sink.preflight().await?;
    assert_eq!(
        sink.capabilities(),
        SinkCapabilities {
            max_batch_messages: 10,
            max_batch_bytes: 256 * 1024,
            max_message_bytes: 256 * 1024,
        }
    );
    let missing = SnsSink::new(
        aws.sns().await,
        "orders",
        "arn:aws:sns:us-east-1:123456789012:missing".to_owned(),
        None,
    );
    let error = missing.preflight().await.err().ok_or("preflight passed")?;
    assert!(error.0.contains("NotFound"), "{error}");
    Ok(())
}

#[tokio::test]
async fn a_standard_topic_delivers_bodies_and_attributes_to_sqs() -> TestResult {
    let aws = AwsEmulator::start().await?;
    let route = aws.topic_with_queue("orders", false).await?;
    let sink = SnsSink::new(aws.sns().await, "orders", route.topic_arn, None);
    sink.preflight().await?;
    let batch = vec![
        outbound(1, "acme")?,
        outbound(2, "globex")?,
        outbound(3, "initech")?,
    ];
    assert_eq!(sink.send(&batch).await, vec![Outcome::Delivered; 3]);
    let messages = aws.receive(&route.queue_url, 3, WAIT).await?;
    let mut bodies: Vec<&str> = messages
        .iter()
        .filter_map(|message| message.body())
        .collect();
    bodies.sort_unstable();
    let mut expected: Vec<&str> = batch
        .iter()
        .map(|outbound| std::str::from_utf8(&outbound.body))
        .collect::<Result<_, _>>()?;
    expected.sort_unstable();
    assert_eq!(bodies, expected);
    for message in &messages {
        let attributes = message.message_attributes().ok_or("no attributes")?;
        let text = |name: &str| {
            attributes
                .get(name)
                .and_then(|value| value.string_value())
                .map(str::to_owned)
        };
        assert_eq!(text("schema").as_deref(), Some("public"));
        assert_eq!(text("table").as_deref(), Some("orders"));
        assert_eq!(text("op").as_deref(), Some("insert"));
        let body: serde_json::Value = serde_json::from_str(message.body().ok_or("no body")?)?;
        assert_eq!(text("tenant").as_deref(), body["after"]["tenant"].as_str());
        assert_eq!(
            attributes
                .get("tenant")
                .map(MessageAttributeValue::data_type),
            Some("String")
        );
    }
    Ok(())
}

async fn filtered_queue(
    aws: &AwsEmulator,
    topic_arn: &str,
    name: &str,
    body_filter: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let sqs = aws.sqs().await;
    let queue_url = sqs
        .create_queue()
        .queue_name(name)
        .send()
        .await?
        .queue_url()
        .ok_or("no queue URL")?
        .to_owned();
    let queue_arn = sqs
        .get_queue_attributes()
        .queue_url(&queue_url)
        .attribute_names(QueueAttributeName::QueueArn)
        .send()
        .await?
        .attributes()
        .and_then(|attributes| attributes.get(&QueueAttributeName::QueueArn))
        .ok_or("no queue ARN")?
        .clone();
    aws.sns()
        .await
        .subscribe()
        .topic_arn(topic_arn)
        .protocol("sqs")
        .endpoint(queue_arn)
        .attributes("RawMessageDelivery", "true")
        .attributes("FilterPolicyScope", "MessageBody")
        .attributes("FilterPolicy", body_filter)
        .send()
        .await?;
    Ok(queue_url)
}

#[tokio::test]
async fn a_body_filter_on_changed_selects_updates_that_touched_the_column() -> TestResult {
    let aws = AwsEmulator::start().await?;
    let route = aws.topic_with_queue("orders", false).await?;
    let revisions = filtered_queue(
        &aws,
        &route.topic_arn,
        "revisions",
        r#"{"changed":["revision"]}"#,
    )
    .await?;
    let sink = SnsSink::new(aws.sns().await, "orders", route.topic_arn, None);
    sink.preflight().await?;
    let update = |ordinal: u32, changed: &[&str]| {
        let mut envelope = envelope(ordinal, "acme");
        envelope.op = Op::Update;
        envelope.changed = Some(changed.iter().map(|&name| name.to_owned()).collect());
        encode(&envelope)
    };
    let batch = vec![
        update(1, &["status", "revision"])?,
        update(2, &["status"])?,
        update(3, &[])?,
        outbound(4, "acme")?,
    ];
    assert_eq!(sink.send(&batch).await, vec![Outcome::Delivered; 4]);
    aws.receive(&route.queue_url, 4, WAIT).await?;
    let matched = aws.receive(&revisions, 1, WAIT).await?;
    let bodies: Vec<serde_json::Value> = matched
        .iter()
        .map(|message| serde_json::from_str(message.body().unwrap_or_default()))
        .collect::<Result<_, _>>()?;
    assert_eq!(
        bodies.iter().map(|body| &body["id"]).collect::<Vec<_>>(),
        [&json!(batch[0].id.to_string())]
    );
    let more = aws.receive(&revisions, 1, Duration::from_secs(2)).await;
    assert!(matches!(more, Err(TestkitError::Timeout(_))), "{more:?}");
    Ok(())
}

#[tokio::test]
async fn a_fifo_topic_groups_by_order_key_and_deduplicates_by_event_id() -> TestResult {
    let aws = AwsEmulator::start().await?;
    let route = aws.topic_with_queue("orders", true).await?;
    let sink = SnsSink::new(aws.sns().await, "orders", route.topic_arn, None);
    sink.preflight().await?;
    let batch = vec![outbound(1, "acme")?, outbound(2, "acme")?];
    assert_eq!(sink.send(&batch).await, vec![Outcome::Delivered; 2]);
    let messages = aws.receive(&route.queue_url, 2, WAIT).await?;
    for message in &messages {
        let system = message.attributes().ok_or("no system attributes")?;
        let body: serde_json::Value = serde_json::from_str(message.body().ok_or("no body")?)?;
        let sent = batch
            .iter()
            .find(|outbound| body["id"] == outbound.id.to_string())
            .ok_or("unknown message")?;
        assert_eq!(
            system.get(&MessageSystemAttributeName::MessageGroupId),
            Some(&sent.order_key.to_string())
        );
        assert_eq!(
            system.get(&MessageSystemAttributeName::MessageDeduplicationId),
            Some(&format!("orders:{}", sent.id))
        );
    }
    Ok(())
}

#[tokio::test]
async fn two_pipelines_sharing_a_fifo_topic_both_deliver_the_same_event_id() -> TestResult {
    let aws = AwsEmulator::start().await?;
    let route = aws.topic_with_queue("shared", true).await?;
    for pipeline in ["orders", "billing"] {
        let sink = SnsSink::new(aws.sns().await, pipeline, route.topic_arn.clone(), None);
        sink.preflight().await?;
        assert_eq!(
            sink.send(&[outbound(1, pipeline)?]).await,
            vec![Outcome::Delivered]
        );
    }
    let messages = aws.receive(&route.queue_url, 2, WAIT).await?;
    let mut tenants: Vec<String> = messages
        .iter()
        .filter_map(|message| message.body())
        .map(serde_json::from_str::<serde_json::Value>)
        .map(|body| body.map(|body| body["after"]["tenant"].to_string()))
        .collect::<Result<_, _>>()?;
    tenants.sort_unstable();
    assert_eq!(tenants, vec![r#""billing""#, r#""orders""#]);
    Ok(())
}

#[tokio::test]
async fn a_deleted_topic_makes_every_entry_retryable() -> TestResult {
    let aws = AwsEmulator::start().await?;
    let route = aws.topic_with_queue("orders", false).await?;
    let sink = SnsSink::new(aws.sns().await, "orders", route.topic_arn.clone(), None);
    sink.preflight().await?;
    aws.sns()
        .await
        .delete_topic()
        .topic_arn(&route.topic_arn)
        .send()
        .await?;
    let outcomes = sink
        .send(&[outbound(1, "acme")?, outbound(2, "acme")?])
        .await;
    assert_eq!(outcomes.len(), 2);
    for outcome in outcomes {
        assert!(
            matches!(&outcome, Outcome::Retryable(error) if error.0.contains("NotFound")),
            "{outcome:?}"
        );
    }
    Ok(())
}
