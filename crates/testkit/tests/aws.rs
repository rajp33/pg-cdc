use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_sqs::types::QueueAttributeName;
use pg_cdc_testkit::AwsEmulator;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const BUCKET: &str = "pgcdc-conditional";
const KEY: &str = "leader.json";

fn status_of<T, E>(result: &Result<T, SdkError<E>>) -> Option<u16> {
    result
        .as_ref()
        .err()
        .and_then(SdkError::raw_response)
        .map(|response| response.status().as_u16())
}

#[tokio::test]
async fn s3_conditional_writes_are_honored() -> TestResult {
    let aws = AwsEmulator::start().await?;
    let s3 = aws.s3().await;
    s3.create_bucket().bucket(BUCKET).send().await?;
    let created = s3
        .put_object()
        .bucket(BUCKET)
        .key(KEY)
        .body(ByteStream::from_static(b"1"))
        .if_none_match("*")
        .send()
        .await?;
    let etag = created.e_tag().ok_or("missing etag")?.to_owned();
    let duplicate = s3
        .put_object()
        .bucket(BUCKET)
        .key(KEY)
        .body(ByteStream::from_static(b"2"))
        .if_none_match("*")
        .send()
        .await;
    assert_eq!(status_of(&duplicate), Some(412));
    let stale = s3
        .put_object()
        .bucket(BUCKET)
        .key(KEY)
        .body(ByteStream::from_static(b"3"))
        .if_match("\"00000000000000000000000000000000\"")
        .send()
        .await;
    assert_eq!(status_of(&stale), Some(412));
    s3.put_object()
        .bucket(BUCKET)
        .key(KEY)
        .body(ByteStream::from_static(b"4"))
        .if_match(etag)
        .send()
        .await?;
    let body = s3
        .get_object()
        .bucket(BUCKET)
        .key(KEY)
        .send()
        .await?
        .body
        .collect()
        .await?
        .into_bytes();
    assert_eq!(body.as_ref(), b"4");
    Ok(())
}

#[tokio::test]
async fn sns_delivers_to_sqs_subscription() -> TestResult {
    let aws = AwsEmulator::start().await?;
    let sns = aws.sns().await;
    let sqs = aws.sqs().await;
    let queue_url = sqs
        .create_queue()
        .queue_name("pgcdc-events")
        .send()
        .await?
        .queue_url()
        .ok_or("missing queue url")?
        .to_owned();
    let attributes = sqs
        .get_queue_attributes()
        .queue_url(&queue_url)
        .attribute_names(QueueAttributeName::QueueArn)
        .send()
        .await?;
    let queue_arn = attributes
        .attributes()
        .and_then(|map| map.get(&QueueAttributeName::QueueArn))
        .ok_or("missing queue arn")?
        .clone();
    let topic_arn = sns
        .create_topic()
        .name("pgcdc-events")
        .send()
        .await?
        .topic_arn()
        .ok_or("missing topic arn")?
        .to_owned();
    sns.subscribe()
        .topic_arn(&topic_arn)
        .protocol("sqs")
        .endpoint(&queue_arn)
        .attributes("RawMessageDelivery", "true")
        .send()
        .await?;
    sns.publish()
        .topic_arn(&topic_arn)
        .message(r#"{"op":"insert"}"#)
        .send()
        .await?;
    let received = sqs
        .receive_message()
        .queue_url(&queue_url)
        .wait_time_seconds(5)
        .max_number_of_messages(1)
        .send()
        .await?;
    let bodies: Vec<&str> = received
        .messages()
        .iter()
        .filter_map(|message| message.body())
        .collect();
    assert_eq!(bodies, vec![r#"{"op":"insert"}"#]);
    Ok(())
}
