use pg_cdc::event::EventId;
use pg_cdc::payload::{PayloadRef, PayloadStore, S3PayloadStore};
use pg_cdc::state::StoreError;
use pg_cdc_decoder::Lsn;
use pg_cdc_testkit::AwsEmulator;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const BUCKET: &str = "pgcdc-payloads";

#[tokio::test]
async fn payloads_are_written_where_their_reference_points() -> TestResult {
    let aws = AwsEmulator::start().await?;
    let s3 = aws.s3().await;
    s3.create_bucket().bucket(BUCKET).send().await?;
    let store = S3PayloadStore::new(s3.clone(), BUCKET.to_owned(), "orders/", "orders");
    let id = EventId::new(Lsn(0x1A2B_3C4D), 7);
    let body = format!(
        r#"{{"id":"{id}","after":{{"blob":"{}"}}}}"#,
        "x".repeat(300_000)
    );
    let reference = store.put(&id, body.as_bytes()).await?;
    assert_eq!(
        reference,
        PayloadRef {
            uri: "s3://pgcdc-payloads/orders/orders/payloads/000000001a2b3c4d-00000007.json"
                .to_owned(),
            bytes: u64::try_from(body.len())?,
        }
    );
    let replayed = store.put(&id, body.as_bytes()).await?;
    assert_eq!(replayed, reference);
    let object = s3
        .get_object()
        .bucket(BUCKET)
        .key("orders/orders/payloads/000000001a2b3c4d-00000007.json")
        .send()
        .await?;
    assert_eq!(object.content_type(), Some("application/json"));
    let stored = object.body.collect().await?.into_bytes();
    assert_eq!(stored.as_ref(), body.as_bytes());
    Ok(())
}

#[tokio::test]
async fn a_missing_bucket_reports_the_payload_store_unavailable() -> TestResult {
    let aws = AwsEmulator::start().await?;
    let store = S3PayloadStore::new(aws.s3().await, "missing".to_owned(), "", "orders");
    let result = store.put(&EventId::new(Lsn(1), 0), b"{}").await;
    assert!(
        matches!(&result, Err(StoreError::Unavailable(message)) if message.contains("NoSuchBucket")),
        "{result:?}"
    );
    Ok(())
}
