#[path = "support/state_store.rs"]
mod state_store;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_smithy_http_client::tls::{Provider, rustls_provider::CryptoMode};
use pg_cdc::state::s3::S3StateStore;
use pg_cdc::state::{
    LeaderRecord, LeaderState, ParkStatus, ParkedEvent, StateStore, StoreError, SwapOutcome,
};
use pg_cdc_decoder::Lsn;
use pg_cdc_testkit::AwsEmulator;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

use state_store::{TestResult, event, id, key};

const BUCKET: &str = "pgcdc-state";

async fn emulator_with_bucket() -> TestResult<(AwsEmulator, aws_sdk_s3::Client)> {
    let aws = AwsEmulator::start().await?;
    let s3 = aws.s3().await;
    s3.create_bucket().bucket(BUCKET).send().await?;
    Ok((aws, s3))
}

#[tokio::test]
async fn the_s3_store_conforms() -> TestResult {
    let (_aws, s3) = emulator_with_bucket().await?;
    state_store::conforms(|| {
        let store: Arc<dyn StateStore> = Arc::new(S3StateStore::new(
            s3.clone(),
            BUCKET.to_owned(),
            "orders/",
            "orders",
            8,
        ));
        async move { Ok(store) }
    })
    .await
}

#[tokio::test]
async fn parked_objects_and_the_leader_record_use_the_spec_layout() -> TestResult {
    let (_aws, s3) = emulator_with_bucket().await?;
    let store = S3StateStore::new(s3.clone(), BUCKET.to_owned(), "team/", "orders", 8);
    let queue = key(0xab, 1)?;
    store
        .append_parked(&[event(queue, id(0x1A2B_3C4D, 3), ParkStatus::Retrying)])
        .await?;
    let record = LeaderRecord {
        epoch: 1,
        holder: "task-a".to_owned(),
        state: LeaderState::Active,
        confirmed_lsn: Lsn(0x10),
        updated_ms: 1,
    };
    assert!(matches!(
        store.swap_leader(&record, None).await?,
        SwapOutcome::Swapped(_)
    ));
    let listed = s3.list_objects_v2().bucket(BUCKET).send().await?;
    let mut keys: Vec<&str> = listed
        .contents()
        .iter()
        .filter_map(|object| object.key())
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec![
            "team/orders/leader.json".to_owned(),
            format!("team/orders/parked/{queue}/000000001a2b3c4d-00000003.json"),
        ]
    );
    let head = s3
        .head_object()
        .bucket(BUCKET)
        .key("team/orders/leader.json")
        .send()
        .await?;
    let Some(current) = store.read_leader().await? else {
        return Err("no leader record".into());
    };
    assert_eq!(head.e_tag(), Some(current.version.0.as_str()));
    Ok(())
}

#[tokio::test]
async fn heads_load_across_listing_pages() -> TestResult {
    let (_aws, s3) = emulator_with_bucket().await?;
    let store = S3StateStore::new(s3, BUCKET.to_owned(), "", "orders", 32);
    let busy = key(0x42, 1)?;
    let quiet = key(0x42, 2)?;
    let mut events: Vec<ParkedEvent> = (0..1020)
        .map(|n| event(busy, id(7, n), ParkStatus::Retrying))
        .collect();
    events.extend((0..30).map(|n| event(quiet, id(9, n), ParkStatus::Retrying)));
    store.append_parked(&events).await?;
    let heads = store.load_parked_heads(0x42).await?;
    let summary: Vec<_> = heads
        .iter()
        .map(|head| (head.order_key, head.head, head.length))
        .collect();
    assert_eq!(summary, vec![(busy, id(7, 0), 1020), (quiet, id(9, 0), 30)]);
    let tail = store.read_parked(&busy, Some(&id(7, 1015)), 10).await?;
    let ids: Vec<_> = tail.iter().map(|event| event.id).collect();
    assert_eq!(ids, (1016..1020).map(|n| id(7, n)).collect::<Vec<_>>());
    Ok(())
}

#[tokio::test]
async fn a_missing_bucket_reports_the_store_unavailable() -> TestResult {
    let aws = AwsEmulator::start().await?;
    let store = S3StateStore::new(aws.s3().await, "missing".to_owned(), "", "orders", 4);
    let result = store.load_parked_heads(0).await;
    assert!(
        matches!(&result, Err(StoreError::Unavailable(message)) if message.contains("NoSuchBucket")),
        "{result:?}"
    );
    let leader = store.read_leader().await;
    assert!(
        matches!(&leader, Err(StoreError::Unavailable(message)) if message.contains("NoSuchBucket")),
        "{leader:?}"
    );
    let removed = store.remove_parked(&[(key(0x01, 1)?, id(1, 0))]).await;
    assert!(
        matches!(removed, Err(StoreError::Unavailable(_))),
        "{removed:?}"
    );
    Ok(())
}

async fn serve_slowly(listener: TcpListener, active: Arc<AtomicUsize>, peak: Arc<AtomicUsize>) {
    while let Ok((socket, _)) = listener.accept().await {
        let (active, peak) = (Arc::clone(&active), Arc::clone(&peak));
        tokio::spawn(async move {
            let (reader, mut writer) = socket.into_split();
            let mut reader = BufReader::new(reader);
            loop {
                let mut length = 0;
                let mut method = String::new();
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).await.unwrap_or(0) == 0 {
                        return;
                    }
                    if method.is_empty() {
                        method = line.split(' ').next().unwrap_or_default().to_owned();
                    }
                    if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        length = value.trim().parse().unwrap_or(0);
                    }
                    if line == "\r\n" {
                        break;
                    }
                }
                let mut body = vec![0; length];
                if reader.read_exact(&mut body).await.is_err() {
                    return;
                }
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(20)).await;
                active.fetch_sub(1, Ordering::SeqCst);
                let response = if method == "DELETE" {
                    "HTTP/1.1 204 No Content\r\n\r\n"
                } else {
                    "HTTP/1.1 200 OK\r\nETag: \"e\"\r\nContent-Length: 0\r\n\r\n"
                };
                if writer.write_all(response.as_bytes()).await.is_err() {
                    return;
                }
            }
        });
    }
}

#[tokio::test]
async fn s3_requests_never_exceed_max_concurrency() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    tokio::spawn(serve_slowly(
        listener,
        Arc::clone(&active),
        Arc::clone(&peak),
    ));
    let http_client = aws_smithy_http_client::Builder::new()
        .tls_provider(Provider::Rustls(CryptoMode::Ring))
        .build_https();
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new("test", "test", None, None, "test"))
        .endpoint_url(endpoint)
        .force_path_style(true)
        .http_client(http_client)
        .build();
    let store = S3StateStore::new(
        aws_sdk_s3::Client::from_conf(config),
        BUCKET.to_owned(),
        "",
        "orders",
        4,
    );
    let queue = key(0x01, 1)?;
    let events: Vec<ParkedEvent> = (0..64)
        .map(|n| event(queue, id(1, n), ParkStatus::Retrying))
        .collect();
    store.append_parked(&events).await?;
    let removals: Vec<_> = events.iter().map(|event| (queue, event.id)).collect();
    store.remove_parked(&removals).await?;
    let peak = peak.load(Ordering::SeqCst);
    assert!(peak <= 4, "{peak} requests were in flight at once");
    assert!(peak >= 2, "the fan-out never ran in parallel");
    Ok(())
}
