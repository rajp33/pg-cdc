#[path = "support/state_store.rs"]
mod state_store;

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_smithy_http_client::tls::{Provider, rustls_provider::CryptoMode};
use pg_cdc::event::EventId;
use pg_cdc::order::OrderKeyId;
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
    let store = S3StateStore::new(
        aws.s3().await,
        "missing".to_owned(),
        "",
        "orders",
        usize::MAX,
    );
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

#[derive(Debug, Default)]
struct FakeS3 {
    failing: Option<String>,
    active: AtomicUsize,
    peak: AtomicUsize,
    per_key: Mutex<HashMap<String, (usize, usize)>>,
    stored: Mutex<Vec<String>>,
}

impl FakeS3 {
    fn peak_for(&self, queue: OrderKeyId) -> usize {
        self.per_key
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&queue.to_string())
            .map_or(0, |(_, peak)| *peak)
    }

    fn stored(&self, queue: OrderKeyId) -> Vec<EventId> {
        let prefix = format!("/{BUCKET}/orders/parked/{queue}/");
        let mut ids: Vec<EventId> = self
            .stored
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter_map(|path| {
                path.strip_prefix(&prefix)?
                    .strip_suffix(".json")?
                    .parse()
                    .ok()
            })
            .collect();
        ids.sort_unstable();
        ids
    }

    fn enter(&self, path: &str) -> Option<String> {
        let now = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        let queue = path.split('/').nth(4).map(str::to_owned)?;
        let mut per_key = self.per_key.lock().unwrap_or_else(PoisonError::into_inner);
        let (active, peak) = per_key.entry(queue.clone()).or_default();
        *active += 1;
        *peak = (*peak).max(*active);
        Some(queue)
    }

    fn leave(&self, queue: Option<String>) {
        self.active.fetch_sub(1, Ordering::SeqCst);
        if let Some(queue) = queue
            && let Some((active, _)) = self
                .per_key
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get_mut(&queue)
        {
            *active -= 1;
        }
    }

    async fn answer(&self, method: &str, path: &str) -> &'static str {
        let queue = self.enter(path);
        tokio::time::sleep(Duration::from_millis(20)).await;
        self.leave(queue);
        if method == "DELETE" {
            return "HTTP/1.1 204 No Content\r\n\r\n";
        }
        if method == "PUT"
            && self
                .failing
                .as_deref()
                .is_some_and(|failing| path.contains(failing))
        {
            return "HTTP/1.1 503 Slow Down\r\nContent-Type: application/xml\r\nContent-Length: 59\r\n\r\n<Error><Code>SlowDown</Code><Message>slow</Message></Error>";
        }
        if method == "PUT" {
            self.stored
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(path.to_owned());
        }
        "HTTP/1.1 200 OK\r\nETag: \"e\"\r\nContent-Length: 0\r\n\r\n"
    }
}

async fn serve(listener: TcpListener, fake: Arc<FakeS3>) {
    while let Ok((socket, _)) = listener.accept().await {
        let fake = Arc::clone(&fake);
        tokio::spawn(async move {
            let (reader, mut writer) = socket.into_split();
            let mut reader = BufReader::new(reader);
            loop {
                let mut length = 0;
                let mut request_line = String::new();
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).await.unwrap_or(0) == 0 {
                        return;
                    }
                    if request_line.is_empty() {
                        request_line = line.clone();
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
                let mut parts = request_line.split(' ');
                let method = parts.next().unwrap_or_default();
                let path = parts.next().unwrap_or_default();
                let path = path.split('?').next().unwrap_or_default();
                let response = fake.answer(method, path).await;
                if writer.write_all(response.as_bytes()).await.is_err() {
                    return;
                }
            }
        });
    }
}

async fn fake_store(fake: &Arc<FakeS3>, max_concurrency: usize) -> TestResult<S3StateStore> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    tokio::spawn(serve(listener, Arc::clone(fake)));
    let http_client = aws_smithy_http_client::Builder::new()
        .tls_provider(Provider::Rustls(CryptoMode::Ring))
        .build_https();
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new("test", "test", None, None, "test"))
        .endpoint_url(endpoint)
        .force_path_style(true)
        .retry_config(
            RetryConfig::standard()
                .with_max_attempts(3)
                .with_initial_backoff(Duration::from_millis(10)),
        )
        .http_client(http_client)
        .build();
    Ok(S3StateStore::new(
        aws_sdk_s3::Client::from_conf(config),
        BUCKET.to_owned(),
        "",
        "orders",
        max_concurrency,
    ))
}

#[tokio::test]
async fn s3_requests_never_exceed_max_concurrency() -> TestResult {
    let fake = Arc::new(FakeS3::default());
    let store = fake_store(&fake, 4).await?;
    let events: Vec<ParkedEvent> = (0..64_u32)
        .map(|n| {
            let queue = key(0x01, u8::try_from(n % 8)?)?;
            Ok(event(queue, id(1, n), ParkStatus::Retrying))
        })
        .collect::<TestResult<_>>()?;
    store.append_parked(&events).await?;
    let removals: Vec<_> = events
        .iter()
        .map(|event| (event.order_key, event.id))
        .collect();
    store.remove_parked(&removals).await?;
    let peak = fake.peak.load(Ordering::SeqCst);
    assert!(peak <= 4, "{peak} requests were in flight at once");
    assert!(peak >= 2, "the fan-out never ran in parallel");
    Ok(())
}

#[tokio::test]
async fn a_failed_put_leaves_each_order_key_with_a_prefix_of_its_events() -> TestResult {
    let failing = key(0x02, 1)?;
    let healthy = key(0x02, 2)?;
    let fake = Arc::new(FakeS3 {
        failing: Some(format!("/{failing}/{}.json", id(5, 3))),
        ..FakeS3::default()
    });
    let store = fake_store(&fake, 8).await?;
    let mut events: Vec<ParkedEvent> = (0..8)
        .map(|n| event(failing, id(5, n), ParkStatus::Retrying))
        .chain((0..8).map(|n| event(healthy, id(6, n), ParkStatus::Retrying)))
        .collect();
    events.reverse();
    let result = store.append_parked(&events).await;
    assert!(
        matches!(&result, Err(StoreError::Unavailable(message)) if message.contains("SlowDown")),
        "{result:?}"
    );
    assert_eq!(
        fake.stored(failing),
        (0..3).map(|n| id(5, n)).collect::<Vec<_>>()
    );
    assert_eq!(
        fake.stored(healthy),
        (0..8).map(|n| id(6, n)).collect::<Vec<_>>()
    );
    assert_eq!(
        fake.peak_for(failing),
        1,
        "one key's events were written at once"
    );
    assert_eq!(
        fake.peak_for(healthy),
        1,
        "one key's events were written at once"
    );
    assert!(
        fake.peak.load(Ordering::SeqCst) >= 2,
        "keys were not written in parallel"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_malformed_event_fails_the_append_before_any_write() -> TestResult {
    let fake = Arc::new(FakeS3::default());
    let store = fake_store(&fake, 8).await?;
    let queue = key(0x03, 1)?;
    let mut malformed = event(queue, id(9, 9), ParkStatus::Retrying);
    malformed.body = bytes::Bytes::from_static(b"not json");
    let events: Vec<ParkedEvent> = (0..2000)
        .map(|n| event(queue, id(9, n), ParkStatus::Retrying))
        .chain([malformed])
        .collect();
    let result = store.append_parked(&events).await;
    assert!(
        matches!(result, Err(StoreError::Malformed { .. })),
        "{result:?}"
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(fake.stored(queue), vec![]);
    assert_eq!(fake.peak.load(Ordering::SeqCst), 0);
    Ok(())
}
