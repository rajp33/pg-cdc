#[path = "support/state_store.rs"]
mod state_store;

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use aws_sdk_s3::config::interceptors::{
    BeforeTransmitInterceptorContextMut, BeforeTransmitInterceptorContextRef,
};
use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::config::{
    BehaviorVersion, ConfigBag, Credentials, Intercept, Region, RuntimeComponents,
};
use aws_sdk_s3::error::BoxError;
use aws_smithy_http_client::tls::{Provider, rustls_provider::CryptoMode};
use pg_cdc::event::EventId;
use pg_cdc::order::OrderKeyId;
use pg_cdc::state::s3::S3StateStore;
use pg_cdc::state::{
    LeaderRecord, LeaderState, ParkStatus, ParkedEvent, ParkedHead, ParkedQueues, StateStore,
    StoreError, SwapOutcome,
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

#[derive(Debug, Default)]
struct Requests {
    listings: AtomicUsize,
    gets: AtomicUsize,
    deletes: AtomicUsize,
    batch_deletes: AtomicUsize,
}

#[derive(Debug)]
struct CountRequests(Arc<Requests>);

impl Intercept for CountRequests {
    fn name(&self) -> &'static str {
        "CountRequests"
    }

    fn read_before_transmit(
        &self,
        context: &BeforeTransmitInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let request = context.request();
        let counter = match request.method() {
            "GET" if request.uri().contains("list-type=2") => &self.0.listings,
            "GET" => &self.0.gets,
            "DELETE" => &self.0.deletes,
            "POST" if request.uri().contains("delete") => &self.0.batch_deletes,
            _ => return Ok(()),
        };
        counter.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn listing_queues_costs_one_page_per_queue_whatever_the_backlog() -> TestResult {
    let (_aws, s3) = emulator_with_bucket().await?;
    let seeding = S3StateStore::new(s3.clone(), BUCKET.to_owned(), "", "orders", 32);
    let busy = key(0x42, 1)?;
    let quiet: Vec<OrderKeyId> = (2..=5).map(|n| key(0x42, n)).collect::<TestResult<_>>()?;
    let elsewhere = key(0x43, 1)?;
    let mut events: Vec<ParkedEvent> = (0..2500)
        .map(|n| event(busy, id(7, n), ParkStatus::Retrying))
        .collect();
    events.extend(
        quiet
            .iter()
            .chain([&elsewhere])
            .map(|queue| event(*queue, id(9, 0), ParkStatus::Retrying)),
    );
    seeding.append_parked(&events).await?;

    let requests = Arc::new(Requests::default());
    let counting = aws_sdk_s3::Client::from_conf(
        s3.config()
            .to_builder()
            .interceptor(CountRequests(Arc::clone(&requests)))
            .build(),
    );
    let store = S3StateStore::new(counting, BUCKET.to_owned(), "", "orders", 32);

    let first = store.list_parked_queues(None, 3, 1000).await?;
    let head = |order_key, head, length| ParkedHead {
        order_key,
        head,
        length,
    };
    assert_eq!(
        first,
        ParkedQueues {
            queues: vec![
                head(busy, id(7, 0), 1000),
                head(quiet[0], id(9, 0), 1),
                head(quiet[1], id(9, 0), 1),
            ],
            next: Some(quiet[1]),
        }
    );
    assert!(requests.listings.load(Ordering::SeqCst) <= 4);
    assert_eq!(requests.gets.load(Ordering::SeqCst), 0);

    let rest = store.list_parked_queues(Some(&quiet[1]), 3, 1000).await?;
    assert_eq!(
        rest,
        ParkedQueues {
            queues: vec![
                head(quiet[2], id(9, 0), 1),
                head(quiet[3], id(9, 0), 1),
                head(elsewhere, id(9, 0), 1),
            ],
            next: None,
        }
    );
    assert_eq!(requests.gets.load(Ordering::SeqCst), 0);
    Ok(())
}

fn counting(s3: &aws_sdk_s3::Client, requests: &Arc<Requests>) -> aws_sdk_s3::Client {
    aws_sdk_s3::Client::from_conf(
        s3.config()
            .to_builder()
            .interceptor(CountRequests(Arc::clone(requests)))
            .build(),
    )
}

#[tokio::test]
async fn removal_deletes_up_to_a_thousand_objects_per_request() -> TestResult {
    let (_aws, s3) = emulator_with_bucket().await?;
    let seeding = S3StateStore::new(s3.clone(), BUCKET.to_owned(), "", "orders", 64);
    let keys: Vec<OrderKeyId> = (1..=5).map(|n| key(0x42, n)).collect::<TestResult<_>>()?;
    let events: Vec<ParkedEvent> = keys
        .iter()
        .cycle()
        .zip(0..2500)
        .map(|(queue, n)| event(*queue, id(7, n), ParkStatus::Retrying))
        .collect();
    seeding.append_parked(&events).await?;
    let removals: Vec<(OrderKeyId, EventId)> = events
        .iter()
        .map(|event| (event.order_key, event.id))
        .collect();

    let requests = Arc::new(Requests::default());
    let store = S3StateStore::new(
        counting(&s3, &requests),
        BUCKET.to_owned(),
        "",
        "orders",
        64,
    );
    store.remove_parked(&removals).await?;
    assert_eq!(requests.batch_deletes.load(Ordering::SeqCst), 3);
    assert_eq!(requests.deletes.load(Ordering::SeqCst), 0);
    let left = s3
        .list_objects_v2()
        .bucket(BUCKET)
        .prefix("orders/parked/")
        .send()
        .await?;
    assert_eq!(left.key_count(), Some(0));

    store.remove_parked(&removals[..10]).await?;
    assert_eq!(requests.batch_deletes.load(Ordering::SeqCst), 4);
    Ok(())
}

#[derive(Debug)]
struct DrainWhileListed(Vec<String>);

impl Intercept for DrainWhileListed {
    fn name(&self) -> &'static str {
        "DrainWhileListed"
    }

    fn modify_before_signing(
        &self,
        context: &mut BeforeTransmitInterceptorContextMut<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let request = context.request_mut();
        let uri = request.uri();
        if uri.contains("list-type=2")
            && !uri.contains("delimiter=")
            && let Some(key) = self.0.iter().find(|key| uri.contains(key.as_str()))
        {
            let hidden = uri.replace(key.as_str(), "drained");
            request.set_uri(hidden)?;
        }
        Ok(())
    }
}

#[tokio::test]
async fn a_queue_draining_while_listed_does_not_end_the_listing() -> TestResult {
    let (_aws, s3) = emulator_with_bucket().await?;
    let seeding = S3StateStore::new(s3.clone(), BUCKET.to_owned(), "", "orders", 32);
    let keys: Vec<OrderKeyId> = (1..=3).map(|n| key(0x42, n)).collect::<TestResult<_>>()?;
    let events: Vec<ParkedEvent> = keys
        .iter()
        .map(|queue| event(*queue, id(9, 0), ParkStatus::Dead))
        .collect();
    seeding.append_parked(&events).await?;

    let draining = aws_sdk_s3::Client::from_conf(
        s3.config()
            .to_builder()
            .interceptor(DrainWhileListed(vec![keys[0].to_string()]))
            .build(),
    );
    let store = S3StateStore::new(draining, BUCKET.to_owned(), "", "orders", 32);
    let head = |order_key| ParkedHead {
        order_key,
        head: id(9, 0),
        length: 1,
    };
    let page = |queues, next| ParkedQueues { queues, next };

    assert_eq!(
        store.list_parked_queues(None, 1, 1000).await?,
        page(vec![], Some(keys[0]))
    );
    assert_eq!(
        store.list_parked_queues(Some(&keys[0]), 1, 1000).await?,
        page(vec![head(keys[1])], Some(keys[1]))
    );
    assert_eq!(
        store.list_parked_queues(None, 2, 1000).await?,
        page(vec![head(keys[1])], Some(keys[1]))
    );
    assert_eq!(
        store.list_parked_queues(Some(&keys[1]), 2, 1000).await?,
        page(vec![head(keys[2])], None)
    );
    Ok(())
}

#[tokio::test]
async fn queues_draining_while_listed_cost_no_more_than_the_limit() -> TestResult {
    let (_aws, s3) = emulator_with_bucket().await?;
    let seeding = S3StateStore::new(s3.clone(), BUCKET.to_owned(), "", "orders", 32);
    let keys: Vec<OrderKeyId> = (1..=6).map(|n| key(0x42, n)).collect::<TestResult<_>>()?;
    let events: Vec<ParkedEvent> = keys
        .iter()
        .map(|queue| event(*queue, id(9, 0), ParkStatus::Dead))
        .collect();
    seeding.append_parked(&events).await?;

    let requests = Arc::new(Requests::default());
    let draining = aws_sdk_s3::Client::from_conf(
        s3.config()
            .to_builder()
            .interceptor(DrainWhileListed(
                keys[..5].iter().map(ToString::to_string).collect(),
            ))
            .interceptor(CountRequests(Arc::clone(&requests)))
            .build(),
    );
    let store = S3StateStore::new(draining, BUCKET.to_owned(), "", "orders", 32);

    let mut after = None;
    let mut pages = Vec::new();
    loop {
        let before = requests.listings.load(Ordering::SeqCst);
        let listed = store.list_parked_queues(after.as_ref(), 1, 1000).await?;
        let listings = requests.listings.load(Ordering::SeqCst) - before;
        assert!(listings <= 2, "limit 1 made {listings} listings");
        after = listed.next;
        pages.push(listed.queues);
        if after.is_none() {
            break;
        }
    }
    let mut expected = vec![vec![]; 5];
    expected.push(vec![ParkedHead {
        order_key: keys[5],
        head: id(9, 0),
        length: 1,
    }]);
    assert_eq!(pages, expected);
    assert_eq!(requests.gets.load(Ordering::SeqCst), 0);
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

const DELETED: &str = "HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nContent-Length: 77\r\n\r\n<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"></DeleteResult>";
const DELETE_FAILED: &str = "HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nContent-Length: 177\r\n\r\n<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Error><Key>orders/parked/x.json</Key><Code>InternalError</Code><Message>try again</Message></Error></DeleteResult>";

#[derive(Debug, Default)]
struct FakeS3 {
    failing: Option<String>,
    stall: Option<String>,
    failing_deletes: AtomicUsize,
    batch_deletes: AtomicUsize,
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
        if method == "POST" {
            self.batch_deletes.fetch_add(1, Ordering::SeqCst);
            let failing = self
                .failing_deletes
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                    left.checked_sub(1)
                })
                .is_ok();
            return if failing { DELETE_FAILED } else { DELETED };
        }
        if method == "DELETE" {
            return "HTTP/1.1 204 No Content\r\n\r\n";
        }
        if method == "GET" && path.ends_with("/leader.json") {
            return "HTTP/1.1 404 Not Found\r\nContent-Type: application/xml\r\nContent-Length: 60\r\n\r\n<Error><Code>NoSuchKey</Code><Message>none</Message></Error>";
        }
        if method == "PUT"
            && self
                .stall
                .as_deref()
                .is_some_and(|stall| path.contains(stall))
        {
            tokio::time::sleep(Duration::from_secs(30)).await;
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
async fn a_per_object_delete_error_fails_the_removal_until_a_retry_succeeds() -> TestResult {
    let fake = Arc::new(FakeS3 {
        failing_deletes: AtomicUsize::new(1),
        ..FakeS3::default()
    });
    let store = fake_store(&fake, 4).await?;
    let removals = [(key(0x05, 1)?, id(1, 0)), (key(0x05, 2)?, id(1, 1))];
    let failed = store.remove_parked(&removals).await;
    assert!(
        matches!(&failed, Err(StoreError::Unavailable(message)) if message.contains("InternalError")),
        "{failed:?}"
    );
    store.remove_parked(&removals).await?;
    assert_eq!(fake.batch_deletes.load(Ordering::SeqCst), 2);
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

#[tokio::test]
async fn leader_calls_do_not_wait_behind_parking_traffic() -> TestResult {
    let parked = key(0x04, 1)?;
    let other = key(0x04, 2)?;
    let fake = Arc::new(FakeS3 {
        stall: Some(format!("/{parked}/")),
        ..FakeS3::default()
    });
    let store = Arc::new(fake_store(&fake, 1).await?);
    let appending = {
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            store
                .append_parked(&[event(parked, id(3, 0), ParkStatus::Retrying)])
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    let record = LeaderRecord {
        epoch: 1,
        holder: "task-a".to_owned(),
        state: LeaderState::Active,
        confirmed_lsn: Lsn(0x10),
        updated_ms: 1,
    };
    let read = tokio::time::timeout(Duration::from_secs(2), store.read_leader()).await;
    assert!(matches!(read, Ok(Ok(None))), "{read:?}");
    let swapped =
        tokio::time::timeout(Duration::from_secs(2), store.swap_leader(&record, None)).await;
    assert!(
        matches!(swapped, Ok(Ok(SwapOutcome::Swapped(_)))),
        "{swapped:?}"
    );
    let parked_read = tokio::time::timeout(
        Duration::from_millis(500),
        store.read_parked(&other, None, 1),
    )
    .await;
    assert!(
        parked_read.is_err(),
        "a parked-event call ran past the held permit"
    );
    appending.abort();
    Ok(())
}
