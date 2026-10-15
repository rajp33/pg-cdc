use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper::body::Incoming;
use hyper::header::{CONTENT_TYPE, HOST, HeaderValue};
use hyper::http::request;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode, client, server};
use hyper_util::rt::TokioIo;
use serde::Serialize;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedMutexGuard, watch};
use tokio::task::{JoinHandle, JoinSet};

const ACCEPT_RETRY: Duration = Duration::from_millis(10);
const S3_ERROR_BODY: &str = r#"<?xml version="1.0" encoding="UTF-8"?><Error><Code>SlowDown</Code><Message>chaos</Message></Error>"#;
const SNS_ERROR_BODY: &str = "<ErrorResponse><Error><Type>Receiver</Type><Code>InternalFailure</Code><Message>chaos</Message></Error></ErrorResponse>";

type TapFn = dyn Fn(&[u8], &[u8]) + Send + Sync;
type OrderGroups = fn(&[u8]) -> Vec<String>;

#[derive(Clone)]
pub struct Tap {
    record: Arc<TapFn>,
    groups: Option<OrderGroups>,
}

impl Tap {
    pub fn new(tap: impl Fn(&[u8], &[u8]) + Send + Sync + 'static) -> Self {
        Self {
            record: Arc::new(tap),
            groups: None,
        }
    }

    #[must_use]
    pub fn ordered_by(self, groups: OrderGroups) -> Self {
        Self {
            groups: Some(groups),
            ..self
        }
    }

    fn groups(&self, request: &[u8]) -> Vec<String> {
        let mut groups = self
            .groups
            .map(|groups| groups(request))
            .unwrap_or_default();
        groups.sort_unstable();
        groups.dedup();
        if groups.is_empty() {
            groups.push(String::new());
        }
        groups
    }
}

impl fmt::Debug for Tap {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Tap")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Upstream {
    S3,
    Sns,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Mode {
    Healthy,
    Outage,
    Brownout(Duration),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Faults {
    pub mode: Mode,
    pub error_rate: f64,
    pub error_after_forward_rate: f64,
    pub reset_rate: f64,
}

impl Faults {
    pub const HEALTHY: Faults = Faults {
        mode: Mode::Healthy,
        error_rate: 0.0,
        error_after_forward_rate: 0.0,
        reset_rate: 0.0,
    };
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct ProxyCounts {
    pub requests: u64,
    pub forwarded: u64,
    pub injected: u64,
    pub resets: u64,
    pub parked_puts: u64,
    pub parked_stored: u64,
    pub parked_deletes: u64,
    pub publishes: u64,
}

#[derive(Debug)]
pub struct FaultProxy {
    addr: SocketAddr,
    faults: watch::Sender<Faults>,
    shared: Arc<Shared>,
    server: JoinHandle<()>,
}

impl FaultProxy {
    pub async fn start(upstream: SocketAddr, kind: Upstream, seed: u64) -> anyhow::Result<Self> {
        Self::start_with_tap(upstream, kind, seed, None).await
    }

    pub async fn start_with_tap(
        upstream: SocketAddr,
        kind: Upstream,
        seed: u64,
        tap: Option<Tap>,
    ) -> anyhow::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let (faults, receiver) = watch::channel(Faults::HEALTHY);
        let shared = Arc::new(Shared {
            upstream,
            kind,
            faults: receiver,
            rng: Mutex::new(fastrand::Rng::with_seed(seed)),
            counters: Counters::default(),
            tap,
            turns: Mutex::default(),
        });
        let server = tokio::spawn(accept_loop(listener, Arc::clone(&shared)));
        Ok(Self {
            addr,
            faults,
            shared,
            server,
        })
    }

    pub fn endpoint(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn set(&self, faults: Faults) {
        self.faults.send_replace(faults);
    }

    pub fn counts(&self) -> ProxyCounts {
        self.shared.counters.snapshot()
    }
}

impl Drop for FaultProxy {
    fn drop(&mut self) {
        self.server.abort();
    }
}

#[derive(Debug)]
struct Shared {
    upstream: SocketAddr,
    kind: Upstream,
    faults: watch::Receiver<Faults>,
    rng: Mutex<fastrand::Rng>,
    counters: Counters,
    tap: Option<Tap>,
    turns: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl Shared {
    fn draw(&self, rate: f64) -> bool {
        rate > 0.0 && self.rng.lock().is_ok_and(|mut rng| rng.f64() < rate)
    }
}

#[derive(Debug, Default)]
struct Counters {
    requests: AtomicU64,
    forwarded: AtomicU64,
    injected: AtomicU64,
    resets: AtomicU64,
    parked_puts: AtomicU64,
    parked_stored: AtomicU64,
    parked_deletes: AtomicU64,
    publishes: AtomicU64,
}

impl Counters {
    fn snapshot(&self) -> ProxyCounts {
        ProxyCounts {
            requests: self.requests.load(Ordering::Relaxed),
            forwarded: self.forwarded.load(Ordering::Relaxed),
            injected: self.injected.load(Ordering::Relaxed),
            resets: self.resets.load(Ordering::Relaxed),
            parked_puts: self.parked_puts.load(Ordering::Relaxed),
            parked_stored: self.parked_stored.load(Ordering::Relaxed),
            parked_deletes: self.parked_deletes.load(Ordering::Relaxed),
            publishes: self.publishes.load(Ordering::Relaxed),
        }
    }
}

fn bump(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}

#[derive(Debug)]
struct Dropped;

impl fmt::Display for Dropped {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("connection dropped by the fault proxy")
    }
}

impl std::error::Error for Dropped {}

type ProxyResponse = Response<Full<Bytes>>;

async fn accept_loop(listener: TcpListener, shared: Arc<Shared>) {
    let mut connections = JoinSet::new();
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            tokio::time::sleep(ACCEPT_RETRY).await;
            continue;
        };
        while connections.try_join_next().is_some() {}
        let shared = Arc::clone(&shared);
        connections.spawn(async move {
            let service = service_fn(move |request| handle(Arc::clone(&shared), request));
            let _ = server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await;
        });
    }
}

async fn handle(shared: Arc<Shared>, request: Request<Incoming>) -> Result<ProxyResponse, Dropped> {
    let (parts, body) = request.into_parts();
    let body = body.collect().await.map_err(|_| Dropped)?.to_bytes();
    record_request(&shared, &parts, &body);
    let parked_put = is_parked_put(&parts);
    let faults = *shared.faults.borrow();

    if faults.mode == Mode::Outage {
        return Ok(injected_error(&shared));
    }
    if shared.draw(faults.reset_rate) {
        bump(&shared.counters.resets);
        return Err(Dropped);
    }
    if shared.draw(faults.error_rate) {
        return Ok(injected_error(&shared));
    }
    if let Mode::Brownout(latency) = faults.mode {
        tokio::time::sleep(latency).await;
    }
    let _turns = match &shared.tap {
        Some(tap) => take_turns(&shared, &tap.groups(&body)).await,
        None => Vec::new(),
    };
    let Ok((reply, reply_body)) = forward(&shared, parts, body.clone()).await else {
        return Ok(bad_gateway());
    };
    bump(&shared.counters.forwarded);
    if parked_put && reply.status().is_success() {
        bump(&shared.counters.parked_stored);
    }
    if let Some(tap) = &shared.tap
        && reply.status().is_success()
    {
        (tap.record)(&body, &reply_body);
    }
    if shared.draw(faults.error_after_forward_rate) {
        Ok(injected_error(&shared))
    } else {
        Ok(reply)
    }
}

async fn take_turns(shared: &Shared, groups: &[String]) -> Vec<OwnedMutexGuard<()>> {
    let locks: Vec<_> = {
        let mut turns = shared.turns.lock().unwrap_or_else(PoisonError::into_inner);
        groups
            .iter()
            .map(|group| Arc::clone(turns.entry(group.clone()).or_default()))
            .collect()
    };
    let mut guards = Vec::with_capacity(locks.len());
    for lock in locks {
        guards.push(lock.lock_owned().await);
    }
    guards
}

fn record_request(shared: &Shared, parts: &request::Parts, body: &Bytes) {
    let counters = &shared.counters;
    bump(&counters.requests);
    if is_parked_put(parts) {
        bump(&counters.parked_puts);
    } else if parts.uri.path().contains("/parked/") && parts.method == Method::DELETE {
        bump(&counters.parked_deletes);
    }
    if shared.kind == Upstream::Sns && contains(body, b"Action=Publish") {
        bump(&counters.publishes);
    }
}

fn is_parked_put(parts: &request::Parts) -> bool {
    parts.method == Method::PUT && parts.uri.path().contains("/parked/")
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

fn injected_error(shared: &Shared) -> ProxyResponse {
    bump(&shared.counters.injected);
    let (status, content_type, body) = match shared.kind {
        Upstream::S3 => (
            StatusCode::SERVICE_UNAVAILABLE,
            "application/xml",
            S3_ERROR_BODY,
        ),
        Upstream::Sns => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "text/xml",
            SNS_ERROR_BODY,
        ),
    };
    let mut response = Response::new(Full::new(Bytes::from_static(body.as_bytes())));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    response
}

fn bad_gateway() -> ProxyResponse {
    let mut response = Response::new(Full::new(Bytes::new()));
    *response.status_mut() = StatusCode::BAD_GATEWAY;
    response
}

const HOP_BY_HOP: [&str; 6] = [
    "connection",
    "proxy-connection",
    "keep-alive",
    "transfer-encoding",
    "upgrade",
    "expect",
];

async fn connect_upstream(upstream: SocketAddr) -> std::io::Result<TcpStream> {
    let stream = TcpStream::connect(upstream).await?;
    // moto answers Connection: close, so each request needs its own socket, and sockets closed on this side leave 30 s of TIME_WAIT: macOS's 16,383 ephemeral ports run out near 550 requests a second
    stream.set_zero_linger()?;
    Ok(stream)
}

async fn forward(
    shared: &Shared,
    parts: request::Parts,
    body: Bytes,
) -> anyhow::Result<(ProxyResponse, Bytes)> {
    let stream = connect_upstream(shared.upstream).await?;
    let (mut sender, connection) = client::conn::http1::handshake(TokioIo::new(stream)).await?;
    let driver = tokio::spawn(async move {
        let _ = connection.await;
    });

    let mut upstream_request = Request::builder()
        .method(parts.method)
        .uri(
            parts
                .uri
                .path_and_query()
                .map_or("/", |target| target.as_str()),
        )
        .body(Full::new(body))?;
    for (name, value) in &parts.headers {
        if name != HOST && !HOP_BY_HOP.contains(&name.as_str()) {
            upstream_request
                .headers_mut()
                .append(name.clone(), value.clone());
        }
    }
    upstream_request
        .headers_mut()
        .insert(HOST, HeaderValue::from_str(&shared.upstream.to_string())?);

    let response = sender.send_request(upstream_request).await;
    let outcome = match response {
        Ok(response) => {
            let (reply_parts, reply_body) = response.into_parts();
            reply_body.collect().await.map(|collected| {
                let bytes = collected.to_bytes();
                let mut reply = Response::new(Full::new(bytes.clone()));
                *reply.status_mut() = reply_parts.status;
                for (name, value) in &reply_parts.headers {
                    if !HOP_BY_HOP.contains(&name.as_str()) {
                        reply.headers_mut().append(name.clone(), value.clone());
                    }
                }
                (reply, bytes)
            })
        }
        Err(error) => Err(error),
    };
    driver.abort();
    Ok(outcome?)
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;

    #[derive(Debug, PartialEq)]
    struct Received {
        method: String,
        path: String,
        body: Vec<u8>,
        host: String,
        probe: String,
    }

    type Seen = Arc<Mutex<Vec<Received>>>;
    type Exchange = (Vec<u8>, Vec<u8>);

    async fn upstream() -> (SocketAddr, Seen) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen = Seen::default();
        let recorded = Arc::clone(&seen);
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let recorded = Arc::clone(&recorded);
                tokio::spawn(async move {
                    let service = service_fn(move |request: Request<Incoming>| {
                        let recorded = Arc::clone(&recorded);
                        async move {
                            let header = |name: &str| {
                                request.headers().get(name).map_or(String::new(), |value| {
                                    value.to_str().unwrap().to_owned()
                                })
                            };
                            let (host, probe) = (header("host"), header("x-probe"));
                            let method = request.method().to_string();
                            let path = request.uri().path().to_owned();
                            let status = if path.ends_with("/exists.json") {
                                StatusCode::PRECONDITION_FAILED
                            } else {
                                StatusCode::CREATED
                            };
                            let body = request.into_body().collect().await.unwrap().to_bytes();
                            recorded.lock().unwrap().push(Received {
                                method,
                                path,
                                body: body.to_vec(),
                                host,
                                probe,
                            });
                            Ok::<_, std::convert::Infallible>(
                                Response::builder()
                                    .status(status)
                                    .body(Full::new(Bytes::from_static(b"ok")))
                                    .unwrap(),
                            )
                        }
                    });
                    let _ = server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        (addr, seen)
    }

    async fn call(
        proxy: &FaultProxy,
        method: Method,
        path: &str,
        body: &'static str,
    ) -> Result<(u16, Vec<(String, String)>, String), hyper::Error> {
        let address = proxy.endpoint().trim_start_matches("http://").to_owned();
        let stream = TcpStream::connect(&address).await.unwrap();
        let (mut sender, connection) = client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .unwrap();
        tokio::spawn(connection);
        let request = Request::builder()
            .method(method)
            .uri(path)
            .header("host", address)
            .header("x-probe", "kept")
            .body(Full::new(Bytes::from_static(body.as_bytes())))
            .unwrap();
        let response = sender.send_request(request).await?;
        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_str().unwrap().to_owned()))
            .collect();
        let body = response.into_body().collect().await?.to_bytes();
        Ok((status, headers, String::from_utf8(body.to_vec()).unwrap()))
    }

    fn header<'a>(headers: &'a [(String, String)], name: &str) -> &'a str {
        headers
            .iter()
            .find(|(candidate, _)| candidate == name)
            .map_or("", |(_, value)| value.as_str())
    }

    const S3_ERROR: &str = r#"<?xml version="1.0" encoding="UTF-8"?><Error><Code>SlowDown</Code><Message>chaos</Message></Error>"#;
    const SNS_ERROR: &str = "<ErrorResponse><Error><Type>Receiver</Type><Code>InternalFailure</Code><Message>chaos</Message></Error></ErrorResponse>";

    async fn answer(proxy: &FaultProxy, method: Method, path: &str) -> (u16, String) {
        let (status, _, body) = call(proxy, method, path, "").await.unwrap();
        (status, body)
    }

    #[tokio::test]
    async fn the_tap_sees_every_body_the_upstream_accepted_in_order() {
        let (upstream_addr, _) = upstream().await;
        let tapped: Arc<Mutex<Vec<Exchange>>> = Arc::default();
        let sink = Arc::clone(&tapped);
        let tap = Tap::new(move |request, response| {
            sink.lock()
                .unwrap()
                .push((request.to_vec(), response.to_vec()));
        });
        let proxy = FaultProxy::start_with_tap(upstream_addr, Upstream::Sns, 1, Some(tap))
            .await
            .unwrap();

        assert_eq!(
            call(&proxy, Method::POST, "/", "first").await.unwrap().0,
            201
        );
        proxy.set(Faults {
            error_rate: 1.0,
            ..Faults::HEALTHY
        });
        assert_eq!(
            call(&proxy, Method::POST, "/", "refused").await.unwrap().0,
            500
        );
        proxy.set(Faults {
            mode: Mode::Outage,
            ..Faults::HEALTHY
        });
        assert_eq!(
            call(&proxy, Method::POST, "/", "outage").await.unwrap().0,
            500
        );
        proxy.set(Faults {
            error_after_forward_rate: 1.0,
            ..Faults::HEALTHY
        });
        assert_eq!(
            call(&proxy, Method::POST, "/", "landed").await.unwrap().0,
            500
        );
        proxy.set(Faults::HEALTHY);
        assert_eq!(
            call(&proxy, Method::POST, "/", "last").await.unwrap().0,
            201
        );

        let accepted = |body: &[u8]| (body.to_vec(), b"ok".to_vec());
        assert_eq!(
            tapped.lock().unwrap().as_slice(),
            [accepted(b"first"), accepted(b"landed"), accepted(b"last")]
        );
    }

    async fn slow_first_upstream() -> (SocketAddr, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let arrived: Arc<Mutex<Vec<String>>> = Arc::default();
        let recorded = Arc::clone(&arrived);
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let recorded = Arc::clone(&recorded);
                tokio::spawn(async move {
                    let service = service_fn(move |request: Request<Incoming>| {
                        let recorded = Arc::clone(&recorded);
                        async move {
                            let body = request.into_body().collect().await.unwrap().to_bytes();
                            let body = String::from_utf8(body.to_vec()).unwrap();
                            recorded.lock().unwrap().push(body.clone());
                            if body.ends_with("slow") {
                                tokio::time::sleep(Duration::from_millis(300)).await;
                            }
                            Ok::<_, std::convert::Infallible>(Response::new(Full::new(
                                Bytes::from(body),
                            )))
                        }
                    });
                    let _ = server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        (addr, arrived)
    }

    #[tokio::test]
    async fn the_tap_order_is_the_order_the_upstream_accepted_requests() {
        let (upstream_addr, arrived) = slow_first_upstream().await;
        let tapped: Arc<Mutex<Vec<String>>> = Arc::default();
        let sink = Arc::clone(&tapped);
        let tap = Tap::new(move |request, _| {
            sink.lock()
                .unwrap()
                .push(String::from_utf8(request.to_vec()).unwrap());
        });
        let proxy = FaultProxy::start_with_tap(upstream_addr, Upstream::Sns, 1, Some(tap))
            .await
            .unwrap();

        let (slow, fast) = tokio::join!(call(&proxy, Method::POST, "/", "slow"), async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            call(&proxy, Method::POST, "/", "fast").await
        });
        assert_eq!(slow.unwrap().0, 200);
        assert_eq!(fast.unwrap().0, 200);

        let arrived = arrived.lock().unwrap().clone();
        assert_eq!(arrived, ["slow", "fast"]);
        assert_eq!(*tapped.lock().unwrap(), arrived);
    }

    fn group_of(request: &[u8]) -> Vec<String> {
        let body = String::from_utf8(request.to_vec()).unwrap();
        body.split_once(':')
            .map(|(group, _)| vec![group.to_owned()])
            .unwrap_or_default()
    }

    async fn grouped_proxy(upstream: SocketAddr) -> (FaultProxy, Arc<Mutex<Vec<String>>>) {
        let tapped: Arc<Mutex<Vec<String>>> = Arc::default();
        let sink = Arc::clone(&tapped);
        let tap = Tap::new(move |request, _| {
            sink.lock()
                .unwrap()
                .push(String::from_utf8(request.to_vec()).unwrap());
        })
        .ordered_by(group_of);
        let proxy = FaultProxy::start_with_tap(upstream, Upstream::Sns, 1, Some(tap))
            .await
            .unwrap();
        (proxy, tapped)
    }

    async fn slow_then_fast(proxy: &FaultProxy, slow: &'static str, fast: &'static str) {
        let (slow, fast) = tokio::join!(call(proxy, Method::POST, "/", slow), async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            call(proxy, Method::POST, "/", fast).await
        });
        assert_eq!(slow.unwrap().0, 200);
        assert_eq!(fast.unwrap().0, 200);
    }

    #[tokio::test]
    async fn only_requests_sharing_an_order_group_wait_for_each_other() {
        let (upstream_addr, arrived) = slow_first_upstream().await;
        let (proxy, tapped) = grouped_proxy(upstream_addr).await;

        slow_then_fast(&proxy, "a:slow", "a:fast").await;
        assert_eq!(*arrived.lock().unwrap(), ["a:slow", "a:fast"]);
        assert_eq!(*tapped.lock().unwrap(), ["a:slow", "a:fast"]);

        arrived.lock().unwrap().clear();
        tapped.lock().unwrap().clear();
        slow_then_fast(&proxy, "a:slow", "b:fast").await;
        assert_eq!(*arrived.lock().unwrap(), ["a:slow", "b:fast"]);
        assert_eq!(*tapped.lock().unwrap(), ["b:fast", "a:slow"]);
    }

    #[tokio::test]
    async fn upstream_connections_are_reset_on_close_so_they_leave_no_time_wait() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let stream = connect_upstream(listener.local_addr().unwrap())
            .await
            .unwrap();
        assert_eq!(stream.linger().unwrap(), Some(Duration::ZERO));
    }

    #[tokio::test]
    async fn a_parked_put_counts_as_stored_only_when_the_upstream_accepts_it() {
        let (upstream_addr, _) = upstream().await;
        let proxy = FaultProxy::start(upstream_addr, Upstream::S3, 1)
            .await
            .unwrap();
        let parked = "/bucket/pre/p/parked/k";

        assert_eq!(
            answer(&proxy, Method::PUT, &format!("{parked}/new.json"))
                .await
                .0,
            201
        );
        assert_eq!(
            answer(&proxy, Method::PUT, &format!("{parked}/exists.json"))
                .await
                .0,
            412
        );
        assert_eq!(
            answer(&proxy, Method::GET, &format!("{parked}/new.json"))
                .await
                .0,
            201
        );
        proxy.set(Faults {
            mode: Mode::Outage,
            ..Faults::HEALTHY
        });
        assert_eq!(
            answer(&proxy, Method::PUT, &format!("{parked}/later.json"))
                .await
                .0,
            503
        );

        let counts = proxy.counts();
        assert_eq!((counts.parked_puts, counts.parked_stored), (3, 1));
    }

    #[tokio::test]
    async fn the_proxy_forwards_delays_fails_and_resets_as_told() {
        let (upstream_addr, seen) = upstream().await;
        let proxy = FaultProxy::start(upstream_addr, Upstream::S3, 1)
            .await
            .unwrap();
        let sns = FaultProxy::start(upstream_addr, Upstream::Sns, 1)
            .await
            .unwrap();
        let parked = "/bucket/pre/p/parked/k/e.json";
        let reached = || seen.lock().unwrap().len();

        let (status, _, body) = call(&proxy, Method::PUT, parked, "payload").await.unwrap();
        assert_eq!((status, body.as_str()), (201, "ok"));
        assert_eq!(
            seen.lock().unwrap().as_slice(),
            [Received {
                method: "PUT".to_owned(),
                path: parked.to_owned(),
                body: b"payload".to_vec(),
                host: upstream_addr.to_string(),
                probe: "kept".to_owned(),
            }]
        );

        let outage = Faults {
            mode: Mode::Outage,
            ..Faults::HEALTHY
        };
        proxy.set(outage);
        let (status, headers, body) = call(&proxy, Method::DELETE, parked, "").await.unwrap();
        assert_eq!(status, 503);
        assert_eq!(header(&headers, "content-type"), "application/xml");
        assert_eq!(body, S3_ERROR);
        sns.set(outage);
        let (status, headers, body) = call(&sns, Method::POST, "/", "Action=Publish&Message=m")
            .await
            .unwrap();
        assert_eq!(status, 500);
        assert_eq!(header(&headers, "content-type"), "text/xml");
        assert_eq!(body, SNS_ERROR);
        assert_eq!(reached(), 1);

        proxy.set(Faults {
            mode: Mode::Brownout(Duration::from_millis(150)),
            ..Faults::HEALTHY
        });
        let started = Instant::now();
        assert_eq!(answer(&proxy, Method::GET, "/bucket/object").await.0, 201);
        assert!(started.elapsed() >= Duration::from_millis(150));
        assert_eq!(reached(), 2);

        proxy.set(Faults {
            error_rate: 1.0,
            ..Faults::HEALTHY
        });
        let (status, body) = answer(&proxy, Method::GET, "/bucket/object").await;
        assert_eq!((status, body.as_str()), (503, S3_ERROR));
        assert_eq!(reached(), 2);

        proxy.set(Faults {
            error_after_forward_rate: 1.0,
            ..Faults::HEALTHY
        });
        let (status, body) = answer(&proxy, Method::GET, "/bucket/other").await;
        assert_eq!((status, body.as_str()), (503, S3_ERROR));
        assert_eq!(reached(), 3);

        proxy.set(Faults {
            reset_rate: 1.0,
            ..Faults::HEALTHY
        });
        assert!(
            call(&proxy, Method::GET, "/bucket/object", "")
                .await
                .is_err()
        );
        assert_eq!(reached(), 3);

        assert_eq!(
            proxy.counts(),
            ProxyCounts {
                requests: 6,
                forwarded: 3,
                injected: 3,
                resets: 1,
                parked_puts: 1,
                parked_stored: 1,
                parked_deletes: 1,
                publishes: 0,
            }
        );
        assert_eq!(
            sns.counts(),
            ProxyCounts {
                requests: 1,
                injected: 1,
                publishes: 1,
                ..ProxyCounts::default()
            }
        );
    }
}
