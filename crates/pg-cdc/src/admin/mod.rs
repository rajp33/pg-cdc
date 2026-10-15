mod api;
pub mod client;

use std::convert::Infallible;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::header::{ALLOW, CONTENT_TYPE, HeaderValue};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode, Uri};
use hyper_util::rt::TokioIo;
use tokio::net::{TcpListener, TcpStream};

pub use client::{ClientError, PARKED_TIMEOUT, Reply, check_health, health_target, request};

use crate::health::{Health, HealthSnapshot};
use crate::state::StateStore;
use crate::status::Status;

pub const ADMIN_CONNECTION_TIMEOUT: Duration = Duration::from_secs(60);
pub const HEALTH_TIMEOUT: Duration = Duration::from_secs(2);
pub const OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
pub const HOLDER_LOOKUP_TIMEOUT: Duration = Duration::from_secs(5);
pub const PARKED_LIMIT_DEFAULT: usize = 100;
pub const PARKED_LIMIT_MAX: usize = 1000;
pub const PARKED_SCAN_LIMIT: usize = 1000;
pub const PARKED_LENGTH_CAP: usize = 1000;
pub const HEAD_READ_CONCURRENCY: usize = 16;
const ACCEPT_RETRY_DELAY: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    pub pipeline: String,
    pub slot: String,
    pub holder: String,
}

#[derive(Debug, Clone)]
pub struct Admin {
    health: Health,
    status: Status,
    identity: Arc<Identity>,
    store: Arc<OnceLock<Arc<dyn StateStore>>>,
}

impl Admin {
    pub fn new(health: Health, status: Status, identity: Identity) -> Self {
        Self {
            health,
            status,
            identity: Arc::new(identity),
            store: Arc::new(OnceLock::new()),
        }
    }

    pub fn set_state_store(&self, store: Arc<dyn StateStore>) {
        let _ = self.store.set(store);
    }
}

enum Endpoint<'a> {
    Status,
    Parked,
    Queue(&'a str),
    Redrive(&'a str),
    Discard(&'a str),
}

impl<'a> Endpoint<'a> {
    fn of(path: &'a str) -> Option<Self> {
        match path {
            "/status" => Some(Self::Status),
            "/parked" => Some(Self::Parked),
            _ => {
                let rest = path.strip_prefix("/parked/")?;
                match rest.split_once('/') {
                    None => Some(Self::Queue(rest)),
                    Some((key, "redrive")) => Some(Self::Redrive(key)),
                    Some((key, "discard")) => Some(Self::Discard(key)),
                    Some(_) => None,
                }
            }
        }
    }

    fn allow(&self) -> &'static str {
        match self {
            Self::Redrive(_) | Self::Discard(_) => "POST",
            Self::Status | Self::Parked | Self::Queue(_) => "GET",
        }
    }
}

pub async fn route(method: &Method, uri: &Uri, admin: &Admin) -> Response<Full<Bytes>> {
    let path = uri.path();
    if matches!(path, "/healthz" | "/readyz") {
        return respond(method, path, admin.health.snapshot());
    }
    let Some(endpoint) = Endpoint::of(path) else {
        return text(StatusCode::NOT_FOUND, "not found\n");
    };
    if method.as_str() != endpoint.allow() {
        return api::method_not_allowed(endpoint.allow());
    }
    let query = uri.query();
    let reply = match endpoint {
        Endpoint::Status => api::status(admin),
        Endpoint::Parked => api::list(admin, query).await,
        Endpoint::Queue(key) => api::show(admin, key, query).await,
        Endpoint::Redrive(key) => api::redrive(admin, key, query).await,
        Endpoint::Discard(key) => api::discard(admin, key, query).await,
    };
    reply.into_response()
}

fn respond(method: &Method, path: &str, health: HealthSnapshot) -> Response<Full<Bytes>> {
    let (ok, healthy_body, unhealthy_body) = match path {
        "/healthz" => (health.healthy(), "ok\n", "unhealthy\n"),
        "/readyz" => (health.ready(), "ready\n", "not ready\n"),
        _ => return text(StatusCode::NOT_FOUND, "not found\n"),
    };
    if method != Method::GET {
        let mut response = text(StatusCode::METHOD_NOT_ALLOWED, "method not allowed\n");
        response
            .headers_mut()
            .insert(ALLOW, HeaderValue::from_static("GET"));
        return response;
    }
    if ok {
        text(StatusCode::OK, healthy_body)
    } else {
        text(StatusCode::SERVICE_UNAVAILABLE, unhealthy_body)
    }
}

fn text(status: StatusCode, body: &'static str) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(Bytes::from_static(body.as_bytes())));
    *response.status_mut() = status;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

pub async fn serve(listener: TcpListener, admin: Admin) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(serve_connection(stream, admin.clone()));
            }
            Err(error) => {
                tracing::warn!(%error, "admin accept failed");
                tokio::time::sleep(ACCEPT_RETRY_DELAY).await;
            }
        }
    }
}

async fn serve_connection(stream: TcpStream, admin: Admin) {
    let service = service_fn(move |request: Request<Incoming>| {
        let admin = admin.clone();
        async move { Ok::<_, Infallible>(route(request.method(), request.uri(), &admin).await) }
    });
    let connection = http1::Builder::new().serve_connection(TokioIo::new(stream), service);
    match tokio::time::timeout(ADMIN_CONNECTION_TIMEOUT, connection).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::debug!(%error, "admin connection failed"),
        Err(_) => tracing::debug!("admin connection timed out"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::health::Role;
    use tokio::net::TcpStream;

    fn admin_with(health: Health) -> Admin {
        Admin::new(
            health,
            Status::new(),
            Identity {
                pipeline: "orders".to_owned(),
                slot: "pgcdc_orders".to_owned(),
                holder: "host-1".to_owned(),
            },
        )
    }

    fn health_of(snapshot: HealthSnapshot) -> Health {
        let health = Health::new();
        health.set_sink_ready(snapshot.sink_ready);
        health.set_store_ready(snapshot.store_ready);
        health.set_validated(snapshot.validated);
        health.set_role(snapshot.role);
        health.set_lease_valid(snapshot.lease_valid);
        health.set_delivering(snapshot.delivering);
        health
    }

    async fn get(admin: &Admin, method: &Method, path: &str) -> Response<Full<Bytes>> {
        route(method, &path.parse().unwrap(), admin).await
    }

    fn snapshots() -> Vec<HealthSnapshot> {
        let mut all = Vec::new();
        for bits in 0u8..32 {
            for role in [Role::Standby, Role::Active, Role::Draining] {
                all.push(HealthSnapshot {
                    sink_ready: bits & 1 != 0,
                    store_ready: bits & 2 != 0,
                    validated: bits & 4 != 0,
                    role,
                    lease_valid: bits & 8 != 0,
                    delivering: bits & 16 != 0,
                });
            }
        }
        all
    }

    async fn body_text(response: Response<Full<Bytes>>) -> String {
        use http_body_util::BodyExt as _;
        let body = response.into_body().collect().await.unwrap().to_bytes();
        String::from_utf8(body.to_vec()).unwrap()
    }

    fn content_type(response: &Response<Full<Bytes>>) -> &str {
        response
            .headers()
            .get(hyper::header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap()
    }

    #[tokio::test]
    async fn healthz_is_200_only_when_every_check_passes() {
        for snapshot in snapshots() {
            let admin = admin_with(health_of(snapshot));
            let response = get(&admin, &Method::GET, "/healthz").await;
            assert_eq!(content_type(&response), "text/plain; charset=utf-8");
            if snapshot.healthy() {
                assert_eq!(response.status(), 200, "{snapshot:?}");
                assert_eq!(body_text(response).await, "ok\n");
            } else {
                assert_eq!(response.status(), 503, "{snapshot:?}");
                assert_eq!(body_text(response).await, "unhealthy\n");
            }
        }
    }

    #[tokio::test]
    async fn readyz_is_200_only_when_active_and_delivering() {
        for snapshot in snapshots() {
            let admin = admin_with(health_of(snapshot));
            let response = get(&admin, &Method::GET, "/readyz").await;
            assert_eq!(content_type(&response), "text/plain; charset=utf-8");
            if snapshot.ready() {
                assert_eq!(response.status(), 200, "{snapshot:?}");
                assert_eq!(body_text(response).await, "ready\n");
            } else {
                assert_eq!(response.status(), 503, "{snapshot:?}");
                assert_eq!(body_text(response).await, "not ready\n");
            }
        }
    }

    #[tokio::test]
    async fn unknown_paths_are_404_and_other_methods_405() {
        let admin = admin_with(Health::new());
        let missing = get(&admin, &Method::GET, "/nowhere").await;
        assert_eq!(missing.status(), 404);
        assert_eq!(content_type(&missing), "text/plain; charset=utf-8");
        assert_eq!(body_text(missing).await, "not found\n");
        for path in ["/healthz", "/readyz"] {
            let refused = get(&admin, &Method::POST, path).await;
            assert_eq!(refused.status(), 405, "{path}");
            assert_eq!(refused.headers().get(hyper::header::ALLOW).unwrap(), "GET");
            assert_eq!(content_type(&refused), "text/plain; charset=utf-8");
        }
    }

    #[tokio::test]
    async fn serves_over_http_and_an_idle_client_blocks_no_one() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let health = Health::new();
        health.set_sink_ready(true);
        health.set_store_ready(true);
        health.set_validated(true);
        let server = tokio::spawn(serve(listener, admin_with(health.clone())));
        let _idle = TcpStream::connect(addr).await.unwrap();
        assert_eq!(check_health(addr, HEALTH_TIMEOUT).await.unwrap(), 200);
        health.set_validated(false);
        assert_eq!(check_health(addr, HEALTH_TIMEOUT).await.unwrap(), 503);
        server.abort();
    }

    #[tokio::test]
    async fn parked_routes_reach_the_router_over_http_with_their_queries() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(serve(listener, admin_with(Health::new())));
        let exchange = async |request: &str| {
            let mut stream = TcpStream::connect(addr).await.unwrap();
            let request =
                format!("{request} HTTP/1.1\r\nHost: pg-cdc\r\nConnection: close\r\n\r\n");
            stream.write_all(request.as_bytes()).await.unwrap();
            let mut reply = String::new();
            stream.read_to_string(&mut reply).await.unwrap();
            reply
        };
        let status = exchange("GET /status").await;
        assert!(status.starts_with("HTTP/1.1 200 OK"), "{status}");
        assert!(status.contains("application/json"), "{status}");
        assert!(status.contains(r#""pipeline":"orders""#), "{status}");
        let starting = exchange("GET /parked?limit=5").await;
        assert!(starting.starts_with("HTTP/1.1 503"), "{starting}");
        assert!(starting.contains("pg-cdc is starting"), "{starting}");
        let rejected = exchange("GET /parked?limit=0").await;
        assert!(rejected.starts_with("HTTP/1.1 400"), "{rejected}");
        let key = "0".repeat(32);
        let wrong = exchange(&format!("GET /parked/{key}/redrive")).await;
        assert!(wrong.starts_with("HTTP/1.1 405"), "{wrong}");
        assert!(wrong.to_lowercase().contains("allow: post"), "{wrong}");
        server.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn an_idle_connection_is_closed_after_the_admin_timeout() {
        use tokio::io::AsyncReadExt as _;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(serve(listener, admin_with(Health::new())));
        let mut idle = TcpStream::connect(addr).await.unwrap();
        let mut buffer = [0u8; 1];
        let started = tokio::time::Instant::now();
        assert_eq!(idle.read(&mut buffer).await.unwrap(), 0);
        assert!(started.elapsed() >= ADMIN_CONNECTION_TIMEOUT);
        server.abort();
    }
}
