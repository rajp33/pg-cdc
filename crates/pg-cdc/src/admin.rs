use std::convert::Infallible;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{Empty, Full};
use hyper::body::Incoming;
use hyper::header::{ALLOW, CONTENT_TYPE, HOST, HeaderValue};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode, client};
use hyper_util::rt::TokioIo;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use crate::health::{Health, HealthSnapshot};

pub const ADMIN_CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);
pub const HEALTH_TIMEOUT: Duration = Duration::from_secs(2);
const ACCEPT_RETRY_DELAY: Duration = Duration::from_millis(100);

#[derive(Debug, thiserror::Error)]
pub enum HealthCheckError {
    #[error("cannot connect to {addr}: {source}")]
    Connect {
        addr: SocketAddr,
        source: std::io::Error,
    },
    #[error("cannot build the request: {0}")]
    Request(#[from] hyper::http::Error),
    #[error("HTTP request failed: {0}")]
    Http(#[from] hyper::Error),
    #[error("no answer within {0:?}")]
    Timeout(Duration),
}

pub fn respond(method: &Method, path: &str, health: &HealthSnapshot) -> Response<Full<Bytes>> {
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

pub async fn serve(listener: TcpListener, health: Health) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(serve_connection(stream, health.clone()));
            }
            Err(error) => {
                tracing::warn!(%error, "admin accept failed");
                tokio::time::sleep(ACCEPT_RETRY_DELAY).await;
            }
        }
    }
}

async fn serve_connection(stream: TcpStream, health: Health) {
    let service = service_fn(move |request: Request<Incoming>| {
        let snapshot = health.snapshot();
        async move { Ok::<_, Infallible>(respond(request.method(), request.uri().path(), &snapshot)) }
    });
    let connection = http1::Builder::new().serve_connection(TokioIo::new(stream), service);
    match tokio::time::timeout(ADMIN_CONNECTION_TIMEOUT, connection).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::debug!(%error, "admin connection failed"),
        Err(_) => tracing::debug!("admin connection timed out"),
    }
}

pub fn health_target(listen: SocketAddr) -> SocketAddr {
    let ip = match listen.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(ip) if ip.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
        ip => ip,
    };
    SocketAddr::new(ip, listen.port())
}

pub async fn check_health(listen: SocketAddr, timeout: Duration) -> Result<u16, HealthCheckError> {
    tokio::time::timeout(timeout, request_healthz(health_target(listen)))
        .await
        .map_err(|_| HealthCheckError::Timeout(timeout))?
}

struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn request_healthz(target: SocketAddr) -> Result<u16, HealthCheckError> {
    let stream = TcpStream::connect(target)
        .await
        .map_err(|source| HealthCheckError::Connect {
            addr: target,
            source,
        })?;
    let (mut sender, connection) = client::conn::http1::handshake(TokioIo::new(stream)).await?;
    let _driver = AbortOnDrop(tokio::spawn(async move {
        let _ = connection.await;
    }));
    let request = Request::builder()
        .method(Method::GET)
        .uri("/healthz")
        .header(HOST, target.to_string())
        .body(Empty::<Bytes>::new())?;
    let response = sender.send_request(request).await?;
    Ok(response.status().as_u16())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::health::Role;
    use tokio::net::TcpStream;

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
            let response = respond(&Method::GET, "/healthz", &snapshot);
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
            let response = respond(&Method::GET, "/readyz", &snapshot);
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
        let snapshot = HealthSnapshot::default();
        let missing = respond(&Method::GET, "/status", &snapshot);
        assert_eq!(missing.status(), 404);
        assert_eq!(content_type(&missing), "text/plain; charset=utf-8");
        assert_eq!(body_text(missing).await, "not found\n");
        for path in ["/healthz", "/readyz"] {
            let refused = respond(&Method::POST, path, &snapshot);
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
        let server = tokio::spawn(serve(listener, health.clone()));
        let _idle = TcpStream::connect(addr).await.unwrap();
        assert_eq!(check_health(addr, HEALTH_TIMEOUT).await.unwrap(), 200);
        health.set_validated(false);
        assert_eq!(check_health(addr, HEALTH_TIMEOUT).await.unwrap(), 503);
        server.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn an_idle_connection_is_closed_after_the_admin_timeout() {
        use tokio::io::AsyncReadExt as _;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(serve(listener, Health::new()));
        let mut idle = TcpStream::connect(addr).await.unwrap();
        let mut buffer = [0u8; 1];
        let started = tokio::time::Instant::now();
        assert_eq!(idle.read(&mut buffer).await.unwrap(), 0);
        assert!(started.elapsed() >= ADMIN_CONNECTION_TIMEOUT);
        server.abort();
    }

    #[tokio::test]
    async fn a_refused_connection_names_the_address() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let error = check_health(addr, HEALTH_TIMEOUT).await.unwrap_err();
        assert!(matches!(error, HealthCheckError::Connect { .. }), "{error}");
        assert!(error.to_string().contains(&addr.to_string()), "{error}");
    }

    #[tokio::test]
    async fn a_silent_server_times_out() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let silent = tokio::spawn(async move {
            let _held = listener.accept().await;
            std::future::pending::<()>().await;
        });
        let timeout = Duration::from_millis(100);
        let error = check_health(addr, timeout).await.unwrap_err();
        assert!(
            matches!(error, HealthCheckError::Timeout(t) if t == timeout),
            "{error}"
        );
        silent.abort();
    }

    #[test]
    fn health_target_maps_unspecified_addresses_to_loopback() {
        let target = |text: &str| health_target(text.parse().unwrap());
        assert_eq!(target("0.0.0.0:9187"), "127.0.0.1:9187".parse().unwrap());
        assert_eq!(target("[::]:1"), "[::1]:1".parse().unwrap());
        assert_eq!(target("10.0.0.5:9187"), "10.0.0.5:9187".parse().unwrap());
    }
}
