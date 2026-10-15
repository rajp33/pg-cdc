use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt as _, Empty};
use hyper::header::HOST;
use hyper::{Method, Request, client};
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;
use tokio::task::JoinHandle;

pub const PARKED_TIMEOUT: Duration = Duration::from_secs(40);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reply {
    pub status: u16,
    pub body: Bytes,
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
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

pub fn health_target(listen: SocketAddr) -> SocketAddr {
    let ip = match listen.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(ip) if ip.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
        ip => ip,
    };
    SocketAddr::new(ip, listen.port())
}

pub async fn request(
    listen: SocketAddr,
    method: Method,
    path_and_query: &str,
    timeout: Duration,
) -> Result<Reply, ClientError> {
    tokio::time::timeout(timeout, send(health_target(listen), method, path_and_query))
        .await
        .map_err(|_| ClientError::Timeout(timeout))?
}

pub async fn check_health(listen: SocketAddr, timeout: Duration) -> Result<u16, ClientError> {
    request(listen, Method::GET, "/healthz", timeout)
        .await
        .map(|reply| reply.status)
}

struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn send(
    target: SocketAddr,
    method: Method,
    path_and_query: &str,
) -> Result<Reply, ClientError> {
    let stream = TcpStream::connect(target)
        .await
        .map_err(|source| ClientError::Connect {
            addr: target,
            source,
        })?;
    let (mut sender, connection) = client::conn::http1::handshake(TokioIo::new(stream)).await?;
    let _driver = AbortOnDrop(tokio::spawn(async move {
        let _ = connection.await;
    }));
    let request = Request::builder()
        .method(method)
        .uri(path_and_query)
        .header(HOST, target.to_string())
        .body(Empty::<Bytes>::new())?;
    let response = sender.send_request(request).await?;
    let status = response.status().as_u16();
    let body = response.into_body().collect().await?.to_bytes();
    Ok(Reply { status, body })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::{HEALTH_TIMEOUT, OPERATION_TIMEOUT};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpListener;

    async fn one_shot(reply: &'static str) -> (SocketAddr, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut received = Vec::new();
            let mut chunk = [0u8; 1024];
            while !received.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = stream.read(&mut chunk).await.unwrap();
                received.extend_from_slice(&chunk[..read]);
            }
            stream.write_all(reply.as_bytes()).await.unwrap();
            String::from_utf8(received).unwrap()
        });
        (addr, server)
    }

    #[tokio::test]
    async fn a_request_sends_its_method_and_path_and_returns_the_status_and_body() {
        let (addr, server) = one_shot(
            "HTTP/1.1 409 Conflict\r\ncontent-length: 11\r\nconnection: close\r\n\r\n{\"a\":\"b\"}\n\n",
        )
        .await;
        let reply = request(addr, Method::POST, "/parked/k/discard?id=i", PARKED_TIMEOUT)
            .await
            .unwrap();
        assert_eq!(reply.status, 409);
        assert_eq!(reply.body, Bytes::from_static(b"{\"a\":\"b\"}\n\n"));
        let received = server.await.unwrap();
        assert!(
            received.starts_with("POST /parked/k/discard?id=i HTTP/1.1\r\n"),
            "{received}"
        );
        assert!(
            received.to_lowercase().contains(&format!("host: {addr}")),
            "{received}"
        );
    }

    #[test]
    fn parked_requests_wait_longer_than_the_operation_timeout() {
        assert_eq!(PARKED_TIMEOUT, Duration::from_secs(40));
        assert!(PARKED_TIMEOUT > OPERATION_TIMEOUT);
    }
    #[tokio::test]
    async fn a_refused_connection_names_the_address() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let error = check_health(addr, HEALTH_TIMEOUT).await.unwrap_err();
        assert!(matches!(error, ClientError::Connect { .. }), "{error}");
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
            matches!(error, ClientError::Timeout(t) if t == timeout),
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
