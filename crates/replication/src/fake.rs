use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use pg_cdc_decoder::Lsn;
use rustls_pki_types::ServerName;
use secrecy::SecretString;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task::JoinHandle;

use crate::config::{ReplicationConfig, TlsSetup};
use crate::error::ReplicationError;
use crate::startup::Io;
use crate::stream::ReplicationStream;

pub(crate) fn config() -> ReplicationConfig {
    ReplicationConfig {
        host: "127.0.0.1".to_owned(),
        port: 5432,
        user: "md5_user".to_owned(),
        password: Some(SecretString::from("password".to_owned())),
        database: "app".to_owned(),
        options: "-c TimeZone=UTC".to_owned(),
        application_name: "pg-cdc-test".to_owned(),
        tls: None,
        slot: "it_slot".to_owned(),
        publication: "it_pub".to_owned(),
        start_lsn: Lsn(0x10),
        status_interval: Duration::from_secs(10),
        connect_timeout: Duration::from_secs(5),
        buffer: 8,
    }
}

pub(crate) fn tls_setup() -> TlsSetup {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();
    TlsSetup {
        config: Arc::new(config),
        server_name: ServerName::try_from("localhost").unwrap(),
    }
}

pub(crate) fn spawn_start(
    config: ReplicationConfig,
) -> (
    JoinHandle<Result<ReplicationStream, ReplicationError>>,
    FakeServer,
) {
    let (client, server) = tokio::io::duplex(1 << 16);
    let start = tokio::spawn(ReplicationStream::start_on(Box::new(client), config));
    (start, FakeServer::new(server))
}

pub(crate) async fn started(config: ReplicationConfig) -> (ReplicationStream, FakeServer) {
    let (start, mut server) = spawn_start(config);
    server.read_startup().await;
    server.ready(4242).await;
    server.accept_replication().await;
    (start.await.unwrap().unwrap(), server)
}

pub(crate) struct Frame {
    pub(crate) tag: u8,
    pub(crate) body: Vec<u8>,
}

pub(crate) struct Status {
    pub(crate) write: u64,
    pub(crate) flush: u64,
    pub(crate) apply: u64,
    pub(crate) reply: u8,
}

impl Status {
    pub(crate) fn parse(body: &[u8]) -> Self {
        assert_eq!(
            (body.len(), body[0]),
            (34, b'r'),
            "not a standby status update"
        );
        let at = |offset: usize| u64::from_be_bytes(body[offset..offset + 8].try_into().unwrap());
        Self {
            write: at(1),
            flush: at(9),
            apply: at(17),
            reply: body[33],
        }
    }
}

pub(crate) struct FakeServer {
    io: Box<dyn Io>,
}

impl FakeServer {
    pub(crate) fn new(io: impl Io + 'static) -> Self {
        Self { io: Box::new(io) }
    }

    pub(crate) async fn read_startup(&mut self) -> Vec<(String, String)> {
        let len = self.io.read_u32().await.unwrap();
        let mut body = vec![0; usize::try_from(len).unwrap() - 4];
        self.io.read_exact(&mut body).await.unwrap();
        assert_eq!(body[..4], [0, 3, 0, 0], "protocol 3.0");
        let mut parts = body[4..]
            .split(|byte| *byte == 0)
            .map(|part| String::from_utf8(part.to_vec()).unwrap());
        let mut parameters = Vec::new();
        loop {
            let key = parts.next().unwrap();
            if key.is_empty() {
                return parameters;
            }
            parameters.push((key, parts.next().unwrap()));
        }
    }

    pub(crate) async fn read(&mut self) -> Frame {
        let tag = self.io.read_u8().await.unwrap();
        let len = self.io.read_u32().await.unwrap();
        let mut body = vec![0; usize::try_from(len).unwrap() - 4];
        self.io.read_exact(&mut body).await.unwrap();
        Frame { tag, body }
    }

    pub(crate) async fn read_status(&mut self) -> Status {
        let frame = self.read().await;
        assert_eq!(frame.tag, b'd', "expected CopyData");
        Status::parse(&frame.body)
    }

    pub(crate) async fn send(&mut self, tag: u8, body: &[u8]) {
        let mut frame = BytesMut::new();
        frame.put_u8(tag);
        frame.put_u32(u32::try_from(body.len() + 4).unwrap());
        frame.put_slice(body);
        self.io.write_all(&frame).await.unwrap();
    }

    pub(crate) async fn auth(&mut self, code: i32, extra: &[u8]) {
        let mut body = code.to_be_bytes().to_vec();
        body.extend_from_slice(extra);
        self.send(b'R', &body).await;
    }

    pub(crate) async fn error(&mut self, sqlstate: &str, message: &str) {
        let body = format!("SFATAL\0VFATAL\0C{sqlstate}\0M{message}\0\0");
        self.send(b'E', body.as_bytes()).await;
    }

    pub(crate) async fn ready(&mut self, pid: i32) {
        self.auth(0, b"").await;
        self.send(b'S', b"server_version\x0018.0\0").await;
        self.send(b'N', b"SWARNING\0C01000\0Mhello\0\0").await;
        let mut key = pid.to_be_bytes().to_vec();
        key.extend_from_slice(&7_i32.to_be_bytes());
        self.send(b'K', &key).await;
        self.send(b'Z', b"I").await;
    }

    pub(crate) async fn accept_replication(&mut self) -> String {
        let query = self.read().await;
        assert_eq!(query.tag, b'Q');
        self.send(b'W', &[0, 0, 0]).await;
        String::from_utf8(query.body.strip_suffix(b"\0").unwrap().to_vec()).unwrap()
    }

    pub(crate) async fn xlog(&mut self, start: u64, end: u64, data: &[u8]) {
        let mut body = vec![b'w'];
        body.extend_from_slice(&start.to_be_bytes());
        body.extend_from_slice(&end.to_be_bytes());
        body.extend_from_slice(&0_i64.to_be_bytes());
        body.extend_from_slice(data);
        self.send(b'd', &body).await;
    }

    pub(crate) async fn keepalive(&mut self, end: u64, reply: bool) {
        let mut body = vec![b'k'];
        body.extend_from_slice(&end.to_be_bytes());
        body.extend_from_slice(&0_i64.to_be_bytes());
        body.push(u8::from(reply));
        self.send(b'd', &body).await;
    }
}
