use std::sync::Arc;

use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use postgres_protocol::authentication::md5_hash;
use postgres_protocol::authentication::sasl::{ChannelBinding, SCRAM_SHA_256, ScramSha256};
use postgres_protocol::message::backend::{AuthenticationSaslBody, Message};
use postgres_protocol::message::frontend;
use secrecy::ExposeSecret;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use crate::config::{ReplicationConfig, TlsSetup};
use crate::error::ReplicationError;
use crate::protocol::{Backend, parse_backend, protocol, server_error, start_replication_sql};

pub(crate) trait Io: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

pub(crate) struct Conn {
    pub(crate) io: Box<dyn Io>,
    pub(crate) read: BytesMut,
    pub(crate) write: BytesMut,
}

impl Conn {
    pub(crate) fn new(io: Box<dyn Io>) -> Self {
        Self {
            io,
            read: BytesMut::with_capacity(8192),
            write: BytesMut::with_capacity(1024),
        }
    }

    pub(crate) async fn flush(&mut self) -> Result<(), ReplicationError> {
        self.io.write_all(&self.write).await?;
        self.write.clear();
        self.io.flush().await?;
        Ok(())
    }

    pub(crate) fn try_next(&mut self) -> Result<Option<Backend>, ReplicationError> {
        loop {
            match parse_backend(&mut self.read)? {
                Some(Backend::Message(Message::ErrorResponse(body))) => {
                    return Err(server_error(&body));
                }
                Some(Backend::Message(
                    Message::NoticeResponse(_) | Message::ParameterStatus(_),
                )) => {}
                other => return Ok(other),
            }
        }
    }

    pub(crate) async fn next(&mut self) -> Result<Backend, ReplicationError> {
        loop {
            if let Some(message) = self.try_next()? {
                return Ok(message);
            }
            if self.io.read_buf(&mut self.read).await? == 0 {
                return Err(ReplicationError::Closed);
            }
        }
    }
}

pub(crate) async fn open(config: &ReplicationConfig) -> Result<Box<dyn Io>, ReplicationError> {
    let tcp = TcpStream::connect((config.host.as_str(), config.port)).await?;
    tcp.set_nodelay(true)?;
    match &config.tls {
        None => Ok(Box::new(tcp)),
        Some(tls) => negotiate_tls(tcp, tls).await,
    }
}

async fn negotiate_tls(
    mut tcp: TcpStream,
    tls: &TlsSetup,
) -> Result<Box<dyn Io>, ReplicationError> {
    let mut request = BytesMut::new();
    frontend::ssl_request(&mut request);
    tcp.write_all(&request).await?;
    match tcp.read_u8().await? {
        b'S' => {}
        b'N' => return Err(ReplicationError::TlsRefused),
        other => {
            return Err(protocol(&format!(
                "unexpected reply {other:#04x} to SSLRequest"
            )));
        }
    }
    let stream = TlsConnector::from(Arc::clone(&tls.config))
        .connect(tls.server_name.clone(), tcp)
        .await
        .map_err(ReplicationError::Tls)?;
    Ok(Box::new(stream))
}

pub(crate) async fn handshake(
    conn: &mut Conn,
    config: &ReplicationConfig,
) -> Result<u32, ReplicationError> {
    frontend::startup_message(
        [
            ("user", config.user.as_str()),
            ("database", config.database.as_str()),
            ("replication", "database"),
            ("application_name", config.application_name.as_str()),
            ("options", config.options.as_str()),
        ],
        &mut conn.write,
    )?;
    conn.flush().await?;
    authenticate(conn, config).await?;
    let backend_pid = wait_until_ready(conn).await?;
    let sql = start_replication_sql(&config.slot, config.start_lsn, &config.publication);
    frontend::query(&sql, &mut conn.write)?;
    conn.flush().await?;
    expect_copy_both(conn).await?;
    Ok(backend_pid)
}

async fn authenticate(conn: &mut Conn, config: &ReplicationConfig) -> Result<(), ReplicationError> {
    loop {
        match conn.next().await? {
            Backend::Message(Message::AuthenticationOk) => return Ok(()),
            Backend::Message(Message::AuthenticationCleartextPassword) => {
                frontend::password_message(password(config)?, &mut conn.write)?;
                conn.flush().await?;
            }
            Backend::Message(Message::AuthenticationMd5Password(body)) => {
                let hash = md5_hash(config.user.as_bytes(), password(config)?, body.salt());
                frontend::password_message(hash.as_bytes(), &mut conn.write)?;
                conn.flush().await?;
            }
            Backend::Message(Message::AuthenticationSasl(body)) => {
                scram(conn, config, &body).await?;
            }
            _ => return Err(protocol("unexpected message during authentication")),
        }
    }
}

fn password(config: &ReplicationConfig) -> Result<&[u8], ReplicationError> {
    config
        .password
        .as_ref()
        .map(|password| password.expose_secret().as_bytes())
        .ok_or(ReplicationError::PasswordRequired)
}

async fn scram(
    conn: &mut Conn,
    config: &ReplicationConfig,
    body: &AuthenticationSaslBody,
) -> Result<(), ReplicationError> {
    let mut offered = Vec::new();
    let mut mechanisms = body.mechanisms();
    while let Some(mechanism) = mechanisms.next().map_err(ReplicationError::Scram)? {
        offered.push(mechanism.to_owned());
    }
    if !offered.iter().any(|mechanism| mechanism == SCRAM_SHA_256) {
        return Err(ReplicationError::UnsupportedSasl(offered.join(", ")));
    }
    let mut scram = ScramSha256::new(password(config)?, ChannelBinding::unsupported());
    frontend::sasl_initial_response(SCRAM_SHA_256, scram.message(), &mut conn.write)?;
    conn.flush().await?;
    match conn.next().await? {
        Backend::Message(Message::AuthenticationSaslContinue(body)) => {
            scram.update(body.data()).map_err(ReplicationError::Scram)?;
        }
        _ => return Err(protocol("unexpected message during SCRAM authentication")),
    }
    frontend::sasl_response(scram.message(), &mut conn.write)?;
    conn.flush().await?;
    match conn.next().await? {
        Backend::Message(Message::AuthenticationSaslFinal(body)) => {
            scram.finish(body.data()).map_err(ReplicationError::Scram)
        }
        _ => Err(protocol("unexpected message during SCRAM authentication")),
    }
}

async fn wait_until_ready(conn: &mut Conn) -> Result<u32, ReplicationError> {
    let mut backend_pid = 0;
    loop {
        match conn.next().await? {
            Backend::Message(Message::BackendKeyData(body)) => {
                backend_pid = body.process_id().cast_unsigned();
            }
            Backend::Message(Message::ReadyForQuery(_)) => return Ok(backend_pid),
            _ => return Err(protocol("unexpected message before ReadyForQuery")),
        }
    }
}

async fn expect_copy_both(conn: &mut Conn) -> Result<(), ReplicationError> {
    match conn.next().await? {
        Backend::CopyBothResponse => Ok(()),
        Backend::Message(_) => Err(protocol("unexpected reply to START_REPLICATION")),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use pg_cdc_decoder::Lsn;
    use tokio::net::TcpListener;

    use crate::error::ReplicationError;
    use crate::fake::{self, FakeServer};
    use crate::stream::ReplicationStream;

    use super::*;

    fn owned(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    #[tokio::test]
    async fn startup_message_carries_replication_parameters() {
        let (start, mut server) = fake::spawn_start(fake::config());
        let parameters = server.read_startup().await;
        assert_eq!(
            parameters,
            owned(&[
                ("user", "md5_user"),
                ("database", "app"),
                ("replication", "database"),
                ("application_name", "pg-cdc-test"),
                ("options", "-c TimeZone=UTC"),
            ])
        );
        server.ready(4242).await;
        server.accept_replication().await;
        let stream = start.await.unwrap().unwrap();
        assert_eq!(stream.backend_pid(), 4242);
    }

    #[tokio::test]
    async fn cleartext_and_md5_send_the_right_password_message() {
        let cases: [(i32, &[u8], &[u8]); 2] = [
            (3, b"", b"password\0"),
            (
                5,
                &[0x2a, 0x3d, 0x8f, 0xe0],
                b"md562af4dd09bbb41884907a838a3233294\0",
            ),
        ];
        for (code, extra, expected) in cases {
            let (start, mut server) = fake::spawn_start(fake::config());
            server.read_startup().await;
            server.auth(code, extra).await;
            let reply = server.read().await;
            assert_eq!(
                (reply.tag, reply.body.as_slice()),
                (b'p', expected),
                "code {code}"
            );
            server.ready(4242).await;
            server.accept_replication().await;
            start.await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn scram_picks_plain_sha_256_without_channel_binding() {
        let (start, mut server) = fake::spawn_start(fake::config());
        server.read_startup().await;
        server
            .auth(10, b"SCRAM-SHA-256-PLUS\0SCRAM-SHA-256\0\0")
            .await;
        let initial = server.read().await;
        assert_eq!(initial.tag, b'p');
        let nul = initial.body.iter().position(|byte| *byte == 0).unwrap();
        assert_eq!(&initial.body[..nul], b"SCRAM-SHA-256");
        let data = &initial.body[nul + 5..];
        assert!(
            data.starts_with(b"n,,n=,r="),
            "{}",
            String::from_utf8_lossy(data)
        );
        server
            .error(
                "28P01",
                "password authentication failed for user \"md5_user\"",
            )
            .await;
        let error = start.await.unwrap().unwrap_err();
        assert_eq!(error.sqlstate(), Some("28P01"), "{error}");
    }

    #[tokio::test]
    async fn sasl_without_scram_sha_256_is_an_error() {
        let (start, mut server) = fake::spawn_start(fake::config());
        server.read_startup().await;
        server.auth(10, b"SCRAM-SHA-256-PLUS\0\0").await;
        let error = start.await.unwrap().unwrap_err();
        assert!(
            matches!(&error, ReplicationError::UnsupportedSasl(offered) if offered == "SCRAM-SHA-256-PLUS"),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn unsupported_authentication_names_its_code() {
        for code in [7, 99] {
            let (start, mut server) = fake::spawn_start(fake::config());
            server.read_startup().await;
            server.auth(code, b"").await;
            let error = start.await.unwrap().unwrap_err();
            assert!(
                matches!(error, ReplicationError::UnsupportedAuthentication(c) if c == code),
                "{error:?}"
            );
            assert!(error.to_string().contains(&code.to_string()), "{error}");
        }
    }

    #[tokio::test]
    async fn a_password_request_without_a_password_is_an_error() {
        let mut config = fake::config();
        config.password = None;
        let (start, mut server) = fake::spawn_start(config);
        server.read_startup().await;
        server.auth(3, b"").await;
        let error = start.await.unwrap().unwrap_err();
        assert!(
            matches!(error, ReplicationError::PasswordRequired),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn start_replication_error_surfaces_its_sqlstate() {
        let (start, mut server) = fake::spawn_start(fake::config());
        server.read_startup().await;
        server.ready(4242).await;
        assert_eq!(server.read().await.tag, b'Q');
        server
            .error("55006", "replication slot \"it_slot\" is active for PID 77")
            .await;
        let error = start.await.unwrap().unwrap_err();
        assert_eq!(error.sqlstate(), Some("55006"), "{error}");
    }

    #[tokio::test]
    async fn d1_start_replication_negotiates_no_optional_features() {
        let mut config = fake::config();
        config.start_lsn = Lsn(0x1_0000_00FF);
        config.publication = "we\"ird'pub".to_owned();
        let (start, mut server) = fake::spawn_start(config);
        server.read_startup().await;
        server.ready(4242).await;
        let sql = server.accept_replication().await;
        assert_eq!(
            sql,
            r#"START_REPLICATION SLOT "it_slot" LOGICAL 1/FF (proto_version '1', publication_names '"we""ird''pub"')"#
        );
        start.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn tls_refusal_never_falls_back_to_plaintext() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = fake::config();
        config.port = listener.local_addr().unwrap().port();
        config.tls = Some(fake::tls_setup());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 8];
            socket.read_exact(&mut request).await.unwrap();
            socket.write_all(b"N").await.unwrap();
            let mut rest = Vec::new();
            socket.read_to_end(&mut rest).await.unwrap();
            (request, rest)
        });
        let error = ReplicationStream::start(config).await.unwrap_err();
        assert!(matches!(error, ReplicationError::TlsRefused), "{error:?}");
        let (request, rest) = server.await.unwrap();
        assert_eq!(request, [0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f]);
        assert!(
            rest.is_empty(),
            "the client sent {rest:?} after the refusal"
        );
    }

    #[tokio::test]
    async fn a_silent_server_times_out() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = fake::config();
        config.port = listener.local_addr().unwrap().port();
        config.connect_timeout = Duration::from_millis(200);
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut server = FakeServer::new(socket);
            server.read_startup().await;
            std::future::pending::<()>().await;
        });
        let error = ReplicationStream::start(config).await.unwrap_err();
        assert!(matches!(error, ReplicationError::Timeout(_)), "{error:?}");
        server.abort();
    }
}
