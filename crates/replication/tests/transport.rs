use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use pg_cdc_decoder::Lsn;
use pg_cdc_decoder::message::{LogicalMessage, decode};
use pg_cdc_replication::{
    ReplicationConfig, ReplicationEvent, ReplicationStream, TlsSetup, verify_login,
};
use pg_cdc_testkit::{Postgres, PostgresOptions, SESSION_OPTIONS};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, ServerName};
use secrecy::SecretString;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const SLOT: &str = "t_slot";
const PUBLICATION: &str = "t_pub";
const WAIT: Duration = Duration::from_secs(30);

async fn prepare(pg: &Postgres, table: &str) -> TestResult<Lsn> {
    pg.execute(&format!(
        "CREATE TABLE {table} (id int PRIMARY KEY, at timestamptz)"
    ))
    .await?;
    pg.create_publication(PUBLICATION, &[table]).await?;
    Ok(Lsn(pg.create_slot(SLOT).await?))
}

fn config(pg: &Postgres, start: Lsn) -> ReplicationConfig {
    ReplicationConfig {
        host: pg.host().to_owned(),
        port: pg.port(),
        user: pg.user().to_owned(),
        password: Some(SecretString::from(pg.password().to_owned())),
        database: pg.database().to_owned(),
        options: SESSION_OPTIONS.to_owned(),
        application_name: "pg-cdc-transport-test".to_owned(),
        tls: None,
        slot: SLOT.to_owned(),
        publication: PUBLICATION.to_owned(),
        start_lsn: start,
        status_interval: Duration::from_millis(200),
        connect_timeout: Duration::from_secs(10),
        buffer: 64,
    }
}

async fn next_xlog(stream: &mut ReplicationStream) -> TestResult<Bytes> {
    loop {
        match tokio::time::timeout(WAIT, stream.recv()).await?? {
            Some(ReplicationEvent::XLogData { data, .. }) => return Ok(data),
            Some(ReplicationEvent::KeepAlive { .. }) => {}
            None => return Err("the stream ended".into()),
        }
    }
}

async fn transaction(stream: &mut ReplicationStream) -> TestResult<(Vec<Bytes>, Lsn)> {
    let mut messages = Vec::new();
    loop {
        let data = next_xlog(stream).await?;
        let end = match decode(&data)? {
            LogicalMessage::Commit(commit) => Some(commit.end_lsn),
            _ => None,
        };
        messages.push(data);
        if let Some(end) = end {
            return Ok((messages, end));
        }
    }
}

#[tokio::test]
async fn backend_pid_matches_slot_active_pid() -> TestResult {
    let pg = Postgres::start().await?;
    let start = prepare(&pg, "pid_probe").await?;
    let stream = ReplicationStream::start(config(&pg, start)).await?;
    let state = pg
        .wait_for_slot(SLOT, WAIT, |state| state.active_pid.is_some())
        .await?;
    assert_eq!(state.active_pid, Some(i32::try_from(stream.backend_pid())?));
    stream.stop().await?;
    Ok(())
}

#[tokio::test]
async fn a4_status_updates_continue_while_consumer_stalls() -> TestResult {
    let pg = Postgres::start().await?;
    let start = prepare(&pg, "stall").await?;
    let mut stream = ReplicationStream::start(ReplicationConfig {
        buffer: 1,
        ..config(&pg, start)
    })
    .await?;
    for id in 0..50 {
        pg.execute(&format!("INSERT INTO stall VALUES ({id}, now())"))
            .await?;
    }
    let pid = i32::try_from(stream.backend_pid())?;
    let monitor = pg.client().await?;
    let reply_epoch =
        "SELECT extract(epoch FROM reply_time)::float8 FROM pg_stat_replication WHERE pid = $1";
    tokio::time::sleep(Duration::from_secs(1)).await;
    let first: f64 = monitor.query_one(reply_epoch, &[&pid]).await?.try_get(0)?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let second: f64 = monitor.query_one(reply_epoch, &[&pid]).await?.try_get(0)?;
    assert!(
        second > first,
        "no status update while stalled: {first} then {second}"
    );
    let mut inserts = 0;
    while inserts < 50 {
        if next_xlog(&mut stream).await?.first() == Some(&b'I') {
            inserts += 1;
        }
    }
    stream.stop().await?;
    Ok(())
}

#[tokio::test]
async fn startup_options_pin_session_settings() -> TestResult {
    let pg = Postgres::start().await?;
    pg.execute("ALTER DATABASE postgres SET timezone = 'America/New_York'")
        .await?;
    let start = prepare(&pg, "tz_probe").await?;
    let mut stream = ReplicationStream::start(config(&pg, start)).await?;
    pg.execute("INSERT INTO tz_probe VALUES (1, '2026-10-14 12:00:00+00')")
        .await?;
    let (messages, _) = transaction(&mut stream).await?;
    let insert = messages
        .iter()
        .find(|message| message.first() == Some(&b'I'))
        .ok_or("no Insert message")?;
    let text = String::from_utf8_lossy(insert);
    assert!(text.contains("2026-10-14 12:00:00+00"), "{text}");
    stream.stop().await?;
    Ok(())
}

#[tokio::test]
async fn confirm_advances_confirmed_flush_lsn() -> TestResult {
    let pg = Postgres::start().await?;
    let start = prepare(&pg, "ack_probe").await?;
    let mut stream = ReplicationStream::start(config(&pg, start)).await?;
    pg.execute("INSERT INTO ack_probe VALUES (1, now())")
        .await?;
    let (_, end) = transaction(&mut stream).await?;
    stream.confirm(end);
    pg.wait_for_slot(SLOT, WAIT, |state| {
        state.confirmed_flush_lsn.is_some_and(|lsn| lsn >= end.0)
    })
    .await?;
    stream.stop().await?;
    Ok(())
}

#[tokio::test]
async fn d1_transactional_logical_message_is_not_streamed() -> TestResult {
    let pg = Postgres::start().await?;
    let start = prepare(&pg, "d1_probe").await?;
    let mut stream = ReplicationStream::start(config(&pg, start)).await?;
    pg.execute(
        "BEGIN; INSERT INTO d1_probe VALUES (1, now()); \
         SELECT pg_logical_emit_message(true, 'pgcdc', 'hello'); \
         INSERT INTO d1_probe VALUES (2, now()); COMMIT",
    )
    .await?;
    let (messages, _) = transaction(&mut stream).await?;
    let tags: Vec<u8> = messages
        .iter()
        .filter_map(|message| message.first().copied())
        .collect();
    assert_eq!(tags, b"BRIIC");
    stream.stop().await?;
    Ok(())
}

#[tokio::test]
async fn scram_over_tls_verifies_the_server() -> TestResult {
    let pg = Postgres::start_with(PostgresOptions {
        tls: true,
        ..PostgresOptions::default()
    })
    .await?;
    let start = prepare(&pg, "tls_probe").await?;
    let mut roots = rustls::RootCertStore::empty();
    for certificate in CertificateDer::pem_file_iter(pg.ca_file().ok_or("no CA file")?)? {
        roots.add(certificate?)?;
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_no_client_auth();
    let setup = TlsSetup {
        config: Arc::new(tls),
        server_name: ServerName::try_from("localhost")?,
    };
    let mut stream = ReplicationStream::start(ReplicationConfig {
        tls: Some(setup),
        ..config(&pg, start)
    })
    .await?;
    pg.execute("INSERT INTO tls_probe VALUES (1, now())")
        .await?;
    transaction(&mut stream).await?;
    let pid = i32::try_from(stream.backend_pid())?;
    let ssl: bool = pg
        .client()
        .await?
        .query_one("SELECT ssl FROM pg_stat_ssl WHERE pid = $1", &[&pid])
        .await?
        .try_get(0)?;
    assert!(ssl, "the walsender connection is not encrypted");
    stream.stop().await?;
    Ok(())
}

#[tokio::test]
async fn a_wrong_password_is_28p01() -> TestResult {
    let pg = Postgres::start().await?;
    let start = prepare(&pg, "auth_probe").await?;
    let error = ReplicationStream::start(ReplicationConfig {
        password: Some(SecretString::from("wrong".to_owned())),
        ..config(&pg, start)
    })
    .await
    .err()
    .ok_or("a wrong password connected")?;
    assert_eq!(error.sqlstate(), Some("28P01"), "{error}");
    Ok(())
}

#[tokio::test]
async fn a_login_check_needs_the_replication_attribute_and_never_takes_the_slot() -> TestResult {
    let pg = Postgres::start().await?;
    let start = prepare(&pg, "login_probe").await?;
    verify_login(&config(&pg, start)).await?;
    let state = pg.slot_state(SLOT).await?.ok_or("the slot disappeared")?;
    assert!(!state.active);

    let holder = ReplicationStream::start(config(&pg, start)).await?;
    verify_login(&config(&pg, start)).await?;
    let state = pg.slot_state(SLOT).await?.ok_or("the slot disappeared")?;
    assert_eq!(state.active_pid, Some(i32::try_from(holder.backend_pid())?));

    pg.execute("CREATE ROLE no_replication LOGIN PASSWORD 'no_replication'")
        .await?;
    let error = verify_login(&ReplicationConfig {
        user: "no_replication".to_owned(),
        password: Some(SecretString::from("no_replication".to_owned())),
        ..config(&pg, start)
    })
    .await
    .err()
    .ok_or("a role without REPLICATION opened a walsender")?;
    assert_eq!(error.sqlstate(), Some("42501"), "{error}");
    holder.stop().await?;
    Ok(())
}

#[tokio::test]
async fn a_second_stream_on_an_active_slot_is_55006() -> TestResult {
    let pg = Postgres::start().await?;
    let start = prepare(&pg, "busy_probe").await?;
    let first = ReplicationStream::start(config(&pg, start)).await?;
    let error = ReplicationStream::start(config(&pg, start))
        .await
        .err()
        .ok_or("two streams shared one slot")?;
    assert_eq!(error.sqlstate(), Some("55006"), "{error}");
    first.stop().await?;
    Ok(())
}

#[tokio::test]
async fn stop_releases_the_slot_at_the_confirmed_lsn() -> TestResult {
    let pg = Postgres::start().await?;
    let start = prepare(&pg, "stop_probe").await?;
    let mut stream = ReplicationStream::start(ReplicationConfig {
        status_interval: Duration::from_secs(60),
        ..config(&pg, start)
    })
    .await?;
    pg.execute("INSERT INTO stop_probe VALUES (1, now())")
        .await?;
    let (_, end) = transaction(&mut stream).await?;
    stream.confirm(end);
    stream.stop().await?;
    let state = pg.wait_for_slot(SLOT, WAIT, |state| !state.active).await?;
    assert_eq!(state.confirmed_flush_lsn, Some(end.0));
    Ok(())
}
