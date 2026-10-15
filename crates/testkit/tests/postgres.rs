use pg_cdc_testkit::{
    Postgres, PostgresOptions, parse_lsn, parse_pg_version, read_fixture, write_fixture,
};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[test]
fn version_defaults_to_17_and_rejects_out_of_range() {
    assert_eq!(parse_pg_version(None).unwrap(), 17);
    assert_eq!(parse_pg_version(Some("14")).unwrap(), 14);
    assert_eq!(parse_pg_version(Some("18")).unwrap(), 18);
    for bad in ["13", "19", "latest", ""] {
        assert!(
            parse_pg_version(Some(bad)).is_err(),
            "{bad:?} must be rejected"
        );
    }
}

#[test]
fn lsn_parsing_accepts_padded_and_unpadded_forms() {
    assert_eq!(parse_lsn("0/1A2B3C4D").unwrap(), 0x1A2B_3C4D);
    assert_eq!(parse_lsn("0/016B3748").unwrap(), 0x016B_3748);
    assert_eq!(parse_lsn("1/FF").unwrap(), 0x1_0000_00FF);
    for bad in ["", "1", "x/1", "1/", "/1", "1/100000000"] {
        assert!(parse_lsn(bad).is_err(), "{bad:?} must be rejected");
    }
}

#[test]
fn fixtures_round_trip_length_prefixed_messages() -> TestResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("nested").join("scenario.bin");
    let messages = vec![b"B123".to_vec(), Vec::new(), vec![7u8; 300]];
    write_fixture(&path, &messages)?;
    assert_eq!(read_fixture(&path)?, messages);
    std::fs::write(&path, [0, 0, 0, 9, 1])?;
    assert!(read_fixture(&path).is_err());
    Ok(())
}

#[tokio::test]
async fn starts_requested_version_with_logical_wal_level_and_pinned_settings() -> TestResult {
    let pg = Postgres::start().await?;
    let client = pg.client().await?;
    let wal_level: String = client.query_one("SHOW wal_level", &[]).await?.try_get(0)?;
    assert_eq!(wal_level, "logical");
    let version: String = client
        .query_one("SHOW server_version_num", &[])
        .await?
        .try_get(0)?;
    assert_eq!(version.get(..2), Some(pg.version().to_string().as_str()));
    let timezone: String = client.query_one("SHOW TimeZone", &[]).await?.try_get(0)?;
    assert_eq!(timezone, "UTC");
    let autovacuum: String = client.query_one("SHOW autovacuum", &[]).await?.try_get(0)?;
    assert_eq!(autovacuum, "off");
    Ok(())
}

#[tokio::test]
async fn slot_state_and_capture_follow_the_slot() -> TestResult {
    let pg = Postgres::start().await?;
    pg.execute("CREATE TABLE probe (id int PRIMARY KEY)")
        .await?;
    pg.create_publication("probe_pub", &["probe"]).await?;
    let created = pg.create_slot("probe_slot").await?;
    let state = pg.slot_state("probe_slot").await?.ok_or("slot missing")?;
    assert!(!state.active);
    assert_eq!(state.active_pid, None);
    assert_eq!(state.wal_status.as_deref(), Some("reserved"));
    assert!(state.confirmed_flush_lsn.is_some_and(|lsn| lsn >= created));
    pg.execute("INSERT INTO probe VALUES (1)").await?;
    let messages = pg.capture_pgoutput("probe_slot", "probe_pub").await?;
    let tags: Vec<u8> = messages
        .iter()
        .filter_map(|message| message.first().copied())
        .collect();
    assert_eq!(tags, b"BRIC".to_vec());
    let after = pg.slot_state("probe_slot").await?.ok_or("slot missing")?;
    assert!(after.confirmed_flush_lsn > state.confirmed_flush_lsn);
    assert!(pg.slot_state("absent_slot").await?.is_none());
    assert!(!pg.terminate_walsender("probe_slot").await?);
    assert!(pg.current_wal_lsn().await? >= created);
    Ok(())
}

#[tokio::test]
async fn max_wal_senders_option_sets_the_server_limit() -> TestResult {
    let pg = Postgres::start_with(PostgresOptions {
        max_wal_senders: Some(1),
        ..PostgresOptions::default()
    })
    .await?;
    let senders: String = pg
        .client()
        .await?
        .query_one("SHOW max_wal_senders", &[])
        .await?
        .try_get(0)?;
    assert_eq!(senders, "1");
    Ok(())
}

#[tokio::test]
async fn tls_option_enables_ssl_with_generated_ca() -> TestResult {
    let pg = Postgres::start_with(PostgresOptions {
        tls: true,
        ..PostgresOptions::default()
    })
    .await?;
    let ssl: String = pg
        .client()
        .await?
        .query_one("SHOW ssl", &[])
        .await?
        .try_get(0)?;
    assert_eq!(ssl, "on");
    let ca = std::fs::read_to_string(pg.ca_file().ok_or("no CA file")?)?;
    assert!(ca.starts_with("-----BEGIN CERTIFICATE-----"));
    Ok(())
}
