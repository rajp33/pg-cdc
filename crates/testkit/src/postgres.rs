use std::path::Path;
use std::time::{Duration, Instant};

use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio_postgres::{Client, NoTls};

use crate::error::TestkitError;
use crate::tls::TestCertificates;

pub const PG_VERSION_ENV: &str = "PGCDC_TEST_PG_VERSION";
pub const DEFAULT_PG_VERSION: u8 = 17;
pub const SESSION_OPTIONS: &str = "-c DateStyle=ISO,MDY -c TimeZone=UTC -c IntervalStyle=postgres -c extra_float_digits=1 -c bytea_output=hex";

const HOST: &str = "127.0.0.1";
const USER: &str = "postgres";
const PASSWORD: &str = "postgres";
const DATABASE: &str = "postgres";
const READY_TIMEOUT: Duration = Duration::from_secs(90);
const POLL: Duration = Duration::from_millis(100);
const FIXED_SERVER_ARGS: [&str; 8] = [
    "-c",
    "fsync=off",
    "-c",
    "autovacuum=off",
    "-c",
    "max_wal_senders=20",
    "-c",
    "max_replication_slots=20",
];

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PostgresOptions {
    pub tls: bool,
    pub wal_level: WalLevel,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WalLevel {
    #[default]
    Logical,
    Replica,
}

impl WalLevel {
    fn as_str(self) -> &'static str {
        match self {
            Self::Logical => "logical",
            Self::Replica => "replica",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotState {
    pub confirmed_flush_lsn: Option<u64>,
    pub active: bool,
    pub active_pid: Option<i32>,
    pub wal_status: Option<String>,
}

#[derive(Debug)]
pub struct Postgres {
    _container: ContainerAsync<GenericImage>,
    port: u16,
    version: u8,
    certificates: Option<TestCertificates>,
}

pub fn parse_pg_version(value: Option<&str>) -> Result<u8, TestkitError> {
    let Some(text) = value else {
        return Ok(DEFAULT_PG_VERSION);
    };
    match text.trim().parse::<u8>() {
        Ok(version @ 14..=18) => Ok(version),
        _ => Err(TestkitError::UnsupportedVersion(text.to_owned())),
    }
}

pub fn parse_lsn(text: &str) -> Result<u64, TestkitError> {
    let invalid = || TestkitError::InvalidLsn(text.to_owned());
    let (high, low) = text.split_once('/').ok_or_else(invalid)?;
    let high = u32::from_str_radix(high, 16).map_err(|_| invalid())?;
    let low = u32::from_str_radix(low, 16).map_err(|_| invalid())?;
    Ok((u64::from(high) << 32) | u64::from(low))
}

fn server_args(wal_level: WalLevel) -> Vec<String> {
    let mut args = vec![
        "postgres".to_owned(),
        "-c".to_owned(),
        format!("wal_level={}", wal_level.as_str()),
    ];
    args.extend(FIXED_SERVER_ARGS.iter().map(|arg| (*arg).to_owned()));
    args
}

fn tls_script(wal_level: WalLevel) -> String {
    format!(
        "set -e; mkdir -p /etc/pgtls; cp /tls/server.crt /tls/server.key /etc/pgtls/; \
         chown -R postgres:postgres /etc/pgtls; chmod 600 /etc/pgtls/server.key; \
         exec docker-entrypoint.sh {} -c ssl=on \
         -c ssl_cert_file=/etc/pgtls/server.crt -c ssl_key_file=/etc/pgtls/server.key",
        server_args(wal_level).join(" ")
    )
}

impl Postgres {
    pub async fn start() -> Result<Self, TestkitError> {
        Self::start_with(PostgresOptions::default()).await
    }

    pub async fn start_with(options: PostgresOptions) -> Result<Self, TestkitError> {
        let version = parse_pg_version(std::env::var(PG_VERSION_ENV).ok().as_deref())?;
        let image = GenericImage::new("postgres", &format!("{version}-alpine"))
            .with_exposed_port(ContainerPort::Tcp(5432))
            .with_wait_for(WaitFor::Nothing);
        let (container, certificates) = if options.tls {
            let certificates = TestCertificates::generate()?;
            let container = image
                .with_entrypoint("sh")
                .with_env_var("POSTGRES_PASSWORD", PASSWORD)
                .with_copy_to(
                    "/tls/server.crt",
                    certificates.server_pem().as_bytes().to_vec(),
                )
                .with_copy_to(
                    "/tls/server.key",
                    certificates.server_key_pem().as_bytes().to_vec(),
                )
                .with_cmd(["-c".to_owned(), tls_script(options.wal_level)])
                .start()
                .await?;
            (container, Some(certificates))
        } else {
            let container = image
                .with_env_var("POSTGRES_PASSWORD", PASSWORD)
                .with_cmd(server_args(options.wal_level))
                .start()
                .await?;
            (container, None)
        };
        let port = container.get_host_port_ipv4(5432).await?;
        let postgres = Self {
            _container: container,
            port,
            version,
            certificates,
        };
        postgres.wait_until_ready().await?;
        Ok(postgres)
    }

    pub fn host(&self) -> &'static str {
        HOST
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn user(&self) -> &'static str {
        USER
    }

    pub fn password(&self) -> &'static str {
        PASSWORD
    }

    pub fn database(&self) -> &'static str {
        DATABASE
    }

    pub fn version(&self) -> u8 {
        self.version
    }

    pub fn ca_file(&self) -> Option<&Path> {
        self.certificates.as_ref().map(TestCertificates::ca_file)
    }

    pub async fn client(&self) -> Result<Client, TestkitError> {
        let mut config = tokio_postgres::Config::new();
        config
            .host(HOST)
            .port(self.port)
            .user(USER)
            .password(PASSWORD)
            .dbname(DATABASE)
            .options(SESSION_OPTIONS);
        let (client, connection) = config.connect(NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(client)
    }

    pub async fn execute(&self, sql: &str) -> Result<(), TestkitError> {
        self.client().await?.batch_execute(sql).await?;
        Ok(())
    }

    pub async fn create_publication(
        &self,
        name: &str,
        tables: &[&str],
    ) -> Result<(), TestkitError> {
        self.execute(&format!(
            "CREATE PUBLICATION {name} FOR TABLE {}",
            tables.join(", ")
        ))
        .await
    }

    pub async fn create_slot(&self, name: &str) -> Result<u64, TestkitError> {
        let row = self
            .client()
            .await?
            .query_one(
                "SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')",
                &[&name],
            )
            .await?;
        parse_lsn(&row.try_get::<_, String>(0)?)
    }

    pub async fn slot_state(&self, name: &str) -> Result<Option<SlotState>, TestkitError> {
        let row = self
            .client()
            .await?
            .query_opt(
                "SELECT confirmed_flush_lsn::text, active, active_pid, wal_status \
                 FROM pg_replication_slots WHERE slot_name = $1",
                &[&name],
            )
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let lsn: Option<String> = row.try_get(0)?;
        Ok(Some(SlotState {
            confirmed_flush_lsn: lsn.as_deref().map(parse_lsn).transpose()?,
            active: row.try_get(1)?,
            active_pid: row.try_get(2)?,
            wal_status: row.try_get(3)?,
        }))
    }

    pub async fn wait_for_slot(
        &self,
        name: &str,
        timeout: Duration,
        ready: impl Fn(&SlotState) -> bool,
    ) -> Result<SlotState, TestkitError> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(state) = self.slot_state(name).await?
                && ready(&state)
            {
                return Ok(state);
            }
            if Instant::now() >= deadline {
                return Err(TestkitError::Timeout("replication slot state"));
            }
            tokio::time::sleep(POLL).await;
        }
    }

    pub async fn sent_lsn(&self, slot: &str) -> Result<Option<u64>, TestkitError> {
        let row = self
            .client()
            .await?
            .query_opt(
                "SELECT r.sent_lsn::text FROM pg_stat_replication r \
                 JOIN pg_replication_slots s ON s.active_pid = r.pid WHERE s.slot_name = $1",
                &[&slot],
            )
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let lsn: Option<String> = row.try_get(0)?;
        lsn.as_deref().map(parse_lsn).transpose()
    }

    pub async fn current_wal_lsn(&self) -> Result<u64, TestkitError> {
        let row = self
            .client()
            .await?
            .query_one("SELECT pg_current_wal_lsn()::text", &[])
            .await?;
        parse_lsn(&row.try_get::<_, String>(0)?)
    }

    pub async fn capture_pgoutput(
        &self,
        slot: &str,
        publication: &str,
    ) -> Result<Vec<Vec<u8>>, TestkitError> {
        let rows = self
            .client()
            .await?
            .query(
                "SELECT data FROM pg_logical_slot_get_binary_changes($1, NULL, NULL, \
                 'proto_version', '1', 'publication_names', $2)",
                &[&slot, &publication],
            )
            .await?;
        rows.iter()
            .map(|row| row.try_get::<_, Vec<u8>>(0).map_err(TestkitError::from))
            .collect()
    }

    pub async fn terminate_walsender(&self, slot: &str) -> Result<bool, TestkitError> {
        let row = self
            .client()
            .await?
            .query_one(
                "SELECT coalesce(bool_or(pg_terminate_backend(active_pid)), false) \
                 FROM pg_replication_slots WHERE slot_name = $1 AND active_pid IS NOT NULL",
                &[&slot],
            )
            .await?;
        Ok(row.try_get(0)?)
    }

    async fn wait_until_ready(&self) -> Result<(), TestkitError> {
        let deadline = Instant::now() + READY_TIMEOUT;
        loop {
            if let Ok(client) = self.client().await
                && client.simple_query("SELECT 1").await.is_ok()
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(TestkitError::Timeout("postgres to accept connections"));
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
}
