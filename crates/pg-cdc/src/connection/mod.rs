mod control;
mod password;
mod rds_iam;
mod tls;

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use pg_cdc_replication::TlsSetup;
use rustls_pki_types::ServerName;
use secrecy::SecretString;

use crate::config::TlsMode;

pub use control::{
    CONNECT_TIMEOUT, ControlConnection, connect_control, connect_control_with_timeout,
};
pub use password::PasswordProvider;
pub use rds_iam::RdsIamProvider;

pub const SESSION_OPTIONS: &str = "-c DateStyle=ISO,MDY -c TimeZone=UTC -c IntervalStyle=postgres -c extra_float_digits=1 -c bytea_output=hex";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsConfig {
    pub mode: TlsMode,
    pub ca_file: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct ConnectParams {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: Option<SecretString>,
    pub tls: TlsConfig,
}

#[async_trait]
pub trait ConnectionProvider: Send + Sync {
    async fn params(&self) -> Result<ConnectParams, ConnectError>;
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    #[error("cannot generate an RDS IAM token: {0}")]
    Token(String),
    #[error("cannot read CA file {path}: {source}")]
    CaFile {
        path: PathBuf,
        source: rustls_pki_types::pem::Error,
    },
    #[error("CA file {0} holds no certificates")]
    EmptyCaFile(PathBuf),
    #[error("TLS setup failed: {0}")]
    Tls(#[from] rustls::Error),
    #[error("TLS verifier setup failed: {0}")]
    Verifier(#[from] rustls::client::VerifierBuilderError),
    #[error("control connection failed: {0}")]
    Postgres(#[from] tokio_postgres::Error),
    #[error("control connection timed out after {0:?}")]
    Timeout(std::time::Duration),
    #[error("host {0:?} is not a valid TLS server name")]
    ServerName(String),
}

pub fn replication_tls(params: &ConnectParams) -> Result<Option<TlsSetup>, ConnectError> {
    let Some(config) = tls::client_config(&params.tls)? else {
        return Ok(None);
    };
    let server_name = ServerName::try_from(params.host.clone())
        .map_err(|_| ConnectError::ServerName(params.host.clone()))?;
    Ok(Some(TlsSetup {
        config: Arc::new(config),
        server_name,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_params_debug_redacts_password() {
        let params = ConnectParams {
            host: "db".to_owned(),
            port: 5432,
            database: "app".to_owned(),
            user: "pgcdc".to_owned(),
            password: Some(SecretString::from("p@ss w:rd/'\"&=%".to_owned())),
            tls: TlsConfig {
                mode: TlsMode::VerifyFull,
                ca_file: None,
            },
        };
        let debug = format!("{params:?}");
        assert!(!debug.contains("p@ss"), "{debug}");
        assert!(debug.contains("pgcdc"));
    }

    fn params(host: &str, mode: TlsMode) -> ConnectParams {
        ConnectParams {
            host: host.to_owned(),
            port: 5432,
            database: "app".to_owned(),
            user: "pgcdc".to_owned(),
            password: None,
            tls: TlsConfig {
                mode,
                ca_file: None,
            },
        }
    }

    #[test]
    fn replication_tls_follows_the_mode_and_takes_the_server_name_from_the_host() {
        assert!(
            replication_tls(&params("db.internal", TlsMode::Disable))
                .unwrap()
                .is_none()
        );
        let by_name = replication_tls(&params("db.internal", TlsMode::VerifyFull))
            .unwrap()
            .unwrap();
        assert_eq!(
            by_name.server_name,
            ServerName::try_from("db.internal").unwrap()
        );
        let by_address = replication_tls(&params("127.0.0.1", TlsMode::Require))
            .unwrap()
            .unwrap();
        assert!(matches!(by_address.server_name, ServerName::IpAddress(_)));
        let error = replication_tls(&params("not a host!", TlsMode::Require)).unwrap_err();
        assert!(matches!(error, ConnectError::ServerName(host) if host == "not a host!"));
    }
}
