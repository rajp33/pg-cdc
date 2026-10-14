mod control;
mod password;
mod rds_iam;
mod tls;

use std::path::PathBuf;

use async_trait::async_trait;
use secrecy::SecretString;

use crate::config::TlsMode;

pub use control::{ControlConnection, connect_control};
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
}
