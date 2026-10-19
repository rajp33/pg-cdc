use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum ReplicationError {
    #[error("invalid replication config: {0}")]
    Config(&'static str),
    #[error("replication connection failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("TLS handshake failed: {0}")]
    Tls(std::io::Error),
    #[error("the server refused TLS")]
    TlsRefused,
    #[error("the server asked for a password and none is configured")]
    PasswordRequired,
    #[error("unsupported authentication method (code {0})")]
    UnsupportedAuthentication(i32),
    #[error("the server offers no supported SASL mechanism (offered: {0})")]
    UnsupportedSasl(String),
    #[error("SCRAM authentication failed: {0}")]
    Scram(std::io::Error),
    #[error("server error {sqlstate}: {message}")]
    Server { sqlstate: String, message: String },
    #[error(
        "the server reports client_encoding {}, not UTF8",
        .0.as_deref().unwrap_or("unset")
    )]
    ClientEncoding(Option<String>),
    #[error("protocol violation: {0}")]
    Protocol(String),
    #[error("the server closed the replication connection")]
    Closed,
    #[error("timed out after {0:?}")]
    Timeout(Duration),
    #[error("the replication worker failed: {0}")]
    Worker(String),
}

impl ReplicationError {
    pub fn sqlstate(&self) -> Option<&str> {
        match self {
            Self::Server { sqlstate, .. } => Some(sqlstate),
            _ => None,
        }
    }
}
