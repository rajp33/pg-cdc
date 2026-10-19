use std::sync::Arc;
use std::time::Duration;

use pg_cdc_decoder::Lsn;
use rustls::ClientConfig;
use rustls_pki_types::ServerName;
use secrecy::SecretString;

#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Option<SecretString>,
    pub database: String,
    pub options: String,
    pub application_name: String,
    pub tls: Option<TlsSetup>,
    pub slot: String,
    pub publication: String,
    pub start_lsn: Lsn,
    pub status_interval: Duration,
    pub connect_timeout: Duration,
    pub buffer: usize,
}

#[derive(Debug, Clone)]
pub struct TlsSetup {
    pub config: Arc<ClientConfig>,
    pub server_name: ServerName<'static>,
}

#[cfg(test)]
mod tests {
    use crate::fake;

    #[test]
    fn debug_never_shows_the_password() {
        let mut config = fake::config();
        config.password = Some(secrecy::SecretString::from("p@ss w:rd/'\"&=%".to_owned()));
        config.tls = Some(fake::tls_setup());
        let debug = format!("{config:?}");
        assert!(!debug.contains("p@ss"), "{debug}");
        assert!(debug.contains("md5_user"), "{debug}");
    }
}
