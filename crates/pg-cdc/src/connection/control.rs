use std::fmt;
use std::time::Duration;

use secrecy::ExposeSecret;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::task::JoinHandle;
use tokio_postgres::config::SslMode;
use tokio_postgres::{Client, Connection, NoTls};
use tokio_postgres_rustls::MakeRustlsConnect;

use super::{ConnectError, ConnectParams, SESSION_OPTIONS, tls};

pub struct ControlConnection {
    client: Client,
    task: JoinHandle<()>,
}

impl ControlConnection {
    pub fn client(&self) -> &Client {
        &self.client
    }
}

impl fmt::Debug for ControlConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlConnection").finish_non_exhaustive()
    }
}

impl Drop for ControlConnection {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

pub async fn connect_control(params: &ConnectParams) -> Result<ControlConnection, ConnectError> {
    connect_control_with_timeout(params, CONNECT_TIMEOUT).await
}

pub async fn connect_control_with_timeout(
    params: &ConnectParams,
    timeout: Duration,
) -> Result<ControlConnection, ConnectError> {
    tokio::time::timeout(timeout, establish(params, timeout))
        .await
        .map_err(|_| ConnectError::Timeout(timeout))?
}

async fn establish(
    params: &ConnectParams,
    timeout: Duration,
) -> Result<ControlConnection, ConnectError> {
    let mut config = tokio_postgres::Config::new();
    config
        .host(&params.host)
        .port(params.port)
        .dbname(&params.database)
        .user(&params.user)
        .options(SESSION_OPTIONS)
        .application_name("pg-cdc")
        .connect_timeout(timeout);
    if let Some(password) = &params.password {
        config.password(password.expose_secret());
    }
    match tls::client_config(&params.tls).await? {
        None => {
            config.ssl_mode(SslMode::Disable);
            let (client, connection) = config.connect(NoTls).await?;
            Ok(spawn(client, connection))
        }
        Some(tls_config) => {
            config.ssl_mode(SslMode::Require);
            let (client, connection) = config.connect(MakeRustlsConnect::new(tls_config)).await?;
            Ok(spawn(client, connection))
        }
    }
}

fn spawn<S, T>(client: Client, connection: Connection<S, T>) -> ControlConnection
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let task = tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::warn!(%error, "control connection closed");
        }
    });
    ControlConnection { client, task }
}
