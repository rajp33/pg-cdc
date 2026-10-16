#[derive(Debug, thiserror::Error)]
pub enum TestkitError {
    #[error("PGCDC_TEST_PG_VERSION must be 14 to 18, got {0:?}")]
    UnsupportedVersion(String),
    #[error("container: {0}")]
    Container(#[from] testcontainers::TestcontainersError),
    #[error("postgres: {0}")]
    Postgres(#[from] tokio_postgres::Error),
    #[error("timed out waiting for {0}")]
    Timeout(&'static str),
    #[error("invalid LSN {0:?}")]
    InvalidLsn(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("fixture {0} is malformed")]
    MalformedFixture(String),
    #[error("certificate: {0}")]
    Certificate(#[from] rcgen::Error),
    #[error("transport: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("docker: {0}")]
    Docker(String),
    #[error("aws: {0}")]
    Aws(String),
}
