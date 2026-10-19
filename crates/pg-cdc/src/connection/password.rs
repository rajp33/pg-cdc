use async_trait::async_trait;

use super::{ConnectError, ConnectParams, ConnectionProvider};

#[derive(Debug, Clone)]
pub struct PasswordProvider {
    params: ConnectParams,
}

impl PasswordProvider {
    pub fn new(params: ConnectParams) -> Self {
        Self { params }
    }
}

#[async_trait]
impl ConnectionProvider for PasswordProvider {
    async fn params(&self) -> Result<ConnectParams, ConnectError> {
        Ok(self.params.clone())
    }
}
