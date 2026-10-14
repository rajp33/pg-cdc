use async_trait::async_trait;
use aws_config::SdkConfig;
use aws_sdk_rds::auth_token::{AuthTokenGenerator, Config};
use secrecy::SecretString;

use crate::aws::aws_loader;

use super::{ConnectError, ConnectParams, ConnectionProvider};

#[derive(Debug, Clone)]
pub struct RdsIamProvider {
    base: ConnectParams,
    sdk: SdkConfig,
}

impl RdsIamProvider {
    pub fn new(base: ConnectParams, sdk: SdkConfig) -> Self {
        Self { base, sdk }
    }

    pub async fn from_environment(base: ConnectParams, region: Option<String>) -> Self {
        Self::new(base, aws_loader(region).load().await)
    }
}

#[async_trait]
impl ConnectionProvider for RdsIamProvider {
    async fn params(&self) -> Result<ConnectParams, ConnectError> {
        let mut builder = Config::builder()
            .hostname(&self.base.host)
            .port(u64::from(self.base.port))
            .username(&self.base.user);
        if let Some(region) = self.sdk.region() {
            builder = builder.region(region.clone());
        }
        let config = builder
            .build()
            .map_err(|error| ConnectError::Token(error.to_string()))?;
        let token = AuthTokenGenerator::new(config)
            .auth_token(&self.sdk)
            .await
            .map_err(|error| ConnectError::Token(error.to_string()))?;
        Ok(ConnectParams {
            password: Some(SecretString::from(token.as_str().to_owned())),
            ..self.base.clone()
        })
    }
}
