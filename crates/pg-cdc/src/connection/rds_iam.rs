use async_trait::async_trait;
use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_rds::auth_token::{AuthTokenGenerator, Config};
use aws_smithy_http_client::tls::{Provider, rustls_provider::CryptoMode};
use secrecy::SecretString;

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
        let http_client = aws_smithy_http_client::Builder::new()
            .tls_provider(Provider::Rustls(CryptoMode::Ring))
            .build_https();
        let mut loader = aws_config::defaults(BehaviorVersion::latest()).http_client(http_client);
        if let Some(region) = region {
            loader = loader.region(Region::new(region));
        }
        Self::new(base, loader.load().await)
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
