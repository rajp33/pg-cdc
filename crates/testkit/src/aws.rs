use std::time::{Duration, Instant};

use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_s3::config::Credentials;
use aws_smithy_http_client::tls::{Provider, rustls_provider::CryptoMode};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage};

use crate::error::TestkitError;

pub const MOTO_IMAGE: &str = "motoserver/moto";
pub const MOTO_TAG: &str = "5.2.4";

#[derive(Debug)]
pub struct AwsEmulator {
    _container: ContainerAsync<GenericImage>,
    endpoint: String,
}

impl AwsEmulator {
    pub async fn start() -> Result<Self, TestkitError> {
        let container = GenericImage::new(MOTO_IMAGE, MOTO_TAG)
            .with_exposed_port(ContainerPort::Tcp(5000))
            .with_wait_for(WaitFor::Nothing)
            .start()
            .await?;
        let port = container.get_host_port_ipv4(5000).await?;
        let emulator = Self {
            _container: container,
            endpoint: format!("http://127.0.0.1:{port}"),
        };
        emulator.wait_until_ready().await?;
        Ok(emulator)
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub async fn sdk_config(&self) -> SdkConfig {
        let http_client = aws_smithy_http_client::Builder::new()
            .tls_provider(Provider::Rustls(CryptoMode::Ring))
            .build_https();
        aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new(
                "test",
                "test",
                None,
                None,
                "pg-cdc-testkit",
            ))
            .endpoint_url(&self.endpoint)
            .http_client(http_client)
            .load()
            .await
    }

    pub async fn s3(&self) -> aws_sdk_s3::Client {
        let config = self.sdk_config().await;
        aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::config::Builder::from(&config)
                .force_path_style(true)
                .build(),
        )
    }

    pub async fn sns(&self) -> aws_sdk_sns::Client {
        aws_sdk_sns::Client::new(&self.sdk_config().await)
    }

    pub async fn sqs(&self) -> aws_sdk_sqs::Client {
        aws_sdk_sqs::Client::new(&self.sdk_config().await)
    }

    async fn wait_until_ready(&self) -> Result<(), TestkitError> {
        let s3 = self.s3().await;
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if s3.list_buckets().send().await.is_ok() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(TestkitError::Timeout("moto to accept requests"));
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
}
