use std::time::{Duration, Instant};

use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_s3::config::Credentials;
use aws_sdk_sqs::types::{Message, MessageSystemAttributeName, QueueAttributeName};
use aws_smithy_http_client::tls::{Provider, rustls_provider::CryptoMode};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage};

use crate::error::TestkitError;

pub const MOTO_IMAGE: &str = "motoserver/moto";
pub const MOTO_TAG: &str = "5.2.4";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicQueue {
    pub topic_arn: String,
    pub queue_url: String,
}

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

    pub async fn topic_with_queue(
        &self,
        name: &str,
        fifo: bool,
    ) -> Result<TopicQueue, TestkitError> {
        let name = if fifo {
            format!("{name}.fifo")
        } else {
            name.to_owned()
        };
        let sqs = self.sqs().await;
        let mut queue = sqs.create_queue().queue_name(&name);
        if fifo {
            queue = queue.attributes(QueueAttributeName::FifoQueue, "true");
        }
        let queue_url = queue
            .send()
            .await
            .map_err(aws_error)?
            .queue_url()
            .ok_or_else(|| TestkitError::Aws("CreateQueue returned no URL".to_owned()))?
            .to_owned();
        let queue_arn = sqs
            .get_queue_attributes()
            .queue_url(&queue_url)
            .attribute_names(QueueAttributeName::QueueArn)
            .send()
            .await
            .map_err(aws_error)?
            .attributes()
            .and_then(|attributes| attributes.get(&QueueAttributeName::QueueArn))
            .cloned()
            .ok_or_else(|| TestkitError::Aws("GetQueueAttributes returned no ARN".to_owned()))?;
        let sns = self.sns().await;
        let mut topic = sns.create_topic().name(&name);
        if fifo {
            topic = topic.attributes("FifoTopic", "true");
        }
        let topic_arn = topic
            .send()
            .await
            .map_err(aws_error)?
            .topic_arn()
            .ok_or_else(|| TestkitError::Aws("CreateTopic returned no ARN".to_owned()))?
            .to_owned();
        sns.subscribe()
            .topic_arn(&topic_arn)
            .protocol("sqs")
            .endpoint(queue_arn)
            .attributes("RawMessageDelivery", "true")
            .send()
            .await
            .map_err(aws_error)?;
        Ok(TopicQueue {
            topic_arn,
            queue_url,
        })
    }

    pub async fn receive(
        &self,
        queue_url: &str,
        count: usize,
        timeout: Duration,
    ) -> Result<Vec<Message>, TestkitError> {
        let sqs = self.sqs().await;
        let deadline = Instant::now() + timeout;
        let mut messages = Vec::new();
        while messages.len() < count {
            if Instant::now() >= deadline {
                return Err(TestkitError::Timeout("messages on the SQS queue"));
            }
            let received = sqs
                .receive_message()
                .queue_url(queue_url)
                .max_number_of_messages(10)
                .wait_time_seconds(1)
                .message_attribute_names("All")
                .message_system_attribute_names(MessageSystemAttributeName::All)
                .send()
                .await
                .map_err(aws_error)?;
            for message in received.messages() {
                if let Some(handle) = message.receipt_handle() {
                    sqs.delete_message()
                        .queue_url(queue_url)
                        .receipt_handle(handle)
                        .send()
                        .await
                        .map_err(aws_error)?;
                }
            }
            messages.extend(received.messages().iter().cloned());
        }
        Ok(messages)
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

fn aws_error(error: impl std::error::Error) -> TestkitError {
    TestkitError::Aws(aws_sdk_sqs::error::DisplayErrorContext(error).to_string())
}
