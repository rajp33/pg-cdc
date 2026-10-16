use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use pg_cdc::event::EventId;
use pg_cdc::order::OrderKeyId;
use pg_cdc::sink::discard::DiscardSink;
use pg_cdc::sink::sns::SnsSink;
use pg_cdc::sink::stdout::StdoutSink;
use pg_cdc::sink::{AttributeValue, Outbound, Outcome, Sink, SinkCapabilities};
use pg_cdc_decoder::Lsn;
use pg_cdc_testkit::AwsEmulator;
use tokio::io::{AsyncBufReadExt, BufReader, DuplexStream};
use tokio::task::JoinHandle;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const WAIT: Duration = Duration::from_secs(30);
const PROBE_BYTES_CAP: usize = 1 << 20;

enum Destination {
    Lines {
        lines: Arc<Mutex<Vec<Vec<u8>>>>,
        reader: Option<JoinHandle<()>>,
    },
    Counter {
        sink: Arc<DiscardSink>,
        seen: u64,
    },
    Queue {
        aws: Box<AwsEmulator>,
        topic_arn: String,
        queue_url: String,
    },
}

enum Received {
    Bodies(Vec<Vec<u8>>),
    Count(u64),
}

impl Destination {
    fn lines(reader: DuplexStream) -> Self {
        let lines = Arc::new(Mutex::new(Vec::new()));
        let collected = Arc::clone(&lines);
        let reader = tokio::spawn(async move {
            let mut reader = BufReader::new(reader);
            loop {
                let mut line = Vec::new();
                match reader.read_until(b'\n', &mut line).await {
                    Ok(0) | Err(_) => return,
                    Ok(_) => {
                        line.pop();
                        let Ok(mut collected) = collected.lock() else {
                            return;
                        };
                        collected.push(line);
                    }
                }
            }
        });
        Self::Lines {
            lines,
            reader: Some(reader),
        }
    }

    async fn received(&mut self, count: usize) -> TestResult<Received> {
        match self {
            Self::Lines { lines, .. } => {
                let deadline = Instant::now() + WAIT;
                loop {
                    {
                        let mut lines = lines.lock().map_err(|_| "the line buffer is poisoned")?;
                        if lines.len() >= count {
                            return Ok(Received::Bodies(lines.drain(..).collect()));
                        }
                    }
                    if Instant::now() >= deadline {
                        return Err("timed out waiting for stdout lines".into());
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
            Self::Counter { sink, seen } => {
                let now = sink.events();
                let delta = now - *seen;
                *seen = now;
                Ok(Received::Count(delta))
            }
            Self::Queue { aws, queue_url, .. } => {
                let messages = aws.receive(queue_url, count, WAIT).await?;
                Ok(Received::Bodies(
                    messages
                        .iter()
                        .map(|message| message.body().unwrap_or_default().as_bytes().to_vec())
                        .collect(),
                ))
            }
        }
    }

    async fn fail(&mut self) -> TestResult<bool> {
        match self {
            Self::Lines { reader, .. } => {
                if let Some(reader) = reader.take() {
                    reader.abort();
                    let _ = reader.await;
                }
                Ok(true)
            }
            Self::Counter { .. } => Ok(false),
            Self::Queue { aws, topic_arn, .. } => {
                aws.sns()
                    .await
                    .delete_topic()
                    .topic_arn(topic_arn.as_str())
                    .send()
                    .await?;
                Ok(true)
            }
        }
    }
}

struct Messages {
    next: u32,
}

impl Messages {
    fn sized(&mut self, encoded_len: usize) -> Outbound {
        let ordinal = self.next;
        self.next += 1;
        let attributes = vec![
            (
                "schema".to_owned(),
                AttributeValue::String("public".to_owned()),
            ),
            (
                "table".to_owned(),
                AttributeValue::String("orders".to_owned()),
            ),
            ("op".to_owned(), AttributeValue::String("insert".to_owned())),
            ("n".to_owned(), AttributeValue::Number(ordinal.to_string())),
        ];
        let mut outbound = Outbound {
            id: EventId::new(Lsn(0x1A2B_3C4D), ordinal),
            order_key: OrderKeyId::of(format!("[{ordinal}]").as_bytes()),
            attributes,
            body: Bytes::new(),
        };
        let head = format!("{{\"ordinal\":{ordinal},\"pad\":\"");
        let tail = "\"}";
        let padding = encoded_len
            .saturating_sub(outbound.encoded_len())
            .saturating_sub(head.len() + tail.len());
        outbound.body = Bytes::from(format!("{head}{}{tail}", "x".repeat(padding)));
        outbound
    }

    fn small(&mut self, count: usize) -> Vec<Outbound> {
        (0..count).map(|_| self.sized(0)).collect()
    }
}

fn assert_valid(capabilities: SinkCapabilities) {
    assert!(capabilities.max_batch_messages >= 1, "{capabilities:?}");
    assert!(capabilities.max_batch_bytes >= 1, "{capabilities:?}");
    assert!(capabilities.max_message_bytes >= 1, "{capabilities:?}");
}

async fn assert_delivered(
    sink: &dyn Sink,
    destination: &mut Destination,
    batch: &[Outbound],
) -> TestResult {
    assert_eq!(
        sink.send(batch).await,
        vec![Outcome::Delivered; batch.len()]
    );
    match destination.received(batch.len()).await? {
        Received::Bodies(mut bodies) => {
            let mut sent: Vec<Vec<u8>> = batch.iter().map(|o| o.body.to_vec()).collect();
            bodies.sort_unstable();
            sent.sort_unstable();
            assert_eq!(bodies, sent);
        }
        Received::Count(count) => assert_eq!(count, u64::try_from(batch.len())?),
    }
    Ok(())
}

async fn conforms(sink: &dyn Sink, mut destination: Destination) -> TestResult {
    sink.preflight().await?;
    let limits = sink.capabilities();
    assert_valid(limits);
    let mut messages = Messages { next: 0 };

    assert_eq!(sink.send(&[]).await, Vec::new());

    let full = messages.small(limits.max_batch_messages);
    assert_delivered(sink, &mut destination, &full).await?;

    let batch_bytes = limits.max_batch_bytes.min(PROBE_BYTES_CAP);
    let count = limits.max_batch_messages.min(10);
    let mut heavy: Vec<Outbound> = (1..count)
        .map(|_| messages.sized(batch_bytes / count))
        .collect();
    let used: usize = heavy.iter().map(Outbound::encoded_len).sum();
    heavy.push(messages.sized(batch_bytes - used));
    assert_eq!(
        heavy.iter().map(Outbound::encoded_len).sum::<usize>(),
        batch_bytes
    );
    assert_delivered(sink, &mut destination, &heavy).await?;

    let largest = limits
        .max_message_bytes
        .min(limits.max_batch_bytes)
        .min(PROBE_BYTES_CAP);
    let single = [messages.sized(largest)];
    assert_eq!(single[0].encoded_len(), largest);
    assert_delivered(sink, &mut destination, &single).await?;

    if destination.fail().await? {
        let outcomes = sink.send(&messages.small(3)).await;
        assert_eq!(outcomes.len(), 3);
        for outcome in outcomes {
            assert!(matches!(outcome, Outcome::Retryable(_)), "{outcome:?}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn the_stdout_sink_conforms() -> TestResult {
    let (writer, reader) = tokio::io::duplex(1 << 16);
    let sink = StdoutSink::new(writer);
    conforms(&sink, Destination::lines(reader)).await
}

#[tokio::test]
async fn the_discard_sink_conforms() -> TestResult {
    let sink = Arc::new(DiscardSink::new());
    let destination = Destination::Counter {
        sink: Arc::clone(&sink),
        seen: 0,
    };
    conforms(sink.as_ref(), destination).await
}

async fn sns_conforms(fifo: bool) -> TestResult {
    let aws = AwsEmulator::start().await?;
    let route = aws.topic_with_queue("conformance", fifo).await?;
    let sink = SnsSink::new(aws.sns().await, "orders", route.topic_arn.clone(), None);
    let destination = Destination::Queue {
        aws: Box::new(aws),
        topic_arn: route.topic_arn,
        queue_url: route.queue_url,
    };
    conforms(&sink, destination).await
}

#[tokio::test]
async fn the_sns_sink_conforms_on_a_standard_topic() -> TestResult {
    sns_conforms(false).await
}

#[tokio::test]
async fn the_sns_sink_conforms_on_a_fifo_topic() -> TestResult {
    sns_conforms(true).await
}
