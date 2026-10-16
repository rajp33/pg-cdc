use std::io::Write as _;
use std::time::Duration;

use anyhow::Context as _;
use aws_sdk_sns::types::PublishBatchRequestEntry;
use pg_cdc_testkit::{AwsEmulator, OtlpReceiver, Received};
use serde::Serialize;
use tokio::task::JoinSet;
use tokio::time::Instant;

use super::rig::{
    self, Bench, ConfigSpec, DRAIN_LIMIT, METRIC_EXPORT_INTERVAL, OFFERED_RATE, SUSTAINED_WINDOW,
    SinkSpec, StoreSpec,
};
use super::{LoadOptions, Verdict, criterion_four, write_report};
use crate::measure::{self, ClockOffset, Machine};

const SLOT: &str = "sns_slot";
const BUCKET: &str = "load-state";
const TOPIC: &str = "load";
const CEILING_TOPIC: &str = "load-ceiling";
const CEILING_WORKERS: usize = 32;
const CEILING_ENTRIES: usize = 10;
const CEILING_WINDOW: Duration = Duration::from_secs(20);
const CEILING_BODY_BYTES: usize = 300;
const DELIVERED: &str = "pgcdc.events";

#[derive(Debug, Serialize)]
struct MotoCeiling {
    workers: usize,
    entries_per_call: usize,
    window_s: f64,
    calls: u64,
    entries: u64,
    entries_per_s: f64,
}

#[derive(Debug, Serialize)]
struct Report {
    scenario: &'static str,
    machine: Machine,
    real_aws: bool,
    topic_arn: String,
    workload: String,
    config: String,
    offered_rate: u32,
    window_s: f64,
    written_events: u64,
    delivered_events: f64,
    delivered_rate: f64,
    max_lag_bytes: u64,
    final_lag_bytes: u64,
    kept_up: bool,
    drained: bool,
    histogram_events: u64,
    p99_upper_bound_s: Option<f64>,
    clock_offsets: Vec<ClockOffset>,
    p99_verified_upper_bound_s: Option<f64>,
    moto_ceiling: Option<MotoCeiling>,
    criterion_4: Verdict,
}

fn delivered(received: &Received) -> f64 {
    received
        .sum(DELIVERED, &[("outcome", "delivered")])
        .unwrap_or(0.0)
}

async fn moto_ceiling(aws: &AwsEmulator) -> anyhow::Result<MotoCeiling> {
    let client = aws.sns().await;
    let topic_arn = client
        .create_topic()
        .name(CEILING_TOPIC)
        .send()
        .await
        .map_err(|error| anyhow::anyhow!("{}", aws_sdk_sns::error::DisplayErrorContext(error)))?
        .topic_arn()
        .context("CreateTopic returned no ARN")?
        .to_owned();
    let body = "x".repeat(CEILING_BODY_BYTES);
    let started = Instant::now();
    let deadline = started + CEILING_WINDOW;
    let mut workers = JoinSet::new();
    for worker in 0..CEILING_WORKERS {
        let client = client.clone();
        let topic_arn = topic_arn.clone();
        let body = body.clone();
        workers.spawn(async move {
            let mut calls: u64 = 0;
            let mut entries: u64 = 0;
            while Instant::now() < deadline {
                let mut request = client.publish_batch().topic_arn(&topic_arn);
                for entry in 0..CEILING_ENTRIES {
                    request = request.publish_batch_request_entries(
                        PublishBatchRequestEntry::builder()
                            .id(format!("{worker}-{entry}"))
                            .message(&body)
                            .build()?,
                    );
                }
                let output = request.send().await.map_err(|error| {
                    anyhow::anyhow!("{}", aws_sdk_sns::error::DisplayErrorContext(error))
                })?;
                calls += 1;
                entries += u64::try_from(output.successful().len())?;
            }
            anyhow::Ok((calls, entries))
        });
    }
    let mut calls = 0;
    let mut entries = 0;
    while let Some(joined) = workers.join_next().await {
        let (worker_calls, worker_entries) = joined.context("a PublishBatch worker panicked")??;
        calls += worker_calls;
        entries += worker_entries;
    }
    let window = started.elapsed();
    Ok(MotoCeiling {
        workers: CEILING_WORKERS,
        entries_per_call: CEILING_ENTRIES,
        window_s: window.as_secs_f64(),
        calls,
        entries,
        entries_per_s: rig::per_second(entries, window),
    })
}

struct Target {
    aws: Option<AwsEmulator>,
    topic_arn: String,
    store: StoreSpec,
    env: Vec<(String, String)>,
}

async fn target(bench: &Bench, options: &LoadOptions) -> anyhow::Result<Target> {
    let mut env = vec![(
        METRIC_EXPORT_INTERVAL.0.to_owned(),
        METRIC_EXPORT_INTERVAL.1.to_owned(),
    )];
    if options.real_aws {
        let topic_arn = options
            .topic_arn
            .clone()
            .context("--real-aws needs --topic-arn")?;
        let store = StoreSpec::Filesystem(bench.scratch("store").await?);
        return Ok(Target {
            aws: None,
            topic_arn,
            store,
            env,
        });
    }
    anyhow::ensure!(
        options.topic_arn.is_none(),
        "--topic-arn needs --real-aws; the moto run creates its own topic"
    );
    let aws = AwsEmulator::start().await?;
    let topic_arn = aws
        .sns()
        .await
        .create_topic()
        .name(TOPIC)
        .send()
        .await
        .map_err(|error| anyhow::anyhow!("{}", aws_sdk_sns::error::DisplayErrorContext(error)))?
        .topic_arn()
        .context("CreateTopic returned no ARN")?
        .to_owned();
    aws.s3()
        .await
        .create_bucket()
        .bucket(BUCKET)
        .send()
        .await
        .map_err(|error| anyhow::anyhow!("{}", aws_sdk_s3::error::DisplayErrorContext(error)))?;
    env.extend([
        ("AWS_ENDPOINT_URL_SNS".to_owned(), aws.endpoint().to_owned()),
        ("AWS_ENDPOINT_URL_S3".to_owned(), aws.endpoint().to_owned()),
        ("AWS_ACCESS_KEY_ID".to_owned(), "test".to_owned()),
        ("AWS_SECRET_ACCESS_KEY".to_owned(), "test".to_owned()),
        ("AWS_REGION".to_owned(), "us-east-1".to_owned()),
        ("AWS_EC2_METADATA_DISABLED".to_owned(), "true".to_owned()),
    ]);
    Ok(Target {
        aws: Some(aws),
        topic_arn,
        store: StoreSpec::S3 {
            bucket: BUCKET.to_owned(),
        },
        env,
    })
}

pub(super) async fn run(options: &LoadOptions) -> anyhow::Result<()> {
    let mut bench = Bench::start(&[SLOT]).await?;
    let target = target(&bench, options).await?;
    if options.real_aws {
        "not used (--real-aws)".clone_into(&mut bench.machine.moto);
    }
    let receiver = OtlpReceiver::start().await?;
    let spec = ConfigSpec {
        slot: SLOT.to_owned(),
        sink: SinkSpec::Sns {
            topic_arn: target.topic_arn.clone(),
        },
        store: target.store.clone(),
        max_inflight_bytes: None,
        park_after: None,
        otlp_endpoint: Some(receiver.endpoint()),
    };
    let (mut process, config) = bench
        .spawn(&options.binary, &spec, &target.env, "sns")
        .await?;
    let before = delivered(&receiver.received());
    let mut clock_offsets = vec![measure::clock_offset(&bench.client).await?];
    let run = rig::sustain(
        &bench,
        &process,
        rig::workload(OFFERED_RATE),
        SUSTAINED_WINDOW,
    )
    .await?;
    let delivered_events = delivered(&receiver.received()) - before;
    let end = rig::last_lsn(&run.ledger)?;
    let drained = rig::drain(&process, end, Instant::now(), DRAIN_LIMIT)
        .await
        .is_ok();
    let latency = rig::latency(&receiver, run.written()).await;
    clock_offsets.push(measure::clock_offset(&bench.client).await?);
    let p99_verified = rig::verified_p99(latency, run.written(), drained, &clock_offsets);
    rig::stop(&mut process).await?;
    let moto_ceiling = match &target.aws {
        Some(aws) => Some(moto_ceiling(aws).await?),
        None => None,
    };
    let delivered_rate = delivered_events / run.elapsed.as_secs_f64().max(f64::EPSILON);
    let criterion_4 = criterion_four(
        delivered_rate,
        run.kept_up(),
        p99_verified,
        moto_ceiling.as_ref().map(|ceiling| ceiling.entries_per_s),
    );
    writeln!(
        std::io::stdout(),
        "load sns: offered {OFFERED_RATE}/s, delivered {delivered_rate:.0}/s, kept up {}, p99 upper bound {:?} s ({} of {} observed, verified with clock offsets {:?} ms: {:?} s), moto ceiling {:?}/s, criterion 4 {}",
        run.kept_up(),
        latency.p99_upper_bound_s,
        latency.observed,
        run.written(),
        clock_offsets
            .iter()
            .map(|offset| offset.offset_ms.round())
            .collect::<Vec<_>>(),
        p99_verified,
        moto_ceiling.as_ref().map(|ceiling| ceiling.entries_per_s),
        serde_json::to_string(&criterion_4)?,
    )?;
    let report = Report {
        scenario: "sns",
        machine: bench.machine.clone(),
        real_aws: options.real_aws,
        topic_arn: target.topic_arn.clone(),
        workload: format!("{:?}", rig::workload(OFFERED_RATE)),
        config,
        offered_rate: OFFERED_RATE,
        window_s: run.elapsed.as_secs_f64(),
        written_events: run.written(),
        delivered_events,
        delivered_rate,
        max_lag_bytes: run.max_lag_bytes,
        final_lag_bytes: run.final_lag_bytes,
        kept_up: run.kept_up(),
        drained,
        histogram_events: latency.observed,
        p99_upper_bound_s: latency.p99_upper_bound_s,
        clock_offsets,
        p99_verified_upper_bound_s: p99_verified,
        moto_ceiling,
        criterion_4,
    };
    write_report(options, "sns", &report).await
}
