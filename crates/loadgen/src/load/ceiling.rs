use std::io::Write as _;

use pg_cdc_testkit::OtlpReceiver;
use serde::Serialize;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::rig::{
    self, Bench, ConfigSpec, DRAIN_LIMIT, METRIC_EXPORT_INTERVAL, OFFERED_RATE, SUSTAINED_WINDOW,
    SinkSpec, StoreSpec,
};
use super::{LoadOptions, write_report};
use crate::measure::Machine;
use crate::workload;

const SLOT: &str = "ceiling_slot";
const BACKLOG_EVENTS: u64 = 2_000_000;

#[derive(Debug, Serialize)]
struct Backlog {
    events: u64,
    wall_s: f64,
    events_per_s: f64,
    cpu_s: f64,
    peak_rss_bytes: u64,
}

#[derive(Debug, Serialize)]
struct Sustain {
    offered_rate: u32,
    window_s: f64,
    written_events: u64,
    achieved_rate: f64,
    max_lag_bytes: u64,
    final_lag_bytes: u64,
    kept_up: bool,
    drained: bool,
    histogram_events: u64,
    p99_upper_bound_s: Option<f64>,
}

#[derive(Debug, Serialize)]
struct Report {
    scenario: &'static str,
    machine: Machine,
    workload: String,
    backlog_config: String,
    sustained_config: String,
    backlog: Backlog,
    sustained: Sustain,
}

fn spec(store: StoreSpec, otlp_endpoint: Option<String>) -> ConfigSpec {
    ConfigSpec {
        slot: SLOT.to_owned(),
        sink: SinkSpec::Discard,
        store,
        max_inflight_bytes: None,
        park_after: None,
        otlp_endpoint,
    }
}

async fn backlog(bench: &Bench, options: &LoadOptions) -> anyhow::Result<(Backlog, String)> {
    let ledger = workload::write_events(
        &bench.client,
        &rig::workload(0),
        CancellationToken::new(),
        Some(BACKLOG_EVENTS),
    )
    .await?;
    let target = rig::last_lsn(&ledger)?;
    let events = u64::try_from(ledger.committed.len())?;
    let store = StoreSpec::Filesystem(bench.scratch("backlog-store").await?);
    let started = Instant::now();
    let (mut process, config) = bench
        .spawn(&options.binary, &spec(store, None), &[], "backlog")
        .await?;
    let drained = rig::drain(&process, target, started, DRAIN_LIMIT).await?;
    rig::stop(&mut process).await?;
    let backlog = Backlog {
        events,
        wall_s: drained.wall.as_secs_f64(),
        events_per_s: rig::per_second(events, drained.wall),
        cpu_s: drained.cpu.as_secs_f64(),
        peak_rss_bytes: drained.peak_rss_bytes,
    };
    Ok((backlog, config))
}

async fn sustained(bench: &Bench, options: &LoadOptions) -> anyhow::Result<(Sustain, String)> {
    let receiver = OtlpReceiver::start().await?;
    let store = StoreSpec::Filesystem(bench.scratch("sustained-store").await?);
    let env = [(
        METRIC_EXPORT_INTERVAL.0.to_owned(),
        METRIC_EXPORT_INTERVAL.1.to_owned(),
    )];
    let (mut process, config) = bench
        .spawn(
            &options.binary,
            &spec(store, Some(receiver.endpoint())),
            &env,
            "sustained",
        )
        .await?;
    let run = rig::sustain(
        bench,
        &process,
        rig::workload(OFFERED_RATE),
        SUSTAINED_WINDOW,
    )
    .await?;
    let target = rig::last_lsn(&run.ledger)?;
    let drained = rig::drain(&process, target, Instant::now(), DRAIN_LIMIT)
        .await
        .is_ok();
    let latency = rig::latency(&receiver, run.written()).await;
    rig::stop(&mut process).await?;
    let sustain = Sustain {
        offered_rate: OFFERED_RATE,
        window_s: run.elapsed.as_secs_f64(),
        written_events: run.written(),
        achieved_rate: run.achieved_rate(),
        max_lag_bytes: run.max_lag_bytes,
        final_lag_bytes: run.final_lag_bytes,
        kept_up: run.kept_up(),
        drained,
        histogram_events: latency.observed,
        p99_upper_bound_s: latency.p99_upper_bound_s,
    };
    Ok((sustain, config))
}

pub(super) async fn run(options: &LoadOptions) -> anyhow::Result<()> {
    let bench = Bench::start(&[SLOT]).await?;
    let (backlog, backlog_config) = backlog(&bench, options).await?;
    let (sustained, sustained_config) = sustained(&bench, options).await?;
    writeln!(
        std::io::stdout(),
        "load ceiling: backlog {:.0} events/s, {:.1} cpu s, peak rss {} MiB; sustained {:.0} events/s offered {}, kept up {}, p99 upper bound {:?} s",
        backlog.events_per_s,
        backlog.cpu_s,
        backlog.peak_rss_bytes >> 20,
        sustained.achieved_rate,
        sustained.offered_rate,
        sustained.kept_up,
        sustained.p99_upper_bound_s,
    )?;
    let report = Report {
        scenario: "ceiling",
        machine: bench.machine.clone(),
        workload: format!("{:?}", rig::workload(OFFERED_RATE)),
        backlog_config,
        sustained_config,
        backlog,
        sustained,
    };
    write_report(options, "ceiling", &report).await
}
