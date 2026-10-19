use std::fmt;
use std::time::Duration;

use tracing::{Event, Subscriber};
use tracing_subscriber::fmt::format::{Format, Json, Writer};
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{EnvFilter, Layer, Registry};

use crate::config::TelemetryConfig;
use crate::health::Health;
use crate::logs::NonBlocking;
use crate::metrics::AlreadyInstalled;
use crate::status::Status;

#[cfg(feature = "otel")]
mod otel;

pub const OTLP_ENDPOINT_VARIABLE: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
pub const TELEMETRY_FLUSH_TIMEOUT: Duration = Duration::from_secs(1);
pub const EXPORT_TIMEOUT: Duration = Duration::from_secs(5);
pub const DURATION_BUCKETS: [f64; 16] = [
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
];

#[derive(Debug, Clone, PartialEq)]
pub enum TelemetryPlan {
    Disabled,
    Enabled { endpoint: String, sample_ratio: f64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetryIdentity {
    pub pipeline: String,
    pub holder: String,
}

pub struct TelemetryLayers {
    #[cfg(feature = "otel")]
    layers: Vec<Box<dyn Layer<Registry> + Send + Sync>>,
}

impl TelemetryLayers {
    #[cfg(feature = "otel")]
    pub fn into_layer(self) -> Box<dyn Layer<Registry> + Send + Sync> {
        Box::new(self.layers)
    }

    #[cfg(not(feature = "otel"))]
    pub fn into_layer(self) -> Box<dyn Layer<Registry> + Send + Sync> {
        Box::new(tracing_subscriber::layer::Identity::new())
    }
}

impl fmt::Debug for TelemetryLayers {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TelemetryLayers")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TelemetryError {
    #[error("cannot build the OTLP {signal} exporter: {reason}")]
    Exporter {
        signal: &'static str,
        reason: String,
    },
    #[error(transparent)]
    Recorder(#[from] AlreadyInstalled),
}

#[derive(Debug)]
pub struct Telemetry {
    #[cfg(feature = "otel")]
    providers: Option<otel::Providers>,
}

impl Telemetry {
    pub fn install(
        plan: &TelemetryPlan,
        identity: &TelemetryIdentity,
        status: &Status,
        health: &Health,
    ) -> Result<(Self, Option<TelemetryLayers>), TelemetryError> {
        match plan {
            TelemetryPlan::Disabled => Ok((Self::disabled(), None)),
            #[cfg(feature = "otel")]
            TelemetryPlan::Enabled {
                endpoint,
                sample_ratio,
            } => Self::enable(endpoint, *sample_ratio, identity, status, health),
            #[cfg(not(feature = "otel"))]
            TelemetryPlan::Enabled { .. } => {
                let _ = (identity, status, health);
                tracing::warn!("telemetry is configured, but this build has no otel feature");
                Ok((Self::disabled(), None))
            }
        }
    }

    #[cfg(feature = "otel")]
    fn disabled() -> Self {
        Self { providers: None }
    }

    #[cfg(not(feature = "otel"))]
    fn disabled() -> Self {
        Self {}
    }

    #[cfg(feature = "otel")]
    fn enable(
        endpoint: &str,
        sample_ratio: f64,
        identity: &TelemetryIdentity,
        status: &Status,
        health: &Health,
    ) -> Result<(Self, Option<TelemetryLayers>), TelemetryError> {
        let (providers, layers) = otel::build(endpoint, sample_ratio, identity, status, health)?;
        Ok((
            Self {
                providers: Some(providers),
            },
            Some(layers),
        ))
    }

    #[cfg(feature = "otel")]
    pub fn is_enabled(&self) -> bool {
        self.providers.is_some()
    }

    #[cfg(not(feature = "otel"))]
    pub fn is_enabled(&self) -> bool {
        false
    }

    #[cfg(feature = "otel")]
    pub fn flush(&self, within: Duration) -> bool {
        self.providers
            .as_ref()
            .is_none_or(|providers| providers.flush(within))
    }

    #[cfg(not(feature = "otel"))]
    pub fn flush(&self, _within: Duration) -> bool {
        true
    }

    #[cfg(feature = "otel")]
    pub fn shutdown(self, within: Duration) -> bool {
        self.providers
            .is_none_or(|providers| providers.shutdown(within))
    }

    #[cfg(not(feature = "otel"))]
    pub fn shutdown(self, _within: Duration) -> bool {
        true
    }
}

pub fn subscriber(
    writer: NonBlocking,
    filter: EnvFilter,
    layers: Option<TelemetryLayers>,
) -> Box<dyn Subscriber + Send + Sync> {
    let registry = Registry::default();
    match layers {
        None => Box::new(
            registry.with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .with_writer(writer)
                    .with_filter(filter),
            ),
        ),
        Some(layers) => Box::new(
            registry.with(layers.into_layer()).with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .event_format(TraceIdFormat::new())
                    .with_writer(writer)
                    .with_filter(filter),
            ),
        ),
    }
}

#[derive(Debug)]
struct TraceIdFormat {
    inner: Format<Json>,
}

impl TraceIdFormat {
    fn new() -> Self {
        Self {
            inner: tracing_subscriber::fmt::format().json(),
        }
    }
}

impl<S, N> FormatEvent<S, N> for TraceIdFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'writer> FormatFields<'writer> + 'static,
{
    fn format_event(
        &self,
        context: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let Some((trace_id, span_id)) = current_ids() else {
            return self.inner.format_event(context, writer, event);
        };
        let mut line = String::new();
        self.inner
            .format_event(context, Writer::new(&mut line), event)?;
        match line.trim_end().strip_suffix('}') {
            Some(body) => writeln!(
                writer,
                "{body},\"trace_id\":\"{trace_id}\",\"span_id\":\"{span_id}\"}}"
            ),
            None => writer.write_str(&line),
        }
    }
}

#[cfg(feature = "otel")]
fn current_ids() -> Option<(String, String)> {
    use opentelemetry::trace::TraceContextExt as _;

    let context = opentelemetry::Context::current();
    let span = context.span();
    let span_context = span.span_context();
    (span_context.is_valid() && span_context.is_sampled()).then(|| {
        (
            format!("{:032x}", span_context.trace_id()),
            format!("{:016x}", span_context.span_id()),
        )
    })
}

#[cfg(not(feature = "otel"))]
fn current_ids() -> Option<(String, String)> {
    None
}

pub fn plan(config: &TelemetryConfig, env_endpoint: Option<&str>) -> TelemetryPlan {
    let configured = config.otlp_endpoint.as_deref().filter(|e| !e.is_empty());
    let environment = env_endpoint.filter(|e| !e.is_empty());
    match configured.or(environment) {
        Some(endpoint) => TelemetryPlan::Enabled {
            endpoint: endpoint.to_owned(),
            sample_ratio: config.trace_sample_ratio,
        },
        None => TelemetryPlan::Disabled,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::io::{self, Write};
    use std::sync::atomic::AtomicU64;
    use std::sync::{Arc, Mutex};
    use std::time::Instant;

    use pg_cdc_testkit::{OtlpReceiver, Received, Signal, blackhole};
    use serde_json::Value;

    use super::*;
    use crate::health::Role;
    use crate::leader::record::TakeoverKind;
    use crate::metrics::{
        self, AdminOperation, ErrorComponent, ErrorKind, EventOutcome, Measurement, SinkOutcome,
        StoreOperation, ThrottleReason,
    };
    use crate::state::now_ms;
    use crate::status::{LaneCounts, ParkedGauges};

    fn config(endpoint: Option<&str>, ratio: f64) -> TelemetryConfig {
        TelemetryConfig {
            otlp_endpoint: endpoint.map(str::to_owned),
            trace_sample_ratio: ratio,
        }
    }

    #[derive(Clone, Default)]
    struct Shared(Arc<Mutex<Vec<u8>>>);

    impl Write for Shared {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn lines(buffer: &Shared, flush: &crate::logs::LogFlush) -> Vec<Value> {
        assert!(flush.flush(Duration::from_secs(5)));
        let bytes = buffer.0.lock().unwrap().clone();
        String::from_utf8(bytes)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    fn message(line: &Value) -> &str {
        line["fields"]["message"].as_str().unwrap()
    }

    fn is_hex(value: &Value, digits: usize) -> bool {
        value.as_str().is_some_and(|text| {
            text.len() == digits && text.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
    }

    fn identity() -> TelemetryIdentity {
        TelemetryIdentity {
            pipeline: "p".to_owned(),
            holder: "h-1".to_owned(),
        }
    }

    fn enabled(endpoint: &str) -> TelemetryPlan {
        TelemetryPlan::Enabled {
            endpoint: endpoint.to_owned(),
            sample_ratio: 1.0,
        }
    }

    #[test]
    fn telemetry_is_disabled_unless_an_endpoint_is_set() {
        assert_eq!(plan(&config(None, 0.01), None), TelemetryPlan::Disabled);
        assert_eq!(
            plan(&config(Some(""), 0.01), Some("")),
            TelemetryPlan::Disabled
        );
        assert_eq!(
            plan(&config(None, 0.25), Some("http://env:4317")),
            TelemetryPlan::Enabled {
                endpoint: "http://env:4317".to_owned(),
                sample_ratio: 0.25,
            }
        );
        assert_eq!(
            plan(
                &config(Some("http://cfg:4317"), 1.0),
                Some("http://env:4317")
            ),
            TelemetryPlan::Enabled {
                endpoint: "http://cfg:4317".to_owned(),
                sample_ratio: 1.0,
            }
        );
        assert_eq!(
            plan(&config(Some(""), 0.5), Some("http://env:4317")),
            TelemetryPlan::Enabled {
                endpoint: "http://env:4317".to_owned(),
                sample_ratio: 0.5,
            }
        );
    }

    #[test]
    fn the_fixed_values_are_the_specified_ones() {
        assert_eq!(TELEMETRY_FLUSH_TIMEOUT, Duration::from_secs(1));
        assert_eq!(EXPORT_TIMEOUT, Duration::from_secs(5));
        assert!(DURATION_BUCKETS.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(DURATION_BUCKETS[0], 0.001);
        assert_eq!(DURATION_BUCKETS[15], 60.0);
    }

    #[test]
    fn the_endpoint_variable_is_the_standard_otlp_one() {
        assert_eq!(OTLP_ENDPOINT_VARIABLE, "OTEL_EXPORTER_OTLP_ENDPOINT");
    }

    #[test]
    fn a_disabled_plan_installs_nothing() {
        let (telemetry, layers) = Telemetry::install(
            &TelemetryPlan::Disabled,
            &identity(),
            &Status::new(),
            &Health::new(),
        )
        .unwrap();
        assert!(!telemetry.is_enabled());
        assert!(layers.is_none());
        assert!(!metrics::enabled());
        assert!(telemetry.flush(Duration::ZERO));
        assert!(telemetry.shutdown(Duration::ZERO));
    }

    #[test]
    fn without_telemetry_debug_spans_are_disabled_at_their_callsites() {
        let buffer = Shared::default();
        let (writer, flush) = crate::logs::non_blocking(buffer.clone(), 64).unwrap();
        let subscriber = subscriber(writer, EnvFilter::new("info"), None);
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::debug_span!(target: "pg_cdc::lane", "sink_send");
            assert!(span.is_disabled());
            tracing::info!(target: "pg_cdc::session", "hello");
        });
        let lines = lines(&buffer, &flush);
        assert_eq!(lines.len(), 1);
        assert_eq!(message(&lines[0]), "hello");
        assert!(lines[0].get("trace_id").is_none());
        assert!(lines[0].get("span_id").is_none());
    }

    #[cfg(not(feature = "otel"))]
    #[test]
    fn an_enabled_plan_without_the_otel_feature_installs_nothing_and_warns() {
        use tracing::level_filters::LevelFilter;

        let (capture, _guard) = crate::capture::Capture::install(LevelFilter::WARN);
        let (telemetry, layers) = Telemetry::install(
            &enabled("http://127.0.0.1:4317"),
            &identity(),
            &Status::new(),
            &Health::new(),
        )
        .unwrap();
        assert!(!telemetry.is_enabled());
        assert!(layers.is_none());
        assert!(!metrics::enabled());
        let messages: Vec<String> = capture
            .events()
            .into_iter()
            .map(|event| event.message)
            .collect();
        assert_eq!(
            messages,
            ["telemetry is configured, but this build has no otel feature"]
        );
    }

    #[cfg(feature = "otel")]
    mod exporting {
        use super::*;

        const SETTLE: Duration = Duration::from_secs(10);
        const FLUSH: Duration = Duration::from_secs(5);

        fn install(
            endpoint: &str,
            status: &Status,
            health: &Health,
        ) -> (Telemetry, Option<TelemetryLayers>) {
            Telemetry::install(&enabled(endpoint), &identity(), status, health).unwrap()
        }

        fn assert_identity(exports: &Received, signal: Signal) {
            for (key, value) in [
                ("service.name", "pg-cdc"),
                ("service.version", env!("CARGO_PKG_VERSION")),
                ("service.instance.id", "h-1"),
                ("pgcdc.pipeline", "p"),
            ] {
                assert_eq!(
                    exports.resource_attribute(signal, key).as_deref(),
                    Some(value),
                    "{signal:?} {key}"
                );
            }
        }

        fn board() -> (Status, Health) {
            let status = Status::new();
            status.set_epoch(Some(4));
            status.set_inflight(2, 20);
            let gauges = ParkedGauges::new(1, Arc::new(AtomicU64::new(7)));
            gauges.publish(
                0,
                LaneCounts {
                    keys: 3,
                    dead: 1,
                    oldest_ms: Some(now_ms().saturating_sub(5_000)),
                },
            );
            status.set_parked(Some(gauges));
            let health = Health::new();
            health.set_role(Role::Active);
            (status, health)
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn metrics_and_gauges_reach_the_receiver_with_their_attributes() {
            let receiver = OtlpReceiver::start().await.unwrap();
            let (status, health) = board();
            let (telemetry, layers) = install(&receiver.endpoint(), &status, &health);
            assert!(telemetry.is_enabled());
            assert!(layers.is_some());
            metrics::record(Measurement::Event {
                table: "orders",
                op: "insert",
                outcome: EventOutcome::Delivered,
                count: 3,
            });
            metrics::record(Measurement::CommitToDelivery(Duration::from_millis(30)));
            metrics::record(Measurement::Takeover(TakeoverKind::First));
            assert!(telemetry.flush(FLUSH));
            let exports = receiver
                .wait_for(SETTLE, |exports| {
                    exports.gauge("pgcdc.role").is_some()
                        && exports.histogram("pgcdc.commit_to_delivery").is_some()
                })
                .await
                .unwrap();
            assert_eq!(
                exports.sum(
                    "pgcdc.events",
                    &[
                        ("table", "orders"),
                        ("op", "insert"),
                        ("outcome", "delivered")
                    ]
                ),
                Some(3.0)
            );
            assert_eq!(
                exports.sum("pgcdc.takeovers", &[("kind", "clean")]),
                Some(1.0)
            );
            let (bounds, buckets, count) = exports.histogram("pgcdc.commit_to_delivery").unwrap();
            assert_eq!(bounds, DURATION_BUCKETS);
            assert_eq!(count, 1);
            assert_eq!(buckets.iter().sum::<u64>(), 1);
            assert_eq!(exports.gauge("pgcdc.role"), Some(1.0));
            assert_eq!(exports.gauge("pgcdc.leader.epoch"), Some(4.0));
            assert_eq!(exports.gauge("pgcdc.parked.events"), Some(7.0));
            assert_eq!(exports.gauge("pgcdc.parked.keys"), Some(3.0));
            assert_eq!(exports.gauge("pgcdc.parked.dead"), Some(1.0));
            assert!(exports.gauge("pgcdc.parked.oldest_age").unwrap() >= 5.0);
            assert_eq!(exports.gauge("pgcdc.inflight.events"), Some(2.0));
            assert_eq!(exports.gauge("pgcdc.inflight.bytes"), Some(20.0));
            assert_eq!(
                exports.gauge("pgcdc.tables_without_full_identity"),
                Some(0.0)
            );
            assert_eq!(exports.gauge("pgcdc.status_update.age"), None);
            assert_eq!(exports.gauge("pgcdc.slot.retained_bytes"), None);
            assert_identity(&exports, Signal::Metrics);
            assert!(telemetry.shutdown(FLUSH));
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn every_measurement_maps_to_its_instrument_and_attributes() {
            let receiver = OtlpReceiver::start().await.unwrap();
            let (telemetry, _layers) =
                install(&receiver.endpoint(), &Status::new(), &Health::new());
            metrics::record(Measurement::Event {
                table: "orders",
                op: "update",
                outcome: EventOutcome::Parked,
                count: 2,
            });
            metrics::record(Measurement::Sink {
                outcome: SinkOutcome::Retryable,
                elapsed: Duration::from_millis(5),
            });
            metrics::record(Measurement::Store {
                operation: StoreOperation::PutPayload,
                elapsed: Duration::from_millis(7),
            });
            metrics::record(Measurement::Takeover(TakeoverKind::Clean));
            metrics::record(Measurement::Takeover(TakeoverKind::Unclean));
            metrics::record(Measurement::Takeover(TakeoverKind::Unclean));
            metrics::record(Measurement::DecodeFallback { type_oid: 1700 });
            assert!(telemetry.flush(FLUSH));
            let exports = receiver
                .wait_for(SETTLE, |exports| {
                    exports.histogram("pgcdc.store.duration").is_some()
                        && exports.histogram("pgcdc.sink.duration").is_some()
                })
                .await
                .unwrap();
            assert_eq!(
                exports.sum(
                    "pgcdc.events",
                    &[("table", "orders"), ("op", "update"), ("outcome", "parked")]
                ),
                Some(2.0)
            );
            assert_eq!(
                exports.sum("pgcdc.takeovers", &[("kind", "clean")]),
                Some(1.0)
            );
            assert_eq!(
                exports.sum("pgcdc.takeovers", &[("kind", "unclean")]),
                Some(2.0)
            );
            assert_eq!(
                exports.sum("pgcdc.decode.fallbacks", &[("type", "1700")]),
                Some(1.0)
            );
            for name in ["pgcdc.sink.duration", "pgcdc.store.duration"] {
                let (bounds, _, count) = exports.histogram(name).unwrap();
                assert_eq!(bounds, DURATION_BUCKETS);
                assert_eq!(count, 1);
            }
            assert!(telemetry.shutdown(FLUSH));
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn throttle_error_and_rejection_counters_export_with_their_labels() {
            let receiver = OtlpReceiver::start().await.unwrap();
            let (telemetry, _layers) =
                install(&receiver.endpoint(), &Status::new(), &Health::new());
            metrics::record(Measurement::SinkThrottled {
                reason: ThrottleReason::Throttling,
                count: 10,
            });
            metrics::record(Measurement::SinkThrottled {
                reason: ThrottleReason::KmsThrottling,
                count: 1,
            });
            metrics::record(Measurement::StoreThrottled {
                operation: StoreOperation::AppendParked,
            });
            metrics::record(Measurement::StoreThrottled {
                operation: StoreOperation::AppendParked,
            });
            metrics::record(Measurement::Error {
                component: ErrorComponent::Sink,
                kind: ErrorKind::Retryable,
                count: 10,
            });
            metrics::record(Measurement::Error {
                component: ErrorComponent::Store,
                kind: ErrorKind::Unavailable,
                count: 1,
            });
            metrics::record(Measurement::AdminRejected {
                operation: AdminOperation::Discard,
            });
            assert!(telemetry.flush(FLUSH));
            let exports = receiver
                .wait_for(SETTLE, |exports| {
                    exports.sum("pgcdc.admin.rejected", &[]).is_some()
                })
                .await
                .unwrap();
            for (name, labels, value) in [
                ("pgcdc.sink.throttled", vec![("reason", "throttling")], 10.0),
                (
                    "pgcdc.sink.throttled",
                    vec![("reason", "kms_throttling")],
                    1.0,
                ),
                (
                    "pgcdc.store.throttled",
                    vec![("operation", "append_parked")],
                    2.0,
                ),
                (
                    "pgcdc.errors",
                    vec![("component", "sink"), ("kind", "retryable")],
                    10.0,
                ),
                (
                    "pgcdc.errors",
                    vec![("component", "store"), ("kind", "unavailable")],
                    1.0,
                ),
                ("pgcdc.admin.rejected", vec![("operation", "discard")], 1.0),
            ] {
                assert_eq!(exports.sum(name, &labels), Some(value), "{name} {labels:?}");
            }
            assert_eq!(exports.sum("pgcdc.sink.throttled", &[]), Some(11.0));
            assert_eq!(exports.sum("pgcdc.errors", &[]), Some(11.0));
            assert!(telemetry.shutdown(FLUSH));
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn spans_and_logs_reach_the_receiver() {
            let receiver = OtlpReceiver::start().await.unwrap();
            let (telemetry, layers) = install(&receiver.endpoint(), &Status::new(), &Health::new());
            let subscriber = tracing_subscriber::registry().with(layers.unwrap().into_layer());
            tracing::subscriber::with_default(subscriber, || {
                let span = tracing::debug_span!(target: "pg_cdc::lane", "sink_send");
                let _entered = span.enter();
                tracing::info!(target: "pg_cdc::session", "hello");
            });
            assert!(telemetry.flush(FLUSH));
            let exports = receiver
                .wait_for(SETTLE, |exports| {
                    exports.span_names().contains("sink_send")
                        && exports.log_bodies().iter().any(|body| body == "hello")
                })
                .await
                .unwrap();
            assert!(exports.span_names().contains("sink_send"));
            assert!(exports.log_bodies().contains(&"hello".to_owned()));
            assert_identity(&exports, Signal::Spans);
            assert_identity(&exports, Signal::Logs);
            assert!(telemetry.shutdown(FLUSH));
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn log_lines_inside_a_sampled_span_carry_trace_and_span_ids() {
            let receiver = OtlpReceiver::start().await.unwrap();
            let (telemetry, layers) = install(&receiver.endpoint(), &Status::new(), &Health::new());
            let buffer = Shared::default();
            let (writer, flush) = crate::logs::non_blocking(buffer.clone(), 64).unwrap();
            let subscriber = subscriber(writer, EnvFilter::new("info"), layers);
            tracing::subscriber::with_default(subscriber, || {
                let span = tracing::info_span!(target: "pg_cdc::session", "handover");
                span.in_scope(|| tracing::info!(target: "pg_cdc::session", "inside"));
                tracing::info!(target: "pg_cdc::session", "outside");
            });
            let lines = lines(&buffer, &flush);
            let inside = lines.iter().find(|l| message(l) == "inside").unwrap();
            assert!(is_hex(&inside["trace_id"], 32), "{inside}");
            assert!(is_hex(&inside["span_id"], 16), "{inside}");
            assert_eq!(inside["fields"]["message"], "inside");
            let outside = lines.iter().find(|l| message(l) == "outside").unwrap();
            assert!(outside.get("trace_id").is_none(), "{outside}");
            assert!(outside.get("span_id").is_none(), "{outside}");
            assert!(telemetry.shutdown(FLUSH));
        }

        fn ids(line: &Value) -> (String, String) {
            (
                line["trace_id"].as_str().unwrap_or_default().to_owned(),
                line["span_id"].as_str().unwrap_or_default().to_owned(),
            )
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn stdout_and_exported_logs_carry_the_ids_of_their_exported_span() {
            let receiver = OtlpReceiver::start().await.unwrap();
            let (telemetry, layers) = install(&receiver.endpoint(), &Status::new(), &Health::new());
            let buffer = Shared::default();
            let (writer, flush) = crate::logs::non_blocking(buffer.clone(), 64).unwrap();
            let subscriber = subscriber(writer, EnvFilter::new("info"), layers);
            tracing::subscriber::with_default(subscriber, || {
                tracing::debug_span!(target: "pg_cdc::lane", "sink_send")
                    .in_scope(|| tracing::info!(target: "pg_cdc::lane", "in debug"));
                tracing::info_span!(target: "pg_cdc::session", "handover")
                    .in_scope(|| tracing::info!(target: "pg_cdc::session", "in info"));
            });
            assert!(telemetry.flush(FLUSH));
            let exports = receiver
                .wait_for(SETTLE, |exports| {
                    exports.span_ids("sink_send").is_some()
                        && exports.span_ids("handover").is_some()
                        && exports.log_ids("in debug").is_some()
                        && exports.log_ids("in info").is_some()
                })
                .await
                .unwrap();
            let lines = lines(&buffer, &flush);
            for (span, body) in [("sink_send", "in debug"), ("handover", "in info")] {
                let exported = exports.span_ids(span).unwrap();
                assert_eq!(exported.0.len(), 32, "{exported:?}");
                assert_eq!(exported.1.len(), 16, "{exported:?}");
                let line = lines.iter().find(|line| message(line) == body).unwrap();
                assert_eq!(ids(line), exported, "{line}");
                assert_eq!(exports.log_ids(body).unwrap(), exported, "{body}");
            }
            assert!(telemetry.shutdown(FLUSH));
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn stdout_lines_in_unsampled_spans_carry_no_ids() {
            let receiver = OtlpReceiver::start().await.unwrap();
            let plan = TelemetryPlan::Enabled {
                endpoint: receiver.endpoint(),
                sample_ratio: 0.0,
            };
            let (telemetry, layers) =
                Telemetry::install(&plan, &identity(), &Status::new(), &Health::new()).unwrap();
            let buffer = Shared::default();
            let (writer, flush) = crate::logs::non_blocking(buffer.clone(), 64).unwrap();
            let subscriber = subscriber(writer, EnvFilter::new("info"), layers);
            tracing::subscriber::with_default(subscriber, || {
                tracing::debug_span!(target: "pg_cdc::lane", "sink_send")
                    .in_scope(|| tracing::info!(target: "pg_cdc::lane", "in debug"));
                tracing::info_span!(target: "pg_cdc::session", "handover")
                    .in_scope(|| tracing::info!(target: "pg_cdc::session", "in info"));
            });
            let lines = lines(&buffer, &flush);
            assert_eq!(lines.len(), 2);
            for line in &lines {
                assert!(line.get("trace_id").is_none(), "{line}");
                assert!(line.get("span_id").is_none(), "{line}");
            }
            assert!(telemetry.shutdown(FLUSH));
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn only_pg_cdc_spans_at_debug_and_above_are_exported() {
            let receiver = OtlpReceiver::start().await.unwrap();
            let (telemetry, layers) = install(&receiver.endpoint(), &Status::new(), &Health::new());
            let subscriber = tracing_subscriber::registry().with(layers.unwrap().into_layer());
            tracing::subscriber::with_default(subscriber, || {
                drop(tracing::debug_span!(target: "pg_cdc::lane", "kept"));
                drop(tracing::trace_span!(target: "pg_cdc::lane", "too_fine"));
                drop(tracing::debug_span!(target: "other_crate", "foreign"));
            });
            assert!(telemetry.flush(FLUSH));
            let exports = receiver
                .wait_for(SETTLE, |exports| !exports.span_names().is_empty())
                .await
                .unwrap();
            assert_eq!(exports.span_names(), BTreeSet::from(["kept".to_owned()]));
            assert!(telemetry.shutdown(FLUSH));
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_zero_sample_ratio_exports_no_spans_but_still_exports_logs() {
            let receiver = OtlpReceiver::start().await.unwrap();
            let plan = TelemetryPlan::Enabled {
                endpoint: receiver.endpoint(),
                sample_ratio: 0.0,
            };
            let (telemetry, layers) =
                Telemetry::install(&plan, &identity(), &Status::new(), &Health::new()).unwrap();
            let subscriber = tracing_subscriber::registry().with(layers.unwrap().into_layer());
            tracing::subscriber::with_default(subscriber, || {
                let span = tracing::debug_span!(target: "pg_cdc::lane", "sink_send");
                let _entered = span.enter();
                tracing::info!(target: "pg_cdc::session", "hello");
            });
            assert!(telemetry.flush(FLUSH));
            let exports = receiver
                .wait_for(SETTLE, |exports| !exports.log_bodies().is_empty())
                .await
                .unwrap();
            assert_eq!(exports.log_bodies(), ["hello"]);
            assert!(exports.span_names().is_empty());
            assert!(telemetry.shutdown(FLUSH));
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_second_install_is_refused() {
            let receiver = OtlpReceiver::start().await.unwrap();
            let (first, _layers) = install(&receiver.endpoint(), &Status::new(), &Health::new());
            let refused = Telemetry::install(
                &enabled(&receiver.endpoint()),
                &identity(),
                &Status::new(),
                &Health::new(),
            )
            .unwrap_err();
            assert!(matches!(refused, TelemetryError::Recorder(_)));
            assert!(first.shutdown(FLUSH));
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_flush_against_a_blackholed_exporter_gives_up_at_its_deadline() {
            let (endpoint, server) = blackhole().await.unwrap();
            let (telemetry, layers) = install(&endpoint, &Status::new(), &Health::new());
            let subscriber = tracing_subscriber::registry().with(layers.unwrap().into_layer());
            tracing::subscriber::with_default(subscriber, || {
                drop(tracing::debug_span!(target: "pg_cdc::lane", "sink_send"));
            });
            let started = Instant::now();
            assert!(!telemetry.flush(Duration::from_millis(200)));
            assert!(started.elapsed() < Duration::from_secs(2));
            server.abort();
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn the_exporter_stack_is_never_exported_as_logs() {
            let receiver = OtlpReceiver::start().await.unwrap();
            let (telemetry, layers) = install(&receiver.endpoint(), &Status::new(), &Health::new());
            let subscriber = tracing_subscriber::registry().with(layers.unwrap().into_layer());
            tracing::subscriber::with_default(subscriber, || {
                tracing::info!(target: "h2", "from h2");
                tracing::info!(target: "tonic::transport", "from tonic");
                tracing::info!(target: "opentelemetry_sdk", "from the sdk");
                tracing::info!(target: "hyper_util::client", "from hyper");
                tracing::info!(target: "tower::buffer", "from tower");
                tracing::info!(target: "pg_cdc::session", "from pg-cdc");
            });
            assert!(telemetry.flush(FLUSH));
            let exports = receiver
                .wait_for(SETTLE, |exports| !exports.log_bodies().is_empty())
                .await
                .unwrap();
            assert_eq!(exports.log_bodies(), ["from pg-cdc"]);
            assert!(telemetry.shutdown(FLUSH));
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn logging_never_waits_on_a_blackholed_exporter() {
            let (endpoint, server) = blackhole().await.unwrap();
            let (telemetry, layers) = install(&endpoint, &Status::new(), &Health::new());
            let subscriber = tracing_subscriber::registry().with(layers.unwrap().into_layer());
            let started = Instant::now();
            tracing::subscriber::with_default(subscriber, || {
                for _ in 0..50_000 {
                    let span = tracing::debug_span!(target: "pg_cdc::lane", "sink_send");
                    let _entered = span.enter();
                    tracing::info!(target: "pg_cdc::session", "line");
                }
            });
            assert!(started.elapsed() < Duration::from_secs(5));
            let shutting_down = Instant::now();
            telemetry.shutdown(Duration::from_millis(500));
            assert!(shutting_down.elapsed() < Duration::from_secs(1));
            server.abort();
        }
    }
}
