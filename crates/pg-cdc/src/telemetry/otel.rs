use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use opentelemetry::metrics::{Counter, Histogram, Meter, MeterProvider};
use opentelemetry::trace::TracerProvider;
use opentelemetry::{KeyValue, Value};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::tonic_types::transport::ClientTlsConfig;
use opentelemetry_otlp::{
    LogExporter, MetricExporter, SpanExporter, WithExportConfig, WithTonicConfig,
};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::{SdkMeterProvider, Temporality};
use opentelemetry_sdk::resource::{EnvResourceDetector, TelemetryResourceDetector};
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use tracing::{Level, Metadata};
use tracing_subscriber::Layer;
use tracing_subscriber::filter::filter_fn;

use super::{DURATION_BUCKETS, EXPORT_TIMEOUT, TelemetryError, TelemetryIdentity, TelemetryLayers};
use crate::health::{Health, Role};
use crate::leader::record::TakeoverKind;
use crate::metrics::{self, Measurement, Recorder};
use crate::state::now_ms;
use crate::status::{Status, StatusSnapshot};

const METER_NAME: &str = "pg-cdc";
const SERVICE_NAME: &str = "pg-cdc";
const SERVICE_NAME_VARIABLE: &str = "OTEL_SERVICE_NAME";
const SPAN_TARGET_PREFIX: &str = "pg_cdc";
const EXPORTER_TARGETS: [&str; 5] = ["opentelemetry", "tonic", "h2", "hyper", "tower"];

#[derive(Debug, Clone)]
pub(super) struct Providers {
    tracer: SdkTracerProvider,
    meter: SdkMeterProvider,
    logger: SdkLoggerProvider,
}

impl Providers {
    pub(super) fn flush(&self, within: Duration) -> bool {
        self.bounded(within, |providers| {
            thread::scope(|scope| {
                scope.spawn(|| providers.tracer.force_flush().ok());
                scope.spawn(|| providers.meter.force_flush().ok());
                scope.spawn(|| providers.logger.force_flush().ok());
            });
        })
    }

    pub(super) fn shutdown(&self, within: Duration) -> bool {
        self.bounded(within, move |providers| {
            thread::scope(|scope| {
                scope.spawn(|| providers.tracer.shutdown_with_timeout(within).ok());
                scope.spawn(|| providers.meter.shutdown_with_timeout(within).ok());
                scope.spawn(|| providers.logger.shutdown_with_timeout(within).ok());
            });
        })
    }

    fn bounded(&self, within: Duration, action: impl FnOnce(&Self) + Send + 'static) -> bool {
        let providers = self.clone();
        let (finished, wait) = mpsc::channel();
        let spawned = thread::Builder::new()
            .name("pg-cdc-telemetry".to_owned())
            .spawn(move || {
                action(&providers);
                finished.send(()).ok();
            });
        spawned.is_ok() && wait.recv_timeout(within).is_ok()
    }
}

pub(super) fn build(
    endpoint: &str,
    sample_ratio: f64,
    identity: &TelemetryIdentity,
    status: &Status,
    health: &Health,
) -> Result<(Providers, TelemetryLayers), TelemetryError> {
    build_with(
        endpoint,
        &collector_tls(),
        sample_ratio,
        identity,
        status,
        health,
    )
}

fn collector_tls() -> ClientTlsConfig {
    ClientTlsConfig::new().with_webpki_roots()
}

fn build_with(
    endpoint: &str,
    tls: &ClientTlsConfig,
    sample_ratio: f64,
    identity: &TelemetryIdentity,
    status: &Status,
    health: &Health,
) -> Result<(Providers, TelemetryLayers), TelemetryError> {
    let resource = resource(identity, std::env::var(SERVICE_NAME_VARIABLE).ok());
    let tracer = tracer_provider(endpoint, tls, sample_ratio, &resource)?;
    let meter = meter_provider(endpoint, tls, &resource)?;
    let logger = logger_provider(endpoint, tls, &resource)?;
    let meter_handle = meter.meter(METER_NAME);
    register_gauges(&meter_handle, status, health);
    let recorder = OtelRecorder::new(&meter_handle);
    let layers = layers(&tracer, &logger);
    metrics::install(Box::new(recorder))?;
    Ok((
        Providers {
            tracer,
            meter,
            logger,
        },
        layers,
    ))
}

fn resource(identity: &TelemetryIdentity, service_name: Option<String>) -> Resource {
    let mut builder = Resource::builder_empty()
        .with_service_name(SERVICE_NAME)
        .with_detectors(&[
            Box::new(TelemetryResourceDetector),
            Box::new(EnvResourceDetector::new()),
        ]);
    if let Some(name) = service_name.filter(|name| !name.is_empty()) {
        builder = builder.with_service_name(name);
    }
    builder
        .with_attributes([
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            KeyValue::new("service.instance.id", identity.holder.clone()),
            KeyValue::new("pgcdc.pipeline", identity.pipeline.clone()),
        ])
        .build()
}

fn exporter_error(signal: &'static str, error: &impl std::fmt::Display) -> TelemetryError {
    TelemetryError::Exporter {
        signal,
        reason: error.to_string(),
    }
}

fn tracer_provider(
    endpoint: &str,
    tls: &ClientTlsConfig,
    sample_ratio: f64,
    resource: &Resource,
) -> Result<SdkTracerProvider, TelemetryError> {
    let exporter = SpanExporter::builder()
        .with_tonic()
        .with_tls_config(tls.clone())
        .with_endpoint(endpoint)
        .with_timeout(EXPORT_TIMEOUT)
        .build()
        .map_err(|error| exporter_error("trace", &error))?;
    Ok(SdkTracerProvider::builder()
        .with_resource(resource.clone())
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
            sample_ratio,
        ))))
        .with_batch_exporter(exporter)
        .build())
}

fn meter_provider(
    endpoint: &str,
    tls: &ClientTlsConfig,
    resource: &Resource,
) -> Result<SdkMeterProvider, TelemetryError> {
    let exporter = MetricExporter::builder()
        .with_tonic()
        .with_tls_config(tls.clone())
        .with_endpoint(endpoint)
        .with_timeout(EXPORT_TIMEOUT)
        .with_temporality(Temporality::Cumulative)
        .build()
        .map_err(|error| exporter_error("metric", &error))?;
    Ok(SdkMeterProvider::builder()
        .with_resource(resource.clone())
        .with_periodic_exporter(exporter)
        .build())
}

fn logger_provider(
    endpoint: &str,
    tls: &ClientTlsConfig,
    resource: &Resource,
) -> Result<SdkLoggerProvider, TelemetryError> {
    let exporter = LogExporter::builder()
        .with_tonic()
        .with_tls_config(tls.clone())
        .with_endpoint(endpoint)
        .with_timeout(EXPORT_TIMEOUT)
        .build()
        .map_err(|error| exporter_error("log", &error))?;
    Ok(SdkLoggerProvider::builder()
        .with_resource(resource.clone())
        .with_batch_exporter(exporter)
        .build())
}

fn traced(metadata: &Metadata<'_>) -> bool {
    if metadata.is_span() {
        metadata.target().starts_with(SPAN_TARGET_PREFIX) && *metadata.level() <= Level::DEBUG
    } else {
        *metadata.level() <= Level::INFO
    }
}

fn exported_as_log(metadata: &Metadata<'_>) -> bool {
    *metadata.level() <= Level::INFO
        && !EXPORTER_TARGETS
            .iter()
            .any(|prefix| metadata.target().starts_with(prefix))
}

fn layers(tracer: &SdkTracerProvider, logger: &SdkLoggerProvider) -> TelemetryLayers {
    let spans = tracing_opentelemetry::layer()
        .with_tracer(tracer.tracer(METER_NAME))
        .with_filter(filter_fn(traced));
    let logs = OpenTelemetryTracingBridge::new(logger).with_filter(filter_fn(exported_as_log));
    TelemetryLayers {
        layers: vec![Box::new(spans), Box::new(logs)],
    }
}

#[derive(Debug)]
struct OtelRecorder {
    events: Counter<u64>,
    commit_to_delivery: Histogram<f64>,
    sink_duration: Histogram<f64>,
    store_duration: Histogram<f64>,
    takeovers: Counter<u64>,
    decode_fallbacks: Counter<u64>,
    sink_throttled: Counter<u64>,
    store_throttled: Counter<u64>,
    errors: Counter<u64>,
    admin_rejected: Counter<u64>,
}

fn duration_histogram(meter: &Meter, name: &'static str) -> Histogram<f64> {
    meter
        .f64_histogram(name)
        .with_unit("s")
        .with_boundaries(DURATION_BUCKETS.to_vec())
        .build()
}

impl OtelRecorder {
    fn new(meter: &Meter) -> Self {
        Self {
            events: meter.u64_counter("pgcdc.events").build(),
            commit_to_delivery: duration_histogram(meter, "pgcdc.commit_to_delivery"),
            sink_duration: duration_histogram(meter, "pgcdc.sink.duration"),
            store_duration: duration_histogram(meter, "pgcdc.store.duration"),
            takeovers: meter.u64_counter("pgcdc.takeovers").build(),
            decode_fallbacks: meter.u64_counter("pgcdc.decode.fallbacks").build(),
            sink_throttled: meter.u64_counter("pgcdc.sink.throttled").build(),
            store_throttled: meter.u64_counter("pgcdc.store.throttled").build(),
            errors: meter.u64_counter("pgcdc.errors").build(),
            admin_rejected: meter.u64_counter("pgcdc.admin.rejected").build(),
        }
    }
}

fn takeover_kind(kind: TakeoverKind) -> &'static str {
    match kind {
        TakeoverKind::First | TakeoverKind::Clean => "clean",
        TakeoverKind::Unclean => "unclean",
    }
}

impl Recorder for OtelRecorder {
    fn record(&self, measurement: Measurement<'_>) {
        match measurement {
            Measurement::Event {
                table,
                op,
                outcome,
                count,
            } => self.events.add(
                count,
                &[
                    KeyValue::new("table", table.to_owned()),
                    KeyValue::new("op", op.to_owned()),
                    KeyValue::new("outcome", outcome.as_str()),
                ],
            ),
            Measurement::CommitToDelivery(elapsed) => {
                self.commit_to_delivery.record(elapsed.as_secs_f64(), &[]);
            }
            Measurement::Sink { outcome, elapsed } => self.sink_duration.record(
                elapsed.as_secs_f64(),
                &[KeyValue::new("outcome", outcome.as_str())],
            ),
            Measurement::Store { operation, elapsed } => self.store_duration.record(
                elapsed.as_secs_f64(),
                &[KeyValue::new("operation", operation.as_str())],
            ),
            Measurement::Takeover(kind) => self
                .takeovers
                .add(1, &[KeyValue::new("kind", takeover_kind(kind))]),
            Measurement::DecodeFallback { type_oid } => self.decode_fallbacks.add(
                1,
                &[KeyValue::new("type", Value::from(type_oid.to_string()))],
            ),
            Measurement::SinkThrottled { reason, count } => self
                .sink_throttled
                .add(count, &[KeyValue::new("reason", reason.as_str())]),
            Measurement::StoreThrottled { operation } => self
                .store_throttled
                .add(1, &[KeyValue::new("operation", operation.as_str())]),
            Measurement::Error {
                component,
                kind,
                count,
            } => self.errors.add(
                count,
                &[
                    KeyValue::new("component", component.as_str()),
                    KeyValue::new("kind", kind.as_str()),
                ],
            ),
            Measurement::AdminRejected { operation } => self
                .admin_rejected
                .add(1, &[KeyValue::new("operation", operation.as_str())]),
        }
    }
}

fn u64_gauge(
    meter: &Meter,
    status: &Status,
    name: &'static str,
    read: fn(&StatusSnapshot) -> Option<u64>,
) {
    let status = status.clone();
    meter
        .u64_observable_gauge(name)
        .with_callback(move |observer| {
            if let Some(value) = read(&status.snapshot()) {
                observer.observe(value, &[]);
            }
        })
        .build();
}

fn seconds_gauge(
    meter: &Meter,
    status: &Status,
    name: &'static str,
    read: fn(&StatusSnapshot) -> Option<Duration>,
) {
    let status = status.clone();
    meter
        .f64_observable_gauge(name)
        .with_unit("s")
        .with_callback(move |observer| {
            if let Some(value) = read(&status.snapshot()) {
                observer.observe(value.as_secs_f64(), &[]);
            }
        })
        .build();
}

fn role_code(role: Role) -> u64 {
    match role {
        Role::Standby => 0,
        Role::Active => 1,
        Role::Draining => 2,
    }
}

fn register_gauges(meter: &Meter, status: &Status, health: &Health) {
    u64_gauge(meter, status, "pgcdc.parked.events", |s| {
        s.parked.map(|parked| parked.events)
    });
    u64_gauge(meter, status, "pgcdc.parked.keys", |s| {
        s.parked.map(|parked| parked.keys)
    });
    u64_gauge(meter, status, "pgcdc.parked.dead", |s| {
        s.parked.map(|parked| parked.dead)
    });
    seconds_gauge(meter, status, "pgcdc.parked.oldest_age", |s| {
        s.parked.and_then(|parked| parked.oldest_age(now_ms()))
    });
    u64_gauge(meter, status, "pgcdc.inflight.events", |s| {
        Some(s.inflight_events)
    });
    u64_gauge(meter, status, "pgcdc.inflight.bytes", |s| {
        Some(s.inflight_bytes)
    });
    u64_gauge(meter, status, "pgcdc.slot.retained_bytes", |s| {
        s.slot_retained_bytes
    });
    seconds_gauge(meter, status, "pgcdc.status_update.age", |s| {
        s.status_update_age
    });
    u64_gauge(meter, status, "pgcdc.leader.epoch", |s| s.epoch);
    u64_gauge(meter, status, "pgcdc.tables_without_full_identity", |s| {
        Some(s.tables_without_full_identity)
    });
    let health = health.clone();
    meter
        .u64_observable_gauge("pgcdc.role")
        .with_callback(move |observer| observer.observe(role_code(health.snapshot().role), &[]))
        .build();
}

#[cfg(test)]
mod tests {
    use opentelemetry::Key;
    use opentelemetry_otlp::tonic_types::transport::Certificate;
    use pg_cdc_testkit::{OtlpReceiver, Received};
    use tracing_subscriber::layer::SubscriberExt as _;

    use super::*;

    fn identity() -> TelemetryIdentity {
        TelemetryIdentity {
            pipeline: "p".to_owned(),
            holder: "h-1".to_owned(),
        }
    }

    fn attribute(resource: &Resource, key: &'static str) -> Option<String> {
        resource
            .get(&Key::from_static_str(key))
            .map(|value| value.to_string())
    }

    #[test]
    fn the_resource_names_the_service_and_the_task() {
        let resource = resource(&identity(), None);
        assert_eq!(
            attribute(&resource, "service.name").as_deref(),
            Some("pg-cdc")
        );
        assert_eq!(
            attribute(&resource, "service.version").as_deref(),
            Some(env!("CARGO_PKG_VERSION"))
        );
        assert_eq!(
            attribute(&resource, "service.instance.id").as_deref(),
            Some("h-1")
        );
        assert_eq!(attribute(&resource, "pgcdc.pipeline").as_deref(), Some("p"));
    }

    #[test]
    fn a_configured_service_name_replaces_the_default() {
        let named = resource(&identity(), Some("pg-cdc-staging".to_owned()));
        assert_eq!(
            attribute(&named, "service.name").as_deref(),
            Some("pg-cdc-staging")
        );
        let blank = resource(&identity(), Some(String::new()));
        assert_eq!(attribute(&blank, "service.name").as_deref(), Some("pg-cdc"));
    }

    #[test]
    fn the_collector_trust_store_is_the_webpki_roots() {
        let tls = format!("{:?}", collector_tls());
        assert!(tls.contains("with_webpki_roots: true"), "{tls}");
    }

    const SETTLE: Duration = Duration::from_secs(10);
    const FLUSH: Duration = Duration::from_secs(5);

    fn export_one_of_each(receiver: &OtlpReceiver, tls: &ClientTlsConfig) -> Providers {
        let (providers, layers) = build_with(
            &receiver.endpoint(),
            tls,
            1.0,
            &identity(),
            &Status::new(),
            &Health::new(),
        )
        .unwrap();
        let subscriber = tracing_subscriber::registry().with(layers.into_layer());
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::debug_span!(target: "pg_cdc::lane", "sink_send");
            let _entered = span.enter();
            tracing::info!(target: "pg_cdc::session", "hello");
        });
        metrics::record(Measurement::Takeover(TakeoverKind::Clean));
        assert!(providers.flush(FLUSH));
        providers
    }

    fn every_signal(exports: &Received) -> bool {
        exports.span_names().contains("sink_send")
            && exports.log_bodies().iter().any(|body| body == "hello")
            && exports.sum("pgcdc.takeovers", &[("kind", "clean")]) == Some(1.0)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn every_signal_exports_over_https_to_a_collector_the_exporters_trust() {
        let receiver = OtlpReceiver::start_tls().await.unwrap();
        let trusted =
            collector_tls().ca_certificate(Certificate::from_pem(receiver.ca_pem().unwrap()));
        let providers = export_one_of_each(&receiver, &trusted);
        receiver.wait_for(SETTLE, every_signal).await.unwrap();
        assert!(providers.shutdown(FLUSH));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn no_signal_reaches_a_collector_whose_certificate_is_untrusted() {
        let receiver = OtlpReceiver::start_tls().await.unwrap();
        let providers = export_one_of_each(&receiver, &collector_tls());
        tokio::time::sleep(Duration::from_secs(1)).await;
        let exports = receiver.received();
        assert_eq!(
            (
                exports.metrics.len(),
                exports.spans.len(),
                exports.logs.len()
            ),
            (0, 0, 0)
        );
        assert!(providers.shutdown(FLUSH));
    }

    #[test]
    fn every_role_has_its_code() {
        assert_eq!(role_code(Role::Standby), 0);
        assert_eq!(role_code(Role::Active), 1);
        assert_eq!(role_code(Role::Draining), 2);
    }
}
