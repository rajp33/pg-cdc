use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::btree_map::Entry;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::{
    LogsService, LogsServiceServer,
};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::{
    MetricsService, MetricsServiceServer,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::{
    TraceService, TraceServiceServer,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::metrics::v1::{NumberDataPoint, metric, number_data_point};
use opentelemetry_proto::tonic::resource::v1::Resource;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status};

use crate::error::TestkitError;
use crate::tls::TestCertificates;

const POLL_INTERVAL: Duration = Duration::from_millis(25);

type SeriesKey = Vec<(String, String)>;
type HistogramPoint = (Vec<f64>, Vec<u64>, u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    Metrics,
    Spans,
    Logs,
}

#[derive(Debug, Clone, Default)]
pub struct Received {
    pub metrics: Vec<ExportMetricsServiceRequest>,
    pub spans: Vec<ExportTraceServiceRequest>,
    pub logs: Vec<ExportLogsServiceRequest>,
}

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[expect(
    clippy::cast_precision_loss,
    reason = "metric values are compared as f64 and exceed 2^53 only in artificial inputs"
)]
fn int_to_f64(value: i64) -> f64 {
    value as f64
}

fn number(point: &NumberDataPoint) -> Option<f64> {
    match point.value.as_ref()? {
        number_data_point::Value::AsInt(value) => Some(int_to_f64(*value)),
        number_data_point::Value::AsDouble(value) => Some(*value),
    }
}

fn text(value: Option<&AnyValue>) -> Option<String> {
    match value?.value.as_ref()? {
        any_value::Value::StringValue(text) => Some(text.clone()),
        any_value::Value::IntValue(number) => Some(number.to_string()),
        any_value::Value::DoubleValue(number) => Some(number.to_string()),
        any_value::Value::BoolValue(flag) => Some(flag.to_string()),
        _ => None,
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut text, byte| {
        let _ = write!(text, "{byte:02x}");
        text
    })
}

fn matches(point: &[KeyValue], wanted: &[(&str, &str)]) -> bool {
    wanted.iter().all(|(key, value)| {
        point.iter().any(|attribute| {
            attribute.key == *key && text(attribute.value.as_ref()).as_deref() == Some(*value)
        })
    })
}

fn series(attributes: &[KeyValue]) -> SeriesKey {
    let mut key: SeriesKey = attributes
        .iter()
        .map(|attribute| {
            (
                attribute.key.clone(),
                text(attribute.value.as_ref()).unwrap_or_default(),
            )
        })
        .collect();
    key.sort();
    key
}

impl Received {
    fn metric_data<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a metric::Data> + 'a {
        self.metrics
            .iter()
            .flat_map(|request| &request.resource_metrics)
            .flat_map(|resource| &resource.scope_metrics)
            .flat_map(|scope| &scope.metrics)
            .filter(move |metric| metric.name == name)
            .filter_map(|metric| metric.data.as_ref())
    }

    pub fn metric_names(&self) -> BTreeSet<String> {
        self.metrics
            .iter()
            .flat_map(|request| &request.resource_metrics)
            .flat_map(|resource| &resource.scope_metrics)
            .flat_map(|scope| &scope.metrics)
            .map(|metric| metric.name.clone())
            .collect()
    }

    pub fn sum(&self, name: &str, attributes: &[(&str, &str)]) -> Option<f64> {
        let mut latest: BTreeMap<SeriesKey, (u64, f64)> = BTreeMap::new();
        for data in self.metric_data(name) {
            let metric::Data::Sum(sum) = data else {
                continue;
            };
            for point in &sum.data_points {
                let Some(value) = number(point).filter(|_| matches(&point.attributes, attributes))
                else {
                    continue;
                };
                match latest.entry(series(&point.attributes)) {
                    Entry::Vacant(slot) => {
                        slot.insert((point.time_unix_nano, value));
                    }
                    Entry::Occupied(mut slot) if point.time_unix_nano >= slot.get().0 => {
                        slot.insert((point.time_unix_nano, value));
                    }
                    Entry::Occupied(_) => {}
                }
            }
        }
        (!latest.is_empty()).then(|| latest.values().map(|(_, value)| value).sum())
    }

    pub fn gauge(&self, name: &str) -> Option<f64> {
        let mut latest: Option<(u64, f64)> = None;
        for data in self.metric_data(name) {
            let metric::Data::Gauge(gauge) = data else {
                continue;
            };
            for point in &gauge.data_points {
                if let Some(value) = number(point)
                    && latest.is_none_or(|(time, _)| point.time_unix_nano >= time)
                {
                    latest = Some((point.time_unix_nano, value));
                }
            }
        }
        latest.map(|(_, value)| value)
    }

    pub fn histogram(&self, name: &str) -> Option<HistogramPoint> {
        let mut latest: Option<(u64, HistogramPoint)> = None;
        for data in self.metric_data(name) {
            let metric::Data::Histogram(histogram) = data else {
                continue;
            };
            for point in &histogram.data_points {
                if latest
                    .as_ref()
                    .is_none_or(|(time, _)| point.time_unix_nano >= *time)
                {
                    latest = Some((
                        point.time_unix_nano,
                        (
                            point.explicit_bounds.clone(),
                            point.bucket_counts.clone(),
                            point.count,
                        ),
                    ));
                }
            }
        }
        latest.map(|(_, histogram)| histogram)
    }

    pub fn span_names(&self) -> BTreeSet<String> {
        self.spans
            .iter()
            .flat_map(|request| &request.resource_spans)
            .flat_map(|resource| &resource.scope_spans)
            .flat_map(|scope| &scope.spans)
            .map(|span| span.name.clone())
            .collect()
    }

    pub fn log_bodies(&self) -> Vec<String> {
        self.logs
            .iter()
            .flat_map(|request| &request.resource_logs)
            .flat_map(|resource| &resource.scope_logs)
            .flat_map(|scope| &scope.log_records)
            .filter_map(|record| text(record.body.as_ref()))
            .collect()
    }

    pub fn span_ids(&self, name: &str) -> Option<(String, String)> {
        self.spans
            .iter()
            .flat_map(|request| &request.resource_spans)
            .flat_map(|resource| &resource.scope_spans)
            .flat_map(|scope| &scope.spans)
            .find(|span| span.name == name)
            .map(|span| (hex(&span.trace_id), hex(&span.span_id)))
    }

    pub fn log_ids(&self, body: &str) -> Option<(String, String)> {
        self.logs
            .iter()
            .flat_map(|request| &request.resource_logs)
            .flat_map(|resource| &resource.scope_logs)
            .flat_map(|scope| &scope.log_records)
            .find(|record| text(record.body.as_ref()).as_deref() == Some(body))
            .map(|record| (hex(&record.trace_id), hex(&record.span_id)))
    }

    fn resources(&self, signal: Signal) -> Vec<Option<&Resource>> {
        match signal {
            Signal::Metrics => self
                .metrics
                .iter()
                .flat_map(|request| &request.resource_metrics)
                .map(|resource| resource.resource.as_ref())
                .collect(),
            Signal::Spans => self
                .spans
                .iter()
                .flat_map(|request| &request.resource_spans)
                .map(|resource| resource.resource.as_ref())
                .collect(),
            Signal::Logs => self
                .logs
                .iter()
                .flat_map(|request| &request.resource_logs)
                .map(|resource| resource.resource.as_ref())
                .collect(),
        }
    }

    pub fn resource_attribute(&self, signal: Signal, key: &str) -> Option<String> {
        let values: BTreeSet<Option<String>> = self
            .resources(signal)
            .into_iter()
            .map(|resource| {
                resource?
                    .attributes
                    .iter()
                    .find(|attribute| attribute.key == key)
                    .and_then(|attribute| text(attribute.value.as_ref()))
            })
            .collect();
        let mut values = values.into_iter();
        match (values.next(), values.next()) {
            (Some(only), None) => only,
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
struct Collector {
    received: Arc<Mutex<Received>>,
}

#[tonic::async_trait]
impl MetricsService for Collector {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        locked(&self.received).metrics.push(request.into_inner());
        Ok(Response::new(ExportMetricsServiceResponse::default()))
    }
}

#[tonic::async_trait]
impl TraceService for Collector {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        locked(&self.received).spans.push(request.into_inner());
        Ok(Response::new(ExportTraceServiceResponse::default()))
    }
}

#[tonic::async_trait]
impl LogsService for Collector {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        locked(&self.received).logs.push(request.into_inner());
        Ok(Response::new(ExportLogsServiceResponse::default()))
    }
}

const LOOPBACK: &str = "127.0.0.1";

#[derive(Debug)]
pub struct OtlpReceiver {
    addr: SocketAddr,
    ca_pem: Option<String>,
    received: Arc<Mutex<Received>>,
    server: JoinHandle<()>,
}

impl OtlpReceiver {
    pub async fn start() -> Result<Self, TestkitError> {
        Self::serve(None).await
    }

    pub async fn start_tls() -> Result<Self, TestkitError> {
        let certificates = TestCertificates::generate_for(LOOPBACK)?;
        Self::serve(Some(certificates)).await
    }

    async fn serve(certificates: Option<TestCertificates>) -> Result<Self, TestkitError> {
        let listener = TcpListener::bind((LOOPBACK, 0)).await?;
        let addr = listener.local_addr()?;
        let received = Arc::new(Mutex::new(Received::default()));
        let collector = Collector {
            received: Arc::clone(&received),
        };
        let mut builder = Server::builder();
        if let Some(certificates) = &certificates {
            builder = builder.tls_config(ServerTlsConfig::new().identity(Identity::from_pem(
                certificates.server_pem(),
                certificates.server_key_pem(),
            )))?;
        }
        let router = builder
            .add_service(MetricsServiceServer::new(collector.clone()))
            .add_service(TraceServiceServer::new(collector.clone()))
            .add_service(LogsServiceServer::new(collector));
        let server = tokio::spawn(async move {
            let _ = router
                .serve_with_incoming(TcpIncoming::from(listener))
                .await;
        });
        Ok(Self {
            addr,
            ca_pem: certificates.map(|certificates| certificates.ca_pem().to_owned()),
            received,
            server,
        })
    }

    pub fn endpoint(&self) -> String {
        let scheme = if self.ca_pem.is_some() {
            "https"
        } else {
            "http"
        };
        format!("{scheme}://{}", self.addr)
    }

    pub fn ca_pem(&self) -> Option<&str> {
        self.ca_pem.as_deref()
    }

    pub fn received(&self) -> Received {
        locked(&self.received).clone()
    }

    pub async fn wait_for(
        &self,
        within: Duration,
        ready: impl Fn(&Received) -> bool,
    ) -> Result<Received, TestkitError> {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            let received = self.received();
            if ready(&received) {
                return Ok(received);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(TestkitError::Timeout("the OTLP receiver's condition"));
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }
}

impl Drop for OtlpReceiver {
    fn drop(&mut self) {
        self.server.abort();
    }
}

pub async fn blackhole() -> Result<(String, JoinHandle<()>), TestkitError> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let task = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((stream, _)) = listener.accept().await {
            held.push(stream);
        }
    });
    Ok((endpoint, task))
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
    use opentelemetry_proto::tonic::metrics::v1::{
        Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, metric, number_data_point,
    };
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpStream;
    use tonic::transport::{Certificate, Channel, ClientTlsConfig};

    use super::*;

    fn one_sum_request(value: i64) -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "pgcdc.events".to_owned(),
                        data: Some(metric::Data::Sum(Sum {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![KeyValue {
                                    key: "table".to_owned(),
                                    value: Some(AnyValue {
                                        value: Some(any_value::Value::StringValue(
                                            "orders".to_owned(),
                                        )),
                                    }),
                                    ..KeyValue::default()
                                }],
                                time_unix_nano: 1,
                                value: Some(number_data_point::Value::AsInt(value)),
                                ..NumberDataPoint::default()
                            }],
                            aggregation_temporality: 2,
                            is_monotonic: true,
                        })),
                        ..Metric::default()
                    }],
                    ..ScopeMetrics::default()
                }],
                ..ResourceMetrics::default()
            }],
        }
    }

    #[tokio::test]
    async fn the_receiver_records_each_export() {
        let receiver = OtlpReceiver::start().await.unwrap();
        let mut client = MetricsServiceClient::connect(receiver.endpoint())
            .await
            .unwrap();
        client.export(one_sum_request(7)).await.unwrap();
        let exports = receiver.received();
        assert_eq!(exports.metrics.len(), 1);
        assert_eq!(
            exports.metric_names(),
            BTreeSet::from(["pgcdc.events".to_owned()])
        );
        assert_eq!(
            exports.sum("pgcdc.events", &[("table", "orders")]),
            Some(7.0)
        );
        assert_eq!(exports.sum("pgcdc.events", &[("table", "items")]), None);
        assert_eq!(exports.gauge("pgcdc.events"), None);
    }

    #[test]
    fn the_latest_cumulative_value_of_each_series_is_summed() {
        let mut received = Received::default();
        for (time, value) in [(2, 5), (1, 3)] {
            let mut request = one_sum_request(value);
            for point in request
                .resource_metrics
                .iter_mut()
                .flat_map(|resource| &mut resource.scope_metrics)
                .flat_map(|scope| &mut scope.metrics)
                .filter_map(|metric| match metric.data.as_mut() {
                    Some(metric::Data::Sum(sum)) => Some(&mut sum.data_points),
                    _ => None,
                })
                .flatten()
            {
                point.time_unix_nano = time;
            }
            received.metrics.push(request);
        }
        let mut other = one_sum_request(4);
        for attribute in other
            .resource_metrics
            .iter_mut()
            .flat_map(|resource| &mut resource.scope_metrics)
            .flat_map(|scope| &mut scope.metrics)
            .filter_map(|metric| match metric.data.as_mut() {
                Some(metric::Data::Sum(sum)) => Some(&mut sum.data_points),
                _ => None,
            })
            .flatten()
            .flat_map(|point| &mut point.attributes)
        {
            attribute.value = Some(AnyValue {
                value: Some(any_value::Value::StringValue("items".to_owned())),
            });
        }
        received.metrics.push(other);
        assert_eq!(
            received.sum("pgcdc.events", &[("table", "orders")]),
            Some(5.0)
        );
        assert_eq!(received.sum("pgcdc.events", &[]), Some(9.0));
    }

    #[tokio::test]
    async fn a_tls_receiver_records_exports_from_a_client_that_trusts_its_ca() {
        let receiver = OtlpReceiver::start_tls().await.unwrap();
        assert!(receiver.endpoint().starts_with("https://127.0.0.1:"));
        let tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(receiver.ca_pem().unwrap()));
        let channel = Channel::from_shared(receiver.endpoint())
            .unwrap()
            .tls_config(tls)
            .unwrap()
            .connect()
            .await
            .unwrap();
        MetricsServiceClient::new(channel)
            .export(one_sum_request(7))
            .await
            .unwrap();
        assert_eq!(receiver.received().metrics.len(), 1);
    }

    fn with_resource(
        mut request: ExportMetricsServiceRequest,
        pipeline: Option<&str>,
    ) -> ExportMetricsServiceRequest {
        for resource in &mut request.resource_metrics {
            resource.resource = Some(Resource {
                attributes: pipeline
                    .map(|value| KeyValue {
                        key: "pgcdc.pipeline".to_owned(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(value.to_owned())),
                        }),
                        ..KeyValue::default()
                    })
                    .into_iter()
                    .collect(),
                ..Resource::default()
            });
        }
        request
    }

    #[test]
    fn a_resource_attribute_is_reported_only_when_every_export_of_the_signal_carries_it() {
        let mut received = Received::default();
        assert_eq!(
            received.resource_attribute(Signal::Metrics, "pgcdc.pipeline"),
            None
        );
        received
            .metrics
            .push(with_resource(one_sum_request(1), Some("p")));
        received
            .metrics
            .push(with_resource(one_sum_request(2), Some("p")));
        assert_eq!(
            received
                .resource_attribute(Signal::Metrics, "pgcdc.pipeline")
                .as_deref(),
            Some("p")
        );
        assert_eq!(
            received.resource_attribute(Signal::Spans, "pgcdc.pipeline"),
            None
        );
        received
            .metrics
            .push(with_resource(one_sum_request(3), None));
        assert_eq!(
            received.resource_attribute(Signal::Metrics, "pgcdc.pipeline"),
            None
        );
        received.metrics.pop();
        received
            .metrics
            .push(with_resource(one_sum_request(3), Some("q")));
        assert_eq!(
            received.resource_attribute(Signal::Metrics, "pgcdc.pipeline"),
            None
        );
    }

    #[tokio::test]
    async fn blackhole_accepts_a_connection_and_never_answers() {
        let (endpoint, task) = blackhole().await.unwrap();
        let address = endpoint.strip_prefix("http://").unwrap();
        let mut stream = TcpStream::connect(address).await.unwrap();
        let mut buffer = [0_u8; 16];
        let read = tokio::time::timeout(Duration::from_millis(200), stream.read(&mut buffer)).await;
        assert!(read.is_err());
        task.abort();
    }
}
