use std::fmt;
use std::time::Duration;

use secrecy::{ExposeSecret, SecretString};

use super::{TelemetryError, TelemetryIdentity, TelemetryLayers};
use crate::config::SentryConfig;

pub const SENTRY_DSN_VARIABLE: &str = "SENTRY_DSN";
pub const REPORTING_FLUSH_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone)]
pub struct ReportingPlan {
    pub dsn: SecretString,
    pub environment: Option<String>,
    pub sample_rate: f64,
}

pub fn reporting_plan(config: &SentryConfig, env_dsn: Option<&str>) -> Option<ReportingPlan> {
    let configured = config
        .dsn
        .as_ref()
        .map(ExposeSecret::expose_secret)
        .filter(|dsn| !dsn.is_empty());
    let dsn = configured.or(env_dsn.filter(|dsn| !dsn.is_empty()))?;
    Some(ReportingPlan {
        dsn: SecretString::from(dsn.to_owned()),
        environment: config
            .environment
            .clone()
            .filter(|environment| !environment.is_empty()),
        sample_rate: config.sample_rate,
    })
}

pub struct ErrorReporting {
    #[cfg(feature = "sentry")]
    guard: Option<sentry::ClientInitGuard>,
}

impl fmt::Debug for ErrorReporting {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ErrorReporting")
            .field("enabled", &self.is_enabled())
            .finish()
    }
}

impl ErrorReporting {
    #[cfg(feature = "sentry")]
    pub fn install(
        plan: Option<&ReportingPlan>,
        identity: &TelemetryIdentity,
    ) -> Result<(Self, Option<TelemetryLayers>), TelemetryError> {
        client::install(plan, identity, None)
    }

    #[cfg(not(feature = "sentry"))]
    pub fn install(
        plan: Option<&ReportingPlan>,
        identity: &TelemetryIdentity,
    ) -> Result<(Self, Option<TelemetryLayers>), TelemetryError> {
        let _ = (plan, identity);
        Ok((Self {}, None))
    }

    #[cfg(feature = "sentry")]
    pub fn is_enabled(&self) -> bool {
        self.guard.is_some()
    }

    #[cfg(not(feature = "sentry"))]
    pub fn is_enabled(&self) -> bool {
        false
    }

    #[cfg(feature = "sentry")]
    pub fn shutdown(self, within: Duration) -> bool {
        let Some(guard) = self.guard else {
            return true;
        };
        let (closed, wait) = std::sync::mpsc::channel();
        let spawned = std::thread::Builder::new()
            .name("pg-cdc-sentry".to_owned())
            .spawn(move || {
                closed.send(guard.close(Some(within))).ok();
            });
        spawned.is_ok() && wait.recv_timeout(within).unwrap_or(false)
    }

    #[cfg(not(feature = "sentry"))]
    pub fn shutdown(self, _within: Duration) -> bool {
        true
    }
}

#[cfg(feature = "sentry")]
mod client {
    use std::borrow::Cow;
    use std::sync::Arc;
    use std::time::Duration;

    use secrecy::ExposeSecret as _;
    use sentry::integrations::tracing::EventFilter;
    use sentry::protocol::{Breadcrumb, Context, Event, Map, Value};
    use sentry::transports::UreqHttpTransportOptions;
    use sentry::types::Dsn;
    use sentry::{ClientOptions, TransportFactory, TransportOptions};
    use tracing::Level;
    use tracing::level_filters::LevelFilter;
    use tracing_subscriber::Layer as _;
    use ureq::tls::{TlsConfig, TlsProvider};

    use super::{
        ErrorReporting, ReportingPlan, TelemetryError, TelemetryIdentity, TelemetryLayers,
    };

    // ureq sets no request timeout by default, so an unreachable Sentry would hold the send queue forever.
    const SEND_TIMEOUT: Duration = Duration::from_secs(5);
    const REDACTED: &str = "[redacted]";
    const SENSITIVE_WORDS: [&str; 26] = [
        "password",
        "passwd",
        "pwd",
        "secret",
        "token",
        "dsn",
        "credential",
        "credentials",
        "authorization",
        "auth",
        "cookie",
        "apikey",
        "signature",
        "url",
        "uri",
        "conninfo",
        "body",
        "payload",
        "row",
        "rows",
        "before",
        "after",
        "value",
        "values",
        "envelope",
        "tuple",
    ];
    const SENSITIVE_SUFFIXES: [&str; 4] = ["password", "passwd", "secret", "token"];
    const SENSITIVE_PAIRS: [&str; 4] = ["api_key", "access_key", "private_key", "session_key"];

    pub(super) fn install(
        plan: Option<&ReportingPlan>,
        identity: &TelemetryIdentity,
        transport: Option<Arc<dyn TransportFactory>>,
    ) -> Result<(ErrorReporting, Option<TelemetryLayers>), TelemetryError> {
        let Some(plan) = plan else {
            return Ok((ErrorReporting { guard: None }, None));
        };
        let mut options = options(plan, identity)?;
        options.transport = transport.or_else(|| bounded_transport(&options));
        let guard = sentry::init(options);
        let layer = sentry::integrations::tracing::layer()
            .event_filter(|metadata| match *metadata.level() {
                Level::ERROR => EventFilter::Event,
                Level::WARN => EventFilter::Breadcrumb,
                _ => EventFilter::Ignore,
            })
            .span_filter(|_| false)
            .with_filter(LevelFilter::WARN);
        Ok((
            ErrorReporting { guard: Some(guard) },
            Some(TelemetryLayers {
                layers: vec![Box::new(layer)],
            }),
        ))
    }

    fn bounded_transport(options: &ClientOptions) -> Option<Arc<dyn TransportFactory>> {
        let general = TransportOptions::try_from_client_options(options)?;
        let agent = ureq::Agent::config_builder()
            .timeout_global(Some(SEND_TIMEOUT))
            .tls_config(TlsConfig::builder().provider(TlsProvider::Rustls).build())
            .build()
            .new_agent();
        let transport = UreqHttpTransportOptions::from(general)
            .with_agent(agent)
            .build();
        Some(Arc::new(Arc::new(transport)))
    }

    #[allow(clippy::cast_possible_truncation)]
    fn rate(ratio: f64) -> f32 {
        ratio as f32
    }

    fn options(
        plan: &ReportingPlan,
        identity: &TelemetryIdentity,
    ) -> Result<ClientOptions, TelemetryError> {
        let dsn: Dsn =
            plan.dsn
                .expose_secret()
                .parse()
                .map_err(|error: sentry::types::ParseDsnError| {
                    TelemetryError::Reporting(error.to_string())
                })?;
        let mut options = ClientOptions::new()
            .sample_rate(rate(plan.sample_rate.clamp(0.0, 1.0)))
            .send_default_pii(false);
        options.dsn = Some(dsn);
        options.release = Some(Cow::Owned(format!("pg-cdc@{}", env!("CARGO_PKG_VERSION"))));
        options.environment = plan.environment.clone().map(Cow::Owned);
        options.server_name = Some(Cow::Owned(identity.holder.clone()));
        let (pipeline, holder) = (identity.pipeline.clone(), identity.holder.clone());
        options.before_send = Some(Arc::new(move |event| {
            Some(prepare(event, &pipeline, &holder))
        }));
        options.before_breadcrumb = Some(Arc::new(|breadcrumb| Some(scrub_breadcrumb(breadcrumb))));
        Ok(options)
    }

    fn prepare(mut event: Event<'static>, pipeline: &str, holder: &str) -> Event<'static> {
        event.user = None;
        event.request = None;
        event.message = event.message.map(|message| scrub_text(&message));
        if let Some(entry) = event.logentry.as_mut() {
            entry.message = scrub_text(&entry.message);
            for param in &mut entry.params {
                scrub_value(param);
            }
        }
        for exception in &mut event.exception.values {
            exception.value = exception.value.as_deref().map(scrub_text);
        }
        event.tags = std::mem::take(&mut event.tags)
            .into_iter()
            .map(|(key, value)| {
                let value = if sensitive(&key) {
                    REDACTED.to_owned()
                } else {
                    scrub_text(&value)
                };
                (key, value)
            })
            .collect();
        scrub_map(&mut event.extra);
        for context in event.contexts.values_mut() {
            if let Context::Other(map) = context {
                scrub_map(map);
            }
        }
        event.breadcrumbs.values = std::mem::take(&mut event.breadcrumbs.values)
            .into_iter()
            .map(scrub_breadcrumb)
            .collect();
        event
            .tags
            .insert("pipeline".to_owned(), pipeline.to_owned());
        event.tags.insert("holder".to_owned(), holder.to_owned());
        event
    }

    fn scrub_breadcrumb(mut breadcrumb: Breadcrumb) -> Breadcrumb {
        breadcrumb.message = breadcrumb.message.map(|message| scrub_text(&message));
        scrub_map(&mut breadcrumb.data);
        breadcrumb
    }

    fn scrub_map(map: &mut Map<String, Value>) {
        for (key, value) in map.iter_mut() {
            if sensitive(key) {
                *value = Value::String(REDACTED.to_owned());
            } else {
                scrub_value(value);
            }
        }
    }

    fn scrub_value(value: &mut Value) {
        match value {
            Value::String(text) => *text = scrub_text(text),
            Value::Array(items) => items.iter_mut().for_each(scrub_value),
            Value::Object(map) => {
                for (key, item) in map.iter_mut() {
                    if sensitive(key) {
                        *item = Value::String(REDACTED.to_owned());
                    } else {
                        scrub_value(item);
                    }
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
        }
    }

    pub(super) fn sensitive(key: &str) -> bool {
        let key = key.to_ascii_lowercase();
        SENSITIVE_PAIRS.iter().any(|pair| key.contains(pair))
            || key
                .split(|character: char| !character.is_ascii_alphanumeric())
                .any(|word| {
                    SENSITIVE_WORDS.contains(&word)
                        || SENSITIVE_SUFFIXES
                            .iter()
                            .any(|suffix| word.ends_with(suffix))
                })
    }

    pub(super) fn scrub_text(text: &str) -> String {
        redact_assignments(&redact_userinfo(text))
    }

    fn redact_userinfo(text: &str) -> String {
        let mut out = String::with_capacity(text.len());
        let mut rest = text;
        while let Some(at) = rest.find("://") {
            let (head, tail) = rest.split_at(at + 3);
            out.push_str(head);
            let authority_end = tail
                .find(|character: char| {
                    character.is_whitespace() || matches!(character, '/' | '?' | '#' | '"' | '\'')
                })
                .unwrap_or(tail.len());
            let (authority, after) = tail.split_at(authority_end);
            match authority.rfind('@') {
                Some(userinfo_end) => {
                    out.push_str(REDACTED);
                    out.push_str(&authority[userinfo_end..]);
                }
                None => out.push_str(authority),
            }
            rest = after;
        }
        out.push_str(rest);
        out
    }

    fn redact_assignments(text: &str) -> String {
        let mut out = String::with_capacity(text.len());
        let mut rest = text;
        while let Some(equals) = rest.find('=') {
            let (head, tail) = rest.split_at(equals);
            let key_start = head
                .rfind(|character: char| {
                    !(character.is_ascii_alphanumeric() || matches!(character, '_' | '-'))
                })
                .map_or(0, |index| index + 1);
            out.push_str(head);
            out.push('=');
            let value = &tail[1..];
            let value_end = value
                .find(|character: char| {
                    character.is_whitespace() || matches!(character, '&' | ';' | ',' | '"' | '\'')
                })
                .unwrap_or(value.len());
            if sensitive(&head[key_start..]) && value_end > 0 {
                out.push_str(REDACTED);
            } else {
                out.push_str(&value[..value_end]);
            }
            rest = &value[value_end..];
        }
        out.push_str(rest);
        out
    }

    #[cfg(test)]
    pub(super) fn install_with(
        plan: &ReportingPlan,
        identity: &TelemetryIdentity,
        transport: Arc<dyn TransportFactory>,
    ) -> (ErrorReporting, TelemetryLayers) {
        let (reporting, layers) = install(Some(plan), identity, Some(transport)).unwrap();
        (reporting, layers.unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sentry(dsn: Option<&str>, environment: Option<&str>) -> SentryConfig {
        SentryConfig {
            dsn: dsn.map(|dsn| SecretString::from(dsn.to_owned())),
            environment: environment.map(str::to_owned),
            ..SentryConfig::default()
        }
    }

    fn identity() -> TelemetryIdentity {
        TelemetryIdentity {
            pipeline: "orders".to_owned(),
            holder: "task-1".to_owned(),
        }
    }

    #[test]
    fn reporting_is_planned_only_when_a_dsn_is_configured_or_in_the_environment() {
        assert!(reporting_plan(&sentry(None, None), None).is_none());
        assert!(reporting_plan(&sentry(Some(""), None), Some("")).is_none());
        let from_env =
            reporting_plan(&sentry(None, Some("prod")), Some("https://k@env/1")).unwrap();
        assert_eq!(from_env.dsn.expose_secret(), "https://k@env/1");
        assert_eq!(from_env.environment.as_deref(), Some("prod"));
        assert_eq!(from_env.sample_rate, 1.0);
        let configured = reporting_plan(
            &sentry(Some("https://k@cfg/2"), Some("")),
            Some("https://k@env/1"),
        )
        .unwrap();
        assert_eq!(configured.dsn.expose_secret(), "https://k@cfg/2");
        assert_eq!(configured.environment, None);
        assert_eq!(SENTRY_DSN_VARIABLE, "SENTRY_DSN");
    }

    #[test]
    fn without_a_plan_nothing_is_installed() {
        let (reporting, layers) = ErrorReporting::install(None, &identity()).unwrap();
        assert!(!reporting.is_enabled());
        assert!(layers.is_none());
        #[cfg(feature = "sentry")]
        assert!(sentry::Hub::current().client().is_none());
        assert!(reporting.shutdown(Duration::ZERO));
    }

    #[cfg(feature = "sentry")]
    mod capturing {
        use std::sync::Arc;

        use sentry::protocol::{Context, Event, Level};
        use sentry::test::TestTransport;
        use tracing_subscriber::layer::SubscriberExt as _;

        use super::*;

        fn plan() -> ReportingPlan {
            ReportingPlan {
                dsn: SecretString::from("https://public@sentry.invalid/1".to_owned()),
                environment: Some("staging".to_owned()),
                sample_rate: 1.0,
            }
        }

        fn captured(emit: impl FnOnce()) -> Vec<Event<'static>> {
            let transport = TestTransport::new();
            let (reporting, layers) =
                client::install_with(&plan(), &identity(), Arc::new(Arc::clone(&transport)));
            assert!(reporting.is_enabled());
            let subscriber = tracing_subscriber::registry().with(layers.into_layer());
            tracing::subscriber::with_default(subscriber, emit);
            assert!(reporting.shutdown(Duration::from_secs(2)));
            transport.fetch_and_clear_events()
        }

        fn fields(event: &Event<'static>) -> String {
            serde_json::to_string(event).unwrap()
        }

        #[test]
        fn an_error_is_sent_with_the_release_environment_and_identity() {
            let events = captured(|| {
                tracing::error!(target: "pg_cdc::session", lane = 3, "the sink rejected the event for good");
            });
            assert_eq!(events.len(), 1, "{events:?}");
            let event = &events[0];
            assert_eq!(event.level, Level::Error);
            assert_eq!(
                event.message.as_deref(),
                Some("the sink rejected the event for good")
            );
            assert_eq!(
                event.release.as_deref(),
                Some(format!("pg-cdc@{}", env!("CARGO_PKG_VERSION")).as_str())
            );
            assert_eq!(event.environment.as_deref(), Some("staging"));
            assert_eq!(event.server_name.as_deref(), Some("task-1"));
            assert_eq!(
                event.tags.get("pipeline").map(String::as_str),
                Some("orders")
            );
            assert_eq!(event.tags.get("holder").map(String::as_str), Some("task-1"));
            assert!(event.user.is_none());
        }

        #[test]
        fn warnings_and_info_lines_send_nothing_on_their_own() {
            let events = captured(|| {
                tracing::warn!(target: "pg_cdc::lane", "delivery failed; retrying");
                tracing::info!(target: "pg_cdc::lane", "delivered");
            });
            assert!(events.is_empty(), "{events:?}");
        }

        #[test]
        fn a_warning_rides_along_as_a_breadcrumb_of_the_next_error() {
            let events = captured(|| {
                tracing::warn!(target: "pg_cdc::lane", "delivery failed; retrying");
                tracing::info!(target: "pg_cdc::lane", "delivered");
                tracing::error!(target: "pg_cdc::lane", "parking it dead");
            });
            assert_eq!(events.len(), 1, "{events:?}");
            let crumbs: Vec<Option<&str>> = events[0]
                .breadcrumbs
                .values
                .iter()
                .map(|crumb| crumb.message.as_deref())
                .collect();
            assert_eq!(crumbs, vec![Some("delivery failed; retrying")]);
        }

        #[test]
        fn credentials_and_row_data_never_reach_sentry() {
            let events = captured(|| {
                tracing::warn!(
                    target: "pg_cdc::session",
                    url = "postgres://pgcdc:crumb-secret@db.internal:5432/app",
                    "control connection closed"
                );
                tracing::error!(
                    target: "pg_cdc::session",
                    password = "hunter2",
                    dsn = "https://public-key@o1.ingest.sentry.io/2",
                    body = r#"{"after":{"email":"ada@example.com"}}"#,
                    error = "connect to postgres://pgcdc:s3cret@db.internal:5432/app failed: X-Amz-Security-Token=IQoJb3JpZ2lu&X-Amz-Signature=abc123 password=topsecret",
                    tags.token = "tok-123",
                    order_key = "ab000000000000000000000000000001",
                    "pg-cdc stopped: cannot reach postgresql://admin:pa55@db.internal/app"
                );
            });
            assert_eq!(events.len(), 1, "{events:?}");
            let sent = fields(&events[0]);
            for leaked in [
                "hunter2",
                "public-key",
                "ada@example.com",
                "s3cret",
                "IQoJb3JpZ2lu",
                "abc123",
                "topsecret",
                "tok-123",
                "pa55",
                "crumb-secret",
            ] {
                assert!(!sent.contains(leaked), "{leaked} leaked: {sent}");
            }
            assert!(sent.contains("ab000000000000000000000000000001"), "{sent}");
            assert!(sent.contains("db.internal:5432/app"), "{sent}");
            assert_eq!(
                events[0].message.as_deref(),
                Some("pg-cdc stopped: cannot reach postgresql://[redacted]@db.internal/app")
            );
            let Some(Context::Other(traced)) = events[0].contexts.get("Rust Tracing Fields") else {
                panic!("no tracing fields: {sent}");
            };
            assert_eq!(traced["password"], "[redacted]");
        }

        #[test]
        fn a_panic_is_captured() {
            let transport = TestTransport::new();
            let (reporting, _layers) =
                client::install_with(&plan(), &identity(), Arc::new(Arc::clone(&transport)));
            let panicked = std::panic::catch_unwind(|| panic!("the lane invariant broke"));
            assert!(panicked.is_err());
            assert!(reporting.shutdown(Duration::from_secs(2)));
            let events = transport.fetch_and_clear_events();
            assert_eq!(events.len(), 1, "{events:?}");
            let exception = &events[0].exception.values[0];
            assert_eq!(exception.value.as_deref(), Some("the lane invariant broke"));
            assert_eq!(
                events[0].tags.get("pipeline").map(String::as_str),
                Some("orders")
            );
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn shutdown_against_a_blackholed_sentry_gives_up_at_its_deadline() {
            let (endpoint, server) = pg_cdc_testkit::blackhole().await.unwrap();
            let address = endpoint.trim_start_matches("http://");
            let plan = ReportingPlan {
                dsn: SecretString::from(format!("http://public@{address}/1")),
                ..plan()
            };
            let (reporting, layers) = ErrorReporting::install(Some(&plan), &identity()).unwrap();
            let subscriber = tracing_subscriber::registry().with(layers.unwrap().into_layer());
            tracing::subscriber::with_default(subscriber, || {
                for attempt in 0..64 {
                    tracing::error!(target: "pg_cdc::lane", attempt, "the sink rejected the event for good");
                }
            });
            let started = std::time::Instant::now();
            let finished =
                tokio::task::spawn_blocking(move || reporting.shutdown(Duration::from_millis(300)));
            let flushed = tokio::time::timeout(Duration::from_secs(5), finished)
                .await
                .expect("shutdown must not outlive its budget")
                .unwrap();
            assert!(!flushed);
            assert!(
                started.elapsed() < Duration::from_secs(2),
                "{:?}",
                started.elapsed()
            );
            server.abort();
        }

        #[test]
        fn text_scrubbing_keeps_hosts_and_drops_secrets() {
            let cases = [
                (
                    "postgres://u:p@h:5432/db and https://k@sentry.io/1",
                    "postgres://[redacted]@h:5432/db and https://[redacted]@sentry.io/1",
                ),
                (
                    "host=db user=pgcdc password=x sslmode=require",
                    "host=db user=pgcdc password=[redacted] sslmode=require",
                ),
                ("http://plain.host/path?a=1", "http://plain.host/path?a=1"),
                ("no secrets here", "no secrets here"),
                ("token=", "token="),
            ];
            for (input, expected) in cases {
                assert_eq!(client::scrub_text(input), expected, "{input}");
            }
            for key in [
                "password",
                "PGPASSWORD",
                "api_key",
                "after",
                "x-amz-security-token",
                "row_values",
            ] {
                assert!(client::sensitive(key), "{key}");
            }
            for key in ["order_key", "lane", "error", "operation", "table", "id"] {
                assert!(!client::sensitive(key), "{key}");
            }
        }
    }
}
