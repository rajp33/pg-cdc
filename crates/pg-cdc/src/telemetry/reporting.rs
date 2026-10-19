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
    pub fn shutdown(mut self, within: Duration) -> bool {
        self.guard
            .take()
            .is_none_or(|guard| client::close_within(guard, within))
    }

    #[cfg(not(feature = "sentry"))]
    pub fn shutdown(self, _within: Duration) -> bool {
        true
    }
}

#[cfg(feature = "sentry")]
impl Drop for ErrorReporting {
    fn drop(&mut self) {
        if let Some(guard) = self.guard.take() {
            client::close_within(guard, REPORTING_FLUSH_TIMEOUT);
        }
    }
}

#[cfg(feature = "sentry")]
mod client {
    use std::borrow::Cow;
    use std::sync::{Arc, Mutex, OnceLock, PoisonError};
    use std::time::Duration;

    use secrecy::ExposeSecret as _;
    use std::fmt;

    use sentry::integrations::tracing::EventMapping;
    use sentry::protocol::{Breadcrumb, Context, Event, Exception, Map, Mechanism, Value};
    use sentry::transports::UreqHttpTransportOptions;
    use sentry::types::Dsn;
    use sentry::{ClientOptions, TransportFactory, TransportOptions};
    use tracing::Level;
    use tracing::field::{Field, Visit};
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
    const OUR_TARGET: &str = "pg_cdc";
    const PANIC_MESSAGE: &str = "pg-cdc panicked";
    const MAX_LABEL_BYTES: usize = 64;
    const NUMBER_FIELDS: [&str; 16] = [
        "lane",
        "partition",
        "attempts",
        "delay_ms",
        "epoch",
        "own_pid",
        "since_last_ok_ms",
        "type_oid",
        "sent",
        "outcomes",
        "entries",
        "count",
        "dead",
        "fifo",
        "max_message_bytes",
        "limit_from_topic",
    ];
    const LABEL_FIELDS: [&str; 4] = ["id", "operation", "kind", "sink"];
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
        install_panic_hook();
        let layer = sentry::integrations::tracing::layer()
            .event_mapper(|event, _| allowlisted(event))
            .span_filter(|_| false)
            .with_filter(LevelFilter::WARN);
        Ok((
            ErrorReporting { guard: Some(guard) },
            Some(TelemetryLayers {
                layers: vec![Box::new(layer)],
            }),
        ))
    }

    type Closer = Box<dyn FnOnce() + Send>;

    pub(super) fn close_within(guard: sentry::ClientInitGuard, within: Duration) -> bool {
        close_within_using(guard, within, |closer| {
            std::thread::Builder::new()
                .name("pg-cdc-sentry".to_owned())
                .spawn(closer)
                .map(drop)
        })
    }

    pub(super) fn close_within_using(
        guard: sentry::ClientInitGuard,
        within: Duration,
        spawn: impl FnOnce(Closer) -> std::io::Result<()>,
    ) -> bool {
        let slot = Arc::new(Mutex::new(Some(guard)));
        let handed = Arc::clone(&slot);
        let (closed, wait) = std::sync::mpsc::channel();
        let spawned = spawn(Box::new(move || {
            let guard = handed.lock().unwrap_or_else(PoisonError::into_inner).take();
            if let Some(guard) = guard {
                closed.send(guard.close(Some(within))).ok();
            }
        }));
        if spawned.is_err() {
            // Dropping a ClientInitGuard closes the client synchronously with no bound.
            if let Some(guard) = slot.lock().unwrap_or_else(PoisonError::into_inner).take() {
                std::mem::forget(guard);
            }
            return false;
        }
        wait.recv_timeout(within).unwrap_or(false)
    }

    static PANIC_HOOK: OnceLock<()> = OnceLock::new();

    fn install_panic_hook() {
        PANIC_HOOK.get_or_init(|| {
            let previous = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |info| {
                sentry::capture_event(panic_event(info));
                previous(info);
            }));
        });
    }

    fn panic_event(info: &std::panic::PanicHookInfo<'_>) -> Event<'static> {
        let exception = Exception {
            ty: "panic".to_owned(),
            value: info
                .location()
                .map(|location| format!("at {}:{}", location.file(), location.line())),
            stacktrace: sentry::integrations::backtrace::current_stacktrace(),
            mechanism: Some(Mechanism {
                ty: "panic".to_owned(),
                handled: Some(false),
                ..Mechanism::default()
            }),
            ..Exception::default()
        };
        Event {
            level: sentry::Level::Fatal,
            message: Some(PANIC_MESSAGE.to_owned()),
            exception: vec![exception].into(),
            ..Event::default()
        }
    }

    #[derive(Default)]
    struct Allowed {
        fields: Map<String, Value>,
    }

    fn label_shaped(text: &str) -> bool {
        (1..=MAX_LABEL_BYTES).contains(&text.len())
            && text.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
            })
    }

    impl Allowed {
        fn label(&mut self, field: &Field, text: String) {
            if LABEL_FIELDS.contains(&field.name()) && label_shaped(&text) {
                self.fields
                    .insert(field.name().to_owned(), Value::String(text));
            }
        }

        fn number(&mut self, field: &Field, value: Value) {
            if NUMBER_FIELDS.contains(&field.name()) {
                self.fields.insert(field.name().to_owned(), value);
            }
        }
    }

    impl Visit for Allowed {
        fn record_i64(&mut self, field: &Field, value: i64) {
            self.number(field, Value::from(value));
        }

        fn record_u64(&mut self, field: &Field, value: u64) {
            self.number(field, Value::from(value));
        }

        fn record_bool(&mut self, field: &Field, value: bool) {
            self.number(field, Value::from(value));
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            self.label(field, value.to_owned());
        }

        fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
            if LABEL_FIELDS.contains(&field.name()) {
                self.label(field, format!("{value:?}"));
            }
        }
    }

    fn allowlisted(event: &tracing::Event<'_>) -> EventMapping {
        let metadata = event.metadata();
        let ours = metadata.target().starts_with(OUR_TARGET);
        let mut allowed = Allowed::default();
        if ours {
            event.record(&mut allowed);
        }
        let message = match (metadata.file(), metadata.line()) {
            (Some(file), Some(line)) => format!("{} at {file}:{line}", metadata.target()),
            _ => metadata.target().to_owned(),
        };
        match *metadata.level() {
            Level::ERROR => {
                let mut location = Map::new();
                location.insert("module_path".to_owned(), metadata.module_path().into());
                location.insert("file".to_owned(), metadata.file().into());
                location.insert("line".to_owned(), metadata.line().into());
                let mut contexts = Map::new();
                contexts.insert("location".to_owned(), Context::Other(location));
                EventMapping::Event(Box::new(Event {
                    level: sentry::Level::Error,
                    logger: Some(metadata.target().to_owned()),
                    message: Some(message),
                    extra: allowed.fields.clone(),
                    contexts,
                    ..Event::default()
                }))
            }
            Level::WARN => EventMapping::Breadcrumb(Breadcrumb {
                ty: "log".to_owned(),
                category: Some(metadata.target().to_owned()),
                level: sentry::Level::Warning,
                message: Some(message),
                data: allowed.fields.clone(),
                ..Breadcrumb::default()
            }),
            _ => EventMapping::Ignore,
        }
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
                .char_indices()
                .rev()
                .find(|(_, character)| {
                    !(character.is_ascii_alphanumeric() || matches!(character, '_' | '-'))
                })
                .map_or(0, |(index, character)| index + character.len_utf8());
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

        use sentry::protocol::{Event, Level};
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
            let line = line!() + 2;
            let events = captured(|| {
                tracing::error!(target: "pg_cdc::session", lane = 3, "the sink rejected the event for good");
            });
            assert_eq!(events.len(), 1, "{events:?}");
            let event = &events[0];
            assert_eq!(event.level, Level::Error);
            assert_eq!(event.message, Some(at("pg_cdc::session", line)));
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
            assert_eq!(event.extra.get("lane"), Some(&serde_json::json!(3)));
            assert_eq!(event.logger.as_deref(), Some("pg_cdc::session"));
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
            let line = line!() + 2;
            let events = captured(|| {
                tracing::warn!(target: "pg_cdc::lane", "delivery failed; retrying");
                tracing::info!(target: "pg_cdc::lane", "delivered");
                tracing::error!(target: "pg_cdc::lane", "parking it dead");
            });
            assert_eq!(events.len(), 1, "{events:?}");
            let crumbs: Vec<Option<String>> = events[0]
                .breadcrumbs
                .values
                .iter()
                .map(|crumb| crumb.message.clone())
                .collect();
            assert_eq!(crumbs, vec![Some(at("pg_cdc::lane", line))]);
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
            assert!(
                from_callsite(events[0].message.as_deref(), "pg_cdc::session"),
                "{sent}"
            );
            assert!(!events[0].extra.contains_key("password"), "{sent}");
            assert!(!sent.contains("ab000000000000000000000000000001"), "{sent}");
        }

        #[test]
        fn provider_error_text_never_reaches_events_or_breadcrumbs() {
            let events = captured(|| {
                tracing::warn!(
                    target: "pg_cdc::supervisor",
                    error = "replication stream failed: db error: ERROR: unrecognized encoding: \"customer-row-value\"",
                    delay_ms = 250_u64,
                    "replication session ended; reconnecting"
                );
                tracing::error!(
                    target: "pg_cdc::session",
                    error = "connection failed: password='two words' password=\"double quoted\" password=abc,leftover",
                    id = "000000001a2b3c4d-00000003",
                    operation = "append_parked",
                    "cannot take over the slot; releasing it"
                );
                tracing::error!(
                    target: "tokio_postgres::connection",
                    "db error: duplicate key value (secret-key-42) violates a constraint"
                );
            });
            assert_eq!(events.len(), 2, "{events:?}");
            let sent: String = events.iter().map(fields).collect();
            for leaked in [
                "customer-row-value",
                "two words",
                "double quoted",
                "leftover",
                "secret-key-42",
                "unrecognized encoding",
            ] {
                assert!(!sent.contains(leaked), "{leaked} leaked: {sent}");
            }
            let ours = &events[0];
            assert!(
                from_callsite(ours.message.as_deref(), "pg_cdc::session"),
                "{sent}"
            );
            assert_eq!(
                ours.extra.get("id"),
                Some(&serde_json::json!("000000001a2b3c4d-00000003"))
            );
            assert_eq!(
                ours.extra.get("operation"),
                Some(&serde_json::json!("append_parked"))
            );
            assert!(!ours.extra.contains_key("error"), "{sent}");
            let crumb = &ours.breadcrumbs.values[0];
            assert!(
                from_callsite(crumb.message.as_deref(), "pg_cdc::supervisor"),
                "{sent}"
            );
            assert_eq!(crumb.data.keys().collect::<Vec<_>>(), vec!["delay_ms"]);
            let foreign = &events[1];
            assert_eq!(
                foreign.logger.as_deref(),
                Some("tokio_postgres::connection")
            );
            assert!(
                from_callsite(foreign.message.as_deref(), "tokio_postgres::connection"),
                "{sent}"
            );
        }

        fn at(target: &str, line: u32) -> String {
            format!("{target} at {}:{line}", file!())
        }

        fn from_callsite(message: Option<&str>, target: &str) -> bool {
            message.is_some_and(|message| {
                message
                    .strip_prefix(&format!("{target} at {}:", file!()))
                    .is_some_and(|line| line.parse::<u32>().is_ok())
            })
        }

        #[test]
        fn messages_come_from_the_callsite_and_never_from_values() {
            let error = "password='two words' customer-row-value";
            let events = captured(|| {
                tracing::warn!(
                    target: "pg_cdc::sink::sns",
                    entries = 2_u64,
                    message = %error,
                    "SNS rejected the batch as too long; sending its entries one at a time"
                );
                tracing::warn!(target: "pg_cdc::lane", message = %error);
                tracing::warn!(target: "pg_cdc::lane", "failed: {}", error.trim_end_matches('x'));
                tracing::warn!(target: "pg_cdc::lane", "failed: \"{error}\"");
                tracing::event!(target: "pg_cdc::lane", tracing::Level::ERROR, "failed: {error}");
            });
            assert_eq!(events.len(), 1, "{events:?}");
            let sent = fields(&events[0]);
            for leaked in ["two words", "customer-row-value", "failed"] {
                assert!(!sent.contains(leaked), "{leaked} leaked: {sent}");
            }
            assert!(
                from_callsite(events[0].message.as_deref(), "pg_cdc::lane"),
                "{sent}"
            );
            let crumbs = &events[0].breadcrumbs.values;
            assert_eq!(crumbs.len(), 4, "{sent}");
            assert!(
                from_callsite(crumbs[0].message.as_deref(), "pg_cdc::sink::sns"),
                "{sent}"
            );
            assert_eq!(crumbs[0].data.get("entries"), Some(&serde_json::json!(2)));
            for crumb in &crumbs[1..] {
                assert!(
                    from_callsite(crumb.message.as_deref(), "pg_cdc::lane"),
                    "{sent}"
                );
            }
        }

        #[test]
        fn reinstalling_keeps_one_panic_hook_and_one_event_per_panic() {
            let first = TestTransport::new();
            let (first_reporting, _layers) =
                client::install_with(&plan(), &identity(), Arc::new(Arc::clone(&first)));
            let second = TestTransport::new();
            let (second_reporting, _layers) =
                client::install_with(&plan(), &identity(), Arc::new(Arc::clone(&second)));
            std::thread::spawn(|| std::panic::catch_unwind(|| panic!("once")).is_err())
                .join()
                .unwrap();
            assert_eq!(second.fetch_and_clear_events().len(), 1);
            assert_eq!(first.fetch_and_clear_events().len(), 0);
            assert!(second_reporting.shutdown(Duration::from_secs(1)));
            assert!(first_reporting.shutdown(Duration::from_secs(1)));
        }

        #[test]
        fn a_panic_is_captured_by_location_without_its_payload() {
            let transport = TestTransport::new();
            let (reporting, _layers) =
                client::install_with(&plan(), &identity(), Arc::new(Arc::clone(&transport)));
            let line = line!() + 1;
            let panicked = std::panic::catch_unwind(|| panic!("row value {}", "customer-secret"));
            assert!(panicked.is_err());
            assert!(reporting.shutdown(Duration::from_secs(2)));
            let events = transport.fetch_and_clear_events();
            assert_eq!(events.len(), 1, "{events:?}");
            let sent = fields(&events[0]);
            assert!(!sent.contains("customer-secret"), "{sent}");
            assert_eq!(events[0].level, Level::Fatal);
            assert_eq!(events[0].message.as_deref(), Some("pg-cdc panicked"));
            let exception = &events[0].exception.values[0];
            assert_eq!(exception.ty, "panic");
            assert_eq!(
                exception.value.as_deref(),
                Some(format!("at {}:{line}", file!()).as_str())
            );
            assert_eq!(
                events[0].tags.get("pipeline").map(String::as_str),
                Some("orders")
            );
        }

        async fn blackholed() -> (ErrorReporting, tokio::task::JoinHandle<()>) {
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
            (reporting, server)
        }

        async fn within<T: Send + 'static>(
            bound: Duration,
            work: impl FnOnce() -> T + Send + 'static,
        ) -> T {
            let started = std::time::Instant::now();
            let finished = tokio::task::spawn_blocking(work);
            let output = tokio::time::timeout(Duration::from_secs(10), finished)
                .await
                .expect("the call must stay within its bound")
                .unwrap();
            assert!(started.elapsed() < bound, "{:?}", started.elapsed());
            output
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn shutdown_against_a_blackholed_sentry_gives_up_at_its_deadline() {
            let (reporting, server) = blackholed().await;
            let flushed = within(Duration::from_millis(500), move || {
                reporting.shutdown(Duration::from_millis(300))
            })
            .await;
            assert!(!flushed);
            server.abort();
        }

        #[derive(Debug)]
        struct Stalled;

        impl sentry::Transport for Stalled {
            fn send_envelope(&self, _envelope: sentry::Envelope) {}

            fn flush(&self, _timeout: Duration) -> bool {
                std::thread::sleep(Duration::from_secs(10));
                false
            }
        }

        fn stalled() -> ErrorReporting {
            let (reporting, _layers) =
                client::install_with(&plan(), &identity(), Arc::new(Arc::new(Stalled)));
            reporting
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn dropping_the_reporter_with_a_stalled_transport_is_bounded() {
            let reporting = stalled();
            within(
                REPORTING_FLUSH_TIMEOUT + Duration::from_millis(500),
                move || {
                    drop(reporting);
                },
            )
            .await;
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_close_whose_thread_cannot_start_abandons_the_client_at_once() {
            let mut reporting = stalled();
            let guard = reporting.guard.take().unwrap();
            let closed = within(Duration::from_millis(500), move || {
                client::close_within_using(guard, Duration::from_millis(100), |_closer| {
                    Err(std::io::Error::other("no more threads"))
                })
            })
            .await;
            assert!(!closed);
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_panic_with_a_stalled_transport_returns_at_once() {
            let reporting = stalled();
            let panicked = within(Duration::from_secs(3), || {
                std::panic::catch_unwind(|| panic!("the lane invariant broke")).is_err()
            })
            .await;
            assert!(panicked);
            assert!(!reporting.shutdown(Duration::from_millis(10)));
        }

        #[test]
        fn text_scrubbing_survives_multibyte_delimiters() {
            for text in [
                "é=x",
                "é://x",
                "ü@é://ö@ä/ß",
                "pässword=é",
                "=é",
                "é=",
                "x://é",
                "a=b é=c",
            ] {
                let _ = client::scrub_text(text);
            }
            assert_eq!(
                client::scrub_text("pässword=é token=ü"),
                "pässword=é token=[redacted]"
            );
        }

        fn scrubbable() -> impl proptest::strategy::Strategy<Value = String> {
            use proptest::prelude::*;
            proptest::collection::vec(
                prop_oneof![
                    any::<char>(),
                    Just('='),
                    Just('@'),
                    Just(':'),
                    Just('/'),
                    Just('é'),
                    Just(' '),
                ],
                0..64,
            )
            .prop_map(|chars| chars.into_iter().collect())
        }

        proptest::proptest! {
            #[test]
            fn text_scrubbing_never_panics_on_any_text(text in scrubbable()) {
                let _ = client::scrub_text(&text);
            }
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
