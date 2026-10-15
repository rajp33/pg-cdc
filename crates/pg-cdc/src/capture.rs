use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use tracing::field::{Field, Visit};
use tracing::level_filters::LevelFilter;
use tracing::span::{Attributes, Id, Record};
use tracing::subscriber::DefaultGuard;
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SpanSeen {
    pub(crate) name: String,
    pub(crate) level: Level,
    pub(crate) fields: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EventSeen {
    pub(crate) level: Level,
    pub(crate) message: String,
    pub(crate) span: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct Capture {
    spans: Arc<Mutex<Vec<SpanSeen>>>,
    events: Arc<Mutex<Vec<EventSeen>>>,
}

impl Capture {
    pub(crate) fn install(level: LevelFilter) -> (Self, DefaultGuard) {
        let capture = Self::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone().with_filter(level));
        (capture, tracing::subscriber::set_default(subscriber))
    }

    pub(crate) fn spans(&self) -> Vec<SpanSeen> {
        self.spans.lock().unwrap().clone()
    }

    pub(crate) fn events(&self) -> Vec<EventSeen> {
        self.events.lock().unwrap().clone()
    }
}

struct Fields<'a>(&'a mut BTreeMap<String, String>);

impl Visit for Fields<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }
}

impl<S> Layer<S> for Capture
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(&self, attributes: &Attributes<'_>, id: &Id, context: Context<'_, S>) {
        let mut fields = BTreeMap::new();
        attributes.record(&mut Fields(&mut fields));
        let mut spans = self.spans.lock().unwrap();
        if let Some(span) = context.span(id) {
            span.extensions_mut().insert(spans.len());
        }
        spans.push(SpanSeen {
            name: attributes.metadata().name().to_owned(),
            level: *attributes.metadata().level(),
            fields,
        });
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, context: Context<'_, S>) {
        let Some(span) = context.span(id) else {
            return;
        };
        let Some(index) = span.extensions().get::<usize>().copied() else {
            return;
        };
        if let Some(seen) = self.spans.lock().unwrap().get_mut(index) {
            values.record(&mut Fields(&mut seen.fields));
        }
    }

    fn on_event(&self, event: &Event<'_>, context: Context<'_, S>) {
        let mut fields = BTreeMap::new();
        event.record(&mut Fields(&mut fields));
        self.events.lock().unwrap().push(EventSeen {
            level: *event.metadata().level(),
            message: fields.remove("message").unwrap_or_default(),
            span: context.event_span(event).map(|span| span.name().to_owned()),
        });
    }
}
