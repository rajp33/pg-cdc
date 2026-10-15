pub mod discard;
pub mod sns;
pub mod stdout;

use std::fmt;

use async_trait::async_trait;
use bytes::Bytes;
use serde_json::Value;

use crate::event::{Envelope, EventId};
use crate::order::{OrderKeyId, column_value};

pub const SNS_MAX_NUMBER_DIGITS: usize = 38;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SinkCapabilities {
    pub max_batch_messages: usize,
    pub max_batch_bytes: usize,
    pub max_message_bytes: usize,
}

pub const UNKNOWN_LABEL: &str = "unknown";

pub fn metric_labels(attributes: &[(String, AttributeValue)]) -> (&str, &str) {
    let find = |name: &str| {
        attributes
            .iter()
            .find(|(key, _)| key == name)
            .map_or(UNKNOWN_LABEL, |(_, value)| value.as_str())
    };
    (find("table"), find("op"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttributeValue {
    String(String),
    Number(String),
}

impl AttributeValue {
    pub fn data_type(&self) -> &'static str {
        match self {
            Self::String(_) => "String",
            Self::Number(_) => "Number",
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::String(value) | Self::Number(value) => value,
        }
    }
}

pub type Attributes = Vec<(String, AttributeValue)>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outbound {
    pub id: EventId,
    pub order_key: OrderKeyId,
    pub attributes: Attributes,
    pub body: Bytes,
}

impl Outbound {
    pub fn encode(
        envelope: &Envelope,
        order_key: OrderKeyId,
        attribute_columns: &[String],
    ) -> Result<Self, serde_json::Error> {
        let core = [
            ("schema", envelope.schema.as_str()),
            ("table", envelope.table.as_str()),
            ("op", envelope.op.as_str()),
        ]
        .map(|(name, value)| (name.to_owned(), AttributeValue::String(value.to_owned())));
        let columns = attribute_columns.iter().filter_map(|column| {
            let value = column_value(envelope, column).and_then(attribute_value)?;
            Some((column.clone(), value))
        });
        let mut attributes = Vec::with_capacity(3 + attribute_columns.len());
        for (name, value) in core.into_iter().chain(columns) {
            if sns_text(value.as_str()) {
                attributes.push((name, value));
            } else {
                tracing::warn!(
                    id = %envelope.id,
                    attribute = %name,
                    "omitting a message attribute whose value has characters SNS rejects"
                );
            }
        }
        Ok(Self {
            id: envelope.id,
            order_key,
            attributes,
            body: Bytes::from(envelope.encode()?),
        })
    }

    pub fn encoded_len(&self) -> usize {
        self.attributes
            .iter()
            .fold(self.body.len(), |total, (name, value)| {
                total
                    .saturating_add(name.len())
                    .saturating_add(value.data_type().len())
                    .saturating_add(value.as_str().len())
            })
    }

    pub fn attribute(&self, name: &str) -> Option<&AttributeValue> {
        self.attributes
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value)
    }
}

fn sns_text(text: &str) -> bool {
    text.chars().all(|character| {
        matches!(
            character,
            '\t' | '\n' | '\r' | ' '..='\u{D7FF}' | '\u{E000}'..='\u{FFFD}' | '\u{10000}'..
        )
    })
}

fn attribute_value(value: &Value) -> Option<AttributeValue> {
    match value {
        Value::Null => None,
        Value::String(text) if text.is_empty() => None,
        Value::String(text) => Some(AttributeValue::String(text.clone())),
        Value::Number(number) => {
            let text = number.to_string();
            let digits = text.bytes().filter(u8::is_ascii_digit).count();
            if digits <= SNS_MAX_NUMBER_DIGITS && !text.contains(['e', 'E']) {
                Some(AttributeValue::Number(text))
            } else {
                Some(AttributeValue::String(text))
            }
        }
        Value::Bool(_) | Value::Array(_) | Value::Object(_) => {
            Some(AttributeValue::String(value.to_string()))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct SinkError(pub String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Delivered,
    Retryable(SinkError),
    Terminal(SinkError),
}

#[async_trait]
pub trait Sink: Send + Sync + fmt::Debug {
    fn capabilities(&self) -> SinkCapabilities;
    fn name(&self) -> &'static str {
        "custom"
    }
    async fn preflight(&self) -> Result<(), SinkError>;
    async fn send(&self, batch: &[Outbound]) -> Vec<Outcome>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Op, sample_envelope};
    use crate::order::{OrderBy, order_key};
    use serde_json::json;

    fn columns(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_owned()).collect()
    }

    fn string(value: &str) -> AttributeValue {
        AttributeValue::String(value.to_owned())
    }

    fn number(value: &str) -> AttributeValue {
        AttributeValue::Number(value.to_owned())
    }

    #[test]
    fn encode_carries_the_id_order_key_body_and_core_attributes() {
        let envelope = sample_envelope(
            Op::Update,
            Some(json!({"id": 42})),
            None,
            Some(json!({"id": 42, "status": "paid"})),
        );
        let key = order_key(&OrderBy::Key, &envelope);
        let outbound = Outbound::encode(&envelope, key, &[]).unwrap();
        assert_eq!(outbound.id, envelope.id);
        assert_eq!(outbound.order_key, key);
        assert_eq!(
            outbound.body.as_ref(),
            envelope.encode().unwrap().as_slice()
        );
        assert_eq!(
            outbound.attributes,
            vec![
                ("schema".to_owned(), string("public")),
                ("table".to_owned(), string("orders")),
                ("op".to_owned(), string("update")),
            ]
        );
    }

    #[test]
    fn attribute_columns_are_typed_for_sns_and_omitted_when_null_empty_or_absent() {
        let envelope = sample_envelope(
            Op::Insert,
            None,
            None,
            Some(json!({
                "tenant": "acme",
                "count": 7,
                "ratio": 1.5,
                "flag": true,
                "tags": ["a", "b"],
                "doc": {"k": 1},
                "nothing": null,
                "blank": "",
                "huge": serde_json::Number::from_string_unchecked("1".repeat(39)),
                "wide": serde_json::Number::from_string_unchecked("1".repeat(38)),
                "vast": serde_json::Number::from_string_unchecked("1e+300".to_owned()),
                "tiny": serde_json::Number::from_string_unchecked("1.5e-7".to_owned()),
                "large": serde_json::Number::from_string_unchecked("1e+15".to_owned()),
            })),
        );
        let names = columns(&[
            "tenant", "count", "ratio", "flag", "tags", "doc", "nothing", "blank", "absent",
            "huge", "wide", "vast", "tiny", "large",
        ]);
        let outbound = Outbound::encode(&envelope, OrderKeyId::of(b"[]"), &names).unwrap();
        let extra: Vec<(&str, &AttributeValue)> = outbound
            .attributes
            .iter()
            .skip(3)
            .map(|(name, value)| (name.as_str(), value))
            .collect();
        assert_eq!(
            extra,
            vec![
                ("tenant", &string("acme")),
                ("count", &number("7")),
                ("ratio", &number("1.5")),
                ("flag", &string("true")),
                ("tags", &string(r#"["a","b"]"#)),
                ("doc", &string(r#"{"k":1}"#)),
                ("huge", &string(&"1".repeat(39))),
                ("wide", &number(&"1".repeat(38))),
                ("vast", &string("1e+300")),
                ("tiny", &string("1.5e-7")),
                ("large", &string("1e+15")),
            ]
        );
        assert_eq!(outbound.attribute("tenant"), Some(&string("acme")));
        assert_eq!(outbound.attribute("nothing"), None);
    }

    #[test]
    fn numbers_in_exponent_form_are_strings_whatever_the_exponent_case() {
        for text in ["2.5E-200", "1E+15", "-3e7", "0e0"] {
            let value = Value::Number(serde_json::Number::from_string_unchecked(text.to_owned()));
            assert_eq!(attribute_value(&value), Some(string(text)), "{text}");
        }
        for text in ["-12.5", "0.000001", "100000000000000000000"] {
            let value = Value::Number(serde_json::Number::from_string_unchecked(text.to_owned()));
            assert_eq!(attribute_value(&value), Some(number(text)), "{text}");
        }
    }

    #[test]
    fn attribute_columns_come_from_before_only_when_there_is_no_after() {
        let delete = sample_envelope(
            Op::Delete,
            Some(json!({"id": 1})),
            Some(json!({"id": 1, "tenant": "acme"})),
            None,
        );
        let outbound =
            Outbound::encode(&delete, OrderKeyId::of(b"[]"), &columns(&["tenant"])).unwrap();
        assert_eq!(outbound.attribute("tenant"), Some(&string("acme")));
        assert_eq!(outbound.attribute("op"), Some(&string("delete")));

        let toast = sample_envelope(
            Op::Update,
            Some(json!({"id": 1})),
            Some(json!({"id": 1, "tenant": "acme"})),
            Some(json!({"id": 1})),
        );
        let outbound =
            Outbound::encode(&toast, OrderKeyId::of(b"[]"), &columns(&["tenant"])).unwrap();
        assert_eq!(outbound.attribute("tenant"), None);
    }

    #[test]
    fn encoded_len_counts_the_body_and_each_attribute_name_type_and_value() {
        let outbound = Outbound {
            id: EventId::new(pg_cdc_decoder::Lsn(1), 0),
            order_key: OrderKeyId::of(b"[]"),
            attributes: vec![
                ("op".to_owned(), string("insert")),
                ("n".to_owned(), number("42")),
            ],
            body: Bytes::from_static(b"0123456789"),
        };
        assert_eq!(outbound.encoded_len(), 10 + (2 + 6 + 6) + (1 + 6 + 2));
    }

    #[test]
    fn sns_text_is_the_xml_character_set() {
        for good in [
            "",
            "plain",
            "tab\tnewline\ncarriage\r",
            "\u{20}\u{D7FF}\u{E000}\u{FFFD}\u{10000}\u{10FFFF}",
            "café 😀",
        ] {
            assert!(sns_text(good), "{good:?}");
        }
        for bad in [
            "\u{0}",
            "bell\u{7}",
            "\u{1F}",
            "\u{B}",
            "\u{FFFE}",
            "x\u{FFFF}",
        ] {
            assert!(!sns_text(bad), "{bad:?}");
        }
    }

    #[test]
    fn attribute_values_with_characters_sns_rejects_are_omitted() {
        let mut envelope = sample_envelope(
            Op::Insert,
            None,
            None,
            Some(json!({
                "tenant": "acme",
                "bell": "ring\u{7}",
                "nul": "a\u{0}b",
                "nonchar": "x\u{FFFE}",
                "tabbed": "a\tb",
                "doc": {"k": "\u{1}"},
            })),
        );
        envelope.table = "bad\u{1}table".to_owned();
        let names = columns(&["tenant", "bell", "nul", "nonchar", "tabbed", "doc"]);
        let outbound = Outbound::encode(&envelope, OrderKeyId::of(b"[]"), &names).unwrap();
        let kept: Vec<&str> = outbound
            .attributes
            .iter()
            .map(|(name, _)| name.as_str())
            .collect();
        assert_eq!(kept, vec!["schema", "op", "tenant", "tabbed", "doc"]);
        let escaped = json!({"k": "\u{1}"}).to_string();
        assert_eq!(outbound.attribute("doc"), Some(&string(&escaped)));
    }

    #[tokio::test]
    async fn built_in_sinks_name_themselves() {
        let client = aws_sdk_sns::Client::from_conf(
            aws_sdk_sns::Config::builder()
                .behavior_version(aws_sdk_sns::config::BehaviorVersion::latest())
                .region(aws_sdk_sns::config::Region::new("us-east-1"))
                .build(),
        );
        let sns = sns::SnsSink::new(client, "arn:aws:sns:us-east-1:1:orders".to_owned(), None);
        assert_eq!(sns.name(), "sns");
        assert_eq!(discard::DiscardSink::new().name(), "discard");
        assert_eq!(stdout::StdoutSink::new(tokio::io::sink()).name(), "stdout");
        let gate = crate::leader::gate::WriteGate::new();
        let gated = crate::leader::gate::GatedSink::new(
            std::sync::Arc::new(discard::DiscardSink::new()),
            gate,
        );
        assert_eq!(gated.name(), "discard");
    }

    #[test]
    fn sink_errors_display_their_message() {
        assert_eq!(SinkError("throttled".to_owned()).to_string(), "throttled");
    }
}
