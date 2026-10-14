pub mod discard;
mod legacy;
pub mod sns;
pub mod stdout;

pub use legacy::{Delivered, StdoutSink};

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
        let mut attributes = vec![
            (
                "schema".to_owned(),
                AttributeValue::String(envelope.schema.clone()),
            ),
            (
                "table".to_owned(),
                AttributeValue::String(envelope.table.clone()),
            ),
            (
                "op".to_owned(),
                AttributeValue::String(envelope.op.as_str().to_owned()),
            ),
        ];
        for column in attribute_columns {
            if let Some(value) = column_value(envelope, column).and_then(attribute_value) {
                attributes.push((column.clone(), value));
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

fn attribute_value(value: &Value) -> Option<AttributeValue> {
    match value {
        Value::Null => None,
        Value::String(text) if text.is_empty() => None,
        Value::String(text) => Some(AttributeValue::String(text.clone())),
        Value::Number(number) => {
            let text = number.to_string();
            let digits = text.bytes().filter(u8::is_ascii_digit).count();
            if digits <= SNS_MAX_NUMBER_DIGITS {
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
            })),
        );
        let names = columns(&[
            "tenant", "count", "ratio", "flag", "tags", "doc", "nothing", "blank", "absent",
            "huge", "wide",
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
            ]
        );
        assert_eq!(outbound.attribute("tenant"), Some(&string("acme")));
        assert_eq!(outbound.attribute("nothing"), None);
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
    fn sink_errors_display_their_message() {
        assert_eq!(SinkError("throttled".to_owned()).to_string(), "throttled");
    }
}
