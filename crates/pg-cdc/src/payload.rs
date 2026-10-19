use std::fmt;

use async_trait::async_trait;
use aws_sdk_s3::Client;
use aws_sdk_s3::error::DisplayErrorContext;
use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use serde::de::{Deserializer, IgnoredAny, MapAccess, Visitor};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::value::RawValue;

use crate::event::EventId;
use crate::state::{StoreError, store_root};

pub const PAYLOADS_DIR: &str = "payloads";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PayloadRef {
    pub uri: String,
    pub bytes: u64,
}

#[async_trait]
pub trait PayloadStore: Send + Sync + fmt::Debug {
    async fn put(&self, id: &EventId, body: &[u8]) -> Result<PayloadRef, StoreError>;
}

#[derive(Debug, Clone)]
pub struct S3PayloadStore {
    client: Client,
    bucket: String,
    root: String,
}

impl S3PayloadStore {
    pub fn new(client: Client, bucket: String, prefix: &str, pipeline: &str) -> Self {
        Self {
            client,
            bucket,
            root: store_root(prefix, pipeline),
        }
    }

    fn object(&self, id: &EventId) -> String {
        format!("{}/{PAYLOADS_DIR}/{id}.json", self.root)
    }
}

#[async_trait]
impl PayloadStore for S3PayloadStore {
    async fn put(&self, id: &EventId, body: &[u8]) -> Result<PayloadRef, StoreError> {
        let object = self.object(id);
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&object)
            .content_type("application/json")
            .body(ByteStream::from(body.to_vec()))
            .send()
            .await
            .map_err(|error| StoreError::Unavailable(DisplayErrorContext(&error).to_string()))?;
        Ok(PayloadRef {
            uri: format!("s3://{}/{object}", self.bucket),
            bytes: u64::try_from(body.len()).unwrap_or(u64::MAX),
        })
    }
}

pub fn pointer_body(body: &[u8], payload: &PayloadRef) -> Result<Bytes, serde_json::Error> {
    let envelope: Fields = serde_json::from_slice(body)?;
    let reference = serde_json::value::to_raw_value(payload)?;
    let mut fields = Vec::with_capacity(envelope.0.len() + 1);
    let mut reference = Some(reference);
    for (name, value) in envelope.0 {
        if name == "source"
            && let Some(reference) = reference.take()
        {
            fields.push(("payload_ref".to_owned(), reference));
        }
        fields.push((name, value));
    }
    if let Some(reference) = reference {
        fields.push(("payload_ref".to_owned(), reference));
    }
    serde_json::to_vec(&Pointer(fields)).map(Bytes::from)
}

struct Fields(Vec<(String, Box<RawValue>)>);

impl<'de> Deserialize<'de> for Fields {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_map(FieldsVisitor)
    }
}

struct FieldsVisitor;

impl<'de> Visitor<'de> for FieldsVisitor {
    type Value = Fields;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Fields, A::Error> {
        let mut fields = Vec::new();
        while let Some(name) = map.next_key::<String>()? {
            if matches!(name.as_str(), "before" | "after" | "unchanged_toast") {
                map.next_value::<IgnoredAny>()?;
            } else {
                fields.push((name, map.next_value::<Box<RawValue>>()?));
            }
        }
        Ok(Fields(fields))
    }
}

struct Pointer(Vec<(String, Box<RawValue>)>);

impl Serialize for Pointer {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_map(self.0.iter().map(|(name, value)| (name, value)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Op, sample_envelope};
    use serde_json::json;

    fn reference() -> PayloadRef {
        PayloadRef {
            uri: "s3://pgcdc-payloads/orders/orders/payloads/000000001a2b3c4d-00000000.json"
                .to_owned(),
            bytes: 912_345,
        }
    }

    #[test]
    fn a_pointer_is_the_envelope_without_row_images_plus_the_payload_ref() {
        let mut envelope = sample_envelope(
            Op::Update,
            Some(json!({"id": 42})),
            Some(json!({"id": 42, "status": "pending"})),
            Some(json!({"id": 42, "status": "paid"})),
        );
        envelope.unchanged_toast = vec!["doc".to_owned()];
        let body = envelope.encode().unwrap();
        let pointer = pointer_body(&body, &reference()).unwrap();
        assert_eq!(
            std::str::from_utf8(&pointer).unwrap(),
            concat!(
                r#"{"id":"000000001a2b3c4d-00000000","version":1,"op":"update","schema":"public","table":"orders","key":{"id":42},"#,
                r#""payload_ref":{"uri":"s3://pgcdc-payloads/orders/orders/payloads/000000001a2b3c4d-00000000.json","bytes":912345},"#,
                r#""source":{"pipeline":"orders","commit_lsn":"0/1A2B3C4D","commit_ts":"2026-10-14T12:00:00.123456+00:00"}}"#
            )
        );
    }

    #[test]
    fn a_pointer_keeps_exact_key_values() {
        let body = br#"{"id":"x","key":{"id":123456789012345678901234567890.000},"after":{"a":1},"source":{}}"#;
        let pointer = pointer_body(body, &reference()).unwrap();
        assert!(
            std::str::from_utf8(&pointer).unwrap().starts_with(
                r#"{"id":"x","key":{"id":123456789012345678901234567890.000},"payload_ref":"#
            ),
            "{pointer:?}"
        );
    }

    #[test]
    fn a_pointer_survives_json_columns_nested_up_to_the_decoder_limit() {
        for depth in [125, 126, 127] {
            let nested = format!("{}{}", "[".repeat(depth), "]".repeat(depth));
            let body = format!(
                r#"{{"id":"x","before":{{"doc":{nested}}},"after":{{"doc":{nested}}},"unchanged_toast":["doc"],"source":{{}}}}"#
            );
            assert!(serde_json::from_str::<serde_json::Value>(&nested).is_ok());
            let pointer = pointer_body(body.as_bytes(), &reference()).unwrap();
            assert!(
                std::str::from_utf8(&pointer)
                    .unwrap()
                    .starts_with(r#"{"id":"x","payload_ref":{"#),
                "depth {depth}"
            );
        }
    }

    #[test]
    fn a_body_that_is_not_an_object_has_no_pointer() {
        assert!(pointer_body(b"[1]", &reference()).is_err());
        assert!(pointer_body(b"not json", &reference()).is_err());
    }
}
