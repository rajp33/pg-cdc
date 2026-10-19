use serde::Serialize;
use serde_json::{Map, Value};

use super::EventId;

pub const ENVELOPE_VERSION: u8 = 1;

pub type Row = Map<String, Value>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Op {
    Insert,
    Update,
    Delete,
    Truncate,
}

impl Op {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
            Self::Truncate => "truncate",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Source {
    pub pipeline: String,
    pub commit_lsn: String,
    pub commit_ts: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Envelope {
    pub id: EventId,
    pub version: u8,
    pub op: Op,
    pub schema: String,
    pub table: String,
    pub key: Option<Row>,
    pub before: Option<Row>,
    pub after: Option<Row>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub unchanged_toast: Vec<String>,
    pub source: Source,
}

impl Envelope {
    pub fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }
}

#[cfg(test)]
pub(crate) fn sample_envelope(
    op: Op,
    key: Option<Value>,
    before: Option<Value>,
    after: Option<Value>,
) -> Envelope {
    let row = |value: Option<Value>| value.and_then(|value| value.as_object().cloned());
    Envelope {
        id: EventId::new(pg_cdc_decoder::Lsn(0x1A2B_3C4D), 0),
        version: ENVELOPE_VERSION,
        op,
        schema: "public".to_owned(),
        table: "orders".to_owned(),
        key: row(key),
        before: row(before),
        after: row(after),
        unchanged_toast: Vec::new(),
        source: Source {
            pipeline: "orders".to_owned(),
            commit_lsn: "0/1A2B3C4D".to_owned(),
            commit_ts: "2026-10-14T12:00:00.123456+00:00".to_owned(),
        },
    }
}
