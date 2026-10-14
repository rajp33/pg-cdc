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
