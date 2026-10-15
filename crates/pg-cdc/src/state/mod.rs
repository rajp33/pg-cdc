pub mod filesystem;
pub mod memory;
pub mod s3;

use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use pg_cdc_decoder::Lsn;
use pg_cdc_decoder::lsn::ParseLsnError;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::event::EventId;
use crate::order::OrderKeyId;
use crate::sink::{AttributeValue, Attributes, Outbound};

pub const PARTITIONS: usize = 256;
pub const LEADER_OBJECT: &str = "leader.json";
pub const PARKED_DIR: &str = "parked";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ParkStatus {
    Retrying,
    Dead,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParkedEvent {
    pub order_key: OrderKeyId,
    pub id: EventId,
    pub status: ParkStatus,
    pub attempts: u32,
    pub first_failure_ms: u64,
    pub last_error: String,
    pub attributes: Attributes,
    pub body: Bytes,
}

impl ParkedEvent {
    pub fn outbound(&self) -> Outbound {
        Outbound {
            id: self.id,
            order_key: self.order_key,
            attributes: self.attributes.clone(),
            body: self.body.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParkedHead {
    pub order_key: OrderKeyId,
    pub head: EventId,
    pub length: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LeaderState {
    Active,
    Released,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(into = "StoredLeader", try_from = "StoredLeader")]
pub struct LeaderRecord {
    pub epoch: u64,
    pub holder: String,
    pub state: LeaderState,
    pub confirmed_lsn: Lsn,
    pub updated_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Version(pub String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Versioned<T> {
    pub value: T,
    pub version: Version,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SwapOutcome {
    Swapped(Version),
    Conflict,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StoreError {
    #[error("state store unavailable: {0}")]
    Unavailable(String),
    #[error("{0} is not in the store")]
    NotFound(String),
    #[error("stored object {key} is malformed: {reason}")]
    Malformed { key: String, reason: String },
}

#[async_trait]
pub trait StateStore: Send + Sync + fmt::Debug {
    async fn append_parked(&self, events: &[ParkedEvent]) -> Result<(), StoreError>;
    async fn load_parked_heads(&self, partition: u8) -> Result<Vec<ParkedHead>, StoreError>;
    async fn list_parked_queues(
        &self,
        after: Option<&OrderKeyId>,
        limit: usize,
        length_cap: usize,
    ) -> Result<Vec<ParkedHead>, StoreError>;
    async fn read_parked(
        &self,
        key: &OrderKeyId,
        after: Option<&EventId>,
        limit: usize,
    ) -> Result<Vec<ParkedEvent>, StoreError>;
    async fn set_parked_status(
        &self,
        key: &OrderKeyId,
        id: &EventId,
        status: ParkStatus,
    ) -> Result<(), StoreError>;
    async fn remove_parked(&self, events: &[(OrderKeyId, EventId)]) -> Result<(), StoreError>;
    async fn read_leader(&self) -> Result<Option<Versioned<LeaderRecord>>, StoreError>;
    async fn swap_leader(
        &self,
        next: &LeaderRecord,
        expected: Option<&Version>,
    ) -> Result<SwapOutcome, StoreError>;
}

pub fn store_root(prefix: &str, pipeline: &str) -> String {
    let prefix = prefix.trim_matches('/');
    if prefix.is_empty() {
        pipeline.to_owned()
    } else {
        format!("{prefix}/{pipeline}")
    }
}

pub fn parked_name(key: &OrderKeyId, id: &EventId) -> String {
    format!("{key}/{id}.json")
}

pub fn parse_parked_name(name: &str) -> Option<(OrderKeyId, EventId)> {
    let (key, file) = name.split_once('/')?;
    let id = file.strip_suffix(".json")?;
    Some((key.parse().ok()?, id.parse().ok()?))
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredAttribute {
    name: String,
    #[serde(rename = "type")]
    data_type: String,
    value: String,
}

#[derive(Serialize)]
struct StoredParkedRef<'a> {
    order_key: String,
    id: String,
    status: ParkStatus,
    attempts: u32,
    first_failure_ms: u64,
    last_error: &'a str,
    attributes: Vec<StoredAttribute>,
    envelope: &'a RawValue,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredParked {
    order_key: String,
    id: String,
    status: ParkStatus,
    attempts: u32,
    first_failure_ms: u64,
    last_error: String,
    attributes: Vec<StoredAttribute>,
    envelope: Box<RawValue>,
}

pub fn encode_parked(event: &ParkedEvent) -> Result<Vec<u8>, StoreError> {
    let malformed = |reason: String| StoreError::Malformed {
        key: parked_name(&event.order_key, &event.id),
        reason,
    };
    let envelope: &RawValue = serde_json::from_slice(&event.body)
        .map_err(|error| malformed(format!("the envelope is not JSON: {error}")))?;
    let stored = StoredParkedRef {
        order_key: event.order_key.to_string(),
        id: event.id.to_string(),
        status: event.status,
        attempts: event.attempts,
        first_failure_ms: event.first_failure_ms,
        last_error: &event.last_error,
        attributes: event
            .attributes
            .iter()
            .map(|(name, value)| StoredAttribute {
                name: name.clone(),
                data_type: value.data_type().to_owned(),
                value: value.as_str().to_owned(),
            })
            .collect(),
        envelope,
    };
    serde_json::to_vec(&stored).map_err(|error| malformed(error.to_string()))
}

pub fn decode_parked(name: &str, bytes: &[u8]) -> Result<ParkedEvent, StoreError> {
    let malformed = |reason: String| StoreError::Malformed {
        key: name.to_owned(),
        reason,
    };
    let stored: StoredParked =
        serde_json::from_slice(bytes).map_err(|error| malformed(error.to_string()))?;
    let mut attributes = Vec::with_capacity(stored.attributes.len());
    for attribute in stored.attributes {
        let value = match attribute.data_type.as_str() {
            "String" => AttributeValue::String(attribute.value),
            "Number" => AttributeValue::Number(attribute.value),
            other => return Err(malformed(format!("unknown attribute type {other:?}"))),
        };
        attributes.push((attribute.name, value));
    }
    Ok(ParkedEvent {
        order_key: stored
            .order_key
            .parse()
            .map_err(|error| malformed(format!("{error}")))?,
        id: stored
            .id
            .parse()
            .map_err(|error| malformed(format!("{error}")))?,
        status: stored.status,
        attempts: stored.attempts,
        first_failure_ms: stored.first_failure_ms,
        last_error: stored.last_error,
        attributes,
        body: Bytes::from(stored.envelope.get().to_owned()),
    })
}

pub fn decode_leader(name: &str, bytes: &[u8]) -> Result<LeaderRecord, StoreError> {
    serde_json::from_slice(bytes).map_err(|error| StoreError::Malformed {
        key: name.to_owned(),
        reason: error.to_string(),
    })
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredLeader {
    epoch: u64,
    holder: String,
    state: LeaderState,
    confirmed_lsn: String,
    updated_ms: u64,
}

impl From<LeaderRecord> for StoredLeader {
    fn from(record: LeaderRecord) -> Self {
        Self {
            epoch: record.epoch,
            holder: record.holder,
            state: record.state,
            confirmed_lsn: record.confirmed_lsn.to_string(),
            updated_ms: record.updated_ms,
        }
    }
}

impl TryFrom<StoredLeader> for LeaderRecord {
    type Error = ParseLsnError;

    fn try_from(stored: StoredLeader) -> Result<Self, Self::Error> {
        Ok(Self {
            epoch: stored.epoch,
            holder: stored.holder,
            state: stored.state,
            confirmed_lsn: stored.confirmed_lsn.parse()?,
            updated_ms: stored.updated_ms,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event() -> ParkedEvent {
        ParkedEvent {
            order_key: "ab000000000000000000000000000001".parse().unwrap(),
            id: EventId::new(Lsn(0x1A2B_3C4D), 3),
            status: ParkStatus::Dead,
            attempts: 7,
            first_failure_ms: 1_760_443_200_123,
            last_error: "InvalidParameter: \"bad\"".to_owned(),
            attributes: vec![
                (
                    "table".to_owned(),
                    AttributeValue::String("orders".to_owned()),
                ),
                (
                    "amount".to_owned(),
                    AttributeValue::Number("12.50".to_owned()),
                ),
            ],
            body: Bytes::from_static(
                r#"{"id":"000000001a2b3c4d-00000003","after":{"n":12345678901234567890.10,"t":"café \"x\""}}"#
                    .as_bytes(),
            ),
        }
    }

    #[test]
    fn parked_events_round_trip_through_the_codec_byte_for_byte() {
        let event = event();
        let encoded = encode_parked(&event).unwrap();
        let text = String::from_utf8(encoded.clone()).unwrap();
        assert!(
            text.starts_with(r#"{"order_key":"ab000000000000000000000000000001","id":"000000001a2b3c4d-00000003","status":"dead","attempts":7,"#),
            "{text}"
        );
        assert!(
            text.ends_with(r#""envelope":{"id":"000000001a2b3c4d-00000003","after":{"n":12345678901234567890.10,"t":"café \"x\""}}}"#),
            "{text}"
        );
        assert_eq!(decode_parked("x", &encoded).unwrap(), event);
    }

    #[test]
    fn a_body_that_is_not_json_cannot_be_encoded() {
        let mut event = event();
        event.body = Bytes::from_static(b"not json");
        assert!(matches!(
            encode_parked(&event),
            Err(StoreError::Malformed { key, .. }) if key == "ab000000000000000000000000000001/000000001a2b3c4d-00000003.json"
        ));
    }

    #[test]
    fn malformed_objects_are_reported_with_their_name() {
        for bytes in [
            &b"{"[..],
            br#"{"order_key":"zz","id":"000000001a2b3c4d-00000003","status":"dead","attempts":1,"first_failure_ms":1,"last_error":"","attributes":[],"envelope":{}}"#,
            br#"{"order_key":"ab000000000000000000000000000001","id":"000000001a2b3c4d-00000003","status":"dead","attempts":1,"first_failure_ms":1,"last_error":"","attributes":[{"name":"a","type":"Binary","value":""}],"envelope":{}}"#,
        ] {
            assert!(
                matches!(decode_parked("p/q.json", bytes), Err(StoreError::Malformed { key, .. }) if key == "p/q.json"),
                "{}",
                String::from_utf8_lossy(bytes)
            );
        }
    }

    #[test]
    fn leader_records_keep_the_lsn_in_postgres_notation() {
        let record = LeaderRecord {
            epoch: 4,
            holder: "task-a".to_owned(),
            state: LeaderState::Released,
            confirmed_lsn: Lsn(0x1_0000_00FF),
            updated_ms: 9,
        };
        let text = serde_json::to_string(&record).unwrap();
        assert_eq!(
            text,
            r#"{"epoch":4,"holder":"task-a","state":"released","confirmed_lsn":"1/FF","updated_ms":9}"#
        );
        assert_eq!(decode_leader("leader.json", text.as_bytes()), Ok(record));
        assert!(matches!(
            decode_leader(
                "leader.json",
                br#"{"epoch":4,"holder":"a","state":"active","confirmed_lsn":"x","updated_ms":9}"#
            ),
            Err(StoreError::Malformed { .. })
        ));
    }

    #[test]
    fn object_names_follow_the_spec_layout() {
        assert_eq!(store_root("orders/", "orders"), "orders/orders");
        assert_eq!(store_root("/a/b/", "p"), "a/b/p");
        assert_eq!(store_root("", "p"), "p");
        let event = event();
        let name = parked_name(&event.order_key, &event.id);
        assert_eq!(
            name,
            "ab000000000000000000000000000001/000000001a2b3c4d-00000003.json"
        );
        assert_eq!(parse_parked_name(&name), Some((event.order_key, event.id)));
        for bad in [
            "ab000000000000000000000000000001/000000001a2b3c4d-00000003",
            "ab000000000000000000000000000001/.tmp-1",
            "ab00000000000000000000000000001/000000001a2b3c4d-00000003.json",
            "ab000000000000000000000000000001",
        ] {
            assert_eq!(parse_parked_name(bad), None, "{bad}");
        }
    }

    #[test]
    fn a_parked_event_rebuilds_its_outbound_message() {
        let event = event();
        let outbound = event.outbound();
        assert_eq!(outbound.id, event.id);
        assert_eq!(outbound.order_key, event.order_key);
        assert_eq!(outbound.attributes, event.attributes);
        assert_eq!(outbound.body, event.body);
    }
}
