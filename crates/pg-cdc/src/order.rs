use std::fmt;
use std::str::FromStr;

use serde_json::Value;
use twox_hash::XxHash3_128;

use crate::event::{Envelope, lower_hex};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum OrderBy {
    #[default]
    Key,
    Table,
    Column(String),
    Pipeline,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0:?} is not key, table, pipeline or column:<name>")]
pub struct InvalidOrderBy(pub String);

impl FromStr for OrderBy {
    type Err = InvalidOrderBy;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text {
            "key" => Ok(Self::Key),
            "table" => Ok(Self::Table),
            "pipeline" => Ok(Self::Pipeline),
            _ => match text.strip_prefix("column:") {
                Some(name) if !name.is_empty() => Ok(Self::Column(name.to_owned())),
                _ => Err(InvalidOrderBy(text.to_owned())),
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OrderKeyId(u128);

impl OrderKeyId {
    pub fn of(canonical: &[u8]) -> Self {
        Self(XxHash3_128::oneshot(canonical))
    }

    pub fn lane(self, lanes: usize) -> usize {
        let lanes = u128::try_from(lanes.max(1)).unwrap_or(u128::MAX);
        usize::try_from(self.0 % lanes).unwrap_or(0)
    }

    pub fn partition(self) -> u8 {
        u8::try_from(self.0 >> 120).unwrap_or(u8::MAX)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0:?} is not an order key id")]
pub struct InvalidOrderKeyId(pub String);

impl FromStr for OrderKeyId {
    type Err = InvalidOrderKeyId;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let invalid = || InvalidOrderKeyId(text.to_owned());
        if !lower_hex(text, 32) {
            return Err(invalid());
        }
        u128::from_str_radix(text, 16)
            .map(Self)
            .map_err(|_| invalid())
    }
}

impl fmt::Display for OrderKeyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:032x}", self.0)
    }
}

pub fn column_value<'a>(envelope: &'a Envelope, column: &str) -> Option<&'a Value> {
    envelope
        .after
        .as_ref()
        .or(envelope.before.as_ref())?
        .get(column)
        .filter(|value| !value.is_null())
}

pub fn canonical_order_key(order_by: &OrderBy, envelope: &Envelope) -> Value {
    let table = || {
        vec![
            Value::String(envelope.schema.clone()),
            Value::String(envelope.table.clone()),
        ]
    };
    let parts = match order_by {
        OrderBy::Key => {
            let mut parts = table();
            if let Some(key) = &envelope.key {
                parts.extend(key.values().cloned());
            }
            parts
        }
        OrderBy::Table => table(),
        OrderBy::Column(name) => match column_value(envelope, name) {
            Some(value) => vec![value.clone()],
            None => table(),
        },
        OrderBy::Pipeline => Vec::new(),
    };
    Value::Array(parts)
}

pub fn order_key(order_by: &OrderBy, envelope: &Envelope) -> OrderKeyId {
    OrderKeyId::of(
        canonical_order_key(order_by, envelope)
            .to_string()
            .as_bytes(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Op, sample_envelope as envelope};
    use proptest::prelude::*;
    use serde_json::json;

    fn insert(after: Value) -> Envelope {
        let key = after.get("id").map(|id| json!({ "id": id }));
        envelope(Op::Insert, key, None, Some(after))
    }

    #[test]
    fn order_by_parses_the_four_forms() {
        assert_eq!("key".parse(), Ok(OrderBy::Key));
        assert_eq!("table".parse(), Ok(OrderBy::Table));
        assert_eq!("pipeline".parse(), Ok(OrderBy::Pipeline));
        assert_eq!(
            "column:tenant_id".parse(),
            Ok(OrderBy::Column("tenant_id".to_owned()))
        );
        assert_eq!("column:a:b".parse(), Ok(OrderBy::Column("a:b".to_owned())));
        for bad in ["", "Key", "column:", "column", "columns:x", " key"] {
            assert_eq!(
                bad.parse::<OrderBy>(),
                Err(InvalidOrderBy(bad.to_owned())),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn canonical_order_keys_follow_the_spec() {
        let event = envelope(
            Op::Update,
            Some(json!({"region": "eu", "id": 42})),
            None,
            Some(json!({"id": 42, "region": "eu", "tenant_id": 7})),
        );
        let canonical = |order_by: &OrderBy| canonical_order_key(order_by, &event).to_string();
        assert_eq!(canonical(&OrderBy::Key), r#"["public","orders","eu",42]"#);
        assert_eq!(canonical(&OrderBy::Table), r#"["public","orders"]"#);
        assert_eq!(canonical(&OrderBy::Column("tenant_id".to_owned())), "[7]");
        assert_eq!(canonical(&OrderBy::Pipeline), "[]");
    }

    #[test]
    fn a_relation_without_a_key_orders_by_table() {
        let event = envelope(Op::Insert, None, None, Some(json!({"a": 1})));
        assert_eq!(
            canonical_order_key(&OrderBy::Key, &event).to_string(),
            r#"["public","orders"]"#
        );
    }

    #[test]
    fn truncate_orders_by_table_under_key() {
        let event = envelope(Op::Truncate, None, None, None);
        assert_eq!(
            order_key(&OrderBy::Key, &event),
            order_key(&OrderBy::Table, &event)
        );
    }

    #[test]
    fn column_falls_back_to_the_table_when_absent_or_null() {
        let by_tenant = OrderBy::Column("tenant_id".to_owned());
        for after in [json!({"id": 1}), json!({"id": 1, "tenant_id": null})] {
            assert_eq!(
                canonical_order_key(&by_tenant, &insert(after)).to_string(),
                r#"["public","orders"]"#
            );
        }
    }

    #[test]
    fn column_comes_from_before_when_there_is_no_after() {
        let delete = envelope(
            Op::Delete,
            Some(json!({"id": 1})),
            Some(json!({"id": 1, "tenant_id": "acme"})),
            None,
        );
        assert_eq!(
            canonical_order_key(&OrderBy::Column("tenant_id".to_owned()), &delete).to_string(),
            r#"["acme"]"#
        );
    }

    #[test]
    fn order_key_id_is_the_xxh3_128_of_the_canonical_key_as_32_hex_digits() {
        let id = order_key(&OrderBy::Key, &insert(json!({"id": 42, "status": "paid"})));
        assert_eq!(id, OrderKeyId::of(br#"["public","orders",42]"#));
        assert_eq!(id.to_string(), GOLDEN_ORDER_KEY_ID);
        assert_eq!(
            OrderKeyId(0xAB).to_string(),
            format!("{}ab", "0".repeat(30))
        );
    }

    const GOLDEN_ORDER_KEY_ID: &str = "aad3af5acd2e8f21df406c0c1530774c";

    #[test]
    fn events_of_one_row_share_an_order_key_and_other_rows_do_not() {
        let paid = insert(json!({"id": 42, "status": "paid"}));
        let delete = envelope(
            Op::Delete,
            Some(json!({"id": 42})),
            Some(json!({"id": 42, "status": "paid"})),
            None,
        );
        assert_eq!(
            order_key(&OrderBy::Key, &paid),
            order_key(&OrderBy::Key, &delete)
        );
        assert_ne!(
            order_key(&OrderBy::Key, &paid),
            order_key(&OrderBy::Key, &insert(json!({"id": 43})))
        );
        assert_ne!(
            order_key(&OrderBy::Key, &paid),
            order_key(&OrderBy::Key, &insert(json!({"id": "42"})))
        );
    }

    #[test]
    fn lanes_take_the_order_key_id_modulo_the_lane_count() {
        assert_eq!(OrderKeyId(130).lane(64), 2);
        assert_eq!(OrderKeyId(u128::MAX).lane(64), 63);
        assert_eq!(OrderKeyId(7).lane(1), 0);
        assert_eq!(OrderKeyId(7).lane(0), 0);
    }

    #[test]
    fn the_partition_is_the_first_byte_of_the_id() {
        assert_eq!(OrderKeyId(0xAB << 120).partition(), 0xAB);
        assert_eq!(OrderKeyId(u128::MAX).partition(), 0xFF);
        assert_eq!(OrderKeyId(0xFF).partition(), 0);
        let id = order_key(&OrderBy::Key, &insert(json!({"id": 42, "status": "paid"})));
        assert_eq!(format!("{:02x}", id.partition()), GOLDEN_ORDER_KEY_ID[..2]);
    }

    #[test]
    fn order_key_ids_parse_from_32_lowercase_hex_digits() {
        assert_eq!(
            GOLDEN_ORDER_KEY_ID
                .parse::<OrderKeyId>()
                .map(|id| id.to_string()),
            Ok(GOLDEN_ORDER_KEY_ID.to_owned())
        );
        for bad in [
            "",
            "aad3af5acd2e8f21df406c0c1530774",
            "aad3af5acd2e8f21df406c0c1530774c0",
            "AAD3AF5ACD2E8F21DF406C0C1530774C",
            "+ad3af5acd2e8f21df406c0c1530774c",
            "aad3af5acd2e8f21df406c0c1530774g",
        ] {
            assert_eq!(
                bad.parse::<OrderKeyId>(),
                Err(InvalidOrderKeyId(bad.to_owned())),
                "{bad:?}"
            );
        }
    }

    proptest! {
        #[test]
        fn order_key_id_text_round_trips(id in any::<u128>()) {
            prop_assert_eq!(OrderKeyId(id).to_string().parse::<OrderKeyId>(), Ok(OrderKeyId(id)));
        }

        #[test]
        fn every_order_key_maps_to_a_lane_in_range(id in any::<u128>(), lanes in 1usize..4096) {
            let lane = OrderKeyId(id).lane(lanes);
            prop_assert!(lane < lanes);
            prop_assert_eq!(u128::try_from(lane).unwrap(), id % u128::try_from(lanes).unwrap());
        }
    }
}
