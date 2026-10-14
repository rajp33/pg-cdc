use pg_cdc_decoder::Lsn;
use pg_cdc_decoder::message::{Delete, Insert, OldTuple, Tuple, TupleValue, Update};
use pg_cdc_decoder::value::{TypeKind, format_commit_ts, map_value};
use serde_json::Value;

use super::{ENVELOPE_VERSION, Envelope, EventId, Op, Row, Source};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnInfo {
    pub name: String,
    pub type_oid: u32,
    pub kind: TypeKind,
    pub is_identity: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationInfo {
    pub oid: u32,
    pub schema: String,
    pub table: String,
    pub columns: Vec<ColumnInfo>,
    pub key_columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fallback {
    pub column: String,
    pub type_oid: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Built {
    pub envelope: Envelope,
    pub fallbacks: Vec<Fallback>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BuildError {
    #[error("row change outside a transaction")]
    NoOpenTransaction,
    #[error("a transaction is already open")]
    TransactionAlreadyOpen,
    #[error("relation {relation} has {expected} columns but the tuple has {actual}")]
    TupleShape {
        relation: u32,
        expected: usize,
        actual: usize,
    },
    #[error("the transaction has more than 2^32 events")]
    OrdinalOverflow,
}

#[derive(Debug)]
pub struct Builder {
    pipeline: String,
    txn: Option<Txn>,
}

#[derive(Debug)]
struct Txn {
    commit_lsn: Lsn,
    commit_ts: String,
    next_ordinal: u64,
}

struct Change {
    op: Op,
    key: Option<Row>,
    before: Option<Row>,
    after: Option<Row>,
    unchanged_toast: Vec<String>,
}

impl Builder {
    pub fn new(pipeline: impl Into<String>) -> Self {
        Self {
            pipeline: pipeline.into(),
            txn: None,
        }
    }

    pub fn begin(&mut self, commit_lsn: Lsn, commit_ts_micros: i64) -> Result<(), BuildError> {
        if self.txn.is_some() {
            return Err(BuildError::TransactionAlreadyOpen);
        }
        self.txn = Some(Txn {
            commit_lsn,
            commit_ts: format_commit_ts(commit_ts_micros),
            next_ordinal: 0,
        });
        Ok(())
    }

    pub fn commit(&mut self) -> Result<(), BuildError> {
        self.txn
            .take()
            .map(|_| ())
            .ok_or(BuildError::NoOpenTransaction)
    }

    pub fn insert(
        &mut self,
        relation: &RelationInfo,
        insert: &Insert,
    ) -> Result<Built, BuildError> {
        check_shape(relation, &insert.new)?;
        let mut fallbacks = Vec::new();
        let (after, unchanged_toast) = map_row(relation, &insert.new.0, |_| true, &mut fallbacks);
        let change = Change {
            op: Op::Insert,
            key: key(relation, &after),
            before: None,
            after: Some(after),
            unchanged_toast,
        };
        self.emit(relation, change, fallbacks)
    }

    pub fn update(
        &mut self,
        relation: &RelationInfo,
        update: &Update,
    ) -> Result<Built, BuildError> {
        check_shape(relation, &update.new)?;
        let mut fallbacks = Vec::new();
        let before = match &update.old {
            Some(old) => Some(old_image(relation, old, &mut fallbacks)?),
            None => None,
        };
        let new_values: Vec<TupleValue> = match &update.old {
            Some(OldTuple::Full(old)) => update
                .new
                .0
                .iter()
                .zip(&old.0)
                .map(|(new, old)| match (new, old) {
                    (TupleValue::UnchangedToast, TupleValue::Text(_) | TupleValue::Null) => {
                        old.clone()
                    }
                    _ => new.clone(),
                })
                .collect(),
            _ => update.new.0.clone(),
        };
        let (after, unchanged_toast) = map_row(relation, &new_values, |_| true, &mut fallbacks);
        let change = Change {
            op: Op::Update,
            key: key(relation, &after),
            before,
            after: Some(after),
            unchanged_toast,
        };
        self.emit(relation, change, fallbacks)
    }

    pub fn delete(
        &mut self,
        relation: &RelationInfo,
        delete: &Delete,
    ) -> Result<Built, BuildError> {
        let mut fallbacks = Vec::new();
        let before = old_image(relation, &delete.old, &mut fallbacks)?;
        let change = Change {
            op: Op::Delete,
            key: key(relation, &before),
            before: Some(before),
            after: None,
            unchanged_toast: Vec::new(),
        };
        self.emit(relation, change, fallbacks)
    }

    pub fn truncate(&mut self, relation: &RelationInfo) -> Result<Built, BuildError> {
        let change = Change {
            op: Op::Truncate,
            key: None,
            before: None,
            after: None,
            unchanged_toast: Vec::new(),
        };
        self.emit(relation, change, Vec::new())
    }

    fn emit(
        &mut self,
        relation: &RelationInfo,
        change: Change,
        fallbacks: Vec<Fallback>,
    ) -> Result<Built, BuildError> {
        let txn = self.txn.as_mut().ok_or(BuildError::NoOpenTransaction)?;
        let ordinal = u32::try_from(txn.next_ordinal).map_err(|_| BuildError::OrdinalOverflow)?;
        txn.next_ordinal += 1;
        let envelope = Envelope {
            id: EventId::new(txn.commit_lsn, ordinal),
            version: ENVELOPE_VERSION,
            op: change.op,
            schema: relation.schema.clone(),
            table: relation.table.clone(),
            key: change.key,
            before: change.before,
            after: change.after,
            unchanged_toast: change.unchanged_toast,
            source: Source {
                pipeline: self.pipeline.clone(),
                commit_lsn: txn.commit_lsn.to_string(),
                commit_ts: txn.commit_ts.clone(),
            },
        };
        Ok(Built {
            envelope,
            fallbacks,
        })
    }
}

fn check_shape(relation: &RelationInfo, tuple: &Tuple) -> Result<(), BuildError> {
    if tuple.0.len() == relation.columns.len() {
        Ok(())
    } else {
        Err(BuildError::TupleShape {
            relation: relation.oid,
            expected: relation.columns.len(),
            actual: tuple.0.len(),
        })
    }
}

fn old_image(
    relation: &RelationInfo,
    old: &OldTuple,
    fallbacks: &mut Vec<Fallback>,
) -> Result<Row, BuildError> {
    let (tuple, key_only) = match old {
        OldTuple::Key(tuple) => (tuple, true),
        OldTuple::Full(tuple) => (tuple, false),
    };
    check_shape(relation, tuple)?;
    let (row, _) = map_row(
        relation,
        &tuple.0,
        |column| !key_only || column.is_identity,
        fallbacks,
    );
    Ok(row)
}

fn map_row(
    relation: &RelationInfo,
    values: &[TupleValue],
    keep: impl Fn(&ColumnInfo) -> bool,
    fallbacks: &mut Vec<Fallback>,
) -> (Row, Vec<String>) {
    let mut row = Row::new();
    let mut unchanged = Vec::new();
    for (column, value) in relation.columns.iter().zip(values) {
        if !keep(column) {
            continue;
        }
        match value {
            TupleValue::Null => {
                row.insert(column.name.clone(), Value::Null);
            }
            TupleValue::UnchangedToast => unchanged.push(column.name.clone()),
            TupleValue::Text(text) => {
                let mapped = map_value(&column.kind, text);
                if mapped.fallback {
                    fallbacks.push(Fallback {
                        column: column.name.clone(),
                        type_oid: column.type_oid,
                    });
                }
                row.insert(column.name.clone(), mapped.value);
            }
        }
    }
    (row, unchanged)
}

fn key(relation: &RelationInfo, image: &Row) -> Option<Row> {
    if relation.key_columns.is_empty() {
        return None;
    }
    relation
        .key_columns
        .iter()
        .map(|name| image.get(name).map(|value| (name.clone(), value.clone())))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg_cdc_decoder::message::{Delete, Insert, OldTuple, Tuple, TupleValue, Update};
    use pg_cdc_decoder::value::TypeKind;
    use serde_json::json;

    const COMMIT_LSN: Lsn = Lsn(0x1A2B_3C4D);
    const COMMIT_TS: i64 = 845_294_400_123_456;

    fn column(name: &str, kind: TypeKind, type_oid: u32, is_identity: bool) -> ColumnInfo {
        ColumnInfo {
            name: name.to_owned(),
            type_oid,
            kind,
            is_identity,
        }
    }

    fn orders(columns: Vec<ColumnInfo>) -> RelationInfo {
        RelationInfo {
            oid: 16_384,
            schema: "public".to_owned(),
            table: "orders".to_owned(),
            columns,
            key_columns: vec!["id".to_owned()],
        }
    }

    fn two_columns() -> RelationInfo {
        orders(vec![
            column("id", TypeKind::Integer, 23, true),
            column("status", TypeKind::Text, 25, false),
        ])
    }

    fn three_columns() -> RelationInfo {
        orders(vec![
            column("id", TypeKind::Integer, 23, true),
            column("status", TypeKind::Text, 25, false),
            column("note", TypeKind::Text, 25, false),
        ])
    }

    fn t(value: &str) -> TupleValue {
        TupleValue::Text(value.to_owned())
    }

    fn open() -> Builder {
        let mut builder = Builder::new("orders");
        builder.begin(COMMIT_LSN, COMMIT_TS).unwrap();
        builder
    }

    fn encoded(built: &Built) -> String {
        String::from_utf8(built.envelope.encode().unwrap()).unwrap()
    }

    #[test]
    fn insert_encodes_the_golden_envelope() {
        let mut builder = open();
        let insert = Insert {
            relation_oid: 16_384,
            new: Tuple(vec![t("42"), t("paid"), t("n")]),
        };
        let built = builder.insert(&three_columns(), &insert).unwrap();
        assert_eq!(
            encoded(&built),
            r#"{"id":"000000001a2b3c4d-00000000","version":1,"op":"insert","schema":"public","table":"orders","key":{"id":42},"before":null,"after":{"id":42,"status":"paid","note":"n"},"source":{"pipeline":"orders","commit_lsn":"0/1A2B3C4D","commit_ts":"2026-10-14T12:00:00.123456+00:00"}}"#
        );
        assert_eq!(built.fallbacks, Vec::new());
    }

    #[test]
    fn update_with_full_identity_matches_the_spec_example() {
        let relation = two_columns();
        let mut builder = open();
        for id in ["1", "2", "3"] {
            builder
                .insert(
                    &relation,
                    &Insert {
                        relation_oid: 16_384,
                        new: Tuple(vec![t(id), t("x")]),
                    },
                )
                .unwrap();
        }
        let update = Update {
            relation_oid: 16_384,
            old: Some(OldTuple::Full(Tuple(vec![t("42"), t("pending")]))),
            new: Tuple(vec![t("42"), t("paid")]),
        };
        let built = builder.update(&relation, &update).unwrap();
        assert_eq!(
            encoded(&built),
            r#"{"id":"000000001a2b3c4d-00000003","version":1,"op":"update","schema":"public","table":"orders","key":{"id":42},"before":{"id":42,"status":"pending"},"after":{"id":42,"status":"paid"},"source":{"pipeline":"orders","commit_lsn":"0/1A2B3C4D","commit_ts":"2026-10-14T12:00:00.123456+00:00"}}"#
        );
    }

    #[test]
    fn update_without_old_tuple_has_null_before() {
        let update = Update {
            relation_oid: 16_384,
            old: None,
            new: Tuple(vec![t("42"), t("paid")]),
        };
        let envelope = open().update(&two_columns(), &update).unwrap().envelope;
        assert_eq!(envelope.before, None);
        assert_eq!(
            envelope.after,
            Some(row(json!({"id": 42, "status": "paid"})))
        );
    }

    #[test]
    fn update_with_key_tuple_keeps_only_identity_columns_in_before() {
        let update = Update {
            relation_oid: 16_384,
            old: Some(OldTuple::Key(Tuple(vec![t("41"), TupleValue::Null]))),
            new: Tuple(vec![t("42"), t("paid")]),
        };
        let envelope = open().update(&two_columns(), &update).unwrap().envelope;
        assert_eq!(envelope.before, Some(row(json!({"id": 41}))));
        assert_eq!(envelope.key, Some(row(json!({"id": 42}))));
    }

    #[test]
    fn unchanged_toast_is_filled_from_a_full_old_tuple() {
        let update = Update {
            relation_oid: 16_384,
            old: Some(OldTuple::Full(Tuple(vec![
                t("42"),
                t("pending"),
                t("long text"),
            ]))),
            new: Tuple(vec![t("42"), t("paid"), TupleValue::UnchangedToast]),
        };
        let envelope = open().update(&three_columns(), &update).unwrap().envelope;
        assert_eq!(
            envelope.after,
            Some(row(
                json!({"id": 42, "status": "paid", "note": "long text"})
            ))
        );
        assert_eq!(envelope.unchanged_toast, Vec::<String>::new());
        let keys: Vec<&String> = envelope.after.as_ref().unwrap().keys().collect();
        assert_eq!(keys, ["id", "status", "note"]);
    }

    #[test]
    fn unchanged_toast_without_old_tuple_is_listed_and_omitted() {
        let update = Update {
            relation_oid: 16_384,
            old: None,
            new: Tuple(vec![t("42"), t("paid"), TupleValue::UnchangedToast]),
        };
        let built = open().update(&three_columns(), &update).unwrap();
        assert_eq!(
            built.envelope.after,
            Some(row(json!({"id": 42, "status": "paid"})))
        );
        assert_eq!(built.envelope.unchanged_toast, vec!["note".to_owned()]);
        assert!(
            encoded(&built).contains(
                r#""after":{"id":42,"status":"paid"},"unchanged_toast":["note"],"source":"#
            )
        );
    }

    #[test]
    fn deletes_carry_the_old_image_and_no_after() {
        let key_delete = Delete {
            relation_oid: 16_384,
            old: OldTuple::Key(Tuple(vec![t("42"), TupleValue::Null])),
        };
        let envelope = open().delete(&two_columns(), &key_delete).unwrap().envelope;
        assert_eq!(envelope.op, Op::Delete);
        assert_eq!(envelope.before, Some(row(json!({"id": 42}))));
        assert_eq!(envelope.after, None);
        assert_eq!(envelope.key, Some(row(json!({"id": 42}))));

        let full_delete = Delete {
            relation_oid: 16_384,
            old: OldTuple::Full(Tuple(vec![t("42"), t("paid")])),
        };
        let envelope = open()
            .delete(&two_columns(), &full_delete)
            .unwrap()
            .envelope;
        assert_eq!(
            envelope.before,
            Some(row(json!({"id": 42, "status": "paid"})))
        );
    }

    #[test]
    fn truncate_events_have_null_images_and_one_ordinal_each() {
        let mut builder = open();
        let mut other = two_columns();
        other.oid = 16_390;
        other.table = "items".to_owned();
        let first = builder.truncate(&two_columns()).unwrap();
        let second = builder.truncate(&other).unwrap();
        assert_eq!(first.envelope.id.ordinal(), 0);
        assert_eq!(second.envelope.id.ordinal(), 1);
        assert_eq!(second.envelope.table, "items");
        for built in [&first, &second] {
            assert_eq!(built.envelope.op, Op::Truncate);
            assert!(encoded(built).contains(r#""key":null,"before":null,"after":null"#));
        }
    }

    #[test]
    fn relation_without_key_has_null_key() {
        let mut relation = two_columns();
        relation.key_columns.clear();
        let insert = Insert {
            relation_oid: 16_384,
            new: Tuple(vec![t("1"), t("a")]),
        };
        assert_eq!(
            open().insert(&relation, &insert).unwrap().envelope.key,
            None
        );
    }

    #[test]
    fn key_is_null_when_a_key_value_is_absent() {
        let mut relation = two_columns();
        relation.key_columns = vec!["id".to_owned(), "status".to_owned()];
        let update = Update {
            relation_oid: 16_384,
            old: None,
            new: Tuple(vec![t("1"), TupleValue::UnchangedToast]),
        };
        assert_eq!(
            open().update(&relation, &update).unwrap().envelope.key,
            None
        );
    }

    #[test]
    fn o1_event_ids_are_deterministic_and_strictly_increasing() {
        let relation = two_columns();
        let run = || {
            let mut builder = Builder::new("orders");
            let mut ids = Vec::new();
            for (lsn, rows) in [(Lsn(0x100), 3), (Lsn(0x200), 2)] {
                builder.begin(lsn, COMMIT_TS).unwrap();
                for row in 0..rows {
                    let insert = Insert {
                        relation_oid: 16_384,
                        new: Tuple(vec![t(&row.to_string()), t("x")]),
                    };
                    ids.push(builder.insert(&relation, &insert).unwrap().envelope.id);
                }
                builder.commit().unwrap();
            }
            ids
        };
        let first = run();
        assert_eq!(first, run());
        assert!(
            first
                .windows(2)
                .all(|pair| pair[0] < pair[1] && pair[0].to_string() < pair[1].to_string())
        );
        assert_eq!(first[3], EventId::new(Lsn(0x200), 0));
    }

    #[test]
    fn d3_tuple_with_wrong_column_count_is_a_fault() {
        let insert = Insert {
            relation_oid: 16_384,
            new: Tuple(vec![t("1")]),
        };
        assert_eq!(
            open().insert(&two_columns(), &insert),
            Err(BuildError::TupleShape {
                relation: 16_384,
                expected: 2,
                actual: 1
            })
        );
        let update = Update {
            relation_oid: 16_384,
            old: Some(OldTuple::Full(Tuple(vec![t("1")]))),
            new: Tuple(vec![t("1"), t("a")]),
        };
        assert!(matches!(
            open().update(&two_columns(), &update),
            Err(BuildError::TupleShape { .. })
        ));
    }

    #[test]
    fn row_changes_need_an_open_transaction() {
        let mut builder = Builder::new("orders");
        let insert = Insert {
            relation_oid: 16_384,
            new: Tuple(vec![t("1"), t("a")]),
        };
        assert_eq!(
            builder.insert(&two_columns(), &insert),
            Err(BuildError::NoOpenTransaction)
        );
        assert_eq!(builder.commit(), Err(BuildError::NoOpenTransaction));
        builder.begin(COMMIT_LSN, COMMIT_TS).unwrap();
        assert_eq!(
            builder.begin(COMMIT_LSN, COMMIT_TS),
            Err(BuildError::TransactionAlreadyOpen)
        );
    }

    #[test]
    fn value_fallbacks_are_reported() {
        let insert = Insert {
            relation_oid: 16_384,
            new: Tuple(vec![t("abc"), t("a")]),
        };
        let built = open().insert(&two_columns(), &insert).unwrap();
        assert_eq!(
            built.fallbacks,
            vec![Fallback {
                column: "id".to_owned(),
                type_oid: 23
            }]
        );
        assert_eq!(
            built.envelope.after,
            Some(row(json!({"id": "abc", "status": "a"})))
        );
    }

    fn row(value: serde_json::Value) -> Row {
        match value {
            serde_json::Value::Object(map) => map,
            other => panic!("not an object: {other}"),
        }
    }
}
