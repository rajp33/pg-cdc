use crate::lsn::Lsn;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    #[error("empty message")]
    Empty,
    #[error("unknown message tag {0:#04x}")]
    UnknownTag(u8),
    #[error("message truncated while reading {what}")]
    Truncated { what: &'static str },
    #[error("{what} is not terminated")]
    UnterminatedString { what: &'static str },
    #[error("{what} is not valid UTF-8")]
    InvalidUtf8 { what: &'static str },
    #[error("{what} is negative: {value}")]
    NegativeCount { what: &'static str, value: i64 },
    #[error("unexpected tuple marker {0:#04x}")]
    UnexpectedTupleMarker(u8),
    #[error("unexpected column kind {0:#04x}")]
    UnexpectedColumnKind(u8),
    #[error("unknown replica identity {0:#04x}")]
    UnknownReplicaIdentity(u8),
    #[error("{0} trailing bytes after the message")]
    TrailingBytes(usize),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalMessage {
    Begin(Begin),
    Commit(Commit),
    Origin(Origin),
    Relation(Relation),
    Type(TypeMessage),
    Insert(Insert),
    Update(Update),
    Delete(Delete),
    Truncate(Truncate),
}

impl LogicalMessage {
    pub fn tag(&self) -> u8 {
        match self {
            Self::Begin(_) => b'B',
            Self::Commit(_) => b'C',
            Self::Origin(_) => b'O',
            Self::Relation(_) => b'R',
            Self::Type(_) => b'Y',
            Self::Insert(_) => b'I',
            Self::Update(_) => b'U',
            Self::Delete(_) => b'D',
            Self::Truncate(_) => b'T',
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Begin {
    pub final_lsn: Lsn,
    pub commit_ts: i64,
    pub xid: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Commit {
    pub flags: u8,
    pub commit_lsn: Lsn,
    pub end_lsn: Lsn,
    pub commit_ts: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Origin {
    pub commit_lsn: Lsn,
    pub name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaIdentity {
    Default,
    Nothing,
    Full,
    Index,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationColumn {
    pub is_key: bool,
    pub name: String,
    pub type_oid: u32,
    pub type_modifier: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relation {
    pub oid: u32,
    pub namespace: String,
    pub name: String,
    pub replica_identity: ReplicaIdentity,
    pub columns: Vec<RelationColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeMessage {
    pub oid: u32,
    pub namespace: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TupleValue {
    Null,
    UnchangedToast,
    Text(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tuple(pub Vec<TupleValue>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OldTuple {
    Key(Tuple),
    Full(Tuple),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Insert {
    pub relation_oid: u32,
    pub new: Tuple,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Update {
    pub relation_oid: u32,
    pub old: Option<OldTuple>,
    pub new: Tuple,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delete {
    pub relation_oid: u32,
    pub old: OldTuple,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Truncate {
    pub cascade: bool,
    pub restart_identity: bool,
    pub relation_oids: Vec<u32>,
}

pub fn decode(bytes: &[u8]) -> Result<LogicalMessage, DecodeError> {
    let (&tag, body) = bytes.split_first().ok_or(DecodeError::Empty)?;
    let mut reader = Reader { bytes: body };
    let message = match tag {
        b'B' => LogicalMessage::Begin(Begin {
            final_lsn: Lsn(reader.u64("final LSN")?),
            commit_ts: reader.i64("commit timestamp")?,
            xid: reader.u32("xid")?,
        }),
        b'C' => LogicalMessage::Commit(Commit {
            flags: reader.u8("commit flags")?,
            commit_lsn: Lsn(reader.u64("commit LSN")?),
            end_lsn: Lsn(reader.u64("end LSN")?),
            commit_ts: reader.i64("commit timestamp")?,
        }),
        b'O' => LogicalMessage::Origin(Origin {
            commit_lsn: Lsn(reader.u64("origin commit LSN")?),
            name: reader.cstring("origin name")?,
        }),
        b'R' => LogicalMessage::Relation(relation(&mut reader)?),
        b'Y' => LogicalMessage::Type(TypeMessage {
            oid: reader.u32("type oid")?,
            namespace: reader.cstring("type namespace")?,
            name: reader.cstring("type name")?,
        }),
        b'I' => LogicalMessage::Insert(insert(&mut reader)?),
        b'U' => LogicalMessage::Update(update(&mut reader)?),
        b'D' => LogicalMessage::Delete(delete(&mut reader)?),
        b'T' => LogicalMessage::Truncate(truncate(&mut reader)?),
        other => return Err(DecodeError::UnknownTag(other)),
    };
    reader.finish()?;
    Ok(message)
}

fn relation(reader: &mut Reader<'_>) -> Result<Relation, DecodeError> {
    let oid = reader.u32("relation oid")?;
    let namespace = reader.cstring("relation namespace")?;
    let name = reader.cstring("relation name")?;
    let replica_identity = match reader.u8("replica identity")? {
        b'd' => ReplicaIdentity::Default,
        b'n' => ReplicaIdentity::Nothing,
        b'f' => ReplicaIdentity::Full,
        b'i' => ReplicaIdentity::Index,
        other => return Err(DecodeError::UnknownReplicaIdentity(other)),
    };
    let count = reader.count16("relation column count")?;
    let mut columns = Vec::with_capacity(count.min(reader.remaining()));
    for _ in 0..count {
        columns.push(RelationColumn {
            is_key: reader.u8("column flags")? & 1 == 1,
            name: reader.cstring("column name")?,
            type_oid: reader.u32("column type oid")?,
            type_modifier: reader.i32("column type modifier")?,
        });
    }
    Ok(Relation {
        oid,
        namespace,
        name,
        replica_identity,
        columns,
    })
}

fn insert(reader: &mut Reader<'_>) -> Result<Insert, DecodeError> {
    let relation_oid = reader.u32("relation oid")?;
    match reader.u8("tuple marker")? {
        b'N' => Ok(Insert {
            relation_oid,
            new: tuple(reader)?,
        }),
        other => Err(DecodeError::UnexpectedTupleMarker(other)),
    }
}

fn update(reader: &mut Reader<'_>) -> Result<Update, DecodeError> {
    let relation_oid = reader.u32("relation oid")?;
    let mut marker = reader.u8("tuple marker")?;
    let old = match marker {
        b'K' | b'O' => {
            let old = old_tuple(marker, reader)?;
            marker = reader.u8("tuple marker")?;
            Some(old)
        }
        _ => None,
    };
    if marker != b'N' {
        return Err(DecodeError::UnexpectedTupleMarker(marker));
    }
    Ok(Update {
        relation_oid,
        old,
        new: tuple(reader)?,
    })
}

fn delete(reader: &mut Reader<'_>) -> Result<Delete, DecodeError> {
    let relation_oid = reader.u32("relation oid")?;
    let marker = reader.u8("tuple marker")?;
    Ok(Delete {
        relation_oid,
        old: old_tuple(marker, reader)?,
    })
}

fn old_tuple(marker: u8, reader: &mut Reader<'_>) -> Result<OldTuple, DecodeError> {
    match marker {
        b'K' => Ok(OldTuple::Key(tuple(reader)?)),
        b'O' => Ok(OldTuple::Full(tuple(reader)?)),
        other => Err(DecodeError::UnexpectedTupleMarker(other)),
    }
}

fn truncate(reader: &mut Reader<'_>) -> Result<Truncate, DecodeError> {
    let count = reader.count32("truncated relation count")?;
    let options = reader.u8("truncate options")?;
    let mut relation_oids = Vec::with_capacity(count.min(reader.remaining() / 4));
    for _ in 0..count {
        relation_oids.push(reader.u32("truncated relation oid")?);
    }
    Ok(Truncate {
        cascade: options & 1 == 1,
        restart_identity: options & 2 == 2,
        relation_oids,
    })
}

fn tuple(reader: &mut Reader<'_>) -> Result<Tuple, DecodeError> {
    let count = reader.count16("tuple column count")?;
    let mut values = Vec::with_capacity(count.min(reader.remaining()));
    for _ in 0..count {
        let value = match reader.u8("column kind")? {
            b'n' => TupleValue::Null,
            b'u' => TupleValue::UnchangedToast,
            b't' => {
                let len = reader.count32("text length")?;
                let bytes = reader.take(len, "text value")?;
                let text = std::str::from_utf8(bytes)
                    .map_err(|_| DecodeError::InvalidUtf8 { what: "text value" })?;
                TupleValue::Text(text.to_owned())
            }
            other => return Err(DecodeError::UnexpectedColumnKind(other)),
        };
        values.push(value);
    }
    Ok(Tuple(values))
}

struct Reader<'a> {
    bytes: &'a [u8],
}

impl<'a> Reader<'a> {
    fn remaining(&self) -> usize {
        self.bytes.len()
    }

    fn take(&mut self, len: usize, what: &'static str) -> Result<&'a [u8], DecodeError> {
        let (head, rest) = self
            .bytes
            .split_at_checked(len)
            .ok_or(DecodeError::Truncated { what })?;
        self.bytes = rest;
        Ok(head)
    }

    fn array<const N: usize>(&mut self, what: &'static str) -> Result<[u8; N], DecodeError> {
        let (head, rest) = self
            .bytes
            .split_first_chunk::<N>()
            .ok_or(DecodeError::Truncated { what })?;
        self.bytes = rest;
        Ok(*head)
    }

    fn u8(&mut self, what: &'static str) -> Result<u8, DecodeError> {
        Ok(u8::from_be_bytes(self.array(what)?))
    }

    fn i32(&mut self, what: &'static str) -> Result<i32, DecodeError> {
        Ok(i32::from_be_bytes(self.array(what)?))
    }

    fn u32(&mut self, what: &'static str) -> Result<u32, DecodeError> {
        Ok(u32::from_be_bytes(self.array(what)?))
    }

    fn i64(&mut self, what: &'static str) -> Result<i64, DecodeError> {
        Ok(i64::from_be_bytes(self.array(what)?))
    }

    fn u64(&mut self, what: &'static str) -> Result<u64, DecodeError> {
        Ok(u64::from_be_bytes(self.array(what)?))
    }

    fn count16(&mut self, what: &'static str) -> Result<usize, DecodeError> {
        let value = i16::from_be_bytes(self.array(what)?);
        usize::try_from(value).map_err(|_| DecodeError::NegativeCount {
            what,
            value: i64::from(value),
        })
    }

    fn count32(&mut self, what: &'static str) -> Result<usize, DecodeError> {
        let value = self.i32(what)?;
        usize::try_from(value).map_err(|_| DecodeError::NegativeCount {
            what,
            value: i64::from(value),
        })
    }

    fn cstring(&mut self, what: &'static str) -> Result<String, DecodeError> {
        let end = self
            .bytes
            .iter()
            .position(|&byte| byte == 0)
            .ok_or(DecodeError::UnterminatedString { what })?;
        let text = self.take(end, what)?;
        self.take(1, what)?;
        std::str::from_utf8(text)
            .map(str::to_owned)
            .map_err(|_| DecodeError::InvalidUtf8 { what })
    }

    fn finish(self) -> Result<(), DecodeError> {
        if self.bytes.is_empty() {
            Ok(())
        } else {
            Err(DecodeError::TrailingBytes(self.bytes.len()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const BEGIN: &str = "4200000000015b2420000300c84b121f09000002f2";
    const COMMIT: &str = "430000000000015b242000000000015b2450000300c84b121f09";
    const ORIGIN: &str = "4f000000000000000066786f00";
    const TYPE: &str = "59000040057075626c6963006d6f6f6400";
    const RELATION: &str = "52000040207075626c6963006678006400030169640000000017ffffffff006e616d650000000019ffffffff006d6f6f640000004005ffffffff";
    const INSERT: &str = "49000040204e000374000000013174000000016174000000056861707079";
    const UPDATE_NEW_ONLY: &str = "55000040204e000374000000013174000000016274000000056861707079";
    const UPDATE_WITH_KEY: &str =
        "55000040204b00037400000001316e6e4e000374000000013274000000016274000000056861707079";
    const UPDATE_FULL_TOAST: &str = "55000040274f00027400000001317400000003616263";
    const DELETE_WITH_KEY: &str = "44000040204b00037400000001326e6e";
    const TRUNCATE: &str = "5400000002030000402000004027";

    fn hex(text: &str) -> Vec<u8> {
        (0..text.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
            .collect()
    }

    fn text(value: &str) -> TupleValue {
        TupleValue::Text(value.to_owned())
    }

    fn update_full_toast() -> Vec<u8> {
        let mut bytes = hex(UPDATE_FULL_TOAST);
        bytes.extend_from_slice(&hex("4e000274000000013275"));
        bytes
    }

    fn samples() -> Vec<Vec<u8>> {
        vec![
            hex(BEGIN),
            hex(COMMIT),
            hex(ORIGIN),
            hex(TYPE),
            hex(RELATION),
            hex(INSERT),
            hex(UPDATE_NEW_ONLY),
            hex(UPDATE_WITH_KEY),
            update_full_toast(),
            hex(DELETE_WITH_KEY),
            hex(TRUNCATE),
        ]
    }

    #[test]
    fn decodes_begin() {
        assert_eq!(
            decode(&hex(BEGIN)),
            Ok(LogicalMessage::Begin(Begin {
                final_lsn: Lsn(0x015B_2420),
                commit_ts: 845_285_183_069_961,
                xid: 754
            }))
        );
    }

    #[test]
    fn decodes_commit() {
        assert_eq!(
            decode(&hex(COMMIT)),
            Ok(LogicalMessage::Commit(Commit {
                flags: 0,
                commit_lsn: Lsn(0x015B_2420),
                end_lsn: Lsn(0x015B_2450),
                commit_ts: 845_285_183_069_961,
            }))
        );
    }

    #[test]
    fn decodes_origin() {
        assert_eq!(
            decode(&hex(ORIGIN)),
            Ok(LogicalMessage::Origin(Origin {
                commit_lsn: Lsn(0),
                name: "fxo".to_owned()
            }))
        );
    }

    #[test]
    fn decodes_type() {
        assert_eq!(
            decode(&hex(TYPE)),
            Ok(LogicalMessage::Type(TypeMessage {
                oid: 16389,
                namespace: "public".to_owned(),
                name: "mood".to_owned()
            }))
        );
    }

    #[test]
    fn decodes_relation() {
        let column = |is_key, name: &str, type_oid| RelationColumn {
            is_key,
            name: name.to_owned(),
            type_oid,
            type_modifier: -1,
        };
        assert_eq!(
            decode(&hex(RELATION)),
            Ok(LogicalMessage::Relation(Relation {
                oid: 16416,
                namespace: "public".to_owned(),
                name: "fx".to_owned(),
                replica_identity: ReplicaIdentity::Default,
                columns: vec![
                    column(true, "id", 23),
                    column(false, "name", 25),
                    column(false, "mood", 16389)
                ],
            }))
        );
    }

    #[test]
    fn decodes_insert() {
        assert_eq!(
            decode(&hex(INSERT)),
            Ok(LogicalMessage::Insert(Insert {
                relation_oid: 16416,
                new: Tuple(vec![text("1"), text("a"), text("happy")])
            }))
        );
    }

    #[test]
    fn decodes_update_without_old_tuple() {
        assert_eq!(
            decode(&hex(UPDATE_NEW_ONLY)),
            Ok(LogicalMessage::Update(Update {
                relation_oid: 16416,
                old: None,
                new: Tuple(vec![text("1"), text("b"), text("happy")]),
            }))
        );
    }

    #[test]
    fn decodes_update_with_key_tuple() {
        assert_eq!(
            decode(&hex(UPDATE_WITH_KEY)),
            Ok(LogicalMessage::Update(Update {
                relation_oid: 16416,
                old: Some(OldTuple::Key(Tuple(vec![
                    text("1"),
                    TupleValue::Null,
                    TupleValue::Null
                ]))),
                new: Tuple(vec![text("2"), text("b"), text("happy")]),
            }))
        );
    }

    #[test]
    fn decodes_update_with_full_tuple_and_unchanged_toast() {
        assert_eq!(
            decode(&update_full_toast()),
            Ok(LogicalMessage::Update(Update {
                relation_oid: 16423,
                old: Some(OldTuple::Full(Tuple(vec![text("1"), text("abc")]))),
                new: Tuple(vec![text("2"), TupleValue::UnchangedToast]),
            }))
        );
    }

    #[test]
    fn decodes_delete_with_key_tuple() {
        assert_eq!(
            decode(&hex(DELETE_WITH_KEY)),
            Ok(LogicalMessage::Delete(Delete {
                relation_oid: 16416,
                old: OldTuple::Key(Tuple(vec![text("2"), TupleValue::Null, TupleValue::Null])),
            }))
        );
    }

    #[test]
    fn decodes_truncate() {
        assert_eq!(
            decode(&hex(TRUNCATE)),
            Ok(LogicalMessage::Truncate(Truncate {
                cascade: true,
                restart_identity: true,
                relation_oids: vec![16416, 16423]
            }))
        );
    }

    #[test]
    fn d1_every_message_in_the_closed_set_decodes() {
        let tags: Vec<u8> = samples()
            .iter()
            .map(|bytes| decode(bytes).unwrap().tag())
            .collect();
        assert_eq!(tags, b"BCOYRIUUUDT".to_vec());
    }

    #[test]
    fn d1_unknown_tag_is_rejected() {
        for tag in [
            b'M', b'S', b'E', b'c', b'P', b'K', b'A', b'b', b's', b'r', b'p', 0,
        ] {
            assert_eq!(
                decode(&[tag, 0, 0, 0, 1]),
                Err(DecodeError::UnknownTag(tag))
            );
        }
    }

    #[test]
    fn d3_truncated_messages_are_errors_not_panics() {
        assert_eq!(decode(&[]), Err(DecodeError::Empty));
        for bytes in samples() {
            for len in 1..bytes.len() {
                assert!(
                    decode(&bytes[..len]).is_err(),
                    "prefix of {len} bytes of {bytes:02x?} decoded"
                );
            }
        }
    }

    #[test]
    fn d3_trailing_bytes_are_rejected() {
        let mut bytes = hex(INSERT);
        bytes.push(0);
        assert_eq!(decode(&bytes), Err(DecodeError::TrailingBytes(1)));
    }

    #[test]
    fn rejects_binary_and_unknown_column_kinds() {
        assert_eq!(
            decode(&hex("49000040204e0001620000000131")),
            Err(DecodeError::UnexpectedColumnKind(b'b'))
        );
        assert_eq!(
            decode(&hex("49000040204e000178")),
            Err(DecodeError::UnexpectedColumnKind(b'x'))
        );
    }

    #[test]
    fn rejects_invalid_utf8_text() {
        assert_eq!(
            decode(&hex("49000040204e00017400000001ff")),
            Err(DecodeError::InvalidUtf8 { what: "text value" })
        );
    }

    #[test]
    fn rejects_negative_lengths_and_counts() {
        assert_eq!(
            decode(&hex("49000040204e000174ffffffff")),
            Err(DecodeError::NegativeCount {
                what: "text length",
                value: -1
            })
        );
        assert_eq!(
            decode(&hex("49000040204effff")),
            Err(DecodeError::NegativeCount {
                what: "tuple column count",
                value: -1
            })
        );
    }

    #[test]
    fn rejects_unexpected_tuple_markers() {
        assert_eq!(
            decode(&hex("55000040205800")),
            Err(DecodeError::UnexpectedTupleMarker(b'X'))
        );
        assert_eq!(
            decode(&hex("44000040204e0000")),
            Err(DecodeError::UnexpectedTupleMarker(b'N'))
        );
    }

    #[test]
    fn rejects_unknown_replica_identity() {
        assert_eq!(
            decode(&hex("52000040207000667800780000")),
            Err(DecodeError::UnknownReplicaIdentity(b'x'))
        );
    }

    #[test]
    fn truncate_with_huge_count_fails_without_allocating() {
        assert_eq!(
            decode(&hex("547fffffff00")),
            Err(DecodeError::Truncated {
                what: "truncated relation oid"
            })
        );
    }

    proptest! {
        #[test]
        fn decode_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
            let _ = decode(&bytes);
        }

        #[test]
        fn mutated_samples_never_panic(index in 0usize..11, position in 0usize..64, value in any::<u8>()) {
            let mut bytes = samples().swap_remove(index);
            if let Some(byte) = bytes.get_mut(position) {
                *byte = value;
            }
            let _ = decode(&bytes);
        }
    }
}
