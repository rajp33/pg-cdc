use std::collections::HashMap;

use pg_cdc_decoder::Lsn;
use pg_cdc_decoder::value::{TypeKind, builtin_kind};
use tokio_postgres::Client;

const MAX_TYPE_DEPTH: usize = 16;

const TYPE_SQL: &str = "SELECT t.typtype, t.typcategory, t.typelem, t.typbasetype, coalesce(e.typdelim, t.typdelim) \
     FROM pg_type t LEFT JOIN pg_type e ON e.oid = t.typelem WHERE t.oid = $1";

const KEY_SQL: &str = "SELECT a.attname::text FROM pg_index i \
     CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) \
     JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum \
     WHERE i.indexrelid = (SELECT x.indexrelid FROM pg_index x \
     WHERE x.indrelid = $1 AND (x.indisprimary OR x.indisreplident) \
     ORDER BY x.indisprimary DESC LIMIT 1) \
     AND k.ord <= i.indnkeyatts \
     ORDER BY k.ord";

const SLOT_SQL: &str =
    "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1";

const ACTIVITY_SQL: &str = "SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1";

const RETAINED_SQL: &str = "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint \
     FROM pg_replication_slots WHERE slot_name = $1";

const IDENTITY_SQL: &str = "SELECT pt.schemaname::text, pt.tablename::text FROM pg_publication_tables pt \
     JOIN pg_namespace n ON n.nspname = pt.schemaname \
     JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = pt.tablename \
     WHERE pt.pubname = $1 AND c.relreplident <> 'f' ORDER BY 1, 2";

#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("catalog query failed: {0}")]
    Query(#[from] tokio_postgres::Error),
    #[error("slot {slot} reports an invalid confirmed_flush_lsn {value:?}")]
    InvalidLsn { slot: String, value: String },
    #[error("slot {slot} reports an invalid active_pid {pid}")]
    InvalidPid { slot: String, pid: i32 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotActivity {
    pub active_pid: Option<u32>,
}

#[derive(Debug, Default)]
pub struct TypeResolver {
    cache: HashMap<u32, TypeKind>,
}

impl TypeResolver {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn resolve(&mut self, client: &Client, oid: u32) -> Result<TypeKind, CatalogError> {
        if let Some(kind) = builtin_kind(oid) {
            return Ok(kind);
        }
        if let Some(kind) = self.cache.get(&oid) {
            return Ok(kind.clone());
        }
        let kind = lookup(client, oid).await?;
        self.cache.insert(oid, kind.clone());
        Ok(kind)
    }
}

async fn lookup(client: &Client, oid: u32) -> Result<TypeKind, CatalogError> {
    let mut delimiters = Vec::new();
    let mut current = oid;
    for _ in 0..MAX_TYPE_DEPTH {
        if let Some(kind) = builtin_kind(current) {
            return Ok(wrap(kind, &delimiters));
        }
        let Some(row) = client.query_opt(TYPE_SQL, &[&current]).await? else {
            return Ok(wrap(TypeKind::Text, &delimiters));
        };
        let typtype: i8 = row.try_get(0)?;
        let category: i8 = row.try_get(1)?;
        let element: u32 = row.try_get(2)?;
        let base: u32 = row.try_get(3)?;
        let delimiter: i8 = row.try_get(4)?;
        match (typtype.cast_unsigned(), category.cast_unsigned()) {
            (b'd', _) => current = base,
            (_, b'A') if element != 0 => {
                delimiters.push(char::from(delimiter.cast_unsigned()));
                current = element;
            }
            _ => return Ok(wrap(TypeKind::Text, &delimiters)),
        }
    }
    Ok(TypeKind::Text)
}

fn wrap(kind: TypeKind, delimiters: &[char]) -> TypeKind {
    delimiters
        .iter()
        .rev()
        .fold(kind, |element, &delimiter| TypeKind::Array {
            element: Box::new(element),
            delimiter,
        })
}

pub async fn key_columns(client: &Client, relation_oid: u32) -> Result<Vec<String>, CatalogError> {
    let rows = client.query(KEY_SQL, &[&relation_oid]).await?;
    rows.iter()
        .map(|row| row.try_get(0).map_err(CatalogError::from))
        .collect()
}

pub async fn slot_confirmed_flush(
    client: &Client,
    slot: &str,
) -> Result<Option<Lsn>, CatalogError> {
    let Some(row) = client.query_opt(SLOT_SQL, &[&slot]).await? else {
        return Ok(None);
    };
    let Some(text) = row.try_get::<_, Option<String>>(0)? else {
        return Ok(None);
    };
    text.parse()
        .map(Some)
        .map_err(|_| CatalogError::InvalidLsn {
            slot: slot.to_owned(),
            value: text,
        })
}

pub async fn slot_activity(
    client: &Client,
    slot: &str,
) -> Result<Option<SlotActivity>, CatalogError> {
    let Some(row) = client.query_opt(ACTIVITY_SQL, &[&slot]).await? else {
        return Ok(None);
    };
    let active_pid = row
        .try_get::<_, Option<i32>>(0)?
        .map(|pid| checked_pid(slot, pid))
        .transpose()?;
    Ok(Some(SlotActivity { active_pid }))
}

fn checked_pid(slot: &str, pid: i32) -> Result<u32, CatalogError> {
    u32::try_from(pid).map_err(|_| CatalogError::InvalidPid {
        slot: slot.to_owned(),
        pid,
    })
}

pub async fn slot_retained_bytes(client: &Client, slot: &str) -> Result<Option<u64>, CatalogError> {
    let Some(row) = client.query_opt(RETAINED_SQL, &[&slot]).await? else {
        return Ok(None);
    };
    Ok(row
        .try_get::<_, Option<i64>>(0)?
        .map(|bytes| u64::try_from(bytes).unwrap_or(0)))
}

pub async fn tables_without_full_identity(
    client: &Client,
    publication: &str,
) -> Result<Vec<(String, String)>, CatalogError> {
    let rows = client.query(IDENTITY_SQL, &[&publication]).await?;
    rows.iter()
        .map(|row| Ok((row.try_get(0)?, row.try_get(1)?)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_non_negative_pid_converts() {
        assert_eq!(checked_pid("s", 4242).ok(), Some(4242));
    }

    #[test]
    fn a_negative_pid_is_invalid() {
        let error = checked_pid("s", -1).err();
        assert!(
            matches!(&error, Some(CatalogError::InvalidPid { slot, pid: -1 }) if slot == "s"),
            "{error:?}"
        );
    }
}
