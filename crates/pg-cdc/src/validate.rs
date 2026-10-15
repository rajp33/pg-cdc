use tokio_postgres::Client;

use crate::catalog::CatalogError;

pub const PGOUTPUT: &str = "pgoutput";

const LOGICAL: &str = "logical";
const LOST: &str = "lost";
const UNRESERVED: &str = "unreserved";

const FACTS_SQL: &str = "SELECT current_setting('wal_level'), current_database()::text, \
     s.slot_name IS NOT NULL, s.plugin::text, s.database::text, s.wal_status, \
     EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $2) \
     FROM (SELECT 1) one LEFT JOIN pg_replication_slots s ON s.slot_name = $1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotFacts {
    pub plugin: Option<String>,
    pub database: Option<String>,
    pub wal_status: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceFacts {
    pub wal_level: String,
    pub database: String,
    pub slot: Option<SlotFacts>,
    pub publication_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Invalid {
    #[error("wal_level is {0}, but logical replication needs logical")]
    WalLevel(String),
    #[error("replication slot {0} does not exist")]
    SlotMissing(String),
    #[error("replication slot {slot} uses plugin {plugin:?}, not pgoutput")]
    SlotPlugin {
        slot: String,
        plugin: Option<String>,
    },
    #[error("replication slot {slot} belongs to database {found:?}, not {expected}")]
    SlotDatabase {
        slot: String,
        found: Option<String>,
        expected: String,
    },
    #[error("replication slot {0} is lost (wal_status lost); recreate it")]
    SlotLost(String),
    #[error("publication {0} does not exist")]
    PublicationMissing(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Valid,
    Unreserved,
}

#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    #[error("startup validation failed: {0}")]
    Invalid(#[from] Invalid),
    #[error(transparent)]
    Catalog(#[from] CatalogError),
}

pub fn judge(facts: &SourceFacts, slot: &str, publication: &str) -> Result<Verdict, Invalid> {
    if facts.wal_level != LOGICAL {
        return Err(Invalid::WalLevel(facts.wal_level.clone()));
    }
    let found = facts
        .slot
        .as_ref()
        .ok_or_else(|| Invalid::SlotMissing(slot.to_owned()))?;
    if found.plugin.as_deref() != Some(PGOUTPUT) {
        return Err(Invalid::SlotPlugin {
            slot: slot.to_owned(),
            plugin: found.plugin.clone(),
        });
    }
    if found.database.as_deref() != Some(facts.database.as_str()) {
        return Err(Invalid::SlotDatabase {
            slot: slot.to_owned(),
            found: found.database.clone(),
            expected: facts.database.clone(),
        });
    }
    if found.wal_status.as_deref() == Some(LOST) {
        return Err(Invalid::SlotLost(slot.to_owned()));
    }
    if !facts.publication_exists {
        return Err(Invalid::PublicationMissing(publication.to_owned()));
    }
    if found.wal_status.as_deref() == Some(UNRESERVED) {
        return Ok(Verdict::Unreserved);
    }
    Ok(Verdict::Valid)
}

pub async fn source_facts(
    client: &Client,
    slot: &str,
    publication: &str,
) -> Result<SourceFacts, CatalogError> {
    let row = client.query_one(FACTS_SQL, &[&slot, &publication]).await?;
    let slot_found: bool = row.try_get(2)?;
    let slot = if slot_found {
        Some(SlotFacts {
            plugin: row.try_get(3)?,
            database: row.try_get(4)?,
            wal_status: row.try_get(5)?,
        })
    } else {
        None
    };
    Ok(SourceFacts {
        wal_level: row.try_get(0)?,
        database: row.try_get(1)?,
        slot,
        publication_exists: row.try_get(6)?,
    })
}

pub async fn validate(
    client: &Client,
    slot: &str,
    publication: &str,
) -> Result<Verdict, ValidationError> {
    let facts = source_facts(client, slot, publication).await?;
    let verdict = judge(&facts, slot, publication)?;
    if verdict == Verdict::Unreserved {
        tracing::error!(
            slot,
            "replication slot wal_status is unreserved: the WAL it needs may be removed at the next checkpoint"
        );
    }
    Ok(verdict)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SLOT: &str = "s";
    const PUBLICATION: &str = "p";

    fn facts() -> SourceFacts {
        SourceFacts {
            wal_level: "logical".to_owned(),
            database: "app".to_owned(),
            slot: Some(SlotFacts {
                plugin: Some(PGOUTPUT.to_owned()),
                database: Some("app".to_owned()),
                wal_status: None,
            }),
            publication_exists: true,
        }
    }

    fn with_slot(change: impl FnOnce(&mut SlotFacts)) -> SourceFacts {
        let mut facts = facts();
        if let Some(slot) = facts.slot.as_mut() {
            change(slot);
        }
        facts
    }

    #[test]
    fn judge_accepts_a_logical_server_with_a_matching_slot_and_publication() {
        for status in [None, Some("reserved"), Some("extended")] {
            let facts = with_slot(|slot| slot.wal_status = status.map(str::to_owned));
            assert_eq!(
                judge(&facts, SLOT, PUBLICATION),
                Ok(Verdict::Valid),
                "{status:?}"
            );
        }
    }

    #[test]
    fn judge_rejects_each_misconfiguration() {
        let cases = [
            (
                SourceFacts {
                    wal_level: "replica".to_owned(),
                    ..facts()
                },
                Invalid::WalLevel("replica".to_owned()),
            ),
            (
                SourceFacts {
                    slot: None,
                    ..facts()
                },
                Invalid::SlotMissing(SLOT.to_owned()),
            ),
            (
                with_slot(|slot| slot.plugin = Some("test_decoding".to_owned())),
                Invalid::SlotPlugin {
                    slot: SLOT.to_owned(),
                    plugin: Some("test_decoding".to_owned()),
                },
            ),
            (
                with_slot(|slot| slot.plugin = None),
                Invalid::SlotPlugin {
                    slot: SLOT.to_owned(),
                    plugin: None,
                },
            ),
            (
                with_slot(|slot| slot.database = Some("other".to_owned())),
                Invalid::SlotDatabase {
                    slot: SLOT.to_owned(),
                    found: Some("other".to_owned()),
                    expected: "app".to_owned(),
                },
            ),
            (
                with_slot(|slot| slot.database = None),
                Invalid::SlotDatabase {
                    slot: SLOT.to_owned(),
                    found: None,
                    expected: "app".to_owned(),
                },
            ),
            (
                with_slot(|slot| slot.wal_status = Some("lost".to_owned())),
                Invalid::SlotLost(SLOT.to_owned()),
            ),
            (
                SourceFacts {
                    publication_exists: false,
                    ..facts()
                },
                Invalid::PublicationMissing(PUBLICATION.to_owned()),
            ),
        ];
        for (facts, expected) in cases {
            assert_eq!(judge(&facts, SLOT, PUBLICATION), Err(expected));
        }
    }

    #[test]
    fn judge_reports_the_first_failure_in_spec_order() {
        let facts_without_anything = SourceFacts {
            wal_level: "replica".to_owned(),
            slot: None,
            publication_exists: false,
            ..facts()
        };
        assert_eq!(
            judge(&facts_without_anything, SLOT, PUBLICATION),
            Err(Invalid::WalLevel("replica".to_owned()))
        );
        let wrong_plugin_and_database = with_slot(|slot| {
            slot.plugin = Some("test_decoding".to_owned());
            slot.database = Some("other".to_owned());
        });
        assert_eq!(
            judge(&wrong_plugin_and_database, SLOT, PUBLICATION),
            Err(Invalid::SlotPlugin {
                slot: SLOT.to_owned(),
                plugin: Some("test_decoding".to_owned()),
            })
        );
        let lost_and_unpublished = SourceFacts {
            publication_exists: false,
            ..with_slot(|slot| slot.wal_status = Some("lost".to_owned()))
        };
        assert_eq!(
            judge(&lost_and_unpublished, SLOT, PUBLICATION),
            Err(Invalid::SlotLost(SLOT.to_owned()))
        );
    }

    #[test]
    fn judge_lets_an_unreserved_slot_through() {
        let facts = with_slot(|slot| slot.wal_status = Some("unreserved".to_owned()));
        assert_eq!(judge(&facts, SLOT, PUBLICATION), Ok(Verdict::Unreserved));
    }
}
