//! Local operator operations. Inspections contain names and sizes, never values.
use crate::player::PlayerRecord;
use std::{collections::BTreeMap, fmt};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageAdminAction {
    Statistics,
    Inspect(String),
    Wipe(String),
}

impl StorageAdminAction {
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        if let Self::Inspect(owner) | Self::Wipe(owner) = self {
            anyhow::ensure!(
                !owner.is_empty() && owner.len() <= 4096,
                "invalid player identity"
            );
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct StorageInspection {
    pub used: usize,
    pub scopes: BTreeMap<String, BTreeMap<String, usize>>,
}

impl From<PlayerRecord> for StorageInspection {
    fn from(record: PlayerRecord) -> Self {
        Self {
            used: record.used(),
            scopes: record
                .scopes
                .into_iter()
                .map(|(scope, entries)| {
                    (
                        scope,
                        entries
                            .into_iter()
                            .map(|(key, value)| (key, value.len()))
                            .collect(),
                    )
                })
                .collect(),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum StorageAdminReply {
    Statistics {
        backend: &'static str,
        records: u64,
        bytes: u64,
    },
    Inspection {
        owner: String,
        record: StorageInspection,
    },
    Wiped {
        owner: String,
    },
}

impl fmt::Display for StorageAdminReply {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Statistics {
                backend,
                records,
                bytes,
            } => write!(
                f,
                "Player storage ({backend}): {records} players, {bytes} stored bytes."
            ),
            Self::Inspection { owner, record } => {
                writeln!(
                    f,
                    "{owner:?}: {} bytes used, {} scopes.",
                    record.used,
                    record.scopes.len()
                )?;
                for (scope, entries) in &record.scopes {
                    writeln!(f, "  {scope:?}")?;
                    for (key, bytes) in entries {
                        writeln!(f, "    {key:?} ({bytes} bytes)")?;
                    }
                }
                Ok(())
            }
            Self::Wiped { owner } => write!(f, "Deleted the storage of {owner:?}."),
        }
    }
}
