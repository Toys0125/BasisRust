//! Per-verified-DID Cilbox server storage.
//! Player filenames are SHA-256 digests, not user-controlled paths.
//! Data lives independently of the server's legacy database.json.
use anyhow::{Context, Result};
use basis_protocol::storage::{
    StorageOp, StorageReply, StorageRequest, StorageStatus, DEFAULT_BYTES_PER_PLAYER,
    MAX_BYTES_PER_PLAYER,
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct PlayerRecord {
    pub(crate) owner: String,
    pub(crate) scopes: BTreeMap<String, BTreeMap<String, Vec<u8>>>,
}

impl PlayerRecord {
    pub(crate) fn used(&self) -> usize {
        self.scopes
            .iter()
            .map(|(scope, entries)| {
                3 + scope.len()
                    + entries
                        .iter()
                        .map(|(key, value)| 3 + key.len() + value.len())
                        .sum::<usize>()
            })
            .sum()
    }

    pub(crate) fn entry_count(&self) -> usize {
        self.scopes.values().map(BTreeMap::len).sum()
    }
}

#[derive(Debug)]
pub(crate) struct RateBudget {
    last: Instant,
    reads: f64,
    writes: f64,
}

impl RateBudget {
    pub(crate) fn new() -> Self {
        Self {
            last: Instant::now(),
            reads: 60.0,
            writes: 10.0,
        }
    }
    #[cfg(feature = "sql-storage")]
    pub(crate) fn inactive_for(&self, period: Duration) -> bool {
        self.last.elapsed() >= period
    }

    pub(crate) fn consume(&mut self, write: bool) -> bool {
        let now = Instant::now();
        let delta = now.saturating_duration_since(self.last).as_secs_f64();
        self.last = now;
        self.reads = (self.reads + delta * 20.0).min(60.0);
        self.writes = (self.writes + delta * 2.0).min(10.0);
        let available = if write {
            &mut self.writes
        } else {
            &mut self.reads
        };
        if *available < 1.0 {
            return false;
        }
        *available -= 1.0;
        true
    }
}

#[derive(Debug, Default)]
struct State {
    records: HashMap<String, PlayerRecord>,
    budgets: HashMap<String, RateBudget>,
}

#[derive(Debug, Clone)]
pub struct PlayerStorage {
    root: Option<PathBuf>,
    state: Arc<Mutex<State>>,
}

impl PlayerStorage {
    pub fn new(root: Option<PathBuf>) -> Result<Self> {
        if let Some(root) = &root {
            fs::create_dir_all(root)
                .with_context(|| format!("creating player storage {}", root.display()))?;
        }
        Ok(Self {
            root,
            state: Arc::new(Mutex::new(State::default())),
        })
    }

    fn file_for(&self, owner: &str) -> Option<PathBuf> {
        let digest = Sha256::digest(owner.as_bytes());
        let name = digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        self.root
            .as_ref()
            .map(|root| root.join(format!("{name}.json")))
    }

    fn read_account(&self, owner: &str) -> Result<PlayerRecord> {
        let Some(path) = self.file_for(owner) else {
            return Ok(PlayerRecord {
                owner: owner.to_owned(),
                ..Default::default()
            });
        };
        if !path.exists() {
            return Ok(PlayerRecord {
                owner: owner.to_owned(),
                ..Default::default()
            });
        }
        let bytes = fs::read(&path)?;
        let record: PlayerRecord = serde_json::from_slice(&bytes)?;
        anyhow::ensure!(record.owner == owner, "player storage DID mismatch");
        Ok(record)
    }

    fn save_account(
        &self,
        owner: &str,
        record: &PlayerRecord,
        max_total_bytes: usize,
    ) -> Result<()> {
        let Some(path) = self.file_for(owner) else {
            return Ok(());
        };
        let bytes = serde_json::to_vec(record)?;
        if max_total_bytes > 0 {
            let old = fs::metadata(&path).map(|m| m.len() as usize).unwrap_or(0);
            let mut used = 0usize;
            for entry in fs::read_dir(self.root.as_ref().expect("file storage root"))? {
                let entry = entry?;
                if entry.path().extension().is_some_and(|ext| ext == "json") {
                    used = used.saturating_add(entry.metadata()?.len() as usize);
                }
            }
            anyhow::ensure!(
                used.saturating_sub(old).saturating_add(bytes.len()) <= max_total_bytes,
                "global player storage quota exceeded"
            );
        }
        super::atomic_write_with(
            &path,
            |file| std::io::Write::write_all(file, &bytes),
            super::sync_directory,
        )
    }

    pub fn execute(
        &self,
        owner: &str,
        request: StorageRequest,
        per_player_limit: usize,
        total_limit: usize,
    ) -> StorageReply {
        let limit = per_player_limit.clamp(1, MAX_BYTES_PER_PLAYER);
        let mut reply = StorageReply {
            op: request.op,
            id: request.id,
            status: StorageStatus::Ok,
            used: 0,
            limit: limit as i32,
            value: Vec::new(),
            keys: Vec::new(),
        };
        if owner.is_empty() || owner.len() > 4096 {
            reply.status = StorageStatus::Denied;
            return reply;
        }
        let mut state = self.state.lock();
        if state.budgets.len() >= 8192 {
            state
                .budgets
                .retain(|_, budget| budget.last.elapsed() < Duration::from_secs(60));
        }
        let write = matches!(
            request.op,
            StorageOp::Set | StorageOp::Delete | StorageOp::Clear
        );
        if !state
            .budgets
            .entry(owner.to_owned())
            .or_insert_with(RateBudget::new)
            .consume(write)
        {
            reply.status = StorageStatus::RateLimited;
            return reply;
        }
        if !state.records.contains_key(owner) {
            match self.read_account(owner) {
                Ok(record) => {
                    state.records.insert(owner.to_owned(), record);
                }
                Err(_) => {
                    reply.status = StorageStatus::Unavailable;
                    return reply;
                }
            }
        }
        let record = state.records.get(owner).expect("inserted record");
        let (reply_for_request, updated) = apply_request(record, request, limit);
        reply = reply_for_request;
        if let Some(next) = updated {
            if let Err(error) = self.save_account(owner, &next, total_limit) {
                reply.status = if error.to_string().contains("quota exceeded") {
                    StorageStatus::Full
                } else {
                    StorageStatus::Unavailable
                };
                return reply;
            }
            state.records.insert(owner.to_owned(), next);
        }
        reply
    }

    pub fn default_limit() -> usize {
        DEFAULT_BYTES_PER_PLAYER
    }
}

pub(crate) fn apply_request(
    record: &PlayerRecord,
    request: StorageRequest,
    limit: usize,
) -> (StorageReply, Option<PlayerRecord>) {
    let mut reply = StorageReply {
        op: request.op,
        id: request.id,
        status: StorageStatus::Ok,
        used: record.used() as i32,
        limit: limit as i32,
        value: Vec::new(),
        keys: Vec::new(),
    };
    match request.op {
        StorageOp::Get => {
            if let Some(value) = record
                .scopes
                .get(&request.scope)
                .and_then(|s| s.get(&request.key))
            {
                reply.value = value.clone();
            } else {
                reply.status = StorageStatus::NotFound;
            }
            return (reply, None);
        }
        StorageOp::Keys => {
            reply.keys = record
                .scopes
                .get(&request.scope)
                .map(|s| s.keys().cloned().collect())
                .unwrap_or_default();
            return (reply, None);
        }
        StorageOp::Usage => return (reply, None),
        _ => {}
    }
    let mut next = record.clone();
    match request.op {
        StorageOp::Set => {
            if !next.scopes.contains_key(&request.scope) && next.scopes.len() >= 64 {
                reply.status = StorageStatus::Full;
                return (reply, None);
            }
            if !next
                .scopes
                .get(&request.scope)
                .is_some_and(|s| s.contains_key(&request.key))
                && next.entry_count() >= 1024
            {
                reply.status = StorageStatus::Full;
                return (reply, None);
            }
            next.scopes
                .entry(request.scope)
                .or_default()
                .insert(request.key, request.value);
        }
        StorageOp::Delete => {
            let Some(entries) = next.scopes.get_mut(&request.scope) else {
                reply.status = StorageStatus::NotFound;
                return (reply, None);
            };
            if entries.remove(&request.key).is_none() {
                reply.status = StorageStatus::NotFound;
                return (reply, None);
            }
            if entries.is_empty() {
                next.scopes.remove(&request.scope);
            }
        }
        StorageOp::Clear => {
            next.scopes.remove(&request.scope);
        }
        _ => unreachable!(),
    }
    if next.used() > limit {
        reply.status = StorageStatus::Full;
        return (reply, None);
    }
    reply.used = next.used() as i32;
    (reply, Some(next))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn per_did_isolation_and_file_restart() {
        let dir = tempfile::tempdir().unwrap();
        let storage = PlayerStorage::new(Some(dir.path().to_path_buf())).unwrap();
        let req = |op, value| StorageRequest {
            op,
            id: 1,
            scope: "level".into(),
            key: "score".into(),
            value,
        };
        assert_eq!(
            storage
                .execute("alice", req(StorageOp::Set, vec![1, 2, 3]), 16384, 100000)
                .status,
            StorageStatus::Ok
        );
        assert_eq!(
            storage
                .execute("bob", req(StorageOp::Get, Vec::new()), 16384, 100000)
                .status,
            StorageStatus::NotFound
        );
        let second = PlayerStorage::new(Some(dir.path().to_path_buf())).unwrap();
        assert_eq!(
            second
                .execute("alice", req(StorageOp::Get, Vec::new()), 16384, 100000)
                .value,
            [1, 2, 3]
        );
        assert_eq!(
            second
                .execute("alice", req(StorageOp::Clear, Vec::new()), 16384, 100000)
                .status,
            StorageStatus::Ok
        );
        assert_eq!(
            second
                .execute("alice", req(StorageOp::Keys, Vec::new()), 16384, 100000)
                .keys
                .len(),
            0
        );
    }
    #[test]
    fn quota_does_not_mutate_existing_record() {
        let storage = PlayerStorage::new(None).unwrap();
        let request = StorageRequest {
            op: StorageOp::Set,
            id: 9,
            scope: "scope".into(),
            key: "key".into(),
            value: vec![0; 100],
        };
        assert_eq!(
            storage.execute("alice", request, 16, 0).status,
            StorageStatus::Full
        );
        let keys = StorageRequest {
            op: StorageOp::Keys,
            id: 10,
            scope: "scope".into(),
            key: String::new(),
            value: vec![],
        };
        assert!(storage.execute("alice", keys, 16, 0).keys.is_empty());
    }
}
