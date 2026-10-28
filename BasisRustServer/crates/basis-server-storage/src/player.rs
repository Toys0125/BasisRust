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
    time::Instant,
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

    fn storage_bytes(&self) -> usize {
        if self.scopes.is_empty() {
            0
        } else {
            serde_json::to_vec(self)
                .expect("player record contains only strings and bytes")
                .len()
        }
    }
}

/// Existing over-quota accounts can recover by reducing their usage.
pub(crate) fn quota_allows(current: usize, next: usize, limit: usize) -> bool {
    limit == 0 || next <= limit || next < current
}

#[derive(Debug)]
pub(crate) struct RateBudget {
    last: Instant,
    reads: f64,
    writes: f64,
    bytes: f64,
    byte_limit: usize,
}

impl RateBudget {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            last: Instant::now(),
            reads: 60.0,
            writes: 10.0,
            bytes: (limit * 2) as f64,
            byte_limit: limit,
        }
    }
    fn fully_refilled(&self, now: Instant) -> bool {
        let delta = now.saturating_duration_since(self.last).as_secs_f64();
        self.reads + delta * 20.0 >= 60.0
            && self.writes + delta * 2.0 >= 10.0
            && self.bytes + delta * self.byte_limit as f64 / 2.0 >= (self.byte_limit * 2) as f64
    }

    fn consume(&mut self, write: bool, bytes: usize, limit: usize) -> bool {
        self.consume_at(write, bytes, limit, Instant::now())
    }

    fn consume_at(&mut self, write: bool, bytes: usize, limit: usize, now: Instant) -> bool {
        let delta = now.saturating_duration_since(self.last).as_secs_f64();
        self.last = now;
        self.reads = (self.reads + delta * 20.0).min(60.0);
        self.writes = (self.writes + delta * 2.0).min(10.0);
        let old_capacity = (self.byte_limit * 2) as f64;
        self.bytes = (self.bytes + delta * self.byte_limit as f64 / 2.0).min(old_capacity);
        let capacity = (limit * 2) as f64;
        // Preserve outstanding byte debt across live quota changes. A completely
        // refilled bucket can adopt the new capacity without creating a bypass.
        self.bytes = if self.bytes >= old_capacity {
            capacity
        } else {
            self.bytes.min(capacity)
        };
        self.byte_limit = limit;
        let available = if write {
            &mut self.writes
        } else {
            &mut self.reads
        };
        // A shrinking write after a quota reduction can exceed the entire new
        // bucket. Admit one from a full bucket and retain its complete byte debt,
        // rather than making it impossible forever or undercharging its payload.
        let oversized_from_full = bytes as f64 > capacity && self.bytes == capacity;
        if *available < 1.0
            || (write && bytes > 0 && self.bytes < bytes as f64 && !oversized_from_full)
        {
            return false;
        }
        *available -= 1.0;
        if write {
            self.bytes -= bytes as f64;
        }
        true
    }
}

#[derive(Debug)]
pub(crate) struct RateLimiter {
    owners: HashMap<String, RateBudget>,
    overflow: RateBudget,
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self {
            owners: HashMap::new(),
            overflow: RateBudget::new(DEFAULT_BYTES_PER_PLAYER),
        }
    }
}

impl RateLimiter {
    const MAX_OWNERS: usize = 8192;

    pub(crate) fn consume(&mut self, owner: &str, write: bool, bytes: usize, limit: usize) -> bool {
        if let Some(budget) = self.owners.get_mut(owner) {
            return budget.consume(write, bytes, limit);
        }
        let now = Instant::now();
        // An untracked owner may already owe tokens to the shared allowance.
        // Do not promote it to a fresh bucket until that debt has refilled.
        if self.overflow.fully_refilled(now) {
            if self.owners.len() >= Self::MAX_OWNERS {
                self.owners.retain(|_, budget| !budget.fully_refilled(now));
            }
            if self.owners.len() < Self::MAX_OWNERS {
                return self
                    .owners
                    .entry(owner.to_owned())
                    .or_insert_with(|| RateBudget::new(limit))
                    .consume(write, bytes, limit);
            }
        }
        // Keep memory bounded without permanently excluding new owners when
        // all tracked buckets are busy. Sharing can only tighten their limits.
        self.overflow.consume(write, bytes, limit)
    }
}

#[derive(Debug, Default)]
struct State {
    // Only the memory backend retains records; files are read on demand.
    records: HashMap<String, PlayerRecord>,
    memory_bytes: usize,
    budgets: RateLimiter,
}

#[derive(Debug, Clone)]
pub struct PlayerStorage {
    root: Option<PathBuf>,
    file_lock: Option<Arc<fs::File>>,
    state: Arc<Mutex<State>>,
}

#[derive(Serialize, Deserialize)]
struct FileUsage {
    used: usize,
    dirty: bool,
}

struct PlayerFileGuard<'a>(&'a fs::File);

impl Drop for PlayerFileGuard<'_> {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}

impl PlayerStorage {
    pub fn new(root: Option<PathBuf>) -> Result<Self> {
        if let Some(root) = &root {
            fs::create_dir_all(root)
                .with_context(|| format!("creating player storage {}", root.display()))?;
        }
        let file_lock = root
            .as_ref()
            .map(|root| {
                fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(root.join(".lock"))
                    .map(Arc::new)
            })
            .transpose()?;
        let storage = Self {
            root,
            file_lock,
            state: Arc::new(Mutex::new(State::default())),
        };
        if storage.root.is_some() {
            let _guard = storage.lock_files()?;
            storage.reconcile_files()?;
        }
        Ok(storage)
    }

    fn lock_files(&self) -> Result<Option<PlayerFileGuard<'_>>> {
        self.file_lock
            .as_ref()
            .map(|file| {
                file.lock().context("locking player storage directory")?;
                Ok(PlayerFileGuard(file))
            })
            .transpose()
    }

    fn write_file_usage(&self, used: usize, dirty: bool) -> Result<()> {
        let path = self
            .root
            .as_ref()
            .expect("file storage root")
            .join(".usage");
        let bytes = serde_json::to_vec(&FileUsage { used, dirty })?;
        super::atomic_write_with(
            &path,
            |file| std::io::Write::write_all(file, &bytes),
            super::sync_directory,
        )
    }

    /// Reconcile once at startup, or recover an interrupted record/ledger update.
    /// Callers hold the directory's OS lock throughout reconciliation and writes.
    fn reconcile_files(&self) -> Result<usize> {
        let root = self.root.as_ref().expect("file storage root");
        let mut used = 0usize;
        for entry in fs::read_dir(root)? {
            let path = entry?.path();
            if !path.extension().is_some_and(|ext| ext == "json") {
                continue;
            }
            let bytes = fs::read(&path)?;
            if let Ok(record) = serde_json::from_slice::<PlayerRecord>(&bytes) {
                if record.scopes.is_empty() && self.file_for(&record.owner).as_ref() == Some(&path)
                {
                    fs::remove_file(&path)?;
                    continue;
                }
            }
            used = used
                .checked_add(bytes.len())
                .context("player storage size overflow")?;
        }
        super::sync_directory(root)?;
        self.write_file_usage(used, false)?;
        Ok(used)
    }

    fn file_usage(&self) -> Result<usize> {
        let path = self
            .root
            .as_ref()
            .expect("file storage root")
            .join(".usage");
        match fs::read(path) {
            Ok(bytes) => {
                if let Ok(FileUsage { used, dirty: false }) = serde_json::from_slice(&bytes) {
                    return Ok(used);
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        self.reconcile_files()
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
        let bytes = if record.scopes.is_empty() {
            Vec::new()
        } else {
            serde_json::to_vec(record)?
        };
        let used = self.file_usage()?;
        let old = match fs::metadata(&path) {
            Ok(metadata) => usize::try_from(metadata.len())?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(error.into()),
        };
        let next = used
            .checked_sub(old)
            .context("player storage accounting mismatch")?
            .checked_add(bytes.len())
            .context("player storage size overflow")?;
        anyhow::ensure!(
            quota_allows(used, next, max_total_bytes),
            "global player storage quota exceeded"
        );
        // Persist the recovery marker before changing data. If any step fails or
        // the process exits, the next operation rebuilds the ledger from files.
        self.write_file_usage(used, true)?;
        if bytes.is_empty() {
            match fs::remove_file(&path) {
                Ok(()) => super::sync_directory(self.root.as_ref().expect("file storage root"))?,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        } else {
            super::atomic_write_with(
                &path,
                |file| std::io::Write::write_all(file, &bytes),
                super::sync_directory,
            )?;
        }
        self.write_file_usage(next, false)
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
        let write = matches!(
            request.op,
            StorageOp::Set | StorageOp::Delete | StorageOp::Clear
        );
        let bytes = if request.op == StorageOp::Set {
            request.value.len()
        } else {
            0
        };
        if !state.budgets.consume(owner, write, bytes, limit) {
            reply.status = StorageStatus::RateLimited;
            return reply;
        }
        let _file_guard = match self.lock_files() {
            Ok(guard) => guard,
            Err(_) => {
                reply.status = StorageStatus::Unavailable;
                return reply;
            }
        };
        let record = match state.records.get(owner) {
            Some(record) => record.clone(),
            None => match self.read_account(owner) {
                Ok(record) => record,
                Err(_) => {
                    reply.status = StorageStatus::Unavailable;
                    return reply;
                }
            },
        };
        let (reply_for_request, updated) = apply_request(&record, request, limit);
        reply = reply_for_request;
        if let Some(next) = updated {
            if self.root.is_some() {
                if let Err(error) = self.save_account(owner, &next, total_limit) {
                    reply.status = if error.to_string().contains("quota exceeded") {
                        StorageStatus::Full
                    } else {
                        StorageStatus::Unavailable
                    };
                    reply.used = record.used() as i32;
                }
            } else {
                let next_bytes = state
                    .memory_bytes
                    .saturating_sub(record.storage_bytes())
                    .saturating_add(next.storage_bytes());
                if !quota_allows(state.memory_bytes, next_bytes, total_limit) {
                    reply.status = StorageStatus::Full;
                    reply.used = record.used() as i32;
                    return reply;
                }
                state.memory_bytes = next_bytes;
                if next.scopes.is_empty() {
                    state.records.remove(owner);
                } else {
                    state.records.insert(owner.to_owned(), next);
                }
            }
        }
        reply
    }

    /// Operator-only access, serialized with client requests and file accounting.
    pub fn admin(
        &self,
        action: crate::admin::StorageAdminAction,
    ) -> Result<crate::admin::StorageAdminReply> {
        use crate::admin::{StorageAdminAction, StorageAdminReply};
        action.validate()?;
        let mut state = self.state.lock();
        let _guard = self.lock_files()?;
        match action {
            StorageAdminAction::Statistics => {
                let (backend, records, bytes) = if let Some(root) = &self.root {
                    let bytes = self.file_usage()? as u64;
                    let mut records = 0;
                    for entry in fs::read_dir(root)? {
                        let entry = entry?;
                        if entry.path().extension().is_some_and(|ext| ext == "json")
                            && entry.file_type()?.is_file()
                        {
                            records += 1;
                        }
                    }
                    ("file", records, bytes)
                } else {
                    (
                        "memory",
                        state.records.len() as u64,
                        state.memory_bytes as u64,
                    )
                };
                Ok(StorageAdminReply::Statistics {
                    backend,
                    records,
                    bytes,
                })
            }
            StorageAdminAction::Inspect(owner) => {
                let record = match state.records.get(&owner) {
                    Some(record) => record.clone(),
                    None => self.read_account(&owner)?,
                };
                Ok(StorageAdminReply::Inspection {
                    owner,
                    record: record.into(),
                })
            }
            StorageAdminAction::Wipe(owner) => {
                if self.root.is_some() {
                    // Do not decode the old record: operators can wipe corrupt saves.
                    self.save_account(&owner, &PlayerRecord::default(), 0)?;
                } else if let Some(record) = state.records.remove(&owner) {
                    state.memory_bytes -= record.storage_bytes();
                }
                Ok(StorageAdminReply::Wiped { owner })
            }
        }
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
            if next.scopes.remove(&request.scope).is_none() {
                return (reply, None);
            }
        }
        _ => unreachable!(),
    }
    if !quota_allows(record.used(), next.used(), limit) {
        reply.status = StorageStatus::Full;
        return (reply, None);
    }
    reply.used = next.used() as i32;
    (reply, Some(next))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn disk_bytes(storage: &PlayerStorage) -> usize {
        fs::read_dir(storage.root.as_ref().unwrap())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
            .map(|path| fs::metadata(path).unwrap().len() as usize)
            .sum()
    }

    #[test]
    fn file_accounting_serializes_independent_instances_and_removes_empty_accounts() {
        let dir = tempfile::tempdir().unwrap();
        let first = PlayerStorage::new(Some(dir.path().to_path_buf())).unwrap();
        assert_eq!(
            first
                .execute("alice", request(StorageOp::Set, "one", 64), 1024, 0)
                .status,
            StorageStatus::Ok
        );
        let size = disk_bytes(&first);
        assert_eq!(
            first
                .execute("alice", request(StorageOp::Clear, "one", 0), 1024, 0)
                .status,
            StorageStatus::Ok
        );
        assert!(!first.file_for("alice").unwrap().exists());
        assert_eq!(first.file_usage().unwrap(), 0);
        let second = PlayerStorage::new(Some(dir.path().to_path_buf())).unwrap();
        let a = first.clone();
        let b = second.clone();
        let one = std::thread::spawn(move || {
            a.execute("alice", request(StorageOp::Set, "one", 64), 1024, size)
        });
        let two = std::thread::spawn(move || {
            b.execute("bobby", request(StorageOp::Set, "one", 64), 1024, size)
        });
        let statuses = [one.join().unwrap().status, two.join().unwrap().status];
        assert_eq!(
            statuses.iter().filter(|s| **s == StorageStatus::Ok).count(),
            1
        );
        assert_eq!(
            statuses
                .iter()
                .filter(|s| **s == StorageStatus::Full)
                .count(),
            1
        );
        assert_eq!(first.file_usage().unwrap(), size);
        assert_eq!(disk_bytes(&first), size);
        for owner in ["alice", "bobby"] {
            first.execute(owner, request(StorageOp::Clear, "one", 0), 1024, 0);
        }
        assert_eq!(disk_bytes(&first), 0);
        assert_eq!(second.file_usage().unwrap(), 0);
    }

    #[test]
    fn file_ledger_recovers_interrupted_updates_and_cleans_legacy_empty_records() {
        let dir = tempfile::tempdir().unwrap();
        let storage = PlayerStorage::new(Some(dir.path().to_path_buf())).unwrap();
        storage.execute("alice", request(StorageOp::Set, "one", 64), 1024, 0);
        storage
            .write_file_usage(storage.file_usage().unwrap(), true)
            .unwrap();
        fs::remove_file(storage.file_for("alice").unwrap()).unwrap();
        assert_eq!(
            storage
                .execute("bobby", request(StorageOp::Set, "one", 16), 1024, 1000)
                .status,
            StorageStatus::Ok
        );
        assert_eq!(storage.file_usage().unwrap(), disk_bytes(&storage));
        let empty = PlayerRecord {
            owner: "legacy".into(),
            ..Default::default()
        };
        fs::write(
            storage.file_for("legacy").unwrap(),
            serde_json::to_vec(&empty).unwrap(),
        )
        .unwrap();
        let restarted = PlayerStorage::new(Some(dir.path().to_path_buf())).unwrap();
        assert!(!restarted.file_for("legacy").unwrap().exists());
        assert_eq!(restarted.file_usage().unwrap(), disk_bytes(&restarted));
        assert_eq!(
            restarted
                .execute("bobby", request(StorageOp::Get, "one", 0), 1024, 1000)
                .value,
            vec![1; 16]
        );
    }

    #[test]
    fn byte_tokens_refill_and_do_not_block_reads_or_cleanup() {
        let limit = DEFAULT_BYTES_PER_PLAYER;
        let mut budget = RateBudget::new(limit);
        let now = budget.last;
        assert!(budget.consume_at(true, 16_300, limit, now));
        assert!(budget.consume_at(true, 16_300, limit, now));
        assert!(!budget.consume_at(true, 16_300, limit, now));
        assert_eq!(
            budget.writes, 8.0,
            "rejections must not consume operation tokens"
        );
        assert!(budget.consume_at(false, 0, limit, now));
        assert!(budget.consume_at(true, 0, limit, now));
        assert!(budget.consume_at(true, 8_192, limit, now + Duration::from_secs(1)));
        assert!(!budget.consume_at(true, 16_300, limit, now + Duration::from_secs(1)));
        assert!(!budget.fully_refilled(now + Duration::from_secs(1)));
    }

    #[test]
    fn oversized_writes_keep_full_byte_debt_and_eventually_refill() {
        let mut budget = RateBudget::new(16);
        let now = budget.last;
        assert!(budget.consume_at(true, 100, 16, now));
        assert_eq!(budget.bytes, -68.0);
        assert!(!budget.consume_at(true, 100, 16, now));
        assert!(
            budget.consume_at(true, 0, 16, now),
            "cleanup must not require byte tokens"
        );
        assert!(budget.consume_at(false, 0, 16, now));
        assert!(!budget.consume_at(true, 100, 16, now + Duration::from_secs(12)));
        assert!(budget.consume_at(true, 100, 16, now + Duration::from_secs(13)));
    }

    #[test]
    fn quota_changes_and_owner_churn_preserve_byte_debt() {
        let limit = DEFAULT_BYTES_PER_PLAYER;
        let mut limiter = RateLimiter::default();
        for owner in 0..RateLimiter::MAX_OWNERS {
            let mut budget = RateBudget::new(limit);
            assert!(budget.consume(true, limit * 2, limit));
            limiter.owners.insert(owner.to_string(), budget);
        }
        assert!(limiter.consume("overflow", true, limit * 2, limit));
        assert!(!limiter.consume("overflow", true, 1, limit));
        limiter.owners.remove("0");
        assert!(!limiter.consume("overflow", true, 1, limit));
        assert!(!limiter.owners.contains_key("overflow"));
        assert!(
            !limiter.consume("1", true, limit, limit * 2),
            "raising a quota must not erase debt"
        );
        limiter.overflow.last = Instant::now() - Duration::from_secs(5);
        assert!(limiter.consume("overflow", true, 1, limit));
        assert!(limiter.owners.contains_key("overflow"));
    }

    #[test]
    fn byte_limited_overwrites_and_admin_operations_cover_memory_and_files() {
        use crate::admin::{StorageAdminAction as Action, StorageAdminReply as Reply};
        let dir = tempfile::tempdir().unwrap();
        for root in [None, Some(dir.path().to_path_buf())] {
            let storage = PlayerStorage::new(root).unwrap();
            for _ in 0..2 {
                assert_eq!(
                    storage
                        .execute("alice", request(StorageOp::Set, "one", 16_300), 16_384, 0)
                        .status,
                    StorageStatus::Ok
                );
            }
            assert_eq!(
                storage
                    .execute("alice", request(StorageOp::Set, "one", 16_300), 16_384, 0)
                    .status,
                StorageStatus::RateLimited
            );
            assert_eq!(
                storage
                    .execute("alice", request(StorageOp::Get, "one", 0), 16_384, 0)
                    .value,
                vec![1; 16_300]
            );
            assert_eq!(
                storage
                    .execute("bobby", request(StorageOp::Set, "other", 16), 16_384, 0)
                    .status,
                StorageStatus::Ok
            );
            let Reply::Inspection { record, .. } =
                storage.admin(Action::Inspect("alice".into())).unwrap()
            else {
                panic!()
            };
            assert_eq!(record.scopes["one"]["key"], 16_300);
            assert!(matches!(
                storage.admin(Action::Statistics).unwrap(),
                Reply::Statistics { records: 2, .. }
            ));
            storage.admin(Action::Wipe("alice".into())).unwrap();
            assert!(matches!(
                storage.admin(Action::Statistics).unwrap(),
                Reply::Statistics { records: 1, .. }
            ));
            assert_eq!(
                storage
                    .execute("bobby", request(StorageOp::Get, "other", 0), 16_384, 0)
                    .value,
                vec![1; 16]
            );
            storage.admin(Action::Wipe("bobby".into())).unwrap();
            assert!(matches!(
                storage.admin(Action::Statistics).unwrap(),
                Reply::Statistics {
                    records: 0,
                    bytes: 0,
                    ..
                }
            ));
            assert_eq!(
                storage
                    .execute("alice", request(StorageOp::Set, "one", 16_300), 16_384, 0)
                    .status,
                StorageStatus::RateLimited,
                "operator wipe must not reset client budgets"
            );
            assert_eq!(
                storage
                    .execute("shrinking", request(StorageOp::Set, "one", 128), 1024, 0)
                    .status,
                StorageStatus::Ok
            );
            assert_eq!(
                storage
                    .execute("shrinking", request(StorageOp::Set, "one", 64), 8, 0)
                    .status,
                StorageStatus::Ok,
                "an above-burst shrinking write must remain possible after a quota reduction"
            );
            assert_eq!(
                storage
                    .execute("shrinking", request(StorageOp::Clear, "one", 0), 8, 0)
                    .status,
                StorageStatus::Ok
            );
            if let Some(path) = storage.file_for("corrupt") {
                fs::write(path, b"not JSON").unwrap();
                storage.write_file_usage(8, false).unwrap();
                assert!(storage.admin(Action::Inspect("corrupt".into())).is_err());
                storage.admin(Action::Wipe("corrupt".into())).unwrap();
                assert!(matches!(
                    storage.admin(Action::Statistics).unwrap(),
                    Reply::Statistics {
                        records: 0,
                        bytes: 0,
                        ..
                    }
                ));
            }
            assert!(storage.admin(Action::Wipe(String::new())).is_err());
        }
    }

    #[test]
    fn full_budget_bookkeeping_admits_new_owners_without_resetting_active_limits() {
        let mut budgets = RateLimiter::default();
        for owner in 0..8192 {
            budgets
                .owners
                .insert(owner.to_string(), RateBudget::new(DEFAULT_BYTES_PER_PLAYER));
        }
        assert!(budgets.consume("new", false, 0, DEFAULT_BYTES_PER_PLAYER));
        assert!(budgets.owners.len() <= RateLimiter::MAX_OWNERS);
        for _ in 0..59 {
            assert!(budgets.consume("new", false, 0, DEFAULT_BYTES_PER_PLAYER));
        }
        assert!(!budgets.consume("new", false, 0, DEFAULT_BYTES_PER_PLAYER));
    }

    #[test]
    fn owner_churn_preserves_depleted_budgets_and_shared_overflow_debt() {
        let mut limiter = RateLimiter::default();
        for owner in 0..RateLimiter::MAX_OWNERS {
            limiter.owners.insert(
                owner.to_string(),
                RateBudget {
                    last: Instant::now(),
                    reads: 0.0,
                    writes: 0.0,
                    bytes: 0.0,
                    byte_limit: DEFAULT_BYTES_PER_PLAYER,
                },
            );
        }
        assert!(!limiter.consume("0", true, 0, DEFAULT_BYTES_PER_PLAYER));
        for owner in RateLimiter::MAX_OWNERS..RateLimiter::MAX_OWNERS + 10 {
            assert!(limiter.consume(&owner.to_string(), true, 0, DEFAULT_BYTES_PER_PLAYER));
        }
        assert!(!limiter.consume("another", true, 0, DEFAULT_BYTES_PER_PLAYER));
        assert!(
            !limiter.consume("0", true, 0, DEFAULT_BYTES_PER_PLAYER),
            "churn reset a depleted owner"
        );
        assert_eq!(limiter.owners.len(), RateLimiter::MAX_OWNERS);

        // A free slot must not give an overflow owner a fresh write allowance.
        limiter.owners.remove("1");
        let overflow_owner = RateLimiter::MAX_OWNERS.to_string();
        assert!(!limiter.consume(&overflow_owner, true, 0, DEFAULT_BYTES_PER_PLAYER));
        assert!(!limiter.owners.contains_key(&overflow_owner));
        limiter.overflow.last = Instant::now() - Duration::from_secs(6);
        assert!(limiter.consume(&overflow_owner, true, 0, DEFAULT_BYTES_PER_PLAYER));
        assert!(limiter.owners.contains_key(&overflow_owner));
    }

    #[test]
    fn evicts_only_budgets_whose_read_and_write_tokens_have_refilled() {
        let mut limiter = RateLimiter::default();
        for owner in 0..RateLimiter::MAX_OWNERS {
            limiter.owners.insert(
                owner.to_string(),
                RateBudget {
                    last: Instant::now(),
                    reads: 0.0,
                    writes: 0.0,
                    bytes: 0.0,
                    byte_limit: DEFAULT_BYTES_PER_PLAYER,
                },
            );
        }
        let refill_time = Instant::now() - Duration::from_secs(6);
        limiter.owners.get_mut("0").unwrap().last = refill_time;
        assert!(limiter.consume("new", true, 0, DEFAULT_BYTES_PER_PLAYER));
        assert!(!limiter.owners.contains_key("0"));
        assert!(limiter.owners.contains_key("1"));
        assert_eq!(limiter.owners.len(), RateLimiter::MAX_OWNERS);
    }

    fn request(op: StorageOp, scope: &str, bytes: usize) -> StorageRequest {
        StorageRequest {
            op,
            id: 1,
            scope: scope.into(),
            key: "key".into(),
            value: vec![1; bytes],
        }
    }

    #[test]
    fn memory_global_quota_preserves_data_and_reclaims_cleared_accounts() {
        let storage = PlayerStorage::new(None).unwrap();
        assert_eq!(
            storage
                .execute("alice", request(StorageOp::Set, "one", 64), 1024, 0)
                .status,
            StorageStatus::Ok
        );
        let total = storage.state.lock().memory_bytes;
        let full = storage.execute("bob", request(StorageOp::Set, "one", 64), 1024, total);
        assert_eq!(full.status, StorageStatus::Full);
        assert_eq!(full.used, 0);
        assert_eq!(storage.state.lock().records.len(), 1);
        assert_eq!(
            storage
                .execute("alice", request(StorageOp::Get, "one", 0), 1024, total)
                .value,
            vec![1; 64]
        );
        assert_eq!(
            storage
                .execute("alice", request(StorageOp::Clear, "one", 0), 1024, total)
                .status,
            StorageStatus::Ok
        );
        assert_eq!(storage.state.lock().memory_bytes, 0);
        assert!(storage.state.lock().records.is_empty());
        assert_eq!(
            storage
                .execute("bob", request(StorageOp::Set, "one", 64), 1024, total)
                .status,
            StorageStatus::Ok
        );
    }

    #[test]
    fn lower_player_quota_allows_delete_clear_and_smaller_sets() {
        let dir = tempfile::tempdir().unwrap();
        for root in [None, Some(dir.path().to_path_buf())] {
            let storage = PlayerStorage::new(root).unwrap();
            for op in [StorageOp::Delete, StorageOp::Clear, StorageOp::Set] {
                let owner = format!("{op:?}");
                for scope in ["one", "two"] {
                    assert_eq!(
                        storage
                            .execute(&owner, request(StorageOp::Set, scope, 64), 1024, 0)
                            .status,
                        StorageStatus::Ok
                    );
                }
                let reduced = storage.execute(&owner, request(op, "one", 16), 8, 0);
                assert_eq!(reduced.status, StorageStatus::Ok);
                assert!(reduced.used > 8, "cleanup must work while still over quota");
                let denied = storage.execute(&owner, request(StorageOp::Set, "three", 0), 8, 0);
                assert_eq!(denied.status, StorageStatus::Full);
                assert_eq!(denied.used, reduced.used);
                assert_eq!(
                    storage
                        .execute(&owner, request(StorageOp::Get, "two", 0), 8, 0)
                        .value,
                    vec![1; 64]
                );
            }
        }
    }

    #[test]
    fn lower_global_quota_allows_cleanup_without_allowing_growth() {
        let dir = tempfile::tempdir().unwrap();
        for root in [None, Some(dir.path().to_path_buf())] {
            let storage = PlayerStorage::new(root).unwrap();
            for op in [StorageOp::Delete, StorageOp::Clear, StorageOp::Set] {
                let owner = format!("{op:?}");
                for scope in ["one", "two"] {
                    assert_eq!(
                        storage
                            .execute(&owner, request(StorageOp::Set, scope, 64), 1024, 0)
                            .status,
                        StorageStatus::Ok
                    );
                }
                let reduced = storage.execute(&owner, request(op, "one", 16), 1024, 1);
                assert_eq!(reduced.status, StorageStatus::Ok);
                let denied = storage.execute(&owner, request(StorageOp::Set, "two", 128), 1024, 1);
                assert_eq!(denied.status, StorageStatus::Full);
                assert_eq!(denied.used, reduced.used);
                assert_eq!(
                    storage
                        .execute(&owner, request(StorageOp::Get, "two", 0), 1024, 1)
                        .value,
                    vec![1; 64]
                );
            }
        }
    }

    #[test]
    fn reads_do_not_retain_accounts_and_files_are_loaded_on_demand() {
        let dir = tempfile::tempdir().unwrap();
        for root in [None, Some(dir.path().to_path_buf())] {
            let storage = PlayerStorage::new(root).unwrap();
            for owner in 0..64 {
                assert_eq!(
                    storage
                        .execute(
                            &owner.to_string(),
                            request(StorageOp::Get, "one", 0),
                            1024,
                            0
                        )
                        .status,
                    StorageStatus::NotFound
                );
            }
            assert!(storage.state.lock().records.is_empty());
        }
        let first = PlayerStorage::new(Some(dir.path().to_path_buf())).unwrap();
        let second = PlayerStorage::new(Some(dir.path().to_path_buf())).unwrap();
        assert_eq!(
            first
                .execute("alice", request(StorageOp::Set, "one", 16), 1024, 0)
                .status,
            StorageStatus::Ok
        );
        assert_eq!(
            second
                .execute("alice", request(StorageOp::Get, "one", 0), 1024, 0)
                .value,
            vec![1; 16]
        );
        assert_eq!(
            first
                .execute("alice", request(StorageOp::Set, "one", 32), 1024, 0)
                .status,
            StorageStatus::Ok
        );
        assert_eq!(
            second
                .execute("alice", request(StorageOp::Get, "one", 0), 1024, 0)
                .value,
            vec![1; 32]
        );
        assert!(first.state.lock().records.is_empty());
        assert!(second.state.lock().records.is_empty());
    }

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
