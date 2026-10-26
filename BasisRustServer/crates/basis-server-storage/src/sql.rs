//! Optional SQL player storage: PostgreSQL remote, persistent SQLite fallback.
//! A remote connection is attempted only at startup; an in-flight outage returns
//! Unavailable rather than silently switching databases and losing read-your-writes.
use crate::player::{apply_request, quota_allows, PlayerRecord, RateLimiter};
use anyhow::{Context, Result};
use basis_protocol::storage::{
    StorageOp, StorageReply, StorageRequest, StorageStatus, MAX_BYTES_PER_PLAYER,
};
use parking_lot::Mutex;
use sqlx::{
    postgres::{PgPool, PgPoolOptions},
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions},
    Row,
};
use std::{path::Path, str::FromStr, sync::Arc, time::Duration};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqlBackend {
    PostgreSql,
    Sqlite,
}

#[derive(Clone, Debug)]
enum Pool {
    Postgres(PgPool),
    Sqlite(SqlitePool),
}

#[derive(Clone, Debug)]
pub struct SqlPlayerStorage {
    pool: Pool,
    table: String,
    limits: Arc<Mutex<RateLimiter>>,
}

impl SqlPlayerStorage {
    /// Remote postgres:// / postgresql:// URLs use TLS-capable sqlx. If absent
    /// or not connectable, use the durable local SQLite database instead.
    pub async fn connect(
        connection_string: &str,
        sqlite_path: Option<&Path>,
        table: &str,
    ) -> Result<(Self, SqlBackend)> {
        anyhow::ensure!(
            !table.is_empty() && table.len() <= 64 &&
            table.as_bytes().first().is_some_and(|b| b.is_ascii_alphabetic() || *b == b'_') &&
            table.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'),
            "StorageSqlTable must be a SQL identifier containing only letters, numbers and underscores"
        );
        if !connection_string.trim().is_empty()
            && (connection_string.starts_with("postgres://")
                || connection_string.starts_with("postgresql://"))
        {
            if let Ok(pool) = PgPoolOptions::new()
                .max_connections(5)
                .acquire_timeout(Duration::from_secs(3))
                .connect(connection_string)
                .await
            {
                let create = format!(
                        "CREATE TABLE IF NOT EXISTS {table} (owner TEXT PRIMARY KEY, payload BYTEA NOT NULL)"
                    );
                if sqlx::query(&create).execute(&pool).await.is_ok() {
                    return Ok((
                        Self {
                            pool: Pool::Postgres(pool),
                            table: table.to_owned(),
                            limits: Arc::new(Mutex::new(RateLimiter::default())),
                        },
                        SqlBackend::PostgreSql,
                    ));
                }
            }
        }

        let options = if let Some(path) = sqlite_path {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).with_context(|| {
                    format!("creating SQLite storage directory {}", parent.display())
                })?;
            }
            SqliteConnectOptions::new()
                .filename(path)
                .create_if_missing(true)
        } else {
            SqliteConnectOptions::from_str("sqlite::memory:")?
        }
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(Duration::from_secs(5));
        // One connection serializes writes (and prevents SQLite in-memory DB
        // inconsistency); SQLite's file locks cover multi-process access.
        let mut pool_options = SqlitePoolOptions::new().max_connections(1);
        if sqlite_path.is_none() {
            // Closing the last connection destroys an in-memory SQLite database.
            pool_options = pool_options
                .min_connections(1)
                .idle_timeout(None)
                .max_lifetime(None);
        }
        let pool = pool_options.connect_with(options).await?;
        let create = format!(
            "CREATE TABLE IF NOT EXISTS {table} (owner TEXT PRIMARY KEY, payload BLOB NOT NULL)"
        );
        sqlx::query(&create).execute(&pool).await?;
        Ok((
            Self {
                pool: Pool::Sqlite(pool),
                table: table.to_owned(),
                limits: Arc::new(Mutex::new(RateLimiter::default())),
            },
            SqlBackend::Sqlite,
        ))
    }

    pub async fn execute(
        &self,
        owner: &str,
        request: StorageRequest,
        per_player_limit: usize,
        total_limit: usize,
    ) -> StorageReply {
        let limit = per_player_limit.clamp(1, MAX_BYTES_PER_PLAYER);
        let fail = |status| StorageReply {
            op: request.op,
            id: request.id,
            status,
            used: 0,
            limit: limit as i32,
            value: Vec::new(),
            keys: Vec::new(),
        };
        if owner.is_empty() || owner.len() > 4096 {
            return fail(StorageStatus::Denied);
        }
        {
            let mut limits = self.limits.lock();
            let write = matches!(
                request.op,
                StorageOp::Set | StorageOp::Delete | StorageOp::Clear
            );
            if !limits.consume(owner, write) {
                return fail(StorageStatus::RateLimited);
            }
        }
        let result = match &self.pool {
            Pool::Postgres(pool) => {
                self.postgres_operation(pool, owner, request.clone(), limit, total_limit)
                    .await
            }
            Pool::Sqlite(pool) => {
                self.sqlite_operation(pool, owner, request.clone(), limit, total_limit)
                    .await
            }
        };
        match result {
            Ok(reply) => reply,
            Err(_) => fail(StorageStatus::Unavailable),
        }
    }

    async fn postgres_operation(
        &self,
        pool: &PgPool,
        owner: &str,
        request: StorageRequest,
        limit: usize,
        total_limit: usize,
    ) -> Result<StorageReply> {
        let mut tx = pool.begin().await?;
        // Protect global quota and avoid lost updates across multiple server
        // instances sharing one PostgreSQL table.
        let writing = matches!(
            request.op,
            StorageOp::Set | StorageOp::Delete | StorageOp::Clear
        );
        if writing {
            sqlx::query("SELECT pg_advisory_xact_lock(813411, 20261025)")
                .execute(&mut *tx)
                .await?;
        }
        let select = if writing {
            format!(
                "SELECT payload FROM {} WHERE owner = $1 FOR UPDATE",
                self.table
            )
        } else {
            format!("SELECT payload FROM {} WHERE owner = $1", self.table)
        };
        let data: Option<Vec<u8>> = sqlx::query(&select)
            .bind(owner)
            .fetch_optional(&mut *tx)
            .await?
            .map(|row| row.try_get("payload"))
            .transpose()?;
        let record = decode_record(data, owner)?;
        let (mut reply, updated) = apply_request(&record, request, limit);
        if let Some(updated) = updated {
            let payload = serde_json::to_vec(&updated)?;
            let query_total = format!(
                "SELECT COALESCE(SUM(octet_length(payload)), 0) AS used FROM {}",
                self.table
            );
            let global_used: i64 = sqlx::query(&query_total)
                .fetch_one(&mut *tx)
                .await?
                .try_get("used")?;
            let existing_size = self.postgres_previous_bytes(&mut tx, owner).await?;
            if !quota_allows(
                global_used as usize,
                (global_used as usize)
                    .saturating_sub(existing_size)
                    .saturating_add(payload.len()),
                total_limit,
            ) {
                reply.status = StorageStatus::Full;
                reply.used = record.used() as i32;
                return Ok(reply);
            }
            let upsert = format!("INSERT INTO {} (owner, payload) VALUES ($1, $2) ON CONFLICT(owner) DO UPDATE SET payload = EXCLUDED.payload", self.table);
            sqlx::query(&upsert)
                .bind(owner)
                .bind(payload)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(reply)
    }

    async fn postgres_previous_bytes(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        owner: &str,
    ) -> Result<usize> {
        let query = format!(
            "SELECT octet_length(payload) AS n FROM {} WHERE owner=$1",
            self.table
        );
        Ok(sqlx::query(&query)
            .bind(owner)
            .fetch_optional(&mut **tx)
            .await?
            .map(|r| r.try_get::<i32, _>("n").map(|n| n.max(0) as usize))
            .transpose()?
            .unwrap_or(0))
    }

    async fn sqlite_operation(
        &self,
        pool: &SqlitePool,
        owner: &str,
        request: StorageRequest,
        limit: usize,
        total_limit: usize,
    ) -> Result<StorageReply> {
        let mut tx = pool.begin().await?;
        let select = format!("SELECT payload FROM {} WHERE owner = ?1", self.table);
        let data: Option<Vec<u8>> = sqlx::query(&select)
            .bind(owner)
            .fetch_optional(&mut *tx)
            .await?
            .map(|row| row.try_get("payload"))
            .transpose()?;
        let record = decode_record(data, owner)?;
        let (mut reply, updated) = apply_request(&record, request, limit);
        if let Some(updated) = updated {
            let payload = serde_json::to_vec(&updated)?;
            let query_total = format!(
                "SELECT COALESCE(SUM(length(payload)), 0) AS used FROM {}",
                self.table
            );
            let global_used: i64 = sqlx::query(&query_total)
                .fetch_one(&mut *tx)
                .await?
                .try_get("used")?;
            let previous = format!(
                "SELECT length(payload) AS n FROM {} WHERE owner = ?1",
                self.table
            );
            let previous_size: usize = sqlx::query(&previous)
                .bind(owner)
                .fetch_optional(&mut *tx)
                .await?
                .map(|r| r.try_get::<i64, _>("n").map(|n| n.max(0) as usize))
                .transpose()?
                .unwrap_or(0);
            if !quota_allows(
                global_used as usize,
                (global_used as usize)
                    .saturating_sub(previous_size)
                    .saturating_add(payload.len()),
                total_limit,
            ) {
                reply.status = StorageStatus::Full;
                reply.used = record.used() as i32;
                return Ok(reply);
            }
            let upsert = format!("INSERT INTO {} (owner, payload) VALUES (?1, ?2) ON CONFLICT(owner) DO UPDATE SET payload=excluded.payload", self.table);
            sqlx::query(&upsert)
                .bind(owner)
                .bind(payload)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(reply)
    }

    pub fn backend(&self) -> SqlBackend {
        match self.pool {
            Pool::Postgres(_) => SqlBackend::PostgreSql,
            Pool::Sqlite(_) => SqlBackend::Sqlite,
        }
    }
}

fn decode_record(data: Option<Vec<u8>>, owner: &str) -> Result<PlayerRecord> {
    if let Some(bytes) = data {
        let record: PlayerRecord = serde_json::from_slice(&bytes)?;
        anyhow::ensure!(record.owner == owner, "player storage DID mismatch");
        Ok(record)
    } else {
        Ok(PlayerRecord {
            owner: owner.to_owned(),
            ..Default::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn req(op: StorageOp, value: Vec<u8>) -> StorageRequest {
        StorageRequest {
            op,
            id: 1,
            scope: "level".into(),
            key: "score".into(),
            value,
        }
    }

    #[tokio::test]
    async fn in_memory_sqlite_keeps_its_only_connection_alive() {
        let (storage, _) = SqlPlayerStorage::connect("", None, "basis_storage")
            .await
            .unwrap();
        let Pool::Sqlite(pool) = &storage.pool else {
            unreachable!()
        };
        assert_eq!(pool.options().get_min_connections(), 1);
        assert_eq!(pool.options().get_idle_timeout(), None);
        assert_eq!(pool.options().get_max_lifetime(), None);
        assert_eq!(
            storage
                .execute("alice", req(StorageOp::Set, vec![7]), 1024, 0)
                .await
                .status,
            StorageStatus::Ok
        );
        assert_eq!(
            storage
                .execute("alice", req(StorageOp::Get, vec![]), 1024, 0)
                .await
                .value,
            vec![7]
        );

        let dir = tempfile::tempdir().unwrap();
        let (disk, _) =
            SqlPlayerStorage::connect("", Some(&dir.path().join("store.db")), "basis_storage")
                .await
                .unwrap();
        let Pool::Sqlite(pool) = &disk.pool else {
            unreachable!()
        };
        assert!(pool.options().get_idle_timeout().is_some());
        assert!(pool.options().get_max_lifetime().is_some());
    }

    #[tokio::test]
    async fn lower_quotas_allow_sqlite_cleanup_without_allowing_growth() {
        let dir = tempfile::tempdir().unwrap();
        for path in [None, Some(dir.path().join("quota.db"))] {
            let (storage, _) = SqlPlayerStorage::connect("", path.as_deref(), "basis_storage")
                .await
                .unwrap();
            for global in [false, true] {
                let (per_player, total) = if global { (1024, 1) } else { (8, 0) };
                for op in [StorageOp::Delete, StorageOp::Clear, StorageOp::Set] {
                    let owner = format!("{global}-{op:?}");
                    for scope in ["one", "two"] {
                        let mut request = req(StorageOp::Set, vec![1; 64]);
                        request.scope = scope.into();
                        assert_eq!(
                            storage.execute(&owner, request, 1024, 0).await.status,
                            StorageStatus::Ok
                        );
                    }
                    let mut request = req(op, vec![1; 16]);
                    request.scope = "one".into();
                    let reduced = storage.execute(&owner, request, per_player, total).await;
                    assert_eq!(reduced.status, StorageStatus::Ok);
                    assert!(reduced.used > 8);
                    let mut request = req(StorageOp::Set, vec![1; 128]);
                    request.scope = "two".into();
                    let denied = storage.execute(&owner, request, per_player, total).await;
                    assert_eq!(denied.status, StorageStatus::Full);
                    assert_eq!(denied.used, reduced.used);
                    let mut request = req(StorageOp::Get, vec![]);
                    request.scope = "two".into();
                    assert_eq!(
                        storage
                            .execute(&owner, request, per_player, total)
                            .await
                            .value,
                        vec![1; 64]
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn sqlite_fallback_is_persistent_and_scoped_to_did() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("storage.sqlite3");
        let (storage, backend) = SqlPlayerStorage::connect("", Some(&path), "basis_storage")
            .await
            .unwrap();
        assert_eq!(backend, SqlBackend::Sqlite);
        assert_eq!(
            storage
                .execute("alice", req(StorageOp::Set, vec![5, 8]), 16384, 262144)
                .await
                .status,
            StorageStatus::Ok
        );
        assert_eq!(
            storage
                .execute("bob", req(StorageOp::Get, vec![]), 16384, 262144)
                .await
                .status,
            StorageStatus::NotFound
        );
        drop(storage);
        let (storage, _) = SqlPlayerStorage::connect("", Some(&path), "basis_storage")
            .await
            .unwrap();
        assert_eq!(
            storage
                .execute("alice", req(StorageOp::Get, vec![]), 16384, 262144)
                .await
                .value,
            vec![5, 8]
        );
        assert_eq!(
            storage
                .execute("alice", req(StorageOp::Delete, vec![]), 16384, 262144)
                .await
                .status,
            StorageStatus::Ok
        );
        assert_eq!(
            storage
                .execute("alice", req(StorageOp::Keys, vec![]), 16384, 262144)
                .await
                .keys,
            Vec::<String>::new()
        );
    }

    #[tokio::test]
    async fn unreachable_remote_falls_back_to_sqlite() {
        let (storage, backend) = SqlPlayerStorage::connect(
            "postgres://user:secret@127.0.0.1:1/db",
            None,
            "basis_storage",
        )
        .await
        .unwrap();
        assert_eq!(backend, SqlBackend::Sqlite);
        assert_eq!(storage.backend(), SqlBackend::Sqlite);
        assert_eq!(
            storage
                .execute("alice", req(StorageOp::Set, vec![1]), 16384, 262144)
                .await
                .status,
            StorageStatus::Ok
        );
    }

    #[tokio::test]
    async fn invalid_table_name_is_rejected() {
        assert!(SqlPlayerStorage::connect("", None, "a; DROP TABLE users")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn quotas_prevent_writing_and_preserve_old_data() {
        let (storage, _) = SqlPlayerStorage::connect("", None, "basis_storage")
            .await
            .unwrap();
        assert_eq!(
            storage
                .execute("alice", req(StorageOp::Set, vec![2, 3]), 16384, 262144)
                .await
                .status,
            StorageStatus::Ok
        );
        assert_eq!(
            storage
                .execute("alice", req(StorageOp::Set, vec![8; 100]), 32, 262144)
                .await
                .status,
            StorageStatus::Full
        );
        assert_eq!(
            storage
                .execute("alice", req(StorageOp::Get, vec![]), 16384, 262144)
                .await
                .value,
            vec![2, 3]
        );
    }
}
