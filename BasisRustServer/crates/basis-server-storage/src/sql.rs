//! Optional SQL player storage: PostgreSQL remote, persistent SQLite fallback.
//! A remote connection is attempted only at startup; an in-flight outage returns
//! Unavailable rather than silently switching databases and losing read-your-writes.
mod accounting;
mod legacy;
pub use legacy::LegacyImportReport;

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
    usage_table: String,
    limits: Arc<Mutex<RateLimiter>>,
    legacy_import: LegacyImportReport,
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
            !table.is_empty() && table.len() <= 63 &&
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
                accounting::init_postgres(&pool, table).await.context(
                    "PostgreSQL connected, but player-storage accounting initialization failed; verify table, function, and trigger permissions"
                )?;
                return Ok((
                    Self {
                        pool: Pool::Postgres(pool),
                        table: table.to_owned(),
                        usage_table: accounting::usage_table(table),
                        limits: Arc::new(Mutex::new(RateLimiter::default())),
                        legacy_import: LegacyImportReport::default(),
                    },
                    SqlBackend::PostgreSql,
                ));
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
        accounting::init_sqlite(&pool, table).await?;
        let legacy_import = if let Some(parent) = sqlite_path.and_then(Path::parent) {
            legacy::import(&pool, table, parent.join("players"))
                .await
                .context("SQLite legacy BSTR migration failed; source files were left unchanged")?
        } else {
            LegacyImportReport::default()
        };
        Ok((
            Self {
                pool: Pool::Sqlite(pool),
                table: table.to_owned(),
                usage_table: accounting::usage_table(table),
                limits: Arc::new(Mutex::new(RateLimiter::default())),
                legacy_import,
            },
            SqlBackend::Sqlite,
        ))
    }

    pub fn legacy_import_report(&self) -> LegacyImportReport {
        self.legacy_import
    }

    /// Local operator access; mutations use the same locks and accounting as writes.
    pub async fn admin(
        &self,
        action: crate::admin::StorageAdminAction,
    ) -> Result<crate::admin::StorageAdminReply> {
        use crate::admin::{StorageAdminAction, StorageAdminReply};
        action.validate()?;
        match action {
            StorageAdminAction::Statistics => {
                // One statement gives the count and counter a consistent snapshot.
                let query = format!(
                    "SELECT COUNT(*) AS records, (SELECT used FROM {} WHERE id=1) AS bytes FROM {}",
                    self.usage_table, self.table
                );
                let (backend, records, bytes): (&'static str, i64, i64) = match &self.pool {
                    Pool::Postgres(pool) => {
                        let row = sqlx::query(&query).fetch_one(pool).await?;
                        ("postgresql", row.try_get("records")?, row.try_get("bytes")?)
                    }
                    Pool::Sqlite(pool) => {
                        let row = sqlx::query(&query).fetch_one(pool).await?;
                        ("sqlite", row.try_get("records")?, row.try_get("bytes")?)
                    }
                };
                Ok(StorageAdminReply::Statistics {
                    backend,
                    records: records.try_into()?,
                    bytes: bytes.try_into()?,
                })
            }
            StorageAdminAction::Inspect(owner) => {
                let data: Option<Vec<u8>> = match &self.pool {
                    Pool::Postgres(pool) => {
                        sqlx::query_scalar(&format!(
                            "SELECT payload FROM {} WHERE owner=$1",
                            self.table
                        ))
                        .bind(&owner)
                        .fetch_optional(pool)
                        .await?
                    }
                    Pool::Sqlite(pool) => {
                        sqlx::query_scalar(&format!(
                            "SELECT payload FROM {} WHERE owner=?1",
                            self.table
                        ))
                        .bind(&owner)
                        .fetch_optional(pool)
                        .await?
                    }
                };
                let record = decode_record(data, &owner)?;
                Ok(StorageAdminReply::Inspection {
                    owner,
                    record: record.into(),
                })
            }
            StorageAdminAction::Wipe(owner) => {
                // Delete without decoding, so corrupt data can also be removed.
                match &self.pool {
                    Pool::Postgres(pool) => {
                        let mut tx = pool.begin().await?;
                        sqlx::query("SELECT pg_advisory_xact_lock(813411, 20261025)")
                            .execute(&mut *tx)
                            .await?;
                        sqlx::query(&format!("DELETE FROM {} WHERE owner=$1", self.table))
                            .bind(&owner)
                            .execute(&mut *tx)
                            .await?;
                        tx.commit().await?;
                    }
                    Pool::Sqlite(pool) => {
                        let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
                        sqlx::query(&format!("DELETE FROM {} WHERE owner=?1", self.table))
                            .bind(&owner)
                            .execute(&mut *tx)
                            .await?;
                        tx.commit().await?;
                    }
                }
                Ok(StorageAdminReply::Wiped { owner })
            }
        }
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
            let bytes = if request.op == StorageOp::Set {
                request.value.len()
            } else {
                0
            };
            if !limits.consume(owner, write, bytes, limit) {
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
        let existing_size = data.as_ref().map_or(0, Vec::len);
        let record = decode_record(data, owner)?;
        let (mut reply, updated) = apply_request(&record, request, limit);
        if let Some(updated) = updated {
            let payload = if updated.scopes.is_empty() {
                Vec::new()
            } else {
                serde_json::to_vec(&updated)?
            };
            let query_total = format!(
                "SELECT used FROM {} WHERE id=1 FOR UPDATE",
                self.usage_table
            );
            let global_used: i64 = sqlx::query(&query_total)
                .fetch_one(&mut *tx)
                .await?
                .try_get("used")?;
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
            if payload.is_empty() {
                sqlx::query(&format!("DELETE FROM {} WHERE owner=$1", self.table))
                    .bind(owner)
                    .execute(&mut *tx)
                    .await?;
            } else {
                let upsert = format!("INSERT INTO {} (owner, payload) VALUES ($1, $2) ON CONFLICT(owner) DO UPDATE SET payload = EXCLUDED.payload", self.table);
                sqlx::query(&upsert)
                    .bind(owner)
                    .bind(payload)
                    .execute(&mut *tx)
                    .await?;
            }
        }
        tx.commit().await?;
        Ok(reply)
    }

    async fn sqlite_operation(
        &self,
        pool: &SqlitePool,
        owner: &str,
        request: StorageRequest,
        limit: usize,
        total_limit: usize,
    ) -> Result<StorageReply> {
        let writing = matches!(
            request.op,
            StorageOp::Set | StorageOp::Delete | StorageOp::Clear
        );
        let mut tx = if writing {
            pool.begin_with("BEGIN IMMEDIATE").await?
        } else {
            pool.begin().await?
        };
        let select = format!("SELECT payload FROM {} WHERE owner = ?1", self.table);
        let data: Option<Vec<u8>> = sqlx::query(&select)
            .bind(owner)
            .fetch_optional(&mut *tx)
            .await?
            .map(|row| row.try_get("payload"))
            .transpose()?;
        let previous_size = data.as_ref().map_or(0, Vec::len);
        let record = decode_record(data, owner)?;
        let (mut reply, updated) = apply_request(&record, request, limit);
        if let Some(updated) = updated {
            let payload = if updated.scopes.is_empty() {
                Vec::new()
            } else {
                serde_json::to_vec(&updated)?
            };
            let query_total = format!("SELECT used FROM {} WHERE id=1", self.usage_table);
            let global_used: i64 = sqlx::query(&query_total)
                .fetch_one(&mut *tx)
                .await?
                .try_get("used")?;
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
            if payload.is_empty() {
                sqlx::query(&format!("DELETE FROM {} WHERE owner=?1", self.table))
                    .bind(owner)
                    .execute(&mut *tx)
                    .await?;
            } else {
                let upsert = format!("INSERT INTO {} (owner, payload) VALUES (?1, ?2) ON CONFLICT(owner) DO UPDATE SET payload=excluded.payload", self.table);
                sqlx::query(&upsert)
                    .bind(owner)
                    .bind(payload)
                    .execute(&mut *tx)
                    .await?;
            }
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

    pub(super) async fn sqlite_usage(storage: &SqlPlayerStorage) -> (i64, i64, i64) {
        let Pool::Sqlite(pool) = &storage.pool else {
            unreachable!()
        };
        let used: i64 = sqlx::query_scalar(&format!(
            "SELECT used FROM {} WHERE id=1",
            storage.usage_table
        ))
        .fetch_one(pool)
        .await
        .unwrap();
        let sum: i64 = sqlx::query_scalar(&format!(
            "SELECT COALESCE(SUM(length(payload)),0) FROM {}",
            storage.table
        ))
        .fetch_one(pool)
        .await
        .unwrap();
        let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {}", storage.table))
            .fetch_one(pool)
            .await
            .unwrap();
        (used, sum, count)
    }

    #[tokio::test]
    async fn sqlite_counters_cover_concurrent_pools_deletion_and_reconciliation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shared.db");
        let (first, _) = SqlPlayerStorage::connect("", Some(&path), "basis_storage")
            .await
            .unwrap();
        assert_eq!(
            first
                .execute("alice", req(StorageOp::Set, vec![1; 64]), 1024, 0)
                .await
                .status,
            StorageStatus::Ok
        );
        let size = sqlite_usage(&first).await.0;
        assert_eq!(
            first
                .execute("alice", req(StorageOp::Delete, vec![]), 1024, 0)
                .await
                .status,
            StorageStatus::Ok
        );
        assert_eq!(sqlite_usage(&first).await, (0, 0, 0));
        let (second, _) = SqlPlayerStorage::connect("", Some(&path), "basis_storage")
            .await
            .unwrap();
        let (a, b) = tokio::join!(
            first.execute(
                "alice",
                req(StorageOp::Set, vec![1; 64]),
                1024,
                size as usize
            ),
            second.execute(
                "bobby",
                req(StorageOp::Set, vec![1; 64]),
                1024,
                size as usize
            )
        );
        assert_eq!(
            [a.status, b.status]
                .iter()
                .filter(|s| **s == StorageStatus::Ok)
                .count(),
            1
        );
        assert_eq!(
            [a.status, b.status]
                .iter()
                .filter(|s| **s == StorageStatus::Full)
                .count(),
            1
        );
        assert_eq!(sqlite_usage(&first).await, (size, size, 1));
        let Pool::Sqlite(pool) = &first.pool else {
            unreachable!()
        };
        let empty = serde_json::to_vec(&PlayerRecord {
            owner: "legacy".into(),
            ..Default::default()
        })
        .unwrap();
        sqlx::query("INSERT INTO basis_storage VALUES ('legacy',?1)")
            .bind(&empty)
            .execute(pool)
            .await
            .unwrap();
        assert_eq!(
            sqlite_usage(&first).await,
            (size + empty.len() as i64, size + empty.len() as i64, 2)
        );
        sqlx::query(&format!(
            "UPDATE {} SET used=999999 WHERE id=1",
            first.usage_table
        ))
        .execute(pool)
        .await
        .unwrap();
        let (restarted, _) = SqlPlayerStorage::connect("", Some(&path), "basis_storage")
            .await
            .unwrap();
        assert_eq!(sqlite_usage(&restarted).await, (size, size, 1));
        for owner in ["alice", "bobby"] {
            restarted
                .execute(owner, req(StorageOp::Clear, vec![]), 1024, 0)
                .await;
        }
        assert_eq!(sqlite_usage(&restarted).await, (0, 0, 0));
    }

    #[tokio::test]
    #[ignore = "requires BASIS_TEST_POSTGRES_URL; run by SQL CI job"]
    async fn postgres_integration_accounting_concurrency_and_empty_cleanup() {
        let url =
            std::env::var("BASIS_TEST_POSTGRES_URL").expect("BASIS_TEST_POSTGRES_URL required");
        let table = format!(
            "storage_test_{:x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        // PostgreSQL mode must never read legacy .bsr files or open SQLite.
        let migration_dir = tempfile::tempdir().unwrap();
        let legacy = migration_dir.path().join("players/aa");
        std::fs::create_dir_all(&legacy).unwrap();
        std::fs::write(
            legacy.join(format!("aa{}.bsr", "0".repeat(62))),
            b"invalid legacy data",
        )
        .unwrap();
        let sqlite_path = migration_dir.path().join("players.sqlite3");
        let (first, backend) = SqlPlayerStorage::connect(&url, Some(&sqlite_path), &table)
            .await
            .unwrap();
        assert_eq!(first.legacy_import_report().imported, 0);
        assert!(!sqlite_path.exists());
        assert_eq!(
            backend,
            SqlBackend::PostgreSql,
            "integration test must not silently use fallback"
        );
        let Pool::Postgres(pool) = &first.pool else {
            unreachable!()
        };
        check_byte_limits_and_admin(&first).await;
        assert_eq!(
            first
                .execute("alice", req(StorageOp::Set, vec![1; 64]), 1024, 0)
                .await
                .status,
            StorageStatus::Ok
        );
        let size: i64 = sqlx::query_scalar(&format!(
            "SELECT used FROM {} WHERE id=1",
            first.usage_table
        ))
        .fetch_one(pool)
        .await
        .unwrap();
        first
            .execute("alice", req(StorageOp::Clear, vec![]), 1024, 0)
            .await;
        let (second, backend) = SqlPlayerStorage::connect(&url, None, &table).await.unwrap();
        assert_eq!(backend, SqlBackend::PostgreSql);
        let (a, b) = tokio::join!(
            first.execute(
                "alice",
                req(StorageOp::Set, vec![1; 64]),
                1024,
                size as usize
            ),
            second.execute(
                "bobby",
                req(StorageOp::Set, vec![1; 64]),
                1024,
                size as usize
            )
        );
        assert_eq!(
            [a.status, b.status]
                .iter()
                .filter(|s| **s == StorageStatus::Ok)
                .count(),
            1
        );
        assert_eq!(
            [a.status, b.status]
                .iter()
                .filter(|s| **s == StorageStatus::Full)
                .count(),
            1
        );
        let empty = serde_json::to_vec(&PlayerRecord {
            owner: "legacy".into(),
            ..Default::default()
        })
        .unwrap();
        sqlx::query(&format!("INSERT INTO {table} VALUES ('legacy',$1)"))
            .bind(&empty)
            .execute(pool)
            .await
            .unwrap();
        let (restarted, backend) = SqlPlayerStorage::connect(&url, None, &table).await.unwrap();
        assert_eq!(backend, SqlBackend::PostgreSql);
        let used: i64 = sqlx::query_scalar(&format!(
            "SELECT used FROM {} WHERE id=1",
            first.usage_table
        ))
        .fetch_one(pool)
        .await
        .unwrap();
        let sum: i64 = sqlx::query_scalar(&format!(
            "SELECT COALESCE(SUM(octet_length(payload)),0)::BIGINT FROM {table}"
        ))
        .fetch_one(pool)
        .await
        .unwrap();
        assert_eq!((used, sum), (size, size));
        for owner in ["alice", "bobby"] {
            restarted
                .execute(owner, req(StorageOp::Delete, vec![]), 1024, 0)
                .await;
        }
        let used: i64 = sqlx::query_scalar(&format!(
            "SELECT used FROM {} WHERE id=1",
            first.usage_table
        ))
        .fetch_one(pool)
        .await
        .unwrap();
        let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(pool)
            .await
            .unwrap();
        assert_eq!((used, count), (0, 0));
        // A connected database with an unusable schema must not silently select
        // SQLite and expose an unrelated empty dataset.
        let view = format!("{table}_view");
        sqlx::query(&format!(
            "CREATE VIEW {view} AS SELECT owner,payload FROM {table}"
        ))
        .execute(pool)
        .await
        .unwrap();
        let error = SqlPlayerStorage::connect(&url, None, &view)
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("accounting initialization failed"));
        sqlx::query(&format!("DROP VIEW {view}"))
            .execute(pool)
            .await
            .unwrap();
        sqlx::query(&format!("DROP TABLE {table} CASCADE"))
            .execute(pool)
            .await
            .unwrap();
        sqlx::query(&format!("DROP FUNCTION {}_fn()", first.usage_table))
            .execute(pool)
            .await
            .unwrap();
        sqlx::query(&format!("DROP TABLE {}", first.usage_table))
            .execute(pool)
            .await
            .unwrap();
    }
    fn req(op: StorageOp, value: Vec<u8>) -> StorageRequest {
        StorageRequest {
            op,
            id: 1,
            scope: "level".into(),
            key: "score".into(),
            value,
        }
    }

    async fn check_byte_limits_and_admin(storage: &SqlPlayerStorage) {
        use crate::admin::{StorageAdminAction as Action, StorageAdminReply as Reply};
        for _ in 0..2 {
            assert_eq!(
                storage
                    .execute(
                        "byte-owner",
                        req(StorageOp::Set, vec![3; 16_300]),
                        16_384,
                        0
                    )
                    .await
                    .status,
                StorageStatus::Ok
            );
        }
        assert_eq!(
            storage
                .execute(
                    "byte-owner",
                    req(StorageOp::Set, vec![4; 16_300]),
                    16_384,
                    0
                )
                .await
                .status,
            StorageStatus::RateLimited
        );
        assert_eq!(
            storage
                .execute("byte-owner", req(StorageOp::Get, vec![]), 16_384, 0)
                .await
                .value,
            vec![3; 16_300]
        );
        let inspection = storage
            .admin(Action::Inspect("byte-owner".into()))
            .await
            .unwrap();
        let Reply::Inspection { record, .. } = inspection else {
            panic!()
        };
        assert_eq!(record.scopes["level"]["score"], 16_300);
        assert!(matches!(
            storage.admin(Action::Statistics).await.unwrap(),
            Reply::Statistics { records: 1, .. }
        ));
        storage
            .admin(Action::Wipe("byte-owner".into()))
            .await
            .unwrap();
        assert!(matches!(
            storage.admin(Action::Statistics).await.unwrap(),
            Reply::Statistics {
                records: 0,
                bytes: 0,
                ..
            }
        ));
        assert_eq!(
            storage
                .execute("shrinking", req(StorageOp::Set, vec![1; 128]), 1024, 0)
                .await
                .status,
            StorageStatus::Ok
        );
        assert_eq!(
            storage
                .execute("shrinking", req(StorageOp::Set, vec![1; 64]), 8, 0)
                .await
                .status,
            StorageStatus::Ok
        );
        assert_eq!(
            storage
                .execute("shrinking", req(StorageOp::Clear, vec![]), 8, 0)
                .await
                .status,
            StorageStatus::Ok
        );
        // A corrupt SQL row must be removable without deserializing its payload.
        match &storage.pool {
            Pool::Postgres(pool) => {
                sqlx::query(&format!(
                    "INSERT INTO {} (owner,payload) VALUES ('corrupt',$1)",
                    storage.table
                ))
                .bind(b"bad".to_vec())
                .execute(pool)
                .await
                .unwrap();
            }
            Pool::Sqlite(pool) => {
                sqlx::query(&format!(
                    "INSERT INTO {} (owner,payload) VALUES ('corrupt',?1)",
                    storage.table
                ))
                .bind(b"bad".to_vec())
                .execute(pool)
                .await
                .unwrap();
            }
        }
        assert!(storage
            .admin(Action::Inspect("corrupt".into()))
            .await
            .is_err());
        storage.admin(Action::Wipe("corrupt".into())).await.unwrap();
        assert!(matches!(
            storage.admin(Action::Statistics).await.unwrap(),
            Reply::Statistics {
                records: 0,
                bytes: 0,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn sqlite_byte_limits_and_operator_recovery() {
        let dir = tempfile::tempdir().unwrap();
        for path in [None, Some(dir.path().join("admin.db"))] {
            let (storage, _) = SqlPlayerStorage::connect("", path.as_deref(), "basis_storage")
                .await
                .unwrap();
            check_byte_limits_and_admin(&storage).await;
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
                    let mut request = req(StorageOp::Set, vec![]);
                    request.scope = "three".into();
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
