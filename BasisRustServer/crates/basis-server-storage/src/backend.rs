//! Configurable, application-registered player-storage backends.
//!
//! The server owns DID authentication and permission checks. Backends must
//! preserve the channel's six operations, per-player and aggregate quotas,
//! rate limits, and owner isolation. Use the built-in backends as references.
//! A backend is selected once at server startup; changing it does not migrate
//! data or switch stores while requests are in flight.
#[cfg(feature = "sql-storage")]
use crate::sql::{SqlBackend, SqlPlayerStorage};
use crate::{
    admin::{StorageAdminAction, StorageAdminReply},
    player::PlayerStorage,
};
use anyhow::{bail, Context, Result};
use basis_protocol::{
    config::ServerConfig,
    storage::{StorageReply, StorageRequest, StorageStatus, MAX_BYTES_PER_PLAYER},
};
use std::{collections::HashMap, future::Future, path::PathBuf, pin::Pin, sync::Arc};

/// Object-safe async operation type, usable without a proc macro or async-trait.
pub type StorageFuture<'a> = Pin<Box<dyn Future<Output = StorageReply> + Send + 'a>>;

/// Operator-only operations, never exposed on the player storage network channel.
pub type StorageAdminFuture<'a> =
    Pin<Box<dyn Future<Output = Result<StorageAdminReply>> + Send + 'a>>;

/// Implement this to supply Get/Set/Delete/Keys/Usage/Clear for verified DIDs.
///
/// `owner` is the authenticated DID, never a client-selected filename. The
/// server checks `basis.storage.use` before calling this trait. Implementations
/// must enforce the supplied quotas and per-owner rate limits atomically, return
/// protocol-compatible replies (including request op/id), and prevent cross-DID
/// access. Never perform blocking filesystem I/O directly on the async runtime.
pub trait PlayerStorageBackend: Send + Sync {
    fn execute<'a>(
        &'a self,
        owner: &'a str,
        request: StorageRequest,
        per_player_limit: usize,
        total_limit: usize,
    ) -> StorageFuture<'a>;

    /// Optional console-only statistics, inspect, and wipe operations.
    ///
    /// Custom backends can implement this if they support operator access;
    /// unsupported operations must fail explicitly rather than misreport data.
    fn admin<'a>(&'a self, _action: StorageAdminAction) -> StorageAdminFuture<'a> {
        Box::pin(async { bail!("storage backend does not support administration") })
    }
}

/// Owned initialization inputs available to custom backend factories.
///
/// `config.storage_connection_string` can carry a custom backend URL. The
/// `BASIS_STORAGE_URL` environment override applies only to the SQL backend.
#[derive(Clone)]
pub struct StorageBackendContext {
    pub base_dir: PathBuf,
    pub config: ServerConfig,
}

type FactoryFuture = Pin<Box<dyn Future<Output = Result<Arc<dyn PlayerStorageBackend>>> + Send>>;
type Factory = Box<dyn Fn(StorageBackendContext) -> FactoryFuture + Send + Sync>;

/// Register once at application startup, then select by `StorageBackend` in XML.
///
/// A custom backend is compiled into the server and registered by its embedder;
/// XML configuration cannot execute arbitrary libraries or load untrusted code.
pub struct StorageBackendRegistry {
    factories: HashMap<String, Factory>,
}

impl StorageBackendRegistry {
    /// Creates an empty registry, useful when embedding a custom-only server.
    pub fn new() -> Self {
        Self {
            factories: HashMap::new(),
        }
    }

    /// Registers a named async constructor. Names are case-insensitive in XML.
    /// Duplicate names are rejected so built-in backends cannot be overridden
    /// accidentally.
    pub fn register<F, Fut>(&mut self, name: &str, factory: F) -> Result<()>
    where
        F: Fn(StorageBackendContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Arc<dyn PlayerStorageBackend>>> + Send + 'static,
    {
        let name = name.trim().to_ascii_lowercase();
        anyhow::ensure!(
            !name.is_empty()
                && name
                    .bytes()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-'),
            "invalid storage backend name"
        );
        anyhow::ensure!(
            !self.factories.contains_key(&name),
            "duplicate storage backend: {name}"
        );
        self.factories
            .insert(name, Box::new(move |ctx| Box::pin(factory(ctx))));
        Ok(())
    }

    /// Register the standard memory/file implementations and optional SQL.
    pub fn with_builtins() -> Self {
        let mut registry = Self::new();
        registry
            .register("memory", |_| async {
                Ok(Arc::new(PlayerStorage::new(None)?) as Arc<dyn PlayerStorageBackend>)
            })
            .expect("valid built-in name");
        registry
            .register("file", |ctx| async move {
                // Preserve the existing file-backend behavior: when file
                // support is disabled, use transient memory storage.
                let root = ctx
                    .config
                    .has_file_support
                    .then(|| ctx.base_dir.join("storage").join("players"));
                Ok(Arc::new(PlayerStorage::new(root)?) as Arc<dyn PlayerStorageBackend>)
            })
            .expect("valid built-in name");
        #[cfg(feature = "sql-storage")]
        registry
            .register("sql", |ctx| async move {
                let sqlite_path = ctx
                    .config
                    .has_file_support
                    .then(|| ctx.base_dir.join("storage").join("players.sqlite3"));
                let remote_url = match std::env::var("BASIS_STORAGE_URL") {
                    Ok(value) if !value.trim().is_empty() => value,
                    Ok(_) | Err(std::env::VarError::NotPresent) => {
                        ctx.config.storage_connection_string.clone()
                    }
                    Err(error) => return Err(error).context("reading BASIS_STORAGE_URL"),
                };
                let allow_fallback = match std::env::var("BASIS_STORAGE_ALLOW_SQLITE_FALLBACK") {
                    Ok(value) if value.eq_ignore_ascii_case("true") => true,
                    Ok(value) if value.eq_ignore_ascii_case("false") => false,
                    Ok(_) => {
                        bail!("BASIS_STORAGE_ALLOW_SQLITE_FALLBACK must be 'true' or 'false'")
                    }
                    Err(std::env::VarError::NotPresent) => false,
                    Err(error) => {
                        return Err(error).context("reading BASIS_STORAGE_ALLOW_SQLITE_FALLBACK")
                    }
                };
                let (backend, selected) = SqlPlayerStorage::connect_with_fallback(
                    &remote_url,
                    sqlite_path.as_deref(),
                    &ctx.config.storage_sql_table,
                    allow_fallback,
                )
                .await?;
                if selected == SqlBackend::Sqlite && !remote_url.trim().is_empty() {
                    tracing::warn!("Remote SQL unavailable at startup: operator opted into local SQLite fallback (data is separate)");
                }
                tracing::info!("Cilbox SQL storage selected backend: {selected:?}");
                let migration = backend.legacy_import_report();
                if migration.imported
                    + migration.preserved
                    + migration.empty
                    + migration.previously_imported
                    > 0
                {
                    tracing::info!(?migration, "SQLite legacy player-storage migration");
                }
                Ok(Arc::new(backend) as Arc<dyn PlayerStorageBackend>)
            })
            .expect("valid built-in name");
        registry
    }

    /// Resolve a case-insensitive backend name and initialize it with the supplied context.
    ///
    /// Returns an error when the name is not registered or its factory fails.
    pub async fn create(
        &self,
        name: &str,
        context: StorageBackendContext,
    ) -> Result<Arc<dyn PlayerStorageBackend>> {
        let key = name.trim().to_ascii_lowercase();
        let Some(factory) = self.factories.get(&key) else {
            bail!("unsupported storage backend '{name}' (not registered in this build)");
        };
        factory(context)
            .await
            .with_context(|| format!("initializing storage backend '{key}'"))
    }
}

impl Default for StorageBackendRegistry {
    fn default() -> Self {
        Self::with_builtins()
    }
}

impl PlayerStorageBackend for PlayerStorage {
    /// Run file/memory operations on a blocking worker, returning Unavailable if it fails.
    fn execute<'a>(
        &'a self,
        owner: &'a str,
        request: StorageRequest,
        per_player_limit: usize,
        total_limit: usize,
    ) -> StorageFuture<'a> {
        // Keep the existing file lock, disk accounting and rate limiter off the
        // Tokio worker; use owned values for the blocking task.
        let storage = self.clone();
        let owner = owner.to_owned();
        let fallback = unavailable(&request, per_player_limit);
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                PlayerStorage::execute(&storage, &owner, request, per_player_limit, total_limit)
            })
            .await
            .unwrap_or(fallback)
        })
    }

    /// Run console administration on a blocking worker and propagate storage or task errors.
    fn admin<'a>(&'a self, action: StorageAdminAction) -> StorageAdminFuture<'a> {
        let storage = self.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || PlayerStorage::admin(&storage, action))
                .await
                .context("player storage operator task failed")?
        })
    }
}

#[cfg(feature = "sql-storage")]
impl PlayerStorageBackend for SqlPlayerStorage {
    fn execute<'a>(
        &'a self,
        owner: &'a str,
        request: StorageRequest,
        per_player_limit: usize,
        total_limit: usize,
    ) -> StorageFuture<'a> {
        Box::pin(SqlPlayerStorage::execute(
            self,
            owner,
            request,
            per_player_limit,
            total_limit,
        ))
    }

    fn admin<'a>(&'a self, action: StorageAdminAction) -> StorageAdminFuture<'a> {
        Box::pin(SqlPlayerStorage::admin(self, action))
    }
}

/// Standard backend-error response for an unavailable store.
pub fn unavailable(request: &StorageRequest, per_player_limit: usize) -> StorageReply {
    StorageReply {
        op: request.op,
        id: request.id,
        status: StorageStatus::Unavailable,
        used: 0,
        limit: per_player_limit.clamp(1, MAX_BYTES_PER_PLAYER) as i32,
        value: Vec::new(),
        keys: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use basis_protocol::storage::{StorageOp, StorageStatus};

    #[cfg(feature = "sql-storage")]
    static SQL_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[cfg(feature = "sql-storage")]
    fn set_sql_test_env(remote: Option<&str>, fallback: Option<&str>) {
        match remote {
            Some(value) => std::env::set_var("BASIS_STORAGE_URL", value),
            None => std::env::remove_var("BASIS_STORAGE_URL"),
        }
        match fallback {
            Some(value) => std::env::set_var("BASIS_STORAGE_ALLOW_SQLITE_FALLBACK", value),
            None => std::env::remove_var("BASIS_STORAGE_ALLOW_SQLITE_FALLBACK"),
        }
    }

    fn request(op: StorageOp, value: &[u8]) -> StorageRequest {
        StorageRequest {
            op,
            id: 7,
            scope: "world".into(),
            key: "score".into(),
            value: value.to_vec(),
        }
    }

    fn context(root: &std::path::Path) -> StorageBackendContext {
        StorageBackendContext {
            base_dir: root.to_path_buf(),
            config: ServerConfig {
                has_file_support: true,
                ..ServerConfig::default()
            },
        }
    }

    #[tokio::test]
    async fn builtins_have_identical_get_set_and_operator_contracts() {
        let dir = tempfile::tempdir().unwrap();
        let registry = StorageBackendRegistry::default();
        for name in ["memory", "file"] {
            let storage = registry.create(name, context(dir.path())).await.unwrap();
            assert_eq!(
                storage
                    .execute("alice", request(StorageOp::Set, b"42"), 1024, 4096)
                    .await
                    .status,
                StorageStatus::Ok
            );
            assert_eq!(
                storage
                    .execute("alice", request(StorageOp::Get, b""), 1024, 4096)
                    .await
                    .value,
                b"42"
            );
            assert_eq!(
                storage
                    .execute("bob", request(StorageOp::Get, b""), 1024, 4096)
                    .await
                    .status,
                StorageStatus::NotFound
            );
            let stats = storage.admin(StorageAdminAction::Statistics).await.unwrap();
            assert!(matches!(
                stats,
                StorageAdminReply::Statistics { records: 1, .. }
            ));
            let inspected = storage
                .admin(StorageAdminAction::Inspect("alice".into()))
                .await
                .unwrap();
            assert!(matches!(inspected, StorageAdminReply::Inspection { .. }));
            assert!(matches!(
                storage
                    .admin(StorageAdminAction::Wipe("alice".into()))
                    .await
                    .unwrap(),
                StorageAdminReply::Wiped { .. }
            ));
            assert_eq!(
                storage
                    .execute("alice", request(StorageOp::Get, b""), 1024, 4096)
                    .await
                    .status,
                StorageStatus::NotFound
            );
        }
    }

    struct CustomBackend;

    impl PlayerStorageBackend for CustomBackend {
        fn execute<'a>(
            &'a self,
            owner: &'a str,
            request: StorageRequest,
            per_player_limit: usize,
            _total_limit: usize,
        ) -> StorageFuture<'a> {
            Box::pin(async move {
                let mut reply = unavailable(&request, per_player_limit);
                reply.status = if owner == "alice" {
                    StorageStatus::Ok
                } else {
                    StorageStatus::Denied
                };
                reply.value = b"custom".to_vec();
                reply
            })
        }
    }

    #[tokio::test]
    async fn configurable_registration_and_missing_backend() {
        let dir = tempfile::tempdir().unwrap();
        let mut registry = StorageBackendRegistry::new();
        registry
            .register("Custom-KV", |_| async {
                Ok(Arc::new(CustomBackend) as Arc<dyn PlayerStorageBackend>)
            })
            .unwrap();
        assert!(registry
            .register("custom-kv", |_| async {
                Ok(Arc::new(PlayerStorage::new(None)?) as Arc<dyn PlayerStorageBackend>)
            })
            .is_err());
        let backend = registry
            .create("CUSTOM-KV", context(dir.path()))
            .await
            .unwrap();
        let allowed = backend
            .execute("alice", request(StorageOp::Get, b""), 4096, 8192)
            .await;
        assert_eq!(allowed.status, StorageStatus::Ok);
        assert_eq!(allowed.value, b"custom");
        assert_eq!((allowed.id, allowed.op), (7, StorageOp::Get));
        assert!(backend.admin(StorageAdminAction::Statistics).await.is_err());
        assert_eq!(
            backend
                .execute("bob", request(StorageOp::Get, b""), 4096, 8192)
                .await
                .status,
            StorageStatus::Denied
        );
        let error = registry
            .create("not-installed", context(dir.path()))
            .await
            .err()
            .unwrap();
        assert!(error.to_string().contains("not registered"));
    }

    #[tokio::test]
    async fn file_storage_keeps_data_across_factory_instances() {
        let dir = tempfile::tempdir().unwrap();
        let registry = StorageBackendRegistry::default();
        let first = registry.create("file", context(dir.path())).await.unwrap();
        first
            .execute("alice", request(StorageOp::Set, b"saved"), 1024, 4096)
            .await;
        drop(first);
        let second = registry.create("file", context(dir.path())).await.unwrap();
        assert_eq!(
            second
                .execute("alice", request(StorageOp::Get, b""), 1024, 4096)
                .await
                .value,
            b"saved"
        );
    }

    #[cfg(feature = "sql-storage")]
    #[tokio::test]
    async fn sql_factory_uses_the_same_interface() {
        let _guard = SQL_ENV_LOCK.lock().await;
        set_sql_test_env(None, None);
        let dir = tempfile::tempdir().unwrap();
        let registry = StorageBackendRegistry::default();
        let mut ctx = context(dir.path());
        ctx.config.storage_connection_string = String::new();
        let store = registry.create("sql", ctx).await.unwrap();
        assert_eq!(
            store
                .execute("alice", request(StorageOp::Set, b"sql"), 1024, 4096)
                .await
                .status,
            StorageStatus::Ok
        );
        assert_eq!(
            store
                .execute("alice", request(StorageOp::Get, b""), 1024, 4096)
                .await
                .value,
            b"sql"
        );
        assert!(matches!(
            store.admin(StorageAdminAction::Statistics).await.unwrap(),
            StorageAdminReply::Statistics { records: 1, .. }
        ));
        assert!(matches!(
            store
                .admin(StorageAdminAction::Wipe("alice".into()))
                .await
                .unwrap(),
            StorageAdminReply::Wiped { .. }
        ));
        assert_eq!(
            store
                .execute("alice", request(StorageOp::Get, b""), 1024, 4096)
                .await
                .status,
            StorageStatus::NotFound
        );
    }

    #[cfg(feature = "sql-storage")]
    #[tokio::test]
    async fn remote_sql_failure_is_denied_without_fallback_opt_in() {
        let _guard = SQL_ENV_LOCK.lock().await;
        set_sql_test_env(Some("postgres://127.0.0.1:1/unavailable"), None);
        let dir = tempfile::tempdir().unwrap();
        let result = StorageBackendRegistry::default()
            .create("sql", context(dir.path()))
            .await;
        assert!(result.is_err());
        let err = format!("{:#}", result.err().unwrap());
        assert!(err.contains("connecting to configured PostgreSQL"), "{err}");
        set_sql_test_env(None, None);
    }

    #[cfg(feature = "sql-storage")]
    #[tokio::test]
    async fn explicit_remote_sql_fallback_uses_sqlite_and_sql_accounting() {
        let _guard = SQL_ENV_LOCK.lock().await;
        set_sql_test_env(Some("postgres://127.0.0.1:1/unavailable"), Some("true"));
        let dir = tempfile::tempdir().unwrap();
        let store = StorageBackendRegistry::default()
            .create("sql", context(dir.path()))
            .await
            .unwrap();
        assert_eq!(
            store
                .execute("alice", request(StorageOp::Set, b"opted-in"), 1024, 4096)
                .await
                .status,
            StorageStatus::Ok
        );
        assert!(matches!(
            store.admin(StorageAdminAction::Statistics).await.unwrap(),
            StorageAdminReply::Statistics {
                backend: "sqlite",
                records: 1,
                bytes
            } if bytes > 0
        ));
        set_sql_test_env(None, None);
    }

    #[cfg(feature = "sql-storage")]
    #[tokio::test]
    async fn invalid_sql_fallback_opt_in_is_rejected() {
        let _guard = SQL_ENV_LOCK.lock().await;
        set_sql_test_env(Some("postgres://127.0.0.1:1/unavailable"), Some("yes"));
        let dir = tempfile::tempdir().unwrap();
        let result = StorageBackendRegistry::default()
            .create("sql", context(dir.path()))
            .await;
        assert!(result.is_err());
        assert!(format!("{:#}", result.err().unwrap()).contains("must be 'true' or 'false'"));
        set_sql_test_env(None, None);
    }

    #[cfg(feature = "sql-storage")]
    #[tokio::test]
    async fn successful_remote_sql_selection_is_preserved() {
        let _guard = SQL_ENV_LOCK.lock().await;
        set_sql_test_env(None, None);
        let url = std::env::var("BASIS_TEST_POSTGRES_URL");
        let Ok(url) = url else {
            // The default successful SQL operation test covers local-only mode;
            // CI can supply this value for remote selection coverage.
            return;
        };
        set_sql_test_env(Some(&url), Some("false"));
        let dir = tempfile::tempdir().unwrap();
        let store = StorageBackendRegistry::default()
            .create("sql", context(dir.path()))
            .await
            .expect("configured remote PostgreSQL should initialize");
        assert!(matches!(
            store.admin(StorageAdminAction::Statistics).await.unwrap(),
            StorageAdminReply::Statistics {
                backend: "postgresql",
                ..
            }
        ));
        set_sql_test_env(None, None);
    }
}
