# Custom Cilbox player-storage backends

BasisRust's player-storage channel (Get, Set, Delete, Keys, Usage, Clear)
uses one public interface: `basis_server_storage::backend::PlayerStorageBackend`.
The server no longer needs a special case for each backend. This does **not**
replace the unrelated legacy `database.json` configuration snapshot.

## Select an existing backend

In the server XML configuration, set:

```xml
<StorageEnabled>true</StorageEnabled>
<StorageBackend>file</StorageBackend>
<StorageMaxBytesPerPlayer>16384</StorageMaxBytesPerPlayer>
<StorageMaxTotalMegabytes>256</StorageMaxTotalMegabytes>
```

Choose `file`, `memory`, or `sql` (requires the `sql-storage` Cargo
feature). Existing paths, PostgreSQL initialization/fallback, and limits are
unchanged. `file` transparently uses memory when `HasFileSupport` is false.
The selected backend is constructed once at startup; changing its name needs
a restart. The built-in SQL backend retains main's one-time legacy player-file
import into SQLite; selecting other backends does not automatically migrate
records.

## Implement a backend

A custom implementation must provide the object-safe `execute` method.
A second, optional `admin` method provides local console statistics,
inspection and wipe commands; it fails explicitly by default. Custom
backends should override `admin` if operators need these commands:

```rust
use basis_server_storage::backend::{PlayerStorageBackend, StorageFuture};
use basis_protocol::storage::{StorageReply, StorageRequest};

struct MyDatabase {
    // Connection pool, KV store, or other state.
}

impl PlayerStorageBackend for MyDatabase {
    fn execute<'a>(
        &'a self,
        owner: &'a str,
        request: StorageRequest,
        per_player_limit: usize,
        total_limit: usize,
    ) -> StorageFuture<'a> {
        Box::pin(async move {
            // Perform Get/Set/Delete/Keys/Usage/Clear for the authenticated
            // `owner` DID, with durable writes and atomic quota enforcement.
            // Return StorageReply with the same op/id as `request`.
            todo!("implement your own storage protocol operations")
        })
    }
}
```

If you only need to instrument or wrap an existing backend, you can delegate
operations to it rather than reimplementing the wire semantics. A complete
working example of registration that wraps the built-in file store:

```rust
use anyhow::Result;
use basis_protocol::{config::ServerConfig, storage::StorageRequest};
use basis_server_core::ServerState;
use basis_server_storage::{
    admin::StorageAdminAction,
    backend::{PlayerStorageBackend, StorageBackendRegistry, StorageFuture, StorageAdminFuture},
    player::PlayerStorage,
};
use std::{path::Path, sync::Arc};

struct AuditedStorage {
    inner: PlayerStorage,
}
impl PlayerStorageBackend for AuditedStorage {
    fn execute<'a>(
        &'a self,
        owner: &'a str,
        request: StorageRequest,
        per_player_limit: usize,
        total_limit: usize,
    ) -> StorageFuture<'a> {
        Box::pin(async move {
            // Add custom GET/SET handling, logging, or storage adapters here.
            PlayerStorageBackend::execute(
                &self.inner, owner, request, per_player_limit, total_limit
            ).await
        })
    }

    fn admin<'a>(&'a self, action: StorageAdminAction) -> StorageAdminFuture<'a> {
        PlayerStorageBackend::admin(&self.inner, action)
    }
}

async fn start(
    config: ServerConfig,
    base_dir: &Path,
) -> Result<(ServerState, tokio::sync::oneshot::Sender<()>)> {
    let mut registry = StorageBackendRegistry::with_builtins();
    registry.register("audited", |ctx| async move {
        let root = ctx.config.has_file_support
            .then(|| ctx.base_dir.join("storage").join("audited-players"));
        Ok(Arc::new(AuditedStorage {
            inner: PlayerStorage::new(root)?,
        }) as Arc<dyn PlayerStorageBackend>)
    })?;
    ServerState::start_with_storage_registry(config, base_dir, registry).await
}
```

Register the backend in your **server executable** at startup. Then select it
without editing the request handler:

```xml
<StorageEnabled>true</StorageEnabled>
<StorageBackend>audited</StorageBackend>
```

The default executable registers only the built-ins. An XML name alone does not
install third-party Rust code; for a custom name, compile its registration into
your executable and call `start_with_storage_registry` (or
`start_with_config_path_and_storage_registry` for a nonstandard config path).
Registered names are case-insensitive, and duplicates are rejected.

The factory receives owned `StorageBackendContext { config, base_dir }`. Custom
backends may use `StorageConnectionString` as their connection parameter, or
obtain credentials from their own environment variables/secrets manager. The
`BASIS_STORAGE_URL` environment override is used only by the built-in SQL
factory; neither its value nor its password is logged.

## Required backend guarantees

- Authorize access only to the authenticated `owner` DID passed by the server;
  never trust a client to choose another account. The server checks the verified
  DID and `basis.storage.use` permission *before* dispatch.
- Implement all six operations and the protocol statuses, returning matching
  operation and request ID. Enforce each per-player quota and the global quota
  atomically, including concurrent requests/processes. A reduction in usage
  must remain possible if quotas are lowered.
- Apply per-DID read/write rate limiting (built-ins use a shared quota-aware
  limiter) and avoid client-controlled unbounded allocations.
- Run blocking file/network operations outside the async executor; return an
  `Unavailable` reply on backend failures rather than leaking sensitive errors.
- Preserve data consistency on startup, restart, and backend outages. The
  built-in SQLite backend retains its legacy import path, but changing between
  arbitrary backends does not migrate old data. Never silently switch the active
  backend after writes have begun.
- Implement `admin` for console-only statistics/inspect/wipe support if desired.
  The default implementation explicitly rejects those commands. Never expose
  these operator commands to untrusted network clients.

See `crates/basis-server-storage/src/backend.rs` for the trait, registry,
default backends, and tests. See `player.rs` for file/memory accounting and
`sql.rs` for SQL transaction and quota handling.
