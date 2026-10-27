# Migrating C# BasisVR player storage

BasisRust can import C# `.bsr` saves automatically **only when persistent SQLite
is the selected backend**. File/JSON, memory, in-memory SQLite and remote
PostgreSQL do not import these files. Network protocol compatibility does not
make the on-disk or SQL schemas interchangeable.

The importer supports BSTR version 1 from
[BasisVR/Basis `acfca6b1`](https://github.com/BasisVR/Basis/blob/acfca6b1c129c9ad5786e8b693e7ad2da2cd73a0/Basis%20Server/BasisNetworkServer/Storage/BasisStorageRecord.cs).
It verifies SHA-256 checksums, reserved/version fields, UTF-8 names, owner-to-file
hash binding, counts, lengths and absolute storage limits. Values, including
binary and zero-length values, are preserved. C# revisions are validated but are
not part of Rust's account JSON format.

## Import into SQLite

1. Stop the C# server and back up its complete storage directory. Stop any
   BasisRust process using the destination SQLite database. Do not let C# keep
   writing the source while import runs.
2. Copy the original nested `players` directory into the Rust base directory:

   ```text
   <base-dir>/storage/players/<first-two-hash-characters>/<owner-hash>.bsr
   ```

   Keep the original files and backup; renaming `.bsr` to `.json` does not work.
3. Edit `<base-dir>/config/config.xml` inside `<Configuration>`:

   ```xml
   <HasFileSupport>true</HasFileSupport>
   <StorageEnabled>true</StorageEnabled>
   <StorageBackend>sql</StorageBackend>
   <StorageConnectionString></StorageConnectionString>
   <StorageSqlTable>basis_storage_rust</StorageSqlTable>
   <OverrideAutoDiscoveryOfIpv>true</OverrideAutoDiscoveryOfIpv>
   <IPv4Address>127.0.0.1</IPv4Address>
   ```

   `basis_storage_rust` is an example destination table name. Use the same name
   throughout the migration. Clear/unset `BASIS_STORAGE_URL` in the environment
   too: that variable overrides the XML connection string. With no remote URL,
   the SQL backend selects `<base-dir>/storage/players.sqlite3`.
4. Start the console from the repository root, using a temporary local port:

   ```sh
   cargo run --release --manifest-path BasisRustServer/Cargo.toml \
     -p basis-server-console --features sql-storage -- \
     --base-dir /srv/basis-migration --port 0
   ```

   Replace `/srv/basis-migration` with your staging base directory. Its XML should
   contain the settings above. Confirm the startup log selects `Sqlite` and
   reports the imported/preserved/empty/already-processed account counts.
5. Use `/storage` and `/storage show <uuid-or-did>` to verify accounts and key
   sizes. Use `/shutdown` before moving the database or continuing to PostgreSQL.
   If moving SQLite, use a SQLite backup or include any active WAL files; copying
   just the database file while another process uses it can omit committed data.

Import is one transaction, including quota accounting and migration markers. If
a new source file is invalid or unreadable, the import rolls back; storage is
reported unavailable and startup logs identify the error. The game server can
continue running, so check these logs before accepting players. Fix/restore the
source save and restart to retry. Source `.bsr` files are never changed or deleted.

Existing destination accounts always win; scopes are not merged or overwritten.
Both imported and preserved identities receive persistent migration markers.
Restarting after a player clears data or an operator wipes an account will not
restore it from the old `.bsr` file. Updated source files for an already-processed
identity are also ignored. For a new full conversion, use a fresh staging SQLite
database rather than deleting markers from a live server.

Migration preserves valid saves even when they exceed the currently configured
player/global quotas; it enforces the format's absolute 1 MiB per-player limit.
Rust's serialized JSON can occupy more bytes than C# binary records. Review the
configured quotas before admitting players; growth can be refused while an
account or server is over quota, but shrinking/clearing remains available.

## Move converted SQLite data to remote PostgreSQL

The PostgreSQL migration is an explicit operator action, not a runtime fallback
or synchronization process. It copies **Rust SQLite account JSON** after the
conversion above; it does not read `.bsr` files directly.

1. Keep the source/staging and destination game servers stopped during the copy.
   Back up SQLite and PostgreSQL first.
2. Initialize the remote destination once with a SQL-enabled BasisRust instance,
   using the same `StorageSqlTable` as the SQLite source. Configure its
   `BASIS_STORAGE_URL` for PostgreSQL, confirm the log actually selects
   `PostgreSql`, then shut that instance down. This installs the account table,
   quota counter and triggers. The database role must own these objects and be
   able to create tables, functions and triggers.

   **Do not reuse a C# SQL table:** C# uses `owner_key/record/revision` columns;
   Rust uses `owner/payload`. Select a new table such as `basis_storage_rust` and
   keep the old C# tables intact. This guide migrates file saves; direct conversion
   of an existing C# SQL database is not implemented.
3. Install Python 3 and PostgreSQL's `psql` client on the migration machine. Set up
   a libpq service in `.pg_service.conf` and a `.pgpass`/`PGPASSFILE`, or use normal
   `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER` and password-file settings. Select the
   **same database and schema** used in step 2; configure the service's TLS mode
   and certificate settings for that server. The script does not read
   `BASIS_STORAGE_URL` or request passwords interactively.
4. Validate the source without contacting PostgreSQL:

   ```sh
   python3 -B scripts/storage/migrate_player_storage.py \
     --sqlite /srv/basis-migration/storage/players.sqlite3 \
     --table basis_storage_rust
   ```
5. Commit using the configured libpq service:

   ```sh
   PGSERVICE=basisrust-migration python3 -B scripts/storage/migrate_player_storage.py \
     --sqlite /srv/basis-migration/storage/players.sqlite3 \
     --table basis_storage_rust --apply
   ```

   The script reports imported, preserved and previously processed account counts.
   It copies a stable read-only SQLite snapshot, takes Rust's PostgreSQL storage
   lock, preserves existing remote accounts and updates accounting via the
   destination triggers. Missing/inconsistent accounting fails the transaction.
   Persistent markers prevent repeated runs from resurrecting remote account
   wipes. Credentials are not command arguments, and payload values are not logged.

   A connection failure during commit can leave the outcome unknown. Rerunning
   safely resolves that uncertainty using the destination accounts and markers.
6. Start the production server with PostgreSQL selected, verify `/storage` and
   sample accounts, then admit players. Keep backups until verification is complete.

The script does not reconcile divergent saves or maintain ongoing replication.
If an account exists on both sides, the entire remote account is preserved. Decide
which dataset is authoritative before migrating; do not alternate live traffic
between PostgreSQL and the separate SQLite fallback database.
