//! Reconcile on startup; row triggers keep usage exact for every later mutation.
use crate::player::PlayerRecord;
use anyhow::Result;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row, SqlitePool};

pub(super) fn usage_table(table: &str) -> String {
    // Unquoted SQL identifiers are case-insensitive. Hashing also keeps names
    // below PostgreSQL's identifier limit even for long configured table names.
    let digest = Sha256::digest(table.to_ascii_lowercase().as_bytes());
    let suffix: String = digest[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("basis_storage_usage_{suffix}")
}

pub(super) async fn init_postgres(pool: &PgPool, table: &str) -> Result<()> {
    let usage = usage_table(table);
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(813411, 20261025)")
        .execute(&mut *tx)
        .await?;
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS {table} (owner TEXT PRIMARY KEY, payload BYTEA NOT NULL)"
    ))
    .execute(&mut *tx)
    .await?;
    // Block outside writers until the counter and its triggers are installed.
    sqlx::query(&format!("LOCK TABLE {table} IN SHARE ROW EXCLUSIVE MODE"))
        .execute(&mut *tx)
        .await?;
    sqlx::query(&format!("CREATE TABLE IF NOT EXISTS {usage} (id INTEGER PRIMARY KEY CHECK (id=1), used BIGINT NOT NULL CHECK (used>=0))")).execute(&mut *tx).await?;
    sqlx::query(&format!("INSERT INTO {usage} (id, used) SELECT 1, COALESCE(SUM(octet_length(payload)),0) FROM {table} ON CONFLICT(id) DO UPDATE SET used=EXCLUDED.used")).execute(&mut *tx).await?;
    let function = format!("{usage}_fn");
    sqlx::query(&format!(
        r#"
        CREATE OR REPLACE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE delta BIGINT;
        BEGIN
            IF TG_OP='INSERT' THEN delta := octet_length(NEW.payload);
            ELSIF TG_OP='DELETE' THEN delta := -octet_length(OLD.payload);
            ELSE delta := octet_length(NEW.payload)-octet_length(OLD.payload);
            END IF;
            UPDATE {usage} SET used=used+delta WHERE id=1;
            RETURN NULL;
        END $$
    "#
    ))
    .execute(&mut *tx)
    .await?;
    sqlx::query(&format!("DROP TRIGGER IF EXISTS {usage}_tr ON {table}"))
        .execute(&mut *tx)
        .await?;
    sqlx::query(&format!("CREATE TRIGGER {usage}_tr AFTER INSERT OR UPDATE OR DELETE ON {table} FOR EACH ROW EXECUTE FUNCTION {function}()")).execute(&mut *tx).await?;

    // Remove legacy empty accounts in bounded pages; valid nonempty/corrupt
    // payloads are left intact. Trigger deltas update the reconciled total.
    let mut cursor = String::new();
    loop {
        let rows = sqlx::query(&format!(
            "SELECT owner,payload FROM {table} WHERE owner>$1 ORDER BY owner LIMIT 128"
        ))
        .bind(&cursor)
        .fetch_all(&mut *tx)
        .await?;
        if rows.is_empty() {
            break;
        }
        for row in rows {
            let owner: String = row.try_get("owner")?;
            let bytes: Vec<u8> = row.try_get("payload")?;
            if is_empty(&owner, &bytes) {
                sqlx::query(&format!("DELETE FROM {table} WHERE owner=$1"))
                    .bind(&owner)
                    .execute(&mut *tx)
                    .await?;
            }
            cursor = owner;
        }
    }
    tx.commit().await?;
    Ok(())
}

pub(super) async fn init_sqlite(pool: &SqlitePool, table: &str) -> Result<()> {
    let usage = usage_table(table);
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS {table} (owner TEXT PRIMARY KEY, payload BLOB NOT NULL)"
    ))
    .execute(&mut *tx)
    .await?;
    sqlx::query(&format!("CREATE TABLE IF NOT EXISTS {usage} (id INTEGER PRIMARY KEY CHECK (id=1), used INTEGER NOT NULL CHECK (used>=0))")).execute(&mut *tx).await?;
    // WHERE true disambiguates SQLite's INSERT SELECT ... ON CONFLICT grammar.
    sqlx::query(&format!("INSERT INTO {usage} (id, used) SELECT 1, COALESCE(SUM(length(payload)),0) FROM {table} WHERE true ON CONFLICT(id) DO UPDATE SET used=excluded.used")).execute(&mut *tx).await?;
    for (event, delta) in [
        ("insert", "length(NEW.payload)"),
        ("update", "length(NEW.payload)-length(OLD.payload)"),
        ("delete", "-length(OLD.payload)"),
    ] {
        sqlx::query(&format!("CREATE TRIGGER IF NOT EXISTS {usage}_{event} AFTER {event} ON {table} BEGIN UPDATE {usage} SET used=used+({delta}) WHERE id=1; END")).execute(&mut *tx).await?;
    }
    let mut cursor = String::new();
    loop {
        let rows = sqlx::query(&format!(
            "SELECT owner,payload FROM {table} WHERE owner>?1 ORDER BY owner LIMIT 128"
        ))
        .bind(&cursor)
        .fetch_all(&mut *tx)
        .await?;
        if rows.is_empty() {
            break;
        }
        for row in rows {
            let owner: String = row.try_get("owner")?;
            let bytes: Vec<u8> = row.try_get("payload")?;
            if is_empty(&owner, &bytes) {
                sqlx::query(&format!("DELETE FROM {table} WHERE owner=?1"))
                    .bind(&owner)
                    .execute(&mut *tx)
                    .await?;
            }
            cursor = owner;
        }
    }
    tx.commit().await?;
    Ok(())
}

fn is_empty(owner: &str, bytes: &[u8]) -> bool {
    serde_json::from_slice::<PlayerRecord>(bytes)
        .is_ok_and(|record| record.owner == owner && record.scopes.is_empty())
}
