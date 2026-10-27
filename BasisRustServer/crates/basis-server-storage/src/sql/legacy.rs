//! Read-only import of BasisVR C# BSTR v1 files into persistent SQLite only.
//! Format reference: BasisVR/Basis acfca6b1, BasisStorageRecord.Serialize/TryParse.
use crate::player::PlayerRecord;
use anyhow::{ensure, Context, Result};
use basis_protocol::storage::{valid_name, MAX_BYTES_PER_PLAYER, MAX_NAME_BYTES};
use sha2::{Digest, Sha256};
use sqlx::SqlitePool;
use std::{
    collections::BTreeMap,
    fs,
    io::Read,
    path::{Path, PathBuf},
};

const MAX_BLOB: usize = 18 + 256 + 2 + MAX_BYTES_PER_PLAYER + 32;

#[derive(Debug, Default, Clone, Copy)]
pub struct LegacyImportReport {
    pub imported: u64,
    pub preserved: u64,
    pub empty: u64,
    pub previously_imported: u64,
}

struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}
impl<'a> Reader<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .at
            .checked_add(length)
            .context("BSTR length overflow")?;
        let bytes = self
            .bytes
            .get(self.at..end)
            .context("truncated BSTR record")?;
        self.at = end;
        Ok(bytes)
    }
    fn u16(&mut self) -> Result<usize> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into()?) as usize)
    }
    fn name(&mut self) -> Result<String> {
        let length = self.take(1)?[0] as usize;
        ensure!(
            length > 0 && length <= MAX_NAME_BYTES,
            "invalid BSTR name length"
        );
        let name = std::str::from_utf8(self.take(length)?)?;
        ensure!(valid_name(name), "invalid BSTR name");
        Ok(name.to_owned())
    }
}

fn decode(blob: &[u8], owner_key: &str) -> Result<PlayerRecord> {
    ensure!(
        (52..=MAX_BLOB).contains(&blob.len()),
        "invalid BSTR record size"
    );
    let end = blob.len() - 32;
    ensure!(
        &blob[..8] == b"BSTR\x01\0\0\0",
        "unsupported BSTR header/version"
    );
    ensure!(
        Sha256::digest(&blob[..end])[..] == blob[end..],
        "BSTR checksum mismatch"
    );
    ensure!(
        i64::from_le_bytes(blob[8..16].try_into()?) >= 0,
        "negative BSTR revision"
    );
    let mut reader = Reader {
        bytes: &blob[..end],
        at: 16,
    };
    let owner_length = reader.u16()?;
    ensure!(
        (1..=256).contains(&owner_length),
        "invalid BSTR owner length"
    );
    let owner = std::str::from_utf8(reader.take(owner_length)?)?.to_owned();
    ensure!(
        format!("{:x}", Sha256::digest(owner.as_bytes())) == owner_key,
        "BSTR owner/filename hash mismatch"
    );
    let count = reader.u16()?;
    ensure!(count <= 64, "too many BSTR scopes");
    let mut scopes = BTreeMap::new();
    let mut entries = 0;
    for _ in 0..count {
        let scope = reader.name()?;
        let count = reader.u16()?;
        entries += count;
        ensure!(count > 0 && entries <= 1024, "invalid BSTR entry count");
        let mut values = BTreeMap::new();
        for _ in 0..count {
            let key = reader.name()?;
            let length = reader.u16()?;
            let value = reader.take(length)?.to_vec();
            ensure!(values.insert(key, value).is_none(), "duplicate BSTR key");
        }
        ensure!(
            scopes.insert(scope, values).is_none(),
            "duplicate BSTR scope"
        );
    }
    ensure!(reader.at == end, "trailing BSTR bytes");
    let record = PlayerRecord { owner, scopes };
    ensure!(
        record.used() <= MAX_BYTES_PER_PLAYER,
        "BSTR player limit exceeded"
    );
    Ok(record)
}

fn candidates(root: &Path) -> Result<Vec<(String, PathBuf)>> {
    let mut result = Vec::new();
    let folders = match fs::read_dir(root) {
        Ok(folders) => folders,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(result),
        Err(error) => return Err(error.into()),
    };
    for folder in folders {
        let folder = folder?;
        let prefix = folder.file_name().to_string_lossy().into_owned();
        if prefix.len() != 2 || !prefix.bytes().all(is_hex) {
            continue;
        }
        ensure!(
            folder.file_type()?.is_dir(),
            "legacy hash directory is not a regular directory: {}",
            folder.path().display()
        );
        for file in fs::read_dir(folder.path())? {
            let file = file?;
            let path = file.path();
            if path.extension().is_none_or(|ext| ext != "bsr") {
                continue;
            }
            ensure!(
                file.file_type()?.is_file(),
                "legacy BSTR path is not a regular file: {}",
                path.display()
            );
            let key = path
                .file_stem()
                .and_then(|name| name.to_str())
                .context("invalid legacy filename")?;
            ensure!(
                key.len() == 64 && key.bytes().all(is_hex) && key.starts_with(&prefix),
                "invalid legacy owner hash path: {}",
                path.display()
            );
            result.push((key.to_owned(), path));
        }
    }
    result.sort_unstable();
    Ok(result)
}
fn is_hex(byte: u8) -> bool {
    byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
}
fn read_record(path: &Path, key: &str) -> Result<PlayerRecord> {
    let mut blob = Vec::new();
    fs::File::open(path)?
        .take((MAX_BLOB + 1) as u64)
        .read_to_end(&mut blob)?;
    decode(&blob, key).with_context(|| format!("invalid legacy save {}", path.display()))
}

pub(super) async fn import(
    pool: &SqlitePool,
    table: &str,
    root: PathBuf,
) -> Result<LegacyImportReport> {
    let files = tokio::task::spawn_blocking(move || candidates(&root)).await??;
    let mut report = LegacyImportReport::default();
    if files.is_empty() {
        return Ok(report);
    }
    let ledger = format!("{}_legacy", super::accounting::usage_table(table));
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS {ledger} (owner_key TEXT PRIMARY KEY NOT NULL)"
    ))
    .execute(&mut *tx)
    .await?;
    for (key, path) in files {
        let seen: i64 =
            sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {ledger} WHERE owner_key=?1"))
                .bind(&key)
                .fetch_one(&mut *tx)
                .await?;
        if seen != 0 {
            report.previously_imported += 1;
            continue;
        }
        let read_key = key.clone();
        let record = tokio::task::spawn_blocking(move || read_record(&path, &read_key)).await??;
        if record.scopes.is_empty() {
            report.empty += 1;
        } else {
            let inserted = sqlx::query(&format!(
                "INSERT INTO {table} (owner,payload) VALUES (?1,?2) ON CONFLICT(owner) DO NOTHING"
            ))
            .bind(&record.owner)
            .bind(serde_json::to_vec(&record)?)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            if inserted == 1 {
                report.imported += 1;
            } else {
                report.preserved += 1;
            }
        }
        // Mark both imported and preserved accounts. Never resurrect a later wipe.
        sqlx::query(&format!("INSERT INTO {ledger} (owner_key) VALUES (?1)"))
            .bind(key)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{admin::StorageAdminAction, player::PlayerStorage, sql::SqlPlayerStorage};
    use basis_protocol::storage::{StorageOp, StorageRequest, StorageStatus};
    const GOLDEN: &[u8] = include_bytes!("../../tests/fixtures/basisvr-v1.bsr");
    const OWNER: &str = "did:key:legacy-fixture";
    fn key(owner: &str) -> String {
        format!("{:x}", Sha256::digest(owner.as_bytes()))
    }
    fn resign(blob: &mut [u8]) {
        let end = blob.len() - 32;
        let checksum = Sha256::digest(&blob[..end]);
        blob[end..].copy_from_slice(&checksum);
    }
    fn renamed(owner: &str) -> Vec<u8> {
        let old_length = u16::from_le_bytes(GOLDEN[16..18].try_into().unwrap()) as usize;
        let mut blob = GOLDEN[..16].to_vec();
        blob.extend((owner.len() as u16).to_le_bytes());
        blob.extend(owner.as_bytes());
        blob.extend(&GOLDEN[18 + old_length..]);
        resign(&mut blob);
        blob
    }
    fn install(root: &Path, owner: &str, blob: &[u8]) -> PathBuf {
        let key = key(owner);
        let folder = root.join("players").join(&key[..2]);
        fs::create_dir_all(&folder).unwrap();
        let path = folder.join(format!("{key}.bsr"));
        fs::write(&path, blob).unwrap();
        path
    }
    fn request(op: StorageOp, value: Vec<u8>) -> StorageRequest {
        StorageRequest {
            op,
            id: 1,
            scope: "save:世界".into(),
            key: "score".into(),
            value,
        }
    }

    #[test]
    fn reads_actual_csharp_serializer_fixture_and_rejects_corruption() {
        let record = decode(GOLDEN, &key(OWNER)).unwrap();
        assert_eq!(record.owner, OWNER);
        assert_eq!(record.scopes["save:世界"]["score"], [0, 1, 255]);
        assert_eq!(record.scopes["save:世界"]["empty"], Vec::<u8>::new());
        assert_eq!(record.scopes["other"]["blob"], [7, 8]);
        assert!(decode(GOLDEN, &key("wrong-owner")).is_err());
        for end in 0..GOLDEN.len() {
            assert!(decode(&GOLDEN[..end], &key(OWNER)).is_err());
        }
        for at in 0..GOLDEN.len() {
            let mut bad = GOLDEN.to_vec();
            bad[at] ^= 1;
            assert!(decode(&bad, &key(OWNER)).is_err());
        }
        for (at, value) in [(4, 2), (5, 1), (15, 255), (16, 0)] {
            let mut bad = GOLDEN.to_vec();
            bad[at] = value;
            resign(&mut bad);
            assert!(decode(&bad, &key(OWNER)).is_err());
        }
    }

    #[tokio::test]
    async fn sqlite_import_preserves_existing_saves_and_never_resurrects_wipes() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("players.sqlite3");
        let (existing, _) = SqlPlayerStorage::connect("", Some(&db), "basis_storage")
            .await
            .unwrap();
        assert_eq!(
            existing
                .execute(OWNER, request(StorageOp::Set, vec![9]), 16384, 0)
                .await
                .status,
            StorageStatus::Ok
        );
        let source = install(dir.path(), OWNER, GOLDEN);
        let (storage, _) = SqlPlayerStorage::connect("", Some(&db), "basis_storage")
            .await
            .unwrap();
        assert_eq!(storage.legacy_import_report().preserved, 1);
        assert_eq!(
            storage
                .execute(OWNER, request(StorageOp::Get, vec![]), 16384, 0)
                .await
                .value,
            [9]
        );
        storage
            .admin(StorageAdminAction::Wipe(OWNER.into()))
            .await
            .unwrap();
        let (restarted, _) = SqlPlayerStorage::connect("", Some(&db), "basis_storage")
            .await
            .unwrap();
        assert_eq!(restarted.legacy_import_report().previously_imported, 1);
        assert_eq!(
            restarted
                .execute(OWNER, request(StorageOp::Get, vec![]), 16384, 0)
                .await
                .status,
            StorageStatus::NotFound
        );
        assert_eq!(fs::read(source).unwrap(), GOLDEN);
    }

    #[tokio::test]
    async fn empty_legacy_accounts_are_marked_without_retaining_empty_rows() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("players.sqlite3");
        let owner_length = u16::from_le_bytes(GOLDEN[16..18].try_into().unwrap()) as usize;
        let mut empty = GOLDEN[..18 + owner_length].to_vec();
        empty.extend(0u16.to_le_bytes());
        empty.extend([0; 32]);
        resign(&mut empty);
        let source = install(dir.path(), OWNER, &empty);
        let (storage, _) = SqlPlayerStorage::connect("", Some(&db), "basis_storage")
            .await
            .unwrap();
        assert_eq!(storage.legacy_import_report().empty, 1);
        assert_eq!(super::super::tests::sqlite_usage(&storage).await, (0, 0, 0));
        fs::write(source, GOLDEN).unwrap();
        let (again, _) = SqlPlayerStorage::connect("", Some(&db), "basis_storage")
            .await
            .unwrap();
        assert_eq!(again.legacy_import_report().previously_imported, 1);
        assert_eq!(super::super::tests::sqlite_usage(&again).await, (0, 0, 0));
    }

    #[tokio::test]
    async fn migration_is_atomic_accounted_and_sqlite_file_only() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("players.sqlite3");
        let other = "did:key:another-legacy-owner";
        let second = renamed(other);
        let first_path = install(dir.path(), OWNER, GOLDEN);
        let second_path = install(dir.path(), other, &second);
        // Corrupt the last sorted account, so an earlier insertion must roll back.
        let bad_path = if key(OWNER) > key(other) {
            &first_path
        } else {
            &second_path
        };
        fs::write(bad_path, b"corrupt").unwrap();
        assert!(SqlPlayerStorage::connect("", Some(&db), "basis_storage")
            .await
            .is_err());
        let pool = SqlitePool::connect(&format!("sqlite://{}", db.display()))
            .await
            .unwrap();
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM basis_storage")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
        pool.close().await;
        fs::write(first_path, GOLDEN).unwrap();
        fs::write(second_path, &second).unwrap();
        let (storage, _) = SqlPlayerStorage::connect("", Some(&db), "basis_storage")
            .await
            .unwrap();
        assert_eq!(storage.legacy_import_report().imported, 2);
        let usage = super::super::tests::sqlite_usage(&storage).await;
        assert_eq!(usage.0, usage.1);
        assert_eq!(usage.2, 2);
        assert_eq!(
            storage
                .execute(OWNER, request(StorageOp::Get, vec![]), 16384, 0)
                .await
                .value,
            [0, 1, 255]
        );
        let (memory, _) = SqlPlayerStorage::connect("", None, "basis_storage")
            .await
            .unwrap();
        assert_eq!(memory.legacy_import_report().imported, 0);
        let files = PlayerStorage::new(Some(dir.path().join("players"))).unwrap();
        assert_eq!(
            files
                .execute(OWNER, request(StorageOp::Get, vec![]), 16384, 0)
                .status,
            StorageStatus::NotFound
        );
    }
}
