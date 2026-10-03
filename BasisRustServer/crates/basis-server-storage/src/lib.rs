use anyhow::{Context, Result};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashMap,
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BasisData {
    pub name: String,
    pub json_payload: Value,
}

#[derive(Debug, Clone, Default)]
pub struct PersistentDatabase {
    path: Option<PathBuf>,
    data: Arc<RwLock<HashMap<String, BasisData>>>,
}

impl PersistentDatabase {
    pub fn in_memory() -> Self {
        Self::default()
    }

    pub fn file_backed(path: impl Into<PathBuf>) -> Self {
        Self {
            path: Some(path.into()),
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn load(&self) -> Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        if !path.exists() {
            return Ok(());
        }
        let text = fs::read_to_string(path)?;
        let values: Vec<BasisData> = serde_json::from_str(&text)?;
        let mut data = self.data.write();
        data.clear();
        for value in values {
            data.insert(value.name.clone(), value);
        }
        Ok(())
    }

    pub fn add_or_update(&self, value: BasisData) {
        self.data.write().insert(value.name.clone(), value);
    }

    pub fn get(&self, name: &str) -> Option<BasisData> {
        self.data.read().get(name).cloned()
    }

    pub fn shutdown(&self) -> Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let values: Vec<_> = self.data.read().values().cloned().collect();
        let json = serde_json::to_string_pretty(&values)?;
        atomic_write_with(path, |file| file.write_all(json.as_bytes()), sync_directory)
    }
}

// Adapt the moderation/permissions same-directory write-sync-replace pattern.
// Unique, exclusively created temporary files avoid clobbering another save or
// an interrupted save. RAII removes this save's temporary file on any failure.
fn atomic_write_with(
    path: &Path,
    write: impl FnOnce(&mut fs::File) -> io::Result<()>,
    sync_parent: impl FnOnce(&Path) -> io::Result<()>,
) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::create_dir_all(parent)
        .with_context(|| format!("creating database directory {}", parent.display()))?;
    let mut temporary = tempfile::Builder::new()
        .prefix(".basis-database-")
        .suffix(".tmp")
        .tempfile_in(parent)
        .with_context(|| format!("creating temporary database in {}", parent.display()))?;
    write(temporary.as_file_mut())
        .with_context(|| format!("writing temporary database for {}", path.display()))?;
    temporary
        .as_file_mut()
        .flush()
        .with_context(|| format!("flushing temporary database for {}", path.display()))?;
    temporary
        .as_file()
        .sync_all()
        .with_context(|| format!("syncing temporary database for {}", path.display()))?;

    // Close before replacement. tempfile uses overwrite-capable rename on Unix
    // and MoveFileExW(REPLACE_EXISTING) on Windows, never delete-then-rename.
    // Dropping the TempPath from a failed persist removes the uncommitted file.
    temporary
        .into_temp_path()
        .persist(path)
        .map_err(|error| error.error)
        .with_context(|| format!("replacing database {}", path.display()))?;
    // Replacement has committed. Report durability failure without implying
    // that the old snapshot is still present, and never roll back/delete it.
    sync_parent(parent).with_context(|| {
        format!(
        "database {} was replaced, but syncing directory {} failed; crash durability is uncertain",
        path.display(), parent.display()
    )
    })?;
    Ok(())
}

fn sync_directory(parent: &Path) -> io::Result<()> {
    #[cfg(unix)]
    fs::File::open(parent)?.sync_all()?;
    // Windows does not offer the Unix directory-fsync contract. The temporary
    // file is synced before its atomic replacement on all supported platforms.
    #[cfg(not(unix))]
    let _ = parent;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn value(name: &str, payload: Value) -> BasisData {
        BasisData {
            name: name.into(),
            json_payload: payload,
        }
    }

    fn assert_no_temporary_files(directory: &Path) {
        assert!(fs::read_dir(directory).unwrap().all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".basis-database-")));
    }

    #[test]
    fn shutdown_roundtrips_pretty_json_and_creates_parent_directory() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config/database.json");
        let database = PersistentDatabase::file_backed(&path);
        let entry = value("world", json!({"nested": [true, 42, "hello"]}));
        database.add_or_update(entry.clone());
        database.shutdown().unwrap();
        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            serde_json::to_string_pretty(&vec![entry]).unwrap()
        );
        let loaded = PersistentDatabase::file_backed(&path);
        loaded.load().unwrap();
        assert_eq!(
            loaded.get("world").unwrap().json_payload,
            json!({"nested": [true, 42, "hello"]})
        );
        assert_no_temporary_files(path.parent().unwrap());
    }

    #[test]
    fn shutdown_replaces_existing_snapshot_and_can_save_again() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("database.json");
        let old = PersistentDatabase::file_backed(&path);
        old.add_or_update(value("old", json!(1)));
        old.shutdown().unwrap();
        let new = PersistentDatabase::file_backed(&path);
        for generation in 2..=3 {
            new.add_or_update(value("new", json!(generation)));
            new.shutdown().unwrap();
            let loaded = PersistentDatabase::file_backed(&path);
            loaded.load().unwrap();
            assert!(loaded.get("old").is_none());
            assert_eq!(loaded.get("new").unwrap().json_payload, json!(generation));
            assert_no_temporary_files(directory.path());
        }
    }

    #[test]
    fn partial_write_failure_retains_old_file_cleans_temp_and_allows_retry() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("database.json");
        let database = PersistentDatabase::file_backed(&path);
        database.add_or_update(value("saved", json!("old")));
        database.shutdown().unwrap();
        let old = fs::read(&path).unwrap();
        let error = atomic_write_with(
            &path,
            |file| {
                file.write_all(b"[{")?;
                assert_eq!(fs::read(&path).unwrap(), old);
                Err(io::Error::other("injected write failure"))
            },
            |_| panic!("must not sync directory before replacement"),
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("injected write failure"));
        assert_eq!(fs::read(&path).unwrap(), old);
        let loaded = PersistentDatabase::file_backed(&path);
        loaded.load().unwrap();
        assert_eq!(loaded.get("saved").unwrap().json_payload, json!("old"));
        assert_no_temporary_files(directory.path());
        database.add_or_update(value("saved", json!("retry")));
        database.shutdown().unwrap();
        loaded.load().unwrap();
        assert_eq!(loaded.get("saved").unwrap().json_payload, json!("retry"));
        assert_no_temporary_files(directory.path());
    }

    #[test]
    fn replacement_failure_preserves_target_and_cleans_temp() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("database.json");
        fs::create_dir(&path).unwrap();
        fs::write(path.join("sentinel"), b"keep").unwrap();
        let database = PersistentDatabase::file_backed(&path);
        database.add_or_update(value("new", json!(1)));
        let error = database.shutdown().unwrap_err();
        assert!(error.to_string().contains("replacing database"));
        assert_eq!(fs::read(path.join("sentinel")).unwrap(), b"keep");
        assert_no_temporary_files(directory.path());
    }

    #[test]
    fn interrupted_save_is_ignored_without_clobbering_its_temporary_file() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("database.json");
        let database = PersistentDatabase::file_backed(&path);
        database.add_or_update(value("saved", json!("old")));
        database.shutdown().unwrap();
        let orphan = directory.path().join(".basis-database-interrupted.tmp");
        fs::write(&orphan, b"[{").unwrap();
        let loaded = PersistentDatabase::file_backed(&path);
        loaded.load().unwrap();
        assert_eq!(loaded.get("saved").unwrap().json_payload, json!("old"));
        loaded.add_or_update(value("saved", json!("new")));
        loaded.shutdown().unwrap();
        assert_eq!(fs::read(&orphan).unwrap(), b"[{");
        fs::remove_file(orphan).unwrap();
        assert_no_temporary_files(directory.path());
    }

    #[test]
    fn directory_sync_failure_reports_committed_valid_snapshot() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("database.json");
        fs::write(&path, b"[]").unwrap();
        let bytes = serde_json::to_vec_pretty(&vec![value("new", json!(1))]).unwrap();
        let error = atomic_write_with(
            &path,
            |file| file.write_all(&bytes),
            |_| Err(io::Error::other("injected directory sync failure")),
        )
        .unwrap_err();
        assert!(error.to_string().contains("was replaced"));
        assert_eq!(fs::read(&path).unwrap(), bytes);
        let loaded = PersistentDatabase::file_backed(&path);
        loaded.load().unwrap();
        assert_eq!(loaded.get("new").unwrap().json_payload, json!(1));
        assert_no_temporary_files(directory.path());
    }

    #[test]
    fn concurrent_saves_use_distinct_temporary_files_and_commit_whole_snapshots() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("database.json");
        fs::write(&path, b"[]").unwrap();
        let ready = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            for name in ["first", "second"] {
                let path = &path;
                let ready = &ready;
                scope.spawn(move || {
                    let bytes = serde_json::to_vec_pretty(&vec![value(name, json!(name))]).unwrap();
                    atomic_write_with(
                        path,
                        |file| {
                            file.write_all(&bytes)?;
                            // Both writes overlap before either snapshot is committed.
                            ready.wait();
                            Ok(())
                        },
                        sync_directory,
                    )
                    .unwrap();
                });
            }
        });
        let entries: Vec<BasisData> = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(entries.len(), 1);
        assert!(["first", "second"].contains(&entries[0].name.as_str()));
        assert_eq!(entries[0].json_payload, json!(entries[0].name));
        assert_no_temporary_files(directory.path());
    }

    #[test]
    fn memory_only_database_load_and_shutdown_preserve_shared_values() {
        let database = PersistentDatabase::in_memory();
        database.add_or_update(value("memory", json!(true)));
        let clone = database.clone();
        clone.shutdown().unwrap();
        clone.load().unwrap();
        assert!(clone.path.is_none());
        assert_eq!(database.get("memory").unwrap().json_payload, json!(true));
    }

    #[cfg(windows)]
    #[test]
    fn sharing_violation_retains_old_valid_file_and_cleans_temp() {
        use std::os::windows::fs::OpenOptionsExt;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("database.json");
        let database = PersistentDatabase::file_backed(&path);
        database.add_or_update(value("saved", json!("old")));
        database.shutdown().unwrap();
        let old = fs::read(&path).unwrap();
        let locked = fs::OpenOptions::new()
            .read(true)
            .share_mode(0)
            .open(&path)
            .unwrap();
        database.add_or_update(value("saved", json!("new")));
        assert!(database.shutdown().is_err());
        drop(locked);
        assert_eq!(fs::read(&path).unwrap(), old);
        assert_no_temporary_files(directory.path());
        database.shutdown().unwrap();
        let loaded = PersistentDatabase::file_backed(&path);
        loaded.load().unwrap();
        assert_eq!(loaded.get("saved").unwrap().json_payload, json!("new"));
    }
}
