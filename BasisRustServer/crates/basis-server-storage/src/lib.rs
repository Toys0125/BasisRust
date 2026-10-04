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

#[cfg(windows)]
mod windows_metadata;

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
    // Follow a configured file symlink, including a dangling link to a new
    // snapshot, just as the original direct write did. Replace its destination.
    let resolved = resolve_database_path(path)
        .with_context(|| format!("resolving database {}", path.display()))?;
    let path = resolved.as_path();
    let metadata = SavedMetadata::read(path)
        .with_context(|| format!("inspecting existing database {}", path.display()))?;
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
    if let Some(metadata) = metadata {
        metadata
            .apply(&temporary)
            .with_context(|| format!("preserving database metadata for {}", path.display()))?;
    }
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

fn resolve_database_path(path: &Path) -> io::Result<PathBuf> {
    let mut resolved = path.to_path_buf();
    for _ in 0..40 {
        match fs::symlink_metadata(&resolved) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                let target = fs::read_link(&resolved)?;
                resolved = if target.is_absolute() {
                    target
                } else {
                    resolved.parent().unwrap_or(Path::new(".")).join(target)
                };
            }
            Ok(_) => return Ok(resolved),
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(resolved),
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::other("too many database symlinks"))
}

struct SavedMetadata {
    metadata: fs::Metadata,
    #[cfg(unix)]
    attributes: Vec<(std::ffi::OsString, Vec<u8>)>,
    #[cfg(windows)]
    security: windows_metadata::Security,
}

impl SavedMetadata {
    fn read(path: &Path) -> io::Result<Option<Self>> {
        // Check write access without truncation, preserving the old writer's
        // access checks even when the containing directory allows replacement.
        let file = match fs::OpenOptions::new().write(true).open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let metadata = file.metadata()?;
        #[cfg(unix)]
        let attributes = {
            use xattr::FileExt;
            list_attributes(&file)?
                .into_iter()
                .map(|name| {
                    let bytes = file.get_xattr(&name)?.ok_or_else(|| {
                        io::Error::other("database attribute changed while inspecting metadata")
                    })?;
                    Ok((name, bytes))
                })
                .collect::<io::Result<Vec<_>>>()?
        };
        #[cfg(windows)]
        let security = windows_metadata::Security::read(path)?;
        Ok(Some(Self {
            metadata,
            #[cfg(unix)]
            attributes,
            #[cfg(windows)]
            security,
        }))
    }

    fn apply(self, temporary: &tempfile::NamedTempFile) -> io::Result<()> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::{fchown, MetadataExt};
            use xattr::FileExt;
            let file = temporary.as_file();
            let current = file.metadata()?;
            let uid = (current.uid() != self.metadata.uid()).then_some(self.metadata.uid());
            let gid = (current.gid() != self.metadata.gid()).then_some(self.metadata.gid());
            if uid.is_some() || gid.is_some() {
                fchown(file, uid, gid)?;
            }
            // chown and writes may clear mode bits; restore them afterward.
            file.set_permissions(self.metadata.permissions())?;
            // Access ACLs and other extended attributes must not be replaced
            // with the defaults inherited by the newly created temporary file.
            for name in list_attributes(file)? {
                if !self.attributes.iter().any(|(saved, _)| *saved == name) {
                    file.remove_xattr(name)?;
                }
            }
            for (name, bytes) in self.attributes {
                file.set_xattr(name, &bytes)?;
            }
        }
        #[cfg(windows)]
        {
            // A read-only target fails the write-access check before any save.
            // Copy its access policy before committing the replacement.
            let _ = self.metadata;
            self.security.apply(temporary.path())?;
        }
        #[cfg(not(any(unix, windows)))]
        temporary
            .as_file()
            .set_permissions(self.metadata.permissions())?;
        Ok(())
    }
}

#[cfg(unix)]
fn list_attributes(file: &fs::File) -> io::Result<Vec<std::ffi::OsString>> {
    use xattr::FileExt;
    match file.list_xattr() {
        Ok(attributes) => Ok(attributes.collect()),
        // Both snapshots live on the same filesystem. A filesystem without
        // extended-attribute support cannot have access ACLs to copy this way.
        Err(error) if error.kind() == io::ErrorKind::Unsupported => Ok(Vec::new()),
        Err(error) => Err(error),
    }
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
        let error = atomic_write_with(
            &path,
            |file| {
                // Another actor creates a directory after the destination check.
                fs::create_dir(&path)?;
                fs::write(path.join("sentinel"), b"keep")?;
                file.write_all(b"[]")
            },
            sync_directory,
        )
        .unwrap_err();
        assert!(error.to_string().contains("replacing database"));
        assert_eq!(fs::read(path.join("sentinel")).unwrap(), b"keep");
        assert_no_temporary_files(directory.path());
    }

    #[cfg(any(unix, windows))]
    fn symlink_file(target: &Path, link: &Path) {
        #[cfg(unix)]
        std::os::unix::fs::symlink(target, link).unwrap();
        #[cfg(windows)]
        std::os::windows::fs::symlink_file(target, link).unwrap();
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn shutdown_preserves_relative_and_absolute_symlinks_and_replaces_destination() {
        let directory = tempfile::tempdir().unwrap();
        let shared = directory.path().join("shared");
        let config = directory.path().join("config");
        fs::create_dir(&shared).unwrap();
        fs::create_dir(&config).unwrap();
        let destination = shared.join("snapshot.json");
        fs::write(&destination, b"[]").unwrap();
        let indirect = config.join("indirect.json");
        let configured = config.join("database.json");
        symlink_file(Path::new("../shared/snapshot.json"), &indirect);
        symlink_file(&indirect, &configured);
        let original_link = fs::read_link(&configured).unwrap();
        let database = PersistentDatabase::file_backed(&configured);
        database.add_or_update(value("saved", json!("new")));
        database.shutdown().unwrap();
        assert_eq!(fs::read_link(&configured).unwrap(), original_link);
        assert!(fs::symlink_metadata(&indirect)
            .unwrap()
            .file_type()
            .is_symlink());
        let loaded = PersistentDatabase::file_backed(&destination);
        loaded.load().unwrap();
        assert_eq!(loaded.get("saved").unwrap().json_payload, json!("new"));
        assert_no_temporary_files(&shared);
        assert_no_temporary_files(&config);
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn shutdown_creates_dangling_symlink_destination_without_removing_link() {
        let directory = tempfile::tempdir().unwrap();
        let configured = directory.path().join("database.json");
        let destination = directory.path().join("new/snapshot.json");
        symlink_file(Path::new("new/snapshot.json"), &configured);
        let database = PersistentDatabase::file_backed(&configured);
        database.load().unwrap();
        database.add_or_update(value("saved", json!(1)));
        database.shutdown().unwrap();
        assert!(fs::symlink_metadata(&configured)
            .unwrap()
            .file_type()
            .is_symlink());
        let loaded = PersistentDatabase::file_backed(&destination);
        loaded.load().unwrap();
        assert_eq!(loaded.get("saved").unwrap().json_payload, json!(1));
        assert_no_temporary_files(directory.path());
        assert_no_temporary_files(destination.parent().unwrap());
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn symlink_cycle_fails_without_modifying_links_or_creating_temps() {
        let directory = tempfile::tempdir().unwrap();
        let first = directory.path().join("database.json");
        let second = directory.path().join("other.json");
        symlink_file(Path::new("other.json"), &first);
        symlink_file(Path::new("database.json"), &second);
        let database = PersistentDatabase::file_backed(&first);
        assert!(database.shutdown().is_err());
        assert_eq!(fs::read_link(&first).unwrap(), Path::new("other.json"));
        assert_eq!(fs::read_link(&second).unwrap(), Path::new("database.json"));
        assert_no_temporary_files(directory.path());
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn failed_save_retains_symlink_and_old_valid_destination() {
        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("snapshot.json");
        let configured = directory.path().join("database.json");
        fs::write(&destination, b"[]").unwrap();
        symlink_file(Path::new("snapshot.json"), &configured);
        assert!(atomic_write_with(
            &configured,
            |file| {
                file.write_all(b"[{")?;
                Err(io::Error::other("injected write failure"))
            },
            sync_directory
        )
        .is_err());
        assert_eq!(
            fs::read_link(&configured).unwrap(),
            Path::new("snapshot.json")
        );
        assert_eq!(fs::read(&destination).unwrap(), b"[]");
        assert_no_temporary_files(directory.path());
    }

    #[cfg(unix)]
    #[test]
    fn replacement_preserves_mode_owner_group_and_extended_attributes() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("database.json");
        let database = PersistentDatabase::file_backed(&path);
        database.shutdown().unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).unwrap();
        xattr::set(&path, "user.basis_storage_test", b"keep").unwrap();
        let old = fs::metadata(&path).unwrap();
        database.add_or_update(value("saved", json!(1)));
        database.shutdown().unwrap();
        let new = fs::metadata(&path).unwrap();
        assert_eq!(new.mode() & 0o7777, 0o640);
        assert_eq!(new.uid(), old.uid());
        assert_eq!(new.gid(), old.gid());
        assert_eq!(
            xattr::get(&path, "user.basis_storage_test")
                .unwrap()
                .unwrap(),
            b"keep"
        );
        let loaded = PersistentDatabase::file_backed(&path);
        loaded.load().unwrap();
        assert_eq!(loaded.get("saved").unwrap().json_payload, json!(1));
        assert_no_temporary_files(directory.path());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn replacement_preserves_access_acl_instead_of_new_directory_defaults() {
        // Linux UAPI posix_acl_xattr.h: LE32 version=2 followed by entries of
        // LE16 tag, LE16 permissions, LE32 ID (undefined for base entries).
        fn acl(named_user: u32) -> Vec<u8> {
            let mut bytes = 2u32.to_le_bytes().to_vec();
            for (tag, permissions, id) in [
                (1u16, 7u16, u32::MAX), // owning user: rwx
                (2, 4, named_user),     // named user: read
                (4, 4, u32::MAX),       // owning group: read
                (16, 4, u32::MAX),      // mask: read
                (32, 0, u32::MAX),      // others: no access
            ] {
                bytes.extend(tag.to_le_bytes());
                bytes.extend(permissions.to_le_bytes());
                bytes.extend(id.to_le_bytes());
            }
            bytes
        }
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("database.json");
        let database = PersistentDatabase::file_backed(&path);
        database.shutdown().unwrap();
        let old_acl = acl(12345);
        xattr::set(&path, "system.posix_acl_access", &old_acl).unwrap();
        xattr::set(directory.path(), "system.posix_acl_default", &acl(54321)).unwrap();
        database.add_or_update(value("saved", json!(1)));
        database.shutdown().unwrap();
        assert_eq!(
            xattr::get(&path, "system.posix_acl_access")
                .unwrap()
                .unwrap(),
            old_acl
        );
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
        let outcomes = std::thread::scope(|scope| {
            let mut saves = Vec::new();
            for name in ["first", "second"] {
                let path = &path;
                let ready = &ready;
                saves.push(scope.spawn(move || {
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
                }));
            }
            saves
                .into_iter()
                .map(|save| save.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert!(outcomes.iter().any(Result::is_ok));
        for outcome in outcomes {
            #[cfg(windows)]
            if let Err(error) = outcome {
                // Concurrent Windows replacements may return ACCESS_DENIED or
                // SHARING_VIOLATION. Propagate these instead of deleting/retrying
                // the target; a successful save must still leave complete JSON.
                assert!(error.to_string().contains("replacing database"));
                let code = error.downcast_ref::<io::Error>().unwrap().raw_os_error();
                assert!(matches!(code, Some(5 | 32)), "{error:#}");
            }
            #[cfg(not(windows))]
            outcome.unwrap();
        }
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
    fn read_only_target_retains_old_snapshot_and_recovers_after_permissions_restored() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("database.json");
        let database = PersistentDatabase::file_backed(&path);
        database.add_or_update(value("saved", json!("old")));
        database.shutdown().unwrap();
        let old = fs::read(&path).unwrap();
        let original = fs::metadata(&path).unwrap().permissions();
        let mut read_only = original.clone();
        read_only.set_readonly(true);
        fs::set_permissions(&path, read_only).unwrap();
        database.add_or_update(value("saved", json!("new")));
        assert!(database.shutdown().is_err());
        assert_eq!(fs::read(&path).unwrap(), old);
        assert!(fs::metadata(&path).unwrap().permissions().readonly());
        assert_no_temporary_files(directory.path());
        fs::set_permissions(&path, original).unwrap();
        database.shutdown().unwrap();
        let loaded = PersistentDatabase::file_backed(&path);
        loaded.load().unwrap();
        assert_eq!(loaded.get("saved").unwrap().json_payload, json!("new"));
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
