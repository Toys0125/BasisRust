//! A bounded worker owns all clip files. Network/cache locks only submit commands and drain results.
use super::{ClipId, SendTarget, CHUNK_BYTES};
use bytes::Bytes;
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::mpsc::{self, Receiver, SyncSender},
};
const CAPACITY: usize = 32;
const PREFIX: &str = "basisrust-clip-v1-";
const MARKER: &[u8] = b"BasisRust owned ephemeral clip cache v1\n";
#[derive(Debug)]
pub(super) enum Command {
    Create {
        id: ClipId,
        generation: u64,
        root: PathBuf,
    },
    Write {
        id: ClipId,
        generation: u64,
        index: usize,
        data: Bytes,
    },
    Read {
        id: ClipId,
        generation: u64,
        index: usize,
        length: usize,
        target: SendTarget,
    },
    Remove {
        id: ClipId,
        generation: u64,
    },
}
#[derive(Debug)]
pub(super) enum Event {
    Created {
        id: ClipId,
        generation: u64,
    },
    Written {
        id: ClipId,
        generation: u64,
        index: usize,
    },
    Read {
        id: ClipId,
        generation: u64,
        index: usize,
        target: SendTarget,
        data: Bytes,
    },
    Failed {
        id: ClipId,
        generation: u64,
        message: String,
    },
}
#[derive(Debug)]
pub(super) struct Worker {
    commands: Option<SyncSender<Command>>,
    results: Receiver<Event>,
}
impl Worker {
    pub(super) fn spawn() -> std::io::Result<Self> {
        let (commands, input) = mpsc::sync_channel(CAPACITY);
        let (output, results) = mpsc::sync_channel(CAPACITY);
        std::thread::Builder::new()
            .name("basis-clip-storage".into())
            .spawn(move || {
                let mut disk = DiskState::default();
                while let Ok(command) = input.recv() {
                    if let Some(event) = disk.execute(command) {
                        if output.send(event).is_err() {
                            break;
                        }
                    }
                }
            })?;
        Ok(Self {
            commands: Some(commands),
            results,
        })
    }
    pub(super) fn submit(&self, command: Command) -> bool {
        self.commands
            .as_ref()
            .is_some_and(|sender| sender.try_send(command).is_ok())
    }
    pub(super) fn drain(&self) -> Vec<Event> {
        self.results.try_iter().collect()
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.commands.take();
    }
}
#[derive(Default)]
struct DiskState {
    directory: Option<OwnedDirectory>,
    files: HashMap<(ClipId, u64), (File, PathBuf)>,
}
impl DiskState {
    fn execute(&mut self, command: Command) -> Option<Event> {
        let (id, generation) = match &command {
            Command::Create { id, generation, .. }
            | Command::Write { id, generation, .. }
            | Command::Read { id, generation, .. }
            | Command::Remove { id, generation } => (*id, *generation),
        };
        let result = (|| -> std::io::Result<Option<Event>> {
            match command {
                Command::Create { root, .. } => {
                    if self.directory.is_none() {
                        self.directory = Some(OwnedDirectory::create(&root)?);
                    }
                    let path = self
                        .directory
                        .as_ref()
                        .unwrap()
                        .path
                        .join(format!("{}-{generation}.clip", uuid::Uuid::from_bytes(id)));
                    let file = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create_new(true)
                        .open(&path)?;
                    self.files.insert((id, generation), (file, path));
                    Ok(Some(Event::Created { id, generation }))
                }
                Command::Write { index, data, .. } => {
                    let (file, _) = self.files.get_mut(&(id, generation)).ok_or_else(missing)?;
                    file.seek(SeekFrom::Start(index as u64 * CHUNK_BYTES as u64))?;
                    file.write_all(&data)?;
                    file.flush()?;
                    Ok(Some(Event::Written {
                        id,
                        generation,
                        index,
                    }))
                }
                Command::Read {
                    index,
                    length,
                    target,
                    ..
                } => {
                    let (file, _) = self.files.get_mut(&(id, generation)).ok_or_else(missing)?;
                    let mut data = vec![0; length];
                    file.seek(SeekFrom::Start(index as u64 * CHUNK_BYTES as u64))?;
                    file.read_exact(&mut data)?;
                    Ok(Some(Event::Read {
                        id,
                        generation,
                        index,
                        target,
                        data: Bytes::from(data),
                    }))
                }
                Command::Remove { .. } => {
                    if let Some((file, path)) = self.files.remove(&(id, generation)) {
                        drop(file);
                        let _ = std::fs::remove_file(path);
                    }
                    Ok(None)
                }
            }
        })();
        match result {
            Ok(event) => event,
            Err(err) => Some(Event::Failed {
                id,
                generation,
                message: err.to_string(),
            }),
        }
    }
}
fn missing() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::NotFound, "clip generation was removed")
}
impl Drop for DiskState {
    fn drop(&mut self) {
        self.files.clear();
        self.directory.take();
    }
}
struct OwnedDirectory {
    path: PathBuf,
    lease: Option<File>,
}
impl OwnedDirectory {
    fn create(root: &Path) -> std::io::Result<Self> {
        let mut root_builder = std::fs::DirBuilder::new();
        root_builder.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            root_builder.mode(0o700);
        }
        root_builder.create(root)?;
        let path = root.join(format!("{PREFIX}{}", uuid::Uuid::new_v4()));
        let mut builder = std::fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(&path)?;
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut lease = options.open(path.join(".owner.lock"))?;
        lease.write_all(MARKER)?;
        lease.flush()?;
        lease
            .try_lock()
            .map_err(|err| std::io::Error::other(err.to_string()))?;
        scavenge(root, &path, &lease)?;
        Ok(Self {
            path,
            lease: Some(lease),
        })
    }
}
impl Drop for OwnedDirectory {
    fn drop(&mut self) {
        self.lease.take();
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
fn same_file_owner(left: &File, right: &File) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        left.metadata()
            .ok()
            .zip(right.metadata().ok())
            .is_some_and(|(a, b)| a.uid() == b.uid())
    }
    #[cfg(windows)]
    {
        windows_owner::same_owner(left, right)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (left, right);
        false
    }
}
fn private_owned_directory(path: &Path, owner: &File) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        std::fs::symlink_metadata(path)
            .ok()
            .zip(owner.metadata().ok())
            .is_some_and(|(dir, file)| {
                dir.is_dir() && dir.uid() == file.uid() && dir.mode() & 0o022 == 0
            })
    }
    #[cfg(windows)]
    {
        windows_owner::private_directory(path, owner)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (path, owner);
        false
    }
}
#[cfg(windows)]
mod windows_owner {
    use super::*;
    use std::{
        os::windows::{fs::OpenOptionsExt, io::AsRawHandle},
        ptr,
    };
    use windows_sys::Win32::{
        Foundation::ERROR_INSUFFICIENT_BUFFER,
        Security::*,
        Storage::FileSystem::{
            FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, READ_CONTROL,
        },
        System::SystemServices::{ACCESS_ALLOWED_ACE_TYPE, ACCESS_DENIED_ACE_TYPE},
    };
    struct Security {
        buffer: Vec<u32>,
    }
    impl Security {
        fn read(file: &File, information: u32) -> std::io::Result<Self> {
            let mut needed = 0;
            // SAFETY: File retains its handle; size probe uses null output; the second output
            // points at a DWORD-aligned buffer containing at least the requested bytes.
            unsafe {
                if GetKernelObjectSecurity(
                    file.as_raw_handle(),
                    information,
                    ptr::null_mut(),
                    0,
                    &mut needed,
                ) == 0
                    && std::io::Error::last_os_error().raw_os_error()
                        != Some(ERROR_INSUFFICIENT_BUFFER as i32)
                {
                    return Err(std::io::Error::last_os_error());
                }
                let mut buffer = vec![0u32; (needed as usize).div_ceil(4)];
                if GetKernelObjectSecurity(
                    file.as_raw_handle(),
                    information,
                    buffer.as_mut_ptr().cast(),
                    needed,
                    &mut needed,
                ) == 0
                {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(Self { buffer })
            }
        }
        fn owner(&mut self) -> Option<PSID> {
            let mut owner = ptr::null_mut();
            let mut defaulted = 0;
            // SAFETY: the descriptor is returned by Windows and owned by this aligned buffer.
            unsafe {
                if GetSecurityDescriptorOwner(
                    self.buffer.as_mut_ptr().cast(),
                    &mut owner,
                    &mut defaulted,
                ) == 0
                    || owner.is_null()
                {
                    None
                } else {
                    Some(owner)
                }
            }
        }
    }
    pub(super) fn same_owner(left: &File, right: &File) -> bool {
        let (Ok(mut a), Ok(mut b)) = (
            Security::read(left, OWNER_SECURITY_INFORMATION),
            Security::read(right, OWNER_SECURITY_INFORMATION),
        ) else {
            return false;
        };
        let (Some(a), Some(b)) = (a.owner(), b.owner()) else {
            return false;
        };
        // SAFETY: both SID pointers remain inside live Security buffers for this call.
        unsafe { EqualSid(a, b) != 0 }
    }
    fn trusted_sid(sid: PSID, owner: PSID) -> bool {
        // SAFETY: owner/sid point into live security descriptors; temporary SID storage is
        // aligned and large enough for any well-known SID Windows writes here.
        unsafe {
            if EqualSid(sid, owner) != 0 {
                return true;
            }
            for kind in [WinLocalSystemSid, WinBuiltinAdministratorsSid] {
                let mut bytes = [0u32; 17];
                let mut length = 68;
                if CreateWellKnownSid(
                    kind,
                    ptr::null_mut(),
                    bytes.as_mut_ptr().cast(),
                    &mut length,
                ) != 0
                    && EqualSid(sid, bytes.as_mut_ptr().cast()) != 0
                {
                    return true;
                }
            }
        }
        false
    }
    pub(super) fn private_directory(path: &Path, current: &File) -> bool {
        let Ok(file) = OpenOptions::new()
            .access_mode(READ_CONTROL)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
            .open(path)
        else {
            return false;
        };
        if !same_owner(&file, current) {
            return false;
        }
        let Ok(mut security) = Security::read(
            &file,
            OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
        ) else {
            return false;
        };
        let Some(owner) = security.owner() else {
            return false;
        };
        let mut present = 0;
        let mut defaulted = 0;
        let mut acl = ptr::null_mut();
        // SAFETY: ACL/ACE pointers are validated Windows descriptor data and stay inside its
        // live buffer; ACE counts and pointers are obtained using Windows security APIs.
        unsafe {
            if GetSecurityDescriptorDacl(
                security.buffer.as_mut_ptr().cast(),
                &mut present,
                &mut acl,
                &mut defaulted,
            ) == 0
                || present == 0
                || acl.is_null()
            {
                return false;
            }
            for index in 0..(*acl).AceCount as u32 {
                let mut ace = ptr::null_mut();
                if GetAce(acl, index, &mut ace) == 0 {
                    return false;
                }
                let header = &*(ace as *const ACE_HEADER);
                if header.AceFlags as u32 & INHERIT_ONLY_ACE != 0 {
                    continue;
                }
                if header.AceType as u32 == ACCESS_DENIED_ACE_TYPE {
                    continue;
                }
                if header.AceType as u32 != ACCESS_ALLOWED_ACE_TYPE {
                    return false;
                }
                let allowed = &*(ace as *const ACCESS_ALLOWED_ACE);
                // Write/append/delete, generic write/all, ACL and owner mutation rights.
                const MUTATION: u32 = 0x0000_0156
                    | 0x0001_0000
                    | 0x0004_0000
                    | 0x0008_0000
                    | 0x4000_0000
                    | 0x1000_0000;
                if allowed.Mask & MUTATION != 0
                    && !trusted_sid((&allowed.SidStart as *const u32).cast_mut().cast(), owner)
                {
                    return false;
                }
            }
        }
        true
    }
}
fn scavenge(root: &Path, current: &Path, current_lease: &File) -> std::io::Result<()> {
    // A directory writable by another account permits rename/symlink races. Do not scavenge it.
    if !private_owned_directory(root, current_lease) {
        return Ok(());
    }
    for candidate in std::fs::read_dir(root)? {
        let candidate = candidate?;
        if candidate.path() == current
            || !candidate.file_type()?.is_dir()
            || !candidate.file_name().to_string_lossy().starts_with(PREFIX)
        {
            continue;
        }
        let metadata = std::fs::symlink_metadata(candidate.path())?;
        if !metadata.is_dir() || !private_owned_directory(&candidate.path(), current_lease) {
            continue;
        }
        let marker = candidate.path().join(".owner.lock");
        let Ok(metadata) = std::fs::symlink_metadata(&marker) else {
            continue;
        };
        if !metadata.is_file() || metadata.len() != MARKER.len() as u64 {
            continue;
        }
        let Ok(mut file) = OpenOptions::new().read(true).write(true).open(&marker) else {
            continue;
        };
        if !same_file_owner(&file, current_lease) {
            continue;
        }
        let mut bytes = Vec::new();
        if file.read_to_end(&mut bytes).is_err() || bytes != MARKER || file.try_lock().is_err() {
            continue;
        }
        // Keep the lease until deletion completes: other live cache instances can never be scavenged.
        let _ = std::fs::remove_dir_all(candidate.path());
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scavenges_only_owned_unlocked_directories() {
        let root =
            std::env::temp_dir().join(format!("basis-clip-scavenge-test-{}", uuid::Uuid::new_v4()));
        let live = OwnedDirectory::create(&root).unwrap();
        let stale = root.join(format!("{PREFIX}stale"));
        std::fs::create_dir(&stale).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&stale, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        std::fs::write(stale.join(".owner.lock"), MARKER).unwrap();
        std::fs::write(stale.join("old.clip"), b"old").unwrap();
        let unrelated = root.join(format!("{PREFIX}unrelated"));
        std::fs::create_dir(&unrelated).unwrap();
        std::fs::write(unrelated.join(".owner.lock"), b"other app").unwrap();
        let next = OwnedDirectory::create(&root).unwrap();
        assert!(!stale.exists());
        assert!(live.path.exists());
        assert!(unrelated.exists());
        drop(next);
        drop(live);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[cfg(unix)]
    #[test]
    fn does_not_follow_marker_or_directory_symlinks_or_scavenge_shared_root() {
        use std::os::unix::fs::{symlink, PermissionsExt};
        let root =
            std::env::temp_dir().join(format!("basis-clip-symlink-test-{}", uuid::Uuid::new_v4()));
        let live = OwnedDirectory::create(&root).unwrap();
        let marker_link = root.join(format!("{PREFIX}marker-link"));
        std::fs::create_dir(&marker_link).unwrap();
        symlink(
            live.path.join(".owner.lock"),
            marker_link.join(".owner.lock"),
        )
        .unwrap();
        let dir_link = root.join(format!("{PREFIX}directory-link"));
        symlink(&live.path, &dir_link).unwrap();
        let next = OwnedDirectory::create(&root).unwrap();
        assert!(marker_link.exists());
        assert!(dir_link.exists());
        let stale = root.join(format!("{PREFIX}stale"));
        std::fs::create_dir(&stale).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&stale, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        std::fs::write(stale.join(".owner.lock"), MARKER).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o777)).unwrap();
        let shared = OwnedDirectory::create(&root).unwrap();
        assert!(stale.exists());
        drop(shared);
        drop(next);
        drop(live);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn storage_worker_bounds_pending_io_and_cleans_up_after_receiver_drop() {
        let root = std::env::temp_dir().join(format!(
            "basis-clip-worker-bound-test-{}",
            uuid::Uuid::new_v4()
        ));
        let worker = Worker::spawn().unwrap();
        let id = [8; 16];
        assert!(worker.submit(Command::Create {
            id,
            generation: 1,
            root: root.clone()
        }));
        assert!(matches!(
            worker
                .results
                .recv_timeout(std::time::Duration::from_secs(2))
                .unwrap(),
            Event::Created { .. }
        ));
        let mut accepted = 0;
        while worker.submit(Command::Write {
            id,
            generation: 1,
            index: 0,
            data: Bytes::from_static(b"x"),
        }) {
            accepted += 1;
            assert!(accepted <= CAPACITY * 2 + 1, "unbounded pending IO");
        }
        drop(worker);
        for _ in 0..1000 {
            if std::fs::read_dir(&root).unwrap().next().is_none() {
                std::fs::remove_dir(&root).unwrap();
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        panic!("storage worker did not release its owned files");
    }
}
