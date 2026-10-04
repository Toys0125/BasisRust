//! Preserve the existing file's owner, group, and access ACL on the temporary
//! snapshot. A failure occurs before atomic replacement and leaves the old file.
use std::{io, os::windows::ffi::OsStrExt, path::Path};
use windows_sys::Win32::{
    Foundation::ERROR_INSUFFICIENT_BUFFER,
    Security::{
        Authorization::{SetNamedSecurityInfoW, SE_FILE_OBJECT},
        EqualSid, GetFileSecurityW, GetSecurityDescriptorControl, GetSecurityDescriptorDacl,
        GetSecurityDescriptorGroup, GetSecurityDescriptorOwner, DACL_SECURITY_INFORMATION,
        GROUP_SECURITY_INFORMATION, OWNER_SECURITY_INFORMATION,
        PROTECTED_DACL_SECURITY_INFORMATION, SE_DACL_PROTECTED,
        UNPROTECTED_DACL_SECURITY_INFORMATION,
    },
};

const ACCESS_INFORMATION: u32 =
    OWNER_SECURITY_INFORMATION | GROUP_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION;

pub(super) struct Security {
    // DWORD alignment is required for the self-relative security descriptor.
    descriptor: Vec<u32>,
}

impl Security {
    pub(super) fn read(path: &Path) -> io::Result<Self> {
        let path: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
        let mut needed = 0;
        // SAFETY: path is terminated and alive for both calls. The first call
        // probes the size; the aligned buffer then holds at least needed bytes.
        unsafe {
            if GetFileSecurityW(
                path.as_ptr(),
                ACCESS_INFORMATION,
                std::ptr::null_mut(),
                0,
                &mut needed,
            ) == 0
            {
                let error = io::Error::last_os_error();
                if error.raw_os_error() != Some(ERROR_INSUFFICIENT_BUFFER as i32) {
                    return Err(error);
                }
            }
            let mut descriptor = vec![0u32; (needed as usize).div_ceil(4)];
            if GetFileSecurityW(
                path.as_ptr(),
                ACCESS_INFORMATION,
                descriptor.as_mut_ptr().cast(),
                needed,
                &mut needed,
            ) == 0
            {
                return Err(io::Error::last_os_error());
            }
            Ok(Self { descriptor })
        }
    }

    pub(super) fn apply(mut self, path: &Path) -> io::Result<()> {
        let mut current = Self::read(path)?;
        let path: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
        // SAFETY: both descriptors come from GetFileSecurityW, remain allocated
        // throughout the calls, and all returned SID/ACL pointers point into
        // those buffers. The setters mutate only the uncommitted temporary file.
        unsafe {
            let saved = self.descriptor.as_mut_ptr().cast();
            let existing = current.descriptor.as_mut_ptr().cast();
            let mut owner = std::ptr::null_mut();
            let mut group = std::ptr::null_mut();
            let mut current_owner = std::ptr::null_mut();
            let mut current_group = std::ptr::null_mut();
            let mut defaulted = 0;
            let mut dacl = std::ptr::null_mut();
            let mut present = 0;
            let mut control = 0;
            let mut revision = 0;
            if GetSecurityDescriptorOwner(saved, &mut owner, &mut defaulted) == 0
                || GetSecurityDescriptorGroup(saved, &mut group, &mut defaulted) == 0
                || GetSecurityDescriptorOwner(existing, &mut current_owner, &mut defaulted) == 0
                || GetSecurityDescriptorGroup(existing, &mut current_group, &mut defaulted) == 0
                || GetSecurityDescriptorDacl(saved, &mut present, &mut dacl, &mut defaulted) == 0
                || GetSecurityDescriptorControl(saved, &mut control, &mut revision) == 0
            {
                return Err(io::Error::last_os_error());
            }
            if present == 0 {
                return Err(io::Error::other("database security descriptor has no DACL"));
            }
            let mut flags = DACL_SECURITY_INFORMATION
                | if control & SE_DACL_PROTECTED != 0 {
                    PROTECTED_DACL_SECURITY_INFORMATION
                } else {
                    UNPROTECTED_DACL_SECURITY_INFORMATION
                };
            // Avoid requiring ownership-changing privileges for an unchanged
            // owner/group, but never silently substitute different identities.
            if owner != current_owner
                && (owner.is_null()
                    || current_owner.is_null()
                    || EqualSid(owner, current_owner) == 0)
            {
                flags |= OWNER_SECURITY_INFORMATION;
            }
            if group != current_group
                && (group.is_null()
                    || current_group.is_null()
                    || EqualSid(group, current_group) == 0)
            {
                flags |= GROUP_SECURITY_INFORMATION;
            }
            let error = SetNamedSecurityInfoW(
                path.as_ptr(),
                SE_FILE_OBJECT,
                flags,
                owner,
                group,
                dacl,
                std::ptr::null(),
            );
            if error != 0 {
                return Err(io::Error::from_raw_os_error(error as i32));
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BasisData, PersistentDatabase};
    use windows_sys::Win32::Security::SetSecurityDescriptorControl;

    #[test]
    fn replacement_preserves_owner_group_and_protected_access_acl() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("database.json");
        let database = PersistentDatabase::file_backed(&path);
        database.add_or_update(BasisData {
            name: "saved".into(),
            json_payload: serde_json::json!("old"),
        });
        database.shutdown().unwrap();
        let mut security = Security::read(&path).unwrap();
        // SAFETY: the aligned descriptor was obtained from GetFileSecurityW and
        // stays alive while its DACL-protection flag is changed.
        assert_ne!(
            unsafe {
                SetSecurityDescriptorControl(
                    security.descriptor.as_mut_ptr().cast(),
                    SE_DACL_PROTECTED,
                    SE_DACL_PROTECTED,
                )
            },
            0
        );
        security.apply(&path).unwrap();
        let before = Security::read(&path).unwrap().descriptor;
        database.add_or_update(BasisData {
            name: "saved".into(),
            json_payload: serde_json::json!("new"),
        });
        database.shutdown().unwrap();
        assert_eq!(Security::read(&path).unwrap().descriptor, before);
        let loaded = PersistentDatabase::file_backed(&path);
        loaded.load().unwrap();
        assert_eq!(
            loaded.get("saved").unwrap().json_payload,
            serde_json::json!("new")
        );
    }
}
