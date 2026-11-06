//! Preserve config-file ownership, access rules, and DACL inheritance when
//! atomically replacing a file on Windows.
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
        // SAFETY: path is terminated and alive for both calls. The aligned
        // buffer then holds at least the number of bytes returned by the probe.
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
        // through the call, and returned SID/ACL pointers refer into them.
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
                return Err(io::Error::other("config security descriptor has no DACL"));
            }
            let mut flags = DACL_SECURITY_INFORMATION
                | if control & SE_DACL_PROTECTED != 0 {
                    PROTECTED_DACL_SECURITY_INFORMATION
                } else {
                    UNPROTECTED_DACL_SECURITY_INFORMATION
                };
            // Avoid requiring ownership-changing privileges when the inherited
            // temporary-file owner/group already match the original.
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
    use windows_sys::Win32::Security::{
        AddAccessAllowedAce, GetSecurityDescriptorOwner, InitializeAcl, ACL_REVISION,
        DACL_SECURITY_INFORMATION, PROTECTED_DACL_SECURITY_INFORMATION,
    };

    #[test]
    fn replacement_preserves_owner_only_protected_config_acl() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.xml");
        let config = crate::ServerConfig {
            use_auth: false,
            ..crate::ServerConfig::default()
        };
        config.save(&path).unwrap();

        let mut security = Security::read(&path).unwrap();
        let mut owner = std::ptr::null_mut();
        let mut defaulted = 0;
        // SAFETY: the aligned descriptor was returned by GetFileSecurityW and
        // its owner SID remains valid for the ACL construction and apply call.
        assert_ne!(
            unsafe {
                GetSecurityDescriptorOwner(
                    security.descriptor.as_mut_ptr().cast(),
                    &mut owner,
                    &mut defaulted,
                )
            },
            0
        );
        assert!(!owner.is_null());
        let mut acl = vec![0u32; 64];
        // SAFETY: acl is DWORD-aligned and has enough space for one allow ACE.
        assert_ne!(
            unsafe {
                InitializeAcl(
                    acl.as_mut_ptr().cast(),
                    (acl.len() * std::mem::size_of::<u32>()) as u32,
                    ACL_REVISION,
                )
            },
            0
        );
        // Restrict the file to its owner; all other principals are denied by
        // omission. The DACL is protected from inheriting broader directory ACLs.
        assert_ne!(
            unsafe {
                AddAccessAllowedAce(acl.as_mut_ptr().cast(), ACL_REVISION, 0x001F_01FF, owner)
            },
            0
        );
        let wide_path: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
        let status = unsafe {
            SetNamedSecurityInfoW(
                wide_path.as_ptr(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                acl.as_mut_ptr().cast(),
                std::ptr::null_mut(),
            )
        };
        assert_eq!(status, 0, "setting owner-only config DACL failed: {status}");
        let before = Security::read(&path).unwrap().descriptor;

        let updated = crate::ServerConfig {
            server_name: "updated with restricted ACL".into(),
            ..config
        };
        updated.save(&path).unwrap();

        assert_eq!(Security::read(&path).unwrap().descriptor, before);
        assert_eq!(
            crate::ServerConfig::load_or_create(&path)
                .unwrap()
                .server_name,
            "updated with restricted ACL"
        );
    }
}
