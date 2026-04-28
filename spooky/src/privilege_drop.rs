#[cfg(unix)]
pub fn drop_privileges(user: &str, group: &str) -> Result<(), String> {
    use std::ffi::CString;

    let c_group = CString::new(group).map_err(|_| "group contains NUL byte".to_string())?;
    let c_user = CString::new(user).map_err(|_| "user contains NUL byte".to_string())?;

    let gid = unsafe {
        // SAFETY: libc lookup is read-only; pointer is checked for null below.
        let grp = libc::getgrnam(c_group.as_ptr());
        if grp.is_null() {
            return Err(format!("group '{}' not found", group));
        }
        (*grp).gr_gid
    };

    let uid = unsafe {
        // SAFETY: libc lookup is read-only; pointer is checked for null below.
        let pwd = libc::getpwnam(c_user.as_ptr());
        if pwd.is_null() {
            return Err(format!("user '{}' not found", user));
        }
        (*pwd).pw_uid
    };

    let clear_groups_rc = unsafe {
        // SAFETY: passing null pointer with length 0 clears supplementary groups.
        libc::setgroups(0, std::ptr::null())
    };
    if clear_groups_rc != 0 {
        return Err("failed to clear supplementary groups".to_string());
    }

    let setgid_rc = unsafe {
        // SAFETY: gid resolved from getgrnam above.
        libc::setgid(gid)
    };
    if setgid_rc != 0 {
        return Err(format!("failed to drop group privileges to '{}'", group));
    }

    let setuid_rc = unsafe {
        // SAFETY: uid resolved from getpwnam above.
        libc::setuid(uid)
    };
    if setuid_rc != 0 {
        return Err(format!("failed to drop user privileges to '{}'", user));
    }

    let effective_uid = unsafe {
        // SAFETY: simple libc getter.
        libc::geteuid()
    };
    if effective_uid == 0 {
        return Err("privilege drop verification failed: still running as root".to_string());
    }

    Ok(())
}

#[cfg(not(unix))]
pub fn drop_privileges(_user: &str, _group: &str) -> Result<(), String> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::drop_privileges;

    #[cfg(unix)]
    #[test]
    fn rejects_unknown_group_or_user_before_system_calls() {
        let missing_group = format!("missing-group-{}", std::process::id());
        let result = drop_privileges("nobody", &missing_group);
        assert!(result.is_err());

        let missing_user = format!("missing-user-{}", std::process::id());
        let result = drop_privileges(&missing_user, "nogroup");
        assert!(result.is_err());
    }
}
