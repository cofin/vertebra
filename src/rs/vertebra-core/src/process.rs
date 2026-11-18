//! Child process utilities for cross-process IPC.
//!
//! Provides FD inheritance helpers and parent-death signaling for worker processes.

#[cfg(unix)]
use std::os::fd::RawFd;

/// Clear FD_CLOEXEC on file descriptors so they are inherited across fork/exec.
///
/// Call this before spawning a child process for any FDs (ring buffers, notifiers)
/// that the child needs to access.
#[cfg(unix)]
pub fn make_inheritable(fds: &[RawFd]) {
    for &fd in fds {
        // SAFETY: `fcntl(F_GETFD)` / `fcntl(F_SETFD)` operate on caller-supplied file descriptors;
        // checking `flags >= 0` skips invalid descriptors before clearing `FD_CLOEXEC`.
        unsafe {
            let flags = libc::fcntl(fd, libc::F_GETFD);
            if flags >= 0 {
                libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC);
            }
        }
    }
}

/// Set PR_SET_PDEATHSIG so the child process receives SIGKILL when the parent exits.
///
/// This prevents orphaned worker processes when the supervisor crashes.
#[cfg(target_os = "linux")]
pub fn set_parent_death_signal(cmd: &mut std::process::Command) {
    use std::os::unix::process::CommandExt;
    // SAFETY: `pre_exec` runs in the forked child before `exec` and only invokes
    // async-signal-safe `libc::prctl(PR_SET_PDEATHSIG, SIGKILL)` and `last_os_error()`.
    unsafe {
        cmd.pre_exec(|| {
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    #[test]
    fn make_inheritable_clears_cloexec() {
        use super::*;

        // Create a pipe with CLOEXEC
        let mut fds = [0i32; 2];
        // SAFETY: `fds` points to a valid writable `[i32; 2]` buffer; `fcntl` operates on the
        // newly created open pipe descriptors.
        unsafe {
            assert!(libc::pipe(fds.as_mut_ptr()) >= 0);
            // Set CLOEXEC on both
            for &fd in &fds {
                let flags = libc::fcntl(fd, libc::F_GETFD);
                libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC);
            }
        }

        // Clear CLOEXEC
        make_inheritable(&fds);

        // Verify
        for &fd in &fds {
            // SAFETY: `fd` is an open pipe descriptor owned by this test until cleanup below.
            let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
            assert!(
                (flags & libc::FD_CLOEXEC) == 0,
                "FD_CLOEXEC should be cleared"
            );
        }

        // Cleanup
        // SAFETY: Both `fds[0]` and `fds[1]` are valid open descriptors owned by this test.
        unsafe {
            libc::close(fds[0]);
            libc::close(fds[1]);
        }
    }
}
