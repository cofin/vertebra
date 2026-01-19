//! Cross-platform notification primitives for async IPC.
//!
//! This module provides a unified `Notifier` type that uses platform-optimized
//! signaling mechanisms:
//! - Linux: eventfd (single FD, 8-byte counter)
//! - macOS/BSD/Windows: pipe (read/write FD pair)
//!
//! The notifier integrates with asyncio's `loop.add_reader()` by exposing a
//! pollable file descriptor.

use std::io;

#[cfg(target_os = "linux")]
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

#[cfg(not(target_os = "linux"))]
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

/// Platform-independent notification handle for async signaling.
///
/// # Usage
///
/// ```ignore
/// let notifier = Notifier::new()?;
/// let fd = notifier.as_raw_fd();  // Register with event loop
///
/// // Producer side:
/// notifier.notify()?;
///
/// // Consumer side (after poll returns readable):
/// notifier.clear()?;
/// ```
pub struct Notifier {
    #[cfg(target_os = "linux")]
    fd: OwnedFd,
    #[cfg(not(target_os = "linux"))]
    read_fd: OwnedFd,
    #[cfg(not(target_os = "linux"))]
    write_fd: OwnedFd,
}

impl Notifier {
    /// Create a new notification handle.
    ///
    /// On Linux, this creates an eventfd with `EFD_NONBLOCK | EFD_CLOEXEC`.
    /// On other platforms, this creates a pipe with non-blocking mode.
    pub fn new() -> io::Result<Self> {
        #[cfg(target_os = "linux")]
        {
            Self::new_eventfd()
        }
        #[cfg(not(target_os = "linux"))]
        {
            Self::new_pipe()
        }
    }

    #[cfg(target_os = "linux")]
    fn new_eventfd() -> io::Result<Self> {
        // SAFETY: eventfd is a well-defined syscall that returns a valid FD on success.
        let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: We just checked that fd is valid (>= 0).
        Ok(Self {
            fd: unsafe { OwnedFd::from_raw_fd(fd) },
        })
    }

    #[cfg(not(target_os = "linux"))]
    fn new_pipe() -> io::Result<Self> {
        let mut fds = [0i32; 2];
        // SAFETY: pipe() is a standard POSIX syscall.
        if unsafe { libc::pipe(fds.as_mut_ptr()) } < 0 {
            return Err(io::Error::last_os_error());
        }

        // Set both ends to non-blocking and close-on-exec.
        for &fd in &fds {
            // SAFETY: We just created these FDs.
            unsafe {
                let flags = libc::fcntl(fd, libc::F_GETFL);
                if flags < 0 {
                    libc::close(fds[0]);
                    libc::close(fds[1]);
                    return Err(io::Error::last_os_error());
                }
                if libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) < 0 {
                    libc::close(fds[0]);
                    libc::close(fds[1]);
                    return Err(io::Error::last_os_error());
                }
                // Set close-on-exec
                let fdflags = libc::fcntl(fd, libc::F_GETFD);
                if fdflags >= 0 {
                    libc::fcntl(fd, libc::F_SETFD, fdflags | libc::FD_CLOEXEC);
                }
            }
        }

        // SAFETY: We just validated the FDs above.
        Ok(Self {
            read_fd: unsafe { OwnedFd::from_raw_fd(fds[0]) },
            write_fd: unsafe { OwnedFd::from_raw_fd(fds[1]) },
        })
    }

    /// Get the file descriptor for polling (read end).
    ///
    /// This FD should be registered with `loop.add_reader()` in Python
    /// or `epoll_ctl()` in native code.
    pub fn as_raw_fd(&self) -> RawFd {
        #[cfg(target_os = "linux")]
        {
            self.fd.as_raw_fd()
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.read_fd.as_raw_fd()
        }
    }

    /// Signal that data is available.
    ///
    /// This is a non-blocking operation. On Linux, it increments the eventfd
    /// counter. On other platforms, it writes a single byte to the pipe.
    pub fn notify(&self) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            let val: u64 = 1;
            // SAFETY: Writing 8 bytes to eventfd is the documented interface.
            let ret = unsafe {
                libc::write(
                    self.fd.as_raw_fd(),
                    &val as *const u64 as *const libc::c_void,
                    8,
                )
            };
            if ret < 0 {
                let err = io::Error::last_os_error();
                // EAGAIN is acceptable for non-blocking eventfd (counter at max).
                if err.raw_os_error() == Some(libc::EAGAIN) {
                    return Ok(());
                }
                return Err(err);
            }
            Ok(())
        }
        #[cfg(not(target_os = "linux"))]
        {
            let val: u8 = 1;
            // SAFETY: Writing a single byte to a pipe.
            let ret = unsafe {
                libc::write(
                    self.write_fd.as_raw_fd(),
                    &val as *const u8 as *const libc::c_void,
                    1,
                )
            };
            if ret < 0 {
                let err = io::Error::last_os_error();
                // EAGAIN is acceptable for non-blocking pipe (buffer full).
                if err.raw_os_error() == Some(libc::EAGAIN) {
                    return Ok(());
                }
                return Err(err);
            }
            Ok(())
        }
    }

    /// Clear pending notifications.
    ///
    /// Call this after the event loop indicates the FD is readable and before
    /// attempting to receive data. This prevents spurious wakeups.
    pub fn clear(&self) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            let mut val: u64 = 0;
            // SAFETY: Reading 8 bytes from eventfd resets the counter.
            let _ = unsafe {
                libc::read(
                    self.fd.as_raw_fd(),
                    &mut val as *mut u64 as *mut libc::c_void,
                    8,
                )
            };
            // Ignore errors - EAGAIN just means no notifications pending.
            Ok(())
        }
        #[cfg(not(target_os = "linux"))]
        {
            // Drain the pipe completely.
            let mut buf = [0u8; 64];
            loop {
                // SAFETY: Reading from pipe is safe.
                let ret = unsafe {
                    libc::read(
                        self.read_fd.as_raw_fd(),
                        buf.as_mut_ptr() as *mut libc::c_void,
                        buf.len(),
                    )
                };
                if ret <= 0 {
                    break;
                }
            }
            Ok(())
        }
    }

    /// Check if the notifier has pending notifications without blocking.
    ///
    /// Returns `true` if there are pending notifications, `false` otherwise.
    pub fn has_pending(&self) -> bool {
        #[cfg(target_os = "linux")]
        {
            // Use poll with zero timeout to check readability.
            let mut pfd = libc::pollfd {
                fd: self.fd.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            // SAFETY: poll with timeout 0 is non-blocking.
            let ret = unsafe { libc::poll(&mut pfd, 1, 0) };
            ret > 0 && (pfd.revents & libc::POLLIN) != 0
        }
        #[cfg(not(target_os = "linux"))]
        {
            let mut pfd = libc::pollfd {
                fd: self.read_fd.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            // SAFETY: poll with timeout 0 is non-blocking.
            let ret = unsafe { libc::poll(&mut pfd, 1, 0) };
            ret > 0 && (pfd.revents & libc::POLLIN) != 0
        }
    }
}

// Implement AsRawFd for compatibility with std polling types.
impl AsRawFd for Notifier {
    fn as_raw_fd(&self) -> RawFd {
        Notifier::as_raw_fd(self)
    }
}

// SAFETY: Notifier only contains file descriptors which are safe to send across threads.
unsafe impl Send for Notifier {}
// SAFETY: Notifier operations are atomic (eventfd counter, pipe writes).
unsafe impl Sync for Notifier {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notifier_roundtrip() {
        let notifier = Notifier::new().expect("failed to create notifier");
        assert!(!notifier.has_pending());

        notifier.notify().expect("notify failed");
        assert!(notifier.has_pending());

        notifier.clear().expect("clear failed");
        assert!(!notifier.has_pending());
    }

    #[test]
    fn notifier_multiple_notifications() {
        let notifier = Notifier::new().expect("failed to create notifier");

        // Multiple notifications should coalesce.
        for _ in 0..10 {
            notifier.notify().expect("notify failed");
        }
        assert!(notifier.has_pending());

        // Single clear should drain all.
        notifier.clear().expect("clear failed");
        assert!(!notifier.has_pending());
    }

    #[test]
    fn notifier_fd_is_valid() {
        let notifier = Notifier::new().expect("failed to create notifier");
        let fd = notifier.as_raw_fd();
        assert!(fd >= 0, "FD should be non-negative");
    }
}
