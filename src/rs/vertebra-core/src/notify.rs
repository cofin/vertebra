//! Cross-platform notification primitives for async IPC.
//!
//! This module provides a unified `Notifier` type that uses platform-optimized
//! signaling mechanisms:
//! - Linux: eventfd (single FD, 8-byte counter)
//! - macOS/BSD/Windows: pipe (read/write FD pair)
//!
//! The notifier integrates with asyncio's `loop.add_reader()` by exposing a
//! pollable file descriptor. FDs are inheritable (no CLOEXEC) to support
//! cross-process IPC via fork.

use std::io;

#[cfg(target_os = "linux")]
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

#[cfg(not(target_os = "linux"))]
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

/// Platform-independent notification handle for async signaling.
///
/// File descriptors are inheritable (no CLOEXEC) to support cross-process IPC.
/// This allows the supervisor to create notifiers before forking workers, with
/// children inheriting the FDs automatically.
///
/// # Usage
///
/// ```no_run
/// use vertebra_core::notify::Notifier;
/// use std::os::fd::AsRawFd;
///
/// let notifier = Notifier::new().unwrap();
/// let fd = notifier.as_raw_fd();  // Register with event loop
///
/// // Producer side:
/// notifier.notify().unwrap();
///
/// // Consumer side (after poll returns readable):
/// notifier.clear().unwrap();
/// ```
///
/// # Cross-Process IPC
///
/// ```text
/// // Parent (supervisor):
/// let notifier = Notifier::new()?;
/// env::set_var("VERTEBRA_REQ_NOTIFY_FD", notifier.as_raw_fd().to_string());
/// fork_child();
///
/// // Child:
/// let req_fd = env::var("VERTEBRA_REQ_NOTIFY_FD")?.parse()?;
/// let notifier = unsafe { Notifier::from_raw_fd(req_fd) };
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
    /// On Linux, this creates an eventfd with `EFD_NONBLOCK` (no CLOEXEC).
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
        // NOTE: We use EFD_NONBLOCK only, NOT EFD_CLOEXEC, to allow inheritance.
        let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
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

        // Set both ends to non-blocking, but NOT close-on-exec (for inheritance).
        for &fd in &fds {
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
                // NOTE: We intentionally do NOT set FD_CLOEXEC to allow inheritance.
            }
        }

        Ok(Self {
            read_fd: unsafe { OwnedFd::from_raw_fd(fds[0]) },
            write_fd: unsafe { OwnedFd::from_raw_fd(fds[1]) },
        })
    }

    /// Reconstruct a notifier from a raw file descriptor.
    ///
    /// This is used by child processes that inherit the FD from their parent.
    /// The FD number is typically passed via an environment variable.
    ///
    /// # Safety
    ///
    /// - The caller must ensure `fd` is a valid, open file descriptor
    /// - The caller must ensure `fd` was created by `Notifier::new()`
    ///   (i.e., it's an eventfd on Linux or the read end of a pipe on other platforms)
    /// - The caller takes ownership of the FD; it will be closed when dropped
    #[cfg(target_os = "linux")]
    pub unsafe fn from_raw_fd(fd: RawFd) -> Self {
        Self {
            fd: unsafe { OwnedFd::from_raw_fd(fd) },
        }
    }

    /// Reconstruct a notifier from raw file descriptors (non-Linux).
    ///
    /// For pipe-based platforms, both read and write FDs are needed.
    ///
    /// # Safety
    ///
    /// - Both `read_fd` and `write_fd` must be valid, open file descriptors
    /// - They must be the two ends of a pipe created by `Notifier::new()`
    /// - The caller takes ownership; both will be closed when dropped
    #[cfg(not(target_os = "linux"))]
    pub unsafe fn from_raw_fds(read_fd: RawFd, write_fd: RawFd) -> Self {
        Self {
            read_fd: unsafe { OwnedFd::from_raw_fd(read_fd) },
            write_fd: unsafe { OwnedFd::from_raw_fd(write_fd) },
        }
    }

    /// Get the file descriptor for polling (read end).
    ///
    /// This FD should be registered with `loop.add_reader()` in Python,
    /// `Bun.file(fd).readable` in TypeScript, or `epoll_ctl()` in native code.
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

    /// Get the write FD (for non-Linux platforms).
    ///
    /// On Linux, eventfd uses a single FD for both read and write, so this
    /// returns the same as `as_raw_fd()`.
    #[cfg(not(target_os = "linux"))]
    pub fn write_fd(&self) -> RawFd {
        self.write_fd.as_raw_fd()
    }

    /// Get the write FD (Linux version - same as read FD for eventfd).
    #[cfg(target_os = "linux")]
    pub fn write_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }

    /// Signal that data is available.
    ///
    /// This is a non-blocking operation. On Linux, it increments the eventfd
    /// counter. On other platforms, it writes a single byte to the pipe.
    pub fn notify(&self) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            let val: u64 = 1;
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
        let fd = self.fd.as_raw_fd();
        #[cfg(not(target_os = "linux"))]
        let fd = self.read_fd.as_raw_fd();

        let mut pfd = libc::pollfd {
            fd,
            events: libc::POLLIN,
            revents: 0,
        };
        let ret = unsafe { libc::poll(&mut pfd, 1, 0) };
        ret > 0 && (pfd.revents & libc::POLLIN) != 0
    }

    /// Wait for a notification with timeout.
    ///
    /// Returns `Ok(true)` if notification received, `Ok(false)` on timeout,
    /// or `Err` on error.
    pub fn wait(&self, timeout_ms: i32) -> io::Result<bool> {
        #[cfg(target_os = "linux")]
        let fd = self.fd.as_raw_fd();
        #[cfg(not(target_os = "linux"))]
        let fd = self.read_fd.as_raw_fd();

        let mut pfd = libc::pollfd {
            fd,
            events: libc::POLLIN,
            revents: 0,
        };
        let ret = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
        if ret < 0 {
            return Err(io::Error::last_os_error());
        }
        if ret == 0 {
            return Ok(false); // Timeout
        }
        Ok((pfd.revents & libc::POLLIN) != 0)
    }

    /// Hybrid spin-then-park wait for low-latency notification.
    ///
    /// Spins checking `has_pending()` for `spin_us` microseconds before falling
    /// back to blocking `wait()`. This captures immediate responses without
    /// syscall overhead while still allowing efficient blocking for longer waits.
    ///
    /// # Arguments
    ///
    /// * `spin_us` - Maximum microseconds to spin before blocking (typically 2-10)
    /// * `timeout_ms` - Total timeout in milliseconds (includes spin time)
    ///
    /// # Returns
    ///
    /// * `Ok(true)` - Notification received (either during spin or park)
    /// * `Ok(false)` - Timeout expired
    /// * `Err` - System error during wait
    ///
    /// # Performance
    ///
    /// For immediate data availability (common case with fast servers):
    /// - Spin phase returns in ~1-5us without syscall
    /// - Pure eventfd wake would take 3-5us
    ///
    /// For delayed data:
    /// - Falls back to efficient poll() after spin deadline
    ///
    /// # Note
    ///
    /// This method uses `poll(0)` syscall during spin, which adds ~200-500ns per
    /// iteration. For ring buffer scenarios, prefer `spin_wait_or_park_with()`
    /// which accepts a custom data-availability check function.
    pub fn spin_wait_or_park(&self, spin_us: u64, timeout_ms: i32) -> io::Result<bool> {
        self.spin_wait_or_park_with(spin_us, timeout_ms, || self.has_pending())
    }

    /// Hybrid spin-then-park wait with custom data-availability check.
    ///
    /// This variant is optimized for ring buffers where data availability can be
    /// checked with a pure atomic memory read (head != tail) rather than a syscall.
    ///
    /// # Arguments
    ///
    /// * `spin_us` - Maximum microseconds to spin before blocking (typically 2-10)
    /// * `timeout_ms` - Total timeout in milliseconds (includes spin time)
    /// * `has_data` - Closure that returns `true` if data is available
    ///
    /// # Returns
    ///
    /// * `Ok(true)` - Data available or notification received
    /// * `Ok(false)` - Timeout expired
    /// * `Err` - System error during wait
    ///
    /// # Performance
    ///
    /// With ring-buffer check (atomic load, ~1ns per iteration):
    /// - Spin phase captures immediate data in ~1-3µs
    /// - ~5000 iterations per 5µs spin duration
    ///
    /// With poll-based check (syscall, ~200-500ns per iteration):
    /// - ~10-25 iterations per 5µs spin duration
    pub fn spin_wait_or_park_with<F>(
        &self,
        spin_us: u64,
        timeout_ms: i32,
        has_data: F,
    ) -> io::Result<bool>
    where
        F: Fn() -> bool,
    {
        use std::time::{Duration, Instant};

        let start = Instant::now();
        let spin_deadline = start + Duration::from_micros(spin_us);

        // Spin phase: tight loop checking for data with provided closure
        while Instant::now() < spin_deadline {
            if has_data() {
                return Ok(true);
            }
            std::hint::spin_loop();
        }

        // Park phase: fall back to blocking wait
        // Adjust timeout to account for spin time elapsed
        let elapsed_ms = start.elapsed().as_millis() as i32;
        let remaining_ms = if timeout_ms < 0 {
            timeout_ms // Infinite timeout
        } else {
            (timeout_ms - elapsed_ms).max(0)
        };

        if remaining_ms == 0 && timeout_ms >= 0 {
            // Time already expired during spin
            return Ok(false);
        }

        self.wait(remaining_ms)
    }
}

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

    #[test]
    fn notifier_wait_timeout() {
        let notifier = Notifier::new().expect("failed to create notifier");

        // Wait with short timeout, should return false (no notification).
        let result = notifier.wait(1).expect("wait failed");
        assert!(!result, "should timeout without notification");

        // Notify and wait, should return true.
        notifier.notify().expect("notify failed");
        let result = notifier.wait(10).expect("wait failed");
        assert!(result, "should receive notification");
    }

    #[test]
    fn notifier_no_cloexec() {
        let notifier = Notifier::new().expect("failed to create notifier");
        for fd in [notifier.as_raw_fd(), notifier.write_fd()] {
            let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
            assert!(flags >= 0, "fcntl F_GETFD failed");
            assert!(
                (flags & libc::FD_CLOEXEC) == 0,
                "FD_CLOEXEC should NOT be set for cross-process IPC"
            );
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn notifier_from_raw_fd() {
        let notifier = Notifier::new().expect("failed to create notifier");
        let fd = notifier.as_raw_fd();

        // Notify on original.
        notifier.notify().expect("notify failed");

        // Duplicate the FD to simulate inheritance.
        let dup_fd = unsafe { libc::dup(fd) };
        assert!(dup_fd >= 0, "dup failed");

        // Clear original to prevent drop issues.
        notifier.clear().expect("clear failed");
        drop(notifier);

        // Reconstruct from duplicated FD.
        let child_notifier = unsafe { Notifier::from_raw_fd(dup_fd) };

        // Notify and check.
        child_notifier.notify().expect("notify failed");
        assert!(child_notifier.has_pending());
        child_notifier.clear().expect("clear failed");
        assert!(!child_notifier.has_pending());
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn notifier_from_raw_fds() {
        let notifier = Notifier::new().expect("failed to create notifier");
        let read_fd = notifier.as_raw_fd();
        let write_fd = notifier.write_fd();

        notifier.notify().expect("notify failed");

        let dup_read = unsafe { libc::dup(read_fd) };
        let dup_write = unsafe { libc::dup(write_fd) };
        assert!(dup_read >= 0 && dup_write >= 0, "dup failed");

        notifier.clear().expect("clear failed");
        drop(notifier);

        let child_notifier = unsafe { Notifier::from_raw_fds(dup_read, dup_write) };
        child_notifier.notify().expect("notify failed");
        assert!(child_notifier.has_pending());
        child_notifier.clear().expect("clear failed");
        assert!(!child_notifier.has_pending());
    }

    #[test]
    fn spin_wait_immediate_data() {
        let notifier = Notifier::new().expect("failed to create notifier");

        // Pre-notify so data is immediately available
        notifier.notify().expect("notify failed");

        let start = std::time::Instant::now();
        let result = notifier
            .spin_wait_or_park(100, 1000)
            .expect("spin_wait failed");
        let elapsed = start.elapsed();

        assert!(result, "should return true when data pending");
        assert!(
            elapsed.as_micros() < 1000,
            "immediate data should return fast: {}us",
            elapsed.as_micros()
        );
    }

    #[test]
    fn spin_wait_timeout() {
        let notifier = Notifier::new().expect("failed to create notifier");

        let start = std::time::Instant::now();
        let result = notifier
            .spin_wait_or_park(5, 10) // 5us spin, 10ms timeout
            .expect("spin_wait failed");
        let elapsed = start.elapsed();

        assert!(!result, "should return false on timeout");
        assert!(
            elapsed.as_millis() >= 8,
            "should wait ~10ms, got {}ms",
            elapsed.as_millis()
        );
    }

    #[test]
    fn spin_wait_park_fallback() {
        let notifier = std::sync::Arc::new(Notifier::new().expect("failed to create notifier"));
        let notifier_clone = std::sync::Arc::clone(&notifier);

        // Spawn thread to notify after a small delay
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(5));
            notifier_clone.notify().expect("notify failed");
        });

        let start = std::time::Instant::now();
        let result = notifier
            .spin_wait_or_park(5, 100) // 5us spin, 100ms timeout
            .expect("spin_wait failed");
        let elapsed = start.elapsed();

        assert!(result, "should return true when notified during park");
        assert!(
            elapsed.as_millis() < 50,
            "should wake before timeout: {}ms",
            elapsed.as_millis()
        );
    }
}
