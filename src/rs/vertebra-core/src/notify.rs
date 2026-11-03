//! Cross-platform notification primitives for async IPC.
//!
//! This module provides a unified `Notifier` type that uses platform-optimized
//! signaling mechanisms:
//! - Linux: eventfd (single FD, 8-byte counter)
//! - macOS/BSD: pipe (read/write FD pair)
//! - Windows: `WaitOnAddress` / `WakeByAddressAll` atomic word
//!
//! On Unix, the notifier integrates with asyncio's `loop.add_reader()` by exposing a
//! pollable file descriptor. FDs are inheritable (no CLOEXEC) to support
//! cross-process IPC via fork.

#[cfg(windows)]
use core::sync::atomic::AtomicU32;
use core::sync::atomic::{AtomicBool, Ordering};
use std::io;
#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::Mutex;
#[cfg(windows)]
use std::time::Duration;

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
///
/// let notifier = Notifier::new().unwrap();
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
    #[cfg(all(unix, not(target_os = "linux")))]
    read_fd: OwnedFd,
    #[cfg(all(unix, not(target_os = "linux")))]
    write_fd: OwnedFd,
    #[cfg(windows)]
    state: AtomicU32,
}

impl Notifier {
    /// Create a new notification handle.
    ///
    /// On Linux, this creates an eventfd with `EFD_NONBLOCK` (no CLOEXEC).
    /// On other Unix platforms, this creates a pipe with non-blocking mode.
    /// On Windows, this initializes an atomic wait word.
    pub fn new() -> io::Result<Self> {
        #[cfg(target_os = "linux")]
        {
            Self::new_eventfd()
        }
        #[cfg(all(unix, not(target_os = "linux")))]
        {
            Self::new_pipe()
        }
        #[cfg(windows)]
        {
            Ok(Self {
                state: AtomicU32::new(0),
            })
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

    #[cfg(all(unix, not(target_os = "linux")))]
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

    /// Reconstruct a notifier from raw file descriptors (non-Linux Unix).
    ///
    /// For pipe-based platforms, both read and write FDs are needed.
    ///
    /// # Safety
    ///
    /// - Both `read_fd` and `write_fd` must be valid, open file descriptors
    /// - They must be the two ends of a pipe created by `Notifier::new()`
    /// - The caller takes ownership; both will be closed when dropped
    #[cfg(all(unix, not(target_os = "linux")))]
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
    #[cfg(unix)]
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

    /// Get the write FD (for non-Linux Unix platforms).
    ///
    /// On Linux, eventfd uses a single FD for both read and write, so this
    /// returns the same as `as_raw_fd()`.
    #[cfg(all(unix, not(target_os = "linux")))]
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
    /// counter. On other Unix platforms, it writes a single byte to the pipe.
    /// On Windows, it sets the atomic notification state and wakes waiters.
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
        #[cfg(all(unix, not(target_os = "linux")))]
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
        #[cfg(windows)]
        {
            self.state.store(1, Ordering::SeqCst);
            let _ = crate::platform::windows::wake_all(&self.state);
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
        #[cfg(all(unix, not(target_os = "linux")))]
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
        #[cfg(windows)]
        {
            self.state.store(0, Ordering::SeqCst);
            Ok(())
        }
    }

    /// Check if the notifier has pending notifications without blocking.
    ///
    /// Returns `true` if there are pending notifications, `false` otherwise.
    #[cfg(test)]
    pub fn has_pending(&self) -> bool {
        #[cfg(unix)]
        {
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
        #[cfg(windows)]
        {
            self.state.load(Ordering::Acquire) != 0
        }
    }

    /// Wait for a notification with timeout.
    ///
    /// Returns `Ok(true)` if notification received, `Ok(false)` on timeout,
    /// or `Err` on error.
    pub fn wait(&self, timeout_ms: i32) -> io::Result<bool> {
        #[cfg(unix)]
        {
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
        #[cfg(windows)]
        {
            if self.state.load(Ordering::Acquire) != 0 {
                return Ok(true);
            }
            if timeout_ms == 0 {
                return Ok(false);
            }
            if timeout_ms < 0 {
                let _ = crate::platform::windows::wait_on_address(&self.state, 0);
            } else {
                let _ = crate::platform::windows::wait_on_address_timeout(
                    &self.state,
                    0,
                    Duration::from_millis(timeout_ms as u64),
                );
            }
            Ok(self.state.load(Ordering::Acquire) != 0)
        }
    }
}

#[cfg(unix)]
impl AsRawFd for Notifier {
    fn as_raw_fd(&self) -> RawFd {
        Notifier::as_raw_fd(self)
    }
}

/// Pure-Rust completion queue with coalesced wake-arming for event loop drainers.
///
/// Multiple background tasks can push completed items concurrently without acquiring
/// the Python GIL. Only the transition from `wake_armed == false` to `wake_armed == true`
/// returns `true` from [`CompletionQueue::push`] (and signals the optional [`Notifier`]),
/// coalescing burst completions into a single event-loop wakeup.
pub struct CompletionQueue<T> {
    queue: Mutex<Vec<T>>,
    wake_armed: AtomicBool,
    notifier: Option<Notifier>,
}

impl<T> CompletionQueue<T> {
    /// Create a completion queue with an optional platform [`Notifier`].
    pub fn new(notifier: Option<Notifier>) -> Self {
        Self {
            queue: Mutex::new(Vec::with_capacity(64)),
            wake_armed: AtomicBool::new(false),
            notifier,
        }
    }

    /// Create a completion queue backed by [`Notifier::new`] when available.
    pub fn with_platform_notifier() -> Self {
        Self::new(Notifier::new().ok())
    }

    /// Borrow the underlying [`Notifier`], if present.
    pub fn notifier(&self) -> Option<&Notifier> {
        self.notifier.as_ref()
    }

    /// Clear any pending readiness state on the underlying [`Notifier`].
    pub fn clear_notifier(&self) {
        if let Some(ref notifier) = self.notifier {
            let _ = notifier.clear();
        }
    }

    /// Push a completed item onto the queue.
    ///
    /// Returns `true` if this push transitioned `wake_armed` from `false` to `true`
    /// (meaning the caller should wake the consumer if not relying solely on `notifier`),
    /// or `false` if a wakeup is already armed.
    pub fn push(&self, item: T) -> bool {
        let mut guard = self.queue.lock().expect("lock completion queue");
        guard.push(item);
        let should_signal = !self.wake_armed.swap(true, Ordering::AcqRel);
        if should_signal && let Some(ref notifier) = self.notifier {
            let _ = notifier.notify();
        }
        should_signal
    }

    /// Drain all queued items into `out`.
    ///
    /// When the queue is non-empty, swaps its contents into `out` and returns `true`.
    /// When the queue is empty, clears any pending [`Notifier`] readiness, disarms
    /// `wake_armed` with `Ordering::Release`, and returns `false`.
    pub fn drain_into(&self, out: &mut Vec<T>) -> bool {
        let mut guard = self.queue.lock().expect("lock completion queue");
        if guard.is_empty() {
            if let Some(ref notifier) = self.notifier {
                let _ = notifier.clear();
            }
            self.wake_armed.store(false, Ordering::Release);
            return false;
        }
        out.clear();
        std::mem::swap(&mut *guard, out);
        true
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

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

    #[cfg(unix)]
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

    #[cfg(unix)]
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

    #[cfg(all(unix, not(target_os = "linux")))]
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
    fn completion_queue_arms_once_per_burst_and_rearms_after_drain() {
        let queue = Arc::new(CompletionQueue::<u32>::with_platform_notifier());
        if let Some(notifier) = queue.notifier() {
            assert!(!notifier.has_pending());
        }

        let mut handles = Vec::with_capacity(5);
        for idx in 0..5u32 {
            let q = Arc::clone(&queue);
            handles.push(thread::spawn(move || q.push(idx)));
        }

        let armed_count = handles
            .into_iter()
            .map(|h| h.join().expect("producer thread panicked"))
            .filter(|&armed| armed)
            .count();
        assert_eq!(
            armed_count, 1,
            "only the first push in a burst should arm wakeup"
        );
        if let Some(notifier) = queue.notifier() {
            assert!(notifier.has_pending());
        }

        let mut drained = Vec::new();
        assert!(queue.drain_into(&mut drained));
        drained.sort_unstable();
        assert_eq!(drained, vec![0, 1, 2, 3, 4]);

        let mut empty = Vec::new();
        assert!(
            !queue.drain_into(&mut empty),
            "empty drain_into should disarm wake_armed and return false"
        );
        if let Some(notifier) = queue.notifier() {
            assert!(!notifier.has_pending());
        }

        assert!(
            queue.push(99),
            "subsequent push after empty drain_into must re-arm wakeup"
        );
        if let Some(notifier) = queue.notifier() {
            assert!(notifier.has_pending());
        }
    }

    #[test]
    fn completion_queue_coalesces_wake_signal_until_drained() {
        let queue = CompletionQueue::<u32>::with_platform_notifier();
        assert!(queue.push(10));
        assert!(!queue.push(20));
        assert!(!queue.push(30));

        let mut batch = Vec::new();
        let mut total = Vec::new();
        while queue.drain_into(&mut batch) {
            total.extend_from_slice(&batch);
        }
        assert_eq!(total, vec![10, 20, 30]);
        assert!(queue.push(40));
    }
}
