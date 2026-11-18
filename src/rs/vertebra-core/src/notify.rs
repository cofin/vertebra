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
use std::collections::VecDeque;
use std::io;
#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::{Arc, Condvar, Mutex};
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
            // SAFETY: `fd` was just returned by `libc::eventfd`, so it is valid, open,
            // non-blocking, and uniquely owned by this `Notifier`.
            fd: unsafe { OwnedFd::from_raw_fd(fd) },
        })
    }

    #[cfg(all(unix, not(target_os = "linux")))]
    fn new_pipe() -> io::Result<Self> {
        let mut fds = [0i32; 2];
        // SAFETY: `fds` points to a valid writable `[i32; 2]` buffer for `libc::pipe`.
        if unsafe { libc::pipe(fds.as_mut_ptr()) } < 0 {
            return Err(io::Error::last_os_error());
        }

        // Set both ends to non-blocking, but NOT close-on-exec (for inheritance).
        for &fd in &fds {
            // SAFETY: `fds` holds two newly opened pipe FDs; both are closed before returning
            // an error if configuring `O_NONBLOCK` fails.
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
            // SAFETY: `fds[0]` is the valid, open, non-blocking read end uniquely owned here.
            read_fd: unsafe { OwnedFd::from_raw_fd(fds[0]) },
            // SAFETY: `fds[1]` is the valid, open, non-blocking write end uniquely owned here.
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
            // SAFETY: Caller upholds `from_raw_fd` contract that `fd` is valid, open,
            // and exclusively transferred to this `Notifier`.
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
            // SAFETY: Caller upholds `from_raw_fds` contract that `read_fd` is valid, open,
            // and exclusively transferred to this `Notifier`.
            read_fd: unsafe { OwnedFd::from_raw_fd(read_fd) },
            // SAFETY: Caller upholds `from_raw_fds` contract that `write_fd` is valid, open,
            // and exclusively transferred to this `Notifier`.
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
            // SAFETY: `self.fd` is an open non-blocking `OwnedFd` and `&val` points to 8 readable bytes.
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
            // SAFETY: `self.write_fd` is an open non-blocking `OwnedFd` and `&val` points to 1 readable byte.
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
            // SAFETY: `self.fd` is an open non-blocking `OwnedFd` and `&mut val` is writable 8-byte storage.
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
                // SAFETY: `self.read_fd` is an open non-blocking `OwnedFd` and `buf` is valid writable
                // storage for `buf.len()` bytes.
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
            // SAFETY: `&mut pfd` points to a valid single-element `libc::pollfd` for the live `OwnedFd`.
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
            // SAFETY: `&mut pfd` points to a valid single-element `libc::pollfd` for the live `OwnedFd`.
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
/// returns `true` from [`AsyncCompletionQueue::push`] (and signals the optional [`Notifier`]),
/// coalescing burst completions into a single event-loop wakeup.
pub struct AsyncCompletionQueue<T> {
    queue: Mutex<Vec<T>>,
    wake_armed: AtomicBool,
    notifier: Option<Notifier>,
}

impl<T> AsyncCompletionQueue<T> {
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

/// Synchronous blocking completion queue with condition variable and optional [`Notifier`].
///
/// Designed for synchronous callers that wait on tasks without an async event loop,
/// supporting blocking pop, timed pop, and bulk draining.
pub struct SyncCompletionQueue<T> {
    queue: Mutex<VecDeque<T>>,
    cvar: Condvar,
    notifier: Option<Arc<Notifier>>,
}

impl<T> SyncCompletionQueue<T> {
    /// Create a new synchronous completion queue without a notifier.
    pub fn new() -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            cvar: Condvar::new(),
            notifier: None,
        }
    }

    /// Create a synchronous completion queue with a shared platform [`Notifier`].
    pub fn with_notifier(notifier: Arc<Notifier>) -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            cvar: Condvar::new(),
            notifier: Some(notifier),
        }
    }

    /// Create a synchronous completion queue backed by a newly initialized [`Notifier`].
    pub fn with_platform_notifier() -> io::Result<Self> {
        let notifier = Arc::new(Notifier::new()?);
        Ok(Self::with_notifier(notifier))
    }

    /// Borrow the underlying [`Notifier`], if configured.
    pub fn notifier(&self) -> Option<&Arc<Notifier>> {
        self.notifier.as_ref()
    }

    /// Return the number of queued items.
    pub fn len(&self) -> usize {
        self.queue.lock().expect("lock sync completion queue").len()
    }

    /// Return `true` if the queue contains no items.
    pub fn is_empty(&self) -> bool {
        self.queue
            .lock()
            .expect("lock sync completion queue")
            .is_empty()
    }

    /// Push an item onto the back of the queue, waking one waiting thread and signaling the notifier.
    pub fn push(&self, item: T) {
        {
            let mut guard = self.queue.lock().expect("lock sync completion queue");
            guard.push_back(item);
            self.cvar.notify_one();
        }
        if let Some(ref notifier) = self.notifier {
            let _ = notifier.notify();
        }
    }

    /// Try to pop an item from the front of the queue without blocking.
    pub fn try_pop(&self) -> Option<T> {
        let mut guard = self.queue.lock().expect("lock sync completion queue");
        guard.pop_front()
    }

    /// Pop an item from the front of the queue, blocking until an item is available.
    pub fn pop_blocking(&self) -> T {
        let mut guard = self.queue.lock().expect("lock sync completion queue");
        loop {
            if let Some(item) = guard.pop_front() {
                return item;
            }
            guard = self
                .cvar
                .wait(guard)
                .expect("wait on sync completion queue");
        }
    }

    /// Pop an item from the front of the queue, blocking up to `timeout`.
    ///
    /// Returns `Some(item)` if an item became available within `timeout`, or `None` on timeout.
    pub fn pop_timeout(&self, timeout: Duration) -> Option<T> {
        let mut guard = self.queue.lock().expect("lock sync completion queue");
        let start = std::time::Instant::now();
        loop {
            if let Some(item) = guard.pop_front() {
                return Some(item);
            }
            let elapsed = start.elapsed();
            if elapsed >= timeout {
                return None;
            }
            let remaining = timeout - elapsed;
            let (new_guard, wait_res) = self
                .cvar
                .wait_timeout(guard, remaining)
                .expect("wait on sync completion queue");
            guard = new_guard;
            if let Some(item) = guard.pop_front() {
                return Some(item);
            }
            if wait_res.timed_out() {
                return None;
            }
        }
    }

    /// Drain all queued items into `out`.
    ///
    /// Clears `out` and moves all items currently in the queue into `out`.
    /// Clears any pending readiness on the underlying [`Notifier`].
    pub fn drain_into(&self, out: &mut Vec<T>) {
        let mut guard = self.queue.lock().expect("lock sync completion queue");
        out.clear();
        out.reserve(guard.len());
        out.extend(guard.drain(..));
        if let Some(ref notifier) = self.notifier {
            let _ = notifier.clear();
        }
    }
}

impl<T> Default for SyncCompletionQueue<T> {
    fn default() -> Self {
        Self::new()
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
            // SAFETY: `fd` is borrowed from the live `notifier` for this call.
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
        // SAFETY: `fd` is a valid open descriptor borrowed from `notifier`.
        let dup_fd = unsafe { libc::dup(fd) };
        assert!(dup_fd >= 0, "dup failed");

        // Clear original to prevent drop issues.
        notifier.clear().expect("clear failed");
        drop(notifier);

        // Reconstruct from duplicated FD.
        // SAFETY: `dup_fd` is a valid, uniquely owned eventfd duplicated from `notifier`.
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

        // SAFETY: `read_fd` is a valid open descriptor borrowed from `notifier`.
        let dup_read = unsafe { libc::dup(read_fd) };
        // SAFETY: `write_fd` is a valid open descriptor borrowed from `notifier`.
        let dup_write = unsafe { libc::dup(write_fd) };
        assert!(dup_read >= 0 && dup_write >= 0, "dup failed");

        notifier.clear().expect("clear failed");
        drop(notifier);

        // SAFETY: `dup_read` and `dup_write` are valid, uniquely owned pipe FDs from `notifier`.
        let child_notifier = unsafe { Notifier::from_raw_fds(dup_read, dup_write) };
        child_notifier.notify().expect("notify failed");
        assert!(child_notifier.has_pending());
        child_notifier.clear().expect("clear failed");
        assert!(!child_notifier.has_pending());
    }

    #[test]
    fn async_completion_queue_arms_once_per_burst_and_rearms_after_drain() {
        let queue = Arc::new(AsyncCompletionQueue::<u32>::with_platform_notifier());
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
    fn async_completion_queue_coalesces_wake_signal_until_drained() {
        let queue = AsyncCompletionQueue::<u32>::with_platform_notifier();
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

    #[test]
    fn sync_completion_queue_multi_producer_blocking_pop() {
        let queue = Arc::new(SyncCompletionQueue::<u32>::new());
        assert!(queue.is_empty());
        assert_eq!(queue.len(), 0);

        let mut handles = Vec::with_capacity(4);
        for i in 0..4u32 {
            let q = Arc::clone(&queue);
            handles.push(thread::spawn(move || {
                for j in 0..25u32 {
                    q.push(i * 100 + j);
                }
            }));
        }

        let mut received = Vec::with_capacity(100);
        for _ in 0..100 {
            received.push(queue.pop_blocking());
        }

        for h in handles {
            h.join().expect("producer failed");
        }

        assert_eq!(received.len(), 100);
        assert!(queue.is_empty());
        assert_eq!(queue.len(), 0);
        assert_eq!(queue.try_pop(), None);
    }

    #[test]
    fn sync_completion_queue_timeout() {
        let queue = Arc::new(SyncCompletionQueue::<u32>::new());

        let timeout = Duration::from_millis(15);
        let start = std::time::Instant::now();
        assert_eq!(queue.pop_timeout(timeout), None);
        assert!(start.elapsed() >= Duration::from_millis(10));

        let ready = Arc::new(std::sync::Barrier::new(2));
        let ready_clone = Arc::clone(&ready);
        let q = Arc::clone(&queue);
        let handle = thread::spawn(move || {
            ready_clone.wait();
            q.push(42);
        });

        ready.wait();
        assert_eq!(queue.pop_timeout(Duration::from_secs(1)), Some(42));
        handle.join().expect("thread join");
    }

    #[test]
    fn sync_completion_queue_notifier_wakeup_and_drain_into() {
        let queue =
            SyncCompletionQueue::<u32>::with_platform_notifier().expect("create notifier queue");
        if let Some(notifier) = queue.notifier() {
            assert!(!notifier.has_pending());
        }

        queue.push(1);
        queue.push(2);
        queue.push(3);
        assert_eq!(queue.len(), 3);
        assert!(!queue.is_empty());

        if let Some(notifier) = queue.notifier() {
            assert!(notifier.has_pending());
        }

        let mut drained = Vec::new();
        queue.drain_into(&mut drained);
        assert_eq!(drained, vec![1, 2, 3]);
        assert!(queue.is_empty());
        assert_eq!(queue.len(), 0);

        if let Some(notifier) = queue.notifier() {
            assert!(!notifier.has_pending());
        }
    }
}
