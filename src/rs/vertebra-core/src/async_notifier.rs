//! Async notifier bridge for non-blocking integration with JavaScript runtimes.
//!
//! This module provides `AsyncNotifierBridge` which bridges synchronous eventfd
//! notification to Bun/Node's async event loop without blocking the main thread.
//!
//! # Architecture
//!
//! ```text
//! Python Worker                    Rust Background Thread           Bun Main Thread
//! ─────────────                    ──────────────────────           ───────────────
//!      │                                    │                              │
//!      │ sends response                     │                              │
//!      │ signals eventfd ─────────────────► │ (blocking poll)              │
//!      │                                    │                              │
//!      │                                    │ wakes up                     │
//!      │                                    │ writes to pipe ─────────────►│
//!      │                                    │                              │ event loop wakes
//!      │                                    │                              │ drains response ring
//! ```
//!
//! # Why This Works
//!
//! - Bun can't efficiently block on arbitrary fds (single-threaded JS)
//! - Background thread does the blocking wait (isolated from JS event loop)
//! - Pipe is a standard fd that Bun's event loop can watch natively
//! - Notification coalescing: multiple signals become one pipe byte
//!
//! # Shutdown Design
//!
//! Uses a dedicated shutdown eventfd instead of polling with timeout:
//! - 0% CPU idle (infinite poll timeout)
//! - Instant shutdown (no 50ms delay waiting for poll timeout)
//! - Cleaner design (no AtomicBool needed)

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::thread::{self, JoinHandle};

/// Async notifier bridge that watches an eventfd on a background thread
/// and signals a pipe when data is ready.
///
/// The pipe's read fd can be watched by Bun's event loop, bridging
/// synchronous Linux primitives to async JavaScript.
pub struct AsyncNotifierBridge {
    /// The source eventfd to watch (from Python response notifier)
    source_fd: RawFd,
    /// Pipe for signaling Bun (read end returned to JS)
    pipe_read: OwnedFd,
    /// Pipe write end (used by background thread)
    pipe_write: OwnedFd,
    /// Eventfd for signaling shutdown (instant wake, no polling delay)
    shutdown_fd: OwnedFd,
    /// Background thread handle
    thread: Option<JoinHandle<()>>,
}

impl AsyncNotifierBridge {
    /// Create a new async notifier bridge.
    ///
    /// # Arguments
    ///
    /// * `source_fd` - The eventfd to watch (must remain valid for bridge lifetime)
    ///
    /// # Returns
    ///
    /// The bridge instance. Call `start()` to begin watching.
    pub fn new(source_fd: RawFd) -> io::Result<Self> {
        // Create non-blocking pipe for signaling Bun
        let mut fds = [0i32; 2];
        if unsafe { libc::pipe(fds.as_mut_ptr()) } < 0 {
            return Err(io::Error::last_os_error());
        }

        // Set both pipe ends to non-blocking and FD_CLOEXEC
        for &fd in &fds {
            if let Err(e) = set_nonblock_cloexec(fd) {
                unsafe {
                    libc::close(fds[0]);
                    libc::close(fds[1]);
                }
                return Err(e);
            }
        }

        // Create shutdown eventfd for instant thread termination
        let shutdown = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        if shutdown < 0 {
            unsafe {
                libc::close(fds[0]);
                libc::close(fds[1]);
            }
            return Err(io::Error::last_os_error());
        }

        Ok(Self {
            source_fd,
            pipe_read: unsafe { OwnedFd::from_raw_fd(fds[0]) },
            pipe_write: unsafe { OwnedFd::from_raw_fd(fds[1]) },
            shutdown_fd: unsafe { OwnedFd::from_raw_fd(shutdown) },
            thread: None,
        })
    }

    /// Get the pipe read fd for Bun to watch.
    ///
    /// Bun should watch this fd with its event loop. When it becomes readable,
    /// it means the source eventfd was signaled (responses are ready).
    pub fn pipe_fd(&self) -> RawFd {
        self.pipe_read.as_raw_fd()
    }

    /// Start the background watcher thread.
    ///
    /// The thread will block on the source eventfd and write to the pipe
    /// when notifications arrive. Safe to call multiple times (no-op if running).
    pub fn start(&mut self) {
        if self.thread.is_some() {
            return; // Already running
        }

        let source_fd = self.source_fd;
        let pipe_write_fd = self.pipe_write.as_raw_fd();
        let shutdown_fd = self.shutdown_fd.as_raw_fd();

        self.thread = Some(thread::spawn(move || {
            watcher_loop(source_fd, pipe_write_fd, shutdown_fd);
        }));
    }

    /// Stop the background watcher thread.
    ///
    /// Signals the shutdown eventfd for instant wake and blocks until thread exits.
    /// Safe to call multiple times.
    pub fn stop(&mut self) {
        if let Some(thread) = self.thread.take() {
            // Signal shutdown eventfd to wake the thread instantly
            signal_eventfd(self.shutdown_fd.as_raw_fd());
            let _ = thread.join();
        }
    }

    /// Check if the watcher is running.
    pub fn is_running(&self) -> bool {
        self.thread.is_some()
    }
}

impl Drop for AsyncNotifierBridge {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Background thread function that watches the eventfd and signals the pipe.
///
/// Polls on two fds with infinite timeout (0% CPU idle):
/// - `source_fd`: The eventfd being watched (Python response notifier)
/// - `shutdown_fd`: Eventfd signaled by `stop()` for instant termination
fn watcher_loop(source_fd: RawFd, pipe_write_fd: RawFd, shutdown_fd: RawFd) {
    let mut pfds = [
        libc::pollfd {
            fd: source_fd,
            events: libc::POLLIN,
            revents: 0,
        },
        libc::pollfd {
            fd: shutdown_fd,
            events: libc::POLLIN,
            revents: 0,
        },
    ];

    loop {
        // Infinite timeout (-1) - 0% CPU idle, wakes only on fd activity
        let ret = unsafe { libc::poll(pfds.as_mut_ptr(), 2, -1) };

        if ret < 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EINTR) {
                continue; // Interrupted by signal, retry
            }
            // Other errors - log and exit (something is seriously wrong)
            eprintln!("AsyncNotifierBridge poll error: {}", err);
            return;
        }

        // Check shutdown fd first (instant exit when stop() is called)
        if (pfds[1].revents & libc::POLLIN) != 0 {
            return;
        }

        // Source fd is readable - drain it and signal the pipe
        if (pfds[0].revents & libc::POLLIN) != 0 {
            drain_eventfd(source_fd);
            signal_pipe(pipe_write_fd);
        }

        // Reset revents for next poll iteration
        pfds[0].revents = 0;
        pfds[1].revents = 0;
    }
}

/// Set O_NONBLOCK and FD_CLOEXEC on a file descriptor.
fn set_nonblock_cloexec(fd: RawFd) -> io::Result<()> {
    unsafe {
        // Set O_NONBLOCK
        let flags = libc::fcntl(fd, libc::F_GETFL);
        if flags < 0 {
            return Err(io::Error::last_os_error());
        }
        if libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) < 0 {
            return Err(io::Error::last_os_error());
        }

        // Set FD_CLOEXEC
        let fd_flags = libc::fcntl(fd, libc::F_GETFD);
        if fd_flags < 0 {
            return Err(io::Error::last_os_error());
        }
        if libc::fcntl(fd, libc::F_SETFD, fd_flags | libc::FD_CLOEXEC) < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

/// Drain the eventfd counter (clear pending notifications).
fn drain_eventfd(fd: RawFd) {
    let mut val: u64 = 0;
    let _ = unsafe { libc::read(fd, &mut val as *mut u64 as *mut libc::c_void, 8) };
}

/// Signal an eventfd (write value 1).
fn signal_eventfd(fd: RawFd) {
    let val: u64 = 1;
    let _ = unsafe { libc::write(fd, &val as *const u64 as *const libc::c_void, 8) };
}

/// Write a single byte to the pipe to signal Bun.
fn signal_pipe(fd: RawFd) {
    let val: u8 = 1;
    let _ = unsafe { libc::write(fd, &val as *const u8 as *const libc::c_void, 1) };
    // Ignore EAGAIN - pipe buffer might be full, but that's OK (coalescing)
}

// SAFETY: The bridge only contains file descriptors and thread-safe atomics.
unsafe impl Send for AsyncNotifierBridge {}
unsafe impl Sync for AsyncNotifierBridge {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn bridge_creates_valid_pipe() {
        // Create a dummy eventfd for testing
        let eventfd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK) };
        assert!(eventfd >= 0);

        let bridge = AsyncNotifierBridge::new(eventfd).expect("bridge creation failed");
        let pipe_fd = bridge.pipe_fd();
        assert!(pipe_fd >= 0);

        drop(bridge);
        unsafe { libc::close(eventfd) };
    }

    #[test]
    fn bridge_signals_pipe_on_eventfd() {
        // Create eventfd
        let eventfd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK) };
        assert!(eventfd >= 0);

        // Create and start bridge
        let mut bridge = AsyncNotifierBridge::new(eventfd).expect("bridge creation failed");
        bridge.start();

        // Give thread time to start
        thread::sleep(Duration::from_millis(10));

        // Signal the eventfd
        let val: u64 = 1;
        let ret = unsafe { libc::write(eventfd, &val as *const u64 as *const libc::c_void, 8) };
        assert!(ret > 0);

        // Wait a bit for the bridge to process
        thread::sleep(Duration::from_millis(100));

        // Check if pipe has data
        let mut pfd = libc::pollfd {
            fd: bridge.pipe_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        let ret = unsafe { libc::poll(&mut pfd, 1, 0) };
        assert!(ret > 0, "pipe should be readable after eventfd signal");

        // Cleanup
        bridge.stop();
        unsafe { libc::close(eventfd) };
    }

    #[test]
    fn bridge_stops_cleanly() {
        let eventfd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK) };
        let mut bridge = AsyncNotifierBridge::new(eventfd).expect("bridge creation failed");

        bridge.start();
        assert!(bridge.is_running());

        bridge.stop();
        assert!(!bridge.is_running());

        // Should be safe to stop again
        bridge.stop();

        unsafe { libc::close(eventfd) };
    }

    #[test]
    fn bridge_stops_instantly() {
        // Verify that stop() returns quickly (< 10ms) due to shutdown eventfd
        // Without shutdown eventfd, it would take up to 50ms (poll timeout)
        let eventfd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK) };
        let mut bridge = AsyncNotifierBridge::new(eventfd).expect("bridge creation failed");

        bridge.start();
        thread::sleep(Duration::from_millis(10)); // Let thread start

        let start = Instant::now();
        bridge.stop();
        let elapsed = start.elapsed();

        // Should complete in under 10ms (instant wake via shutdown_fd)
        // Previously with 50ms poll timeout, this would take >= 50ms
        assert!(
            elapsed < Duration::from_millis(10),
            "stop() took {:?}, expected < 10ms (instant shutdown)",
            elapsed
        );

        unsafe { libc::close(eventfd) };
    }

    #[test]
    fn bridge_multiple_signals_coalesce() {
        // Verify that multiple eventfd signals result in pipe data (coalescing)
        let eventfd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK) };
        let mut bridge = AsyncNotifierBridge::new(eventfd).expect("bridge creation failed");

        bridge.start();
        thread::sleep(Duration::from_millis(10));

        // Send multiple signals rapidly
        for _ in 0..10 {
            let val: u64 = 1;
            let _ = unsafe { libc::write(eventfd, &val as *const u64 as *const libc::c_void, 8) };
        }

        thread::sleep(Duration::from_millis(50));

        // Pipe should have at least some data (signals may coalesce)
        let mut pfd = libc::pollfd {
            fd: bridge.pipe_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        let ret = unsafe { libc::poll(&mut pfd, 1, 0) };
        assert!(ret > 0, "pipe should be readable after multiple signals");

        bridge.stop();
        unsafe { libc::close(eventfd) };
    }
}
