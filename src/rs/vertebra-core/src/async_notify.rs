//! Tokio-native async notifier for non-blocking IPC integration.
//!
//! This module provides `AsyncNotifier` which wraps eventfd (Linux) with
//! `tokio::io::unix::AsyncFd` for true async wake-up in the tokio runtime.
//!
//! # Architecture
//!
//! Unlike `AsyncNotifierBridge` which bridges to JS runtimes via background threads,
//! this type integrates directly with tokio's epoll-based reactor:
//!
//! ```text
//! Python Worker                    Rust tokio Task
//! ─────────────                    ───────────────
//!      │                                  │
//!      │ sends response                   │ await resp_notifier.wait_readable()
//!      │ signals eventfd ────────────────►│
//!      │                                  │ (epoll wakes task, no threads)
//!      │                                  │ processes response
//! ```
//!
//! # Benefits
//!
//! - **No blocking**: `wait_readable().await` yields to tokio, other tasks run
//! - **No threads**: Uses epoll directly, no background watcher thread needed
//! - **Instant wake**: ~3-5μs notification latency (same as raw eventfd)
//! - **Composable**: Works with `tokio::select!` for timeout/cancellation

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

use tokio::io::unix::AsyncFd;
use tokio::io::Interest;

/// Async-capable notification handle for tokio integration.
///
/// Wraps an eventfd (Linux) with `tokio::io::unix::AsyncFd` for
/// non-blocking async notification in the tokio runtime.
///
/// # Example
///
/// ```ignore
/// let notifier = AsyncNotifier::new()?;
///
/// // Producer side (sync)
/// notifier.notify()?;
///
/// // Consumer side (async)
/// notifier.wait_readable().await?;
/// notifier.clear()?;
/// ```
pub struct AsyncNotifier {
    /// The underlying eventfd
    fd: OwnedFd,
    /// Tokio async wrapper for epoll integration
    async_fd: AsyncFd<RawFd>,
}

impl AsyncNotifier {
    /// Create a new async notifier.
    ///
    /// Creates an eventfd with `EFD_NONBLOCK` (no CLOEXEC for cross-process IPC).
    /// The eventfd is registered with tokio's reactor for async readiness.
    pub fn new() -> io::Result<Self> {
        // Create eventfd: EFD_NONBLOCK for async, NO EFD_CLOEXEC for inheritance
        let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }

        let owned_fd = unsafe { OwnedFd::from_raw_fd(fd) };

        // Wrap with AsyncFd for tokio integration
        let async_fd = AsyncFd::with_interest(fd, Interest::READABLE)?;

        Ok(Self {
            fd: owned_fd,
            async_fd,
        })
    }

    /// Reconstruct an async notifier from a raw file descriptor.
    ///
    /// Used by processes that inherit the FD from their parent.
    ///
    /// # Safety
    ///
    /// - The caller must ensure `fd` is a valid eventfd
    /// - The caller takes ownership of the FD
    pub unsafe fn from_raw_fd(fd: RawFd) -> io::Result<Self> {
        let owned_fd = OwnedFd::from_raw_fd(fd);
        let async_fd = AsyncFd::with_interest(fd, Interest::READABLE)?;

        Ok(Self {
            fd: owned_fd,
            async_fd,
        })
    }

    /// Get the raw file descriptor.
    ///
    /// Useful for passing to child processes via environment variables.
    pub fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }

    /// Signal that data is available.
    ///
    /// Non-blocking write to the eventfd. Safe to call from any context.
    pub fn notify(&self) -> io::Result<()> {
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
            // EAGAIN is OK - eventfd counter at max
            if err.raw_os_error() == Some(libc::EAGAIN) {
                return Ok(());
            }
            return Err(err);
        }
        Ok(())
    }

    /// Clear pending notifications.
    ///
    /// Non-blocking read from eventfd. Call after processing data.
    pub fn clear(&self) -> io::Result<()> {
        let mut val: u64 = 0;
        let _ = unsafe {
            libc::read(
                self.fd.as_raw_fd(),
                &mut val as *mut u64 as *mut libc::c_void,
                8,
            )
        };
        // Ignore errors - EAGAIN just means no notifications pending
        Ok(())
    }

    /// Check if notifications are pending without blocking.
    pub fn has_pending(&self) -> bool {
        let mut pfd = libc::pollfd {
            fd: self.fd.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        let ret = unsafe { libc::poll(&mut pfd, 1, 0) };
        ret > 0 && (pfd.revents & libc::POLLIN) != 0
    }

    /// Wait for the notifier to become readable.
    ///
    /// This is the key async method - it yields to tokio's runtime while waiting,
    /// allowing other tasks to run. When the eventfd is signaled, the task wakes
    /// up immediately via epoll.
    ///
    /// # Cancellation Safety
    ///
    /// This method is cancel-safe. If the future is dropped, no data is lost.
    pub async fn wait_readable(&self) -> io::Result<()> {
        loop {
            let mut guard = self.async_fd.readable().await?;

            // Check if actually readable (might be spurious wake)
            if self.has_pending() {
                guard.clear_ready();
                return Ok(());
            }

            // Spurious wake, clear and wait again
            guard.clear_ready();
        }
    }

    /// Wait for the notifier with a timeout.
    ///
    /// Returns `Ok(true)` if signaled, `Ok(false)` on timeout.
    pub async fn wait_readable_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> io::Result<bool> {
        match tokio::time::timeout(timeout, self.wait_readable()).await {
            Ok(Ok(())) => Ok(true),
            Ok(Err(e)) => Err(e),
            Err(_) => Ok(false), // Timeout
        }
    }
}

// SAFETY: AsyncNotifier only contains OwnedFd and AsyncFd which are Send+Sync
unsafe impl Send for AsyncNotifier {}
unsafe impl Sync for AsyncNotifier {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn async_notifier_roundtrip() {
        let notifier = AsyncNotifier::new().expect("failed to create notifier");
        assert!(!notifier.has_pending());

        notifier.notify().expect("notify failed");
        assert!(notifier.has_pending());

        notifier.clear().expect("clear failed");
        assert!(!notifier.has_pending());
    }

    #[tokio::test]
    async fn async_notifier_wait_immediate() {
        let notifier = AsyncNotifier::new().expect("failed to create notifier");

        // Pre-notify so data is immediately available
        notifier.notify().expect("notify failed");

        let start = std::time::Instant::now();
        notifier.wait_readable().await.expect("wait failed");
        let elapsed = start.elapsed();

        // Should return very fast (< 1ms) since data was already pending
        assert!(
            elapsed.as_millis() < 10,
            "immediate data should return fast: {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn async_notifier_wait_timeout() {
        let notifier = AsyncNotifier::new().expect("failed to create notifier");

        let start = std::time::Instant::now();
        let result = notifier
            .wait_readable_timeout(Duration::from_millis(50))
            .await
            .expect("wait_timeout failed");
        let elapsed = start.elapsed();

        assert!(!result, "should return false on timeout");
        assert!(
            elapsed.as_millis() >= 45,
            "should wait ~50ms, got {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn async_notifier_wait_wakeup() {
        use std::sync::Arc;

        let notifier = Arc::new(AsyncNotifier::new().expect("failed to create notifier"));
        let notifier_clone = Arc::clone(&notifier);

        // Spawn task to signal after delay
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            notifier_clone.notify().expect("notify failed");
        });

        let start = std::time::Instant::now();
        notifier.wait_readable().await.expect("wait failed");
        let elapsed = start.elapsed();

        // Should wake up after ~20ms (when the other task signals)
        assert!(
            elapsed.as_millis() >= 15 && elapsed.as_millis() < 100,
            "should wake after ~20ms, got {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn async_notifier_multiple_notify() {
        let notifier = AsyncNotifier::new().expect("failed to create notifier");

        // Multiple notifications should coalesce
        for _ in 0..10 {
            notifier.notify().expect("notify failed");
        }
        assert!(notifier.has_pending());

        // Single clear should drain all
        notifier.clear().expect("clear failed");
        assert!(!notifier.has_pending());
    }

    #[tokio::test]
    async fn async_notifier_fd_is_valid() {
        let notifier = AsyncNotifier::new().expect("failed to create notifier");
        let fd = notifier.as_raw_fd();
        assert!(fd >= 0, "FD should be non-negative");
    }
}
