//! Cross-process wait/wake primitives.

use core::sync::atomic::{AtomicU32, Ordering};
use std::io;
use std::sync::atomic::AtomicU8;
use std::time::{Duration, Instant};

use crate::platform;

#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    #[error("platform wait is not implemented")]
    Unsupported,
    #[error("system call failed: {0}")]
    Syscall(io::Error),
}

const WAIT_STATE_UNKNOWN: u8 = 0;
const WAIT_STATE_READY: u8 = 1;
const WAIT_STATE_FALLBACK: u8 = 2;

static WAIT_STATE: AtomicU8 = AtomicU8::new(WAIT_STATE_UNKNOWN);

fn enable_fallback(reason: &str) {
    let previous = WAIT_STATE.swap(WAIT_STATE_FALLBACK, Ordering::Release);
    if previous != WAIT_STATE_FALLBACK {
        eprintln!("WARN: wait/wake unsupported; falling back to thread sleep ({reason})");
    }
}

fn fallback_enabled() -> bool {
    WAIT_STATE.load(Ordering::Acquire) == WAIT_STATE_FALLBACK
}

fn mark_wait_ready() {
    let _ = WAIT_STATE.compare_exchange(
        WAIT_STATE_UNKNOWN,
        WAIT_STATE_READY,
        Ordering::Release,
        Ordering::Relaxed,
    );
}

fn wait_is_unsupported(err: &SyncError) -> bool {
    match err {
        SyncError::Unsupported => true,
        SyncError::Syscall(io_err) => {
            #[cfg(unix)]
            {
                io_err.raw_os_error() == Some(libc::ENOSYS)
            }
            #[cfg(not(unix))]
            {
                let _ = io_err;
                false
            }
        }
    }
}

/// A shared wait word used for address-based waits (futex/ulock/WaitOnAddress).
#[repr(transparent)]
pub struct WaitWord {
    word: AtomicU32,
}

impl WaitWord {
    pub const fn new(value: u32) -> Self {
        Self {
            word: AtomicU32::new(value),
        }
    }

    pub fn load(&self) -> u32 {
        self.word.load(Ordering::Acquire)
    }

    pub fn store(&self, value: u32) {
        self.word.store(value, Ordering::Release);
    }

    pub fn fetch_add(&self, val: u32) -> u32 {
        self.word.fetch_add(val, Ordering::AcqRel)
    }

    pub fn wait(&self, expected: u32) -> Result<(), SyncError> {
        wait_on_address(&self.word, expected)
    }

    pub fn wait_timeout(&self, expected: u32, timeout: Duration) -> Result<bool, SyncError> {
        wait_on_address_timeout(&self.word, expected, timeout)
    }

    pub fn wake_one(&self) -> Result<(), SyncError> {
        wake_one(&self.word)
    }

    pub fn wake_all(&self) -> Result<(), SyncError> {
        wake_all(&self.word)
    }
}

pub fn wait_on_address(word: &AtomicU32, expected: u32) -> Result<(), SyncError> {
    if fallback_enabled() {
        while !fallback_wait_timeout(word, expected, Duration::from_secs(60)) {}
        return Ok(());
    }
    match platform::wait_on_address(word, expected) {
        Ok(()) => {
            mark_wait_ready();
            Ok(())
        }
        Err(err) if wait_is_unsupported(&err) => {
            enable_fallback("wait primitive unsupported");
            while !fallback_wait_timeout(word, expected, Duration::from_secs(60)) {}
            Ok(())
        }
        Err(err) => Err(err),
    }
}

pub fn wait_on_address_timeout(
    word: &AtomicU32,
    expected: u32,
    timeout: Duration,
) -> Result<bool, SyncError> {
    if timeout.is_zero() {
        return Ok(false);
    }
    if fallback_enabled() {
        return Ok(fallback_wait_timeout(word, expected, timeout));
    }
    match platform::wait_on_address_timeout(word, expected, timeout) {
        Ok(result) => {
            mark_wait_ready();
            Ok(result)
        }
        Err(err) if wait_is_unsupported(&err) => {
            enable_fallback("wait primitive unsupported");
            Ok(fallback_wait_timeout(word, expected, timeout))
        }
        Err(err) => Err(err),
    }
}

pub fn wake_one(word: &AtomicU32) -> Result<(), SyncError> {
    platform::wake_one(word)
}

pub fn wake_all(word: &AtomicU32) -> Result<(), SyncError> {
    platform::wake_all(word)
}

fn fallback_wait_timeout(word: &AtomicU32, expected: u32, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if word.load(Ordering::Acquire) != expected {
            return true;
        }
        let now = Instant::now();
        if now >= deadline {
            return false;
        }
        let remaining = deadline - now;
        std::thread::sleep(remaining.min(Duration::from_millis(1)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wait_word_load_store() {
        let word = WaitWord::new(42);
        assert_eq!(word.load(), 42);
        word.store(100);
        assert_eq!(word.load(), 100);
    }
}
