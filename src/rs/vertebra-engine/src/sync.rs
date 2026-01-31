//! Cross-process wait/wake primitives.
//!
//! Provides futex-based synchronization:
//! - Linux: FUTEX_WAIT/FUTEX_WAKE syscalls
//! - macOS: spin + nanosleep fallback (ulock requires special feature)
//! - Windows: WaitOnAddress/WakeByAddressAll
//!
//! Ported from litecore::sync

use core::cell::UnsafeCell;
use core::ops::{Deref, DerefMut};
use core::sync::atomic::{AtomicU32, Ordering};
use std::io;
use std::time::{Duration, Instant};

/// Errors that can occur during synchronization operations.
#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    #[error("platform wait is not implemented")]
    Unsupported,
    #[error("system call failed: {0}")]
    Syscall(io::Error),
}

// ============================================================================
// Platform-specific wait/wake primitives
// ============================================================================

/// Wait on an atomic word until its value changes from `expected`.
///
/// Returns immediately if the current value differs from expected (spurious wakeup ok).
pub fn wait_on_address(word: &AtomicU32, expected: u32) -> Result<(), SyncError> {
    #[cfg(target_os = "linux")]
    {
        return linux_futex_wait(word, expected);
    }

    #[cfg(target_os = "macos")]
    {
        return macos_spin_sleep_wait(word, expected);
    }

    #[cfg(windows)]
    {
        return windows_wait(word, expected);
    }

    #[allow(unreachable_code)]
    Err(SyncError::Unsupported)
}

/// Wait on an atomic word with timeout.
///
/// Returns `Ok(true)` if woken normally, `Ok(false)` if timed out.
pub fn wait_on_address_timeout(
    word: &AtomicU32,
    expected: u32,
    timeout: Duration,
) -> Result<bool, SyncError> {
    if timeout.is_zero() {
        return Ok(false);
    }

    #[cfg(target_os = "linux")]
    {
        return linux_futex_wait_timeout(word, expected, timeout);
    }

    #[cfg(target_os = "macos")]
    {
        return macos_spin_sleep_wait_timeout(word, expected, timeout);
    }

    #[cfg(windows)]
    {
        return windows_wait_timeout(word, expected, timeout);
    }

    #[allow(unreachable_code)]
    Err(SyncError::Unsupported)
}

/// Wake one thread waiting on the atomic word.
pub fn wake_one(word: &AtomicU32) -> Result<(), SyncError> {
    #[cfg(target_os = "linux")]
    {
        return linux_futex_wake(word, 1);
    }

    #[cfg(target_os = "macos")]
    {
        // macOS spin-sleep fallback doesn't have explicit wake
        let _ = word.load(Ordering::Acquire);
        return Ok(());
    }

    #[cfg(windows)]
    {
        return windows_wake_one(word);
    }

    #[allow(unreachable_code)]
    Err(SyncError::Unsupported)
}

/// Wake all threads waiting on the atomic word.
pub fn wake_all(word: &AtomicU32) -> Result<(), SyncError> {
    #[cfg(target_os = "linux")]
    {
        return linux_futex_wake(word, i32::MAX);
    }

    #[cfg(target_os = "macos")]
    {
        // macOS spin-sleep fallback doesn't have explicit wake
        let _ = word.load(Ordering::Acquire);
        return Ok(());
    }

    #[cfg(windows)]
    {
        return windows_wake_all(word);
    }

    #[allow(unreachable_code)]
    Err(SyncError::Unsupported)
}

// ============================================================================
// Linux implementation (futex)
// ============================================================================

#[cfg(target_os = "linux")]
fn linux_futex_wait(word: &AtomicU32, expected: u32) -> Result<(), SyncError> {
    let addr = word as *const AtomicU32 as *const u32;
    let res = unsafe {
        libc::syscall(
            libc::SYS_futex,
            addr,
            libc::FUTEX_WAIT,
            expected,
            std::ptr::null::<libc::timespec>(),
        )
    } as i64;

    if res == 0 {
        return Ok(());
    }

    let err = io::Error::last_os_error();
    match err.raw_os_error() {
        Some(libc::EAGAIN) | Some(libc::EINTR) => Ok(()),
        _ => Err(SyncError::Syscall(err)),
    }
}

#[cfg(target_os = "linux")]
fn linux_futex_wait_timeout(
    word: &AtomicU32,
    expected: u32,
    timeout: Duration,
) -> Result<bool, SyncError> {
    let addr = word as *const AtomicU32 as *const u32;
    let secs = timeout.as_secs().min(i64::MAX as u64) as i64;
    let ts = libc::timespec {
        tv_sec: secs as libc::time_t,
        tv_nsec: timeout.subsec_nanos() as libc::c_long,
    };

    let res = unsafe {
        libc::syscall(
            libc::SYS_futex,
            addr,
            libc::FUTEX_WAIT,
            expected,
            &ts as *const libc::timespec,
        )
    } as i64;

    if res == 0 {
        return Ok(true);
    }

    let err = io::Error::last_os_error();
    match err.raw_os_error() {
        Some(libc::ETIMEDOUT) => Ok(false),
        Some(libc::EAGAIN) | Some(libc::EINTR) => Ok(true),
        _ => Err(SyncError::Syscall(err)),
    }
}

#[cfg(target_os = "linux")]
fn linux_futex_wake(word: &AtomicU32, count: i32) -> Result<(), SyncError> {
    let addr = word as *const AtomicU32 as *const u32;
    let res = unsafe { libc::syscall(libc::SYS_futex, addr, libc::FUTEX_WAKE, count) } as i64;

    if res >= 0 {
        Ok(())
    } else {
        Err(SyncError::Syscall(io::Error::last_os_error()))
    }
}

// ============================================================================
// macOS implementation (spin + nanosleep fallback)
// ============================================================================

#[cfg(target_os = "macos")]
const MACOS_SPIN_ITERS: usize = 1024;
#[cfg(target_os = "macos")]
const MACOS_SLEEP_NS: i64 = 1_000_000; // 1ms

#[cfg(target_os = "macos")]
fn macos_spin_sleep_wait(word: &AtomicU32, expected: u32) -> Result<(), SyncError> {
    let mut spins = 0usize;
    loop {
        if word.load(Ordering::Acquire) != expected {
            return Ok(());
        }
        if spins < MACOS_SPIN_ITERS {
            spins += 1;
            std::hint::spin_loop();
            continue;
        }
        let ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: MACOS_SLEEP_NS,
        };
        unsafe {
            libc::nanosleep(&ts, std::ptr::null_mut());
        }
    }
}

#[cfg(target_os = "macos")]
fn macos_spin_sleep_wait_timeout(
    word: &AtomicU32,
    expected: u32,
    timeout: Duration,
) -> Result<bool, SyncError> {
    let deadline = Instant::now() + timeout;
    let mut spins = 0usize;
    loop {
        if word.load(Ordering::Acquire) != expected {
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Ok(false);
        }
        if spins < MACOS_SPIN_ITERS {
            spins += 1;
            std::hint::spin_loop();
            continue;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        let sleep_ns = (remaining.as_nanos().min(MACOS_SLEEP_NS as u128)) as i64;
        let ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: sleep_ns.max(0),
        };
        unsafe {
            libc::nanosleep(&ts, std::ptr::null_mut());
        }
    }
}

// ============================================================================
// Windows implementation (WaitOnAddress)
// ============================================================================

#[cfg(windows)]
fn windows_wait(word: &AtomicU32, expected: u32) -> Result<(), SyncError> {
    use windows_sys::Win32::System::Threading::WaitOnAddress;

    let addr = word as *const AtomicU32 as *mut std::ffi::c_void;
    let expected_ptr = &expected as *const u32 as *const std::ffi::c_void;
    let result = unsafe { WaitOnAddress(addr, expected_ptr, std::mem::size_of::<u32>(), u32::MAX) };

    if result != 0 {
        Ok(())
    } else {
        Err(SyncError::Syscall(io::Error::last_os_error()))
    }
}

#[cfg(windows)]
fn windows_wait_timeout(
    word: &AtomicU32,
    expected: u32,
    timeout: Duration,
) -> Result<bool, SyncError> {
    use windows_sys::Win32::Foundation::WAIT_TIMEOUT;
    use windows_sys::Win32::System::Threading::WaitOnAddress;

    let addr = word as *const AtomicU32 as *mut std::ffi::c_void;
    let expected_ptr = &expected as *const u32 as *const std::ffi::c_void;
    let millis = timeout.as_millis().min(u32::MAX as u128) as u32;
    let result = unsafe { WaitOnAddress(addr, expected_ptr, std::mem::size_of::<u32>(), millis) };

    if result != 0 {
        Ok(true)
    } else {
        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(WAIT_TIMEOUT as i32) {
            Ok(false)
        } else {
            Err(SyncError::Syscall(err))
        }
    }
}

#[cfg(windows)]
fn windows_wake_one(word: &AtomicU32) -> Result<(), SyncError> {
    use windows_sys::Win32::System::Threading::WakeByAddressSingle;

    let addr = word as *const AtomicU32 as *const std::ffi::c_void;
    unsafe { WakeByAddressSingle(addr) };
    Ok(())
}

#[cfg(windows)]
fn windows_wake_all(word: &AtomicU32) -> Result<(), SyncError> {
    use windows_sys::Win32::System::Threading::WakeByAddressAll;

    let addr = word as *const AtomicU32 as *const std::ffi::c_void;
    unsafe { WakeByAddressAll(addr) };
    Ok(())
}

// ============================================================================
// WaitWord abstraction
// ============================================================================

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

// ============================================================================
// SharedMutex - mutex for shared memory
// ============================================================================

/// A shared mutex intended for use in shared memory.
///
/// Uses futex-based blocking on Linux, spin+sleep on macOS.
#[repr(C)]
pub struct SharedMutex<T> {
    futex: AtomicU32,
    value: UnsafeCell<T>,
}

// SAFETY: SharedMutex is designed for cross-process sharing
unsafe impl<T: Send> Send for SharedMutex<T> {}
unsafe impl<T: Send> Sync for SharedMutex<T> {}

impl<T> SharedMutex<T> {
    pub const fn new(value: T) -> Self {
        Self {
            futex: AtomicU32::new(0),
            value: UnsafeCell::new(value),
        }
    }

    pub fn lock(&self) -> Result<MutexGuard<'_, T>, SyncError> {
        loop {
            if self
                .futex
                .compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
            {
                return Ok(MutexGuard { mutex: self });
            }
            let state = self.futex.load(Ordering::Acquire);
            wait_on_address(&self.futex, state)?;
        }
    }

    pub fn try_lock(&self) -> Result<Option<MutexGuard<'_, T>>, SyncError> {
        if self
            .futex
            .compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            Ok(Some(MutexGuard { mutex: self }))
        } else {
            Ok(None)
        }
    }

    pub fn lock_timeout(&self, timeout: Duration) -> Result<Option<MutexGuard<'_, T>>, SyncError> {
        let start = Instant::now();
        loop {
            if let Some(guard) = self.try_lock()? {
                return Ok(Some(guard));
            }
            if start.elapsed() >= timeout {
                return Ok(None);
            }
            let state = self.futex.load(Ordering::Acquire);
            wait_on_address(&self.futex, state)?;
        }
    }

    fn unlock(&self) -> Result<(), SyncError> {
        self.futex.store(0, Ordering::Release);
        wake_one(&self.futex)
    }
}

/// Guard for SharedMutex that releases the lock on drop.
pub struct MutexGuard<'a, T> {
    mutex: &'a SharedMutex<T>,
}

impl<T> Deref for MutexGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.mutex.value.get() }
    }
}

impl<T> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.mutex.value.get() }
    }
}

impl<T> Drop for MutexGuard<'_, T> {
    fn drop(&mut self) {
        let _ = self.mutex.unlock();
    }
}

// ============================================================================
// SharedRwLock - readers-writer lock for shared memory
// ============================================================================

/// Shared reader-writer lock.
///
/// Supports multiple concurrent readers or one exclusive writer.
#[repr(C)]
pub struct SharedRwLock<T> {
    state: AtomicU32,
    value: UnsafeCell<T>,
}

// SAFETY: SharedRwLock is designed for cross-process sharing
unsafe impl<T: Send + Sync> Send for SharedRwLock<T> {}
unsafe impl<T: Send + Sync> Sync for SharedRwLock<T> {}

const WRITE_BIT: u32 = 1 << 31;

impl<T> SharedRwLock<T> {
    pub const fn new(value: T) -> Self {
        Self {
            state: AtomicU32::new(0),
            value: UnsafeCell::new(value),
        }
    }

    pub fn read(&self) -> Result<RwLockReadGuard<'_, T>, SyncError> {
        loop {
            let state = self.state.load(Ordering::Acquire);
            if state & WRITE_BIT != 0 {
                wait_on_address(&self.state, state)?;
                continue;
            }
            if self
                .state
                .compare_exchange(state, state + 1, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
            {
                return Ok(RwLockReadGuard { lock: self });
            }
        }
    }

    pub fn try_read(&self) -> Result<Option<RwLockReadGuard<'_, T>>, SyncError> {
        let state = self.state.load(Ordering::Acquire);
        if state & WRITE_BIT != 0 {
            return Ok(None);
        }
        if self
            .state
            .compare_exchange(state, state + 1, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            Ok(Some(RwLockReadGuard { lock: self }))
        } else {
            Ok(None)
        }
    }

    pub fn write(&self) -> Result<RwLockWriteGuard<'_, T>, SyncError> {
        loop {
            let state = self.state.load(Ordering::Acquire);
            if state != 0 {
                wait_on_address(&self.state, state)?;
                continue;
            }
            if self
                .state
                .compare_exchange(0, WRITE_BIT, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
            {
                return Ok(RwLockWriteGuard { lock: self });
            }
        }
    }

    pub fn try_write(&self) -> Result<Option<RwLockWriteGuard<'_, T>>, SyncError> {
        if self
            .state
            .compare_exchange(0, WRITE_BIT, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            Ok(Some(RwLockWriteGuard { lock: self }))
        } else {
            Ok(None)
        }
    }

    fn read_unlock(&self) -> Result<(), SyncError> {
        self.state.fetch_sub(1, Ordering::Release);
        wake_all(&self.state)
    }

    fn write_unlock(&self) -> Result<(), SyncError> {
        self.state.store(0, Ordering::Release);
        wake_all(&self.state)
    }
}

/// Guard for SharedRwLock read access.
pub struct RwLockReadGuard<'a, T> {
    lock: &'a SharedRwLock<T>,
}

impl<T> Deref for RwLockReadGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.value.get() }
    }
}

impl<T> Drop for RwLockReadGuard<'_, T> {
    fn drop(&mut self) {
        let _ = self.lock.read_unlock();
    }
}

/// Guard for SharedRwLock write access.
pub struct RwLockWriteGuard<'a, T> {
    lock: &'a SharedRwLock<T>,
}

impl<T> Deref for RwLockWriteGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.value.get() }
    }
}

impl<T> DerefMut for RwLockWriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.lock.value.get() }
    }
}

impl<T> Drop for RwLockWriteGuard<'_, T> {
    fn drop(&mut self) {
        let _ = self.lock.write_unlock();
    }
}

// ============================================================================
// SharedCondvar - condition variable for shared memory
// ============================================================================

/// Shared condition variable implemented with a sequence counter.
#[repr(C)]
pub struct SharedCondvar {
    seq: AtomicU32,
}

impl SharedCondvar {
    pub const fn new() -> Self {
        Self {
            seq: AtomicU32::new(0),
        }
    }

    pub fn notify_one(&self) -> Result<(), SyncError> {
        self.seq.fetch_add(1, Ordering::Release);
        wake_one(&self.seq)
    }

    pub fn notify_all(&self) -> Result<(), SyncError> {
        self.seq.fetch_add(1, Ordering::Release);
        wake_all(&self.seq)
    }

    pub fn wait<'a, T>(&self, guard: MutexGuard<'a, T>) -> Result<MutexGuard<'a, T>, SyncError> {
        let seq = self.seq.load(Ordering::Acquire);
        let mutex = guard.mutex;
        drop(guard);
        wait_on_address(&self.seq, seq)?;
        mutex.lock()
    }

    pub fn wait_while<'a, T, F>(
        &self,
        mut guard: MutexGuard<'a, T>,
        mut condition: F,
    ) -> Result<MutexGuard<'a, T>, SyncError>
    where
        F: FnMut(&mut T) -> bool,
    {
        while condition(&mut guard) {
            guard = self.wait(guard)?;
        }
        Ok(guard)
    }
}

impl Default for SharedCondvar {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_mutex_basic() {
        let mutex = SharedMutex::new(5u32);
        {
            let mut guard = mutex.lock().unwrap();
            *guard += 1;
        }
        let guard = mutex.lock().unwrap();
        assert_eq!(*guard, 6);
    }

    #[test]
    fn shared_rwlock_basic() {
        let lock = SharedRwLock::new(10u32);
        let guard = lock.read().unwrap();
        assert_eq!(*guard, 10);
        drop(guard);
        let mut guard = lock.write().unwrap();
        *guard += 2;
        drop(guard);
        let guard = lock.read().unwrap();
        assert_eq!(*guard, 12);
    }

    #[test]
    fn shared_condvar_notify() {
        let mutex = SharedMutex::new(false);
        let cond = SharedCondvar::new();

        std::thread::scope(|s| {
            s.spawn(|| {
                let mut guard = mutex.lock().unwrap();
                *guard = true;
                let _ = cond.notify_one();
            });

            let guard = mutex.lock().unwrap();
            let guard = cond.wait_while(guard, |ready| !*ready).unwrap();
            assert!(*guard);
        });
    }

    #[test]
    fn shared_mutex_try_lock() {
        let mutex = SharedMutex::new(42u32);
        let guard = mutex.try_lock().unwrap();
        assert!(guard.is_some());
        assert_eq!(*guard.unwrap(), 42);
    }

    #[test]
    fn shared_rwlock_multiple_readers() {
        let lock = SharedRwLock::new(100u32);
        let guard1 = lock.read().unwrap();
        let guard2 = lock.try_read().unwrap();
        assert!(guard2.is_some());
        assert_eq!(*guard1, 100);
        assert_eq!(*guard2.unwrap(), 100);
    }
}
