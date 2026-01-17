//! Cross-process wait/wake primitives.

use core::cell::UnsafeCell;
use core::ops::{Deref, DerefMut};
use core::sync::atomic::{AtomicU32, Ordering};
use std::io;
use std::time::{Duration, Instant};

use std::fs::OpenOptions;
use std::path::PathBuf;
use std::sync::atomic::AtomicU8;
use std::sync::OnceLock;

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

#[cfg(unix)]
struct FileLock {
    file: std::fs::File,
}

#[cfg(unix)]
impl FileLock {
    fn lock(&self) -> io::Result<()> {
        rustix::fs::flock(&self.file, rustix::fs::FlockOperation::LockExclusive).map_err(Into::into)
    }

    fn try_lock(&self) -> io::Result<bool> {
        match rustix::fs::flock(
            &self.file,
            rustix::fs::FlockOperation::NonBlockingLockExclusive,
        ) {
            Ok(()) => Ok(true),
            Err(err) => {
                let io_err: io::Error = err.into();
                if io_err.kind() == io::ErrorKind::WouldBlock {
                    Ok(false)
                } else {
                    Err(io_err)
                }
            }
        }
    }

    fn unlock(&self) -> io::Result<()> {
        rustix::fs::flock(&self.file, rustix::fs::FlockOperation::Unlock).map_err(Into::into)
    }
}

#[cfg(unix)]
fn fallback_path() -> PathBuf {
    if let Ok(path) = std::env::var("VERTEBRA_FLOCK_PATH") {
        return PathBuf::from(path);
    }
    std::env::temp_dir().join("vertebra.lock")
}

#[cfg(unix)]
fn fallback_lock() -> Result<&'static FileLock, SyncError> {
    static FILE_LOCK: OnceLock<Result<FileLock, io::Error>> = OnceLock::new();
    let lock = FILE_LOCK.get_or_init(|| {
        let path = fallback_path();
        OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)
            .map(|file| FileLock { file })
    });
    match lock {
        Ok(lock) => Ok(lock),
        Err(err) => Err(SyncError::Syscall(io::Error::new(
            err.kind(),
            err.to_string(),
        ))),
    }
}

#[cfg(not(unix))]
fn fallback_lock() -> Result<&'static (), SyncError> {
    Err(SyncError::Unsupported)
}

fn enable_fallback(reason: &str) {
    let previous = WAIT_STATE.swap(WAIT_STATE_FALLBACK, Ordering::Release);
    if previous != WAIT_STATE_FALLBACK {
        eprintln!("WARN: wait/wake unsupported; falling back to file locks ({reason})");
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

    pub fn wait(&self, expected: u32) -> Result<(), SyncError> {
        wait_on_address(&self.word, expected)
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
        return Err(SyncError::Unsupported);
    }
    match platform::wait_on_address(word, expected) {
        Ok(()) => {
            mark_wait_ready();
            Ok(())
        }
        Err(err) if wait_is_unsupported(&err) => {
            enable_fallback("wait primitive unsupported");
            Err(SyncError::Unsupported)
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

/// A shared mutex intended for use in shared memory.
#[repr(C)]
pub struct SharedMutex<T> {
    futex: AtomicU32,
    value: UnsafeCell<T>,
}

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
        if fallback_enabled() {
            return self.lock_fallback();
        }
        loop {
            if self
                .futex
                .compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
            {
                return Ok(MutexGuard {
                    mutex: self,
                    fallback: false,
                });
            }
            let state = self.futex.load(Ordering::Acquire);
            if let Err(err) = wait_on_address(&self.futex, state) {
                if wait_is_unsupported(&err) {
                    enable_fallback("wait primitive unavailable for SharedMutex");
                    return self.lock_fallback();
                }
                return Err(err);
            }
        }
    }

    pub fn try_lock(&self) -> Result<Option<MutexGuard<'_, T>>, SyncError> {
        if fallback_enabled() {
            return self.try_lock_fallback();
        }
        if self
            .futex
            .compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            Ok(Some(MutexGuard {
                mutex: self,
                fallback: false,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn lock_timeout(&self, timeout: Duration) -> Result<Option<MutexGuard<'_, T>>, SyncError> {
        if fallback_enabled() {
            return self.lock_timeout_fallback(timeout);
        }
        let start = Instant::now();
        loop {
            if let Some(guard) = self.try_lock()? {
                return Ok(Some(guard));
            }
            if start.elapsed() >= timeout {
                return Ok(None);
            }
            let state = self.futex.load(Ordering::Acquire);
            if let Err(err) = wait_on_address(&self.futex, state) {
                if wait_is_unsupported(&err) {
                    enable_fallback("wait primitive unavailable for SharedMutex");
                    return self.lock_timeout_fallback(timeout);
                }
                return Err(err);
            }
        }
    }

    fn unlock(&self) -> Result<(), SyncError> {
        self.futex.store(0, Ordering::Release);
        wake_one(&self.futex)
    }

    fn lock_fallback(&self) -> Result<MutexGuard<'_, T>, SyncError> {
        let lock = fallback_lock()?;
        #[cfg(unix)]
        {
            lock.lock().map_err(SyncError::Syscall)?;
        }
        Ok(MutexGuard {
            mutex: self,
            fallback: true,
        })
    }

    fn try_lock_fallback(&self) -> Result<Option<MutexGuard<'_, T>>, SyncError> {
        let lock = fallback_lock()?;
        #[cfg(unix)]
        {
            if !lock.try_lock().map_err(SyncError::Syscall)? {
                return Ok(None);
            }
        }
        Ok(Some(MutexGuard {
            mutex: self,
            fallback: true,
        }))
    }

    fn lock_timeout_fallback(
        &self,
        timeout: Duration,
    ) -> Result<Option<MutexGuard<'_, T>>, SyncError> {
        let start = Instant::now();
        loop {
            if let Some(guard) = self.try_lock_fallback()? {
                return Ok(Some(guard));
            }
            if start.elapsed() >= timeout {
                return Ok(None);
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn unlock_fallback(&self) -> Result<(), SyncError> {
        let lock = fallback_lock()?;
        #[cfg(unix)]
        {
            lock.unlock().map_err(SyncError::Syscall)?;
        }
        Ok(())
    }
}

pub struct MutexGuard<'a, T> {
    mutex: &'a SharedMutex<T>,
    fallback: bool,
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
        if self.fallback {
            let _ = self.mutex.unlock_fallback();
        } else {
            let _ = self.mutex.unlock();
        }
    }
}

/// Shared reader-writer lock.
#[repr(C)]
pub struct SharedRwLock<T> {
    state: AtomicU32,
    value: UnsafeCell<T>,
}

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
                if let Err(err) = wait_on_address(&self.state, state) {
                    if wait_is_unsupported(&err) {
                        std::thread::yield_now();
                        continue;
                    }
                    return Err(err);
                }
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
                if let Err(err) = wait_on_address(&self.state, state) {
                    if wait_is_unsupported(&err) {
                        std::thread::yield_now();
                        continue;
                    }
                    return Err(err);
                }
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
        if let Err(err) = wait_on_address(&self.seq, seq) {
            if wait_is_unsupported(&err) {
                std::thread::sleep(Duration::from_millis(1));
            } else {
                return Err(err);
            }
        }
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
    #[cfg(unix)]
    use crate::shm::ShmRegion;
    #[cfg(unix)]
    use std::process::Command;

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
                let _ = cond.notify_one();
            });
            let guard = mutex.lock().unwrap();
            let guard = cond.wait(guard).unwrap();
            drop(guard);
        });
    }

    #[test]
    fn shared_condvar_wait_while() {
        let mutex = SharedMutex::new(0u32);
        let cond = SharedCondvar::new();
        std::thread::scope(|s| {
            s.spawn(|| {
                let mut guard = mutex.lock().unwrap();
                *guard = 1;
                let _ = cond.notify_one();
            });
            let guard = mutex.lock().unwrap();
            let guard = cond.wait_while(guard, |value| *value == 0).unwrap();
            assert_eq!(*guard, 1);
        });
    }

    #[cfg(unix)]
    #[test]
    fn shared_mutex_multi_process() {
        if std::env::var("VERTEBRA_MUTEX_CHILD").is_ok() {
            let name = std::env::var("VERTEBRA_MUTEX_NAME").unwrap();
            let size: usize = std::env::var("VERTEBRA_MUTEX_SIZE")
                .unwrap()
                .parse()
                .unwrap();
            let region = ShmRegion::open_named(&name, size).unwrap();
            let ptr = region.as_ptr().cast::<SharedMutex<u32>>();
            let mutex = unsafe { &*ptr };
            let mut guard = mutex.lock().unwrap();
            *guard += 1;
            return;
        }

        let name = format!("/vertebra-mutex-{}", std::process::id());
        let size = core::mem::size_of::<SharedMutex<u32>>();
        let mut region = match ShmRegion::create_named(&name, size) {
            Ok(region) => region,
            Err(crate::shm::ShmError::Sys(err))
                if err.kind() == io::ErrorKind::PermissionDenied =>
            {
                return;
            }
            Err(err) => panic!("create_named failed: {err:?}"),
        };
        let ptr = unsafe {
            region
                .as_mut_slice()
                .as_mut_ptr()
                .cast::<SharedMutex<u32>>()
        };
        unsafe {
            ptr.write(SharedMutex::new(0u32));
        }

        let exe = std::env::current_exe().unwrap();
        let status = Command::new(exe)
            .env("VERTEBRA_MUTEX_CHILD", "1")
            .env("VERTEBRA_MUTEX_NAME", &name)
            .env("VERTEBRA_MUTEX_SIZE", size.to_string())
            .arg("--exact")
            .arg("shared_mutex_multi_process")
            .arg("--nocapture")
            .status()
            .unwrap();
        assert!(status.success());

        let mutex = unsafe { &*ptr };
        let mut guard = mutex.lock().unwrap();
        *guard += 1;
        assert_eq!(*guard, 2);
    }
}
