//! Async-signal-safe owned shared-memory registry and stale segment reaping.

use core::cell::UnsafeCell;
use core::sync::atomic::{AtomicBool, AtomicU8, AtomicU16, Ordering};

/// Maximum number of owned shared-memory segments tracked for process cleanup.
pub const MAX_OWNED_SEGMENTS: usize = 256;

const MAX_SEGMENT_NAME_BYTES: usize = 256;

const SLOT_EMPTY: u8 = 0;
const SLOT_WRITING: u8 = 1;
const SLOT_ACTIVE: u8 = 2;
const SLOT_UNLINKING: u8 = 3;

struct OwnedSegmentSlot {
    state: AtomicU8,
    len: AtomicU16,
    bytes: UnsafeCell<[u8; MAX_SEGMENT_NAME_BYTES]>,
}

impl OwnedSegmentSlot {
    const fn new() -> Self {
        Self {
            state: AtomicU8::new(SLOT_EMPTY),
            len: AtomicU16::new(0),
            bytes: UnsafeCell::new([0u8; MAX_SEGMENT_NAME_BYTES]),
        }
    }
}

// SAFETY: `OwnedSegmentSlot` synchronizes every read/write of `len` and `bytes`
// behind atomic state transitions (`SLOT_EMPTY` -> `SLOT_WRITING` -> `SLOT_ACTIVE` -> `SLOT_UNLINKING`).
unsafe impl Sync for OwnedSegmentSlot {}

static OWNED_SEGMENTS: [OwnedSegmentSlot; MAX_OWNED_SEGMENTS] =
    [const { OwnedSegmentSlot::new() }; MAX_OWNED_SEGMENTS];
static OVERFLOW_LOGGED: AtomicBool = AtomicBool::new(false);

/// Register a named POSIX shared-memory segment owned by this process for cleanup on panic or signal.
pub fn register_owned(name: &str) {
    let raw = name.as_bytes();
    if raw.is_empty()
        || !raw.starts_with(b"/")
        || raw.len() >= MAX_SEGMENT_NAME_BYTES
        || raw.contains(&0)
    {
        return;
    }

    for slot in &OWNED_SEGMENTS {
        if slot
            .state
            .compare_exchange(
                SLOT_ACTIVE,
                SLOT_WRITING,
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            let cur_len = slot.len.load(Ordering::Relaxed) as usize;
            // SAFETY: Holding `SLOT_WRITING` gives exclusive access to `slot.bytes`.
            let same = cur_len == raw.len()
                && unsafe {
                    core::slice::from_raw_parts(slot.bytes.get().cast::<u8>(), cur_len) == raw
                };
            slot.state.store(SLOT_ACTIVE, Ordering::Release);
            if same {
                return;
            }
        }
    }

    for slot in &OWNED_SEGMENTS {
        if slot
            .state
            .compare_exchange(
                SLOT_EMPTY,
                SLOT_WRITING,
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            // SAFETY: Holding `SLOT_WRITING` gives exclusive access to `slot.bytes`.
            unsafe {
                let buf = &mut *slot.bytes.get();
                buf[..raw.len()].copy_from_slice(raw);
                buf[raw.len()] = 0;
            }
            slot.len.store(raw.len() as u16, Ordering::Relaxed);
            slot.state.store(SLOT_ACTIVE, Ordering::Release);
            return;
        }
    }

    if !OVERFLOW_LOGGED.swap(true, Ordering::Relaxed) {
        eprintln!(
            "vertebra-core: owned SHM cleanup table full ({MAX_OWNED_SEGMENTS} entries); skipping registration for {name}"
        );
    }
}

/// Remove a previously registered owned shared-memory segment from process cleanup.
pub fn unregister_owned(name: &str) {
    let raw = name.as_bytes();
    if raw.is_empty() || raw.len() >= MAX_SEGMENT_NAME_BYTES {
        return;
    }

    for slot in &OWNED_SEGMENTS {
        if slot
            .state
            .compare_exchange(
                SLOT_ACTIVE,
                SLOT_WRITING,
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            let cur_len = slot.len.load(Ordering::Relaxed) as usize;
            // SAFETY: Holding `SLOT_WRITING` gives exclusive access to `slot.bytes`.
            let matches = cur_len == raw.len()
                && unsafe {
                    core::slice::from_raw_parts(slot.bytes.get().cast::<u8>(), cur_len) == raw
                };
            if matches {
                slot.len.store(0, Ordering::Relaxed);
                slot.state.store(SLOT_EMPTY, Ordering::Release);
            } else {
                slot.state.store(SLOT_ACTIVE, Ordering::Release);
            }
        }
    }
}

/// Unlink every active owned shared-memory segment without allocating or acquiring locks.
pub fn cleanup_all() {
    #[cfg(unix)]
    {
        for slot in &OWNED_SEGMENTS {
            if slot
                .state
                .compare_exchange(
                    SLOT_ACTIVE,
                    SLOT_UNLINKING,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                let cur_len =
                    (slot.len.load(Ordering::Relaxed) as usize).min(MAX_SEGMENT_NAME_BYTES - 1);
                let mut local = [0u8; MAX_SEGMENT_NAME_BYTES];
                // SAFETY: `SLOT_UNLINKING` prevents writers from modifying `slot.bytes`.
                unsafe {
                    let src = core::slice::from_raw_parts(slot.bytes.get().cast::<u8>(), cur_len);
                    local[..cur_len].copy_from_slice(src);
                }
                local[cur_len] = 0;
                if cur_len > 0 {
                    let ptr = local.as_ptr().cast::<libc::c_char>();
                    if local[..cur_len].starts_with(b"/tmp/vtb-") {
                        // SAFETY: `local` is stack-allocated and NUL-terminated.
                        unsafe {
                            let _ = libc::unlink(ptr);
                        }
                    } else {
                        // SAFETY: `local` is stack-allocated and NUL-terminated.
                        unsafe {
                            let _ = libc::shm_unlink(ptr);
                        }
                    }
                }
                slot.len.store(0, Ordering::Relaxed);
                slot.state.store(SLOT_EMPTY, Ordering::Release);
            }
        }
    }
}

#[cfg(unix)]
mod signal_hook {
    use core::mem::MaybeUninit;
    use core::sync::atomic::{AtomicBool, Ordering};

    static INSTALLED: AtomicBool = AtomicBool::new(false);
    static mut PREV_SIGINT: MaybeUninit<libc::sigaction> = MaybeUninit::uninit();
    static mut PREV_SIGTERM: MaybeUninit<libc::sigaction> = MaybeUninit::uninit();

    extern "C" fn handle_termination_signal(
        sig: libc::c_int,
        info: *mut libc::siginfo_t,
        ucontext: *mut libc::c_void,
    ) {
        super::cleanup_all();

        let prev = match sig {
            // SAFETY: `PREV_SIGINT` is populated by `libc::sigaction` before `install()` registers
            // this handler and is never mutated afterward while `INSTALLED` remains true.
            libc::SIGINT => unsafe { (*core::ptr::addr_of!(PREV_SIGINT)).assume_init() },
            // SAFETY: `PREV_SIGTERM` is populated by `libc::sigaction` before `install()` registers
            // this handler and is never mutated afterward while `INSTALLED` remains true.
            libc::SIGTERM => unsafe { (*core::ptr::addr_of!(PREV_SIGTERM)).assume_init() },
            _ => {
                // SAFETY: `libc::_exit` is async-signal-safe and avoids running non-signal-safe
                // Rust destructors or stdio flushes inside a signal context.
                unsafe { libc::_exit(128 + sig) };
            }
        };

        let handler_addr = prev.sa_sigaction;
        if (prev.sa_flags & libc::SA_SIGINFO) != 0 && handler_addr != 0 {
            // SAFETY: `SA_SIGINFO` is set and `handler_addr` is non-null, so `sa_sigaction`
            // holds a valid C `extern "C" fn(c_int, *mut siginfo_t, *mut c_void)` handler.
            let sigaction_fn: extern "C" fn(libc::c_int, *mut libc::siginfo_t, *mut libc::c_void) =
                unsafe { core::mem::transmute(handler_addr) };
            sigaction_fn(sig, info, ucontext);
            return;
        }

        if handler_addr != libc::SIG_DFL && handler_addr != libc::SIG_IGN {
            // SAFETY: `SA_SIGINFO` is unset and `handler_addr` is neither `SIG_DFL` nor `SIG_IGN`,
            // so `sa_sigaction` holds a valid C `extern "C" fn(c_int)` handler.
            let handler_fn: extern "C" fn(libc::c_int) =
                unsafe { core::mem::transmute(handler_addr) };
            handler_fn(sig);
            return;
        }

        if handler_addr == libc::SIG_DFL {
            // SAFETY: `libc::sigaction` is POD valid when zeroed; `sigemptyset`, `sigaction`, and
            // `raise` are async-signal-safe POSIX functions used to re-raise with `SIG_DFL`.
            unsafe {
                let mut dfl: libc::sigaction = core::mem::zeroed();
                dfl.sa_sigaction = libc::SIG_DFL;
                libc::sigemptyset(&mut dfl.sa_mask);
                libc::sigaction(sig, &dfl, core::ptr::null_mut());
                libc::raise(sig);
            }
        }
    }

    pub fn install() -> std::io::Result<()> {
        if INSTALLED
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }

        // SAFETY: `INSTALLED` serializes one-time installation; `action` is zero-initialized and
        // its mask is cleared with `sigemptyset` before `libc::sigaction` writes `PREV_SIGINT` and
        // `PREV_SIGTERM` (`MaybeUninit<libc::sigaction>`) and activates `handle_termination_signal`.
        unsafe {
            let mut action: libc::sigaction = core::mem::zeroed();
            action.sa_sigaction = handle_termination_signal as *const () as usize;
            action.sa_flags = libc::SA_SIGINFO | libc::SA_RESTART;
            libc::sigemptyset(&mut action.sa_mask);

            if libc::sigaction(
                libc::SIGINT,
                &action,
                core::ptr::addr_of_mut!(PREV_SIGINT).cast(),
            ) != 0
            {
                INSTALLED.store(false, Ordering::Release);
                return Err(std::io::Error::last_os_error());
            }
            if libc::sigaction(
                libc::SIGTERM,
                &action,
                core::ptr::addr_of_mut!(PREV_SIGTERM).cast(),
            ) != 0
            {
                INSTALLED.store(false, Ordering::Release);
                return Err(std::io::Error::last_os_error());
            }
        }
        Ok(())
    }
}

/// Install async-signal-safe `SIGINT` and `SIGTERM` handlers that invoke [`cleanup_all`].
pub fn install_signal_cleanup() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        signal_hook::install()
    }
    #[cfg(not(unix))]
    {
        Ok(())
    }
}

/// Reap shared-memory segments under `/dev/shm` matching `prefixes` whose owner PID is dead.
///
/// Segments without a valid [`crate::shm::ShmOwnerHeader`] or owned by a live PID are preserved.
pub fn reap_dead_owner_segments(prefixes: &[&str]) -> u32 {
    #[cfg(unix)]
    {
        let shm_dir = std::path::Path::new("/dev/shm");
        if !shm_dir.exists() {
            return 0;
        }
        let Ok(entries) = std::fs::read_dir(shm_dir) else {
            return 0;
        };

        let mut reaped = 0u32;
        for entry in entries.flatten() {
            let Some(file_name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let matches_prefix = prefixes.iter().any(|prefix| {
                let bare = prefix.strip_prefix('/').unwrap_or(prefix);
                !bare.is_empty() && file_name.starts_with(bare)
            });
            if !matches_prefix {
                continue;
            }
            let posix_name = format!("/{file_name}");
            if crate::shm::try_unlink_if_dead_owner(&posix_name) {
                reaped += 1;
            }
        }
        reaped
    }
    #[cfg(not(unix))]
    {
        let _ = prefixes;
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registration_handles_overflow_without_panicking() {
        let mut names = Vec::new();
        for idx in 0..(MAX_OWNED_SEGMENTS + 8) {
            let name = crate::shm::unique_test_shm_name(&format!("/vtb-reg-cap-{idx}"));
            register_owned(&name);
            names.push(name);
        }
        for name in &names {
            unregister_owned(name);
        }
    }
}
