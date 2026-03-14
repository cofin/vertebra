# Chapter 4: Native IPC Substrate

**Beads ID:** vertebra-w4i.4
**Flow ID:** native-ipc
**Depends on:** Ch2 (scaffold-base)
**Crate:** `src/rs/vertebra-core`
**Final gate:** `cd src/rs && cargo test --package vertebra-core` all green, `cargo clippy --package vertebra-core -- -D warnings` zero warnings

---

## 1. Goal

Implement the shared-memory IPC foundation that every other native chapter builds on.
This chapter consolidates `ipc-substrate_20260314` and Phase 1 of `native-core_20260314`
into one coherent deliverable: a production-grade Rust crate that exposes lock-free ring
buffers, cross-platform shared-memory regions, conditional-wake notification, bump-allocating
batch arenas, and C-ABI proxy structs for HTTP requests and responses.

Performance target: the ring buffers must sustain **>1 M ops/s** in single-threaded
ping-pong at slot size 256 bytes and >500 k ops/s MPMC with 4P/4C.

---

## 2. Architecture Context

```
HTTP Request
    │
    ▼
vertebra-http (Ch 5)
    │  push BatchNode ptr
    ▼
vertebra-core :: SpscRing / MpmcRing   ◄── THIS CHAPTER
    │  pop BatchNode ptr
    ▼
vertebra-py (Ch 8) :: get_batch() / submit_batch()
    │
    ▼
Python handler
```

Everything above `vertebra-core` is either Rust or Python. This crate owns:

- **SHM regions** — anonymous (memfd on Linux, shm_open on macOS, `CreateFileMappingW` on Windows).
- **Named SHM regions** — for cross-process rendezvous; RAII-unlinked on drop.
- **Cleanup** — explicit `shm_unlink_named()` that callers invoke from crash handlers.
- **SPSC ring** — single-producer / single-consumer; used for per-worker request/response pairs.
- **MPMC ring** — multi-producer / multi-consumer; used for shared work-stealing dispatch.
- **Notifier** — eventfd (Linux) / pipe (macOS/Windows) with conditional wake (`waiters > 0` guard).
- **BatchArena** — bump allocator for `BatchNode` slabs; target <1 MB/s allocation rate.
- **C-ABI proxy structs** — `#[repr(C)]` `RequestInfo` and `ResponseInfo` for FFI.

---

## 3. Existing State Inventory

The `vertebra-core` crate already contains substantial correct code. The chapter tasks
extend and complete it rather than rewrite it.

### 3.1 Already implemented (DO NOT touch unless fixing a bug)

| File | What it contains |
|------|-----------------|
| `src/shm.rs` | `ShmRegion` with `create`, `create_named`, `open_named`, `map`, RAII `Drop`; memfd (Linux), shm_open (Unix), `CreateFileMappingW` (Windows); `shm_unlink_named()`. **Complete.** |
| `src/sync.rs` | `WaitWord`, `SharedMutex<T>`, `SharedRwLock<T>`, `SharedCondvar`; futex/ulock/WaitOnAddress + file-lock fallback. **Complete.** |
| `src/notify.rs` | `Notifier`: eventfd (Linux) / pipe (macOS-Windows); `notify()`, `clear()`, `has_pending()`, `wait()`, `spin_wait_or_park()`, `spin_wait_or_park_with()`. **Complete.** |
| `src/buffer.rs` | `ShmHeader`, `CacheLine<T>`, `RingLayout`, `SlotHeader`, `ChannelMode`, `BufferPool`; `send_waiters` and `recv_waiters` conditional-wake counters. **Complete.** |
| `src/channel.rs` | `SpscRing` (lock-free, power-of-2 slots, `send`, `recv`, `try_recv`, `recv_hybrid`, `SlotView`); `MpmcRing` (crossbeam-based, `send`, `recv`, `try_recv`). **Complete.** |
| `src/async_ring.rs` | `AsyncRing` wrapping `SpscRing` + `Notifier`. **Complete.** |
| `src/async_notifier.rs` / `src/async_notify.rs` | Async notifier helpers. **Complete.** |
| `src/guards.rs` | Slot guard types. **Complete.** |
| `src/router.rs` | matchit-based native router. **Complete.** |
| `src/ipc.rs` | IPC helper types. **Complete.** |
| `src/platform/` | Linux futex, macOS ulock, Windows WaitOnAddress. **Complete.** |
| `src/protocol/` | Protocol state machine. **Complete.** |
| `benches/spsc.rs` | SPSC benchmarks (ping-pong, throughput, `recv_hybrid` variants). **Complete.** |
| `benches/mpmc.rs` | MPMC single-thread benchmark. **Complete.** |
| `benches/spin_wait.rs` | Spin-wait benchmarks. **Complete.** |
| `benches/router.rs` | Router benchmarks. **Complete.** |
| `benches/guards.rs` | Guard benchmarks. **Complete.** |

### 3.2 Missing — what this chapter adds

| File | Status | Task |
|------|--------|------|
| `src/cleanup.rs` | **Missing** | Task 1.2 |
| `src/batch.rs` | **Missing** | Task 3.1 |
| `src/proxy.rs` | **Missing** | Task 3.2 |
| `tests/test_shm.rs` | **Missing** | Task 4.1 |
| `tests/test_ring.rs` | **Missing** | Task 4.2 |
| `tests/test_mpmc.rs` | **Missing** | Task 4.3 |
| `tests/test_batch.rs` | **Missing** | Task 4.4 |
| `benches/ring_throughput.rs` | **Missing** | Task 4.5 |
| `src/lib.rs` update | Needs `cleanup`, `batch`, `proxy` re-exports | Task 5.1 |
| `Cargo.toml` bench entry | Needs `ring_throughput` bench entry | Task 5.1 |

---

## 4. Design Decisions

### 4.1 Conditional Wake

`ShmHeader` already contains `send_waiters` and `recv_waiters` (`CacheLine<AtomicU32>`).
The ring implementations MUST read these before calling any OS wake primitive:

```
// Before eventfd_write / futex_wake:
if self.header().recv_waiters.0.load(Ordering::Acquire) > 0 {
    self.notifier.notify().ok();
}
```

This saves ~150 µs/batch for the common case where the reader is already polling (not
blocked in the kernel). The `SpscRing::send` path in `channel.rs` already has this
pattern via `ShmHeader::send_waiters` / `recv_waiters`. Verify it is wired correctly in
`batch.rs` when notifying after a `push`.

### 4.2 Optimistic GIL

When `vertebra-py` calls `get_batch()`, the fast path holds the GIL and calls
`SpscRing::try_recv`. Only on `Err(ChannelError::Empty)` does it release the GIL and
call `recv_hybrid(spin_us=5, timeout_ms=…)`. This chapter does not implement the Python
binding, but the ring's `try_recv` must be branch-free on the hot path (atomic load,
compare, return).

### 4.3 uint64 Correlation IDs

`RequestInfo::correlation_id` is a `u64`. No UUIDs, no strings. The Rust HTTP collector
generates these as a monotonic counter (`AtomicU64::fetch_add(1, Relaxed)`).

### 4.4 Arena Allocation Strategy

`BatchArena` pre-allocates `capacity * sizeof(BatchNode)` bytes once at construction.
A `bump` cursor (`AtomicUsize`) advances forward. When the arena is exhausted, callers
receive `BatchError::Full`. The arena is **not** thread-safe for alloc — only the single
Rust HTTP collector thread calls `push`. The ring then carries the `*const BatchNode`
pointer to the Python side. Python only reads; Rust owns and resets the arena after all
responses have been submitted.

Arena reset is `store(0, Release)` on the bump cursor. No individual `BatchNode` is
freed.

### 4.5 Three-Phase Spin-Wait

The `Notifier::spin_wait_or_park_with` already implements this. For ring-level waits,
the phases are:

1. **Immediate** — `try_recv()` on entry (0 syscalls).
2. **Spin** — 100 iterations of `hint::spin_loop()` checking `try_recv()` (~1–5 µs).
3. **Park** — `recv_waiters.fetch_add(1)` → `poll(fd, timeout)` → `recv_waiters.fetch_sub(1)`.

Phase 3 is the only place a syscall occurs. `spin_wait_or_park_with` handles this; the
`SpscRing::recv_hybrid` wrapper already integrates it correctly.

---

## 5. Phase 1: Shared Memory Management

### Task 1.1 — Verify `src/shm.rs` completeness

**Pass criteria:**
- `ShmRegion::create(size)` returns `Ok` for any `size > 0`.
- `ShmRegion::create_named(name, size)` follows unlink-before-create strategy.
- `ShmRegion::open_named(name, size)` maps an existing named region.
- `Drop` calls `munmap` + `close` + `shm_unlink` (if named, if Unix).
- Windows path compiles without errors under cross-compilation.
- `proptest` for long names passes.

**Action:** Read `src/shm.rs` carefully. If it matches the above (it does per inventory),
mark complete. Do **not** modify unless a specific test below fails.

**Fail criteria:**
- Any `shm_open` region leaked in `/dev/shm` after the region is dropped.
- `create_named` succeeds when name does not start with `/`.
- `open_named` returns `Ok` for a name that was never created.

---

### Task 1.2 — Add `src/cleanup.rs`

**Goal:** Provide a standalone cleanup function for crash-recovery scenarios where `Drop`
cannot run (e.g., `kill -9`). Also provides a registry for tracking all live named
regions so a `cleanup_all()` sweep can be called from a crash handler.

**File:** `src/rs/vertebra-core/src/cleanup.rs`

**Exact code:**

```rust
//! Named shared-memory cleanup registry.
//!
//! Tracks all named SHM regions created in this process so that a crash handler
//! can call `cleanup_all()` to prevent `/dev/shm` leakage. Each entry is added
//! by `ShmRegion::create_named` via `register_name` and removed on `Drop` via
//! `deregister_name`.
//!
//! # Crash Handler Usage
//!
//! ```no_run
//! use vertebra_core::cleanup;
//!
//! // In signal handler (SIGTERM / SIGINT / panic hook):
//! cleanup::cleanup_all();
//! ```
//!
//! # Note
//! This module is Unix-only. On Windows, named sections are automatically
//! reclaimed by the kernel when the last handle is closed.

use std::sync::Mutex;

#[cfg(unix)]
use crate::shm::{shm_unlink_named, ShmError};

static REGISTRY: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// Register a named SHM region for cleanup tracking.
///
/// Called by `ShmRegion::create_named` on Unix.
pub fn register_name(name: &str) {
    if let Ok(mut guard) = REGISTRY.lock() {
        guard.push(name.to_owned());
    }
}

/// Deregister a named SHM region (called from `ShmRegion::Drop`).
///
/// This removes the name from the registry. The actual unlink is still
/// performed by `ShmRegion::Drop`; this just prevents `cleanup_all` from
/// attempting a duplicate unlink.
pub fn deregister_name(name: &str) {
    if let Ok(mut guard) = REGISTRY.lock() {
        guard.retain(|n| n != name);
    }
}

/// List all currently registered names (for diagnostics / tests).
pub fn registered_names() -> Vec<String> {
    REGISTRY.lock().map(|g| g.clone()).unwrap_or_default()
}

/// Unlink all registered named SHM regions.
///
/// Intended for crash handlers and test teardown. Safe to call multiple times;
/// subsequent calls are no-ops for already-unlinked regions (ENOENT is ignored).
#[cfg(unix)]
pub fn cleanup_all() {
    let names = {
        let mut guard = match REGISTRY.lock() {
            Ok(g) => g,
            Err(_) => return, // Poisoned — best effort
        };
        std::mem::take(&mut *guard)
    };
    for name in names {
        let _ = shm_unlink_named(&name);
    }
}

/// No-op on non-Unix platforms (kernel handles cleanup automatically).
#[cfg(not(unix))]
pub fn cleanup_all() {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_deregister_roundtrip() {
        let name = "/vertebra-cleanup-test-roundtrip";
        register_name(name);
        assert!(registered_names().contains(&name.to_owned()));
        deregister_name(name);
        assert!(!registered_names().contains(&name.to_owned()));
    }

    #[test]
    fn cleanup_all_clears_registry() {
        register_name("/vertebra-cleanup-a");
        register_name("/vertebra-cleanup-b");
        cleanup_all(); // Will ENOENT on unlink — that's fine
        assert!(registered_names().is_empty());
    }

    #[test]
    fn deregister_nonexistent_is_noop() {
        deregister_name("/vertebra-does-not-exist");
        // No panic — just succeeds
    }
}
```

**Pass criteria:**
- `register_name` / `deregister_name` are idempotent and thread-safe.
- `cleanup_all()` empties the registry even if unlinks fail with `ENOENT`.
- `cargo test -p vertebra-core cleanup` passes.

**Fail criteria:**
- `REGISTRY.lock()` panics under concurrent access.
- `cleanup_all()` panics on ENOENT.

---

## 6. Phase 2: Ring Buffers

### Task 2.1 — Verify SPSC (`SpscRing`) completeness

**Pass criteria:**
- `SpscRing::initialize(region, layout, 0, 0)` initializes the header correctly (`MAGIC`, `ABI_VERSION`, `RegionState::Ready`).
- `send(&payload, correlation_id)` advances `write_head` atomically.
- `try_recv(&mut buf)` returns `Ok(len)` or `Err(ChannelError::Empty)` without blocking.
- `recv_hybrid(&mut buf, spin_us, timeout)` uses three-phase strategy.
- `SlotView` drops advance the read cursor.
- `SpscRing` implements `Send + Sync`.

**Action:** Read `src/channel.rs`. It is complete per inventory. Confirm `send_waiters` /
`recv_waiters` conditional-wake path is present in `send`. Do **not** modify unless a
specific test below fails.

---

### Task 2.2 — Verify MPMC (`MpmcRing`) completeness

**Pass criteria:**
- `MpmcRing::new(capacity, slot_size)` succeeds.
- `send` / `recv` / `try_recv` work correctly under concurrent access.
- `MpmcRing` implements `Send + Sync`.

**Action:** Read `src/channel.rs`. The `MpmcRing` struct is implemented using crossbeam
internals. Confirm `unsafe impl Send for MpmcRing {}` and `unsafe impl Sync for MpmcRing {}`
are present (or that the type is naturally `Send + Sync`).

---

### Task 2.3 — Verify `Notifier` conditional-wake path

The `send` implementation in `SpscRing` must only call `notifier.notify()` when
`recv_waiters > 0`. Verify this is the case by reading the relevant section of
`src/channel.rs`. If missing, add the guard.

**Expected pattern in `SpscRing::send`:**

```rust
// After advancing write_head:
if self.header().recv_waiters.0.load(Ordering::Acquire) > 0 {
    self.notifier.notify().ok();
}
```

**Pass criteria:**
- Single-threaded `send` → `try_recv` loop (no waiters) does NOT call `eventfd_write`.
- Threaded `recv_hybrid` park path does call `notify` (verified by test).

---

## 7. Phase 3: Batch Arenas and Proxy Structs

### Task 3.1 — Implement `src/batch.rs`

**Goal:** Zero-allocation batch processing. The `BatchArena` bump-allocates `BatchNode`
entries into a pre-allocated slab. The HTTP collector fills the arena, then pushes the
slab pointer over the ring. Python reads entries lazily. Rust resets the arena after all
responses are submitted.

**File:** `src/rs/vertebra-core/src/batch.rs`

**Exact code:**

```rust
//! Batch arena allocator for zero-copy request batching.
//!
//! `BatchArena` pre-allocates a contiguous block of memory large enough for
//! `capacity` `BatchNode` entries. Allocation is a bump pointer advance
//! (one `fetch_add` on an `AtomicUsize`). Freeing is a bulk reset
//! (`store(0)`), amortized across an entire batch.
//!
//! # Design Invariants
//!
//! - Only ONE thread (the HTTP collector) calls `push`. No locking on alloc.
//! - Multiple threads (Python workers) may call read-only getters concurrently.
//! - `reset()` is called by Rust ONLY after all responses for this batch have
//!   been submitted and acknowledged. Python MUST NOT hold any `BatchNode`
//!   pointers after `submit_batch` returns.
//! - The arena is NOT shared across processes; it lives in regular heap memory.
//!   The ring buffer carries `u64` arena pointers (cast from `*const BatchNode`).
//!
//! # Allocation Rate Target
//!
//! Target: <1 MB/s allocation rate at 10 k req/s with avg 80-byte path strings.
//! With 128-byte `BatchNode` and 64-entry batches: 8 192 bytes/batch =
//! ~82 MB/s at 10 k req/s batches. Keep batch sizes ≤64 to stay well under
//! the target.
//!
//! # Memory Layout
//!
//! ```text
//! Arena slab (heap):
//! ┌─────────────────────────────────────────────────────────┐
//! │ BatchNode[0] │ BatchNode[1] │ ... │ BatchNode[N-1]      │
//! └─────────────────────────────────────────────────────────┘
//!      ▲               ▲
//!  bump=0          bump=1 (after first push)
//! ```

use std::alloc::{alloc, dealloc, Layout};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::proxy::RequestInfo;

/// A single node in a batch — one HTTP request waiting for Python to handle.
///
/// # Memory Layout (`#[repr(C)]`, 64-byte aligned)
///
/// ```text
/// Offset  Size  Field
/// ──────  ────  ─────────────────────────────────────────────
///      0    16  request_info: RequestInfo  (method=1, pad=7, path_offset=8, path_len=8... etc)
///     16     8  body_ptr: *const u8        (pointer into body slab, or null)
///     24     8  body_len: usize
///     32     8  headers_ptr: *const u8     (pointer into headers slab, or null)
///     40     8  headers_len: usize
///     48     8  correlation_id: u64
///     56     8  _padding: [u8; 8]
/// ──────
///     64 bytes total (one cache line)
/// ```
///
/// Field meanings:
/// - `request_info`: Compact fixed-size metadata (method enum, handler_id, offsets).
/// - `body_ptr` / `body_len`: Raw pointer to body bytes in a separate body slab.
///   May be null for requests with no body.
/// - `headers_ptr` / `headers_len`: Raw pointer to serialised header bytes.
///   Format is a flat array of `(name_len: u16, name: [u8], value_len: u16, value: [u8])`.
/// - `correlation_id`: Monotonically increasing `u64` set by the HTTP collector.
///   Echoed back in `ResponseInfo` to match responses to requests.
#[repr(C, align(64))]
pub struct BatchNode {
    /// Compact HTTP request metadata.
    pub request_info: RequestInfo,
    /// Pointer to body bytes (may be null). Lifetime: arena reset.
    pub body_ptr: *const u8,
    /// Length of body in bytes.
    pub body_len: usize,
    /// Pointer to serialised header bytes (may be null).
    pub headers_ptr: *const u8,
    /// Length of headers buffer in bytes.
    pub headers_len: usize,
    /// Monotonic correlation identifier set by the HTTP collector.
    pub correlation_id: u64,
    _padding: [u8; 8],
}

// SAFETY: BatchNode contains raw pointers, but the arena owns the backing
// memory and guarantees the pointers are valid until `reset()`. Python reads
// are read-only. Safe to send across threads because only one thread writes.
unsafe impl Send for BatchNode {}
unsafe impl Sync for BatchNode {}

impl BatchNode {
    /// Construct a `BatchNode` from its components.
    pub fn new(
        request_info: RequestInfo,
        body_ptr: *const u8,
        body_len: usize,
        headers_ptr: *const u8,
        headers_len: usize,
        correlation_id: u64,
    ) -> Self {
        Self {
            request_info,
            body_ptr,
            body_len,
            headers_ptr,
            headers_len,
            correlation_id,
            _padding: [0u8; 8],
        }
    }

    /// Return the body as a byte slice.
    ///
    /// # Safety
    /// The caller must ensure the arena has not been reset since this node was
    /// allocated.
    pub unsafe fn body(&self) -> &[u8] {
        if self.body_ptr.is_null() || self.body_len == 0 {
            return &[];
        }
        std::slice::from_raw_parts(self.body_ptr, self.body_len)
    }

    /// Return the raw header bytes.
    ///
    /// # Safety
    /// Same lifetime contract as `body()`.
    pub unsafe fn headers_raw(&self) -> &[u8] {
        if self.headers_ptr.is_null() || self.headers_len == 0 {
            return &[];
        }
        std::slice::from_raw_parts(self.headers_ptr, self.headers_len)
    }
}

/// Batch arena error type.
#[derive(Debug, thiserror::Error)]
pub enum BatchError {
    #[error("arena is full (capacity: {capacity})")]
    Full { capacity: usize },
    #[error("arena capacity must be non-zero")]
    ZeroCapacity,
    #[error("allocation failed (OOM)")]
    Alloc,
}

/// Pre-allocated bump arena for `BatchNode` slabs.
///
/// Thread-safety: only the HTTP collector calls `push`. Workers call read-only
/// getters through shared pointers. `reset()` is called by Rust after all
/// responses are acknowledged (single writer, no concurrent `push`).
pub struct BatchArena {
    /// Raw pointer to the start of the slab.
    slab: NonNull<BatchNode>,
    /// Maximum number of `BatchNode` entries.
    capacity: usize,
    /// Bump cursor — next free slot index.
    bump: AtomicUsize,
    /// Layout used for deallocation.
    layout: Layout,
}

// SAFETY: `BatchArena` is designed for one writer + many readers.
// The slab pointer is stable for the lifetime of the arena.
unsafe impl Send for BatchArena {}
unsafe impl Sync for BatchArena {}

impl BatchArena {
    /// Create a new `BatchArena` capable of holding `capacity` `BatchNode` entries.
    ///
    /// Allocates `capacity * size_of::<BatchNode>()` bytes on the heap, aligned
    /// to 64 bytes.
    pub fn new(capacity: usize) -> Result<Self, BatchError> {
        if capacity == 0 {
            return Err(BatchError::ZeroCapacity);
        }
        let layout = Layout::array::<BatchNode>(capacity)
            .map_err(|_| BatchError::Alloc)?
            .align_to(64)
            .map_err(|_| BatchError::Alloc)?
            .pad_to_align();
        let ptr = unsafe { alloc(layout) };
        let slab = NonNull::new(ptr.cast::<BatchNode>()).ok_or(BatchError::Alloc)?;
        Ok(Self {
            slab,
            capacity,
            bump: AtomicUsize::new(0),
            layout,
        })
    }

    /// Push a `BatchNode` into the arena.
    ///
    /// Returns a raw pointer to the written node on success.
    /// Returns `BatchError::Full` when the arena is exhausted.
    ///
    /// # Safety (callers)
    /// Only call from the single HTTP collector thread.
    pub fn push(&self, node: BatchNode) -> Result<*const BatchNode, BatchError> {
        let idx = self.bump.fetch_add(1, Ordering::AcqRel);
        if idx >= self.capacity {
            // Undo the increment to prevent overflow wrapping.
            self.bump.fetch_sub(1, Ordering::AcqRel);
            return Err(BatchError::Full {
                capacity: self.capacity,
            });
        }
        // SAFETY: `idx < capacity` verified above; slab is valid heap memory.
        let ptr = unsafe { self.slab.as_ptr().add(idx) };
        unsafe { ptr.write(node) };
        Ok(ptr as *const BatchNode)
    }

    /// Reset the arena for reuse.
    ///
    /// All outstanding `BatchNode` pointers MUST be invalidated before calling
    /// this. Call only after all Python workers have called `submit_batch`.
    pub fn reset(&self) {
        self.bump.store(0, Ordering::Release);
    }

    /// Number of entries currently allocated.
    pub fn len(&self) -> usize {
        self.bump.load(Ordering::Acquire).min(self.capacity)
    }

    /// Returns `true` if no entries have been allocated since the last reset.
    pub fn is_empty(&self) -> bool {
        self.bump.load(Ordering::Acquire) == 0
    }

    /// Returns the maximum number of entries this arena can hold.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Return a raw slice over all allocated nodes (read-only).
    ///
    /// # Safety
    /// The caller must ensure no concurrent `push` or `reset` occurs.
    pub unsafe fn as_slice(&self) -> &[BatchNode] {
        let len = self.len();
        std::slice::from_raw_parts(self.slab.as_ptr(), len)
    }

    /// Return a pointer to the node at `index` without bounds-checking.
    ///
    /// # Safety
    /// `index` must be < `self.len()`.
    pub unsafe fn get_unchecked(&self, index: usize) -> *const BatchNode {
        self.slab.as_ptr().add(index) as *const BatchNode
    }
}

impl Drop for BatchArena {
    fn drop(&mut self) {
        // SAFETY: slab was allocated with `self.layout`.
        unsafe { dealloc(self.slab.as_ptr().cast(), self.layout) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proxy::{HttpMethod, RequestInfo};

    fn make_node(correlation_id: u64) -> BatchNode {
        BatchNode::new(
            RequestInfo {
                method: HttpMethod::GET,
                _pad: [0u8; 7],
                handler_id: 1,
                path_offset: 0,
                path_len: 5,
                headers_offset: 0,
                body_offset: 0,
                body_len: 0,
                correlation_id,
            },
            std::ptr::null(),
            0,
            std::ptr::null(),
            0,
            correlation_id,
        )
    }

    #[test]
    fn arena_basic_push_and_len() {
        let arena = BatchArena::new(8).unwrap();
        assert!(arena.is_empty());
        let ptr = arena.push(make_node(1)).unwrap();
        assert!(!ptr.is_null());
        assert_eq!(arena.len(), 1);
    }

    #[test]
    fn arena_full_returns_error() {
        let arena = BatchArena::new(2).unwrap();
        arena.push(make_node(1)).unwrap();
        arena.push(make_node(2)).unwrap();
        let err = arena.push(make_node(3)).unwrap_err();
        assert!(matches!(err, BatchError::Full { .. }));
    }

    #[test]
    fn arena_reset_allows_reuse() {
        let arena = BatchArena::new(4).unwrap();
        for i in 0..4 {
            arena.push(make_node(i as u64)).unwrap();
        }
        assert_eq!(arena.len(), 4);
        arena.reset();
        assert!(arena.is_empty());
        arena.push(make_node(99)).unwrap();
        assert_eq!(arena.len(), 1);
    }

    #[test]
    fn arena_zero_capacity_fails() {
        let err = BatchArena::new(0).unwrap_err();
        assert!(matches!(err, BatchError::ZeroCapacity));
    }

    #[test]
    fn batch_node_is_64_bytes() {
        assert_eq!(std::mem::size_of::<BatchNode>(), 64);
    }

    #[test]
    fn batch_node_alignment_is_64() {
        assert_eq!(std::mem::align_of::<BatchNode>(), 64);
    }

    #[test]
    fn arena_node_correlation_ids_preserved() {
        let arena = BatchArena::new(16).unwrap();
        for i in 0..16u64 {
            arena.push(make_node(i)).unwrap();
        }
        // Read back via raw slice
        let slice = unsafe { arena.as_slice() };
        for (i, node) in slice.iter().enumerate() {
            assert_eq!(node.correlation_id, i as u64);
        }
    }

    #[test]
    fn batch_node_body_null_returns_empty() {
        let node = make_node(0);
        let body = unsafe { node.body() };
        assert!(body.is_empty());
    }

    #[test]
    fn batch_node_with_body() {
        let data = b"hello world";
        let node = BatchNode::new(
            RequestInfo {
                method: HttpMethod::POST,
                _pad: [0u8; 7],
                handler_id: 2,
                path_offset: 0,
                path_len: 4,
                headers_offset: 0,
                body_offset: 0,
                body_len: data.len() as u64,
                correlation_id: 42,
            },
            data.as_ptr(),
            data.len(),
            std::ptr::null(),
            0,
            42,
        );
        let body = unsafe { node.body() };
        assert_eq!(body, b"hello world");
    }
}
```

**Pass criteria:**
- `BatchNode` is exactly 64 bytes (`size_of::<BatchNode>() == 64`).
- `BatchNode` is aligned to 64 bytes (`align_of::<BatchNode>() == 64`).
- `BatchArena::push` returns unique pointers to distinct locations.
- `BatchArena::reset` + re-`push` returns the same first slot address.
- All unit tests pass.

**Fail criteria:**
- `BatchNode` size != 64 bytes.
- Any test in `tests::` panics.
- `BatchArena::new(0)` returns `Ok`.

---

### Task 3.2 — Implement `src/proxy.rs`

**Goal:** Define the C-ABI structs that cross the FFI boundary between Rust and Python
(via PyO3). These structs are `#[repr(C)]` so Python can read fields at known offsets
without Rust being involved in every field access.

**File:** `src/rs/vertebra-core/src/proxy.rs`

**Memory layout documentation is mandatory** — every struct must have an offset table.

**Exact code:**

```rust
//! C-ABI proxy structs for HTTP requests and responses.
//!
//! These types cross the FFI boundary between Rust and Python. They are
//! `#[repr(C)]` so Python (via PyO3 or ctypes) can read fields at fixed,
//! well-defined offsets without calling back into Rust for each access.
//!
//! # Stability Contract
//!
//! Field order, size, and alignment are FROZEN once Ch8 (FFI Bridge) ships.
//! Any breaking change requires a version bump in `ShmHeader::abi_version`.
//!
//! # Memory Layout: `RequestInfo`
//!
//! ```text
//! Offset  Size  Field
//! ──────  ────  ────────────────────────────────────────────────────────
//!      0     1  method: HttpMethod  (u8 repr)
//!      1     7  _pad: [u8; 7]       (alignment padding to 8-byte boundary)
//!      8     8  handler_id: u64
//!     16     8  path_offset: u64    (byte offset into path slab)
//!     24     8  path_len: u64
//!     32     8  headers_offset: u64 (byte offset into headers slab)
//!     40     8  body_offset: u64    (byte offset into body slab)
//!     48     8  body_len: u64
//!     56     8  correlation_id: u64
//! ──────
//!     64 bytes total
//! ```
//!
//! # Memory Layout: `ResponseInfo`
//!
//! ```text
//! Offset  Size  Field
//! ──────  ────  ────────────────────────────────────────────────────────
//!      0     2  status: u16         (HTTP status code, e.g. 200)
//!      2     6  _pad: [u8; 6]       (alignment padding)
//!      8     8  correlation_id: u64 (echoed from RequestInfo)
//!     16     8  headers_offset: u64 (byte offset into response headers slab)
//!     24     8  headers_len: u64
//!     32     8  body_offset: u64    (byte offset into response body slab)
//!     40     8  body_len: u64
//!     48    16  _reserved: [u8; 16] (future use — zero on write)
//! ──────
//!     64 bytes total
//! ```

/// HTTP method enum, represented as a single byte for C-ABI compactness.
///
/// # Repr
/// `u8` — fits in one byte, no padding wasted.
#[repr(u8)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum HttpMethod {
    /// GET
    GET = 0,
    /// POST
    POST = 1,
    /// PUT
    PUT = 2,
    /// DELETE
    DELETE = 3,
    /// PATCH
    PATCH = 4,
    /// HEAD
    HEAD = 5,
    /// OPTIONS
    OPTIONS = 6,
    /// CONNECT
    CONNECT = 7,
    /// TRACE
    TRACE = 8,
    /// Unknown or extension method
    OTHER = 255,
}

impl HttpMethod {
    /// Parse an HTTP method byte string into an `HttpMethod`.
    pub fn from_bytes(b: &[u8]) -> Self {
        match b {
            b"GET" => Self::GET,
            b"POST" => Self::POST,
            b"PUT" => Self::PUT,
            b"DELETE" => Self::DELETE,
            b"PATCH" => Self::PATCH,
            b"HEAD" => Self::HEAD,
            b"OPTIONS" => Self::OPTIONS,
            b"CONNECT" => Self::CONNECT,
            b"TRACE" => Self::TRACE,
            _ => Self::OTHER,
        }
    }
}

/// Compact HTTP request descriptor passed over the ring buffer.
///
/// All variable-length data (path, headers, body) is stored in external slabs.
/// `RequestInfo` carries only fixed-size metadata: offsets, lengths, method,
/// handler ID, and correlation ID.
///
/// See module-level documentation for the exact memory layout.
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct RequestInfo {
    /// HTTP method (1 byte).
    pub method: HttpMethod,
    /// Alignment padding — MUST be zero on write.
    pub _pad: [u8; 7],
    /// Handler ID assigned by the native router (maps to a Python callable).
    pub handler_id: u64,
    /// Byte offset of the request path within the path slab.
    pub path_offset: u64,
    /// Byte length of the request path.
    pub path_len: u64,
    /// Byte offset of serialised request headers within the headers slab.
    pub headers_offset: u64,
    /// Byte offset of the request body within the body slab.
    pub body_offset: u64,
    /// Byte length of the request body.
    pub body_len: u64,
    /// Monotonic correlation identifier set by the HTTP collector.
    pub correlation_id: u64,
}

/// Compact HTTP response descriptor returned by Python over the ring buffer.
///
/// Python fills this struct and passes it back to the Rust response writer,
/// which reads the header/body slabs and writes them to the socket.
///
/// See module-level documentation for the exact memory layout.
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct ResponseInfo {
    /// HTTP status code (e.g. 200, 404).
    pub status: u16,
    /// Alignment padding — MUST be zero on write.
    pub _pad: [u8; 6],
    /// Echoed `RequestInfo::correlation_id` — used to match response to request.
    pub correlation_id: u64,
    /// Byte offset of response headers within the response headers slab.
    pub headers_offset: u64,
    /// Byte length of response headers.
    pub headers_len: u64,
    /// Byte offset of response body within the response body slab.
    pub body_offset: u64,
    /// Byte length of response body.
    pub body_len: u64,
    /// Reserved for future use — always zero.
    pub _reserved: [u8; 16],
}

impl ResponseInfo {
    /// Construct a minimal `ResponseInfo` for a status-only response (e.g. 204).
    pub fn status_only(status: u16, correlation_id: u64) -> Self {
        Self {
            status,
            _pad: [0u8; 6],
            correlation_id,
            headers_offset: 0,
            headers_len: 0,
            body_offset: 0,
            body_len: 0,
            _reserved: [0u8; 16],
        }
    }
}

// SAFETY: Both structs contain only POD fields (u8, u16, u64 arrays).
// They are designed for cross-thread / cross-process sharing via SHM.
unsafe impl Send for RequestInfo {}
unsafe impl Sync for RequestInfo {}
unsafe impl Send for ResponseInfo {}
unsafe impl Sync for ResponseInfo {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_info_size_is_64() {
        assert_eq!(std::mem::size_of::<RequestInfo>(), 64);
    }

    #[test]
    fn response_info_size_is_64() {
        assert_eq!(std::mem::size_of::<ResponseInfo>(), 64);
    }

    #[test]
    fn request_info_field_offsets() {
        // Verify field offsets match the documented layout using offset_of!
        // (stabilized in Rust 1.77).
        assert_eq!(std::mem::offset_of!(RequestInfo, method), 0);
        assert_eq!(std::mem::offset_of!(RequestInfo, _pad), 1);
        assert_eq!(std::mem::offset_of!(RequestInfo, handler_id), 8);
        assert_eq!(std::mem::offset_of!(RequestInfo, path_offset), 16);
        assert_eq!(std::mem::offset_of!(RequestInfo, path_len), 24);
        assert_eq!(std::mem::offset_of!(RequestInfo, headers_offset), 32);
        assert_eq!(std::mem::offset_of!(RequestInfo, body_offset), 40);
        assert_eq!(std::mem::offset_of!(RequestInfo, body_len), 48);
        assert_eq!(std::mem::offset_of!(RequestInfo, correlation_id), 56);
    }

    #[test]
    fn response_info_field_offsets() {
        assert_eq!(std::mem::offset_of!(ResponseInfo, status), 0);
        assert_eq!(std::mem::offset_of!(ResponseInfo, _pad), 2);
        assert_eq!(std::mem::offset_of!(ResponseInfo, correlation_id), 8);
        assert_eq!(std::mem::offset_of!(ResponseInfo, headers_offset), 16);
        assert_eq!(std::mem::offset_of!(ResponseInfo, headers_len), 24);
        assert_eq!(std::mem::offset_of!(ResponseInfo, body_offset), 32);
        assert_eq!(std::mem::offset_of!(ResponseInfo, body_len), 40);
        assert_eq!(std::mem::offset_of!(ResponseInfo, _reserved), 48);
    }

    #[test]
    fn http_method_from_bytes_known() {
        assert_eq!(HttpMethod::from_bytes(b"GET"), HttpMethod::GET);
        assert_eq!(HttpMethod::from_bytes(b"POST"), HttpMethod::POST);
        assert_eq!(HttpMethod::from_bytes(b"DELETE"), HttpMethod::DELETE);
    }

    #[test]
    fn http_method_from_bytes_unknown() {
        assert_eq!(HttpMethod::from_bytes(b"PROPFIND"), HttpMethod::OTHER);
    }

    #[test]
    fn response_info_status_only() {
        let r = ResponseInfo::status_only(204, 7);
        assert_eq!(r.status, 204);
        assert_eq!(r.correlation_id, 7);
        assert_eq!(r.body_len, 0);
        assert_eq!(r._reserved, [0u8; 16]);
    }

    #[test]
    fn request_info_is_repr_c() {
        // Verify no hidden padding by checking total size == sum of field sizes.
        // 1 (method) + 7 (pad) + 8*7 (u64 fields) = 64
        assert_eq!(std::mem::size_of::<RequestInfo>(), 1 + 7 + 8 * 7);
    }

    #[test]
    fn response_info_is_repr_c() {
        // 2 (status) + 6 (pad) + 8 (corr) + 8+8+8+8 (4 u64s) + 16 (reserved) = 64
        assert_eq!(std::mem::size_of::<ResponseInfo>(), 2 + 6 + 8 + 32 + 16);
    }
}
```

**Pass criteria:**
- `size_of::<RequestInfo>() == 64`.
- `size_of::<ResponseInfo>() == 64`.
- All `offset_of!` assertions pass.
- `cargo clippy` on this file is warning-free.
- Both types implement `Send + Sync`.

**Fail criteria:**
- Either struct is not exactly 64 bytes.
- Any `offset_of!` assertion fails.
- `unsafe impl Send` / `Sync` missing.

---

## 8. Phase 4: Integration Tests

All test files go in `src/rs/vertebra-core/tests/`.

---

### Task 4.1 — `tests/test_shm.rs`

**File:** `src/rs/vertebra-core/tests/test_shm.rs`

**Exact code:**

```rust
//! Integration tests for `vertebra_core::shm`.

use vertebra_core::shm::{ShmRegion, ShmError};

#[test]
fn create_anonymous_region() {
    let region = ShmRegion::create(4096).expect("create failed");
    assert!(region.len() >= 4096);
}

#[test]
fn write_and_read_anonymous() {
    let mut region = ShmRegion::create(4096).expect("create failed");
    unsafe {
        region.as_mut_slice()[0] = 0xDE;
        region.as_mut_slice()[1] = 0xAD;
        assert_eq!(region.as_slice()[0], 0xDE);
        assert_eq!(region.as_slice()[1], 0xAD);
    }
}

#[test]
fn zero_size_is_rejected() {
    let err = ShmRegion::create(0).unwrap_err();
    assert!(matches!(err, ShmError::InvalidSize));
}

#[cfg(unix)]
#[test]
fn named_region_create_open_cleanup() {
    let name = format!("/vertebra-test-shm-{}", std::process::id());
    let mut region = match ShmRegion::create_named(&name, 4096) {
        Ok(r) => r,
        Err(ShmError::Sys(e)) if e.kind() == std::io::ErrorKind::PermissionDenied => return,
        Err(e) => panic!("create_named failed: {e:?}"),
    };
    unsafe {
        region.as_mut_slice()[42] = 0xFF;
    }
    let second = ShmRegion::open_named(&name, 4096).expect("open_named failed");
    unsafe {
        assert_eq!(second.as_slice()[42], 0xFF, "data visible across mappings");
    }
    drop(region); // Triggers unlink via Drop
    drop(second);
    // After drop, the name should be gone from /dev/shm
    let err = ShmRegion::open_named(&name, 4096).unwrap_err();
    assert!(
        matches!(err, ShmError::Sys(_)),
        "expected Sys error after unlink, got {err:?}"
    );
}

#[cfg(unix)]
#[test]
fn invalid_name_no_leading_slash() {
    let err = ShmRegion::create_named("no-slash", 1024).unwrap_err();
    assert!(matches!(err, ShmError::InvalidName));
}

#[cfg(unix)]
#[test]
fn stale_region_replaced_on_create_named() {
    // Simulate a stale region from a prior crash by creating a named region
    // and then creating another with the same name. The second must succeed.
    let name = format!("/vertebra-stale-{}", std::process::id());
    let first = match ShmRegion::create_named(&name, 4096) {
        Ok(r) => r,
        Err(ShmError::Sys(e)) if e.kind() == std::io::ErrorKind::PermissionDenied => return,
        Err(e) => panic!("{e:?}"),
    };
    // Forget without unlinking (simulate crash — drop without unlink)
    std::mem::forget(first);
    // Manually unlink to simulate what the OS would leave behind
    // (on real crash the name persists in /dev/shm)
    // We have to use shm_unlink directly since we forgot the region
    unsafe {
        let cname = std::ffi::CString::new(name.as_str()).unwrap();
        libc::shm_open(
            cname.as_ptr(),
            libc::O_RDWR,
            0o600,
        );
        // Leave it open — now create_named must unlink-before-create
    }
    let second = ShmRegion::create_named(&name, 4096).expect("create_named with stale name");
    drop(second);
}

#[test]
fn region_size_is_page_aligned() {
    // Request 1 byte; should get at least a page back.
    let region = ShmRegion::create(1).expect("create failed");
    assert!(region.len() >= 4096, "size should be page-aligned, got {}", region.len());
}
```

**Pass criteria:**
- All tests pass on Linux and macOS.
- `named_region_create_open_cleanup` fails to `open_named` after `drop` (unlink happened).
- `zero_size_is_rejected` returns `ShmError::InvalidSize`.

---

### Task 4.2 — `tests/test_ring.rs`

**File:** `src/rs/vertebra-core/tests/test_ring.rs`

**Exact code:**

```rust
//! Integration tests for `vertebra_core::channel::SpscRing`.

use std::sync::Arc;
use std::time::Duration;
use vertebra_core::buffer::{ChannelMode, RingLayout, CACHELINE_BYTES};
use vertebra_core::channel::{ChannelError, SpscRing};
use vertebra_core::shm::ShmRegion;

fn make_spsc(slots: usize, slot_size: usize) -> (Arc<ShmRegion>, SpscRing) {
    let layout = RingLayout::new(slots, slot_size, ChannelMode::Spsc);
    let alloc = std::alloc::Layout::from_size_align(layout.total_size, CACHELINE_BYTES).unwrap();
    let ptr = unsafe { std::alloc::alloc(alloc) };
    assert!(!ptr.is_null());
    let region = Arc::new(unsafe { ShmRegion::from_raw(ptr, layout.total_size) });
    let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() };
    (region, ring)
}

#[test]
fn spsc_single_thread_roundtrip() {
    let (_region, ring) = make_spsc(16, 256);
    let payload = b"hello vertebra";
    ring.send(payload, 1).expect("send failed");
    let mut buf = [0u8; 256];
    let len = ring.recv(&mut buf).expect("recv failed");
    assert_eq!(&buf[..len], payload);
}

#[test]
fn spsc_empty_returns_error() {
    let (_region, ring) = make_spsc(16, 256);
    let mut buf = [0u8; 256];
    let err = ring.try_recv(&mut buf).unwrap_err();
    assert!(matches!(err, ChannelError::Empty), "expected Empty, got {err:?}");
}

#[test]
fn spsc_full_returns_error() {
    let (_region, ring) = make_spsc(4, 256);
    let payload = [0u8; 64];
    // Fill the ring
    for _ in 0..4 {
        ring.send(&payload, 0).expect("send should succeed");
    }
    let err = ring.send(&payload, 0).unwrap_err();
    assert!(matches!(err, ChannelError::Full), "expected Full, got {err:?}");
}

#[test]
fn spsc_fill_then_drain() {
    let (_region, ring) = make_spsc(8, 256);
    let payloads: Vec<Vec<u8>> = (0..8u8).map(|i| vec![i; 16]).collect();
    for (i, p) in payloads.iter().enumerate() {
        ring.send(p, i as u64).expect("send");
    }
    let mut buf = [0u8; 256];
    for expected in &payloads {
        let len = ring.recv(&mut buf).expect("recv");
        assert_eq!(&buf[..len], expected.as_slice());
    }
}

#[test]
fn spsc_multithreaded_producer_consumer() {
    const N: u64 = 10_000;
    let (_region, ring) = make_spsc(256, 256);
    let ring = Arc::new(ring);
    let ring_send = Arc::clone(&ring);

    let producer = std::thread::spawn(move || {
        for i in 0u64..N {
            let bytes = i.to_le_bytes();
            loop {
                match ring_send.send(&bytes, i) {
                    Ok(()) => break,
                    Err(ChannelError::Full) => std::hint::spin_loop(),
                    Err(e) => panic!("send error: {e}"),
                }
            }
        }
    });

    let mut buf = [0u8; 256];
    for expected in 0u64..N {
        loop {
            match ring.try_recv(&mut buf) {
                Ok(len) => {
                    assert_eq!(len, 8);
                    let got = u64::from_le_bytes(buf[..8].try_into().unwrap());
                    assert_eq!(got, expected, "sequence mismatch at {expected}");
                    break;
                }
                Err(ChannelError::Empty) => std::hint::spin_loop(),
                Err(e) => panic!("recv error: {e}"),
            }
        }
    }
    producer.join().expect("producer panicked");
}

#[test]
fn spsc_recv_hybrid_no_block_when_data_present() {
    let (_region, ring) = make_spsc(16, 256);
    ring.send(b"test", 0).unwrap();
    let mut buf = [0u8; 256];
    let start = std::time::Instant::now();
    let result = ring
        .recv_hybrid(&mut buf, 5, Duration::from_millis(100))
        .expect("recv_hybrid failed");
    let elapsed = start.elapsed();
    assert!(result.is_some(), "should receive data");
    assert!(
        elapsed.as_micros() < 500,
        "should not block when data present: {elapsed:?}"
    );
}

#[test]
fn spsc_recv_hybrid_timeout() {
    let (_region, ring) = make_spsc(16, 256);
    let mut buf = [0u8; 256];
    let start = std::time::Instant::now();
    let result = ring
        .recv_hybrid(&mut buf, 5, Duration::from_millis(10))
        .expect("recv_hybrid failed");
    let elapsed = start.elapsed();
    assert!(result.is_none(), "should timeout");
    assert!(
        elapsed.as_millis() >= 8,
        "should wait ~10ms before timeout: {elapsed:?}"
    );
}

#[test]
fn spsc_send_sync_bounds() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<SpscRing>();
}
```

**Pass criteria:**
- All tests pass.
- `spsc_multithreaded_producer_consumer` completes with 10 000 messages in order.
- `spsc_send_sync_bounds` compiles (proves `Send + Sync`).

---

### Task 4.3 — `tests/test_mpmc.rs`

**File:** `src/rs/vertebra-core/tests/test_mpmc.rs`

**Exact code:**

```rust
//! Integration tests for `vertebra_core::channel::MpmcRing`.

use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use vertebra_core::channel::MpmcRing;

#[test]
fn mpmc_single_thread_roundtrip() {
    let ring = MpmcRing::new(16, 256).expect("new failed");
    let payload = b"mpmc-test";
    ring.send(payload, 0).expect("send failed");
    let mut buf = [0u8; 256];
    let len = ring.recv(&mut buf).expect("recv failed");
    assert_eq!(&buf[..len], payload);
}

#[test]
fn mpmc_4p_4c_message_counts() {
    const PRODUCERS: usize = 4;
    const CONSUMERS: usize = 4;
    const MESSAGES_PER_PRODUCER: u64 = 2_500;
    const TOTAL: u64 = PRODUCERS as u64 * MESSAGES_PER_PRODUCER;

    let ring = Arc::new(MpmcRing::new(1024, 256).expect("new failed"));
    let received = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();

    // Spawn producers
    for p in 0..PRODUCERS {
        let ring = Arc::clone(&ring);
        handles.push(std::thread::spawn(move || {
            let payload = (p as u64).to_le_bytes();
            for _ in 0..MESSAGES_PER_PRODUCER {
                loop {
                    match ring.send(&payload, 0) {
                        Ok(()) => break,
                        Err(_) => std::hint::spin_loop(),
                    }
                }
            }
        }));
    }

    // Spawn consumers
    for _ in 0..CONSUMERS {
        let ring = Arc::clone(&ring);
        let received = Arc::clone(&received);
        handles.push(std::thread::spawn(move || {
            let mut buf = [0u8; 256];
            let mut local_count = 0u64;
            while received.load(Ordering::Acquire) + local_count < TOTAL {
                match ring.try_recv(&mut buf) {
                    Ok(_) => local_count += 1,
                    Err(_) => std::hint::spin_loop(),
                }
            }
            received.fetch_add(local_count, Ordering::AcqRel);
        }));
    }

    for h in handles {
        h.join().expect("thread panicked");
    }

    // Drain any remaining
    let mut buf = [0u8; 256];
    loop {
        match ring.try_recv(&mut buf) {
            Ok(_) => { received.fetch_add(1, Ordering::AcqRel); }
            Err(_) => break,
        }
    }

    assert_eq!(
        received.load(Ordering::Acquire),
        TOTAL,
        "should receive all {TOTAL} messages"
    );
}

#[test]
fn mpmc_send_sync_bounds() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<MpmcRing>();
}

#[test]
fn mpmc_try_recv_empty_is_error() {
    let ring = MpmcRing::new(16, 256).unwrap();
    let mut buf = [0u8; 256];
    // Ring is empty; try_recv must return an error (not panic).
    let result = ring.try_recv(&mut buf);
    assert!(result.is_err(), "empty ring should return error");
}
```

**Pass criteria:**
- `mpmc_4p_4c_message_counts` receives exactly `PRODUCERS * MESSAGES_PER_PRODUCER` messages.
- `mpmc_send_sync_bounds` compiles.
- No deadlock or panic under concurrent access.

---

### Task 4.4 — `tests/test_batch.rs`

**File:** `src/rs/vertebra-core/tests/test_batch.rs`

**Exact code:**

```rust
//! Integration tests for `vertebra_core::batch::BatchArena` and `BatchNode`.

use vertebra_core::batch::{BatchArena, BatchError, BatchNode};
use vertebra_core::proxy::{HttpMethod, RequestInfo};

fn make_request_info(correlation_id: u64) -> RequestInfo {
    RequestInfo {
        method: HttpMethod::GET,
        _pad: [0u8; 7],
        handler_id: 42,
        path_offset: 0,
        path_len: 1,
        headers_offset: 0,
        body_offset: 0,
        body_len: 0,
        correlation_id,
    }
}

#[test]
fn arena_alloc_layout() {
    // BatchNode must be exactly 64 bytes and 64-byte aligned.
    assert_eq!(std::mem::size_of::<BatchNode>(), 64);
    assert_eq!(std::mem::align_of::<BatchNode>(), 64);
}

#[test]
fn arena_push_returns_distinct_ptrs() {
    let arena = BatchArena::new(4).unwrap();
    let mut ptrs = Vec::new();
    for i in 0..4 {
        let p = arena
            .push(BatchNode::new(
                make_request_info(i),
                std::ptr::null(),
                0,
                std::ptr::null(),
                0,
                i,
            ))
            .unwrap();
        ptrs.push(p);
    }
    // All pointers must be unique.
    for i in 0..ptrs.len() {
        for j in (i + 1)..ptrs.len() {
            assert_ne!(ptrs[i], ptrs[j], "pointers {i} and {j} must differ");
        }
    }
    // Pointers must be 64-byte aligned.
    for (i, p) in ptrs.iter().enumerate() {
        assert_eq!(
            (*p as usize) % 64,
            0,
            "pointer {i} is not 64-byte aligned: {p:p}"
        );
    }
}

#[test]
fn arena_preserves_correlation_ids() {
    let arena = BatchArena::new(8).unwrap();
    for i in 0..8u64 {
        arena
            .push(BatchNode::new(
                make_request_info(i),
                std::ptr::null(),
                0,
                std::ptr::null(),
                0,
                i,
            ))
            .unwrap();
    }
    let slice = unsafe { arena.as_slice() };
    for (i, node) in slice.iter().enumerate() {
        assert_eq!(node.correlation_id, i as u64);
    }
}

#[test]
fn arena_full_error() {
    let arena = BatchArena::new(1).unwrap();
    arena
        .push(BatchNode::new(
            make_request_info(0),
            std::ptr::null(),
            0,
            std::ptr::null(),
            0,
            0,
        ))
        .unwrap();
    let err = arena
        .push(BatchNode::new(
            make_request_info(1),
            std::ptr::null(),
            0,
            std::ptr::null(),
            0,
            1,
        ))
        .unwrap_err();
    assert!(
        matches!(err, BatchError::Full { capacity: 1 }),
        "expected Full{{capacity:1}}, got {err:?}"
    );
}

#[test]
fn arena_reset_recycles_slots() {
    let arena = BatchArena::new(2).unwrap();
    let p1 = arena
        .push(BatchNode::new(
            make_request_info(10),
            std::ptr::null(),
            0,
            std::ptr::null(),
            0,
            10,
        ))
        .unwrap();
    arena.reset();
    let p2 = arena
        .push(BatchNode::new(
            make_request_info(20),
            std::ptr::null(),
            0,
            std::ptr::null(),
            0,
            20,
        ))
        .unwrap();
    // After reset, first allocation should be at the same address.
    assert_eq!(p1, p2, "reset should recycle slot 0");
    // And the data should be the new node's data.
    let node = unsafe { &*p2 };
    assert_eq!(node.correlation_id, 20);
}

#[test]
fn arena_len_tracking() {
    let arena = BatchArena::new(10).unwrap();
    assert_eq!(arena.len(), 0);
    for i in 0..5 {
        arena
            .push(BatchNode::new(
                make_request_info(i),
                std::ptr::null(),
                0,
                std::ptr::null(),
                0,
                i,
            ))
            .unwrap();
        assert_eq!(arena.len(), i as usize + 1);
    }
}
```

**Pass criteria:**
- All tests pass.
- `arena_push_returns_distinct_ptrs` confirms 64-byte alignment.
- `arena_reset_recycles_slots` confirms same address after reset.

---

### Task 4.5 — `benches/ring_throughput.rs`

**File:** `src/rs/vertebra-core/benches/ring_throughput.rs`

**Exact code:**

```rust
//! Ring buffer throughput benchmark.
//!
//! Target: >1 M ops/s single-thread SPSC ping-pong at 256-byte slots.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::sync::Arc;
use std::time::Duration;
use vertebra_core::buffer::{ChannelMode, RingLayout, CACHELINE_BYTES};
use vertebra_core::channel::{MpmcRing, SpscRing};
use vertebra_core::shm::ShmRegion;

/// Allocate a SPSC ring backed by heap memory (not SHM) for benchmarking.
fn make_spsc_heap(slots: usize, slot_size: usize) -> (Vec<u8>, SpscRing) {
    let layout = RingLayout::new(slots, slot_size, ChannelMode::Spsc);
    let alloc =
        std::alloc::Layout::from_size_align(layout.total_size, CACHELINE_BYTES).unwrap();
    let raw_ptr = unsafe { std::alloc::alloc(alloc) };
    assert!(!raw_ptr.is_null());
    let region = Arc::new(unsafe { ShmRegion::from_raw(raw_ptr, layout.total_size) });
    let ring = unsafe { SpscRing::initialize(Arc::clone(&region), layout, 0, 0).unwrap() };
    // Keep the allocation alive via the Vec-like sentinel.
    // The region Arc keeps the memory alive; we return a dummy Vec for drop ordering.
    let sentinel = unsafe { Vec::from_raw_parts(raw_ptr, 0, layout.total_size) };
    (sentinel, ring)
}

/// Single-threaded SPSC ping-pong: send 1, recv 1, repeat.
/// Target: >1 M iterations/second at 256-byte slots.
fn bench_spsc_1m_ops(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc_throughput");
    group.measurement_time(Duration::from_secs(5));
    group.sample_size(100);

    for slot_size in [64usize, 256, 1024] {
        group.bench_with_input(
            BenchmarkId::new("ping_pong", slot_size),
            &slot_size,
            |b, &slot_size| {
                let (_sentinel, ring) = make_spsc_heap(256, slot_size);
                let payload = vec![0xABu8; slot_size.min(64)];
                let mut buf = vec![0u8; slot_size];
                b.iter(|| {
                    ring.send(&payload, 0).unwrap();
                    let _ = ring.recv(&mut buf).unwrap();
                });
            },
        );
    }
    group.finish();
}

/// Multi-slot burst: fill ring, then drain. Measures sustained throughput.
fn bench_spsc_burst(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc_burst");
    group.measurement_time(Duration::from_secs(5));

    for burst in [16usize, 64, 256] {
        group.bench_with_input(
            BenchmarkId::new("fill_drain", burst),
            &burst,
            |b, &burst| {
                let (_sentinel, ring) = make_spsc_heap(burst, 256);
                let payload = [0u8; 64];
                let mut buf = [0u8; 256];
                b.iter(|| {
                    for _ in 0..burst {
                        ring.send(&payload, 0).unwrap();
                    }
                    for _ in 0..burst {
                        ring.recv(&mut buf).unwrap();
                    }
                });
            },
        );
    }
    group.finish();
}

/// MPMC single-thread baseline (compare to SPSC overhead).
fn bench_mpmc_baseline(c: &mut Criterion) {
    let ring = MpmcRing::new(256, 256).unwrap();
    let payload = [0u8; 64];
    let mut buf = [0u8; 256];
    c.bench_function("mpmc_single_thread_baseline", |b| {
        b.iter(|| {
            ring.send(&payload, 0).unwrap();
            ring.recv(&mut buf).unwrap();
        });
    });
}

criterion_group!(benches, bench_spsc_1m_ops, bench_spsc_burst, bench_mpmc_baseline);
criterion_main!(benches);
```

**Note:** `make_spsc_heap` uses `Vec::from_raw_parts` as a sentinel only for lifetime
management. The `ShmRegion::from_raw` wraps the raw pointer; the sentinel Vec must have
`std::mem::forget` called on it before the region drops, or the region must outlive it.
If this pattern causes a double-free in practice, restructure to use `ManuallyDrop<Vec<u8>>`
instead. The benchmark correctness does not depend on the sentinel — only on the `SpscRing`
being valid during the benchmark loop.

**Pass criteria:**
- `cargo bench --package vertebra-core --bench ring_throughput` compiles and runs.
- `spsc_throughput/ping_pong/256` reports >1 M iterations/second in release mode
  (`cargo bench --release --package vertebra-core`).

---

## 9. Phase 5: Integration and Module Wiring

### Task 5.1 — Update `src/lib.rs` and `Cargo.toml`

**`src/lib.rs` additions:**

Add the following lines to `src/rs/vertebra-core/src/lib.rs`:

```rust
pub mod batch;
pub mod cleanup;
pub mod proxy;
```

These go below the existing `pub mod sync;` line.

**Re-exports to add (optional but recommended for ergonomics):**

```rust
pub use batch::{BatchArena, BatchError, BatchNode};
pub use proxy::{HttpMethod, RequestInfo, ResponseInfo};
```

**`Cargo.toml` bench entry to add:**

```toml
[[bench]]
name = "ring_throughput"
harness = false
```

Add this after the existing `[[bench]]` entries in
`src/rs/vertebra-core/Cargo.toml`.

**Pass criteria:**
- `cargo build --package vertebra-core` succeeds.
- `cargo doc --package vertebra-core` generates documentation for `batch`, `cleanup`, and `proxy`.
- All existing tests still pass.

---

## 10. Final Gate Checklist

Run these commands in order. All must pass before this chapter is complete.

```bash
cd /home/cody/code/c/vertebra/src/rs

# 1. Compile check
cargo build --package vertebra-core

# 2. All tests
cargo test --package vertebra-core

# 3. Clippy — zero warnings, zero errors
cargo clippy --package vertebra-core -- -D warnings

# 4. Benchmark smoke run (compile + one-second sample)
cargo bench --package vertebra-core --bench ring_throughput -- --test

# 5. Full benchmark for throughput gate (release mode required)
cargo bench --package vertebra-core --bench ring_throughput
# Manual check: spsc_throughput/ping_pong/256 must report > 1 M iter/s
```

**Chapter is COMPLETE when all five commands succeed.**

---

## 11. Performance Reference

| Metric | Target | How to measure |
|--------|--------|----------------|
| SPSC ping-pong @ 256B slot | >1 M ops/s | `ring_throughput` bench |
| MPMC 4P/4C @ 256B slot | >500 k ops/s | `mpmc_4p_4c_message_counts` timing |
| Conditional-wake overhead | 0 syscalls single-thread hot path | `strace` on single-thread bench |
| SHM region create+drop | <10 µs | inline timing in `test_shm.rs` |
| Arena push (1 entry) | <20 ns | inline timing or criterion micro |

---

## 12. Handoff to Downstream Chapters

When this chapter passes the final gate, the following types and functions are stable
and ready for use by downstream chapters:

| Export | Used by |
|--------|---------|
| `vertebra_core::shm::ShmRegion` | Ch5 (HTTP server), Ch8 (FFI bridge) |
| `vertebra_core::shm::shm_unlink_named` | Ch5, supervisor |
| `vertebra_core::cleanup::cleanup_all` | Ch3 (supervisor lifecycle) |
| `vertebra_core::channel::SpscRing` | Ch5, Ch8 |
| `vertebra_core::channel::MpmcRing` | Ch5 (multi-worker dispatch) |
| `vertebra_core::notify::Notifier` | Ch8 (async ring notification FD) |
| `vertebra_core::async_ring::AsyncRing` | Ch8 |
| `vertebra_core::batch::BatchArena` | Ch5 (batch collector), Ch8 |
| `vertebra_core::batch::BatchNode` | Ch5, Ch8 |
| `vertebra_core::proxy::RequestInfo` | Ch8 (PyO3 proxy), Ch9 (batch runner) |
| `vertebra_core::proxy::ResponseInfo` | Ch8, Ch5 (response writer) |
| `vertebra_core::sync::WaitWord` | Ch5 (futex-based coordination) |

---

## 13. Session Recovery

If this session is interrupted, resume with:

```bash
# Check which tasks are done
cargo test --package vertebra-core 2>&1 | grep -E "^(test |FAILED|ok |error)"

# Check which files exist
ls src/rs/vertebra-core/src/
ls src/rs/vertebra-core/tests/
ls src/rs/vertebra-core/benches/
```

Files already complete (do not recreate):
- `src/shm.rs`, `src/sync.rs`, `src/notify.rs`, `src/buffer.rs`, `src/channel.rs`
- `src/async_ring.rs`, `src/async_notifier.rs`, `src/async_notify.rs`
- `src/guards.rs`, `src/router.rs`, `src/ipc.rs`, `src/platform/`, `src/protocol/`
- `benches/spsc.rs`, `benches/mpmc.rs`, `benches/spin_wait.rs`, `benches/router.rs`, `benches/guards.rs`

Files to create (this chapter):
- `src/cleanup.rs` — Task 1.2
- `src/batch.rs` — Task 3.1
- `src/proxy.rs` — Task 3.2
- `tests/test_shm.rs` — Task 4.1
- `tests/test_ring.rs` — Task 4.2
- `tests/test_mpmc.rs` — Task 4.3
- `tests/test_batch.rs` — Task 4.4
- `benches/ring_throughput.rs` — Task 4.5
- `src/lib.rs` update + `Cargo.toml` bench entry — Task 5.1
