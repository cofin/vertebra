---
name: ipc-ring-buffers-and-shm
description: ShmHeader cache-line layout, SpscRing zero-copy SlotView, Vyukov MpmcRing, LZ4/Zstd compression, and Linux/macOS wake primitives.
sources:
  - src/rs/vertebra-core/src/shm.rs
  - src/rs/vertebra-core/src/channel.rs
  - src/rs/vertebra-core/src/ipc.rs
  - src/rs/vertebra-core/src/protocol/codec.rs
  - src/rs/vertebra-core/src/platform/linux.rs
  - src/rs/vertebra-core/src/platform/macos.rs
---

# Lock-Free Ring Buffers & Shared Memory

Vertebra's cross-process IPC transport in [`vertebra-core`](../../../../../src/rs/vertebra-core/src/lib.rs) maps lock-free ring buffers directly into anonymous or POSIX shared memory with cache-line-isolated atomics and zero-copy slot views.

## 1. `ShmHeader` Cache-Line Layout (`shm.rs`)

Every [`ShmRegion`](../../../../../src/rs/vertebra-core/src/shm.rs) starts with a `#[repr(C, align(64))]` [`ShmHeader`](../../../../../src/rs/vertebra-core/src/shm.rs) partitioned across 64-byte cache lines to eliminate false sharing:

| Cache Line | Fields | Access Pattern |
|---|---|---|
| Metadata Line | `magic` (`0x5645_5254_4542_5241`), `version` (`1`), `capacity`, `slot_size`, `epoch: AtomicU64`, `creator_pid: u32`, `compression_codec: u8` | Read-only metadata after init (`epoch` incremented on crash recovery) |
| Producer Line | `write_idx: AtomicU64` + cache-line padding | Producer-owned monotonic write cursor |
| Consumer Line | `read_idx: AtomicU64` + cache-line padding | Consumer-owned monotonic read cursor |
| Waiter Line | `send_waiters: AtomicU32`, `recv_waiters: AtomicU32`, `wait_write_ptr: AtomicU32`, `wait_read_ptr: AtomicU32` | Futex / ulock sleeper-waker coordination |

## 2. Single-Producer Single-Consumer Ring (`SpscRing` & `SlotView`)

[`SpscRing`](../../../../../src/rs/vertebra-core/src/channel.rs) implements a bounded power-of-two ring over `ShmRegion`. Each slot reserves an 8-byte header (`len: u32`, `flags: u32`) followed by `slot_size - 8` payload bytes:

- **Producer (`try_send` / `try_send_with`)**: Reads `write_idx` (`Relaxed`) and `read_idx` (`Acquire`). If `write_idx - read_idx >= capacity`, returns `RingFull`. Invokes the zero-copy writer closure `try_send_with` directly into the slot buffer, publishes `write_idx + 1` with `Ordering::Release`, increments `wait_write_ptr` (`Ordering::SeqCst`), and wakes sleeping consumers only when `recv_waiters.load(Ordering::SeqCst) > 0`.
- **Zero-Copy Consumer (`try_recv_view` -> `SlotView`)**: Returns a [`SlotView<'a>`](../../../../../src/rs/vertebra-core/src/channel.rs) borrowing the raw SHM slice without copying or advancing `read_idx`. Calling [`SlotView::release()`](../../../../../src/rs/vertebra-core/src/channel.rs) stores `read_idx + 1` (`Ordering::Release`), unconditionally increments `wait_read_ptr` (`Ordering::SeqCst`), and wakes blocked producers if `send_waiters > 0`.

## 3. Vyukov Bounded MPMC Ring (`MpmcRing` in `ipc.rs`)

[`MpmcRing`](../../../../../src/rs/vertebra-core/src/ipc.rs) implements Vyukov's bounded MPMC queue over `ShmRegion` for multi-worker request dispatch:

- Each slot starts with `sequence: AtomicU64` (initialized to slot index `i`), `data_len: u32`, and `flags: u32`.
- Producers CAS `write_idx` (`head`) when `(slot.sequence as i64) - (head as i64) == 0` (`has_capacity` checks `>= 0`) and publish `head + 1` to `slot.sequence` (`Ordering::Release`).
- Consumers CAS `read_idx` (`tail`) when `(slot.sequence as i64) - ((tail + 1) as i64) == 0` and release the slot via `try_recv_with` by storing `tail + capacity` into `slot.sequence` (`Ordering::Release`).

## 4. Payload Compression & Platform Primitives

- **Per-Slot Compression ([`protocol/codec.rs`](../../../../../src/rs/vertebra-core/src/protocol/codec.rs))**: [`CompressionCodec`](../../../../../src/rs/vertebra-core/src/protocol/codec.rs) (`Lz4` or `Zstd`) compresses payloads when compressed output is strictly smaller than raw input.
- **Linux ([`platform/linux.rs`](../../../../../src/rs/vertebra-core/src/platform/linux.rs))**: `memfd_create(MFD_CLOEXEC)`, `eventfd(EFD_NONBLOCK | EFD_CLOEXEC)`, and `futex(FUTEX_WAIT / FUTEX_WAKE)`.
- **macOS ([`platform/macos.rs`](../../../../../src/rs/vertebra-core/src/platform/macos.rs))**: `shm_open` with $\le 31$-byte names (`/vtb-{pid:x}-{seq:x}-{attempt:x}`), non-blocking `pipe(2)` pairs, and `__ulock_wait` / `__ulock_wake` (`UL_COMPARE_AND_WAIT_SHARED | ULF_NO_ERRNO`).
