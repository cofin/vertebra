---
type: Guide
title: "Lock-Free Ring Buffers & Shared Memory"
description: "ShmRegion allocation, the ShmHeader layout, SpscRing and MpmcRing slot protocols, blocking waits, and platform wake primitives in vertebra-core."
status: stable
tags: [architecture, ipc, shared-memory, ring-buffer, rust]
sources:
  - resource: src/rs/vertebra-core/src/shm.rs
  - resource: src/rs/vertebra-core/src/buffer.rs
  - resource: src/rs/vertebra-core/src/channel.rs
  - resource: src/rs/vertebra-core/src/notify.rs
  - resource: src/rs/vertebra-core/src/sync.rs
  - resource: src/rs/vertebra-core/src/platform/linux.rs
  - resource: src/rs/vertebra-core/src/platform/macos.rs
  - resource: src/rs/vertebra-core/src/platform/windows.rs
---

# Lock-Free Ring Buffers & Shared Memory

[`vertebra-core`](../../../../../src/rs/vertebra-core/src/lib.rs) maps fixed-slot ring buffers into shared memory. Each ring region is one mapping: a `ShmHeader`, an optional sequence array (MPMC only), then the slot area.

## Region Allocation (`shm.rs`)

`ShmRegion::create(size)` ([`shm.rs`](../../../../../src/rs/vertebra-core/src/shm.rs)) page-aligns the size and tries, in order:

1. **Linux**: `memfd_create` with `MFD_CLOEXEC | MFD_ALLOW_SEALING`, then `F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_SEAL`.
2. **Unix fallback** (macOS, or Linux when memfd fails): `shm_open` with a generated name `/vtb-{pid:x}-{seq:x}-{attempt:x}` from a process-wide `AtomicU64` counter.
3. **Windows**: a pagefile-backed section.

`ShmRegion::map(handle, size)` attaches to an inherited FD. `create_named` / `open_named` exist for explicit POSIX names; `create_named` unlinks a stale name first.

## `ShmHeader` Layout (`buffer.rs`)

[`ShmHeader`](../../../../../src/rs/vertebra-core/src/buffer.rs) is `#[repr(C)]`. Hot atomics are wrapped in `CacheLine<T>`, which is `repr(align(64))`, or `repr(align(128))` on aarch64 macOS (`CACHELINE_BYTES`).

| Group | Fields |
|---|---|
| Metadata | `magic: u64` (`MAGIC = u64::from_le_bytes(*b"VERTEBRA")`), `abi_version: u32` (`ABI_VERSION = 2`), `flags`, `total_size`, `ring_offset`, `ring_size`, `generation: AtomicU32`, `state: AtomicU32` (`RegionState::{Uninitialized, Ready, Stale}`), `creator_pid`, `creator_epoch: u64`, reserved bytes |
| Cursors | `read_head: CacheLine<AtomicU64>`, `write_head: CacheLine<AtomicU64>` |
| Wait words | `wait_read`, `wait_write`, `send_waiters`, `recv_waiters` (each `CacheLine<AtomicU32>`) |

`RingLayout::new(slot_size, slots, ChannelMode)` computes `seq_offset()` (header end), `ring_offset()` (after the MPMC sequence array), and `total_size`. `initialize` writes the header and sets `state = Ready`; `attach` rejects a region whose `magic`, `abi_version`, or `state` do not match. Each slot starts with an 8-byte `SlotHeader { len: u32, flags: u32 }`, so `payload_capacity()` is `slot_size - 8`.

## `SpscRing` & `SlotView` (`channel.rs`)

[`SpscRing`](../../../../../src/rs/vertebra-core/src/channel.rs) uses masked cursors (`index & mask`), so one slot always stays empty and usable capacity is `slots - 1`.

- **Send** (`try_send`, `try_send_with(len, flags, writer)`): rejects `len > payload_capacity()` with `PayloadTooLarge`; loads `write_head` (`Relaxed`) and `read_head` (`Acquire`); returns `Full` when `(write_head + 1) & mask == read_head`. `try_send_with` lets the closure write directly into the slot. It then stores `write_head` (`Release`), bumps `wait_write` (`SeqCst`), and wakes a receiver only when `recv_waiters > 0`.
- **Zero-copy receive** (`try_recv_view`, `recv_view`, `recv_view_timeout`): returns a `SlotView` that holds an `Arc<SpscRing>`, the slot index, and a raw pointer/length into the slot (clamped against `payload_capacity()`). Only one view may exist per ring; a second call returns `SlotBorrowed` until the first is released. `SlotView::release()` (also run on `Drop`) stores the next index into `read_head` (`Release`), bumps `wait_read` (`SeqCst`), wakes a sender if `send_waiters > 0`, and clears the borrow flag.
- **Copying receive** (`try_recv`, `recv`, `recv_timeout`, `try_recv_batch`): copies into a caller buffer and returns `BufferTooSmall` if it does not fit.
- `split()` yields `SpscSender` / `SpscReceiver` handles.

## `MpmcRing` (`channel.rs`)

[`MpmcRing`](../../../../../src/rs/vertebra-core/src/channel.rs) is Vyukov's bounded queue. A separate `AtomicU64` sequence array (seeded with `i`) precedes the slot area. `head()` is `read_head` and `tail()` is `write_head`.

- **Producers** (`try_send`, `try_send_with(flags, f)`): CAS `tail` when `seq - tail == 0`, write the slot, then store `seq = tail + 1` (`Release`). `has_capacity()` checks `(seq as i64 - tail as i64) >= 0`.
- **Consumers** (`try_recv`, `try_recv_with_flags`, `try_recv_with(reader)`, `recv_timeout_with`): CAS `head` when `seq - (head + 1) == 0`, read the slot, then store `seq = head + slots` (`Release`). `try_recv_with` passes the slot payload slice to the closure without copying.

## Blocking Waits (`sync.rs`, `platform/`)

`SpscRing` blocking paths (`send`, `send_with`, `send_timeout`, `recv`, `recv_timeout`, `recv_view`) increment `send_waiters` / `recv_waiters`, re-check the cursor, and sleep on the matching wait word through [`sync.rs`](../../../../../src/rs/vertebra-core/src/sync.rs): `wait_on_address`, `wait_on_address_timeout`, `wake_one`, `wake_all`. `MpmcRing` blocking paths (`send`, `recv`, `recv_timeout`, `recv_timeout_with`) spin with `crossbeam_utils::Backoff::snooze` instead and never touch the wait words.

| Platform | Implementation |
|---|---|
| Linux ([`linux.rs`](../../../../../src/rs/vertebra-core/src/platform/linux.rs)) | `SYS_futex` `FUTEX_WAIT` / `FUTEX_WAKE`; also `pidfd_open(pid)` (flags `0`) |
| macOS ([`macos.rs`](../../../../../src/rs/vertebra-core/src/platform/macos.rs)) | default: 1024 spin iterations, then 1 ms `nanosleep`; with the `macos-ulock` feature, `__ulock_wait` / `__ulock_wake` using `UL_COMPARE_AND_WAIT | ULF_NO_ERRNO` |
| Windows ([`windows.rs`](../../../../../src/rs/vertebra-core/src/platform/windows.rs)) | `WaitOnAddress`, `WakeByAddressSingle`, `WakeByAddressAll` |

`sync.rs` also defines `WaitWord`, `SharedMutex<T>`, `SharedRwLock<T>`, and `SharedCondvar` for cross-process locking. No production code uses them.

## Cross-Process Wakeup FDs (`notify.rs`)

[`Notifier`](../../../../../src/rs/vertebra-core/src/notify.rs) is the FD that asyncio watches with `add_reader`. On Linux it is `eventfd(0, EFD_NONBLOCK)`; elsewhere a non-blocking `pipe(2)`. Neither sets `CLOEXEC`, so children inherit them. API: `new`, `notify`, `clear`, `as_raw_fd`, `from_raw_fd` (Linux), `from_raw_fds(read, write)` and `write_fd` (non-Linux).

Batch-level LZ4/Zstd compression is applied by `BatchBridge`, not by the rings; see [Tiered Rings & Batch Bridge](tiered-rings-and-batch-bridge.md).
