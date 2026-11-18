---
type: Guide
title: "Lock-Free Ring Buffers & Shared Memory"
description: "ShmRegion allocation, the ShmHeader layout, SpscRing and MpmcRing slot protocols, blocking waits, and platform wake primitives in vertebra-core."
status: stable
tags:
  - architecture
  - ipc
  - shared-memory
  - ring-buffer
  - rust
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: 067fa901
  - by: agent:flow-setup
    at: "2026-11-04T04:33:00Z"
    commit: f741701
sources:
  - id: src-vertebra-core-src-shm-rs
    resource: src/rs/vertebra-core/src/shm.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/shm.rs)"
  - id: src-vertebra-core-src-buffer-rs
    resource: src/rs/vertebra-core/src/buffer.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/buffer.rs)"
  - id: src-vertebra-core-src-channel-rs
    resource: src/rs/vertebra-core/src/channel.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/channel.rs)"
  - id: src-vertebra-core-src-notify-rs
    resource: src/rs/vertebra-core/src/notify.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/notify.rs)"
  - id: src-vertebra-core-src-sync-rs
    resource: src/rs/vertebra-core/src/sync.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/sync.rs)"
  - id: src-vertebra-core-src-platform-linux-rs
    resource: src/rs/vertebra-core/src/platform/linux.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/platform/linux.rs)"
  - id: src-vertebra-core-src-platform-macos-rs
    resource: src/rs/vertebra-core/src/platform/macos.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/platform/macos.rs)"
  - id: src-vertebra-core-src-platform-windows-rs
    resource: src/rs/vertebra-core/src/platform/windows.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/platform/windows.rs)"
  - id: src-vertebra-core-src-broker-rs
    resource: src/rs/vertebra-core/src/broker.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/broker.rs)"
---

# Lock-Free Ring Buffers & Shared Memory

<!-- truth: start -->
- `ShmRegion` maps page-aligned shared memory via sealed `memfd_create` on Linux, `shm_open` on POSIX fallbacks, and pagefile-backed sections on Windows, initialized with `ShmHeader` (`ABI_VERSION = 3`).
- `SpscRing` and `MpmcRing` exchange frames via cache-line-padded sequence cursors, zero-copy `SlotView` borrows, and Dekker `Ordering::SeqCst` sleep/wake handshakes over `futex`, `ulock`, or pipes.
<!-- truth: end -->

[`vertebra-core`](../../../../../src/rs/vertebra-core/src/lib.rs) maps fixed-slot ring buffers into shared memory. Each ring region is one mapping: a `ShmHeader`, an optional sequence array (MPMC only), then the slot area.

## Region Allocation (`shm.rs`)

`ShmRegion::create(size)` ([`shm.rs`](../../../../../src/rs/vertebra-core/src/shm.rs)[^src-vertebra-core-src-shm-rs]) page-aligns the size and tries, in order:

1. **Linux**: `memfd_create` with `MFD_CLOEXEC | MFD_ALLOW_SEALING`, then `F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_SEAL`.
2. **Unix fallback** (macOS, or Linux when memfd fails): `shm_open` with a generated name `/vtb-{pid:x}-{seq:x}-{attempt:x}` from a process-wide `AtomicU64` counter.
3. **Windows**: a pagefile-backed section.

`ShmRegion::map(handle, size)` attaches to an inherited FD. `create_named` / `open_named` exist for explicit POSIX names; `create_named` unlinks a stale name first.

## `ShmHeader` Layout (`buffer.rs`)

[`ShmHeader`](../../../../../src/rs/vertebra-core/src/buffer.rs)[^src-vertebra-core-src-buffer-rs] is `#[repr(C)]`. Hot atomics are wrapped in `CacheLine<T>`, which is `repr(align(64))`, or `repr(align(128))` on aarch64 macOS (`CACHELINE_BYTES`).

| Group | Fields |
|---|---|
| Metadata | `magic: u64` (`MAGIC = u64::from_le_bytes(*b"VERTEBRA")`), `abi_version: u32` (`ABI_VERSION = 3`), `flags`, `total_size`, `ring_offset`, `ring_size`, `generation: AtomicU32`, `state: AtomicU32` (`RegionState::{Uninitialized, Ready, Stale}`), `creator_pid`, `creator_epoch: u64`, reserved bytes |
| Cursors | `read_head: CacheLine<AtomicU64>`, `write_head: CacheLine<AtomicU64>` |
| Wait words | `wait_read`, `wait_write`, `send_waiters`, `recv_waiters` (each `CacheLine<AtomicU32>`) |
| Dekker & tier state | `consumer_sleeping: CacheLine<AtomicU32>`, `active_flags: CacheLine<AtomicU32>` |

`RingLayout::new(slot_size, slots, ChannelMode)` uses `checked_mul` and `checked_add` (`Option<Self>`) to compute `seq_offset()` (header end), `ring_offset()` (after the cache-line-padded MPMC sequence array), and `total_size`. `ShmHeader` enforces compile-time size (`1152` bytes on 128-byte-cacheline targets such as aarch64 macOS, `640` bytes elsewhere) and alignment (`align_of::<ShmHeader>() == align_of::<CacheLine<AtomicU64>>()`) assertions. `initialize` writes the header and sets `state = Ready`; `attach` rejects a region whose `magic`, `abi_version` (`3`), or `state` do not match. `ShmHeader` provides `prepare_sleep` (`consumer_sleeping.store(1, SeqCst)`), `cancel_sleep` (`store(0, Release)`), and `should_notify_and_clear` (`SeqCst` fence + `swap(0, AcqRel) == 1`) so producers skip `eventfd`/`pipe` `write()` syscalls whenever the consumer is actively polling, while `active_flags` records non-empty response tiers. Each slot starts with an 8-byte `SlotHeader { len: u32, flags: u32 }`, so `payload_capacity()` is `slot_size - 8`.

## `SpscRing` & `SlotView` (`channel.rs`)

[`SpscRing`](../../../../../src/rs/vertebra-core/src/channel.rs)[^src-vertebra-core-src-channel-rs] uses masked cursors (`index & mask`), so one slot always stays empty and usable capacity is `slots - 1`, and caches remote cursors (`cached_read_head` / `cached_write_head` as generation-synchronized `CacheLine<AtomicU64>` snapshots) so producers only reload `read_head` (`Acquire`) when `next == cached_read_head` and consumers reuse `cached_write_head`. `SpscRing::attach`, `SpscRing::from_region`, `MpmcRing::attach`, and `MpmcRing::from_region` run `validate_region_for_layout`, rejecting regions shorter than `layout.total_size`, overflowing layout arithmetic, or misaligned for `ShmHeader` or the MPMC sequence array with `ChannelError::InvalidLayout` or `ChannelError::CorruptedSegment(&'static str)`.

- **Send** (`try_send`, `try_send_with(len, flags, writer)`): rejects `len > payload_capacity()` with `PayloadTooLarge`; checks cached `read_head` before reloading `header.read_head` (`Acquire`); returns `Full` when `(write_head + 1) & mask == read_head`. `try_send_with` lets the closure write directly into the slot, stores `write_head` (`Release`), and only executes the `wait_write` `SeqCst` `fetch_add` + futex wake when `recv_waiters > 0`. Blocking `send_with` spins up to 32 `Backoff` steps before entering `park_send`.
- **Zero-copy receive** (`try_recv_view`, `recv_view`, `recv_view_timeout`, `drain_with`): `try_recv_view` checks `head == tail` **before** attempting `slot_borrowed.compare_exchange`, avoiding RMW cache-line dirties on empty tiers, validates `header.len <= payload_capacity()` (returning `ChannelError::CorruptedSlot`), and returns a `SlotView` holding `Arc<SpscRing>`, the slot index, and a bounds-clamped slice. `SlotView::release()` (also run on `Drop`) advances `read_head` (`Release`), clears the borrow flag, and wakes a parked sender when `send_waiters > 0`. `drain_with(max_items, f)` visits up to `max_items` slots zero-copy and advances `read_head` once per drained batch.
- **Copying receive** (`read_slot_to_vec`, `try_recv`, `recv`, `recv_timeout`, `try_recv_batch`): `read_slot_to_vec` returns `Result<(Vec<u8>, u32), ChannelError>` and rejects `header.len > payload_capacity()` with `ChannelError::CorruptedSlot` without advancing `read_head` (`try_recv_batch` propagates the error); `try_recv` copies into a caller buffer and returns `BufferTooSmall` if it does not fit or `CorruptedSlot` on invalid lengths.
- `split()` yields `SpscSender` / `SpscReceiver` handles.

## `MpmcRing` & `MpmcSlotView` (`channel.rs`)

[`MpmcRing`](../../../../../src/rs/vertebra-core/src/channel.rs) is Vyukov's bounded queue. A `CacheLine<AtomicU64>` sequence array (one cache line per slot to prevent false sharing, seeded with `i`) precedes the slot area. `head()` is `read_head` and `tail()` is `write_head`.

- **Producers** (`try_send`, `try_send_with(flags, f)`): CAS `tail` when `seq - tail == 0`, write the slot, then store `seq = tail + 1` (`Release`). `has_capacity()` checks `(seq as i64 - tail as i64) >= 0`.
- **Consumers** (`try_recv`, `try_recv_with_flags`, `try_recv_with(reader)`, `try_recv_view`, `recv_timeout_with`): CAS `head` when `seq - (head + 1) == 0`, validate slot length against `payload_capacity()` (`ChannelError::CorruptedSlot`), read or borrow the slot (`MpmcSlotView` releases `seq = head + slots` on `Drop`), and expose zero-copy slices directly to `BatchSnapshot` without heap copying.

## Broker Segment & Seqlock Bounds (`broker.rs`)

[`broker.rs`](../../../../../src/rs/vertebra-core/src/broker.rs)[^src-vertebra-core-src-broker-rs] guards shared lease-table and statement-cache mappings via `checked_aligned_section<T>`, returning `BrokerError::RegionTooSmall` on overflow/short regions and `BrokerError::MisalignedRegion { offset, align }` when either section offset or base-plus-offset fails `align_of::<T>()`. `ShmStatementCache::insert` bounds odd-seqlock recovery spins at `MAX_STUCK_SPINS` (`2048`) before taking over a crashed writer's slot with a CAS to `cur.saturating_add(2)`.

## Blocking Waits (`sync.rs`, `platform/`)

`SpscRing` blocking paths (`send`, `send_with`, `send_timeout`, `recv`, `recv_timeout`, `recv_view`) increment `send_waiters` / `recv_waiters`, re-check the cursor, and sleep on the matching wait word through [`sync.rs`](../../../../../src/rs/vertebra-core/src/sync.rs)[^src-vertebra-core-src-sync-rs]: `wait_on_address`, `wait_on_address_timeout`, `wake_one`, `wake_all`. `MpmcRing` blocking paths (`send`, `recv`, `recv_timeout`, `recv_timeout_with`) spin with `crossbeam_utils::Backoff::snooze` instead and never touch the wait words.

| Platform | Implementation |
|---|---|
| Linux ([`linux.rs`](../../../../../src/rs/vertebra-core/src/platform/linux.rs)[^src-vertebra-core-src-platform-linux-rs]) | `SYS_futex` `FUTEX_WAIT` / `FUTEX_WAKE`; also `pidfd_open(pid)` (flags `0`) |
| macOS ([`macos.rs`](../../../../../src/rs/vertebra-core/src/platform/macos.rs)[^src-vertebra-core-src-platform-macos-rs]) | unconditional `__ulock_wait` / `__ulock_wake` using `UL_COMPARE_AND_WAIT | ULF_NO_ERRNO` (plus `ULF_WAKE_ALL` for `wake_all`) |
| Windows ([`windows.rs`](../../../../../src/rs/vertebra-core/src/platform/windows.rs)[^src-vertebra-core-src-platform-windows-rs]) | `WaitOnAddress`, `WakeByAddressSingle`, `WakeByAddressAll` |

`sync.rs` also defines `WaitWord`, `SharedMutex<T>`, `SharedRwLock<T>`, and `SharedCondvar` for cross-process locking. No production code uses them.

## Cross-Process Wakeup FDs (`notify.rs`)

[`Notifier`](../../../../../src/rs/vertebra-core/src/notify.rs)[^src-vertebra-core-src-notify-rs] is the FD that asyncio watches with `add_reader`. On Linux it is `eventfd(0, EFD_NONBLOCK)`; elsewhere a non-blocking `pipe(2)`. Neither sets `CLOEXEC`, so children inherit them. API: `new`, `notify`, `clear`, `as_raw_fd`, `from_raw_fd` (Linux), `from_raw_fds(read, write)` and `write_fd` (non-Linux).

Batch-level LZ4/Zstd compression is applied by `BatchBridge`, not by the rings; see [Tiered Rings & Batch Bridge](tiered-rings-and-batch-bridge.md).

[^src-vertebra-core-src-shm-rs]: `src/rs/vertebra-core/src/shm.rs` — Rust implementation module (src/rs/vertebra-core/src/shm.rs).
[^src-vertebra-core-src-buffer-rs]: `src/rs/vertebra-core/src/buffer.rs` — Rust implementation module (src/rs/vertebra-core/src/buffer.rs).
[^src-vertebra-core-src-channel-rs]: `src/rs/vertebra-core/src/channel.rs` — Rust implementation module (src/rs/vertebra-core/src/channel.rs).
[^src-vertebra-core-src-notify-rs]: `src/rs/vertebra-core/src/notify.rs` — Rust implementation module (src/rs/vertebra-core/src/notify.rs).
[^src-vertebra-core-src-sync-rs]: `src/rs/vertebra-core/src/sync.rs` — Rust implementation module (src/rs/vertebra-core/src/sync.rs).
[^src-vertebra-core-src-platform-linux-rs]: `src/rs/vertebra-core/src/platform/linux.rs` — Rust implementation module (src/rs/vertebra-core/src/platform/linux.rs).
[^src-vertebra-core-src-platform-macos-rs]: `src/rs/vertebra-core/src/platform/macos.rs` — Rust implementation module (src/rs/vertebra-core/src/platform/macos.rs).
[^src-vertebra-core-src-platform-windows-rs]: `src/rs/vertebra-core/src/platform/windows.rs` — Rust implementation module (src/rs/vertebra-core/src/platform/windows.rs).
[^src-vertebra-core-src-broker-rs]: `src/rs/vertebra-core/src/broker.rs` — Rust implementation module (src/rs/vertebra-core/src/broker.rs).
