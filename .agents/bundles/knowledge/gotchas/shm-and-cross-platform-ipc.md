---
type: Guide
title: "Shared-Memory Rings & Cross-Platform IPC Gotchas"
description: "Critical shared-memory ring, futex/ulock memory ordering, macOS XNU shm_open/pipe FD, response framing, and matchit 0.9 routing gotchas."
status: stable
tags: [gotchas, shared-memory, ipc, macos, routing]
sources:
  - resource: src/rs/vertebra-core/src/channel.rs
  - resource: src/rs/vertebra-core/src/ipc.rs
  - resource: src/rs/vertebra-core/src/shm.rs
  - resource: src/rs/vertebra-core/src/notify.rs
  - resource: src/rs/vertebra-core/src/buffer.rs
  - resource: src/rs/vertebra-core/src/platform/macos.rs
  - resource: src/rs/vertebra-core/src/transport/ring_pair.rs
  - resource: src/rs/vertebra-core/src/protocol/mod.rs
  - resource: src/rs/vertebra-core/src/protocol/codec.rs
  - resource: src/rs/vertebra-core/src/router.rs
  - resource: src/rs/vertebra-http/src/router.rs
  - resource: src/rs/vertebra-http/src/dispatcher/shm.rs
---

# Shared-Memory Rings & Cross-Platform IPC Gotchas

## 1. `SlotView` Borrow Flag, `Ordering::SeqCst` & Unchecked Reads (`channel.rs`)

- **Single Active `SlotView` Borrow**: [`SpscRing::try_recv_view()`](../../../../src/rs/vertebra-core/src/channel.rs) borrows the SHM slot in place. Although `Drop for SlotView` calls `self.release()`, a live `SlotView` holds the ring's single borrow flag: until it is released or dropped, subsequent `try_recv_view` calls return `ChannelError::SlotBorrowed` and the producer cannot reuse the slot. Never hold a `SlotView` across `await` points or long Python calls.
- **Store-Load Reordering on `send_waiters` / `recv_waiters`**: In `SlotView::release()`, after `self.ring.read_head().store(next, Ordering::Release)`, `self.ring.wait_read_ptr().fetch_add(1, Ordering::SeqCst)` **must** execute unconditionally before loading `self.ring.send_waiters().load(Ordering::SeqCst)`. Using `Acquire`/`Release` alone allows the CPU to reorder the store to `read_head` past the load of `send_waiters`, causing missed `futex_wake` notifications.
- **SPSC Usable Capacity & Unchecked Zero-Copy Lengths**: SPSC rings have a usable slot capacity of `slots - 1`. In addition, zero-copy reads (`SpscRing::peek_slot` and `MpmcRing::try_recv_with`) currently trust `SlotHeader::len` from shared memory without clamping to `payload_capacity()`; callers must ensure worker processes cannot write out-of-bounds lengths.

## 2. `MpmcRing::has_capacity()` Sequence Check (`channel.rs`)

- Never use SPSC capacity arithmetic (`write_head - read_head < capacity`) on [`MpmcRing`](../../../../src/rs/vertebra-core/src/channel.rs). In Vyukov's bounded MPMC queue, a slot at `tail = write_head` is available for writing if and only if `(slot.sequence.load(Acquire) as i64) - (tail as i64) >= 0`.

## 3. macOS XNU `shm_open`, `pipe` FDs, `__ulock_wait` & Cache Lines (`shm.rs`, `notify.rs`, `macos.rs`, `buffer.rs`)

1. **31-Byte `PSHMNAMLEN` Limit**: macOS XNU (and the Linux fallback when `memfd_create` fails) rejects `shm_open` names longer than 31 bytes (including the leading `/`) with `EINVAL`. [`shm.rs`](../../../../src/rs/vertebra-core/src/shm.rs) formats names as `/vtb-{pid:x}-{seq:x}-{attempt:x}` (compact by construction while `seq` and `attempt` stay small, with no runtime length check). Also note `make rs-test` purges `/dev/shm/test-*` and `/dev/shm/*vertebra*`, so orphaned `/dev/shm/vtb-*` segments from interrupted server runs must be removed manually (`rm -f /dev/shm/vtb-*`).
2. **Inherited Notifier & memfd Descriptors**: On non-Linux Unix (`pipe(2)`), child workers must inherit **both** `read_fd` (`VERTEBRA_REQ_NOTIFY_FD`) and `write_fd` (`VERTEBRA_REQ_NOTIFY_WRITE_FD`) via [`Notifier::from_raw_fds`](../../../../src/rs/vertebra-core/src/notify.rs). On Linux, `eventfd` is created without `EFD_CLOEXEC` while `memfd` regions start with `MFD_CLOEXEC` and are cleared via `make_inheritable` before spawning workers; unrelated `fork`/`exec` calls in the parent process will leak notifier FDs unless closed.
3. **`__ulock_wait` with `ULF_NO_ERRNO` (`0x0100_0000`)**: With the opt-in `macos-ulock` feature enabled (default macOS wait uses spin + 1 ms `nanosleep` in [`macos.rs`](../../../../src/rs/vertebra-core/src/platform/macos.rs)), `__ulock_wait` returns `rc >= 0` (remaining waiters) on success and `-errno` on failure (`-ETIMEDOUT` $\to$ timeout; `-EINTR`/`-EAGAIN`/`-EFAULT` $\to$ spurious wake retry; `-ENOENT` on `__ulock_wake` $\to$ no waiters).
4. **Apple Silicon 128-Byte Cache Lines**: [`CacheLine<T>`](../../../../src/rs/vertebra-core/src/buffer.rs) uses 128-byte alignment on `aarch64` macOS and 64-byte alignment elsewhere, so `ShmHeader` size and field offsets differ across targets.

## 4. Tiered Ring Boundaries, Framing & Flag Overlaps (`ring_pair.rs`, `codec.rs`, `shm.rs`)

- **Tier Boundary Off-by-8 (`select_ring`)**: [`TieredRingSet::select_ring`](../../../../src/rs/vertebra-core/src/transport/ring_pair.rs) compares frame length against `slot_size` rather than `payload_capacity()` (`slot_size - 8`), so frames within 8 bytes of a tier boundary fail with `PayloadTooLarge`.
- **`VRES` vs `ResponseSlot` Framing & `u16` Header Truncation**: [`ring_pair.rs`](../../../../src/rs/vertebra-core/src/transport/ring_pair.rs) uses a 20-byte `VRES` wire header that casts `headers_raw.len() as u16` (truncating header blocks over 64 KiB), whereas [`ipc.rs`](../../../../src/rs/vertebra-core/src/ipc.rs) defines a 16-byte `ResponseSlot` header. Furthermore, `RingPair::from_env` ignores `VERTEBRA_*_SIZE` environment variables and always uses `RingPairConfig::default()`.
- **Compression vs Streaming Flag Collision**: [`CompressionCodec`](../../../../src/rs/vertebra-core/src/protocol/codec.rs) uses bits 6–7 (`0b1100_0000`), which overlap `protocol::flags::STREAMING` (`1 << 6`) and `FINAL` (`1 << 7`) in [`protocol/mod.rs`](../../../../src/rs/vertebra-core/src/protocol/mod.rs).
- **24-Bit Slab Generation Truncation**: [`ShmDispatcher`](../../../../src/rs/vertebra-http/src/dispatcher/shm.rs) packs external request IDs as `(internal_id << 8) | shard_idx`, truncating the upper 8 bits of the 32-bit slab generation counter.

## 5. `matchit 0.9` Catch-All Syntax (`router.rs`)

- `matchit 0.9` uses `{name}` for single segments and `{*name}` for catch-all parameters. Always register routes through [`vertebra_core::router::normalize_typed_path()`](../../../../src/rs/vertebra-core/src/router.rs) (called by `Router::insert` and [`vertebra-http/src/router.rs`](../../../../src/rs/vertebra-http/src/router.rs) `from_entries`), which rewrites `:name`, `*name`, `{name:type}`, and `{name:path}` into `{name}` / `{*name}`. Never call `matchit::Router::insert` directly with unnormalized paths.
