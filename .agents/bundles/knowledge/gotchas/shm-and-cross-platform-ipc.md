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
  - resource: src/rs/vertebra-http/src/routing.rs
  - resource: src/rs/vertebra-http/src/dispatcher/shm.rs
---

# Shared-Memory Rings & Cross-Platform IPC Gotchas

## 1. `SlotView` Borrow Flag, `Ordering::SeqCst` & Length Clamping (`channel.rs`)

- **Single Active `SlotView` Borrow**: [`SpscRing::try_recv_view()`](../../../../src/rs/vertebra-core/src/channel.rs) borrows the SHM slot in place. Although `Drop for SlotView` calls `self.release()`, a live `SlotView` holds the ring's single borrow flag: until it is released or dropped, subsequent `try_recv_view` calls return `ChannelError::SlotBorrowed` and the producer cannot reuse the slot. Never hold a `SlotView` across `await` points or long Python calls.
- **Store-Load Reordering on `send_waiters` / `recv_waiters`**: In `SlotView::release()`, after `self.ring.read_head().store(next, Ordering::Release)`, `self.ring.wait_read_ptr().fetch_add(1, Ordering::SeqCst)` **must** execute unconditionally before loading `self.ring.send_waiters().load(Ordering::SeqCst)`. Using `Acquire`/`Release` alone allows the CPU to reorder the store to `read_head` past the load of `send_waiters`, causing missed `futex_wake` notifications.
- **SPSC Usable Capacity & Clamped Zero-Copy Lengths**: SPSC rings have a usable slot capacity of `slots - 1`. Both copying and zero-copy reads (`SpscRing::peek_slot` and `MpmcRing::try_recv_with`) clamp `SlotHeader::len` from shared memory to `payload_capacity()` before constructing slices.

## 2. `MpmcRing::has_capacity()` Sequence Check (`channel.rs`)

- Never use SPSC capacity arithmetic (`write_head - read_head < capacity`) on [`MpmcRing`](../../../../src/rs/vertebra-core/src/channel.rs). In Vyukov's bounded MPMC queue, a slot at `tail = write_head` is available for writing if and only if `(slot.sequence.load(Acquire) as i64) - (tail as i64) >= 0`.

## 3. macOS XNU `shm_open`, `pipe` FDs, `__ulock_wait` & Cache Lines (`shm.rs`, `notify.rs`, `macos.rs`, `buffer.rs`)

1. **31-Byte `PSHMNAMLEN` Limit**: macOS XNU (and the Linux fallback when `memfd_create` fails) rejects `shm_open` names longer than 31 bytes (including the leading `/`) with `EINVAL`. [`shm.rs`](../../../../src/rs/vertebra-core/src/shm.rs) formats names as `/vtb-{pid:x}-{seq:x}-{attempt:x}` and enforces `validate_shm_name_len`. Also note `make rs-test` purges `/dev/shm/test-*` and `/dev/shm/*vertebra*`, so orphaned `/dev/shm/vtb-*` segments from interrupted server runs must be removed manually (`rm -f /dev/shm/vtb-*`).
2. **Inherited Notifier & memfd Descriptors**: On non-Linux Unix (`pipe(2)`), child workers must inherit **both** `read_fd` (`VERTEBRA_REQ_NOTIFY_FD`) and `write_fd` (`VERTEBRA_REQ_NOTIFY_WRITE_FD`) via [`Notifier::from_raw_fds`](../../../../src/rs/vertebra-core/src/notify.rs). On Linux, `eventfd` is created with `EFD_CLOEXEC` and `memfd` regions start with `MFD_CLOEXEC`, cleared via `make_inheritable` right before spawning workers.
3. **`__ulock_wait` with `ULF_NO_ERRNO` (`0x0100_0000`)**: With the opt-in `macos-ulock` feature enabled (default macOS wait uses spin + 1 ms `nanosleep` in [`macos.rs`](../../../../src/rs/vertebra-core/src/platform/macos.rs)), `__ulock_wait` returns `rc >= 0` (remaining waiters) on success and `-errno` on failure (`-ETIMEDOUT` $\to$ timeout; `-EINTR`/`-EAGAIN`/`-EFAULT` $\to$ spurious wake retry; `-ENOENT` on `__ulock_wake` $\to$ no waiters).
4. **Apple Silicon 128-Byte Cache Lines**: [`CacheLine<T>`](../../../../src/rs/vertebra-core/src/buffer.rs) uses 128-byte alignment on `aarch64` macOS and 64-byte alignment elsewhere, so `ShmHeader` size and field offsets differ across targets.

## 4. Tiered Ring Boundaries, Framing & Flag Bitmasks (`ring_pair.rs`, `codec.rs`, `shm.rs`)

- **Tier Boundary Payload Capacity (`select_ring`)**: [`TieredRingSet::select_ring`](../../../../src/rs/vertebra-core/src/transport/ring_pair.rs) compares frame length against `ring.payload_capacity()` (`slot_size - 8`), accounting for the 8-byte `SlotHeader`.
- **`VRES` vs `ResponseSlot` Framing**: [`ring_pair.rs`](../../../../src/rs/vertebra-core/src/transport/ring_pair.rs) uses a 20-byte `VRES` wire header, whereas [`ipc.rs`](../../../../src/rs/vertebra-core/src/ipc.rs) defines a 16-byte `ResponseSlot` header. `RingPair::from_env` reads `VERTEBRA_*_SIZE` and `VERTEBRA_*_SLOTS` environment variables when attaching inherited rings.
- **Disjoint Compression & Streaming Flags**: [`CompressionCodec`](../../../../src/rs/vertebra-core/src/protocol/codec.rs) uses bits 8–9 (`0x0300`), keeping bits 6–7 (`STREAMING = 1 << 6`, `FINAL = 1 << 7`) in [`protocol/mod.rs`](../../../../src/rs/vertebra-core/src/protocol/mod.rs) disjoint.

## 5. `matchit 0.9` Catch-All Syntax (`routing.rs`)

- `matchit 0.9` uses `{name}` for single segments and `{*name}` for catch-all parameters. Always register routes through [`vertebra_core::router::normalize_typed_path()`](../../../../src/rs/vertebra-core/src/router.rs) (called by `Router::insert` and [`vertebra-http/src/routing.rs`](../../../../src/rs/vertebra-http/src/routing.rs) `from_entries`), which rewrites `:name`, `*name`, `{name:type}`, and `{name:path}` into `{name}` / `{*name}`. Never call `matchit::Router::insert` directly with unnormalized paths.
