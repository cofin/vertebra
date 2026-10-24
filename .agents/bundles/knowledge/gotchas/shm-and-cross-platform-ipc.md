---
type: Guide
title: "Shared-Memory Rings & Cross-Platform IPC Gotchas"
description: "Critical shared-memory ring, futex/ulock memory ordering, macOS XNU shm_open/pipe FD, response framing, and matchit 0.9 routing gotchas."
status: stable
tags: [gotchas, shared-memory, ipc, macos, routing]
sources:
  - resource: src/rs/vertebra-core/src/bridge.rs
  - resource: src/rs/vertebra-core/src/channel.rs
  - resource: src/rs/vertebra-core/src/ipc.rs
  - resource: src/rs/vertebra-core/src/shm.rs
  - resource: src/rs/vertebra-core/src/notify.rs
  - resource: src/rs/vertebra-core/src/buffer.rs
  - resource: src/rs/vertebra-core/src/platform/macos.rs
  - resource: src/rs/vertebra-core/src/tiered_ring.rs
  - resource: src/rs/vertebra-core/src/transport/ring_pair.rs
  - resource: src/rs/vertebra-core/src/protocol/mod.rs
  - resource: src/rs/vertebra-core/src/protocol/codec.rs
  - resource: src/rs/vertebra-core/src/router.rs
  - resource: src/rs/vertebra-http/src/backpressure.rs
  - resource: src/rs/vertebra-http/src/server.rs
  - resource: src/rs/vertebra-http/src/routing.rs
  - resource: src/rs/vertebra-http/src/dispatcher/shm.rs
---

# Shared-Memory Rings & Cross-Platform IPC Gotchas

## 1. `BatchBridge` Deadlock Prevention, `SlotView` Borrow Flag & Dekker `SeqCst` Barriers

- **Never Block Tokio Worker Threads on Bounded `BatchBridge` Channels**: In [`bridge.rs`](../../../../src/rs/vertebra-core/src/bridge.rs), `batch_tx` and fallback `resp_tx` must remain **unbounded** (`crossbeam_channel::unbounded` / `tokio::sync::mpsc::unbounded_channel`), with unary completions delivered inline through `ResponseSink` (`DirectResponseSink`) and concurrency bounded at ingress by [`BackpressureController`](../../../../src/rs/vertebra-http/src/backpressure.rs). Bounding `batch_tx` and `resp_tx` causes an ABBA deadlock whenever Tokio worker threads block in `batch_tx.send` while Python blocks in `resp_tx.blocking_send`.
- **Single Active `SlotView` Borrow & Fast-Empty Check**: [`SpscRing::try_recv_view()`](../../../../src/rs/vertebra-core/src/channel.rs) borrows the SHM slot in place (`slot_borrowed` flag; `try_recv_view` checks `read_head == write_head` *before* the `slot_borrowed` CAS so empty tier polls never dirty cache lines). Never hold a `SlotView` or `MpmcSlotView` across `await` points or long Python calls.
- **Store-Load `SeqCst` Barriers (`send_waiters`, `consumer_sleeping`, `active_flags`, `BackpressureController`)**:
  - In `SlotView::release()`, `wait_read_ptr().fetch_add(1, Ordering::SeqCst)` must execute unconditionally before `send_waiters().load(Ordering::SeqCst)`.
  - In [`TieredRingSet`](../../../../src/rs/vertebra-core/src/transport/ring_pair.rs), the producer must call `mark_tier_active(bit)` *before* publishing `write_head` in `try_send_with` and test-and-clear `ShmHeader::should_notify_and_clear()` (`consumer_sleeping`) with `SeqCst` *after* `write_head` is visible; otherwise a waking consumer can miss a medium/large/xlarge response when `has_non_small_data()` reads `0`.
  - In [`BackpressureController`](../../../../src/rs/vertebra-http/src/backpressure.rs), `in_flight` and `queue_depth` updates require `Ordering::SeqCst` so concurrent admission checks cannot observe stale zero depths across threads.

## 2. `MpmcRing::has_capacity()` Sequence Check & Padded Sequence Array (`channel.rs`)

- Never use SPSC capacity arithmetic (`write_head - read_head < capacity`) on [`MpmcRing`](../../../../src/rs/vertebra-core/src/channel.rs). In Vyukov's bounded MPMC queue, a slot at `tail = write_head` is available for writing if and only if `(slot.sequence.load(Acquire) as i64) - (tail as i64) >= 0`, and each sequence slot is `CacheLine<AtomicU64>`-padded to eliminate false sharing.

## 3. macOS XNU `shm_open`, `pipe` FDs, `__ulock_wait`, Cache Lines & gVisor `SO_REUSEPORT`

1. **31-Byte `PSHMNAMLEN` Limit**: macOS XNU (and the Linux fallback when `memfd_create` fails) rejects `shm_open` names longer than 31 bytes (including `/`) with `EINVAL`. [`shm.rs`](../../../../src/rs/vertebra-core/src/shm.rs) formats names as `/vtb-{pid:x}-{seq:x}-{attempt:x}` and enforces `validate_shm_name_len`. Orphaned `/dev/shm/vtb-*` segments from interrupted server runs can be removed with `rm -f /dev/shm/vtb-*`.
2. **Inherited Notifier & memfd Descriptors**: On non-Linux Unix (`pipe(2)`), child workers must inherit **both** `read_fd` (`VERTEBRA_REQ_NOTIFY_FD`) and `write_fd` (`VERTEBRA_REQ_NOTIFY_WRITE_FD`) via [`Notifier::from_raw_fds`](../../../../src/rs/vertebra-core/src/notify.rs). On Linux, `eventfd` (`EFD_CLOEXEC`) and `memfd` (`MFD_CLOEXEC`) clear `FD_CLOEXEC` via `make_inheritable` right before spawning workers.
3. **`__ulock_wait` with `ULF_NO_ERRNO` (`0x0100_0000`)**: With `macos-ulock` enabled ([`macos.rs`](../../../../src/rs/vertebra-core/src/platform/macos.rs)), `__ulock_wait` returns `rc >= 0` on success and `-errno` on failure (`-ETIMEDOUT` $\to$ timeout; `-EINTR`/`-EAGAIN`/`-EFAULT` $\to$ retry; `-ENOENT` on `__ulock_wake` $\to$ no waiters).
4. **Apple Silicon 128-Byte Cache Lines**: [`CacheLine<T>`](../../../../src/rs/vertebra-core/src/buffer.rs) uses 128-byte alignment on `aarch64` macOS and 64-byte alignment elsewhere.
5. **gVisor Sandbox `SO_REUSEPORT` Constraint**: [`server::bind_listener`](../../../../src/rs/vertebra-http/src/server.rs) sets `SO_REUSEADDR`, `SO_REUSEPORT`, and `TCP_NODELAY` so multi-worker `EmbeddedSupervisor` processes bind independent accept loops on the same port. Container sandboxes (such as gVisor) that reject a second `listen()` on a `SO_REUSEPORT` socket with `EADDRINUSE` require running multi-worker `SO_REUSEPORT` benchmarks on a native host kernel.

## 4. Right-Sized Tiered Ring Capacities, Framing & Flag Bitmasks (`tiered_ring.rs`, `ring_pair.rs`, `codec.rs`)

- **Right-Sized Default Tiers (`>= BatchLimit`)**: [`TieredRingConfig::default()`](../../../../src/rs/vertebra-core/src/tiered_ring.rs) allocates `small` `4 KiB × 512` (`511` usable slots, well above `BatchLimit = 64` so a full 64-response batch never parks mid-flush), `medium` `32 KiB × 128`, `large` `256 KiB × 32`, and `xlarge` `2 MiB × 8` (`30 MiB` per direction). `TieredRingSet::select_ring` compares frame length against `ring.payload_capacity()` (`slot_size - 8`).
- **`VRES` vs `ResponseSlot` Framing**: [`ring_pair.rs`](../../../../src/rs/vertebra-core/src/transport/ring_pair.rs) uses a 20-byte `VRES` wire header (`drain_responses_with` zero-copy drain), whereas [`ipc.rs`](../../../../src/rs/vertebra-core/src/ipc.rs) defines a 16-byte `ResponseSlot` header. `RingPair::from_env` reads `VERTEBRA_*_SIZE` and `VERTEBRA_*_SLOTS` environment variables when attaching inherited rings.
- **Compression Codec vs Streaming Flag Bits**: [`CompressionCodec`](../../../../src/rs/vertebra-core/src/protocol/codec.rs) encodes the batch-slab codec in `CODEC_MASK = 0b11 << 6` (bits 6–7) gated on `flags::COMPRESSED` (bit 1), while `response_flags` (`STREAMING = 1 << 6`, `FINAL = 1 << 7`, `HEADERS_SENT = 1 << 8`, `TRAILERS = 1 << 9`) in [`protocol/mod.rs`](../../../../src/rs/vertebra-core/src/protocol/mod.rs) occupy bits 6–9 on `ResponseSlot` streaming response frames; always check `flags & flags::COMPRESSED != 0` via `CompressionCodec::from_flags` before reading bits 6–7 as a codec.

## 5. `matchit 0.9` Catch-All Syntax (`routing.rs`)

- `matchit 0.9` uses `{name}` for single segments and `{*name}` for catch-all parameters. Always register routes through [`vertebra_core::router::normalize_typed_path()`](../../../../src/rs/vertebra-core/src/router.rs) (called by `Router::insert` and [`vertebra-http/src/routing.rs`](../../../../src/rs/vertebra-http/src/routing.rs) `from_entries`), which rewrites `:name`, `*name`, `{name:type}`, and `{name:path}` into `{name}` / `{*name}`. Never call `matchit::Router::insert` directly with unnormalized paths.
