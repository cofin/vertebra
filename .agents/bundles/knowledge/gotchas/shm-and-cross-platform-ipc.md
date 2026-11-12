---
type: Guide
title: "Shared-Memory Rings & Cross-Platform IPC Gotchas"
description: "Critical shared-memory ring, futex/ulock memory ordering, macOS XNU shm_open/pipe FD, response framing, and matchit 0.9 routing gotchas."
status: stable
tags:
  - gotchas
  - shared-memory
  - ipc
  - macos
  - routing
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
  - id: src-vertebra-core-src-bridge-rs
    resource: src/rs/vertebra-core/src/bridge.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/bridge.rs)"
  - id: src-vertebra-core-src-channel-rs
    resource: src/rs/vertebra-core/src/channel.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/channel.rs)"
  - id: src-vertebra-core-src-ipc-rs
    resource: src/rs/vertebra-core/src/ipc.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/ipc.rs)"
  - id: src-vertebra-core-src-shm-rs
    resource: src/rs/vertebra-core/src/shm.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/shm.rs)"
  - id: src-vertebra-core-src-notify-rs
    resource: src/rs/vertebra-core/src/notify.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/notify.rs)"
  - id: src-vertebra-core-src-buffer-rs
    resource: src/rs/vertebra-core/src/buffer.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/buffer.rs)"
  - id: src-vertebra-core-src-platform-macos-rs
    resource: src/rs/vertebra-core/src/platform/macos.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/platform/macos.rs)"
  - id: src-vertebra-core-src-tiered-ring-rs
    resource: src/rs/vertebra-core/src/tiered_ring.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/tiered_ring.rs)"
  - id: src-vertebra-core-src-transport-ring-pair-rs
    resource: src/rs/vertebra-core/src/transport/ring_pair.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/transport/ring_pair.rs)"
  - id: src-vertebra-core-src-protocol-mod-rs
    resource: src/rs/vertebra-core/src/protocol/mod.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/protocol/mod.rs)"
  - id: src-vertebra-core-src-protocol-codec-rs
    resource: src/rs/vertebra-core/src/protocol/codec.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/protocol/codec.rs)"
  - id: src-vertebra-core-src-router-rs
    resource: src/rs/vertebra-core/src/router.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/router.rs)"
  - id: src-vertebra-http-src-backpressure-rs
    resource: src/rs/vertebra-http/src/backpressure.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/backpressure.rs)"
  - id: src-vertebra-http-src-server-rs
    resource: src/rs/vertebra-http/src/server.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/server.rs)"
  - id: src-vertebra-http-src-routing-rs
    resource: src/rs/vertebra-http/src/routing.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/routing.rs)"
  - id: src-vertebra-http-src-dispatcher-shm-rs
    resource: src/rs/vertebra-http/src/dispatcher/shm.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/dispatcher/shm.rs)"
---

# Shared-Memory Rings & Cross-Platform IPC Gotchas

<!-- truth: start -->
- Never block Tokio worker threads on bounded `BatchBridge` channels, release `SlotView` borrows before polling again, and maintain `Ordering::SeqCst` barriers across Dekker sleep/wake handshakes.
- Keep `shm_open` names within macOS's 31-byte `PSHMNAMLEN` limit, clear `FD_CLOEXEC` on inherited worker ring/pipe descriptors, and keep `matchit 0.9` and Python `_router.py` FNV-1a normalization identical.
<!-- truth: end -->

## 1. `BatchBridge` Deadlock Prevention, `SlotView` Borrow Flag & Dekker `SeqCst` Barriers

- **Never Block Tokio Worker Threads on Bounded `BatchBridge` Channels**: In [`bridge.rs`](../../../../src/rs/vertebra-core/src/bridge.rs)[^src-vertebra-core-src-bridge-rs], `batch_tx` and fallback `resp_tx` must remain **unbounded** (`crossbeam_channel::unbounded` / `tokio::sync::mpsc::unbounded_channel`), with unary completions delivered inline through `ResponseSink` (`DirectResponseSink`) and concurrency bounded at ingress by [`BackpressureController`](../../../../src/rs/vertebra-http/src/backpressure.rs)[^src-vertebra-http-src-backpressure-rs]. Bounding `batch_tx` and `resp_tx` causes an ABBA deadlock whenever Tokio worker threads block in `batch_tx.send` while Python blocks in `resp_tx.blocking_send`.
- **Single Active `SlotView` Borrow & Fast-Empty Check**: [`SpscRing::try_recv_view()`](../../../../src/rs/vertebra-core/src/channel.rs)[^src-vertebra-core-src-channel-rs] borrows the SHM slot in place (`slot_borrowed` flag; `try_recv_view` checks `read_head == write_head` *before* the `slot_borrowed` CAS so empty tier polls never dirty cache lines). Never hold a `SlotView` or `MpmcSlotView` across `await` points or long Python calls.
- **Store-Load `SeqCst` Barriers (`send_waiters`, `consumer_sleeping`, `active_flags`, `BackpressureController`)**:
  - In `SlotView::release()`, `wait_read_ptr().fetch_add(1, Ordering::SeqCst)` must execute unconditionally before `send_waiters().load(Ordering::SeqCst)`.
  - In [`TieredRingSet`](../../../../src/rs/vertebra-core/src/transport/ring_pair.rs)[^src-vertebra-core-src-transport-ring-pair-rs], the producer must call `mark_tier_active(bit)` *before* publishing `write_head` in `try_send_with` and test-and-clear `ShmHeader::should_notify_and_clear()` (`consumer_sleeping`) with `SeqCst` *after* `write_head` is visible; otherwise a waking consumer can miss a medium/large/xlarge response when `has_non_small_data()` reads `0`.
  - In [`BackpressureController`](../../../../src/rs/vertebra-http/src/backpressure.rs), `in_flight` and `queue_depth` updates require `Ordering::SeqCst` so concurrent admission checks cannot observe stale zero depths across threads.
  - In [`Transport::poll_batch`](../../../../src/rs/vertebra-core/src/transport/mod.rs), always `drop(_guard)` (`cancel_request_sleep()`, setting `consumer_sleeping = 0`) *before* calling `request_notifier.clear()`. If `clear()` runs while `consumer_sleeping == 1`, a concurrent producer calling `notify_request_if_sleeping()` can clear `consumer_sleeping` and write to `eventfd` right before `request_notifier.clear()` drains that `eventfd`, losing the wakeup for the newly arrived batch.
- **Tokio LIFO Slot Preemption in `RequestBatcher` (`batcher.rs`)**: When `response::handle` sends the first `PendingRequest` on an `mpsc` channel, Tokio places the woken `RequestBatcher` task into its thread-local LIFO slot and can preempt the sender before sibling connection tasks enqueue. Calling `tokio::task::yield_now().await` once after the initial `try_recv()` loop allows sibling connection tasks from the same `epoll_wait` readiness batch to enqueue, but must be gated on `dispatcher.in_flight() > 0 || !rx.is_empty()` so single-connection (`c=1`) requests flush immediately with zero yield latency.

## 2. `MpmcRing::has_capacity()` Sequence Check & Padded Sequence Array (`channel.rs`)

- Never use SPSC capacity arithmetic (`write_head - read_head < capacity`) on [`MpmcRing`](../../../../src/rs/vertebra-core/src/channel.rs). In Vyukov's bounded MPMC queue, a slot at `tail = write_head` is available for writing if and only if `(slot.sequence.load(Acquire) as i64) - (tail as i64) >= 0`, and each sequence slot is `CacheLine<AtomicU64>`-padded to eliminate false sharing.

## 3. macOS XNU `shm_open`, `pipe` FDs, `__ulock_wait`, Cache Lines & gVisor `SO_REUSEPORT`

1. **31-Byte `PSHMNAMLEN` Limit**: macOS XNU (and the Linux fallback when `memfd_create` fails) rejects `shm_open` names longer than 31 bytes (including `/`) with `EINVAL`. [`shm.rs`](../../../../src/rs/vertebra-core/src/shm.rs)[^src-vertebra-core-src-shm-rs] formats names as `/vtb-{pid:x}-{seq:x}-{attempt:x}` and enforces `validate_shm_name_len`. Orphaned `/dev/shm/vtb-*` segments from interrupted server runs can be removed with `rm -f /dev/shm/vtb-*`.
2. **Inherited Notifier & memfd Descriptors**: On non-Linux Unix (`pipe(2)`), child workers must inherit **both** `read_fd` (`VERTEBRA_REQ_NOTIFY_FD`) and `write_fd` (`VERTEBRA_REQ_NOTIFY_WRITE_FD`) via [`Notifier::from_raw_fds`](../../../../src/rs/vertebra-core/src/notify.rs)[^src-vertebra-core-src-notify-rs]. On Linux, `eventfd` (`EFD_CLOEXEC`) and `memfd` (`MFD_CLOEXEC`) clear `FD_CLOEXEC` via `make_inheritable` right before spawning workers.
3. **`__ulock_wait` with `ULF_NO_ERRNO` (`0x0100_0000`)**: With `macos-ulock` enabled ([`macos.rs`](../../../../src/rs/vertebra-core/src/platform/macos.rs)[^src-vertebra-core-src-platform-macos-rs]), `__ulock_wait` returns `rc >= 0` on success and `-errno` on failure (`-ETIMEDOUT` $\to$ timeout; `-EINTR`/`-EAGAIN`/`-EFAULT` $\to$ retry; `-ENOENT` on `__ulock_wake` $\to$ no waiters).
4. **Apple Silicon 128-Byte Cache Lines**: [`CacheLine<T>`](../../../../src/rs/vertebra-core/src/buffer.rs)[^src-vertebra-core-src-buffer-rs] uses 128-byte alignment on `aarch64` macOS and 64-byte alignment elsewhere.
5. **gVisor Sandbox `SO_REUSEPORT` Constraint**: [`server::bind_listener`](../../../../src/rs/vertebra-http/src/server.rs)[^src-vertebra-http-src-server-rs] sets `SO_REUSEADDR`, `SO_REUSEPORT`, and `TCP_NODELAY` so multi-worker `EmbeddedSupervisor` processes bind independent accept loops on the same port. Container sandboxes (such as gVisor) that reject a second `listen()` on a `SO_REUSEPORT` socket with `EADDRINUSE` require running multi-worker `SO_REUSEPORT` benchmarks on a native host kernel.

## 4. Container `/dev/shm` Sizing, `memfd_create` Primary Rings & `/tmp/vtb-*` Overflow Fallback (`shm.rs`, `tiered_ring.rs`, `ring_pair.rs`, `codec.rs`)

- **Container `/dev/shm` (`64 MiB` Default) vs `memfd_create` and `/tmp/vtb-*` Overflow**: Docker and Kubernetes default `/dev/shm` to `64 MiB`, whereas Cloud Run and AWS Lambda size in-memory filesystems proportionally to instance memory. On Linux, primary `RingPair` rings use `memfd_create` (`ShmRegion::create`, backed by general cgroup anonymous RAM rather than `/dev/shm`), and [`RingPairConfig::for_environment(worker_count)`](../../../../src/rs/vertebra-core/src/transport/ring_pair.rs) automatically selects `RingPairConfig::default()` (`46 MiB/worker`: `16 MiB` request ring + `30 MiB` `TieredRingConfig::default()`) or `RingPairConfig::compact()` (`12 MiB/worker`: `4 MiB` request ring + `8 MiB` `TieredRingConfig::compact()`) based on `VERTEBRA_SHM_PROFILE`, cgroup `memory.max`, or `/dev/shm` capacity on non-Linux Unix. For oversized single requests (`> 64 KiB`, `VQOV`) and responses (`> 2 MiB`, `VROV`), [`ShmRegion::create_overflow_region`](../../../../src/rs/vertebra-core/src/shm.rs) probes `/dev/shm` free capacity (`statvfs` >= `2 × size + 8 MiB` headroom) and automatically falls back to `/tmp/vtb-*` (`create_file_backed`, or immediately when `VERTEBRA_OVERFLOW_BACKEND=tmp`).
- **Right-Sized Default Tiers (`>= BatchLimit`) & Dynamic `from_env` Slot Calculation**: [`TieredRingConfig::default()`](../../../../src/rs/vertebra-core/src/tiered_ring.rs)[^src-vertebra-core-src-tiered-ring-rs] allocates `small` `4 KiB × 512` (`511` usable slots, well above `BatchLimit = 64` so a full 64-response batch never parks mid-flush), `medium` `32 KiB × 128`, `large` `256 KiB × 32`, and `xlarge` `2 MiB × 8` (`30 MiB` per direction). `RingPair::from_env` reconstructs `request_slots` and each response tier's `slot_count` from `VERTEBRA_REQ_RING_SIZE` and `VERTEBRA_RESP_RING_{SMALL,MEDIUM,LARGE,XLARGE}_SIZE` via `RingLayout::slots_from_total_size` so child workers attach to both `default` and `compact` ring geometries.
- **`VRES` vs `ResponseSlot` Framing**: [`ring_pair.rs`](../../../../src/rs/vertebra-core/src/transport/ring_pair.rs) uses a 20-byte `VRES` wire header (`drain_responses_with` zero-copy drain), whereas [`ipc.rs`](../../../../src/rs/vertebra-core/src/ipc.rs)[^src-vertebra-core-src-ipc-rs] defines a 16-byte `ResponseSlot` header.
- **Compression Codec vs Streaming Flag Bits**: [`CompressionCodec`](../../../../src/rs/vertebra-core/src/protocol/codec.rs)[^src-vertebra-core-src-protocol-codec-rs] encodes the batch-slab codec in `CODEC_MASK = 0b11 << 6` (bits 6–7) gated on `flags::COMPRESSED` (bit 1), while `response_flags` (`STREAMING = 1 << 6`, `FINAL = 1 << 7`, `HEADERS_SENT = 1 << 8`, `TRAILERS = 1 << 9`) in [`protocol/mod.rs`](../../../../src/rs/vertebra-core/src/protocol/mod.rs)[^src-vertebra-core-src-protocol-mod-rs] occupy bits 6–9 on `ResponseSlot` streaming response frames; always check `flags & flags::COMPRESSED != 0` via `CompressionCodec::from_flags` before reading bits 6–7 as a codec.

## 5. `matchit 0.9` Catch-All Syntax (`routing.rs`)

- `matchit 0.9` uses `{name}` for single segments and `{*name}` for catch-all parameters. Always register routes through [`vertebra_core::router::normalize_typed_path()`](../../../../src/rs/vertebra-core/src/router.rs)[^src-vertebra-core-src-router-rs] (called by `Router::insert` and [`vertebra-http/src/routing.rs`](../../../../src/rs/vertebra-http/src/routing.rs)[^src-vertebra-http-src-routing-rs] `from_entries`), which rewrites `:name`, `*name`, `{name:type}`, and `{name:path}` into `{name}` / `{*name}`. Never call `matchit::Router::insert` directly with unnormalized paths.

Provenance sources: [^src-vertebra-http-src-dispatcher-shm-rs].

[^src-vertebra-core-src-bridge-rs]: `src/rs/vertebra-core/src/bridge.rs` — Rust implementation module (src/rs/vertebra-core/src/bridge.rs).
[^src-vertebra-core-src-channel-rs]: `src/rs/vertebra-core/src/channel.rs` — Rust implementation module (src/rs/vertebra-core/src/channel.rs).
[^src-vertebra-core-src-ipc-rs]: `src/rs/vertebra-core/src/ipc.rs` — Rust implementation module (src/rs/vertebra-core/src/ipc.rs).
[^src-vertebra-core-src-shm-rs]: `src/rs/vertebra-core/src/shm.rs` — Rust implementation module (src/rs/vertebra-core/src/shm.rs).
[^src-vertebra-core-src-notify-rs]: `src/rs/vertebra-core/src/notify.rs` — Rust implementation module (src/rs/vertebra-core/src/notify.rs).
[^src-vertebra-core-src-buffer-rs]: `src/rs/vertebra-core/src/buffer.rs` — Rust implementation module (src/rs/vertebra-core/src/buffer.rs).
[^src-vertebra-core-src-platform-macos-rs]: `src/rs/vertebra-core/src/platform/macos.rs` — Rust implementation module (src/rs/vertebra-core/src/platform/macos.rs).
[^src-vertebra-core-src-tiered-ring-rs]: `src/rs/vertebra-core/src/tiered_ring.rs` — Rust implementation module (src/rs/vertebra-core/src/tiered_ring.rs).
[^src-vertebra-core-src-transport-ring-pair-rs]: `src/rs/vertebra-core/src/transport/ring_pair.rs` — Rust implementation module (src/rs/vertebra-core/src/transport/ring_pair.rs).
[^src-vertebra-core-src-protocol-mod-rs]: `src/rs/vertebra-core/src/protocol/mod.rs` — Rust implementation module (src/rs/vertebra-core/src/protocol/mod.rs).
[^src-vertebra-core-src-protocol-codec-rs]: `src/rs/vertebra-core/src/protocol/codec.rs` — Rust implementation module (src/rs/vertebra-core/src/protocol/codec.rs).
[^src-vertebra-core-src-router-rs]: `src/rs/vertebra-core/src/router.rs` — Rust implementation module (src/rs/vertebra-core/src/router.rs).
[^src-vertebra-http-src-backpressure-rs]: `src/rs/vertebra-http/src/backpressure.rs` — Rust implementation module (src/rs/vertebra-http/src/backpressure.rs).
[^src-vertebra-http-src-server-rs]: `src/rs/vertebra-http/src/server.rs` — Rust implementation module (src/rs/vertebra-http/src/server.rs).
[^src-vertebra-http-src-routing-rs]: `src/rs/vertebra-http/src/routing.rs` — Rust implementation module (src/rs/vertebra-http/src/routing.rs).
[^src-vertebra-http-src-dispatcher-shm-rs]: `src/rs/vertebra-http/src/dispatcher/shm.rs` — Rust implementation module (src/rs/vertebra-http/src/dispatcher/shm.rs).
