---
type: Pattern
title: "Lock-Free Shared Memory & Sharded Concurrency"
description: "Rust conventions for cache-line isolation, Dekker SeqCst sleeper/waker handshakes, cached SPSC heads, padded MPMC slots, and lock-free sharded dispatch."
status: stable
tags:
  - patterns
  - rust
  - shared-memory
  - concurrency
  - sharding
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
  - id: src-vertebra-core-src-buffer-rs
    resource: src/rs/vertebra-core/src/buffer.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/buffer.rs)"
  - id: src-vertebra-core-src-channel-rs
    resource: src/rs/vertebra-core/src/channel.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/channel.rs)"
  - id: src-vertebra-core-src-transport-mod-rs
    resource: src/rs/vertebra-core/src/transport/mod.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/transport/mod.rs)"
  - id: src-vertebra-core-src-transport-ring-pair-rs
    resource: src/rs/vertebra-core/src/transport/ring_pair.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/transport/ring_pair.rs)"
  - id: src-vertebra-core-src-bridge-rs
    resource: src/rs/vertebra-core/src/bridge.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/bridge.rs)"
  - id: src-vertebra-http-src-backpressure-rs
    resource: src/rs/vertebra-http/src/backpressure.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/backpressure.rs)"
  - id: src-vertebra-http-src-batcher-rs
    resource: src/rs/vertebra-http/src/batcher.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/batcher.rs)"
  - id: src-vertebra-http-src-server-rs
    resource: src/rs/vertebra-http/src/server.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/server.rs)"
  - id: src-vertebra-http-src-dispatcher-correlation-rs
    resource: src/rs/vertebra-http/src/dispatcher/correlation.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/dispatcher/correlation.rs)"
  - id: src-vertebra-http-src-dispatcher-shm-rs
    resource: src/rs/vertebra-http/src/dispatcher/shm.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/dispatcher/shm.rs)"
  - id: src-vertebra-http-src-dispatcher-shm-reader-rs
    resource: src/rs/vertebra-http/src/dispatcher/shm_reader.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/dispatcher/shm_reader.rs)"
  - id: src-vertebra-http-src-main-rs
    resource: src/rs/vertebra-http/src/main.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/main.rs)"
---

# Lock-Free Shared Memory & Sharded Concurrency

<!-- truth: start -->
- Wrap every cross-process hot atomic in `CacheLine<T>` (`64 B`, or `128 B` on `aarch64` macOS) inside `#[repr(C)]` `ShmHeader` (`ABI_VERSION = 3`) and `MpmcRing` sequence slots.
- Maintain `Ordering::SeqCst` barriers across Dekker sleeper/waker handshakes (`send_waiters`, `consumer_sleeping`, `mark_tier_active`, and `BackpressureController` counters).
- Clamp every untrusted `SlotHeader::len` read from shared memory to `payload_capacity()` before constructing slices.
- Partition ingress batching and correlation across power-of-two shards (`RequestBatcher::spawn_pool`, 8 `InFlightRequests` shards) and release shard locks before waking responders.
<!-- truth: end -->

All crates use `edition = "2024"`, and `make lint-all` runs `cargo clippy --workspace --all-targets --features tls,compression,gcp -- -D warnings`. The patterns below apply to [`vertebra-core`](../../../../../src/rs/vertebra-core/src/lib.rs) rings and the [`vertebra-http`](../../../../../src/rs/vertebra-http/src/lib.rs) dispatch path.

## Cache-Line Isolation (`ABI_VERSION = 3`) & Padded MPMC Sequence Slots

Wrap every cross-process hot atomic in `CacheLine<T>` ([`buffer.rs`](../../../../../src/rs/vertebra-core/src/buffer.rs)[^src-vertebra-core-src-buffer-rs]) inside the `#[repr(C)]` `ShmHeader` (`ABI_VERSION = 3`, including `consumer_sleeping: CacheLine<AtomicU32>` and `active_flags: CacheLine<AtomicU32>`). `CacheLine` is 64-byte aligned (128-byte on `aarch64` macOS), and [`MpmcRing`](../../../../../src/rs/vertebra-core/src/channel.rs)[^src-vertebra-core-src-channel-rs] stores its sequence array as `CacheLine<AtomicU64>` per slot so adjacent Vyukov sequence updates never false-share cache lines across concurrent producers or consumers. Bump `ABI_VERSION` whenever `ShmHeader` or `MpmcRing` layout changes; `attach` rejects mismatched regions.

## `SeqCst` Sleeper/Waker & Dekker `consumer_sleeping` Handshakes

A side that may sleep arms its waiter word (`recv_waiters`/`send_waiters` for futex rings, or `ShmHeader::prepare_sleep()` setting `consumer_sleeping = 1` with `SeqCst` for `eventfd`/pipe-driven `TieredRingSet` and `BatchBridge`), re-checks the ring cursor (`cancel_sleep()` if data arrived), and blocks. The producer must publish its payload and cursor first (`TieredRingSet` sets `mark_tier_active(bit)` *before* `write_head` is published so non-small tiers are never skipped by `has_non_small_data()`), and only then test-and-clear the sleeper flag with `SeqCst` (`should_notify_and_clear()` / `send_waiters().load(Ordering::SeqCst)`). `SlotView::release` in [`channel.rs`](../../../../../src/rs/vertebra-core/src/channel.rs):

```rust
    pub fn release(&mut self) {
        if self.released {
            return;
        }
        self.released = true;

        // Advance read head to release the slot
        let next = self.slot_index.wrapping_add(1) & self.ring.mask;
        self.ring.read_head().store(next, Ordering::Release);
        self.ring.wait_read_ptr().fetch_add(1, Ordering::SeqCst);
        if self.ring.send_waiters().load(Ordering::SeqCst) > 0 {
            let _ = crate::sync::wake_one(self.ring.wait_read_ptr());
        }

        // Clear the borrowed flag to allow new borrows
        self.ring.slot_borrowed.store(false, Ordering::Release);
    }
```

`SpscRing` caches remote `read_head` and `write_head` snapshots locally (`cached_read_head`, `cached_write_head`, invalidated when `ShmHeader.generation` advances on `reset()`), checks fast-empty (`read_head == write_head`) before attempting `slot_borrowed.compare_exchange` in `try_recv_view`, and drains bursts with `drain_with` using a single `Release` `read_head` commit per batch. In [`Transport::poll_batch`](../../../../../src/rs/vertebra-core/src/transport/mod.rs)[^src-vertebra-core-src-transport-mod-rs], always drop the `RequestSleepGuard` (`cancel_request_sleep()`, setting `consumer_sleeping = 0`) *before* calling `request_notifier.clear()` so a concurrent producer calling `notify_request_if_sleeping()` between wakeup and `clear()` is never swallowed.

## Validate SHM-Supplied Lengths & Zero-Copy In-Process Views

Treat every `SlotHeader::len` read from shared memory as untrusted: clamp it to `payload_capacity()` (and to the frame's own declared lengths) before building a slice with `from_raw_parts`. Both `SpscRing::peek_slot` (`try_recv_view` / `drain_with`) and `MpmcRing::try_recv_view` (`MpmcSlotView`) clamp `len` to `payload_capacity()`. For in-process `Topology::Embedded`, [`BatchBridge`](../../../../../src/rs/vertebra-core/src/bridge.rs)[^src-vertebra-core-src-bridge-rs] passes `Arc<BatchSnapshot>` directly via an unbounded `crossbeam_channel` (bounded upstream by `BackpressureController`) and completes unary responses inline through `ResponseSink` (`DirectResponseSink`) without intermediate channel serialization; see [SHM gotchas](../../gotchas/shm-and-cross-platform-ipc.md).

## Sharded & Lock-Free Hot Paths in `vertebra-http`

- **Collector**: `RequestBatcher::spawn_pool(dispatcher, limit, shard_count, channel_capacity)` ([`batcher.rs`](../../../../../src/rs/vertebra-http/src/batcher.rs)[^src-vertebra-http-src-batcher-rs]) starts one batcher task per shard (`config.workers.count.clamp(1, 8)` in `Topology::Shm` and `config.worker_threads.clamp(1, 8)` in `Topology::Embedded`), tagging each batcher with `shard_idx: Option<usize>`, maintaining double-buffered `Vec<PendingRequest>` storage (`BatchLimit::default()` = `64`), draining bursts via `try_recv()`, and gating `tokio::task::yield_now().await` on `dispatcher.in_flight() > 0 || !rx.is_empty()` so isolated `c=1` requests flush with zero yield delay.
- **Correlation & 1:1 Shard-Sticky SHM Routing**: `ShmDispatcher::new` ([`dispatcher/shm.rs`](../../../../../src/rs/vertebra-http/src/dispatcher/shm.rs)[^src-vertebra-http-src-dispatcher-shm-rs]) splits `max_inflight` across 8 `Mutex<InFlightRequests>` shards (`(generation << 32) | index` ABA-safe keys), binds each `RequestBatcher` shard `i` to home worker `i % worker_count` and correlation shard `worker_idx & SHM_CORRELATION_MASK` (sticking to the warm MRU worker when `prev_active == 0` and spilling to the least-loaded healthy worker when `home` is unhealthy, full, or `in_flight >= 64`), and releases the shard `Mutex` *before* calling `oneshot::Sender::send`.
- **Lock-Free Admission & Circuit Breaker**: [`BackpressureController`](../../../../../src/rs/vertebra-http/src/backpressure.rs)[^src-vertebra-http-src-backpressure-rs] enforces `SeqCst` ordering between `in_flight` and `queue_depth` and tracks failure thresholds with a 16-bucket atomic sliding window (`AtomicU64` `(epoch_sec << 32) | count`) instead of a `Mutex<VecDeque<Instant>>`.
- **Streaming Responders**: `PendingRequests` ([`dispatcher/shm_reader.rs`](../../../../../src/rs/vertebra-http/src/dispatcher/shm_reader.rs)[^src-vertebra-http-src-dispatcher-shm-reader-rs]) is a `DashMap<u64, PendingResponder>` (unary `oneshot` or stream `mpsc`), separate from `InFlightRequests`.

Prefer per-shard or stack-local ownership over shared locks when adding hot-path state, and keep shard counts powers of two when indexing with a mask.

Provenance sources: [^src-vertebra-core-src-transport-ring-pair-rs], [^src-vertebra-http-src-server-rs], [^src-vertebra-http-src-dispatcher-correlation-rs], [^src-vertebra-http-src-main-rs].

[^src-vertebra-core-src-buffer-rs]: `src/rs/vertebra-core/src/buffer.rs` — Rust implementation module (src/rs/vertebra-core/src/buffer.rs).
[^src-vertebra-core-src-channel-rs]: `src/rs/vertebra-core/src/channel.rs` — Rust implementation module (src/rs/vertebra-core/src/channel.rs).
[^src-vertebra-core-src-transport-mod-rs]: `src/rs/vertebra-core/src/transport/mod.rs` — Rust implementation module (src/rs/vertebra-core/src/transport/mod.rs).
[^src-vertebra-core-src-transport-ring-pair-rs]: `src/rs/vertebra-core/src/transport/ring_pair.rs` — Rust implementation module (src/rs/vertebra-core/src/transport/ring_pair.rs).
[^src-vertebra-core-src-bridge-rs]: `src/rs/vertebra-core/src/bridge.rs` — Rust implementation module (src/rs/vertebra-core/src/bridge.rs).
[^src-vertebra-http-src-backpressure-rs]: `src/rs/vertebra-http/src/backpressure.rs` — Rust implementation module (src/rs/vertebra-http/src/backpressure.rs).
[^src-vertebra-http-src-batcher-rs]: `src/rs/vertebra-http/src/batcher.rs` — Rust implementation module (src/rs/vertebra-http/src/batcher.rs).
[^src-vertebra-http-src-server-rs]: `src/rs/vertebra-http/src/server.rs` — Rust implementation module (src/rs/vertebra-http/src/server.rs).
[^src-vertebra-http-src-dispatcher-correlation-rs]: `src/rs/vertebra-http/src/dispatcher/correlation.rs` — Rust implementation module (src/rs/vertebra-http/src/dispatcher/correlation.rs).
[^src-vertebra-http-src-dispatcher-shm-rs]: `src/rs/vertebra-http/src/dispatcher/shm.rs` — Rust implementation module (src/rs/vertebra-http/src/dispatcher/shm.rs).
[^src-vertebra-http-src-dispatcher-shm-reader-rs]: `src/rs/vertebra-http/src/dispatcher/shm_reader.rs` — Rust implementation module (src/rs/vertebra-http/src/dispatcher/shm_reader.rs).
[^src-vertebra-http-src-main-rs]: `src/rs/vertebra-http/src/main.rs` — Rust implementation module (src/rs/vertebra-http/src/main.rs).
