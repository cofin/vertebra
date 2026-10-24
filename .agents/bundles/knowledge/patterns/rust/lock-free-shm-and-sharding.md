---
type: Pattern
title: "Lock-Free Shared Memory & Sharded Concurrency"
description: "Rust conventions for cache-line isolation, Dekker SeqCst sleeper/waker handshakes, cached SPSC heads, padded MPMC slots, and lock-free sharded dispatch."
status: stable
tags: [patterns, rust, shared-memory, concurrency, sharding]
sources:
  - resource: src/rs/vertebra-core/src/buffer.rs
  - resource: src/rs/vertebra-core/src/channel.rs
  - resource: src/rs/vertebra-core/src/transport/mod.rs
  - resource: src/rs/vertebra-core/src/transport/ring_pair.rs
  - resource: src/rs/vertebra-core/src/bridge.rs
  - resource: src/rs/vertebra-http/src/backpressure.rs
  - resource: src/rs/vertebra-http/src/batcher.rs
  - resource: src/rs/vertebra-http/src/server.rs
  - resource: src/rs/vertebra-http/src/dispatcher/correlation.rs
  - resource: src/rs/vertebra-http/src/dispatcher/shm.rs
  - resource: src/rs/vertebra-http/src/dispatcher/shm_reader.rs
  - resource: src/rs/vertebra-http/src/main.rs
---

# Lock-Free Shared Memory & Sharded Concurrency

All crates use `edition = "2024"`, and `make lint-all` runs `cargo clippy --workspace --all-targets --features tls,compression -- -D warnings`. The patterns below apply to [`vertebra-core`](../../../../../src/rs/vertebra-core/src/lib.rs) rings and the [`vertebra-http`](../../../../../src/rs/vertebra-http/src/lib.rs) dispatch path.

## Cache-Line Isolation (`ABI_VERSION = 3`) & Padded MPMC Sequence Slots

Wrap every cross-process hot atomic in `CacheLine<T>` ([`buffer.rs`](../../../../../src/rs/vertebra-core/src/buffer.rs)) inside the `#[repr(C)]` `ShmHeader` (`ABI_VERSION = 3`, including `consumer_sleeping: CacheLine<AtomicU32>` and `active_flags: CacheLine<AtomicU32>`). `CacheLine` is 64-byte aligned (128-byte on `aarch64` macOS), and [`MpmcRing`](../../../../../src/rs/vertebra-core/src/channel.rs) stores its sequence array as `CacheLine<AtomicU64>` per slot so adjacent Vyukov sequence updates never false-share cache lines across concurrent producers or consumers. Bump `ABI_VERSION` whenever `ShmHeader` or `MpmcRing` layout changes; `attach` rejects mismatched regions.

## `SeqCst` Sleeper/Waker & Dekker `consumer_sleeping` Handshakes

A side that may sleep arms its waiter word (`recv_waiters`/`send_waiters` for futex rings, or `ShmHeader::prepare_sleep()` setting `consumer_sleeping = 1` with `SeqCst` for `eventfd`/pipe-driven `TieredRingSet` and `BatchBridge`), re-checks the ring cursor (`cancel_sleep()` if data arrived), and blocks. The producer must publish its payload and cursor first (`TieredRingSet` sets `mark_tier_active(bit)` *before* `write_head` is published so non-small tiers are never skipped by `has_non_small_data()`), and only then test-and-clear the sleeper flag with `SeqCst` (`should_notify_and_clear()` / `send_waiters().load(Ordering::SeqCst)`). `SlotView::release` in [`channel.rs`](../../../../../src/rs/vertebra-core/src/channel.rs):

<!-- truth: start -->
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
<!-- truth: end -->

`SpscRing` caches remote `read_head` and `write_head` snapshots locally (`cached_read_head`, `cached_write_head`, invalidated when `ShmHeader.generation` advances on `reset()`), checks fast-empty (`read_head == write_head`) before attempting `slot_borrowed.compare_exchange` in `try_recv_view`, and drains bursts with `drain_with` using a single `Release` `read_head` commit per batch.

## Validate SHM-Supplied Lengths & Zero-Copy In-Process Views

Treat every `SlotHeader::len` read from shared memory as untrusted: clamp it to `payload_capacity()` (and to the frame's own declared lengths) before building a slice with `from_raw_parts`. Both `SpscRing::peek_slot` (`try_recv_view` / `drain_with`) and `MpmcRing::try_recv_view` (`MpmcSlotView`) clamp `len` to `payload_capacity()`. For in-process `Topology::Embedded`, [`BatchBridge`](../../../../../src/rs/vertebra-core/src/bridge.rs) passes `Arc<BatchSnapshot>` directly via an unbounded `crossbeam_channel` (bounded upstream by `BackpressureController`) and completes unary responses inline through `ResponseSink` (`DirectResponseSink`) without intermediate channel serialization; see [SHM gotchas](../../gotchas/shm-and-cross-platform-ipc.md).

## Sharded & Lock-Free Hot Paths in `vertebra-http`

- **Collector**: `RequestBatcher::spawn_pool(dispatcher, limit, shard_count, channel_capacity)` ([`batcher.rs`](../../../../../src/rs/vertebra-http/src/batcher.rs)) starts one batcher task per shard (capped to active runtime threads so 1-thread runtimes avoid 8-task scheduling overhead), each maintaining double-buffered `Vec<PendingRequest>` storage (`BatchLimit::default()` = `64`) and draining bursts opportunistically via `try_recv()` with zero timer linger.
- **Correlation & Per-Batch SHM Routing**: `ShmDispatcher::new` ([`dispatcher/shm.rs`](../../../../../src/rs/vertebra-http/src/dispatcher/shm.rs)) splits `max_inflight` across 8 `Mutex<InFlightRequests>` shards (`(generation << 32) | index` ABA-safe keys), routes an entire batch to one target worker (`next_worker.fetch_add(1, Relaxed) % worker_count`, probing forward only if full) using stack-local arrays (zero global scratch `Mutex`), and releases the shard `Mutex` *before* calling `oneshot::Sender::send`.
- **Lock-Free Admission & Circuit Breaker**: [`BackpressureController`](../../../../../src/rs/vertebra-http/src/backpressure.rs) enforces `SeqCst` ordering between `in_flight` and `queue_depth` and tracks failure thresholds with a 16-bucket atomic sliding window (`AtomicU64` `(epoch_sec << 32) | count`) instead of a `Mutex<VecDeque<Instant>>`.
- **Streaming Responders**: `PendingRequests` ([`dispatcher/shm_reader.rs`](../../../../../src/rs/vertebra-http/src/dispatcher/shm_reader.rs)) is a `DashMap<u64, PendingResponder>` (unary `oneshot` or stream `mpsc`), separate from `InFlightRequests`.

Prefer per-shard or stack-local ownership over shared locks when adding hot-path state, and keep shard counts powers of two when indexing with a mask.
