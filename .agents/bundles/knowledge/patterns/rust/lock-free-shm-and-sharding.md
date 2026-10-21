---
type: Pattern
title: "Lock-Free Shared Memory & Sharded Concurrency"
description: "Rust 2024 lock-free SHM cache-line alignment, SeqCst futex handshakes, bounds-checked unsafe slices, and sharded batch/correlation tables."
status: stable
tags: [patterns, rust, shared-memory, concurrency, sharding]
sources:
  - resource: src/rs/vertebra-core/src/shm.rs
  - resource: src/rs/vertebra-core/src/channel.rs
  - resource: src/rs/vertebra-core/src/ipc.rs
  - resource: src/rs/vertebra-http/src/collector.rs
  - resource: src/rs/vertebra-http/src/correlation.rs
---

# Lock-Free Shared Memory & Sharded Concurrency

All Rust crates target `edition = "2024"` under `-D warnings` (`clippy::all`). Shared-memory and high-concurrency structures in [`vertebra-core`](../../../../../src/rs/vertebra-core/src/lib.rs) and [`vertebra-http`](../../../../../src/rs/vertebra-http/src/lib.rs) enforce cache-line isolation, explicit atomic memory orderings, and sharded lock tables.

## 1. Cache-Line Alignment & `SeqCst` Sleeper-Waker Handshake

Producer cursors, consumer cursors, and futex waiter flags in [`ShmHeader`](../../../../../src/rs/vertebra-core/src/shm.rs) sit on separate 64-byte cache lines (`#[repr(C, align(64))]`). When releasing a zero-copy [`SlotView`](../../../../../src/rs/vertebra-core/src/channel.rs), `read_idx` is published with `Ordering::Release`, followed by an unconditional `Ordering::SeqCst` increment of `wait_read_ptr` before checking `send_waiters`:

<!-- truth: start -->
```rust
    pub fn release(mut self) {
        if !self.released {
            self.released = true;
            let header = self.ring.shm.header();
            header.read_idx.store(self.next_read, Ordering::Release);
            header.wait_read_ptr().fetch_add(1, Ordering::SeqCst);
            if header.send_waiters.load(Ordering::SeqCst) > 0 {
                let _ = platform::futex_wake(header.wait_read_ptr(), 1);
            }
        }
    }
```
<!-- truth: end -->

## 2. Bounds Validation Before `std::slice::from_raw_parts`

Every `unsafe` pointer offset into `ShmRegion` must validate `offset + len <= self.data_len` and verify `slot_len <= max_payload` *before* constructing a slice via `std::slice::from_raw_parts` ([`channel.rs`](../../../../../src/rs/vertebra-core/src/channel.rs), [`ipc.rs`](../../../../../src/rs/vertebra-core/src/ipc.rs), [`tiered_ring.rs`](../../../../../src/rs/vertebra-core/src/tiered_ring.rs)).

## 3. Sharded Hot-Path Synchronization

Avoid single-mutex bottlenecks across Tokio worker threads:

- **[`RequestBatcher`](../../../../../src/rs/vertebra-http/src/collector.rs)**: Shards incoming requests across `NUM_SHARDS = 8` cache-line-aligned `Shard` structs (`#[repr(align(64))]`) indexed by thread ID hash.
- **[`InFlightRequests`](../../../../../src/rs/vertebra-http/src/correlation.rs)**: Shards `65,536` correlation slots across `NUM_SHARDS = 256` independent `Mutex` buckets (`256` slots per shard).
