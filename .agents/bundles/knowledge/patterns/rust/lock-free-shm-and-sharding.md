---
type: Pattern
title: "Lock-Free Shared Memory & Sharded Concurrency"
description: "Rust conventions for cache-line isolation, SeqCst sleeper/waker handshakes, reserve-then-rollback atomics, SHM length validation, and sharded batching and correlation."
status: stable
tags: [patterns, rust, shared-memory, concurrency, sharding]
sources:
  - resource: src/rs/vertebra-core/src/buffer.rs
  - resource: src/rs/vertebra-core/src/channel.rs
  - resource: src/rs/vertebra-core/src/transport/mod.rs
  - resource: src/rs/vertebra-core/src/bridge.rs
  - resource: src/rs/vertebra-http/src/batcher.rs
  - resource: src/rs/vertebra-http/src/server.rs
  - resource: src/rs/vertebra-http/src/dispatcher/correlation.rs
  - resource: src/rs/vertebra-http/src/dispatcher/shm.rs
  - resource: src/rs/vertebra-http/src/dispatcher/shm_reader.rs
  - resource: src/rs/vertebra-http/src/main.rs
---

# Lock-Free Shared Memory & Sharded Concurrency

All crates use `edition = "2024"`, and `make lint-all` runs `cargo clippy --workspace --all-targets --features tls,compression -- -D warnings`. The patterns below apply to [`vertebra-core`](../../../../../src/rs/vertebra-core/src/lib.rs) rings and the [`vertebra-http`](../../../../../src/rs/vertebra-http/src/lib.rs) dispatch path.

## Cache-Line Isolation

Wrap every cross-process hot atomic in `CacheLine<T>` ([`buffer.rs`](../../../../../src/rs/vertebra-core/src/buffer.rs)) inside the `#[repr(C)]` `ShmHeader`. `CacheLine` is 64-byte aligned, 128-byte on aarch64 macOS, so the header size differs on Apple Silicon. Bump `ABI_VERSION` whenever the header layout changes; `attach` rejects mismatched regions.

## `SeqCst` Sleeper/Waker Handshake

A side that may sleep increments its waiter counter, re-checks the cursor, and waits on a wait word. The other side must publish its cursor, bump the wait word with `SeqCst`, and only then load the waiter counter with `SeqCst`. With weaker orderings the store and load can reorder and a wake is lost. `SlotView::release` in [`channel.rs`](../../../../../src/rs/vertebra-core/src/channel.rs):

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

`SpscRing::try_send_with` mirrors this on the producer side with `write_head`, `wait_write`, and `recv_waiters`.

## Validate SHM-Supplied Lengths

Treat every `SlotHeader::len` read from shared memory as untrusted: clamp it to `payload_capacity()` (and to the frame's own declared lengths) before building a slice with `from_raw_parts`. Both copying readers and zero-copy readers (`SpscRing::peek_slot` behind `try_recv_view` and `MpmcRing::try_recv_with`) clamp `len` to `payload_capacity()`; see [SHM gotchas](../../gotchas/shm-and-cross-platform-ipc.md).

## Reserve-Then-Rollback Atomics

`DataSlab::write` and `TransportArena::push` ([`transport/mod.rs`](../../../../../src/rs/vertebra-core/src/transport/mod.rs)) reserve space with `fetch_add`, and on overflow undo it with `fetch_sub` before returning `SlabFull` / `ArenaFull`. They assume one writer at a time: `BatchBridge::batch_writer()` holds `write_lock` across pushes and `seal()`, and sealing swaps buffers with `take_and_replace` instead of copying.

## Sharded Hot Paths in `vertebra-http`

- **Collector**: `RequestBatcher::spawn_pool(dispatcher, policy, shard_count, channel_capacity)` ([`batcher.rs`](../../../../../src/rs/vertebra-http/src/batcher.rs)) starts one batcher task per shard, each with its own `mpsc` channel. `main.rs` uses 8 shards with capacity 16384. `ServerState::get_collector` ([`server.rs`](../../../../../src/rs/vertebra-http/src/server.rs)) picks a shard round-robin with an `AtomicUsize`.
- **Correlation**: `ShmDispatcher::new` ([`dispatcher/shm.rs`](../../../../../src/rs/vertebra-http/src/dispatcher/shm.rs)) splits `max_inflight` (65,536 from `main.rs`) across 8 `Mutex<InFlightRequests>` shards of 8,192 slots. [`InFlightRequests`](../../../../../src/rs/vertebra-http/src/dispatcher/correlation.rs) is a slab with a LIFO free list and per-slot generation counters; `insert` returns `(generation << 32) | index` to prevent ABA. A full shard answers 503.
- **Worker and shard selection**: a single `fetch_add` on `next_worker` gives a start index per batch. Request `i` prefers worker `(start + i) % worker_count`, probing forward for a ring with capacity (503 when all are full), and uses correlation shard `(start + i) & shard_mask`.
- **Streaming responders**: `PendingRequests` ([`dispatcher/shm_reader.rs`](../../../../../src/rs/vertebra-http/src/dispatcher/shm_reader.rs)) is a `DashMap<u64, PendingResponder>` (unary `oneshot` or stream `mpsc`), separate from `InFlightRequests`.

Prefer per-shard ownership over a single shared lock when adding new hot-path state, and keep shard counts powers of two when indexing with a mask.
