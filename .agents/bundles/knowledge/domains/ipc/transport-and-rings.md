---
type: Guide
title: "Rust IPC, Shared Memory & Ring Transport Domain"
description: "Canonical domain types for Vertebra's Rust shared-memory regions, SPSC/MPMC ring buffers, tiered rings, streaming slots, and BatchBridge."
status: stable
tags: [domains, ipc, shared-memory, ring-buffer, rust]
sources:
  - resource: src/rs/vertebra-core/src/shm.rs
  - resource: src/rs/vertebra-core/src/channel.rs
  - resource: src/rs/vertebra-core/src/ipc.rs
  - resource: src/rs/vertebra-core/src/buffer.rs
  - resource: src/rs/vertebra-core/src/protocol/mod.rs
  - resource: src/rs/vertebra-core/src/bridge.rs
  - resource: src/rs/vertebra-core/src/transport/ring_pair.rs
  - resource: src/rs/vertebra-core/src/tiered_ring.rs
  - resource: src/rs/vertebra-py/src/api.rs
  - resource: src/rs/vertebra-py/src/batch.rs
---

# Rust IPC, Shared Memory & Ring Transport Domain

[`vertebra-core`](../../../../../src/rs/vertebra-core/src/lib.rs) and [`vertebra-py`](../../../../../src/rs/vertebra-py/src/lib.rs) define the shared-memory regions, lock-free ring buffers, tiered wire envelopes, multi-slot buffer pools, and in-process batch transport.

## 1. Shared Memory & Ring Primitives (`vertebra-core`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`ShmRegion`](../../../../../src/rs/vertebra-core/src/shm.rs) / [`ShmHeader`](../../../../../src/rs/vertebra-core/src/shm.rs) | `vertebra_core::shm` | Anonymous (`memfd_create`) or POSIX (`shm_open`) memory-mapped region with a cache-line-aligned `#[repr(C, align(64))]` header (`magic`, `epoch`, `write_idx`, `read_idx`, waiter counters). |
| [`SpscRing`](../../../../../src/rs/vertebra-core/src/channel.rs) / [`SlotView`](../../../../../src/rs/vertebra-core/src/channel.rs) | `vertebra_core::channel` | Lock-free single-producer single-consumer ring supporting zero-copy writes (`try_send_with`), zero-copy borrows (`try_recv_view` $\to$ `SlotView::release`), and `SeqCst` futex/ulock blocking waits. |
| [`MpmcRing`](../../../../../src/rs/vertebra-core/src/ipc.rs) / [`ResponseSlot`](../../../../../src/rs/vertebra-core/src/ipc.rs) | `vertebra_core::ipc` | Lock-free Vyukov bounded MPMC ring over `ShmRegion` and 44-byte `ResponseSlot` supporting `stream_seq` and `FLAG_STREAM_CHUNK` / `FLAG_STREAM_END` / `FLAG_TRAILERS`. |
| [`BufferPool`](../../../../../src/rs/vertebra-core/src/buffer.rs) / [`DataSlab`](../../../../../src/rs/vertebra-core/src/buffer.rs) | `vertebra_core::buffer` | Contiguous slab allocator and lock-free bitmap pool supporting single-slot (`acquire`/`release`) and contiguous multi-slot (`acquire_multi`/`release_multi`) reservations. |
| [`CompressionCodec`](../../../../../src/rs/vertebra-core/src/protocol/codec.rs) | `vertebra_core::protocol::codec` | Per-slot transparent `None`, `Lz4`, or `Zstd` payload compression. |
| [`Notifier`](../../../../../src/rs/vertebra-core/src/notify.rs) | `vertebra_core::notify` | OS wakeup primitive (`eventfd` on Linux, non-blocking `pipe(2)` on macOS) + `futex_wait`/`futex_wake` (`__ulock_wait`/`__ulock_wake` on macOS). |

## 2. Composite Transports (`transport/`, `tiered_ring.rs`, `bridge.rs`, `api.rs`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`RingPair`](../../../../../src/rs/vertebra-core/src/transport/ring_pair.rs) / [`RingPairFds`](../../../../../src/rs/vertebra-core/src/transport/ring_pair.rs) | `vertebra_core::transport::ring_pair` | Paired request `MpmcRing` + 4-tier response `TieredRingSet` and `Notifier`s for a supervised worker subprocess, reconstructible across `exec` via `RingPair::from_env`. |
| [`TieredRingSet`](../../../../../src/rs/vertebra-core/src/tiered_ring.rs) | `vertebra_core::tiered_ring` | 4-tier `SpscRing` response matrix (`512 B`, `4 KiB`, `64 KiB`, `1 MiB`) with automatic spillover promotion, 20-byte `VRES` wire framing, and zero-allocation `send_response_bytes`. |
| [`BatchBridge`](../../../../../src/rs/vertebra-core/src/bridge.rs) / [`BatchSnapshot`](../../../../../src/rs/vertebra-core/src/bridge.rs) | `vertebra_core::bridge` | In-process [`TransportArena`](../../../../../src/rs/vertebra-core/src/bridge.rs) + [`DataSlab`](../../../../../src/rs/vertebra-core/src/buffer.rs) + [`BufferPool`](../../../../../src/rs/vertebra-core/src/buffer.rs) transport for `Topology::Embedded`. |
| [`RequestBatch`](../../../../../src/rs/vertebra-py/src/batch.rs) / [`RequestBatchIterator`](../../../../../src/rs/vertebra-py/src/batch.rs) | `vertebra._vertebra` | PyO3 sequence view over a `BatchSnapshot`, yielding lazy slab-backed `Request` instances and supporting vectorized `submit_buffers`. |
| [`init_transport`](../../../../../src/rs/vertebra-py/src/api.rs) / [`get_batch`](../../../../../src/rs/vertebra-py/src/api.rs) / [`submit_results`](../../../../../src/rs/vertebra-py/src/api.rs) / [`submit_response`](../../../../../src/rs/vertebra-py/src/api.rs) / [`submit_raw_response`](../../../../../src/rs/vertebra-py/src/api.rs) | `vertebra._vertebra` | Vectorized and zero-intermediate-allocation PyO3 transport entrypoints connecting Python workers to `BatchBridge` (`mode="inprocess"`) or `RingPair` (`mode="shm"`). |
