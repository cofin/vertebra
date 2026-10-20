---
name: domain-ipc-transport-and-rings
description: Canonical domain types for Vertebra's Rust shared-memory regions, SPSC/MPMC ring buffers, tiered rings, and in-process BatchBridge.
sources:
  - src/rs/vertebra-core/src/shm.rs
  - src/rs/vertebra-core/src/channel.rs
  - src/rs/vertebra-core/src/ipc.rs
  - src/rs/vertebra-core/src/bridge.rs
  - src/rs/vertebra-core/src/transport/ring_pair.rs
  - src/rs/vertebra-core/src/tiered_ring.rs
  - src/rs/vertebra-py/src/api.rs
  - src/rs/vertebra-py/src/batch.rs
---

# Rust IPC, Shared Memory & Ring Transport Domain

[`vertebra-core`](../../../../../src/rs/vertebra-core/src/lib.rs) and [`vertebra-py`](../../../../../src/rs/vertebra-py/src/lib.rs) define the shared-memory regions, lock-free ring buffers, tiered wire envelopes, and in-process batch transport.

## 1. Shared Memory & Ring Primitives (`vertebra-core`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`ShmRegion`](../../../../../src/rs/vertebra-core/src/shm.rs) / [`ShmHeader`](../../../../../src/rs/vertebra-core/src/shm.rs) | `vertebra_core::shm` | Anonymous (`memfd_create`) or POSIX (`shm_open`) memory-mapped region with a cache-line-aligned `#[repr(C, align(64))]` header (`magic`, `epoch`, `write_idx`, `read_idx`, waiter counters). |
| [`SpscRing`](../../../../../src/rs/vertebra-core/src/channel.rs) / [`SlotView`](../../../../../src/rs/vertebra-core/src/channel.rs) | `vertebra_core::channel` | Lock-free single-producer single-consumer ring supporting zero-copy writes (`try_send_with`), zero-copy borrows (`try_recv_view` $\to$ `SlotView::release`), and `SeqCst` futex/ulock blocking waits. |
| [`MpmcRing`](../../../../../src/rs/vertebra-core/src/ipc.rs) | `vertebra_core::ipc` | Lock-free Vyukov bounded MPMC ring over `ShmRegion` with per-slot atomic `sequence` numbers and in-place `try_recv_with` reads. |
| [`CompressionCodec`](../../../../../src/rs/vertebra-core/src/protocol/codec.rs) | `vertebra_core::protocol::codec` | Per-slot transparent `None`, `Lz4`, or `Zstd` payload compression. |
| [`Notifier`](../../../../../src/rs/vertebra-core/src/notify.rs) | `vertebra_core::notify` | OS wakeup primitive (`eventfd` on Linux, non-blocking `pipe(2)` on macOS) + `futex_wait`/`futex_wake` (`__ulock_wait`/`__ulock_wake` on macOS). |

## 2. Composite Transports (`transport/`, `tiered_ring.rs`, `bridge.rs`, `api.rs`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`RingPair`](../../../../../src/rs/vertebra-core/src/transport/ring_pair.rs) / [`RingPairFds`](../../../../../src/rs/vertebra-core/src/transport/ring_pair.rs) | `vertebra_core::transport::ring_pair` | Paired request `MpmcRing` + 4-tier response `TieredRingSet` and `Notifier`s for a supervised worker subprocess, reconstructible across `exec` via `RingPair::from_env`. |
| [`TieredRingSet`](../../../../../src/rs/vertebra-core/src/tiered_ring.rs) | `vertebra_core::tiered_ring` | 4-tier `SpscRing` response matrix (`512 B`, `4 KiB`, `64 KiB`, `1 MiB`) with automatic spillover promotion and 20-byte `VRES` wire framing. |
| [`BatchBridge`](../../../../../src/rs/vertebra-core/src/bridge.rs) / [`BatchSnapshot`](../../../../../src/rs/vertebra-core/src/bridge.rs) | `vertebra_core::bridge` | In-process [`TransportArena`](../../../../../src/rs/vertebra-core/src/bridge.rs) + [`DataSlab`](../../../../../src/rs/vertebra-core/src/buffer.rs) + [`BufferPool`](../../../../../src/rs/vertebra-core/src/buffer.rs) transport for `Topology::Embedded`. |
| [`RequestBatch`](../../../../../src/rs/vertebra-py/src/batch.rs) / [`RequestBatchIterator`](../../../../../src/rs/vertebra-py/src/batch.rs) | `vertebra._vertebra` | PyO3 sequence view over a `BatchSnapshot`, yielding lazy slab-backed `Request` instances without copying headers or bodies. |
| [`init_transport`](../../../../../src/rs/vertebra-py/src/api.rs) / [`get_batch`](../../../../../src/rs/vertebra-py/src/api.rs) / [`submit_results`](../../../../../src/rs/vertebra-py/src/api.rs) / [`submit_response`](../../../../../src/rs/vertebra-py/src/api.rs) | `vertebra._vertebra` | Vectorized PyO3 transport entrypoints connecting Python workers to `BatchBridge` (`mode="inprocess"`) or `RingPair` (`mode="shm"`). |
