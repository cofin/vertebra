---
type: Guide
title: "Rust IPC, Shared Memory & Ring Transport Domain"
description: "Symbol map of the vertebra-core and vertebra-py types that make up the shared-memory regions, rings, batch transport, and PyO3 transport API."
status: stable
tags: [domains, ipc, shared-memory, ring-buffer, rust]
sources:
  - resource: src/rs/vertebra-core/src/lib.rs
  - resource: src/rs/vertebra-core/src/shm.rs
  - resource: src/rs/vertebra-core/src/buffer.rs
  - resource: src/rs/vertebra-core/src/channel.rs
  - resource: src/rs/vertebra-core/src/notify.rs
  - resource: src/rs/vertebra-core/src/sync.rs
  - resource: src/rs/vertebra-core/src/ipc.rs
  - resource: src/rs/vertebra-core/src/protocol/mod.rs
  - resource: src/rs/vertebra-core/src/protocol/codec.rs
  - resource: src/rs/vertebra-core/src/transport/mod.rs
  - resource: src/rs/vertebra-core/src/transport/ring_pair.rs
  - resource: src/rs/vertebra-core/src/tiered_ring.rs
  - resource: src/rs/vertebra-core/src/bridge.rs
  - resource: src/rs/vertebra-py/src/transport.rs
  - resource: src/rs/vertebra-py/src/request.rs
---

# Rust IPC, Shared Memory & Ring Transport Domain

Where each IPC type lives and what it is for. Behavior and memory orderings are in [Ring Buffers & Shared Memory](../../architecture/ipc/ring-buffers-and-shm.md) and [Tiered Rings & Batch Bridge](../../architecture/ipc/tiered-rings-and-batch-bridge.md).

## Regions & Rings (`vertebra-core`)

| Symbol | Module | Responsibility |
|---|---|---|
| [`ShmRegion`](../../../../../src/rs/vertebra-core/src/shm.rs), `ShmHandle`, `ShmError` | `shm` | Mapped region from `memfd_create` (Linux), `shm_open` (`/vtb-*`, Unix fallback), or a Windows section; `create`, `map`, `create_named`, `open_named`. |
| [`ShmHeader`](../../../../../src/rs/vertebra-core/src/buffer.rs), `CacheLine<T>`, `RegionState`, `MAGIC`, `ABI_VERSION` | `buffer` | `#[repr(C)]` region header: metadata, `generation`, `state`, cache-line-wrapped `read_head`/`write_head` and wait words. |
| `RingLayout`, `ChannelMode`, `SlotHeader` | `buffer` | Header/sequence/slot offsets for SPSC or MPMC rings; 8-byte per-slot `{ len, flags }`. |
| `buffer::BufferPool`, `Buffer`, `BufferError` | `buffer` | Fixed-size buffer leases from a mutex-guarded free list (`acquire`, all-or-nothing `acquire_multi(total_bytes)`). Test-only. |
| [`SpscRing`](../../../../../src/rs/vertebra-core/src/channel.rs), `SlotView`, `SpscSender`, `SpscReceiver`, `RingStats` | `channel` | Single-producer ring with zero-copy `try_send_with` and borrowed `SlotView` reads; futex-style blocking waits. |
| `MpmcRing` | `channel` | Vyukov bounded MPMC ring with a sequence array; zero-copy `try_send_with` / `try_recv_with`. Used as the SHM request ring. |
| `BufferChannel`, `BufferSender`, `BufferReceiver`, `BufferMessage` | `channel` | Pass `buffer::Buffer` leases through a ring. |
| `ChannelError` | `channel` | `Full`, `Empty`, `PayloadTooLarge`, `BufferTooSmall`, `SlotBorrowed`, `NotReady`, `InvalidLayout`, and wrapped buffer/SHM/sync errors. |
| [`Notifier`](../../../../../src/rs/vertebra-core/src/notify.rs) | `notify` | Inheritable wakeup FD: `eventfd(0, EFD_NONBLOCK)` on Linux, non-blocking pipe elsewhere. |
| [`sync`](../../../../../src/rs/vertebra-core/src/sync.rs) functions and types | `sync` | `wait_on_address`, `wait_on_address_timeout`, `wake_one`, `wake_all`; `WaitWord`, `SharedMutex`, `SharedRwLock`, `SharedCondvar`. |

## Frames & Flags

| Symbol | Module | Responsibility |
|---|---|---|
| [`ResponseSlot`](../../../../../src/rs/vertebra-core/src/ipc.rs), `IpcError`, `FLAG_*` (`u16`) | `ipc` | 16-byte-header response frame with `FLAG_STREAMING`/`FLAG_FINAL`/`FLAG_ERROR`/`FLAG_COMPRESSED`/`FLAG_HEADERS_SENT`/`FLAG_TRAILERS`; `encode_raw_parts_into`. |
| `Opcode` | `ipc` | Message kinds (`AsgiHttp`, `AsgiWebsocket`, `DirectHttp`, `Ws*`, `SseData`, ...). |
| [`RequestHeader`, `ResponseHeader`, `flags`, `response_flags`](../../../../../src/rs/vertebra-core/src/protocol/mod.rs) | `protocol` | `u32` protocol flags (`PRIORITY`, `COMPRESSED`, `FIRE_AND_FORGET`, `BATCH`, `OK`, `ERROR`, `STREAMING`, `FINAL`, `HEADERS_SENT`, `TRAILERS`) and `STREAM_START`/`STREAM_CHUNK`/`STREAM_END` combinations. |
| [`CompressionCodec`](../../../../../src/rs/vertebra-core/src/protocol/codec.rs) | `protocol::codec` | `None`/`Lz4`/`Zstd`, encoded as `flags::COMPRESSED` plus bits 6-7; applied per sealed batch slab. |
| `RESPONSE_MAGIC`, `ResponseData` | `transport::ring_pair` | 20-byte `VRES` response frame written by `send_response` and parsed by `try_recv_response`. |

## Composite Transports

| Symbol | Module | Responsibility |
|---|---|---|
| [`Transport`, `Backend`, `TransportError`](../../../../../src/rs/vertebra-core/src/transport/mod.rs) | `transport` | One API over `Backend::InProcess` (crossbeam receiver) and `Backend::SharedMemory(Arc<RingPair>)`: `poll_batch`, `poll_batch_async`, `send_response`, `send_response_blocking`, `send_response_bytes`, `try_recv_response`, notifier helpers. |
| `TransportNode`, `DataSlab`, `TransportArena`, `BatchSnapshot`, `ShmBatchWriter`, `transport::BufferPool` | `transport` | 56-byte request descriptors, slab and arena write buffers, the sealed batch, its SHM serializer, and the recycled `(slab, nodes)` pairs. |
| [`RingPair`, `RingPairConfig`, `RingPairFds`, `RingPairError`, `TieredRingSet`](../../../../../src/rs/vertebra-core/src/transport/ring_pair.rs) | `transport::ring_pair` | Per-worker request `MpmcRing` + response tiers + notifiers; `create`, `from_inherited`, `from_env`, `fd_set`, `reclaim`. |
| [`TieredRingConfig`, `RingTier`](../../../../../src/rs/vertebra-core/src/tiered_ring.rs) | `tiered_ring` | Tier geometry (`small` 4 KiB×64, `medium` 64 KiB×16, `large` 512 KiB×4, `xlarge` 2 MiB×2) and `select_tier`. |
| [`BatchBridge`, `BatchWriter`, `WriteRequest`, `BridgeResponse`](../../../../../src/rs/vertebra-core/src/bridge.rs) | `bridge` | Process-global in-process transport for `Topology::Embedded`. |

[`lib.rs`](../../../../../src/rs/vertebra-core/src/lib.rs) re-exports `BatchBridge`, `BridgeResponse`, `WriteRequest`, `RingPair`, `TieredRingSet`, `BatchSnapshot`, `TransportError`, and `TransportNode` at the crate root.

## PyO3 Transport API (`vertebra._vertebra`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`RequestBatch`, `RequestBatchIterator`](../../../../../src/rs/vertebra-py/src/request.rs) | `request.rs` | Sequence view over an `Arc<BatchSnapshot>` yielding lazy `Request` proxies; `submit_buffers`. |
| [`init_transport`](../../../../../src/rs/vertebra-py/src/transport.rs) | `transport.rs` | `mode="inprocess"` (default) initializes or reuses `BatchBridge`; `mode="shm"` sets `SHM_TRANSPORT` from `RingPair::from_env`. |
| `get_batch`, `try_get_batch` | `transport.rs` | Blocking poll with the GIL released, or non-blocking SHM poll. |
| `submit_results`, `submit_response` | `transport.rs` | Send `Response` objects as `VRES` frames (SHM) or `BridgeResponse`s (in-process). |
| `submit_raw_response` | `transport.rs` | Encode a buffer-protocol body as `ResponseSlot` frames; writes them only in SHM mode. |
| `get_request_notify_fd`, `clear_request_notification` | `transport.rs` | Expose and drain the SHM request notifier for `asyncio` (`-1` outside SHM mode). |
