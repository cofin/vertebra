---
type: Guide
title: "Rust IPC, Shared Memory & Ring Transport Domain"
description: "Symbol map of the vertebra-core and vertebra-py types that make up the shared-memory regions, rings, batch transport, and PyO3 transport API."
status: stable
tags:
  - domains
  - ipc
  - shared-memory
  - ring-buffer
  - rust
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
  - id: src-vertebra-core-src-lib-rs
    resource: src/rs/vertebra-core/src/lib.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/lib.rs)"
  - id: src-vertebra-core-src-shm-rs
    resource: src/rs/vertebra-core/src/shm.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/shm.rs)"
  - id: src-vertebra-core-src-buffer-rs
    resource: src/rs/vertebra-core/src/buffer.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/buffer.rs)"
  - id: src-vertebra-core-src-channel-rs
    resource: src/rs/vertebra-core/src/channel.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/channel.rs)"
  - id: src-vertebra-core-src-notify-rs
    resource: src/rs/vertebra-core/src/notify.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/notify.rs)"
  - id: src-vertebra-core-src-sync-rs
    resource: src/rs/vertebra-core/src/sync.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/sync.rs)"
  - id: src-vertebra-core-src-ipc-rs
    resource: src/rs/vertebra-core/src/ipc.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/ipc.rs)"
  - id: src-vertebra-core-src-protocol-mod-rs
    resource: src/rs/vertebra-core/src/protocol/mod.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/protocol/mod.rs)"
  - id: src-vertebra-core-src-protocol-codec-rs
    resource: src/rs/vertebra-core/src/protocol/codec.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/protocol/codec.rs)"
  - id: src-vertebra-core-src-transport-mod-rs
    resource: src/rs/vertebra-core/src/transport/mod.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/transport/mod.rs)"
  - id: src-vertebra-core-src-transport-ring-pair-rs
    resource: src/rs/vertebra-core/src/transport/ring_pair.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/transport/ring_pair.rs)"
  - id: src-vertebra-core-src-tiered-ring-rs
    resource: src/rs/vertebra-core/src/tiered_ring.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/tiered_ring.rs)"
  - id: src-vertebra-core-src-bridge-rs
    resource: src/rs/vertebra-core/src/bridge.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/bridge.rs)"
  - id: src-vertebra-py-src-transport-rs
    resource: src/rs/vertebra-py/src/transport.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/transport.rs)"
  - id: src-vertebra-py-src-request-rs
    resource: src/rs/vertebra-py/src/request.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/request.rs)"
  - id: src-vertebra-core-src-broker-rs
    resource: src/rs/vertebra-core/src/broker.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/broker.rs)"
  - id: src-vertebra-py-src-broker-rs
    resource: src/rs/vertebra-py/src/broker.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/broker.rs)"
  - id: src-vertebra-py-src-webtransport-rs
    resource: src/rs/vertebra-py/src/webtransport.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/webtransport.rs)"
---

# Rust IPC, Shared Memory & Ring Transport Domain

<!-- truth: start -->
- `vertebra-core` owns shared-memory regions (`ShmRegion`), ring layouts (`ShmHeader`, `SpscRing`, `MpmcRing`), `RingPair` / `TieredRingSet`, `BatchBridge`, and cross-platform `Notifier` / `WaitWord` primitives.
- `vertebra-py` (`transport.rs`) binds workers to `BatchBridge` (`inprocess`) or `RingPair::from_env` (`shm`) and exposes GIL-detached `get_batch` and `submit_batch_raw` FFI calls.
<!-- truth: end -->

Where each IPC type lives and what it owns. Behavior, Dekker sleep/wake handshakes, and memory orderings are detailed in [Ring Buffers & Shared Memory](../../architecture/ipc/ring-buffers-and-shm.md) and [Tiered Rings & Batch Bridge](../../architecture/ipc/tiered-rings-and-batch-bridge.md).

## Regions & Rings (`vertebra-core`)

| Symbol | Module | Responsibility |
|---|---|---|
| [`ShmRegion`](../../../../../src/rs/vertebra-core/src/shm.rs)[^src-vertebra-core-src-shm-rs], `ShmHandle`, `ShmError` | `shm` | Mapped region from `memfd_create` (Linux), `shm_open` (`/vtb-*`, Unix fallback), `/tmp/vtb-*` file-backed overflow (`create_file_backed` / `open_file_backed`), or a Windows section; `create`, `map`, `create_named`, `open_named`, `create_overflow_region`, `open_overflow_region`. |
| [`ShmHeader`](../../../../../src/rs/vertebra-core/src/buffer.rs)[^src-vertebra-core-src-buffer-rs], `CacheLine<T>`, `RegionState`, `MAGIC`, `ABI_VERSION` (`3`) | `buffer` | `#[repr(C)]` region header with compile-time size (`1152`/`640`) and alignment assertions: metadata, `generation`, `state`, cache-line-wrapped `read_head`/`write_head`, wait words, Dekker `consumer_sleeping`, and `active_flags` (`prepare_sleep`, `cancel_sleep`, `should_notify_and_clear`). |
| `RingLayout`, `ChannelMode`, `SlotHeader` | `buffer` | Checked header/sequence/slot offsets (`RingLayout::new` via `checked_mul`/`checked_add`, `slots_from_total_size` inverse layout calculation, and `CacheLine<AtomicU64>`-padded MPMC sequence words); 8-byte per-slot `{ len, flags }`. |
| `buffer::BufferPool`, `Buffer`, `BufferError` | `buffer` | Fixed-size buffer leases from a mutex-guarded free list (`acquire`, all-or-nothing `acquire_multi(total_bytes)`). |
| [`SpscRing`](../../../../../src/rs/vertebra-core/src/channel.rs)[^src-vertebra-core-src-channel-rs], `SlotView`, `SpscSender`, `SpscReceiver`, `RingStats` | `channel` | Single-producer ring with checked `attach`/`from_region` layout validation, slot-length bounds checks on `read_slot_to_vec` and views, generation-synchronized cached remote heads, fast-empty `try_recv_view`, zero-copy `try_send_with` / `drain_with`, bounded `Backoff` spin before `park_send`, and futex-style waits. |
| `MpmcRing`, `MpmcSlotView` | `channel` | Vyukov bounded MPMC ring with checked `attach`/`from_region` alignment/bounds validation and cache-line-padded sequence slots; zero-copy `try_send_with`, `try_recv_with`, and borrowed `MpmcSlotView` reads. Used as the SHM request ring. |
| `BufferChannel`, `BufferSender`, `BufferReceiver`, `BufferMessage`, `ChannelError` | `channel` | Buffer lease channel and error variants (`Full`, `Empty`, `PayloadTooLarge`, `BufferTooSmall`, `CorruptedSlot`, `CorruptedSegment`, `SlotBorrowed`, `NotReady`, `InvalidLayout`). |
| [`ShmLeaseTable`, `ShmStatementCache`, `BrokerError`](../../../../../src/rs/vertebra-core/src/broker.rs)[^src-vertebra-core-src-broker-rs] | `broker` | Shared-memory connection lease table (`SHM_PUBSUB_CHANNEL` channel support) and 4-way statement cache (`insert` bounded at `MAX_STUCK_SPINS = 2048` for crashed-writer odd seqlock recovery); `checked_aligned_section` returns `BrokerError::MisalignedRegion { offset, align }` or `RegionTooSmall`. |
| [`Notifier`](../../../../../src/rs/vertebra-core/src/notify.rs)[^src-vertebra-core-src-notify-rs] | `notify` | Inheritable wakeup FD: `eventfd(0, EFD_NONBLOCK)` on Linux, non-blocking pipe elsewhere. |
| [`sync`](../../../../../src/rs/vertebra-core/src/sync.rs)[^src-vertebra-core-src-sync-rs] functions and types | `sync` | `wait_on_address`, `wait_on_address_timeout`, `wake_one`, `wake_all`; `WaitWord`, `SharedMutex`, `SharedRwLock`, `SharedCondvar`. |

## Frames & Flags

| Symbol | Module | Responsibility |
|---|---|---|
| [`ResponseSlot`](../../../../../src/rs/vertebra-core/src/ipc.rs)[^src-vertebra-core-src-ipc-rs], `IpcError`, `FLAG_*` (`u16`) | `ipc` | 16-byte-header response frame with `FLAG_STREAMING`/`FLAG_FINAL`/`FLAG_ERROR`/`FLAG_COMPRESSED`/`FLAG_HEADERS_SENT`/`FLAG_TRAILERS`; `encode_raw_parts_into`. |
| `Opcode` | `ipc` | Message kinds (`AsgiHttp`, `AsgiWebsocket`, `DirectHttp`, `Ws*`, `SseData`, ...). |
| [`RequestHeader`, `ResponseHeader`, `flags`, `response_flags`](../../../../../src/rs/vertebra-core/src/protocol/mod.rs)[^src-vertebra-core-src-protocol-mod-rs] | `protocol` | `u32` protocol flags (`PRIORITY`, `COMPRESSED`, `FIRE_AND_FORGET`, `BATCH`, `OK`, `ERROR`, `STREAMING`, `FINAL`, `HEADERS_SENT`, `TRAILERS`) and `STREAM_START`/`STREAM_CHUNK`/`STREAM_END` combinations. |
| [`CompressionCodec`](../../../../../src/rs/vertebra-core/src/protocol/codec.rs)[^src-vertebra-core-src-protocol-codec-rs] | `protocol::codec` | `None`/`Lz4`/`Zstd`, encoded as `flags::COMPRESSED` plus bits 6-7; applied per sealed batch slab. |
| `RESPONSE_MAGIC`, `ResponseData` | `transport::ring_pair` | 20-byte `VRES` response frame written by `send_response_raw` and drained zero-copy by `drain_responses_with`. |

## Composite Transports

| Symbol | Module | Responsibility |
|---|---|---|
| [`Transport`, `Backend`, `InProcessRequest`, `TransportError`](../../../../../src/rs/vertebra-core/src/transport/mod.rs)[^src-vertebra-core-src-transport-mod-rs] | `transport` | Unified API over `Backend::InProcess` (`BatchBridge`) and `Backend::SharedMemory(Arc<RingPair>)`: `poll_batch`, `try_poll_batch`, `send_response`, `send_response_raw`, `drain_responses_with`, Dekker sleep/wake helpers, and `TransportError` (`ArenaFull`, `SlabFull`, `EmptyBatch`, `Disconnected`, `RingFull`, `ResponseTooLarge`, `DeserializeFailed`, `CorruptSegment`). |
| `TransportNode`, `DataSlab`, `TransportArena`, `BatchSnapshot`, `ShmSlotHolder`, `ShmBatchWriter`, `REQUEST_OVERFLOW_MAGIC` (`VQOV`), `transport::BufferPool` | `transport` | Request descriptors, zero-copy `BatchSnapshot` backings (`Slab`, `InProcess(Vec<InProcessRequest>)`, `ShmSlot(ShmSlotHolder::Mpmc(MpmcSlotView) \| Spsc(SlotView) \| Overflow)` with depth-1 overflow cap and eager `decompress_slab`), compact binary header codec (`encode_headers_compact`, `iter_raw_headers`), and SHM batch/overflow serializer. |
| [`RingPair`, `RingPairConfig`, `RingPairFds`, `RingPairError`, `TieredRingSet`, `RESPONSE_OVERFLOW_MAGIC` (`VROV`)](../../../../../src/rs/vertebra-core/src/transport/ring_pair.rs)[^src-vertebra-core-src-transport-ring-pair-rs] | `transport::ring_pair` | Per-worker request `MpmcRing` + four-tier response `SpscRing`s + notifiers (`RingPairConfig::default` `46 MiB/worker`, `compact` `12 MiB/worker`, `for_environment(worker_count)`); active-tier bitmask ordering (`mark_tier_active` before `write_head` + `has_non_small_data()`), Dekker `prepare_request_sleep`/`prepare_response_sleep`, `VROV` overflow region transfer, and zero-copy `drain_responses_with`. |
| [`TieredRingConfig`, `RingTier`](../../../../../src/rs/vertebra-core/src/tiered_ring.rs)[^src-vertebra-core-src-tiered-ring-rs] | `tiered_ring` | Default tier geometry (`small` 4 KiB×512, `medium` 32 KiB×128, `large` 256 KiB×32, `xlarge` 2 MiB×8; ~30 MiB total), `compact()` geometry (`8 MiB` total), and `select_tier`. |
| [`BatchBridge`, `BatchWriter`, `WriteRequest`, `BridgeResponse`, `ResponseSink`, `PubSubSink`, `WebTransportSessionOps`, `WebTransportStreamOps`, `WebTransportBridgeError`](../../../../../src/rs/vertebra-core/src/bridge.rs)[^src-vertebra-core-src-bridge-rs] | `bridge` | Lock-free unbounded in-process transport for `Topology::Embedded` with zero-copy `submit_inprocess_batch`, `BridgeResponse { request_id, status, headers_raw, body }`, direct `ResponseSink`/`StreamSink`/`PubSubSink`/`WebTransportSink` callbacks, `SHM_PUBSUB_CHANNEL` (`encode_shm_pubsub_frame`), and single-consumer Dekker request `Notifier`. |

[`lib.rs`](../../../../../src/rs/vertebra-core/src/lib.rs)[^src-vertebra-core-src-lib-rs] re-exports `BatchBridge`, `BridgeResponse`, `ResponseSink`, `WriteRequest`, `MpmcSlotView`, `SlotView`, `CacheLine`, `RingPair`, `TieredRingSet`, `BatchSnapshot`, `InProcessRequest`, `TransportError`, `TransportNode`, and the `broker` types at the crate root.

## PyO3 Transport API (`vertebra._vertebra`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`RequestBatch`, `RequestBatchIterator`](../../../../../src/rs/vertebra-py/src/request.rs)[^src-vertebra-py-src-request-rs] | `request.rs` | Sequence view over `Arc<BatchSnapshot>` yielding lazy `Request` proxies (implementing PEP 688 `__buffer__` / anchored `body_view`); `submit_buffers`. |
| [`init_transport`](../../../../../src/rs/vertebra-py/src/transport.rs)[^src-vertebra-py-src-transport-rs] | `transport.rs` | `mode="inprocess"` (default) attaches `BatchBridge`; `mode="shm"` attaches `RingPair::from_env`. |
| `get_batch`, `try_get_batch` | `transport.rs` | Blocking poll with the GIL released, or non-blocking SHM / `BatchBridge` poll. |
| `submit_response_raw`, `submit_batch_raw` | `transport.rs` | Zero-copy raw response/batch writers accepting any contiguous buffer-protocol body (`bytes`, `bytearray`, `memoryview`), writing inline for `<= 64 KiB` and coalescing Dekker `notify_if_sleeping` once per batch. |
| `submit_results`, `submit_response`, `submit_raw_response`, `submit_stream_frame`, `max_stream_frame_payload` | `transport.rs` | `Response` object submission, `VRSF` chunked stream framing, and multi-frame `ResponseSlot` helpers. |
| `get_request_notify_fd`, `clear_request_notification`, `prepare_request_sleep`, `cancel_request_sleep` | `transport.rs` | Expose the request notifier FD and Dekker `SeqCst` sleep/wake handshake for `uvloop` `loop.add_reader` polling across SHM and single-consumer Embedded workers. |
| [`pubsub_publish`](../../../../../src/rs/vertebra-py/src/broker.rs)[^src-vertebra-py-src-broker-rs] | `broker.rs` | Publishes `(topic, payload, idempotency_key)` through `BatchBridge::publish_pubsub` (`Embedded`) or `SHM_PUBSUB_CHANNEL` on `ShmLeaseTable` (`Shm`). |
| [`WebTransportSession`, `WebTransportStream`, `register_webtransport_handler`](../../../../../src/rs/vertebra-py/src/webtransport.rs)[^src-vertebra-py-src-webtransport-rs] | `webtransport.rs` | Async Python session/stream wrappers and embedded handler registration bridging `BatchBridge::set_webtransport_sink`. |

[^src-vertebra-core-src-lib-rs]: `src/rs/vertebra-core/src/lib.rs` — Rust implementation module (src/rs/vertebra-core/src/lib.rs).
[^src-vertebra-core-src-shm-rs]: `src/rs/vertebra-core/src/shm.rs` — Rust implementation module (src/rs/vertebra-core/src/shm.rs).
[^src-vertebra-core-src-buffer-rs]: `src/rs/vertebra-core/src/buffer.rs` — Rust implementation module (src/rs/vertebra-core/src/buffer.rs).
[^src-vertebra-core-src-channel-rs]: `src/rs/vertebra-core/src/channel.rs` — Rust implementation module (src/rs/vertebra-core/src/channel.rs).
[^src-vertebra-core-src-notify-rs]: `src/rs/vertebra-core/src/notify.rs` — Rust implementation module (src/rs/vertebra-core/src/notify.rs).
[^src-vertebra-core-src-sync-rs]: `src/rs/vertebra-core/src/sync.rs` — Rust implementation module (src/rs/vertebra-core/src/sync.rs).
[^src-vertebra-core-src-ipc-rs]: `src/rs/vertebra-core/src/ipc.rs` — Rust implementation module (src/rs/vertebra-core/src/ipc.rs).
[^src-vertebra-core-src-protocol-mod-rs]: `src/rs/vertebra-core/src/protocol/mod.rs` — Rust implementation module (src/rs/vertebra-core/src/protocol/mod.rs).
[^src-vertebra-core-src-protocol-codec-rs]: `src/rs/vertebra-core/src/protocol/codec.rs` — Rust implementation module (src/rs/vertebra-core/src/protocol/codec.rs).
[^src-vertebra-core-src-transport-mod-rs]: `src/rs/vertebra-core/src/transport/mod.rs` — Rust implementation module (src/rs/vertebra-core/src/transport/mod.rs).
[^src-vertebra-core-src-transport-ring-pair-rs]: `src/rs/vertebra-core/src/transport/ring_pair.rs` — Rust implementation module (src/rs/vertebra-core/src/transport/ring_pair.rs).
[^src-vertebra-core-src-tiered-ring-rs]: `src/rs/vertebra-core/src/tiered_ring.rs` — Rust implementation module (src/rs/vertebra-core/src/tiered_ring.rs).
[^src-vertebra-core-src-bridge-rs]: `src/rs/vertebra-core/src/bridge.rs` — Rust implementation module (src/rs/vertebra-core/src/bridge.rs).
[^src-vertebra-py-src-transport-rs]: `src/rs/vertebra-py/src/transport.rs` — Rust implementation module (src/rs/vertebra-py/src/transport.rs).
[^src-vertebra-py-src-request-rs]: `src/rs/vertebra-py/src/request.rs` — Rust implementation module (src/rs/vertebra-py/src/request.rs).
[^src-vertebra-core-src-broker-rs]: `src/rs/vertebra-core/src/broker.rs` — Rust implementation module (src/rs/vertebra-core/src/broker.rs).
[^src-vertebra-py-src-broker-rs]: `src/rs/vertebra-py/src/broker.rs` — Rust implementation module (src/rs/vertebra-py/src/broker.rs).
[^src-vertebra-py-src-webtransport-rs]: `src/rs/vertebra-py/src/webtransport.rs` — Rust implementation module (src/rs/vertebra-py/src/webtransport.rs).
