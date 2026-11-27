---
type: Guide
title: "Tiered Rings & In-Process Batch Bridge"
description: "RingPair unary and ordered stream framing, TieredRingSet size classes, and bounded in-process and SHM response delivery."
status: stable
tags:
  - architecture
  - ipc
  - tiered-rings
  - batch-bridge
  - rust
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-executor
    at: "2026-11-13T06:06:00Z"
    commit: 05d88d00
  - by: agent:flow-setup
    at: "2026-11-12T23:30:00Z"
    commit: 18baadcd
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: 067fa901
  - by: agent:flow-setup
    at: "2026-11-04T04:33:00Z"
    commit: f741701
sources:
  - id: src-vertebra-http-src-dispatcher-streams-rs
    resource: src/rs/vertebra-http/src/dispatcher/streams.rs
    title: Bounded response stream registry and worker lifecycle cleanup
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
  - id: src-vertebra-core-src-ipc-rs
    resource: src/rs/vertebra-core/src/ipc.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/ipc.rs)"
  - id: src-vertebra-core-src-protocol-codec-rs
    resource: src/rs/vertebra-core/src/protocol/codec.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/protocol/codec.rs)"
  - id: src-vertebra-py-src-transport-rs
    resource: src/rs/vertebra-py/src/transport.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/transport.rs)"
  - id: src-vertebra-http-src-dispatcher-shm-reader-rs
    resource: src/rs/vertebra-http/src/dispatcher/shm_reader.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/dispatcher/shm_reader.rs)"
  - id: src-vertebra-py-src-webtransport-rs
    resource: src/rs/vertebra-py/src/webtransport.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/webtransport.rs)"
---

# Tiered Rings & In-Process Batch Bridge

<!-- truth: start -->
- `BatchSnapshot` backs zero-copy requests across `Vec<InProcessRequest>` (carrying `http::uri::PathAndQuery` directly), borrowed SHM slots (`MpmcSlotView`/`SlotView`), and depth-1 `VQOV`/`VROV` overflow segments, decompressing `flags::COMPRESSED` slabs eagerly (`TransportError::CorruptSegment` on corrupt or nested segments).
- `TieredRingSet` routes unary responses by size; all `VRSF` stream frames share the `xlarge` `SpscRing` so mixed chunk sizes cannot reorder a stream [^src-vertebra-core-src-transport-ring-pair-rs].
- `BatchBridge` delivers batched unary replies through `BatchResponseSink` (with owned `Vec<u8>` handoff via `ResponseSubmitter::submit_batch_flushed`), single replies through `ResponseSink`, `ResponseStreamFrame`s through `StreamSink`, pub/sub events through `PubSubSink`, and embedded WebTransport sessions through `WebTransportSink` [^src-vertebra-core-src-bridge-rs] [^src-vertebra-http-src-dispatcher-streams-rs] [^src-vertebra-py-src-transport-rs].
- Python exposes `submit_stream_frame` and `max_stream_frame_payload` for chunked `VRSF` delivery across ASGI, RSGI, and native streaming handlers [^src-vertebra-py-src-transport-rs].
<!-- truth: end -->

Both runtime topologies move requests as `BatchSnapshot` values. Embedded mode passes `InProcessRequest` snapshots through [`BatchBridge`](../../../../../src/rs/vertebra-core/src/bridge.rs)[^src-vertebra-core-src-bridge-rs]; SHM mode serializes batches into per-worker [`RingPair`](../../../../../src/rs/vertebra-core/src/transport/ring_pair.rs)[^src-vertebra-core-src-transport-ring-pair-rs] slots borrowed zero-copy on the Python side. [`Transport`](../../../../../src/rs/vertebra-core/src/transport/mod.rs)[^src-vertebra-core-src-transport-mod-rs] wraps both backends.

## Request Batches (`transport/mod.rs`)

- **`TransportNode` & `InProcessRequest`**: `TransportNode` (`repr(C)`, 56 B) stores `method_tag`, `http_version_tag`, `handler_id`, `correlation_id`, and slab offset/length pairs. `InProcessRequest` carries Hyper's refcounted `http::uri::PathAndQuery`, compact `headers_mp: Bytes`, and `body: Bytes` directly with zero path string copies, no `DataSlab` copy, and no 1 MiB cap.
- **`BatchSnapshot` Backings**: Holds either `Vec<InProcessRequest>`, a borrowed SHM slot (released on `Drop`), a one-shot `VQOV` overflow segment (unlinked immediately on open), or pooled `(slab, nodes)`. Compressed slots (`flags::COMPRESSED`) decompress eagerly and validate every node against slab bounds, returning `TransportError::CorruptSegment` on invalid data.
- **SHM Framing & Overflow**: Batches serialize a 16-byte header, `TransportNode` array, and byte slab via `try_send_with`. Requests exceeding the 64 KiB slot spill to a named `ShmRegion` (`/vtb-rq-...`) with a compact `VQOV` envelope in the ring. `from_shm_slot_holder_with_depth` rejects nested overflow (`depth >= 1`), invalid path prefixes (must be `/vtb-` or `/tmp/vtb-`, 5–128 B), and truncated segments.

## In-Process `BatchBridge` (`bridge.rs`)

Initialized once via `BatchBridge::init` strictly in `Topology::Embedded`:

- **Request Queue & Dekker Notifier**: `submit_inprocess_batch` pushes `Arc<BatchSnapshot>` onto an unbounded `crossbeam_channel` (bounded at ingress by `BackpressureController` to avoid ABBA deadlocks). Single-loop embedded workers register `loop.add_reader(request_notify_fd, ...)` with `prepare_request_sleep`/`cancel_request_sleep`; multi-thread free-threaded pools poll `get_batch` directly.
- **Batched & Direct Response Completion (`BatchResponseSink` / `ResponseSink`)**: `BatchBridge` registers a lock-free `OnceLock<BatchResponseSink>` (with `RwLock` test override) alongside `ResponseSink`. [`ResponseSubmitter::submit_batch_flushed`](../../../../../src/rs/vertebra-py/src/transport.rs)[^src-vertebra-py-src-transport-rs] hands owned `Vec<u8>` header and body buffers into `BridgeResponse` (`Bytes::from(Vec<u8>)`, zero extra memcpy) and flushes the whole batch through `BatchResponseSink`, allowing `BridgeCorrelation::complete_many` to pop `oneshot::Sender`s per shard in a single lock acquisition and wake Hyper outside shard locks.
- **Streaming, Pub/Sub & WebTransport Sinks**: `StreamSink` delivers `ResponseStreamFrame`s via `ResponseStreamRegistry::deliver_blocking` on a detached producer thread (returning `Closed` if mistakenly called inside Tokio) [^src-vertebra-http-src-dispatcher-streams-rs]. `PubSubSink` delivers `pubsub_publish` straight to `PubSubBroker` (SHM workers use `SHM_PUBSUB_CHANNEL`). `WebTransportSink` forwards `WebTransportSessionHandle` / `WebTransportStreamHandle` to embedded Python handlers ([`webtransport.rs`](../../../../../src/rs/vertebra-py/src/webtransport.rs)[^src-vertebra-py-src-webtransport-rs]).

## Per-Worker `RingPair` & Response Tiers (`ring_pair.rs`, `tiered_ring.rs`)

- **Container-Aware Sizing**: `RingPairConfig::default()` allocates 256 x 64 KiB request slots (16 MiB) + 30 MiB response tiers (46 MiB/worker). `compact()` allocates 64 request slots (4 MiB) + 8 MiB response tiers (12 MiB/worker). `RingPairConfig::for_environment` auto-selects `compact()` when `VERTEBRA_SHM_PROFILE=compact`, cgroup `memory.max <= 1 GiB`, or `/dev/shm` headroom is below `max(128 MiB, workers * 48 MiB)`. Dekker `consumer_sleeping` handshakes (`ShmHeader` ABI v3) cut wake syscalls to near zero under load; overflow segments fall back to `/tmp/vtb-*` when `/dev/shm` is tight or `VERTEBRA_OVERFLOW_BACKEND=tmp`.
- **Response Tiers (`TieredRingSet` in [`tiered_ring.rs`](../../../../../src/rs/vertebra-core/src/tiered_ring.rs)[^src-vertebra-core-src-tiered-ring-rs])**:

| Tier | Slot size | Slots |
|---|---|---|
| `small` | 4 KiB | 512 |
| `medium` | 32 KiB | 128 |
| `large` | 256 KiB | 32 |
| `xlarge` | 2 MiB | 8 |

- **Tier Routing & `VROV` Overflow**: `select_ring` chooses the smallest fitting tier. Unary responses between 2 MiB and 64 MiB (`MAX_OVERFLOW_RESPONSE_SIZE`) spill to a one-shot named segment (`/vtb-rs-...`) with a `VROV` envelope in `small` (unlinked on drain); `> 64 MiB` returns `ResponseTooLarge`. Non-small sends set bits 1..3 in the small ring's `ShmHeader.active_flags` so readers probe larger tiers only when active.
- **`VRES` Unary Frames**: 20-byte header (`"VRES"`, `correlation_id`, `status`, `headers_len`, `body_len`) followed by compact headers and body, parsed zero-copy in `drain_responses_with`.

## Native `VRSF` Stream Frames & Bounded Delivery

- **`VRSF` Wire Format**: 22-byte header (`"VRSF"`, `correlation_id`, `status`, `flags`, `headers_len`, `body_len`) + headers + body. All stream frames use `xlarge` so chunk order is preserved (`max_stream_frame_payload` reports usable payload budget; stream frames do not use `VROV` overflow) [^src-vertebra-core-src-transport-ring-pair-rs].
- **Frame Draining**: `drain_frames_with` yields borrowed `ResponseFrameView`s for both unary and stream frames; the HTTP reader ([`shm_reader.rs`](../../../../../src/rs/vertebra-http/src/dispatcher/shm_reader.rs)[^src-vertebra-http-src-dispatcher-shm-reader-rs]) copies stream chunks into `Bytes` before releasing ring slots. Python uses `submit_stream_frame` and `max_stream_frame_payload` to chunk progressive ASGI, RSGI, and native responses [^src-vertebra-py-src-transport-rs].
- **Stream Lifecycle (`streams.rs`)**: `ResponseStreamRegistry` defaults to 64 queued frames per response. The first nonterminal frame converts the correlation to `PendingResponse::streaming` and exits the 30 s pending-request timeout slab. Final data and valid trailers flush before close; error frames terminate the body cleanly. Worker crashes sweep and close that worker's streams to unblock backpressured senders (`ResponseSlot` in [`ipc.rs`](../../../../../src/rs/vertebra-core/src/ipc.rs)[^src-vertebra-core-src-ipc-rs] is a separate core codec, not the SHM ring wire format) [^src-vertebra-http-src-dispatcher-streams-rs].

Provenance sources: [^src-vertebra-core-src-protocol-codec-rs].

[^src-vertebra-core-src-transport-mod-rs]: `src/rs/vertebra-core/src/transport/mod.rs` — Rust implementation module (src/rs/vertebra-core/src/transport/mod.rs).
[^src-vertebra-core-src-transport-ring-pair-rs]: `src/rs/vertebra-core/src/transport/ring_pair.rs` — Rust implementation module (src/rs/vertebra-core/src/transport/ring_pair.rs).
[^src-vertebra-core-src-tiered-ring-rs]: `src/rs/vertebra-core/src/tiered_ring.rs` — Rust implementation module (src/rs/vertebra-core/src/tiered_ring.rs).
[^src-vertebra-core-src-bridge-rs]: `src/rs/vertebra-core/src/bridge.rs` — Rust implementation module (src/rs/vertebra-core/src/bridge.rs).
[^src-vertebra-core-src-ipc-rs]: `src/rs/vertebra-core/src/ipc.rs` — Rust implementation module (src/rs/vertebra-core/src/ipc.rs).
[^src-vertebra-core-src-protocol-codec-rs]: `src/rs/vertebra-core/src/protocol/codec.rs` — Rust implementation module (src/rs/vertebra-core/src/protocol/codec.rs).
[^src-vertebra-py-src-transport-rs]: `src/rs/vertebra-py/src/transport.rs` — Rust implementation module (src/rs/vertebra-py/src/transport.rs).
[^src-vertebra-http-src-dispatcher-shm-reader-rs]: `src/rs/vertebra-http/src/dispatcher/shm_reader.rs` — Rust implementation module (src/rs/vertebra-http/src/dispatcher/shm_reader.rs).
[^src-vertebra-py-src-webtransport-rs]: `src/rs/vertebra-py/src/webtransport.rs` — Rust implementation module (src/rs/vertebra-py/src/webtransport.rs).
[^src-vertebra-http-src-dispatcher-streams-rs]: `src/rs/vertebra-http/src/dispatcher/streams.rs` — Bounded native response delivery and worker lifecycle cleanup.
