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
    commit: 1575427f
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
---

# Tiered Rings & In-Process Batch Bridge

<!-- truth: start -->
- `BatchSnapshot` unifies zero-copy request backing across in-process `Vec<InProcessRequest>`, borrowed SHM ring slots (`MpmcSlotView` / `SlotView`), and file-backed `VQOV`/`VROV` overflow segments.
- `TieredRingSet` routes unary responses by size; all flagged `VRSF` stream frames share the xlarge `SpscRing` so different body sizes cannot reorder a stream [^src-vertebra-core-src-transport-ring-pair-rs].
- `BatchBridge` carries native `ResponseStreamFrame`s through a dedicated `StreamSink`; bounded registry channels report client disconnects and apply blocking or awaited backpressure [^src-vertebra-core-src-bridge-rs] [^src-vertebra-http-src-dispatcher-streams-rs].
- Python exposes `submit_stream_frame` and `max_stream_frame_payload` for chunked `VRSF` delivery; mounted ASGI, RSGI, and native `(status, headers, AsyncIterable | Iterable)` handlers flush progressive chunks once response headers start streaming [^src-vertebra-py-src-transport-rs].
<!-- truth: end -->

Both topologies move requests as `BatchSnapshot`s. Embedded mode hands zero-copy `InProcessRequest` snapshots across threads through [`BatchBridge`](../../../../../src/rs/vertebra-core/src/bridge.rs)[^src-vertebra-core-src-bridge-rs] and completes unary responses through `ResponseSink` or progressive responses through `StreamSink`. Shm mode serializes batches directly into a per-worker [`RingPair`](../../../../../src/rs/vertebra-core/src/transport/ring_pair.rs)[^src-vertebra-core-src-transport-ring-pair-rs] and borrows the slot zero-copy on the Python side (`MpmcSlotView` / `SlotView`). [`Transport`](../../../../../src/rs/vertebra-core/src/transport/mod.rs)[^src-vertebra-core-src-transport-mod-rs] wraps either backend cleanly.

## Request Batches (`transport/mod.rs`)

- **`TransportNode`** (`repr(C)`, 56 bytes): `method_tag`, `http_version_tag`, `handler_id: u64` (FNV-1a route id or `MOUNT_HANDLER_BASE | mount_idx`), `correlation_id: u64`, and `u32` offset/length pairs for path, query, headers, and body.
- **`InProcessRequest`**: zero-copy in-process request record holding ref-counted `Bytes` slices (`path`, `query`, `headers_raw`, `body`, `method_tag`, `http_version_tag`, `handler_id`, `correlation_id`) with zero `DataSlab` copying and zero `1 MiB` batch size cap.
- **`BatchSnapshot`**: supports multiple zero-copy backings—in-process `Vec<InProcessRequest>`, borrowed SHM `MpmcSlotView` / `SlotView` (releasing the ring slot on `Drop` without copying the slot onto the Python heap), one-shot named `ShmRegion` overflow segments (`ShmSlotHolder::Overflow` on Unix when `flags == REQUEST_OVERFLOW_MAGIC` `0x56_51_4F_56` `"VQOV"`, immediately unlinked on open), or pooled `(slab, nodes)` pairs (`DataSlab` + `TransportArena` + `BufferPool`, with optional LZ4/Zstd slab decompression under `compression`).
- **SHM framing**: `BatchSnapshot::serialize_into` and `ShmDispatcher` write a 16-byte batch header (`flags`, `node_count`, `slab_len`, padding), the `TransportNode` array, and the contiguous byte slab directly into one request-ring slot via `try_send_with`. When a single request exceeds the 64 KiB request ring slot capacity on Unix, `ShmBatchWriter::write_overflow_envelope` (and `BatchSnapshot::write_overflow_envelope`) writes the full serialized batch into a named `ShmRegion` (`/vtb-rq-...`) and sends a compact `16 + name_len` byte `VQOV` envelope in the request ring slot.
- Header helpers: `encode_headers_compact`, `iter_raw_headers`, `decode_headers_raw`.

## In-Process `BatchBridge` (`bridge.rs`)

- `static BRIDGE: OnceLock<BatchBridge>` is initialized once by `BatchBridge::init` **only** when running in `Topology::Embedded`.
- **Lock-free request queue**: `RequestBatcher` shards call `BatchBridge::submit_inprocess_batch(Vec<InProcessRequest>)`, pushing `Arc<BatchSnapshot>` onto an unbounded `crossbeam_channel` (bounded at ingress by `BackpressureController` `max_in_flight`, eliminating bounded-channel ABBA deadlocks between Tokio workers and Python). Legacy `BatchWriter::push_request` callers auto-seal on `SlabFull`/`ArenaFull`.
- **Single-consumer Dekker request `Notifier`**: `BatchBridge` embeds a `Notifier` (`request_notify_fd`, `consumer_sleeping: AtomicU32`, `consumer_count: AtomicUsize`). When `consumer_count <= 1`, single-loop embedded `uvloop` workers register `loop.add_reader(request_notify_fd, ...)` and bracket idle waits with `prepare_request_sleep` / `cancel_request_sleep` so producers signal the FD only when the loop is sleeping. When `consumer_count > 1` (free-threaded multi-thread pools), workers poll `get_batch` directly.
- **Direct response completion (`ResponseSink`)**: `BridgeResponse` carries `(request_id: u64, status: u16, headers_raw: Bytes, body: Bytes)` with zero `Vec<(Vec<u8>, Vec<u8>)>` header transcoding. When `vertebra-http` registers a `ResponseSink` (`OnceLock<ResponseSink>`), `submit_response_raw` and `submit_batch_raw` complete the sharded `InFlightRequests` `oneshot::Sender` directly on the submitting thread without an intermediate `mpsc` channel or `spawn_response_reader` task (falling back to an unbounded `mpsc` response channel in standalone bridge tests).

- **Progressive response delivery (`StreamSink`)**: `ResponseStreamFrame` owns the request ID, status, flags, compact headers, and a body `Bytes`. `BatchBridge::submit_stream_frame` invokes a replaceable sink outside its read lock and returns `StreamDelivery::{Accepted, Closed}`. No sink returns `TransportError::Disconnected`. `BridgeCorrelation` registers a weakly held sink that calls `ResponseStreamRegistry::deliver_blocking` on a detached producer thread; calling this blocking path inside a Tokio runtime returns `Closed` rather than blocking the runtime [^src-vertebra-core-src-bridge-rs] [^src-vertebra-http-src-dispatcher-streams-rs].

## Per-Worker `RingPair` (`transport/ring_pair.rs`)

`RingPair` holds `request_ring: Arc<MpmcRing>`, `response_rings: TieredRingSet`, `request_notifier`, `response_notifier`, and its `RingPairConfig`. `RingPairConfig::default()` uses 256 request slots of 64 KiB (`16 MiB`) and `TieredRingConfig::default()` (`30 MiB`, `46 MiB/worker` total), while `RingPairConfig::compact()` uses 64 request slots (`4 MiB`) and `TieredRingConfig::compact()` (`8 MiB`, `12 MiB/worker` total). `RingPairConfig::for_environment(worker_count)` automatically selects `compact()` when `VERTEBRA_SHM_PROFILE=compact`, container cgroup `memory.max <= 1 GiB`, or `/dev/shm` available space is below `max(128 MiB, worker_count * 48 MiB)`, and `RingPair::from_env` reconstructs slot counts dynamically from `VERTEBRA_REQ_RING_SIZE` and `VERTEBRA_RESP_RING_*_SIZE` via `RingLayout::slots_from_total_size`. `ShmHeader` (`ABI_VERSION = 3`) `consumer_sleeping` Dekker handshakes (`prepare_request_sleep`, `cancel_request_sleep`, `notify_request_if_sleeping`, `prepare_response_sleep`, `cancel_response_sleep`, `notify_response_if_sleeping`) coalesce `eventfd`/`pipe` wakeups to ~0 syscalls under sustained load. When a single request or response overflows ring slot capacity on Unix and `/dev/shm` lacks headroom (or `VERTEBRA_OVERFLOW_BACKEND=tmp`), `create_overflow_region` falls back from POSIX `shm_open` to `/tmp/vtb-*` `mmap` files.

### Response Tiers

`TieredRingSet` (in `ring_pair.rs`) holds one `SpscRing` per size class from [`TieredRingConfig`](../../../../../src/rs/vertebra-core/src/tiered_ring.rs)[^src-vertebra-core-src-tiered-ring-rs]:

| Tier | Slot size | Slots |
|---|---|---|
| `small` | 4 KiB | 512 |
| `medium` | 32 KiB | 128 |
| `large` | 256 KiB | 32 |
| `xlarge` | 2 MiB | 8 |

`select_ring(total_size)` returns the smallest tier with `total_size <= payload_capacity()`. On Unix, responses exceeding `xlarge.payload_capacity()` (`> 2 MiB`, up to `MAX_OVERFLOW_RESPONSE_SIZE = 64 MiB`) spill into a one-shot named `ShmRegion` (`/vtb-rs-...`) via `send_overflow_response_frame` and publish a compact `RESPONSE_OVERFLOW_MAGIC` (`0x56_52_4F_56`, `"VROV"`) envelope into the `small` ring (`ResponseData::with_overflow_slice` opens and immediately unlinks the segment on drain). Responses exceeding `64 MiB` return `Err(RingPairError::ResponseTooLarge)`. Before publishing `write_head` on a non-small tier, the sender sets that tier's bit (`bit 1..3`) in the small ring's `ShmHeader.active_flags` (`mark_tier_active`, clearing if empty on send error). Consumers drain `small` first via zero-copy `drain_responses_with` and only probe `medium`/`large`/`xlarge` when `active_tier_mask()` or `has_non_small_data()` (`ring.has_data_acquire()`) indicates pending frames.

### `VRES` Response Frames

`send_response` writes a 20-byte header followed by compact binary headers and body: `magic: u32` (`RESPONSE_MAGIC = 0x5652_4553`, "VRES"), `correlation_id: u64`, `status: u16`, `headers_len: u16`, `body_len: u32`. `drain_responses_with` parses `HeaderMap` and `Bytes` directly from the borrowed SHM slot slice without intermediate `Vec<u8>` copies.

## Native `VRSF` Stream Frames

`send_stream_frame` writes a 22-byte header: `magic: u32` (`RESPONSE_STREAM_MAGIC = 0x5652_5346`, "VRSF"), `correlation_id: u64`, `status: u16`, `flags: u16`, `headers_len: u16`, and `body_len: u32`, followed by compact headers and body. All stream frames use the xlarge ring regardless of size, preserving first/body/final order across small and large chunks. `max_stream_frame_payload` reports that tier's payload budget after the frame header; callers account for header bytes when chunking. Oversized frames return `ResponseTooLarge`; stream frames do not use unary overflow envelopes [^src-vertebra-core-src-transport-ring-pair-rs].

`drain_frames_with` exposes a borrowed `ResponseFrameView` for unary (`flags == FLAG_FINAL` without `FLAG_STREAMING`) and stream frames. Unary `VRES`/`VROV` framing and the legacy unary drain callback remain available. The HTTP reader copies headers and body into owned `Bytes` before releasing ring slots and awaiting stream-channel capacity; unary completions keep their batching path. `Transport` wraps stream send, blocking send, and payload-budget access for SHM callers [^src-vertebra-core-src-transport-ring-pair-rs] [^src-vertebra-core-src-transport-mod-rs] [^src-vertebra-http-src-dispatcher-shm-reader-rs].

## Bounded HTTP Stream Delivery

`ResponseStreamRegistry` defaults to 64 queued frames per response. A first nonterminal frame completes the correlation as `PendingResponse::streaming`; a terminal first frame stays unary. Continuations use blocking sends for detached bridge producers or awaited sends for SHM readers. Final data and trailers are delivered before the sender closes; an error frame ends the body without a transport error because the body receiver is `Infallible`. Invalid trailer fields are skipped [^src-vertebra-http-src-dispatcher-streams-rs].

The first frame leaves the request timeout slab, so long-lived streams do not inherit the 30-second pending-request timeout. Closed receivers are reclaimed by dispatcher sweeps. SHM streams retain their worker ownership: a worker sweep closes its streams and wakes backpressured senders, while response-reader shutdown cancels delivery rather than waiting indefinitely on a full client channel. The unused `PendingRequests`/`handle_raw_response_slot` dispatcher path is absent; `ResponseSlot` remains a separate core codec and is not the SHM response-ring format [^src-vertebra-http-src-dispatcher-streams-rs] [^src-vertebra-http-src-dispatcher-shm-reader-rs] [^src-vertebra-core-src-ipc-rs].

Python invokes `submit_stream_frame` and queries `max_stream_frame_payload` to split oversized header-plus-body payloads into ordered `VRSF` chunks; `AsgiProtocol`, `RsgiHttpProtocol`, and native `App` tuple streams flush progressive frames once headers begin sending [^src-vertebra-py-src-transport-rs].

Provenance sources: [^src-vertebra-core-src-protocol-codec-rs].

[^src-vertebra-core-src-transport-mod-rs]: `src/rs/vertebra-core/src/transport/mod.rs` — Rust implementation module (src/rs/vertebra-core/src/transport/mod.rs).
[^src-vertebra-core-src-transport-ring-pair-rs]: `src/rs/vertebra-core/src/transport/ring_pair.rs` — Rust implementation module (src/rs/vertebra-core/src/transport/ring_pair.rs).
[^src-vertebra-core-src-tiered-ring-rs]: `src/rs/vertebra-core/src/tiered_ring.rs` — Rust implementation module (src/rs/vertebra-core/src/tiered_ring.rs).
[^src-vertebra-core-src-bridge-rs]: `src/rs/vertebra-core/src/bridge.rs` — Rust implementation module (src/rs/vertebra-core/src/bridge.rs).
[^src-vertebra-core-src-ipc-rs]: `src/rs/vertebra-core/src/ipc.rs` — Rust implementation module (src/rs/vertebra-core/src/ipc.rs).
[^src-vertebra-core-src-protocol-codec-rs]: `src/rs/vertebra-core/src/protocol/codec.rs` — Rust implementation module (src/rs/vertebra-core/src/protocol/codec.rs).
[^src-vertebra-py-src-transport-rs]: `src/rs/vertebra-py/src/transport.rs` — Rust implementation module (src/rs/vertebra-py/src/transport.rs).
[^src-vertebra-http-src-dispatcher-shm-reader-rs]: `src/rs/vertebra-http/src/dispatcher/shm_reader.rs` — Rust implementation module (src/rs/vertebra-http/src/dispatcher/shm_reader.rs).

[^src-vertebra-http-src-dispatcher-streams-rs]: `src/rs/vertebra-http/src/dispatcher/streams.rs` — Bounded native response delivery and worker lifecycle cleanup.
