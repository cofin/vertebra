---
type: Guide
title: "Tiered Rings & In-Process Batch Bridge"
description: "RingPair request/response framing, TieredRingSet size classes, ResponseSlot streaming frames, and the in-process BatchBridge batch transport."
status: stable
tags: [architecture, ipc, tiered-rings, batch-bridge, rust]
sources:
  - resource: src/rs/vertebra-core/src/transport/mod.rs
  - resource: src/rs/vertebra-core/src/transport/ring_pair.rs
  - resource: src/rs/vertebra-core/src/tiered_ring.rs
  - resource: src/rs/vertebra-core/src/bridge.rs
  - resource: src/rs/vertebra-core/src/ipc.rs
  - resource: src/rs/vertebra-core/src/protocol/codec.rs
  - resource: src/rs/vertebra-py/src/transport.rs
  - resource: src/rs/vertebra-http/src/dispatcher/shm_reader.rs
---

# Tiered Rings & In-Process Batch Bridge

Both topologies move requests as `BatchSnapshot`s. Embedded mode hands zero-copy `InProcessRequest` snapshots across threads through [`BatchBridge`](../../../../../src/rs/vertebra-core/src/bridge.rs) and completes responses directly through a registered `ResponseSink`. Shm mode serializes batches directly into a per-worker [`RingPair`](../../../../../src/rs/vertebra-core/src/transport/ring_pair.rs) and borrows the slot zero-copy on the Python side (`MpmcSlotView` / `SlotView`). [`Transport`](../../../../../src/rs/vertebra-core/src/transport/mod.rs) wraps either backend cleanly.

## Request Batches (`transport/mod.rs`)

- **`TransportNode`** (`repr(C)`, 56 bytes): `method_tag`, `http_version_tag`, `handler_id: u64` (FNV-1a route id or `MOUNT_HANDLER_BASE | mount_idx`), `correlation_id: u64`, and `u32` offset/length pairs for path, query, headers, and body.
- **`InProcessRequest`**: zero-copy in-process request record holding ref-counted `Bytes` slices (`path`, `query`, `headers_raw`, `body`, `method_tag`, `http_version_tag`, `handler_id`, `correlation_id`) with zero `DataSlab` copying and zero `1 MiB` batch size cap.
- **`BatchSnapshot`**: supports multiple zero-copy backings—in-process `Vec<InProcessRequest>`, borrowed SHM `MpmcSlotView` / `SlotView` (releasing the ring slot on `Drop` without copying the slot onto the Python heap), or pooled `(slab, nodes)` pairs (`DataSlab` + `TransportArena` + `BufferPool`, with optional LZ4/Zstd slab decompression under `compression`).
- **SHM framing**: `BatchSnapshot::serialize_into` and `ShmDispatcher` write a 16-byte batch header (`flags`, `node_count`, `slab_len`, padding), the `TransportNode` array, and the contiguous byte slab directly into one request-ring slot via `try_send_with`.
- Header helpers: `encode_headers_compact`, `iter_raw_headers`, `decode_headers_raw`.

## In-Process `BatchBridge` (`bridge.rs`)

- `static BRIDGE: OnceLock<BatchBridge>` is initialized once by `BatchBridge::init` **only** when running in `Topology::Embedded`.
- **Lock-free request queue**: `RequestBatcher` shards call `BatchBridge::submit_inprocess_batch(Vec<InProcessRequest>)`, pushing `Arc<BatchSnapshot>` onto an unbounded `crossbeam_channel` (bounded at ingress by `BackpressureController` `max_in_flight`, eliminating bounded-channel ABBA deadlocks between Tokio workers and Python). Legacy `BatchWriter::push_request` callers auto-seal on `SlabFull`/`ArenaFull`.
- **Single-consumer Dekker request `Notifier`**: `BatchBridge` embeds a `Notifier` (`request_notify_fd`, `consumer_sleeping: AtomicU32`, `consumer_count: AtomicUsize`). When `consumer_count <= 1`, single-loop embedded `uvloop` workers register `loop.add_reader(request_notify_fd, ...)` and bracket idle waits with `prepare_request_sleep` / `cancel_request_sleep` so producers signal the FD only when the loop is sleeping. When `consumer_count > 1` (free-threaded multi-thread pools), workers poll `get_batch` directly.
- **Direct response completion (`ResponseSink`)**: `BridgeResponse` carries `(request_id: u64, status: u16, headers_raw: Bytes, body: Bytes)` with zero `Vec<(Vec<u8>, Vec<u8>)>` header transcoding. When `vertebra-http` registers a `ResponseSink` (`OnceLock<ResponseSink>`), `submit_response_raw` and `submit_batch_raw` complete the sharded `InFlightRequests` `oneshot::Sender` directly on the submitting thread without an intermediate `mpsc` channel or `spawn_response_reader` task (falling back to an unbounded `mpsc` response channel in standalone bridge tests).

## Per-Worker `RingPair` (`transport/ring_pair.rs`)

`RingPair` holds `request_ring: Arc<MpmcRing>`, `response_rings: TieredRingSet`, `request_notifier`, `response_notifier`, and its `RingPairConfig`. The default config uses 1024 request slots of 64 KiB and `TieredRingConfig::default()`, using `ShmHeader` (`ABI_VERSION = 3`) `consumer_sleeping` Dekker handshakes (`prepare_request_sleep`, `cancel_request_sleep`, `notify_request_if_sleeping`, `prepare_response_sleep`, `cancel_response_sleep`, `notify_response_if_sleeping`) to coalesce `eventfd`/`pipe` wakeups to ~0 syscalls under sustained load.

### Response Tiers

`TieredRingSet` (in `ring_pair.rs`) holds one `SpscRing` per size class from [`TieredRingConfig`](../../../../../src/rs/vertebra-core/src/tiered_ring.rs):

| Tier | Slot size | Slots |
|---|---|---|
| `small` | 4 KiB | 512 |
| `medium` | 32 KiB | 128 |
| `large` | 256 KiB | 32 |
| `xlarge` | 2 MiB | 8 |

`select_ring(total_size)` returns the smallest tier with `total_size <= payload_capacity()`, or `None` (`ResponseTooLarge`). Before publishing `write_head` on a non-small tier, the sender sets that tier's bit (`bit 1..3`) in the small ring's `ShmHeader.active_flags` (`mark_tier_active`, clearing if empty on send error). Consumers drain `small` first via zero-copy `drain_responses_with` and only probe `medium`/`large`/`xlarge` when `active_tier_mask()` or `has_non_small_data()` (`ring.has_data_acquire()`) indicates pending frames.

### `VRES` Response Frames

`send_response` writes a 20-byte header followed by compact binary headers and body: `magic: u32` (`RESPONSE_MAGIC = 0x5652_4553`, "VRES"), `correlation_id: u64`, `status: u16`, `headers_len: u16`, `body_len: u32`. `drain_responses_with` parses `HeaderMap` and `Bytes` directly from the borrowed SHM slot slice without intermediate `Vec<u8>` copies.

## `ResponseSlot` Streaming Frames (`ipc.rs`)

[`ResponseSlot`](../../../../../src/rs/vertebra-core/src/ipc.rs) provides the unified response framing with a 16-byte header `[request_id u64][status u16][flags u16][header_count u16][reserved u16]`, length-prefixed headers, a `u32` body length, and the body. Flags are `u16`: `FLAG_STREAMING`, `FLAG_FINAL`, `FLAG_ERROR`, `FLAG_COMPRESSED`, `FLAG_HEADERS_SENT`, `FLAG_TRAILERS` (bits 0-5).

- `ResponseSlot::encode_raw_parts_into(request_id, status, flags, headers, body, dst)` encodes borrowed parts into a caller buffer without building a `ResponseSlot`.
- `submit_raw_response` ([`transport.rs`](../../../../../src/rs/vertebra-py/src/transport.rs)) reads the body through `PyBuffer`, splits it at `slot_capacity` (default 16384) into `STREAMING | HEADERS_SENT` → `STREAMING` → `STREAMING | FINAL` frames (one `Vec<u8>` each), and returns the frames. In SHM mode it also writes them with `send_response_bytes`.
- `ShmDispatcher::handle_raw_response_slot` and [`shm_reader::PendingRequests`](../../../../../src/rs/vertebra-http/src/dispatcher/shm_reader.rs) decode and route streaming `ResponseSlot` frames.
