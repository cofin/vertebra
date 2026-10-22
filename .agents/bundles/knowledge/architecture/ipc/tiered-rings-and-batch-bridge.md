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
  - resource: src/rs/vertebra-py/src/api.rs
  - resource: src/rs/vertebra-http/src/response_reader.rs
---

# Tiered Rings & In-Process Batch Bridge

Both topologies move requests as `BatchSnapshot`s (a `Vec<TransportNode>` plus one byte slab). Embedded mode hands snapshots across threads through [`BatchBridge`](../../../../../src/rs/vertebra-core/src/bridge.rs). Shm mode serializes them into a per-worker [`RingPair`](../../../../../src/rs/vertebra-core/src/transport/ring_pair.rs). [`Transport`](../../../../../src/rs/vertebra-core/src/transport/mod.rs) wraps either case as `Backend::InProcess(crossbeam Receiver)` or `Backend::SharedMemory(Arc<RingPair>)`.

## Request Batches (`transport/mod.rs`)

- **`TransportNode`** (`repr(C)`, 56 bytes): `method_tag`, `http_version_tag`, `handler_id: u64` (FNV-1a route id), `correlation_id: u64`, and `u32` offset/length pairs for path, query, headers, and body inside the slab.
- **`DataSlab`** and **`TransportArena`**: fixed-capacity buffers filled with `fetch_add` reservations that roll back on overflow. `take_and_replace` swaps the backing `Vec` in O(1).
- **`BatchSnapshot`**: owned `slab: Vec<u8>`, `nodes: Vec<TransportNode>`, an optional `transport::BufferPool` handle, `flags: u32`, and a lazily decompressed slab (`compression` feature). Dropping a pooled snapshot returns its `(slab, nodes)` pair to the pool.
- **SHM framing**: `BatchSnapshot::serialize_into` / `deserialize_from` and `ShmBatchWriter` write a 16-byte batch header (`flags`, `node_count`, `slab_len`, padding), the node array, then the slab, into one request-ring slot.
- Header helpers: `encode_headers_compact`, `iter_raw_headers`, `decode_headers_raw`.

## In-Process `BatchBridge` (`bridge.rs`)

- `static BRIDGE: OnceLock<BatchBridge>` is set once by `BatchBridge::init(arena_capacity, slab_capacity, channel_depth, max_workers)`; `init_with_codec` also picks a `CompressionCodec`.
- Writers take `batch_writer()`, which holds `write_lock` for the duration of `push_request` calls and `seal()`. Sealing swaps the active slab and arena with a pooled pair, optionally compresses the slab, and sends the `Arc<BatchSnapshot>` on a bounded `crossbeam_channel` of depth `channel_depth`. The `transport::BufferPool` holds `channel_depth + max(max_workers, 1)` buffer pairs and allocates fresh ones when empty.
- **Compression**: `try_compress_slab` compresses the whole sealed slab with the configured codec and keeps it only when the output is strictly smaller. The codec is recorded as `flags::COMPRESSED` plus codec bits 6-7 ([`codec.rs`](../../../../../src/rs/vertebra-core/src/protocol/codec.rs)).
- `poll_batch(timeout)` returns `Option<Arc<BatchSnapshot>>`; PyO3 wraps it in `RequestBatch`.
- **Responses**: Python calls `submit_responses(Vec<BridgeResponse>)` (`blocking_send` on a `tokio::sync::mpsc` channel); the server task awaits `recv_responses()`. `BridgeResponse` carries `request_id`, `status`, `headers`, and `body`.

## Per-Worker `RingPair` (`transport/ring_pair.rs`)

`RingPair` holds `request_ring: Arc<MpmcRing>`, `response_rings: TieredRingSet`, `request_notifier`, `response_notifier`, and its `RingPairConfig`. The default config uses 1024 request slots of 64 KiB and `TieredRingConfig::default()`.

### Response Tiers

`TieredRingSet` (in `ring_pair.rs`) holds one `SpscRing` per size class from [`TieredRingConfig`](../../../../../src/rs/vertebra-core/src/tiered_ring.rs):

| Tier | Slot size | Slots |
|---|---|---|
| `small` | 4 KiB | 64 |
| `medium` | 64 KiB | 16 |
| `large` | 512 KiB | 4 |
| `xlarge` | 2 MiB | 2 |

`select_ring(total_size)` returns the smallest tier with `total_size <= slot_size`, or `None` (`ResponseTooLarge`). A full tier returns `ChannelError::Full`; there is no promotion to a larger tier. `send_response_blocking` waits on the chosen tier instead.

### `VRES` Response Frames

`send_response` writes a 20-byte header followed by headers and body: `magic: u32` (`RESPONSE_MAGIC = 0x5652_4553`, "VRES"), `correlation_id: u64`, `status: u16`, `headers_len: u16`, `body_len: u32`. `try_recv_response` scans `small` → `xlarge` with `try_recv_view` and parses the first frame into `ResponseData`. The server's `response_reader::drain_worker` reads only this format.

## `ResponseSlot` Streaming Frames (`ipc.rs`)

[`ResponseSlot`](../../../../../src/rs/vertebra-core/src/ipc.rs) is a second framing with a 16-byte header `[request_id u64][status u16][flags u16][header_count u16][reserved u16]`, length-prefixed headers, a `u32` body length, and the body. Flags are `u16`: `FLAG_STREAMING`, `FLAG_FINAL`, `FLAG_ERROR`, `FLAG_COMPRESSED`, `FLAG_HEADERS_SENT`, `FLAG_TRAILERS` (bits 0-5).

- `ResponseSlot::encode_raw_parts_into(request_id, status, flags, headers, body, dst)` encodes borrowed parts into a caller buffer without building a `ResponseSlot`.
- `submit_raw_response` ([`api.rs`](../../../../../src/rs/vertebra-py/src/api.rs)) reads the body through `PyBuffer`, splits it at `slot_capacity` (default 16384) into `STREAMING | HEADERS_SENT` → `STREAMING` → `STREAMING | FINAL` frames (one `Vec<u8>` each), and returns the frames. In SHM mode it also writes them with `send_response_bytes`; in-process mode submits nothing.
- The server side does not consume these frames today: `Dispatcher::handle_raw_response_slot` and `response_reader::PendingRequests` streaming responders have no production caller, while `drain_worker` only parses `VRES`.

`buffer::BufferPool` (fixed-size buffer leases, `acquire_multi(total_bytes)`) is a different type from `transport::BufferPool` and is used only in tests.
