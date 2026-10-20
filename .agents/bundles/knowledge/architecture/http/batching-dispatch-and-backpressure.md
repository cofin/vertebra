---
type: Guide
title: "Request Batching, Dispatch & Backpressure"
description: "Dual-path VertebraBody, 8-shard RequestBatcher, adaptive BatchPolicy, Dispatcher modes, 256-shard PendingRequests, and BackpressureController."
status: stable
tags: [architecture, http, batching, dispatch, streaming, backpressure]
sources:
  - resource: src/rs/vertebra-http/src/response.rs
  - resource: src/rs/vertebra-http/src/response_reader.rs
  - resource: src/rs/vertebra-http/src/collector.rs
  - resource: src/rs/vertebra-http/src/policy.rs
  - resource: src/rs/vertebra-http/src/dispatcher/mod.rs
  - resource: src/rs/vertebra-http/src/dispatcher/direct.rs
  - resource: src/rs/vertebra-http/src/dispatcher/bridge.rs
  - resource: src/rs/vertebra-http/src/dispatcher/shm.rs
  - resource: src/rs/vertebra-http/src/correlation.rs
  - resource: src/rs/vertebra-http/src/backpressure.rs
  - resource: src/rs/vertebra-http/src/sse.rs
  - resource: src/rs/vertebra-http/src/ws_mount.rs
  - resource: src/rs/vertebra-http/src/pubsub.rs
  - resource: src/py/vertebra/server/_asgi/_sse.py
  - resource: src/py/vertebra/server/_asgi/_websocket.py
---

# Request Batching, Dispatch & Backpressure

Dynamic requests matched by `Router` pass through [`BackpressureController`](../../../../../src/rs/vertebra-http/src/backpressure.rs), [`RequestBatcher`](../../../../../src/rs/vertebra-http/src/collector.rs), and [`Dispatcher`](../../../../../src/rs/vertebra-http/src/dispatcher/mod.rs) to amortize IPC and GIL acquisition across concurrent connections while supporting both unary and multi-frame streaming responses via [`VertebraBody`](../../../../../src/rs/vertebra-http/src/response.rs).

```mermaid
flowchart LR
    Conn["Hyper Connection Task"] --> BP["BackpressureController"]
    BP --> Batcher["RequestBatcher (8 Shards)"]
    Batcher -->|"BatchPolicy Trigger"| Disp["Dispatcher"]
    Disp -->|"Direct / Bridge"| PyBridge["BatchBridge (Embedded)"]
    Disp -->|"SharedMemory"| ShmDisp["ShmDispatcher + PendingRequests (256 Shards)"]
```

## 1. Dual-Path `VertebraBody` & `PendingRequests` (`response.rs`, `correlation.rs`, `response_reader.rs`)

- **[`VertebraBody`](../../../../../src/rs/vertebra-http/src/response.rs)**: Defined as `Either<Full<Bytes>, StreamingBody>` (`http_body_util::Either`). Unary responses use `full_body(Bytes)` (`Either::Left(Full::new(bytes))`) with zero channel allocation on the fast path, while progressive responses use `stream_body(rx)` (`Either::Right(StreamingBody)` backed by `tokio::sync::mpsc::Receiver<Result<Frame<Bytes>, Infallible>>`) for both `Frame::data` chunks and `Frame::trailers(HeaderMap)`.
- **[`PendingRequests`](../../../../../src/rs/vertebra-http/src/correlation.rs)** (aliased as `InFlightRequests`): Pre-allocates `65,536` correlation slots across `256` independent `Mutex` shards (`256` slots per shard). Supports both unary `complete(correlation_id, Response)` and streaming `start_stream` / `push_chunk` / `push_trailers` / `finish_stream` transitions driven by [`response_reader.rs`](../../../../../src/rs/vertebra-http/src/response_reader.rs).

## 2. 8-Shard `RequestBatcher` & `BatchPolicy` (`collector.rs`, `policy.rs`)

[`RequestBatcher`](../../../../../src/rs/vertebra-http/src/collector.rs) partitions incoming requests across `NUM_SHARDS = 8` cache-line-aligned `Mutex<Vec<PendingRequest>>` shards selected via multiplicative Fibonacci hashing of the caller thread ID (`thread_id * 0x9E37_79B9_7F4A_7C15`):

- **Zero-Timer Idle Sleep**: When `pending_count == 0`, the drain loop sleeps on `Notify::notified().await` with zero timer wheel overhead.
- **Adaptive Drain (`BatchPolicy`)**: Configured by [`BatchPolicy`](../../../../../src/rs/vertebra-http/src/policy.rs) (`max_batch_size = 64`, `linger_us`, `adaptive = true`). When a batch reaches `max_batch_size`, `submit()` immediately wakes `flush_notify`. Under sustained high load, the drain loop skips the linger timer and drains all 8 shards immediately.

## 3. Three `Dispatcher` Modes (`dispatcher/`)

[`Dispatcher`](../../../../../src/rs/vertebra-http/src/dispatcher/mod.rs) routes requests according to deployment topology:

1. **[`Direct`](../../../../../src/rs/vertebra-http/src/dispatcher/direct.rs)** (`direct_worker.rs`): Dispatches directly to the embedded Python handler inside the same process without intermediate batch queuing.
2. **[`Bridge`](../../../../../src/rs/vertebra-http/src/dispatcher/bridge.rs)**: Flushes [`RequestBatcher`](../../../../../src/rs/vertebra-http/src/collector.rs) batches into the in-process [`BatchBridge`](../../../../../src/rs/vertebra-core/src/bridge.rs).
3. **[`SharedMemory`](../../../../../src/rs/vertebra-http/src/dispatcher/shm.rs)** (`response_reader.rs`): Encodes requests into `VREQ` wire format across power-of-two worker `RingPair` shards (`idx & shard_mask`) and supports multi-chunk streaming via `ShmDispatcher::dispatch_stream_chunk`. Dedicated Tokio response reader tasks (`response_reader.rs`) drain the 4-tier response `TieredRingSet` using zero-copy `SlotView` and resolve callers via [`PendingRequests`](../../../../../src/rs/vertebra-http/src/correlation.rs).

## 4. `BackpressureController` (`backpressure.rs`)

- **[`BackpressureController`](../../../../../src/rs/vertebra-http/src/backpressure.rs)**: Guards ingress with a bounded `Semaphore` (`max_in_flight`) and a sliding-window circuit breaker (`Closed`, `Open`, `HalfOpen`), shedding overload immediately with `503 Service Unavailable` and `Retry-After`.
