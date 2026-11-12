---
type: Spec
parent_flow: server-runtime-correctness
chapter_id: ch01-bridge-stream-frames
title: "Chapter 01: Response Stream Frames on the In-Process Bridge and SHM Rings"
description: Add transport-neutral ResponseStreamFrame and StreamSink to the in-process BatchBridge, add a flagged VRSF stream frame to the SHM response rings, route both into PendingResponse::streaming through one ResponseStreamRegistry, and retire the unreachable ResponseSlot routing path in the SHM dispatcher.
status: draft
state: planned
planning_status: complete
depends_on_chapters: []
replan_trigger: null
current_task: null
plan_revision: 1
plan_commit: null
state_revision: 0
last_verified_checkpoint: null
created_at: "2026-11-12T00:45:00Z"
updated_at: "2026-11-12T00:45:00Z"
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:45:00Z"
sources:
  - id: bridge-core-rs
    resource: src/rs/vertebra-core/src/bridge.rs::BatchBridge
    title: In-process bridge, BridgeResponse, ResponseSink, and sink dispatch helpers
  - id: ring-pair-rs
    resource: src/rs/vertebra-core/src/transport/ring_pair.rs::ResponseData
    title: VRES and VROV SHM response frame layout, send_response, and drain_with
  - id: bridge-http-rs
    resource: src/rs/vertebra-http/src/dispatcher/bridge.rs::BridgeCorrelation
    title: Embedded correlation table and response sink registration
  - id: body-rs
    resource: src/rs/vertebra-http/src/body.rs::PendingResponse
    title: Unary and streaming pending responses over FrameReceiver
  - id: shm-reader-rs
    resource: src/rs/vertebra-http/src/dispatcher/shm_reader.rs::drain_worker
    title: SHM reader that completes every drained frame as a unary PendingResponse
tags:
  - rust
  - streaming
  - bridge
  - shm
---

# Chapter 01: Response Stream Frames on the In-Process Bridge and SHM Rings

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Neither transport can carry a streamed response. The embedded topology delivers responses through `BatchBridge::submit_response` and a `ResponseSink` that returns `()`, and `BridgeCorrelation::ensure_sink_registered` completes every correlation with `PendingResponse { stream_rx: None, .. }` [^bridge-core-rs] [^bridge-http-rs]. The SHM topology writes `VRES` frames (`magic, correlation_id, status, headers_len, body_len`) through `RingPair::send_response`; the frame has no flags, and `shm_reader::drain_worker` completes every drained frame as a unary response [^ring-pair-rs] [^shm-reader-rs]. `ShmDispatcher::handle_raw_response_slot`, `Dispatcher::handle_raw_response_slot`, and the stream half of `PendingRequests` decode the 16-byte `ResponseSlot` format with flags, but no production code calls them and the rings never carry that format. This chapter delivers the transport layer for [#81](https://github.com/cofin/vertebra/issues/81): ordered, lossless stream frames on both transports, completed into `PendingResponse::streaming` by one registry.

### 1.2 Non-Goals

- Python-facing submission (`ch02`) and protocol flushing (`ch03`).
- Changing `BridgeResponse`, `ResponseSink`, `VRES` frames, or any unary path.
- Aborting the hyper body with an error (`FrameReceiver` is `Infallible`); error frames end the body early.

### 1.3 Upstream Baseline & Code Analysis

`PendingResponse::streaming(status, headers_mp, rx)` and `response::build_response` already serve progressive bodies [^body-rs]. `BridgeCorrelation::complete` and `Dispatcher::complete` both take the oneshot sender out of the correlation slab, so completing at the first stream frame removes the request from the `30s` `sweep_expired` window. `RingPair::send_overflow_response` moves frames above the xlarge tier into named SHM regions identified by `VROV`; stream frames stay within the xlarge slot by construction (the submitter chunks bodies by `RingPair::max_stream_frame_payload`).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R1** (`t01`): `ResponseStreamFrame`, `StreamDelivery`, `StreamSink`, `BatchBridge::set_stream_sink`, `BatchBridge::submit_stream_frame`.
- **R3a** (`t02`): `VRSF` SHM stream frames, `RingPair::{send_stream_frame,send_stream_frame_blocking,max_stream_frame_payload,drain_frames_with}`, and `Transport` wrappers.
- **R2** (`t03`): `ResponseStreamRegistry` shared by both dispatch backends and registered as the bridge stream sink.
- **R3b** (`t04`): `drain_worker` routes `VRSF` frames through the registry with awaited delivery; the unreachable `ResponseSlot` routing path is removed.

### 2.2 Concrete Type & Signature Contracts

```rust
// src/rs/vertebra-core/src/bridge.rs
#[derive(Debug, Clone)]
pub struct ResponseStreamFrame {
    pub request_id: u64,
    pub status: u16,
    pub flags: u16,
    pub headers_raw: bytes::Bytes,
    pub body: bytes::Bytes,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamDelivery { Accepted, Closed }
pub type StreamSink = Arc<dyn Fn(ResponseStreamFrame) -> StreamDelivery + Send + Sync>;
impl BatchBridge {
    pub fn set_stream_sink(&self, sink: StreamSink);
    pub fn submit_stream_frame(&self, frame: ResponseStreamFrame) -> Result<StreamDelivery, TransportError>;
}

// src/rs/vertebra-core/src/transport/ring_pair.rs
pub const RESPONSE_STREAM_MAGIC: u32 = 0x56_52_53_46; // "VRSF"
pub struct ResponseFrameView<'a> { pub correlation_id: u64, pub status: u16, pub flags: u16, pub headers_raw: &'a [u8], pub body: &'a [u8] }
impl RingPair {
    pub fn send_stream_frame(&self, correlation_id: u64, status: u16, flags: u16, headers_raw: &[u8], body: &[u8]) -> Result<(), RingPairError>;
    pub fn send_stream_frame_blocking(&self, correlation_id: u64, status: u16, flags: u16, headers_raw: &[u8], body: &[u8]) -> Result<(), RingPairError>;
    pub fn max_stream_frame_payload(&self) -> usize;
    pub fn drain_frames_with<F: FnMut(ResponseFrameView<'_>)>(&self, max_items: usize, f: F) -> Result<usize, RingPairError>;
}

// src/rs/vertebra-http/src/dispatcher/streams.rs
pub struct ResponseStreamRegistry { /* DashMap<u64, mpsc::Sender<Result<Frame<Bytes>, Infallible>>>, capacity */ }
impl ResponseStreamRegistry {
    pub fn with_capacity(capacity: usize) -> Self;
    pub fn deliver_blocking(&self, frame: ResponseStreamFrame, complete: impl FnOnce(u64, PendingResponse) -> bool) -> StreamDelivery;
    pub async fn deliver(&self, frame: ResponseStreamFrame, complete: impl FnOnce(u64, PendingResponse) -> bool) -> StreamDelivery;
    pub fn active_streams(&self) -> usize;
}
```

### 2.3 Caller -> Seam -> Collaborator Map

| Caller | Seam | Collaborator |
| --- | --- | --- |
| `ResponseSubmitter::submit_stream_frame_inner` (`ch02`, embedded) | `BatchBridge::submit_stream_frame` | stream sink → `ResponseStreamRegistry::deliver_blocking` → `BridgeCorrelation::complete` / per-stream `Sender::blocking_send` |
| `ResponseSubmitter::submit_stream_frame_inner` (`ch02`, SHM) | `Transport::send_stream_frame` | `RingPair::send_stream_frame` (`VRSF`) |
| `shm_reader::drain_worker` | `RingPair::drain_frames_with` | unary → `Dispatcher::complete_many`; `VRSF` → `ResponseStreamRegistry::deliver(...).await` → `Dispatcher::complete` / `Sender::send` |

### 2.4 Edge-Case & Failure-Mode Matrix

| Input | Behaviour |
| --- | --- |
| No stream sink registered | `submit_stream_frame` returns `Err(TransportError::Disconnected)`. |
| First frame (`FLAG_HEADERS_SENT`) also terminal | Completes as `PendingResponse::unary`. |
| Continuation for unknown or finished id | `StreamDelivery::Closed`, debug log. |
| Per-stream queue full | `deliver_blocking` blocks the calling non-runtime thread; `deliver` awaits; no frame dropped. |
| Hyper receiver dropped (client gone) | Entry removed; `StreamDelivery::Closed`. |
| `FLAG_TRAILERS` on final frame | `Frame::trailers` from `headers_raw`; invalid names skipped. |
| `FLAG_ERROR` | Entry removed, warn log, body ends. |
| `VRSF` frame larger than the xlarge slot | `RingPairError::ResponseTooLarge`; submitters chunk by `max_stream_frame_payload`. |
| `deliver_blocking` called inside a Tokio runtime | Returns `Closed` after `error!` instead of panicking in `blocking_send`. |

### 2.5 Alternatives Considered

Adding `flags` to `BridgeResponse` or `VRES` was rejected: every unary constructor and parser changes, and the unary sink still cannot report disconnects. Reviving the 16-byte `ResponseSlot` format in the rings was rejected because the ring tiers, overflow regions, and `drain_with` all parse `VRES`; a sibling `VRSF` magic keeps one parser per frame kind. Separate registries per backend were rejected in favour of one `ResponseStreamRegistry` parameterised by a completion closure.

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in ch01]`; bridge and SHM stream semantics land in `ch10:t03` (`tiered-rings-and-batch-bridge.md`, `server-and-dispatch.md`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test / Gate |
| --- | --- | --- | --- |
| R1 | [t01](tasks/t01-core-stream-frame-api.md) | `behavior_tdd` | `src/rs/vertebra-core/src/bridge.rs` `mod tests` |
| R3a | [t02](tasks/t02-shm-ring-stream-frames.md) | `behavior_tdd` | `src/rs/vertebra-core/tests/it/shm.rs` |
| R2 | [t03](tasks/t03-response-stream-registry.md) | `behavior_tdd` | `src/rs/vertebra-http/tests/it/streaming_body.rs` |
| R3b | [t04](tasks/t04-shm-reader-stream-routing.md) | `behavior_tdd` | `src/rs/vertebra-http/tests/it/streaming_body.rs`, `broker_shm.rs` |

## 5. Implementation Plan

- [ ] server-runtime-correctness:ch01:t01 — Add ResponseStreamFrame, StreamDelivery, StreamSink, and submit_stream_frame to BatchBridge (tasks/t01-core-stream-frame-api.md)
- [ ] server-runtime-correctness:ch01:t02 — Add VRSF stream frames and flag-aware draining to the SHM response rings (tasks/t02-shm-ring-stream-frames.md)
- [ ] server-runtime-correctness:ch01:t03 — Route stream frames into PendingResponse::streaming through ResponseStreamRegistry and the bridge sink (tasks/t03-response-stream-registry.md)
- [ ] server-runtime-correctness:ch01:t04 — Route drained VRSF frames through the registry and remove the unreachable ResponseSlot routing path (tasks/t04-shm-reader-stream-routing.md)

**Chapter Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core -p vertebra-http --features vertebra-http/tls,vertebra-http/compression && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-core -p vertebra-http --all-targets --features vertebra-http/tls,vertebra-http/compression -- -D warnings` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Baseline**: `plan_commit: null`; chapter `state: planned`.
- **Remaining Tasks**: `t01`–`t04` (`0/4`).
- **Downstream Dependents**: `ch02:t01` depends on `ch01:t01`–`ch01:t04`.

[^bridge-core-rs]: `src/rs/vertebra-core/src/bridge.rs::BatchBridge` — In-process bridge, `BridgeResponse`, `ResponseSink`, and sink dispatch helpers.
[^ring-pair-rs]: `src/rs/vertebra-core/src/transport/ring_pair.rs::ResponseData` — `VRES` and `VROV` SHM response frame layout, `send_response`, and `drain_with`.
[^bridge-http-rs]: `src/rs/vertebra-http/src/dispatcher/bridge.rs::BridgeCorrelation` — Embedded correlation table and response sink registration.
[^body-rs]: `src/rs/vertebra-http/src/body.rs::PendingResponse` — Unary and streaming pending responses over `FrameReceiver`.
[^shm-reader-rs]: `src/rs/vertebra-http/src/dispatcher/shm_reader.rs::drain_worker` — SHM reader that completes every drained frame as a unary `PendingResponse`.
