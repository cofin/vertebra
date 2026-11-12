---
type: Task
id: "server-runtime-correctness:ch01:t01"
chapter_id: ch01-bridge-stream-frames
title: Add ResponseStreamFrame, StreamDelivery, StreamSink, and submit_stream_frame to BatchBridge
description: Introduce a transport-neutral ordered stream frame type and a dedicated stream sink on the in-process BatchBridge that reports acceptance or a closed client per frame, without changing unary BridgeResponse delivery.
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - .agents/bundles/specs/server-runtime-correctness/chapters/ch01-bridge-stream-frames/spec.md
  - src/rs/vertebra-core/src/bridge.rs
  - src/rs/vertebra-core/src/lib.rs
  - src/rs/vertebra-core/src/ipc.rs
  - src/rs/vertebra-core/src/transport/mod.rs
files:
  - src/rs/vertebra-core/src/bridge.rs
  - src/rs/vertebra-core/src/lib.rs
tests:
  - src/rs/vertebra-core/src/bridge.rs
estimated_loc: 150
plan_revision: 2
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: 2e4ef248
verification_evidence: "Red: bridge tests exit 101 (missing stream API); Green: 23 bridge tests exit 0; core all-target Clippy -D warnings exit 0; cargo fmt --check exit 0."
created_at: "2026-11-12T00:45:00Z"
updated_at: "2026-11-12T00:45:00Z"
commit: 2e4ef248
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:45:00Z"
sources:
  - id: bridge-core-rs
    resource: src/rs/vertebra-core/src/bridge.rs::BatchBridge
    title: Bridge sink registration, override sink, and submit_response dispatch
  - id: ipc-flags-rs
    resource: src/rs/vertebra-core/src/ipc.rs::FLAG_STREAMING
    title: Shared response frame flag constants
tags:
  - rust
  - bridge
  - streaming
---

# Task t01: Add ResponseStreamFrame, StreamDelivery, StreamSink, and submit_stream_frame to BatchBridge

## Objective & Scope Boundary

Add a stream frame type, delivery outcome enum, and a dedicated stream sink to `src/rs/vertebra-core/src/bridge.rs`, re-export them from `src/rs/vertebra-core/src/lib.rs`, and cover them with inline bridge tests. Out of scope: SHM ring frames (`t02`), the HTTP registry (`t03`), Python bindings (`ch02`), and any change to `BridgeResponse`, `ResponseSink`, `submit_response`, or `submit_responses`.

## Bounded Read Manifest

- [spec.md](../spec.md) — chapter contracts and edge cases.
- `src/rs/vertebra-core/src/bridge.rs::{BridgeResponse,ResponseSink,BatchBridge::set_response_sink,BatchBridge::try_dispatch_to_sink_single,BatchBridge::submit_response}` and `tests::{make_bridge,make_bridge_with_codec}` [^bridge-core-rs].
- `src/rs/vertebra-core/src/lib.rs` — `pub use bridge::{BatchBridge, BridgeResponse, ResponseSink, WriteRequest};`.
- `src/rs/vertebra-core/src/ipc.rs::{FLAG_STREAMING,FLAG_FINAL,FLAG_ERROR,FLAG_HEADERS_SENT,FLAG_TRAILERS}` [^ipc-flags-rs].
- `src/rs/vertebra-core/src/transport/mod.rs::TransportError`.

## Symbol & Signature Contracts

- `src/rs/vertebra-core/src/bridge.rs`:
  - `#[derive(Debug, Clone)] pub struct ResponseStreamFrame { pub request_id: u64, pub status: u16, pub flags: u16, pub headers_raw: bytes::Bytes, pub body: bytes::Bytes }` with `impl ResponseStreamFrame { pub fn is_first(&self) -> bool; pub fn is_terminal(&self) -> bool; pub fn has_trailers(&self) -> bool; pub fn is_error(&self) -> bool; pub fn iter_headers(&self) -> impl Iterator<Item = (&[u8], &[u8])> + '_ }` (`is_first` = `FLAG_HEADERS_SENT`, `is_terminal` = `FLAG_FINAL | FLAG_ERROR`, `iter_headers` delegates to `iter_raw_headers`).
  - `#[derive(Debug, Clone, Copy, PartialEq, Eq)] pub enum StreamDelivery { Accepted, Closed }`.
  - `pub type StreamSink = Arc<dyn Fn(ResponseStreamFrame) -> StreamDelivery + Send + Sync>;` with a doc comment stating that sinks may block the calling thread until the stream accepts the frame and must only be invoked from threads outside an async runtime with the GIL detached.
  - New field `stream_sink: RwLock<Option<StreamSink>>` on `BatchBridge`, initialised to `RwLock::new(None)` in `init_with_codec` and in both test constructors.
  - `pub fn set_stream_sink(&self, sink: StreamSink)` replaces any prior sink.
  - `pub fn submit_stream_frame(&self, frame: ResponseStreamFrame) -> Result<StreamDelivery, TransportError>` clones the sink `Arc` under the read lock, releases the lock, and calls it; no sink or a poisoned lock returns `Err(TransportError::Disconnected)`.
- `src/rs/vertebra-core/src/lib.rs`: `pub use bridge::{BatchBridge, BridgeResponse, ResponseSink, ResponseStreamFrame, StreamDelivery, StreamSink, WriteRequest};`.
- Caller wiring: `ResponseSubmitter::submit_stream_frame_inner` (`ch02:t01`) → `BatchBridge::submit_stream_frame` → sink installed by `BridgeCorrelation::ensure_sink_registered` (`t03`).

## Edge Cases & Error Semantics

- No sink: `Err(TransportError::Disconnected)`, never a panic or silent `Ok`.
- Sink replaced while frames flow: later frames reach the new sink; the lock is not held during the sink call, so a sink may call `set_stream_sink`.
- `flags = FLAG_STREAMING | FLAG_HEADERS_SENT | FLAG_FINAL`: `is_first` and `is_terminal` are both `true`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-core/src/bridge.rs` (`#[cfg(test)] mod tests`), dependency category `unit`; tests use `make_bridge()` rather than the global `BRIDGE` singleton.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| No sink | `submit_stream_frame(ResponseStreamFrame { request_id: 7, status: 200, flags: FLAG_STREAMING \| FLAG_HEADERS_SENT, headers_raw: Bytes::new(), body: Bytes::from_static(b"a") })` | `Err(TransportError::Disconnected)` |
| Ordered passthrough | Sink pushes into `Arc<Mutex<Vec<Bytes>>>` and returns `Accepted`; submit bodies `b"a"`, `b"b"` for id `7` | Both `Ok(StreamDelivery::Accepted)`; recorded `[b"a", b"b"]` |
| Closed | Sink returns `Closed` | `Ok(StreamDelivery::Closed)` |
| Sink replacement | Register sink A, submit `b"1"`, register sink B, submit `b"2"` | A recorded `[b"1"]`; B recorded `[b"2"]` |
| Flag helpers | `FLAG_STREAMING` / `FLAG_STREAMING \| FLAG_ERROR` / `FLAG_STREAMING \| FLAG_HEADERS_SENT \| FLAG_FINAL \| FLAG_TRAILERS` | `(is_first, is_terminal, has_trailers, is_error)` = `(false,false,false,false)` / `(false,true,false,true)` / `(true,true,true,false)` |
| Unary path untouched | Register a `ResponseSink` and a `StreamSink`; `submit_response(BridgeResponse { request_id: 9, status: 200, headers_raw: Bytes::new(), body: Bytes::from_static(b"u") })` | Response sink observes id `9`; stream sink records nothing |

## Verification

- Initial Red baseline: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --lib bridge::tests`
- Final Green + lint:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --lib bridge::tests
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-core --all-targets -- -D warnings
  cargo fmt --manifest-path src/rs/Cargo.toml -p vertebra-core -- --check
  ```
- Planned local commit: `feat(core): add response stream frames and bridge stream sink`

## Acceptance Criteria

- [x] `ResponseStreamFrame`, `StreamDelivery`, `StreamSink`, `set_stream_sink`, and `submit_stream_frame` exist with the signatures above and are re-exported from `vertebra_core` (R1).
- [x] Submitting without a sink fails with `TransportError::Disconnected` (R1).
- [x] Unary bridge behaviour and existing bridge tests are unchanged (R1).

## Notes & Discoveries

[^bridge-core-rs]: `src/rs/vertebra-core/src/bridge.rs::BatchBridge` — Bridge sink registration, override sink, and `submit_response` dispatch.
[^ipc-flags-rs]: `src/rs/vertebra-core/src/ipc.rs::FLAG_STREAMING` — Shared response frame flag constants.
