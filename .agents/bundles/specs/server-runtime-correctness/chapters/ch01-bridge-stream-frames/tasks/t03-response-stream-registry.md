---
type: Task
id: "server-runtime-correctness:ch01:t03"
chapter_id: ch01-bridge-stream-frames
title: Route stream frames into PendingResponse::streaming through ResponseStreamRegistry and the bridge sink
description: Implement ResponseStreamRegistry in a new dispatcher streams module with blocking and async delivery parameterised by a completion closure, register it as the BatchBridge stream sink from BridgeCorrelation, and move correlation allocation into a shared register_pending seam.
status: draft
state: in_progress
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "server-runtime-correctness:ch01:t01"
read_set:
  - .agents/bundles/specs/server-runtime-correctness/chapters/ch01-bridge-stream-frames/spec.md
  - src/rs/vertebra-http/src/dispatcher/bridge.rs
  - src/rs/vertebra-http/src/dispatcher/mod.rs
  - src/rs/vertebra-http/src/body.rs
  - src/rs/vertebra-http/src/response.rs
  - src/rs/vertebra-http/tests/it/streaming_body.rs
files:
  - src/rs/vertebra-http/src/dispatcher/streams.rs
  - src/rs/vertebra-http/src/dispatcher/bridge.rs
  - src/rs/vertebra-http/src/dispatcher/mod.rs
tests:
  - src/rs/vertebra-http/tests/it/streaming_body.rs
estimated_loc: 240
plan_revision: 2
plan_commit: null
state_revision: 0
claimed_by: agent:bulk_runtime_registry
claimed_at: "2026-11-12T03:33:00Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-12T03:33:36Z"
last_verified_commit: null
verification_evidence: "Red: missing ResponseStreamRegistry/register_pending (11 compile errors); Green: 12 canonical streaming tests, 17 dispatcher tests, full HTTP TLS/compression 153 tests, HTTP3 feature check, all-target Clippy and whitespace all exit 0."
created_at: "2026-11-12T00:45:00Z"
updated_at: "2026-11-12T00:45:00Z"
commit: null
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:45:00Z"
sources:
  - id: bridge-http-rs
    resource: src/rs/vertebra-http/src/dispatcher/bridge.rs::BridgeCorrelation
    title: Embedded correlation slab, ensure_sink_registered, complete, sweep_expired, and dispatch_bridge_vec
  - id: body-rs
    resource: src/rs/vertebra-http/src/body.rs::PendingResponse
    title: PendingResponse::streaming and PendingResponse::unary
  - id: response-rs
    resource: src/rs/vertebra-http/src/response.rs::encode_response_headers
    title: Response header encoding and build_response streaming branch
tags:
  - rust
  - http
  - streaming
  - embedded
---

# Task t03: Route Stream Frames into PendingResponse::streaming through ResponseStreamRegistry and the Bridge Sink

## Objective & Scope Boundary

Create `src/rs/vertebra-http/src/dispatcher/streams.rs` with `ResponseStreamRegistry`, hold one registry in `BridgeCorrelation`, and have `BridgeCorrelation::ensure_sink_registered` install a `StreamSink` that calls `deliver_blocking` with `BridgeCorrelation::complete` as the completion closure. Move the shard-probing allocation out of `dispatch_bridge_vec` into `BridgeCorrelation::register_pending`. Export both from `dispatcher/mod.rs`. Out of scope: the SHM reader (`t04`) and Python submission (`ch02`).

## Bounded Read Manifest

- [spec.md](../spec.md) — chapter contracts.
- `src/rs/vertebra-http/src/dispatcher/bridge.rs::{BridgeCorrelation,BridgeCorrelation::ensure_sink_registered,BridgeCorrelation::complete,BridgeCorrelation::sweep_expired,dispatch_bridge_vec}` [^bridge-http-rs].
- `src/rs/vertebra-http/src/dispatcher/mod.rs` — module list and re-exports.
- `src/rs/vertebra-http/src/body.rs::{PendingResponse::streaming,PendingResponse::unary,FrameReceiver}` [^body-rs].
- `src/rs/vertebra-http/src/response.rs::{encode_response_headers,build_response}` [^response-rs].
- `src/rs/vertebra-http/tests/it/streaming_body.rs` — existing streaming test helpers.

## Symbol & Signature Contracts

- `src/rs/vertebra-http/src/dispatcher/streams.rs`:
  - `pub struct ResponseStreamRegistry { streams: DashMap<u64, mpsc::Sender<Result<Frame<Bytes>, Infallible>>>, capacity: usize }`; `impl Default` uses capacity `64`.
  - `pub fn with_capacity(capacity: usize) -> Self` (`capacity.max(1)`).
  - `pub fn deliver_blocking(&self, frame: ResponseStreamFrame, complete: impl FnOnce(u64, PendingResponse) -> bool) -> StreamDelivery` — uses `Sender::blocking_send`; when `tokio::runtime::Handle::try_current().is_ok()` it logs `error!` and returns `Closed` without sending.
  - `pub async fn deliver(&self, frame: ResponseStreamFrame, complete: impl FnOnce(u64, PendingResponse) -> bool) -> StreamDelivery` — uses `Sender::send().await`.
  - Shared routing (private `fn route_first` / `fn take_sender`):
    1. `is_first() && is_terminal()` → `complete(id, PendingResponse::unary(status, headers_mp, body))` → `Accepted` if `true`, else `Closed`.
    2. `is_first()` → `mpsc::channel(capacity)`; queue the first body chunk when non-empty with `try_send` (the channel is empty); call `complete(id, PendingResponse::streaming(status, headers_mp, rx))`; insert the sender only on `true`; else `Closed`.
    3. Continuation → missing sender → `Closed` (debug log); `is_error()` → remove, `warn!`, `Accepted`; non-empty body → send `Frame::data`; `has_trailers()` → send `Frame::trailers` built from `iter_headers()`; `is_terminal()` → remove after sending; send failure → remove, `Closed`.
  - `headers_mp = encode_response_headers(&frame.iter_headers().map(|(k, v)| (k.to_vec(), v.to_vec())).collect::<Vec<_>>())`.
  - `pub fn active_streams(&self) -> usize`.
- `src/rs/vertebra-http/src/dispatcher/bridge.rs`:
  - `BridgeCorrelation` gains `streams: ResponseStreamRegistry` and `pub fn stream_registry(&self) -> &ResponseStreamRegistry`.
  - `ensure_sink_registered` also calls `bridge.set_stream_sink(Arc::new(move |frame| match weak.upgrade() { Some(corr) => corr.streams.deliver_blocking(frame, |id, resp| corr.complete(id, resp)), None => StreamDelivery::Closed }))`.
  - `pub fn register_pending(&self, reply: oneshot::Sender<PendingResponse>) -> Option<u64>` allocates in the next shard with spare capacity (probing all `BRIDGE_SHARDS`), increments `active_requests`, and sends `service_unavailable_response()` and returns `None` when every shard is full; `dispatch_bridge_vec` calls it per non-fire-and-forget request and counts `None` as a rejected request.
- `src/rs/vertebra-http/src/dispatcher/mod.rs`: `pub mod streams;` and `pub use self::streams::ResponseStreamRegistry;`.

## Edge Cases & Error Semantics

- Completing at the first frame removes the correlation from `sweep_expired`, so long-lived SSE streams are not cut at `30s`.
- Hyper drops the body receiver on disconnect: the next send fails, the entry is removed, `Closed` is returned.
- Invalid trailer names or values are skipped, matching `ShmDispatcher` tolerance.
- `register_pending` under full shards sends `503` before returning `None` because it owns the reply sender; `dispatch_bridge_vec` keeps its overflow warn log.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-http/tests/it/streaming_body.rs`, dependency category `integration` (tokio, no sockets). Correlations are allocated with `BridgeCorrelation::register_pending`; blocking delivery runs on `std::thread::spawn` while the tokio test drains.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Progressive delivery | First frame `FLAG_STREAMING\|FLAG_HEADERS_SENT`, status `200`, header `content-type: text/event-stream`, body `b"data: 1\n\n"`; continuation `b"data: 2\n\n"`; final `FLAG_STREAMING\|FLAG_FINAL` empty | Oneshot yields `status == 200`, `stream_rx.is_some()`; `build_response(..)` body collects to `b"data: 1\n\ndata: 2\n\n"`; `active_streams() == 0` |
| First frame terminal | `FLAG_STREAMING\|FLAG_HEADERS_SENT\|FLAG_FINAL`, body `b"x"` | Unary response, `body == b"x"`, `stream_rx.is_none()` |
| Blocking backpressure | `with_capacity(1)`; first frame `b"a"`; spawn thread delivering `b"b"` | Thread stays blocked until the test reads one frame; then returns `Accepted`; frames arrive `b"a"`, `b"b"` |
| Async delivery | Same frames through `deliver(..).await` | Same order; all `Accepted` |
| Closed client | Drop the response body; deliver continuation `b"c"` | `Closed`; `active_streams() == 0` |
| Unknown id | Continuation for id `999` | `Closed` |
| Trailers | Final `FLAG_STREAMING\|FLAG_FINAL\|FLAG_TRAILERS` with `grpc-status: 0` | Collected trailers contain `grpc-status: 0` |
| Error frame | Continuation `FLAG_STREAMING\|FLAG_ERROR` | `Accepted`; body ends after earlier data; `active_streams() == 0` |
| Runtime guard | `deliver_blocking` called inside `#[tokio::test]` | `Closed`, nothing queued |
| Bridge sink wiring | `BatchBridge` test instance + `ensure_sink_registered`; `submit_stream_frame(first)` for a registered id | `Ok(StreamDelivery::Accepted)` and the oneshot resolves with `stream_rx.is_some()` |

## Verification

- Initial Red baseline: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http --test it streaming_body`
- Final Green + lint:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http --test it streaming_body
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http --lib dispatcher
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-http --all-targets --features tls,compression -- -D warnings
  ```
- Planned local commit: `feat(http): stream embedded responses through ResponseStreamRegistry`

## Acceptance Criteria

- [x] `ResponseStreamRegistry` implements first, continuation, trailer, final, and error semantics with blocking and async delivery (R2).
- [x] `BridgeCorrelation::ensure_sink_registered` registers the stream sink, and `dispatch_bridge_vec` allocates through `register_pending` (R2).
- [x] Existing dispatcher and bridge tests stay green (R2).

## Notes & Discoveries

- `BridgeCorrelation::complete` returns the result of `oneshot::send`, so a dropped client prevents first-frame registration. Empty final frames also check the receiver state and report `Closed`.
- The consuming `register_pending` signature answers full-capacity callers with `503` internally; dispatch records the rejection and logs it.
- Red: canonical streaming tests fail on missing registry and allocator APIs. Green: 12 streaming tests, 17 dispatcher tests, all-target Clippy with TLS/compression, and whitespace check exit `0`. Full HTTP TLS/compression tests (`153`) and the HTTP/3 feature check also exit `0`.

[^bridge-http-rs]: `src/rs/vertebra-http/src/dispatcher/bridge.rs::BridgeCorrelation` — Embedded correlation slab, `ensure_sink_registered`, `complete`, `sweep_expired`, and `dispatch_bridge_vec`.
[^body-rs]: `src/rs/vertebra-http/src/body.rs::PendingResponse` — `PendingResponse::streaming` and `PendingResponse::unary`.
[^response-rs]: `src/rs/vertebra-http/src/response.rs::encode_response_headers` — Response header encoding and `build_response` streaming branch.
