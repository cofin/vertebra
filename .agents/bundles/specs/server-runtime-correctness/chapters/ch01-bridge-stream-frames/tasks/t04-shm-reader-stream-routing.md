---
type: Task
id: "server-runtime-correctness:ch01:t04"
chapter_id: ch01-bridge-stream-frames
title: Route drained VRSF frames through the registry and remove the unreachable ResponseSlot routing path
description: Make the SHM response reader drain frames with flags, complete unary frames as today, deliver VRSF stream frames through a ShmDispatcher-owned ResponseStreamRegistry with awaited lossless delivery, and delete the unreachable ResponseSlot-based handle_raw_response_slot and PendingRequests stream machinery.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "server-runtime-correctness:ch01:t02"
  - "server-runtime-correctness:ch01:t03"
read_set:
  - .agents/bundles/specs/server-runtime-correctness/chapters/ch01-bridge-stream-frames/spec.md
  - src/rs/vertebra-http/src/dispatcher/shm_reader.rs
  - src/rs/vertebra-http/src/dispatcher/shm.rs
  - src/rs/vertebra-http/src/dispatcher/mod.rs
  - src/rs/vertebra-http/src/dispatcher/streams.rs
  - src/rs/vertebra-http/tests/it/streaming_body.rs
files:
  - src/rs/vertebra-http/src/dispatcher/shm_reader.rs
  - src/rs/vertebra-http/src/dispatcher/shm.rs
  - src/rs/vertebra-http/src/dispatcher/mod.rs
tests:
  - src/rs/vertebra-http/tests/it/streaming_body.rs
  - src/rs/vertebra-http/tests/it/broker_shm.rs
estimated_loc: 200
plan_revision: 2
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T00:45:00Z"
updated_at: "2026-11-12T00:45:00Z"
commit: null
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:45:00Z"
sources:
  - id: shm-reader-rs
    resource: src/rs/vertebra-http/src/dispatcher/shm_reader.rs::drain_worker
    title: SHM reader drain loop, PendingRequests, and run_worker_reader variants
  - id: shm-dispatch-rs
    resource: src/rs/vertebra-http/src/dispatcher/shm.rs::ShmDispatcher
    title: ShmDispatcher complete_one, pending_streams, and handle_raw_response_slot
tags:
  - rust
  - shm
  - streaming
  - dead-code
---

# Task t04: Route Drained VRSF Frames through the Registry and Remove the Unreachable ResponseSlot Routing Path

## Objective & Scope Boundary

Change `shm_reader::drain_worker` to use `RingPair::drain_frames_with`: unary frames (`flags == FLAG_FINAL` without `FLAG_STREAMING`) complete through `Dispatcher::complete`/`complete_many` as today; `VRSF` frames become owned `ResponseStreamFrame`s that the async reader loops deliver in order through `Dispatcher::deliver_stream_frame(...).await`. `ShmDispatcher` owns a `ResponseStreamRegistry`. Delete `ShmDispatcher::handle_raw_response_slot`, `Dispatcher::handle_raw_response_slot`, `ShmDispatcher.pending_streams`, `PendingRequests`, and `PendingResponder`, which no production path reaches, and replace their tests. Out of scope: Python submission (`ch02`).

## Bounded Read Manifest

- [spec.md](../spec.md) — chapter contracts.
- `src/rs/vertebra-http/src/dispatcher/shm_reader.rs::{PendingRequests,PendingResponder,run_worker_reader_async,run_worker_reader_polling,drain_worker}` [^shm-reader-rs].
- `src/rs/vertebra-http/src/dispatcher/shm.rs::ShmDispatcher::{new,complete_one,handle_raw_response_slot}` [^shm-dispatch-rs].
- `src/rs/vertebra-http/src/dispatcher/mod.rs::{Dispatcher::complete,Dispatcher::complete_many,Dispatcher::handle_raw_response_slot,DispatchBackend}`.
- `src/rs/vertebra-http/src/dispatcher/streams.rs::ResponseStreamRegistry` (`t03`).
- `src/rs/vertebra-http/tests/it/streaming_body.rs` — tests to replace.

## Symbol & Signature Contracts

- `src/rs/vertebra-http/src/dispatcher/shm.rs`: field `streams: ResponseStreamRegistry` (default capacity `64`); `pub async fn deliver_stream_frame(&self, frame: ResponseStreamFrame) -> StreamDelivery { self.streams.deliver(frame, |id, resp| self.complete_one(id, resp)).await }`; `pub fn active_streams(&self) -> usize`. Remove `pending_streams` and `handle_raw_response_slot`.
- `src/rs/vertebra-http/src/dispatcher/mod.rs`: `pub async fn deliver_stream_frame(&self, frame: ResponseStreamFrame) -> StreamDelivery` (non-SHM backends return `StreamDelivery::Closed` after `debug!`); remove `handle_raw_response_slot`.
- `src/rs/vertebra-http/src/dispatcher/shm_reader.rs`:
  - `fn drain_worker(worker: &Arc<RingPair>, dispatcher: &Dispatcher, stream_frames: &mut Vec<ResponseStreamFrame>) -> bool` collects stream frames (copying `headers_raw`/`body` into `Bytes`) and completes unary frames.
  - Both reader loops call `drain_worker`, then `for frame in stream_frames.drain(..) { dispatcher.deliver_stream_frame(frame).await; }` before the next drain, including the final drain after shutdown.
  - Delete `PendingRequests`, `PendingResponder`, and their imports.

## Edge Cases & Error Semantics

- A slow client stalls only the reader of its own worker ring while `deliver` awaits capacity; recorded in the limitations chapter (`ch10:t04`).
- `Closed` outcomes (client gone or unknown id) are logged at `debug!` and the loop continues.
- Ring parse errors keep today's `warn!("error reading response ring")` behaviour.
- Shutdown: the final drain still delivers buffered stream frames before the loop exits.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/rs/vertebra-http/tests/it/streaming_body.rs` (stream routing), `src/rs/vertebra-http/tests/it/broker_shm.rs` (unary SHM regression); dependency category `integration` (SHM ring pair + `ShmDispatcher`).

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| SHM progressive stream | Ring pair worker writes `send_stream_frame(id, 200, FLAG_STREAMING\|FLAG_HEADERS_SENT, ct_event_stream, b"data: 1\n\n")`, then `b"data: 2\n\n"` (`FLAG_STREAMING`), then `FLAG_STREAMING\|FLAG_FINAL` | Dispatched request's oneshot yields `stream_rx.is_some()`; collected body `b"data: 1\n\ndata: 2\n\n"`; `active_streams() == 0` |
| Lossless under slow client | Registry capacity `1`; worker writes `5` continuation frames before the test drains the body | Body collects all `5` chunks in order |
| Unary unchanged | Worker writes `send_response(id, 200, b"", b"ok")` | Oneshot yields unary `b"ok"` |
| Closed client | Drop the response body after the first frame; worker writes continuation | Reader logs and continues; next unary response for another id completes |
| Removed API | `streaming_body.rs` no longer imports `PendingRequests`; `pending_requests_keeps_stream_open_until_flag_final_or_error` is replaced by the cases above | Test file compiles against the new API |

## Verification

- Initial Red baseline: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http --test it streaming_body`
- Final Green + lint:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http --test it
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http --lib
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-http --all-targets --features tls,compression -- -D warnings
  ```
- Planned local commit: `feat(http): route SHM stream frames through ResponseStreamRegistry`

## Acceptance Criteria

- [ ] SHM `VRSF` frames produce progressive `PendingResponse::streaming` bodies with no frame loss (R3).
- [ ] Unary SHM responses and broker tests are unchanged (R3).
- [ ] `handle_raw_response_slot`, `pending_streams`, `PendingRequests`, and `PendingResponder` are deleted with no remaining references (R3).

## Notes & Discoveries

[^shm-reader-rs]: `src/rs/vertebra-http/src/dispatcher/shm_reader.rs::drain_worker` — SHM reader drain loop, `PendingRequests`, and `run_worker_reader` variants.
[^shm-dispatch-rs]: `src/rs/vertebra-http/src/dispatcher/shm.rs::ShmDispatcher` — `ShmDispatcher` `complete_one`, `pending_streams`, and `handle_raw_response_slot`.
