---
type: Task
id: "server-runtime-correctness:ch01:t04"
chapter_id: ch01-bridge-stream-frames
title: Route drained VRSF frames through the registry and remove the unreachable ResponseSlot routing path
description: Make the SHM response reader drain frames with flags, complete unary frames as today, deliver VRSF stream frames through a ShmDispatcher-owned ResponseStreamRegistry with awaited lossless delivery, and delete the unreachable ResponseSlot-based handle_raw_response_slot and PendingRequests stream machinery.
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "server-runtime-correctness:ch01:t02"
  - "server-runtime-correctness:ch01:t03"
read_set:
  - src/rs/vertebra-http/src/dispatcher/shm_reader.rs
  - src/rs/vertebra-http/src/dispatcher/shm.rs
  - src/rs/vertebra-http/src/dispatcher/mod.rs
  - src/rs/vertebra-http/src/dispatcher/streams.rs
  - src/rs/vertebra-http/src/dispatcher/bridge.rs
  - src/rs/vertebra-http/tests/it/streaming_body.rs
files:
  - src/rs/vertebra-http/src/dispatcher/streams.rs
  - src/rs/vertebra-http/src/dispatcher/bridge.rs
  - src/rs/vertebra-http/src/dispatcher/shm_reader.rs
  - src/rs/vertebra-http/src/dispatcher/shm.rs
  - src/rs/vertebra-http/src/dispatcher/mod.rs
tests:
  - src/rs/vertebra-http/tests/it/streaming_body.rs
  - src/rs/vertebra-http/tests/it/broker_shm.rs
estimated_loc: 250
plan_revision: 2
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: f45a5fe8
verification_evidence: "Red: abandoned bridge receiver regression fails with active_streams 1 versus 0 (exit 101). Green: full core+HTTP tls/compression chapter checkpoint exits 0 (169 core unit, 47 core integration, 49 HTTP unit, 105 HTTP integration, 4 doctests); matching all-target Clippy -D warnings, cargo fmt --check and scoped whitespace exit 0. Logs: /tmp/bulk-runtime-checkpoint.log, /tmp/bulk-runtime-clippy.log."
created_at: "2026-11-12T00:45:00Z"
updated_at: "2026-11-12T00:45:00Z"
commit: f45a5fe8
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

- `src/rs/vertebra-http/src/dispatcher/bridge.rs::BridgeCorrelation::sweep_expired` — periodic closed-receiver pruning.
- `src/rs/vertebra-http/src/dispatcher/shm_reader.rs::{PendingRequests,PendingResponder,run_worker_reader_async,run_worker_reader_polling,drain_worker}` [^shm-reader-rs].
- `src/rs/vertebra-http/src/dispatcher/shm.rs::ShmDispatcher::{new,complete_one,handle_raw_response_slot}` [^shm-dispatch-rs].
- `src/rs/vertebra-http/src/dispatcher/mod.rs::{Dispatcher::complete,Dispatcher::complete_many,Dispatcher::handle_raw_response_slot,DispatchBackend}`.
- `src/rs/vertebra-http/src/dispatcher/streams.rs::ResponseStreamRegistry` (`t03`).
- `src/rs/vertebra-http/tests/it/streaming_body.rs` — tests to replace.

## Symbol & Signature Contracts

- `src/rs/vertebra-http/src/dispatcher/shm.rs`: field `streams: ResponseStreamRegistry` (default capacity `64`); `pub async fn deliver_stream_frame(&self, frame: ResponseStreamFrame) -> StreamDelivery { self.streams.deliver(frame, |id, resp| self.complete_one(id, resp)).await }`; `pub fn active_streams(&self) -> usize`. Remove `pending_streams` and `handle_raw_response_slot`.
- `src/rs/vertebra-http/src/dispatcher/mod.rs`: `pub async fn deliver_stream_frame(&self, frame: ResponseStreamFrame) -> StreamDelivery` (non-SHM backends return `StreamDelivery::Closed` after `debug!`); remove `handle_raw_response_slot`.
- `src/rs/vertebra-http/src/dispatcher/shm_reader.rs`:
  - `async fn drain_worker(worker_id: usize, worker: &Arc<RingPair>, dispatcher: &Dispatcher, shutdown: &CancellationToken) -> bool` collects owned stream frames (copying `headers_raw`/`body` into `Bytes` before any await), completes unary frames in batches and awaits stream delivery in frame order.
  - Both reader loops await `drain_worker` before the next drain, including the final drain after shutdown. Each delivery selects shutdown; a blocked shutdown closes worker streams and releases sender clones. Reader exit sweeps unfinished worker responses.
  - Delete `PendingRequests`, `PendingResponder`, and their imports.

- Registry lifecycle: each entry holds a sender, optional worker id and cancellation token. Insert the entry before invoking the completion callback, releasing every DashMap guard first. On rejected completion remove/cancel the entry. `set_worker(request_id, worker_id)` records ownership while `ShmDispatcher::complete_one` holds the correlation shard lock. `close_worker(worker_id)` removes/cancels owned entries after the worker slab sweep; async send selects cancellation to release cloned senders. `sweep_closed()` prunes dropped receivers; existing bridge and SHM `sweep_expired` call it without changing unary timeout counts.
- Reader cancellation: lossless awaited delivery selects the shutdown token; shutdown cancels blocked delivery, closes the worker streams and exits without waiting for a disconnected or stalled client. Unblocked buffered final frames still drain on shutdown.

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
| Worker dies while send backpressured | Start SHM stream, fill bounded channel, poll another send pending, call `sweep_worker(0)` | Pending send returns `Closed`; buffered body ends after draining; registry count is zero |
| Abandoned response | Drop the body without another frame, invoke existing periodic sweep | Closed sender is removed |
| Shutdown with full stream | Fill channel, cancel response reader | Reader returns promptly; worker streams close |
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

- [x] SHM `VRSF` frames produce progressive `PendingResponse::streaming` bodies with no frame loss (R3).
- [x] Unary SHM responses and broker tests are unchanged (R3).
- [x] `handle_raw_response_slot`, `pending_streams`, `PendingRequests`, and `PendingResponder` are deleted with no remaining references (R3).

## Notes & Discoveries

Accepted streaming responses leave the unary slab at their first frame, so stream entries retain worker ownership until final/error, worker teardown or closed-receiver pruning. Cancellation releases an async sender clone blocked by a full channel; existing periodic sweeps perform abandoned-receiver cleanup.

[^shm-reader-rs]: `src/rs/vertebra-http/src/dispatcher/shm_reader.rs::drain_worker` — SHM reader drain loop, `PendingRequests`, and `run_worker_reader` variants.
[^shm-dispatch-rs]: `src/rs/vertebra-http/src/dispatcher/shm.rs::ShmDispatcher` — `ShmDispatcher` `complete_one`, `pending_streams`, and `handle_raw_response_slot`.
