---
type: Task
id: "server-runtime-correctness:ch01:t02"
chapter_id: ch01-bridge-stream-frames
title: Add VRSF stream frames and flag-aware draining to the SHM response rings
description: Add a flagged VRSF response frame to RingPair alongside VRES, with non-blocking and blocking writers, a maximum stream payload query, a flag-aware drain_frames_with, and Transport wrappers, leaving unary VRES and VROV frames unchanged.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "server-runtime-correctness:ch01:t01"
read_set:
  - .agents/bundles/specs/server-runtime-correctness/chapters/ch01-bridge-stream-frames/spec.md
  - src/rs/vertebra-core/src/transport/ring_pair.rs
  - src/rs/vertebra-core/src/transport/mod.rs
  - src/rs/vertebra-core/src/ipc.rs
  - src/rs/vertebra-core/tests/it/shm.rs
files:
  - src/rs/vertebra-core/src/transport/ring_pair.rs
  - src/rs/vertebra-core/src/transport/mod.rs
tests:
  - src/rs/vertebra-core/tests/it/shm.rs
estimated_loc: 200
plan_revision: 1
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
  - id: ring-pair-rs
    resource: src/rs/vertebra-core/src/transport/ring_pair.rs::ResponseData
    title: VRES frame layout, write_response_slot, send_response, select_ring_with_bit, and drain_with
  - id: transport-mod-rs
    resource: src/rs/vertebra-core/src/transport/mod.rs::Transport
    title: Transport send_response and send_response_bytes wrappers with RingPairError mapping
tags:
  - rust
  - shm
  - streaming
---

# Task t02: Add VRSF Stream Frames and Flag-Aware Draining to the SHM Response Rings

## Objective & Scope Boundary

Teach the SHM response rings to carry stream frames: a `VRSF` frame (`magic u32 | correlation_id u64 | status u16 | flags u16 | headers_len u16 | body_len u32 | headers | body`, `22`-byte header) written by new `RingPair` methods and parsed by a flag-aware `drain_frames_with`, with `Transport` wrappers. `drain_with` keeps its signature and delegates to `drain_frames_with`, reporting only frames as `(correlation_id, status, headers_raw, body)` for existing callers. Out of scope: the HTTP reader (`t04`) and Python submission (`ch02`).

## Bounded Read Manifest

- [spec.md](../spec.md) — chapter contracts.
- `src/rs/vertebra-core/src/transport/ring_pair.rs::{RESPONSE_MAGIC,RESPONSE_OVERFLOW_MAGIC,RESPONSE_HEADER_SIZE,ResponseData::parse_slice,ResponseData::with_overflow_slice,RingPair::write_response_slot,RingPair::send_response,RingPair::send_response_blocking,RingPair::select_ring_with_bit,RingPair::drain_with}` [^ring-pair-rs].
- `src/rs/vertebra-core/src/transport/mod.rs::{Transport::send_response,Transport::send_response_blocking,Transport::send_response_bytes,TransportError}` [^transport-mod-rs].
- `src/rs/vertebra-core/src/ipc.rs::{FLAG_FINAL,FLAG_STREAMING}`.
- `src/rs/vertebra-core/tests/it/shm.rs` — existing `RingPair` `drain_with` round-trip tests.

## Symbol & Signature Contracts

- `src/rs/vertebra-core/src/transport/ring_pair.rs`:
  - `pub const RESPONSE_STREAM_MAGIC: u32 = 0x56_52_53_46; // "VRSF"` and `const RESPONSE_STREAM_HEADER_SIZE: usize = 22;`.
  - `#[derive(Debug, Clone, Copy)] pub struct ResponseFrameView<'a> { pub correlation_id: u64, pub status: u16, pub flags: u16, pub headers_raw: &'a [u8], pub body: &'a [u8] }`.
  - `impl ResponseData { pub fn parse_frame(data: &[u8]) -> Result<ResponseFrameView<'_>, RingPairError> }`: `VRES` → view with `flags = FLAG_FINAL`; `VRSF` → view with decoded flags; anything else → `InvalidResponse("bad magic")`; truncation and length overflow errors mirror `parse_slice`.
  - `pub fn send_stream_frame(&self, correlation_id: u64, status: u16, flags: u16, headers_raw: &[u8], body: &[u8]) -> Result<(), RingPairError>` and `pub fn send_stream_frame_blocking(...)` with the same arguments; both reject `headers_raw.len() > u16::MAX` and frames larger than the xlarge slot with `ResponseTooLarge(total)` (never the overflow region).
  - `pub fn max_stream_frame_payload(&self) -> usize` = xlarge slot size minus `RESPONSE_STREAM_HEADER_SIZE`.
  - `pub fn drain_frames_with<F: FnMut(ResponseFrameView<'_>)>(&self, max_items: usize, f: F) -> Result<usize, RingPairError>` — same tier order, overflow handling (`VROV` regions parse through `parse_frame`), and `parse_err` semantics as `drain_with`; `drain_with` becomes a thin wrapper.
- `src/rs/vertebra-core/src/transport/mod.rs`:
  - `pub fn send_stream_frame(&self, correlation_id: u64, status: u16, flags: u16, headers_raw: &[u8], body: &[u8]) -> Result<(), TransportError>` and `send_stream_frame_blocking`, mapping errors exactly like `send_response`; `Backend::InProcess` returns `Err(TransportError::Disconnected)` (stream frames on the in-process backend go through `BatchBridge::submit_stream_frame`).
  - `pub fn max_stream_frame_payload(&self) -> Option<usize>` (`None` for `InProcess`).

## Edge Cases & Error Semantics

- Frame exactly at the xlarge slot size succeeds; one byte larger returns `ResponseTooLarge`.
- `VRSF` with `headers_len` or `body_len` pointing past the slot: `InvalidResponse("truncated")`.
- Mixed `VRES` and `VRSF` frames in one tier drain in send order.
- `send_stream_frame` on a full ring returns `Channel(ChannelError::Full)` → `TransportError::RingFull`; the blocking variant waits.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-core/tests/it/shm.rs`, dependency category `integration` (SHM, cleans its own `/dev/shm/test-*` names).

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Stream round trip | `send_stream_frame(11, 200, FLAG_STREAMING \| FLAG_HEADERS_SENT, b"\x00\x0ccontent-type...", b"data: 1\n\n")` then `send_stream_frame(11, 200, FLAG_STREAMING \| FLAG_FINAL, b"", b"")` | `drain_frames_with` yields two views with ids `11`, flags in order, bodies `b"data: 1\n\n"` and `b""` |
| Unary reports final | `send_response(12, 201, b"", b"ok")` | View with `flags == FLAG_FINAL`, `status == 201`, body `b"ok"` |
| Legacy drain unchanged | Same `VRES` frame via `drain_with` | Callback receives `(12, 201, b"", b"ok")` |
| Mixed order | `VRES` id 1, `VRSF` id 2, `VRES` id 3 in the small tier | Views ids `[1, 2, 3]` |
| Oversize | Body of `max_stream_frame_payload() + 1` bytes | `Err(RingPairError::ResponseTooLarge(_))` |
| Bad magic | `ResponseData::parse_frame(&[0u8; 32])` | `Err(InvalidResponse("bad magic"))` |
| Truncated | `VRSF` header claiming `body_len = 100` with `10` body bytes | `Err(InvalidResponse("truncated"))` |
| In-process backend | `Transport::new_in_process(..).send_stream_frame(..)` | `Err(TransportError::Disconnected)`; `max_stream_frame_payload() == None` |

## Verification

- Initial Red baseline: `rm -f /dev/shm/test-* && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --test it shm`
- Final Green + lint:
  ```bash
  rm -f /dev/shm/test-*
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --test it shm
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --lib transport
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-core --all-targets -- -D warnings
  ```
- Planned local commit: `feat(core): add VRSF stream frames to SHM response rings`

## Acceptance Criteria

- [ ] `VRSF` frames round-trip with flags through every ring tier and through `drain_frames_with` (R3).
- [ ] `VRES`, `VROV`, and `drain_with` behaviour and existing SHM tests are unchanged (R3).
- [ ] `Transport` exposes `send_stream_frame`, `send_stream_frame_blocking`, and `max_stream_frame_payload` with the stated in-process semantics (R3).

## Notes & Discoveries

[^ring-pair-rs]: `src/rs/vertebra-core/src/transport/ring_pair.rs::ResponseData` — `VRES` frame layout, `write_response_slot`, `send_response`, `select_ring_with_bit`, and `drain_with`.
[^transport-mod-rs]: `src/rs/vertebra-core/src/transport/mod.rs::Transport` — `Transport` `send_response` and `send_response_bytes` wrappers with `RingPairError` mapping.
