---
type: Task
id: "native-postgres-driver:ch02:t01"
chapter_id: ch02-wire-codec-and-session-startup
title: Implement bounded PostgreSQL Protocol 3.0 startup encoder and backend frame decoder
description: Implement crate-private protocol::read_message enforcing 4..=16_777_216 byte frame bounds before allocation over postgres-protocol 0.6.12 and protocol::encode_startup_frame with Protocol 3.0 UTF8 parameters.
status: done
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch01:t04"
read_set:
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/src/config.rs
  - src/rs/vertebra-postgres/src/errors.rs
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
files:
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/src/protocol.rs
tests:
  - src/rs/vertebra-postgres/src/protocol.rs
estimated_loc: 180
plan_revision: 5
plan_commit: c8722c76
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-08T02:05:22Z"
commit: ba367822
sources:
  - id: proto-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: postgres-protocol 0.6.12 frontend startup_message and backend Message::parse contracts
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Protocol 3.0 framing limits and startup parameter defaults
tags:
  - postgres
  - protocol
  - framing
  - codec
---

# Task t01: Implement Bounded PostgreSQL Protocol 3.0 Startup Encoder and Backend Frame Decoder

## Objective & Scope Boundary

Implement the crate-private `protocol` module in `src/rs/vertebra-postgres/src/protocol.rs` providing bounded 16 MiB backend message decoding (`read_message`) and Protocol 3.0 (`196608`) UTF-8 startup frame encoding (`encode_startup_frame`) backed by `postgres-protocol = "=0.6.12"` [^proto-research] [^pg-contracts]. Socket connection lifecycles, `SSLRequest`, authentication state machines, and `CopyBothResponse` replication extensions belong to downstream tasks (`ch02:t02`–`ch02:t04` and `ch22:t01`, [../spec.md](../spec.md)).

## Bounded Read Manifest

- `src/rs/vertebra-postgres/src/lib.rs` — Crate root module declarations from `ch01:t04`.
- `src/rs/vertebra-postgres/src/config.rs` — `PostgresConfig` (`user`, `database`, `application_name`) from `ch01:t04`.
- `src/rs/vertebra-postgres/src/errors.rs` — `PostgresError::{Transport, Protocol, Server}` categories from `ch01:t04`.
- `.agents/bundles/specs/native-postgres-driver/contracts.md` — Startup parameter and protocol bounds ([../../../contracts.md](../../../contracts.md)).
- `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — `postgres_protocol::message::{frontend, backend}` APIs [^proto-research].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/lib.rs`:
  - Declare `pub(crate) mod protocol;` (strictly crate-private; zero test-only `pub` exports).
- `src/rs/vertebra-postgres/src/protocol.rs`:
  - `pub(crate) const MAX_BACKEND_FRAME_LENGTH: i32 = 16_777_216;` (`16 MiB` inclusive of the 4-byte length word).
  - `pub(crate) const MIN_BACKEND_FRAME_LENGTH: i32 = 4;`
  - `pub(crate) async fn read_message<R: AsyncRead + Unpin>(reader: &mut R) -> Result<postgres_protocol::message::backend::Message, PostgresError>`:
    1. Read exact 5-byte header `[tag, b0, b1, b2, b3]` via `reader.read_exact(&mut header)` (mapping `UnexpectedEof` / I/O errors to `PostgresError::Transport` or `PostgresError::Protocol` when truncated mid-header).
    2. Parse `raw_len = i32::from_be_bytes([b0, b1, b2, b3])` and validate `(4..=16_777_216).contains(&raw_len)` **before** allocating or reserving body capacity.
    3. Allocate `BytesMut::with_capacity(1 + raw_len as usize)` (exact frame size), copy the 5-byte header, resize to `1 + raw_len as usize`, and `reader.read_exact(&mut buf[5..])`.
    4. Parse via `postgres_protocol::message::backend::Message::parse(&mut buf)`: require `Ok(Some(msg))` AND `buf.is_empty()` (rejecting `Ok(None)`, `Err(_)`, or trailing unconsumed bytes inside the declared frame with `PostgresError::Protocol`).
  - `pub(crate) fn encode_startup_frame(config: &PostgresConfig) -> Result<BytesMut, PostgresError>`:
    - Delegate to `postgres_protocol::message::frontend::startup_message` with `[("user", config.user.as_str()), ("database", config.database.as_str()), ("application_name", config.application_name.as_str()), ("client_encoding", "UTF8")]` (emitting protocol version `196608` = `0x0003_0000`). Do not reuse or extend `vertebra_db`'s handwritten startup helper (which omits `client_encoding=UTF8`).
  - `pub(crate) const TERMINATE_FRAME: [u8; 5] = *b"X\0\0\0\x04";`

## Edge Cases & Error Semantics

- Signed/short lengths `raw_len <= 3` (`-1`, `0`, `1`, `2`, `3`) and oversized lengths `raw_len > 16_777_216` (`16_777_217`, `i32::MAX`) fail immediately with `PostgresError::Protocol` before any heap body allocation occurs.
- Exact `raw_len == 16_777_216` passes header validation without unbounded `Vec`/`BytesMut` over-reservation and fails cleanly with `PostgresError::Transport` / `PostgresError::Protocol` if the stream closes at EOF before delivering the remaining `16_777_212` bytes.
- Byte-at-a-time (`1`-byte chunk writes) and coalesced back-to-back frames (`AuthenticationOk` + `ParameterStatus` + `ReadyForQuery` in a single write) decode identically without losing or over-reading adjacent bytes on the `AsyncRead` stream.
- Unknown message tags (e.g. `'?'`) or malformed body payloads (e.g. `ParameterStatus` missing NUL terminator, or `AuthenticationOk` with trailing garbage byte inside declared length) fail explicitly with `PostgresError::Protocol`.

## Public-Seam Test Blueprint

- **Canonical test module**: Colocated `src/rs/vertebra-postgres/src/protocol.rs` (`#[cfg(test)] mod tests`) exercising `read_message` over `tokio::io::duplex` and `encode_startup_frame` (the crate-private deep framing seam; `ch02:t02` exercises the same functions over real TCP sockets).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Bytewise & adjacent valid frames | Write `[b'R', 0,0,0,8, 0,0,0,0]` (`AuthenticationOk`), `[b'S', 0,0,0,25, b"client_encoding\0UTF8\0"]`, `[b'Z', 0,0,0,5, b'I']` both coalesced and 1 byte at a time | Decodes `Message::AuthenticationOk`, `Message::ParameterStatus` (`"client_encoding"`, `"UTF8"`), and `Message::ReadyForQuery` (`b'I'`) in order. |
| EOF in header & mid-body | 3-byte truncated header `[b'R', 0, 0]` or header `[b'R', 0,0,0,8]` followed by 2 bytes then EOF | Returns `Err(PostgresError::Transport(_))` or `Err(PostgresError::Protocol(_))`. |
| Short & negative lengths | `[b'Z', 0,0,0,0]`, `[b'Z', 0,0,0,3]`, `[b'Z', 0xff,0xff,0xff,0xff]` | Each returns `Err(PostgresError::Protocol(_))` with zero body bytes read. |
| Oversized `> 16 MiB` frame | Header `[b'D', 0x01, 0x00, 0x00, 0x01]` (`16_777_217`) on a 5-byte duplex stream | Returns `Err(PostgresError::Protocol(_))` immediately before reading body or allocating 16 MiB. |
| Max `16 MiB` header with early EOF | Header `[b'D', 0x01, 0x00, 0x00, 0x00]` (`16_777_216`) followed immediately by writer drop | Header length check passes without panic/unbounded reserve; body read returns `Err` on EOF. |
| Trailing unconsumed bytes & unknown tag | `[b'R', 0,0,0,9, 0,0,0,0, 0xaa]` or `[b'?', 0,0,0,4]` | Both return `Err(PostgresError::Protocol(_))`. |
| Startup frame encoding | `encode_startup_frame` for `user="alice"`, `database="analytics"`, `application_name="vertebra"` | Frame bytes `[4..8] == [0, 3, 0, 0]` (`196608`), payload contains `"user\0alice\0"`, `"database\0analytics\0"`, `"application_name\0vertebra\0"`, `"client_encoding\0UTF8\0\0"`, and total big-endian `i32` prefix `[0..4]` equals `buf.len() as i32`. |

## Verification

Write the colocated `protocol::tests` cases first (confirming Red), implement `protocol.rs`, and verify exit `0`:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --lib protocol::tests
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
```

Commit as `feat(postgres): bound wire message decoding and startup framing`.

## Acceptance Criteria

- [x] `protocol::read_message` validates `4..=16_777_216` header lengths before allocating body buffers and delegates frame parsing to `postgres_protocol::message::backend::Message::parse` with zero unconsumed trailing bytes (R3).
- [x] `protocol::encode_startup_frame` encodes Protocol 3.0 (`196608`) startup packets including `user`, `database`, `application_name`, and `client_encoding = "UTF8"` via `postgres_protocol::message::frontend::startup_message` (R3).
- [x] `protocol` remains `pub(crate)` with all unit tests and workspace Clippy checks exiting `0` (R3).

## Notes & Discoveries

- [2026-11-07 05:00] `postgres_protocol::message::backend::Message::parse` consumes from a `BytesMut` starting at the tag byte (`buf[0]`), so passing a single exact-sized `5 + (len - 4)` `BytesMut` preserves zero-copy `Bytes` slices inside decoded `Message` variants while guaranteeing `buf.is_empty()` on exact consumption.

[^proto-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — `postgres-protocol =0.6.12` frontend/backend framing analysis.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Startup defaults and bounded framing rules.
