---
type: Task
id: "native-postgres-driver:ch02:t04"
chapter_id: ch02-wire-codec-and-session-startup
title: Authenticate cleartext password and MD5 startup exchanges
description: Implement crate-private auth::Authentication state machine handling trust, cleartext password, and salted MD5 challenges via postgres-protocol 0.6.12 with strict phase ordering and secret redaction.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch02:t02"
  - "native-postgres-driver:ch02:t03"
read_set:
  - src/rs/vertebra-postgres/src/config.rs
  - src/rs/vertebra-postgres/src/errors.rs
  - src/rs/vertebra-postgres/tests/postgres_wire_tests.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/tests/support/mod.rs
  - .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
files:
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/src/auth.rs
  - src/rs/vertebra-postgres/src/session.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_auth_wire_tests.rs
  - src/rs/vertebra-postgres/tests/postgres_wire_tests.rs
estimated_loc: 210
plan_revision: 6
plan_commit: 8833b064
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-07T23:09:05Z"
last_verified_commit: "67421061"
verification_evidence:
  red: 'The password_ TCP suite exits 101 against the trust-only baseline: all 8 test groups fail before production changes.'
  focused: "cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_auth_wire_tests password_ exits 0: 8 real TCP peer test groups."
  aggregate: "cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres exits 0 under the shared SHM test lock: 9 codec, 8 auth, 10 TLS, and 20 existing wire/config/error tests; zero failures or ignores."
  lint: "cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings exits 0 without lint allowances."
  formatting: "cargo fmt --manifest-path src/rs/Cargo.toml -p vertebra-postgres -- --check and git diff --check exit 0."
  review_privacy: "UV_NO_SYNC=1 uv run --no-sync python -m tools.scripts.review_setup validate exits 0."
  environment: "UV_NO_SYNC=1; PYO3_PYTHON=${PWD}/.venv/bin/python; dedicated Cargo target; aggregate tests acquire the shared SHM test lock."
  checkpoint: "Completed source receipt: 67421061. Chapter checkpoint 7bf8e2e3 passes all 47 Rust tests and locked all-target Clippy with -D warnings."
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-07T23:31:15Z"
commit: null
sources:
  - id: proto-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: postgres-protocol 0.6.12 password_message and authentication::md5_hash APIs
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Startup authentication error classification and secret redaction invariants
tags:
  - postgres
  - auth
  - md5
  - password
---

# Task t04: Authenticate Cleartext Password and MD5 Startup Exchanges

## Objective & Scope Boundary

Implement the crate-private `auth::Authentication` state machine in `src/rs/vertebra-postgres/src/auth.rs` and wire it into `session.rs` to authenticate trust (`AuthenticationOk`), cleartext (`AuthenticationCleartextPassword`), and salted MD5 (`AuthenticationMd5Password`) startup challenges using `postgres_protocol::message::frontend::password_message` and `postgres_protocol::authentication::md5_hash` [^proto-research] [^pg-contracts]. `SCRAM-SHA-256` and `SCRAM-SHA-256-PLUS` SASL exchanges belong to Chapter 03 (`ch03:t01`–`ch03:t03`, [../spec.md](../spec.md)).

## Bounded Read Manifest

- `src/rs/vertebra-postgres/src/config.rs` — `PostgresConfig` (`user`, `password`).
- `src/rs/vertebra-postgres/src/errors.rs` — `PostgresError::{Config, Protocol, Server, UnsupportedAuth}`.
- `src/rs/vertebra-postgres/tests/postgres_wire_tests.rs` — Preserve the startup suite while migrating obsolete unsupported-cleartext expectations to password authentication coverage.
- `src/rs/vertebra-postgres/src/session.rs` — Startup frame loop in `ConnectAttempt::run` from `ch02:t02` and `ch02:t03`.
- `src/rs/vertebra-postgres/tests/support/mod.rs` — Loopback server and raw packet helpers.
- `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — Upstream `password_message` and `md5_hash` contracts [^proto-research].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/lib.rs`:
  - Declare `pub(crate) mod auth;`.
- `src/rs/vertebra-postgres/src/auth.rs`:
  - `#[derive(Debug, Clone, Copy, PartialEq, Eq)] enum AuthPhase { Initial, PasswordSent, Complete }`
  - `pub(crate) struct Authentication { phase: AuthPhase }`:
    - `pub(crate) fn new() -> Self` (`AuthPhase::Initial`).
    - `pub(crate) fn is_complete(&self) -> bool` (`self.phase == AuthPhase::Complete`).
    - `pub(crate) fn handle_message(&mut self, msg: &postgres_protocol::message::backend::Message, config: &PostgresConfig, out: &mut BytesMut) -> Result<bool, PostgresError>` returning `Ok(true)` when `msg` was an authentication message (writing any outbound response frame into `out`) and `Ok(false)` for non-auth messages:
      - `Message::AuthenticationOk`: allowed in `Initial` (trust) or `PasswordSent` -> sets `Complete`; in `Complete` returns `Err(PostgresError::Protocol(_))`.
      - `Message::AuthenticationCleartextPassword`: allowed ONLY in `Initial` (else `PostgresError::Protocol`); requires `config.password.as_deref()` (missing `None` -> `PostgresError::Config`); encodes `frontend::password_message(password.as_bytes(), out)` and sets `PasswordSent`.
      - `Message::AuthenticationMd5Password(body)`: allowed ONLY in `Initial` (else `PostgresError::Protocol`); requires `config.password.as_deref()` (missing `None` -> `PostgresError::Config`); computes `let digest = postgres_protocol::authentication::md5_hash(config.user.as_bytes(), password.as_bytes(), body.salt());`, encodes `frontend::password_message(digest.as_bytes(), out)`, and sets `PasswordSent`.
      - `Message::AuthenticationKerberosV5 | AuthenticationScmCredential | AuthenticationGss | AuthenticationSspi | AuthenticationGssContinue(_) | AuthenticationSasl(_)`: returns `Err(PostgresError::UnsupportedAuth(_))` (`AuthenticationSasl` is extended in `ch03:t01`).
      - `Message::AuthenticationSaslContinue(_) | AuthenticationSaslFinal(_)` in `Initial` / `PasswordSent` / `Complete`: returns `Err(PostgresError::Protocol(_))`.
- `src/rs/vertebra-postgres/src/session.rs`:
  - Delegate authentication `Message` frames in `ConnectAttempt::run` to `Authentication::handle_message`, flushing `out` under the single outer `deadline` whenever `!out.is_empty()`.
  - Accept `ParameterStatus`, `BackendKeyData`, and `ReadyForQuery(b'I')` strictly when `auth.is_complete()` is `true`; `NoticeResponse` leaves `Authentication` unchanged in any phase.

## Edge Cases & Error Semantics

- Missing `config.password` (`None`) on cleartext or MD5 challenge fails locally with `PostgresError::Config` before transmitting any `PasswordMessage` frame, and closes the socket.
- Wrong password causing server `ErrorResponse` `28P01` returns `PostgresError::Server(err)` (`err.sqlstate == "28P01"`), closes the socket (`EOF` observed by server), and never retries on the same stream.
- Duplicate challenges (`AuthenticationCleartextPassword` after `PasswordSent`, `AuthenticationMd5Password` after `AuthenticationCleartextPassword`, or second `AuthenticationOk`) fail with `PostgresError::Protocol`.
- Error strings and `Debug` representations never contain the configured password or computed MD5 digest.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/postgres_auth_wire_tests.rs` (test names prefixed `password_` for targeted verification) using `mod support;`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `password_cleartext_success` | `user = "alice"`, `password = Some("s3cr3t!")`, `sslmode = "disable"`, server sends `[b'R', 0,0,0,8, 0,0,0,3]` (`AuthenticationCleartextPassword`) | Client writes exact `[b'p', 0,0,0,12, b"s3cr3t!\0"]`; after `AuthenticationOk` + `UTF8` + `ReadyForQuery(b'I')`, session commits `Ready`. |
| `password_md5_success` | `user = "postgres"`, `password = Some("hunter2")`, salt `[0x01, 0x02, 0x03, 0x04]` (`[b'R', 0,0,0,12, 0,0,0,5, 1,2,3,4]`) | Client writes `PasswordMessage` containing exact 35-byte `"md5..."` + NUL matching `md5_hash(b"postgres", b"hunter2", [1,2,3,4])` (`"md5c73cff48cd454994b0c263a04cdfc859\0"`); session commits `Ready`. |
| `password_missing_rejected` | `password = None` against cleartext or MD5 challenge | Returns `Err(PostgresError::Config(_))`; server observes zero `PasswordMessage` bytes and immediate socket EOF. |
| `password_wrong_28p01_and_redaction` | `password = Some("ultra-secret-pw")`, server replies to `PasswordMessage` with `ErrorResponse` `C="28P01"`, `M="password authentication failed for user \"alice\""` | Returns `Err(PostgresError::Server(err))` with `err.sqlstate == "28P01"`, formatted error excludes `"ultra-secret-pw"`, and server observes socket EOF without retry. |
| `password_duplicate_challenge_or_ok` | Server sends `AuthenticationCleartextPassword` twice (without `AuthenticationOk`), or sends `AuthenticationOk` twice after password | Returns `Err(PostgresError::Protocol(_))` and closes socket. |
| `password_unsupported_mechanisms` | Server sends `AuthenticationKerberosV5` (`2`), `AuthenticationGss` (`7`), `AuthenticationSspi` (`9`), or `AuthenticationScmCredential` (`6`) | Each returns `Err(PostgresError::UnsupportedAuth(_))` and closes socket. |

## Verification

Write the `password_*` scripted server tests in `src/rs/vertebra-postgres/tests/postgres_auth_wire_tests.rs` first (confirming Red), implement `auth.rs` and `session.rs` wiring, and verify exit `0`:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_auth_wire_tests password_
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
```

Commit as `feat(postgres): authenticate password and MD5 connections`.

## Acceptance Criteria

- [x] `auth::Authentication` handles trust, cleartext, and MD5 challenges strictly from `Initial`, delegating framing and hashing to `postgres_protocol` without custom crypto (R5).
- [x] Missing passwords, `28P01` server rejections, duplicate challenges/OKs, and unsupported GSS/SSPI/Kerberos challenges fail terminally with redacted diagnostics and immediate socket closure (R5).
- [x] `session.rs` rejects `ParameterStatus`, `BackendKeyData`, and `ReadyForQuery` prior to `Authentication::is_complete()` while tolerating `NoticeResponse` in any phase (R3, R5).

## Notes & Discoveries

- Authentication responses use the pinned upstream encoders and MD5 helper. The TCP peer checks the literal `postgres`/`hunter2`/`01020304` response `md5c73cff48cd454994b0c263a04cdfc859` without deriving its expected value through production hashing.
- Error formatting and Debug checks exclude both the configured password and the actual response captured by the peer. Missing credentials and unsupported mechanisms produce zero outbound password bytes; duplicate and server-rejected exchanges close without retry.
- The startup suite covers 20 wire/configuration/error cases; the canonical password suite covers cleartext/MD5 exchanges and unsupported Kerberos, SCM, GSS, and SSPI challenges. Shared peer fixtures preserve deterministic publication-deadline barriers.

[^proto-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — `postgres-protocol` password and MD5 authentication helpers.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Startup authentication error mapping and redaction rules.
