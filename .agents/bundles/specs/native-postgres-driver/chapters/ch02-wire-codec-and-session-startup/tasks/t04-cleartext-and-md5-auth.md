---
type: Task
id: "native-postgres-driver:ch02:t04"
chapter_id: ch02-wire-codec-and-session-startup
title: Authenticate cleartext password and MD5 startup exchanges
description: Implement crate-private auth::Authentication state machine handling trust, cleartext password, and salted MD5 challenges via postgres-protocol 0.6.12 with strict phase ordering and secret redaction.
status: stable
state: completed
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
state_revision: 2
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-07T23:43:16Z"
last_verified_commit: c5173f1b
verification_evidence:
  red: "Committed pre-fix session consumer fails all three password_server_echo regressions with exit 101, including independent cleartext and actual captured MD5 wire credentials in Display; the protocol-preservation regression separately fails against the C-only exemption."
  focused: "cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --locked --test postgres_auth_wire_tests password_ exits 0: 13 real TCP authentication test groups."
  aggregate: "flock /tmp/vertebra-flow-shm-tests.lock cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --locked exits 0: 54 tests (9 codec, 13 auth, 10 TLS, 22 wire/config/error), zero failures or ignores."
  lint: "cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets --locked -- -D warnings exits 0 without lint allowances."
  formatting: "cargo fmt --manifest-path src/rs/Cargo.toml -p vertebra-postgres -- --check and git diff --check exit 0."
  review_privacy: "UV_NO_SYNC=1 uv run --no-sync python -m tools.scripts.review_setup validate exits 0."
  environment: "UV_NO_SYNC=1; PYO3_PYTHON=${PWD}/.venv/bin/python; dedicated Cargo target; aggregate tests acquire the shared SHM test lock."
  checkpoint: "Completed repair receipt binds to c5173f1b. Exact chapter checkpoint 0446ee46 passes all 54 tests and locked all-target Clippy with -D warnings."
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-07T23:44:42Z"
commit: c5173f1b
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
- `src/rs/vertebra-postgres/tests/postgres_wire_tests.rs` — Preserve the startup lifecycle and lazy diagnostic-body validation coverage.
- `src/rs/vertebra-postgres/src/session.rs` — Startup frame loop in `ConnectAttempt::run` from `ch02:t02` and `ch02:t03`.
- `src/rs/vertebra-postgres/tests/support/mod.rs` — Loopback server and raw packet helpers.
- `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — Upstream `password_message` and `md5_hash` contracts [^proto-research].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/lib.rs`:
  - Declare `pub(crate) mod auth;`.
- `src/rs/vertebra-postgres/src/auth.rs`:
  - `#[derive(Debug, Clone, Copy, PartialEq, Eq)] enum AuthPhase { Initial, PasswordSent, Complete }`
  - `pub(crate) struct Authentication { phase: AuthPhase, md5_response: Option<String> }`:
    - `pub(crate) fn new() -> Self` (`AuthPhase::Initial`).
    - `pub(crate) fn is_complete(&self) -> bool` (`self.phase == AuthPhase::Complete`).
    - `pub(crate) fn redact_server_text(&self, text: &str, config: &PostgresConfig) -> String` masks the actual encoded MD5 response and non-empty configured password before startup free-text diagnostic ownership; severity, SQLSTATE, numeric positions, and source-line protocol fields remain unchanged. The private authentication object retains only the actual challenge response and has no `Debug` implementation.
    - `pub(crate) fn handle_message(&mut self, msg: &postgres_protocol::message::backend::Message, config: &PostgresConfig, out: &mut BytesMut) -> Result<bool, PostgresError>` returning `Ok(true)` when `msg` was an authentication message (writing any outbound response frame into `out`) and `Ok(false)` for non-auth messages:
      - `Message::AuthenticationOk`: allowed in `Initial` (trust) or `PasswordSent` -> sets `Complete`; in `Complete` returns `Err(PostgresError::Protocol(_))`.
      - `Message::AuthenticationCleartextPassword`: allowed ONLY in `Initial` (else `PostgresError::Protocol`); requires `config.password.as_deref()` (missing `None` -> `PostgresError::Config`); encodes `frontend::password_message(password.as_bytes(), out)` and sets `PasswordSent`.
      - `Message::AuthenticationMd5Password(body)`: allowed ONLY in `Initial` (else `PostgresError::Protocol`); requires `config.password.as_deref()` (missing `None` -> `PostgresError::Config`); computes `let digest = postgres_protocol::authentication::md5_hash(config.user.as_bytes(), password.as_bytes(), body.salt());`, encodes `frontend::password_message(digest.as_bytes(), out)`, retains that actual response for diagnostic redaction, and sets `PasswordSent`.
      - `Message::AuthenticationKerberosV5 | AuthenticationScmCredential | AuthenticationGss | AuthenticationSspi | AuthenticationGssContinue(_) | AuthenticationSasl(_)`: returns `Err(PostgresError::UnsupportedAuth(_))` (`AuthenticationSasl` is extended in `ch03:t01`).
      - `Message::AuthenticationSaslContinue(_) | AuthenticationSaslFinal(_)` in `Initial` / `PasswordSent` / `Complete`: returns `Err(PostgresError::Protocol(_))`.
- `src/rs/vertebra-postgres/src/session.rs`:
  - Delegate authentication `Message` frames in `ConnectAttempt::run` to `Authentication::handle_message`, flushing `out` under the single outer `deadline` whenever `!out.is_empty()`.
  - Accept `ParameterStatus`, `BackendKeyData`, and `ReadyForQuery(b'I')` strictly when `auth.is_complete()` is `true`; `NoticeResponse` validates all lazy fields and leaves `Authentication` unchanged in any phase. Startup `ErrorResponse` free text is redacted before constructing typed server errors; protocol tags `S`, `V`, `C`, `P`, `p`, and `L` remain exact even when they coincide with a low-entropy password.

## Edge Cases & Error Semantics

- Missing `config.password` (`None`) on cleartext or MD5 challenge fails locally with `PostgresError::Config` before transmitting any `PasswordMessage` frame, and closes the socket.
- Wrong password causing server `ErrorResponse` `28P01` returns `PostgresError::Server(err)` (`err.sqlstate == "28P01"`), closes the socket (`EOF` observed by server), and never retries on the same stream.
- Duplicate challenges (`AuthenticationCleartextPassword` after `PasswordSent`, `AuthenticationMd5Password` after `AuthenticationCleartextPassword`, or second `AuthenticationOk`) fail with `PostgresError::Protocol`.
- Untrusted human/free-text diagnostics never reflect the non-empty configured password or actual submitted MD5 token into error strings or `Debug` fields. Structured severity, SQLSTATE, numeric positions, and source-line protocol values remain exact even when they coincide with a low-entropy password; private credential storage has no `Debug` implementation.

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
| `password_server_echo_cleartext_is_redacted` / `password_server_echo_md5_is_redacted` | Server reflects configured password and actual captured response into `M`, `D`, and `W` | Typed `28P01`/`FATAL`, safe hint and position remain; free-text values become `<redacted>`, Display/Debug omit noncoincidental credentials, and peer observes EOF. |
| `password_redaction_preserves_protocol_fields` | Password `17` or `FATAL`, server echoes it into `M`/`D` and sends structured `S/V=FATAL`, `C=28P01`, `P/p/L=17` | Free-text echoes are masked; every structured protocol field remains exact, and peer observes EOF. |

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
- [x] Missing passwords, `28P01` server rejections, duplicate challenges/OKs, and unsupported GSS/SSPI/Kerberos challenges fail terminally with credential-safe free-text diagnostics and immediate socket closure; typed protocol values remain exact when coincidentally equal to a low-entropy password (R5).
- [x] `session.rs` rejects `ParameterStatus`, `BackendKeyData`, and `ReadyForQuery` prior to `Authentication::is_complete()` while tolerating `NoticeResponse` in any phase (R3, R5).

## Notes & Discoveries

- Authentication responses use the pinned upstream encoders and MD5 helper. The TCP peer checks the literal `postgres`/`hunter2`/`01020304` response `md5c73cff48cd454994b0c263a04cdfc859` without deriving its expected value through production hashing.
- Startup free-text diagnostics mask the non-empty configured password and actual MD5 response before constructing `ServerError`; replacing the response first protects credentials containing a password substring. Pre-challenge errors redact the configured password; empty passwords leave server text unchanged. Credential-bearing authentication storage has no `Debug` implementation.
- Structured `S`/`V`/`C`/`P`/`p`/`L` protocol fields remain exact, including passwords `17` and `FATAL` that coincide with legitimate values. Safe hint text remains observable. Real peers verify redacted credential echoes in primary, detail, and context text through Display/Debug plus socket EOF. Missing credentials and unsupported mechanisms produce zero outbound password bytes; duplicate and server-rejected exchanges close without retry.
- The startup suite covers 22 wire/configuration/error cases; the canonical password suite covers cleartext/MD5 exchanges and unsupported Kerberos, SCM, GSS, and SSPI challenges. Cleartext success accepts valid notices before challenge, after the password response, and after AuthenticationOk; MD5 success also accepts notices before challenge and while PasswordSent. Shared peer fixtures preserve deterministic publication-deadline barriers.

[^proto-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — `postgres-protocol` password and MD5 authentication helpers.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Startup authentication error mapping and redaction rules.
