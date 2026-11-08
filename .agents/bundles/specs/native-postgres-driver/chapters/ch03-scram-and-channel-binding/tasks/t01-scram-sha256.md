---
type: Task
id: "native-postgres-driver:ch03:t01"
chapter_id: ch03-scram-and-channel-binding
title: Authenticate ordered SCRAM-SHA-256 SASL exchanges
description: Extend auth::Authentication with ordered SCRAM-SHA-256 SASL phases over postgres-protocol 0.6.12, enforcing 1..=100_000 iteration bounds, strict server nonce extension, and post-PBKDF2 deadline checks.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch02:t04"
read_set:
  - src/rs/vertebra-postgres/src/auth.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/protocol.rs
  - src/rs/vertebra-postgres/tests/postgres_auth_wire_tests.rs
  - .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
files:
  - src/rs/vertebra-postgres/src/auth.rs
  - src/rs/vertebra-postgres/src/session.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_auth_wire_tests.rs
estimated_loc: 235
plan_revision: 7
plan_commit: 1c75af65
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
updated_at: "2026-11-07T23:59:30Z"
commit: null
sources:
  - id: proto-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: postgres-protocol 0.6.12 ScramSha256 SASL state machine, iteration cap, and nonce prefix caveats
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Startup SASL error classification and connection deadline invariants
tags:
  - postgres
  - scram
  - sasl
  - auth
---

# Task t01: Authenticate Ordered `SCRAM-SHA-256` SASL Exchanges

## Objective & Scope Boundary

Extend `src/rs/vertebra-postgres/src/auth.rs` and `session.rs` to authenticate ordered `SCRAM-SHA-256` startup exchanges using pinned `postgres_protocol::authentication::sasl::{ScramSha256, ChannelBinding, SCRAM_SHA_256, SCRAM_SHA_256_PLUS}` (`=0.6.12`), adding client-side `1..=100_000` PBKDF2 iteration validation, strict server-nonce extension checks, verified-before-`AuthenticationOk` phase gating, and post-PBKDF2 deadline verification [^proto-research] [^pg-contracts]. Peer certificate `tls-server-end-point` digests (`ch03:t02`) and `SCRAM-SHA-256-PLUS` / `GS2` `y,,` policy selection (`ch03:t03`) are out of scope ([../spec.md](../spec.md)).

## Bounded Read Manifest

- `src/rs/vertebra-postgres/src/auth.rs` — `AuthPhase` and `Authentication::handle_message` from `ch02:t04`.
- `src/rs/vertebra-postgres/src/session.rs` — `startup(config, deadline)` loop called by `ConnectAttempt::run` and original reservation-deadline enforcement.
- `src/rs/vertebra-postgres/src/protocol.rs` — Bounded `Message::{AuthenticationSasl, AuthenticationSaslContinue, AuthenticationSaslFinal, AuthenticationOk}` decoding.
- `src/rs/vertebra-postgres/tests/postgres_auth_wire_tests.rs` — Scripted authentication server fixtures from `ch02:t04`.
- `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — Upstream `ScramSha256` behavior (prefix-only nonce check, acceptance of zero iterations despite the upstream `100_000` cap, and `message()` panic/misuse after `finish()`) [^proto-research].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/auth.rs`:
  - Extend `AuthPhase` with `ScramStarted { scram: ScramSha256, client_nonce: String }`, `ScramFinalSent(ScramSha256)`, `ScramVerifiedAwaitingOk`, and `Complete` (preserving `Initial` and `PasswordSent`).
  - `Message::AuthenticationSasl(body)` (valid ONLY in `AuthPhase::Initial`):
    1. Parse mechanism list via `body.mechanisms()`, rejecting empty lists or malformed non-UTF8/unterminated entries with `PostgresError::Protocol`.
    2. In `ch03:t01`, select `SCRAM_SHA_256` (`"SCRAM-SHA-256"`) when offered alone or alongside `SCRAM_SHA_256_PLUS`; if ONLY `SCRAM_SHA_256_PLUS` (or unknown mechanisms) are offered, return `Err(PostgresError::UnsupportedAuth(_))` (`ch03:t03` adds `PLUS` selection).
    3. Require `config.password.as_deref()` (missing `None` -> `Err(PostgresError::Config(_))` before emitting bytes).
    4. Construct `let scram = ScramSha256::new(password.as_bytes(), ChannelBinding::unsupported());`, extract the initial `r=` client nonce from `scram.message()`, encode `frontend::sasl_initial_response(SCRAM_SHA_256, scram.message(), out)`, and transition to `AuthPhase::ScramStarted { scram, client_nonce }`.
  - `Message::AuthenticationSaslContinue(body)` (valid ONLY in `AuthPhase::ScramStarted`):
    1. Minimally pre-validate `body.data()` before calling `scram.update`: require exactly one `i=` attribute consisting solely of ASCII digits parsing as `u32` within `1..=100_000` (rejecting `0`, `100_001`, non-decimal characters, `+`/`-` signs, `u32` overflow, or duplicate `i=`), and require the `r=` attribute strictly extends `client_nonce` (`server_nonce.len() > client_nonce.len() && server_nonce.starts_with(&client_nonce)`, closing upstream's prefix-only check that accepts `server_nonce == client_nonce`). Leave SASLprep, base64 decoding, PBKDF2/HMAC crypto, and full RFC 5802 attribute grammar to `scram.update(body.data())`.
    2. Map `scram.update(body.data())` errors to `PostgresError::Protocol`, encode `frontend::sasl_response(scram.message(), out)`, and transition to `AuthPhase::ScramFinalSent(scram)`.
  - `Message::AuthenticationSaslFinal(body)` (valid ONLY in `AuthPhase::ScramFinalSent`):
    1. Call `scram.finish(body.data())` (mapping server verifier mismatch or malformed `v=`/`e=` to `PostgresError::Protocol`). Never call `scram.message()` after `finish()`.
    2. Transition to `AuthPhase::ScramVerifiedAwaitingOk` without emitting outbound bytes.
  - `Message::AuthenticationOk`:
    - Accepted ONLY in `Initial`, `PasswordSent`, or `ScramVerifiedAwaitingOk` -> `AuthPhase::Complete`; rejected with `PostgresError::Protocol` in `ScramStarted`, `ScramFinalSent`, or `Complete`.
- `src/rs/vertebra-postgres/src/session.rs`:
  - In `startup(config, deadline)`, after `Authentication::handle_message` returns from CPU-bound `AuthenticationSaslContinue` PBKDF2 work and before flushing `out` or returning a publishable connection, check `tokio::time::Instant::now() >= deadline` and abort with `PostgresError::Transport` if the original reservation budget elapsed during PBKDF2. `PendingSession::commit` retains its existing check against that same deadline.

## Edge Cases & Error Semantics

- Iteration counts `0` and `100_001` (no million-iteration custom cap), non-decimal `i=4096x` / `i=+4096`, `u32` overflow `i=999999999999`, and duplicate `i=4096,i=4096` fail before `scram.update` runs PBKDF2.
- Server nonces equal to the client nonce (`r=<client_nonce>` with zero server entropy) or with a mutated prefix fail before `scram.update` with `PostgresError::Protocol`.
- Wrong `v=` server signature in `AuthenticationSaslFinal`, duplicate `AuthenticationSaslContinue`, duplicate `AuthenticationSaslFinal`, or premature `AuthenticationOk` prior to `AuthenticationSaslFinal` fail terminally with `PostgresError::Protocol` and close the socket.
- Missing `config.password` fails locally with `PostgresError::Config` before sending `SASLInitialResponse`, and error/`Debug` output never exposes the password or derived keys.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/postgres_auth_wire_tests.rs` (test names prefixed `scram_`). Test-only server-fixture code uses `hmac`, `sha2`, and `base64` dev-dependencies to derive `ServerKey` and `ServerSignature` (`v=`) against the live client nonce and auth message (never used in production crate code), and `tokio-postgres` (`=0.7.18` dev-dependency) acts as an independent reference client oracle against the same scripted server fixture.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `scram_sha256_golden_and_oracle` | Server offers `["SCRAM-SHA-256"]` (and `["SCRAM-SHA-256-PLUS", "SCRAM-SHA-256"]`), `password = "secret"`, raw salt `b"vtb-test-salt"` (`s=dnRiLXRlc3Qtc2FsdA==`), `i = 4096` (salted-password hex `"2482c2af90bd00962845709770664bb70a090bcbd937e9d1fa1f855044d513bb"`), valid `v=` + `AuthenticationOk` | Both `tokio-postgres` dev oracle and `vertebra-postgres` `SessionHandle` select `"SCRAM-SHA-256"` with `GS2` `"n,,"`, verify `v=`, and commit `Ready`. |
| `scram_mechanism_list_rejections` | Server offers empty `[]`, malformed non-UTF8 mechanism bytes, `["SCRAM-SHA-256-PLUS"]` only, or `["GSSAPI"]` | Empty/malformed returns `Err(PostgresError::Protocol(_))`; `PLUS`-only (until `ch03:t03`) and unknown mechanisms return `Err(PostgresError::UnsupportedAuth(_))`. |
| `scram_iteration_bounds` | `AuthenticationSaslContinue` with `i=0`, `i=100001`, `i=4096abc`, `i=42949672960`, or `r=...,s=...,i=4096,i=4096`; plus boundary `i=1` and `i=100000` | Invalid `i=` inputs return `Err(PostgresError::Protocol(_))` with zero `SASLResponse` bytes sent; `i=1` and `i=100000` succeed. |
| `scram_nonce_strict_extension` | Server echoes exact `r=<client_nonce>` (zero appended bytes) or `r=wrongprefix<server>` | Returns `Err(PostgresError::Protocol(_))` before sending `SASLResponse` and closes socket. |
| `scram_wrong_server_verifier` | Server sends `AuthenticationSaslFinal` with `v=` computed from wrong password or flipped byte | `ScramSha256::finish` rejects verifier with `Err(PostgresError::Protocol(_))` and closes socket without `Ready`. |
| `scram_phase_order_violations` | Duplicate `AuthenticationSaslContinue`, duplicate `AuthenticationSaslFinal`, or premature `AuthenticationOk` immediately after `SASLInitialResponse` / `SASLResponse` (skipping `SASLFinal`) | Each returns `Err(PostgresError::Protocol(_))` and transitions `SessionHandle` to `Closed`. |

## Verification

Write the `scram_*` tests and `tokio-postgres` fixture check in `src/rs/vertebra-postgres/tests/postgres_auth_wire_tests.rs` first (confirming Red), implement `auth.rs` and `session.rs` updates, and verify exit `0`:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_auth_wire_tests scram_
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres
```

Commit as `feat(postgres): validate SCRAM startup authentication`.

## Acceptance Criteria

- [ ] `auth::Authentication` drives `ScramStarted -> ScramFinalSent -> ScramVerifiedAwaitingOk -> Complete` via `postgres-protocol` `ScramSha256` without custom client crypto or calling `message()` after `finish()` (R5).
- [ ] Pre-update validation enforces `i in 1..=100_000` (single decimal attribute) and strict server nonce extension (`len > client_nonce.len()`), and `AuthenticationOk` is rejected unless `ScramVerifiedAwaitingOk` was reached (R5).
- [ ] `session.rs` rechecks the original connection deadline after bounded PBKDF2 before flushing or publishing `Ready` (R3, R5).

## Notes & Discoveries

- `postgres-protocol` `0.6.12` rejects iterations above `100_000` before PBKDF2, but accepts zero and checks only the nonce prefix. Local validation enforces the explicit `1..=100_000` policy and strict nonce extension before `scram.update`; upstream retains all cryptography and full SCRAM grammar checks.

[^proto-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — `postgres-protocol` `0.6.12` SASL SCRAM audit findings.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Startup SASL authentication and timeout invariants.
