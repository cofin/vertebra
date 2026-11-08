---
type: Task
id: "native-postgres-driver:ch03:t01"
chapter_id: ch03-scram-and-channel-binding
title: Authenticate ordered SCRAM-SHA-256 SASL exchanges
description: Extend auth::Authentication with ordered SCRAM-SHA-256 SASL phases over postgres-protocol 0.6.12, enforcing 1..=100_000 iteration bounds, strict server nonce extension, and post-PBKDF2 deadline checks.
status: stable
state: completed
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
plan_revision: 8
plan_commit: 8c540809
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-08T00:21:07Z"
last_verified_commit: 9b94bff8
verification_evidence: |
  Preflight: chapter 02 completed/reviewed at 27b5a9e5; root/chapter/task plan revision 7 and anchor 1c75af65 match; declared source/test symbols exist.
  Red: the independent tokio-postgres golden handshake succeeds, then the unchanged native client rejects SCRAM with UnsupportedAuth; focused cargo test exits 101.
  Green: all 10 scram_* groups pass, including the literal PBKDF2 golden, plain/both mechanism offers, independent tokio-postgres oracle, iteration boundaries 1/100000, malformed lists/counts/nonces, phase/verifier refusal, missing password, and interleaved native notices.
  Deadline mutation: removing only the explicit post-handle/preflush guard makes scram_deadline_expired_before_ready_continue fail with transmitted SASLResponse (exit 101); restoring the guard passes (exit 0). The restored deterministic case passes 20/20 consecutive runs.
  UV_NO_SYNC=1 PYO3_PYTHON=${PWD}/.venv/bin/python CARGO_TARGET_DIR=/tmp/vtb-cargo-target flock /tmp/vertebra-flow-shm-tests.lock cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --locked: exit 0, 64 tests (9 codec, 23 auth, 10 TLS, 22 wire/config/error), no skips.
  Locked all-target Clippy with -D warnings, package rustfmt --check, no-sync review_setup validation, and git diff --check pass. No Python exports or bindings change.
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-08T00:23:12Z"
commit: 9b94bff8
sources:
  - id: proto-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: postgres-protocol 0.6.12 ScramSha256 SASL state machine, iteration cap, and nonce prefix caveats
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Startup SASL error classification and connection deadline invariants
  - id: tokio-timeout
    resource: https://raw.githubusercontent.com/tokio-rs/tokio/tokio-1.54.1/tokio/src/time/timeout.rs
    title: Pinned Tokio Timeout polls its inner future before the deadline timer
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
| `scram_deadline_expired_before_ready_continue` | Paused Tokio clock, one-second reservation, valid `i=4096` server-first message queued after the peer reads `SASLInitialResponse`; the public client future remains unpolled until a fresh counting waker proves socket readiness, then the clock advances to reservation + `1001 ms` | Resuming the client performs bounded SCRAM work but returns `Transport` before flushing `SASLResponse`; the handle is terminal `Closed`, reconnection is refused, and the peer reaches EOF with zero response bytes. Removing only the explicit post-authentication/preflush deadline check makes this same test fail on nonempty outbound bytes. |


### Deterministic Deadline Fixture

The canonical auth test uses only the public `SessionHandle` / `ConnectAttempt::run` seam, real loopback TCP, `std::task::{Wake, Waker, Context}`, and existing Tokio test utilities. The locked Tokio `1.54.1` timeout polls its inner future before checking the expired timer; an outer timeout alone can therefore permit an already-ready inner operation to write after expiry [^tokio-timeout]. The test parks the client rather than timing PBKDF2 on the host:

1. Use `#[tokio::test(start_paused = true)]`, `sslmode = Disable`, password `"secret"`, and `connect_timeout = 1 s`. Capture `reserved_at = Instant::now()` immediately before `begin_connect()`, and keep `let mut client = Box::pin(attempt.run())` local to the test. Never spawn the client future.
2. The peer reads startup, offers plain SCRAM, reads `SASLInitialResponse`, extracts its live client nonce, and acknowledges that read through a oneshot. It then waits on a separate `send_continue` oneshot before sending anything else. The test manually polls the pinned client with a counting `Wake` implementation while polling this peer-stage acknowledgement with `try_recv`; it yields between polls. Each stage has an independent generous `std::time::Instant` watchdog (30 seconds), so the test stays runnable, Tokio does not automatically advance its paused clock, and broken fixture I/O cannot spin forever.
3. After the initial-response acknowledgement, create a **fresh** `Arc<CountingWake>` with zero wakes and poll the client exactly once using a `Context` built from its `Waker`. Require `Poll::Pending`, wake count zero, and `Instant::now() == reserved_at < reserved_at + 1 s`. The peer still withholds Continue, so this registers a new socket-read waker while the reservation timer is in the future. No test code manually wakes this waker.
4. Release `send_continue`. The peer sends and flushes one complete valid Continue frame with `r=<client_nonce>server`, `s=dnRiLXRlc3Qtc2FsdA==`, and `i=4096`, then acknowledges the completed write through another oneshot and waits to collect subsequent client bytes until EOF. The test leaves `client` completely unpolled and busy-yields with the same independent watchdog until both the write acknowledgement and the **fresh waker's nonzero count** are observed. Assert Tokio time is still exactly `reserved_at`; the future timer cannot have caused this wake. This public reactor-readiness barrier establishes that the queued frame is ready before the expired client is resumed. A peer write acknowledgement alone is insufficient.
5. With the client still unpolled, call `tokio::time::advance(Duration::from_millis(1001)).await`. Assert the clock is beyond the original reservation deadline, then await the pinned client. Its inner startup future can consume the ready Continue and run the valid bounded SCRAM update; the explicit post-work/preflush check must return `PostgresError::Transport` before any response is written.
6. Assert terminal `handle.is_closed()`, empty parameter metadata, and refusal of another `begin_connect()`. Resume Tokio time only after the client result is observed, then await the peer under the existing EOF watchdog and require its collected bytes to be empty. No server-final, `AuthenticationOk`, or `ReadyForQuery` is sent in this case.
7. After the implemented test is Green, temporarily remove **only** the explicit post-`Authentication::handle_message` / preflush deadline check from `startup(config, deadline)` and rerun this exact case. It must turn Red because the peer receives `SASLResponse` bytes even though the outer timeout may still return `Transport`. Restore the check and require Green again; record both exit codes before committing. The production clock, auth state machine, and socket are never mocked or exposed through a test-only API.

## Verification

Write the `scram_*` tests and `tokio-postgres` fixture check in `src/rs/vertebra-postgres/tests/postgres_auth_wire_tests.rs` first (confirming Red), implement `auth.rs` and `session.rs` updates, prove the focused deadline test fails when only its explicit preflush guard is removed, restore that guard, and verify exit `0`:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_auth_wire_tests scram_deadline_expired_before_ready_continue
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_auth_wire_tests scram_
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres
```

Commit as `feat(postgres): validate SCRAM startup authentication`.

## Acceptance Criteria

- [x] `auth::Authentication` drives `ScramStarted -> ScramFinalSent -> ScramVerifiedAwaitingOk -> Complete` via `postgres-protocol` `ScramSha256` without custom client crypto or calling `message()` after `finish()` (R5).
- [x] Pre-update validation enforces `i in 1..=100_000` (single decimal attribute) and strict server nonce extension (`len > client_nonce.len()`), and `AuthenticationOk` is rejected unless `ScramVerifiedAwaitingOk` was reached (R5).
- [x] `session.rs` rechecks the original connection deadline after bounded PBKDF2 before flushing or publishing `Ready`; the deterministic parked-future/real-TCP test proves terminal failure with zero `SASLResponse` bytes and turns Red when only that explicit check is removed (R3, R5).

## Notes & Discoveries

- `postgres-protocol` `0.6.12` rejects iterations above `100_000` before PBKDF2, but accepts zero and checks only the nonce prefix. Local validation enforces the explicit `1..=100_000` policy and strict nonce extension before `scram.update`; upstream retains all cryptography and full SCRAM grammar checks.
- The independent `tokio-postgres` oracle validates the same core SCRAM peer exchange with plain and combined mechanism offers; native-only coverage adds notices at every authentication phase because the oracle rejects notices during authentication.
- The public deadline fixture registers a fresh read waker while the peer withholds Continue, observes its wake before any clock advance, and resumes the unspawned client only after expiry. Its guard-removal failure proves zero outbound credentials rather than merely observing an outer timeout.

[^proto-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — `postgres-protocol` `0.6.12` SASL SCRAM audit findings.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Startup SASL authentication and timeout invariants.

[^tokio-timeout]: `https://raw.githubusercontent.com/tokio-rs/tokio/tokio-1.54.1/tokio/src/time/timeout.rs` — `Timeout::poll` polls the inner future before its deadline timer.
