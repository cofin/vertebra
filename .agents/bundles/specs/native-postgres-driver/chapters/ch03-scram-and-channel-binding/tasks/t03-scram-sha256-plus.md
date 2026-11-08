---
type: Task
id: "native-postgres-driver:ch03:t03"
chapter_id: ch03-scram-and-channel-binding
title: Enforce SCRAM-SHA-256-PLUS channel-binding policies and GS2 downgrade protection
description: Select and verify SCRAM-SHA-256-PLUS across Disable, Prefer, and Require channel-binding policies, emitting RFC 5802 GS2 y,, on bound TLS plain-only offers and rejecting downgrade or replay.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch03:t02"
read_set:
  - src/rs/vertebra-postgres/src/config.rs
  - src/rs/vertebra-postgres/tests/postgres_auth_wire_tests.rs
  - src/rs/vertebra-postgres/tests/postgres_tls_wire_tests.rs
  - src/rs/vertebra-postgres/src/auth.rs
  - src/rs/vertebra-postgres/src/session.rs
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - src/rs/vertebra-postgres/src/config.rs
  - src/rs/vertebra-postgres/src/auth.rs
  - src/rs/vertebra-postgres/src/session.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_auth_wire_tests.rs
  - src/rs/vertebra-postgres/tests/postgres_tls_wire_tests.rs
estimated_loc: 245
plan_revision: 8
plan_commit: 8c540809
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-08T00:40:03Z"
last_verified_commit: aa538f9a
verification_evidence:
  red_tls:
    command: 'UV_NO_SYNC=1 PYO3_PYTHON=${PWD}/.venv/bin/python CARGO_TARGET_DIR=/tmp/vtb-cargo-target cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --locked --test postgres_tls_wire_tests scram_plus_plain_offer_gs2_y'
    exit_code: 101
    result: 'Before production changes, trusted bound TLS with a plain-only offer emits n,, rather than the required y,,; the wire assertion fails.'
  red_plaintext:
    command: 'UV_NO_SYNC=1 PYO3_PYTHON=${PWD}/.venv/bin/python CARGO_TARGET_DIR=/tmp/vtb-cargo-target cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --locked --test postgres_auth_wire_tests scram_plus_'
    exit_code: 101
    result: 'Final canonical tests against baseline production source reject Require plaintext startup bytes: peer reads one byte instead of terminal EOF; client also lacks the required UnsupportedAuth refusal. Source restoration exits 0 and restored-source focused Green passes.'
  focused_auth:
    command: 'UV_NO_SYNC=1 PYO3_PYTHON=${PWD}/.venv/bin/python CARGO_TARGET_DIR=/tmp/vtb-cargo-target cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --locked --test postgres_auth_wire_tests scram_plus_'
    exit_code: 0
    result: '1 test passes, executing all 18 plaintext policy/offer combinations with complete successful SCRAM/password exchanges and terminal pre-startup Require refusal.'
  focused_tls:
    command: 'UV_NO_SYNC=1 PYO3_PYTHON=${PWD}/.venv/bin/python CARGO_TARGET_DIR=/tmp/vtb-cargo-target cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --locked --test postgres_tls_wire_tests scram_plus_'
    exit_code: 0
    result: '4 tests pass: 36 real trusted TLS matrix combinations across ECDSA and Ed25519; full bound PLUS success, different-leaf and server-proof refusals, successful plain y,, and terminal stripped-offer downgrade rejection. Decoded c= and independent server proofs are verified; EOF and a completion barrier prove no extra credential frames or second connection.'
  aggregate:
    command: 'UV_NO_SYNC=1 PYO3_PYTHON=${PWD}/.venv/bin/python CARGO_TARGET_DIR=/tmp/vtb-cargo-target flock /tmp/vertebra-flow-shm-tests.lock cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --locked'
    exit_code: 0
    result: '76 tests pass: 9 codec, 24 auth, 21 TLS, 22 wire/config/error. All 71 prior tests remain unchanged and pass; zero ignored or failed.'
  clippy:
    command: 'UV_NO_SYNC=1 PYO3_PYTHON=${PWD}/.venv/bin/python CARGO_TARGET_DIR=/tmp/vtb-cargo-target cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets --locked -- -D warnings'
    exit_code: 0
  formatting:
    command: 'cargo fmt --manifest-path src/rs/Cargo.toml -p vertebra-postgres --check'
    exit_code: 0
  review_setup:
    command: 'UV_NO_SYNC=1 .venv/bin/python -m tools.scripts.review_setup validate'
    exit_code: 0
  diff:
    command: 'git diff --check'
    exit_code: 0
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-08T00:40:03Z"
commit: aa538f9a
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: channel_binding="prefer" default and Disable/Prefer/Require selection rules
  - id: proto-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: postgres-protocol ChannelBinding variants and RFC 5802 GS2 downgrade signaling
tags:
  - postgres
  - scram-plus
  - channel-binding
  - gs2
---

# Task t03: Enforce `SCRAM-SHA-256-PLUS` Channel-Binding Policies and `GS2` Downgrade Protection

## Objective & Scope Boundary

Complete `ChannelBindingPolicy` (`Disable`, `Prefer`, `Require`) selection and `SCRAM-SHA-256-PLUS` verification across `config.rs`, `auth.rs`, and `session.rs` using `postgres_protocol::authentication::sasl::ChannelBinding::{unsupported, unrequested, tls_server_end_point}`, enforcing RFC 5802 Section 6 `GS2` `"y,,"` downgrade signaling on bound TLS plain-only offers and terminal failure without replay on mismatch or downgrade [^pg-contracts] [^proto-research]. Cloud control-plane material caching begins in Chapter 04 (`ch04:t01`, [../spec.md](../spec.md)).

## Bounded Read Manifest

- `src/rs/vertebra-postgres/src/config.rs` — `ChannelBindingPolicy` (`Disable`, `Prefer` default, `Require`) and `PostgresConfig.channel_binding`.
- `src/rs/vertebra-postgres/tests/postgres_auth_wire_tests.rs` — Canonical SCRAM server fixtures and wire assertions from `ch03:t01`.
- `src/rs/vertebra-postgres/tests/postgres_tls_wire_tests.rs` — Canonical TLS certificate and endpoint-binding fixtures from `ch03:t02`, and the destination for SCRAM-PLUS wire cases.
- `src/rs/vertebra-postgres/src/auth.rs` — `Authentication` SASL state machine from `ch03:t01`.
- `src/rs/vertebra-postgres/src/session.rs` — `ConnectAttempt::run` passing transport binding into `Authentication::handle_message`.
- `.agents/bundles/specs/native-postgres-driver/contracts.md` — `channel_binding = "prefer"` constructor contract ([../../../contracts.md](../../../contracts.md)).

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/config.rs`:
  - Ensure `ChannelBindingPolicy` (`Disable`, `Prefer`, `Require`) exposes `as_str(&self) -> &'static str` (`"disable" | "prefer" | "require"`) and remains validated once in `PostgresConfig::validate` with default `Prefer`.
- `src/rs/vertebra-postgres/src/auth.rs`:
  - Update `Authentication::handle_message(&mut self, msg: &Message, config: &PostgresConfig, tls_server_end_point: Option<&[u8]>, out: &mut BytesMut) -> Result<bool, PostgresError>`:
    - Under `ChannelBindingPolicy::Require`:
      1. If `tls_server_end_point.is_none()` (plaintext socket or TLS with unavailable binding such as Ed25519), reject BEFORE processing any startup challenge or sending credentials (`PostgresError::UnsupportedAuth` / `Transport`).
      2. Reject `AuthenticationOk` (trust), `AuthenticationCleartextPassword`, `AuthenticationMd5Password`, and `AuthenticationSasl` lists lacking `SCRAM-SHA-256-PLUS` with `PostgresError::UnsupportedAuth` before transmitting any credential frame.
      3. When `SCRAM-SHA-256-PLUS` is advertised and `tls_server_end_point == Some(digest)`, construct `ScramSha256::new(password.as_bytes(), ChannelBinding::tls_server_end_point(digest.to_vec()))` and emit `frontend::sasl_initial_response(SCRAM_SHA_256_PLUS, scram.message(), out)` (`GS2` header `"p=tls-server-end-point,,"`).
    - Under `ChannelBindingPolicy::Prefer`:
      1. With `tls_server_end_point == Some(digest)`:
         - If `SCRAM-SHA-256-PLUS` is advertised (alone or alongside `SCRAM-SHA-256`), select `SCRAM-SHA-256-PLUS` with `ChannelBinding::tls_server_end_point(digest.to_vec())` (`GS2` `"p=tls-server-end-point,,"`).
         - If ONLY `SCRAM-SHA-256` is advertised, select `SCRAM-SHA-256` with `ChannelBinding::unrequested()`, emitting `GS2` `"y,,"` (`c=eSws` in client-final) to declare client channel-binding support per RFC 5802 Section 6.
      2. With `tls_server_end_point == None` (plaintext or unavailable Ed25519 binding):
         - If `SCRAM-SHA-256` is advertised, select `SCRAM-SHA-256` with `ChannelBinding::unsupported()`, emitting `GS2` `"n,,"`.
         - If ONLY `SCRAM-SHA-256-PLUS` is advertised, fail with `PostgresError::UnsupportedAuth`.
    - Under `ChannelBindingPolicy::Disable`:
      - Select ONLY `SCRAM-SHA-256` with `ChannelBinding::unsupported()` (`GS2` `"n,,"`) even over TLS with usable binding; fail with `PostgresError::UnsupportedAuth` if the server offers only `SCRAM-SHA-256-PLUS`.
- `src/rs/vertebra-postgres/src/session.rs`:
  - Enforce `config.channel_binding == ChannelBindingPolicy::Require` pre-auth check if `dialed.tls_server_end_point.is_none()` before entering the auth challenge loop (or before sending any credentials), pass `dialed.tls_server_end_point.as_deref()` into `Authentication::handle_message`, and never retry a failed `PLUS` or `"y,,"` exchange with `"n,,"`, another mechanism, or a second TCP/TLS connection.

## Edge Cases & Error Semantics

- Stripped-advertisement MITM attack (`scram_plus_downgrade_rejected`): when a `PLUS`-capable server's mechanism list is stripped to `["SCRAM-SHA-256"]` over TLS with usable binding, the client sends `"y,,"`, completes `AuthenticationSaslContinue` / client-final (`c=eSws`), and receives `AuthenticationSaslFinal` `"e=server-does-support-channel-binding"` (or `ErrorResponse` `28P01`). `ScramSha256::finish` rejects the exchange, closes the socket without `Ready`, and never replays credentials with `"n,,"`.
- Certificate substitution / proxy leaf mismatch: if the scripted TLS server computes `SCRAM-SHA-256-PLUS` proof against a different certificate digest than the leaf actually presented on the `rustls` connection (or sends a bad server proof), client/server proof verification fails and closes the connection.
- `ChannelBindingPolicy::Require` over TLS with an Ed25519 server certificate (where `tls_server_end_point` is `None`) fails before sending `SASLInitialResponse`, whereas `Prefer` over the same Ed25519 TLS stream falls back to advertised `SCRAM-SHA-256` with `"n,,"`.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/rs/vertebra-postgres/tests/postgres_auth_wire_tests.rs` and `src/rs/vertebra-postgres/tests/postgres_tls_wire_tests.rs` (test names prefixed `scram_plus_`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `scram_plus_policy_matrix` | Full 54-case cartesian matrix of `(Disable, Prefer, Require)` x `(bound TLS, Ed25519 TLS, plaintext)` x `(PLUS+plain, PLUS-only, plain-only, trust, cleartext, MD5)` | Matches exact policy contract: `Disable` -> plain `"n,,"`; `Prefer` bound TLS -> `PLUS` (`p=tls-server-end-point,,`) or plain `"y,,"`; `Prefer` unbound -> plain `"n,,"` (fails `PLUS`-only); `Require` -> `PLUS` on bound TLS only, rejecting trust/cleartext/MD5/plain-only/unbound before credentials. |
| `scram_plus_tls_leaf_bound_success_and_mismatch` | Live `tokio-rustls` server (`ECDSA P-256 SHA-256` leaf) offering `["SCRAM-SHA-256-PLUS", "SCRAM-SHA-256"]`: (a) matching leaf digest, (b) server computes `c=` against different cert DER, (c) corrupted server `v=` | (a) client-final `c=` base64-decodes to `b"p=tls-server-end-point,," ++ Sha256(leaf_der)` and commits `Ready`; (b) and (c) fail terminally and close socket. |
| `scram_plus_plain_offer_gs2_y` | Trusted TLS with usable `Sha256(leaf_der)` binding (`channel_binding = "prefer"`), server advertises ONLY `["SCRAM-SHA-256"]` and genuinely does not support PLUS | Client-first message begins with `"y,,"` and client-final `c=` attribute base64-decodes to exact `b"y,,"`; valid plain server proof completes `Ready`. |
| `scram_plus_downgrade_rejected` | Trusted TLS with usable binding (`channel_binding = "prefer"`), stripped advertisement `["SCRAM-SHA-256"]`, server receives `"y,,"`, runs `Continue`/client-final, and sends `SASLFinal` `b"e=server-does-support-channel-binding"` | `ScramSha256::finish` returns error (`PostgresError::Protocol`), `SessionHandle` transitions to `Closed`, listener count stays `1`, and zero `"n,,"` or retry frames are transmitted. |

## Verification

Write the `scram_plus_*` tests in `postgres_auth_wire_tests.rs` and `postgres_tls_wire_tests.rs` first (confirming Red), implement `config.rs`, `auth.rs`, and `session.rs` updates, and verify exit `0`:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_auth_wire_tests scram_plus_
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_tls_wire_tests scram_plus_
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres
```

Commit as `feat(postgres): authenticate certificate-bound SCRAM sessions`.

## Acceptance Criteria

- [x] `Disable`, `Prefer`, and `Require` select `ChannelBinding::{unsupported, unrequested, tls_server_end_point}` strictly per `contracts.md` and RFC 5802 Section 6 without custom SCRAM crypto (R5).
- [x] `scram_plus_plain_offer_gs2_y` verifies wire-level `"y,,"` in both client-first and base64-decoded `c=` when bound TLS receives a plain-only offer, while `Disable` and unbound transports emit `"n,,"` (R5).
- [x] `scram_plus_downgrade_rejected` and `Require` negative cases fail terminally without credential leakage, mechanism fallback, or reconnect replay (R4, R5).

## Notes & Discoveries

- Testing `e=server-does-support-channel-binding` inside `AuthenticationSaslFinal` verifies real RFC 5802 wire downgrade signaling through `postgres_protocol::authentication::sasl::ScramSha256::finish` rather than stopping at client-first selection.
- The complete 54-case matrix executes 18 plaintext cases in the auth test file and 36 real trusted TLS cases in the TLS test file. Unbound `Require` fails immediately after transport negotiation with zero startup or credential bytes; bound `Require` accepts `AuthenticationOk` only after successful PLUS verification.
- `ChannelBindingPolicy::as_str(&self)` exposes the constructor spellings from `config.rs`; policy selection lives in `auth.rs`, and the pre-startup binding check lives in `session.rs`. Nonce, iteration, reservation-deadline, ordering, and credential-redaction tests remain intact. Only server-side test helpers calculate proofs; client crypto remains upstream `ScramSha256`.
- The original reservation deadline covers authentication work and publication. `startup` rechecks it after `Authentication::handle_message` and before flushing credentials; unavailable required binding is rejected before the startup frame.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — `channel_binding` policy defaults and failure categories.
[^proto-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — `ChannelBinding` APIs and `GS2` `y,,` downgrade semantics.
