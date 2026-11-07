---
type: Task
id: "native-postgres-driver:ch02:t03"
chapter_id: ch02-wire-codec-and-session-startup
title: Negotiate PostgreSQL SSLRequest and verify rustls peer TLS identities
description: Implement tls::connect negotiating the 8-byte PostgreSQL SSLRequest preamble and upgrading via shared TransportStream::upgrade_tls across Disable, Prefer, Require, VerifyCa, and VerifyFull with CA, SNI, and mTLS verification.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch02:t02"
read_set:
  - src/rs/vertebra-db/src/connection.rs
  - src/rs/vertebra-postgres/src/config.rs
  - src/rs/vertebra-postgres/tests/postgres_wire_tests.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/tests/support/mod.rs
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/src/tls.rs
  - src/rs/vertebra-postgres/src/session.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_wire_tests.rs
  - src/rs/vertebra-postgres/tests/support/mod.rs
  - src/rs/vertebra-postgres/tests/postgres_tls_wire_tests.rs
estimated_loc: 235
plan_revision: 6
plan_commit: 8833b064
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-07T23:36:44Z"
last_verified_commit: 0c3a0eaa
verification_evidence:
  red: 'Canonical enabled_modes_upgrade_before_trust_startup test exits 101 with UnsupportedFeature("only direct plaintext TCP is available") before integration.'
  focused: "cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_tls_wire_tests exits 0: 10 real rcgen/tokio-rustls peer tests cover the complete TLS matrix."
  aggregate: "flock /tmp/vertebra-flow-shm-tests.lock cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --locked exits 0: 49 tests (9 codec, 8 auth, 10 TLS, 22 wire/config/error), zero failures or ignores."
  lint: "cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets --locked -- -D warnings exits 0 without lint allowances."
  formatting: "cargo fmt --manifest-path src/rs/Cargo.toml -p vertebra-postgres -- --check and git diff --check exit 0."
  review_privacy: "UV_NO_SYNC=1 .venv/bin/python -m tools.scripts.review_setup validate exits 0."
  environment: "UV_NO_SYNC=1; PYO3_PYTHON=${PWD}/.venv/bin/python; dedicated Cargo target; aggregate tests acquire the shared SHM test lock."
  deadline_characterization: "Original sslresponse_and_tls_share_the_reserved_deadline passes before replacement. The paused-clock replacement passes 20/20 independent focused runs, preserving all 10 TLS cases."
  deadline_boundary: "After 75 ms reserved before run and 100 ms waiting for SSLResponse, both SSLResponse-stall and accepted-S/TLS-stall cases remain pending at virtual 249 ms and fail Transport at virtual 251 ms (Tokio timer rounding); the clock equals exactly the commanded advance, handle is terminal, and the actual peer observes EOF after clock resumption."
  checkpoint: "Completed repair receipt binds to 0c3a0eaa. Exact chapter checkpoint 0446ee46 passes all 54 tests and locked all-target Clippy with -D warnings."
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-07T23:44:42Z"
commit: 0c3a0eaa
sources:
  - id: rs-upgrade-tls
    resource: src/rs/vertebra-db/src/connection.rs::TransportStream
    title: TransportStream::upgrade_tls, install_rustls_ring_provider, and TlsMode verification policies
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Direct TCP sslmode, server_name override, and mTLS PEM contracts
  - id: proto-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: frontend::ssl_request and rustls peer transport constraints
tags:
  - postgres
  - tls
  - rustls
  - sslrequest
---

# Task t03: Negotiate PostgreSQL `SSLRequest` and Verify `rustls` Peer TLS Identities

## Objective & Scope Boundary

Implement `src/rs/vertebra-postgres/src/tls.rs` and wire it into `session.rs` so direct TCP sessions negotiate PostgreSQL's 8-byte `SSLRequest` (`[0, 0, 0, 8, 4, 210, 22, 47]`) before upgrading via `vertebra_db::connection::TransportStream::upgrade_tls` (`ring` provider) under the single outer connection deadline [^rs-upgrade-tls] [^pg-contracts] [^proto-research]. X.509 `tls-server-end-point` digest extraction lands in `ch03:t02` and immediate cloud mTLS (`CloudSql` / `AlloyDb`) lands in `ch05` ([../spec.md](../spec.md)).

## Bounded Read Manifest

- `src/rs/vertebra-db/src/connection.rs::{TransportStream, ConnectOptions, EndpointTarget, TlsMode, install_rustls_ring_provider}` — Shared raw TCP connect and `upgrade_tls` verifier [^rs-upgrade-tls].
- `src/rs/vertebra-postgres/src/config.rs` — `PostgresConfig` (`sslmode`, `ca_pem`, `client_cert_pem`, `client_key_pem`, `server_name`, `host`, `port`).
- `src/rs/vertebra-postgres/tests/postgres_wire_tests.rs` — Preserve the trust/auth tests, remove the obsolete TLS-unsupported assertion, and gate the deadline peer with an explicit barrier.
- `src/rs/vertebra-postgres/src/session.rs` — `ConnectAttempt::run` transport dial seam from `ch02:t02`.
- `src/rs/vertebra-postgres/tests/support/mod.rs` — Loopback listener and startup frame helpers from `ch02:t02`.
- `.agents/bundles/specs/native-postgres-driver/contracts.md` — Direct TLS mode and `server_name` rules ([../../../contracts.md](../../../contracts.md)).

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/lib.rs`:
  - Declare `pub(crate) mod tls;`.
- `src/rs/vertebra-postgres/src/tls.rs`:
  - `pub(crate) struct DialedTransport { pub stream: TransportStream, pub tls_server_end_point: Option<Vec<u8>> }` (`tls_server_end_point` initialized to `None` here and populated in `ch03:t02`).
  - `pub(crate) async fn connect(config: &PostgresConfig, deadline: tokio::time::Instant) -> Result<DialedTransport, PostgresError>`:
    1. Call `install_rustls_ring_provider()`.
    2. Build `full_options = ConnectOptions` carrying `EndpointTarget::Tcp { host: config.host.clone(), port: config.port }`, `tls_mode: config.sslmode`, `ca_pem: config.ca_pem.clone()`, `client_cert_pem: config.client_cert_pem.clone()`, `client_key_pem: config.client_key_pem.clone()`, `server_name: config.server_name.clone()`, `tcp_nodelay: true`, and remaining budget `deadline.saturating_duration_since(Instant::now())`.
    3. Clone `dial_options` from `full_options` with `tls_mode = TlsMode::Disable` so `TransportStream::connect(&dial_options)` establishes a plain TCP socket without emitting an immediate TLS ClientHello before `SSLRequest`.
    4. If `config.sslmode == TlsMode::Disable`: return `Ok(DialedTransport { stream: plain_stream, tls_server_end_point: None })` without sending `SSLRequest`.
    5. Otherwise encode `postgres_protocol::message::frontend::ssl_request(&mut buf)` (`[0, 0, 0, 8, 4, 210, 22, 47]`), write + flush, and read exact 1 byte under `tokio::time::timeout_at(deadline, ...)`:
       - `b'S'`: extract `TcpStream` from `TransportStream::PlainTcp(tcp)` and call `TransportStream::upgrade_tls(tcp, &config.host, &full_options)` inside `timeout_at(deadline, ...)`. Never retry or fall back to plaintext on TLS handshake error.
       - `b'N'`: if `config.sslmode == TlsMode::Prefer`, return `PlainTcp`; for `Require`, `VerifyCa`, or `VerifyFull`, fail with `PostgresError::Transport`.
       - Any other byte (e.g. `b'E'`, `0x00`) or EOF: fail with `PostgresError::Protocol` / `PostgresError::Transport`.
- `src/rs/vertebra-postgres/src/session.rs`:
  - Replace the `ch02:t02` plaintext-only dial in `ConnectAttempt::run` with `tls::connect(&self.handle.config, self.deadline).await?` inside the same outer `timeout_at(self.deadline, ...)` budget, storing `tls_server_end_point` on `PostgresConnection` (kept purely in Rust).

## Edge Cases & Error Semantics

- `Prefer` + `b'S'` delegates to `TransportStream::upgrade_tls` (which upgrades `Prefer` to `Require` policy: encrypted without CA when `ca_pem` is `None`, or CA-verified when `ca_pem` is `Some`); if the TLS handshake after `b'S'` fails (e.g. bad cert with `ca_pem` or broken TLS record), the attempt fails terminally and never reconnects in plaintext.
- `VerifyCa` verifies the server certificate chain against `ca_pem` (or system roots) without checking hostname, whereas `VerifyFull` verifies both chain and SAN (`config.server_name.as_deref().unwrap_or(&config.host)`).
- `VerifyCa`, `VerifyFull`, and `Require`/`Prefer` with `ca_pem` reject expired server certificates and untrusted chains; only `VerifyFull` checks the server hostname. `Require`/`Prefer` without `ca_pem` provide encryption without CA or hostname verification. Server rejection of a missing or CA-mismatched required client mTLS identity fails with `PostgresError::Transport` and transitions `SessionHandle` to `Closed`.
- Delaying the `SSLRequest` response or TLS handshake past `deadline` aborts the dial, closes the TCP socket, and leaves `SessionHandle` in `Closed`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/postgres_tls_wire_tests.rs` using `rcgen` and `tokio-rustls` server fixtures plus `mod support;`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `Disable` skips `SSLRequest` | `sslmode = "disable"` against plaintext loopback server | First 8 client bytes are `[len.., 0, 3, 0, 0]` (`StartupMessage`), not `[0, 0, 0, 8, 4, 210, 22, 47]`; session commits `Ready`. |
| `Prefer` + `N` plaintext fallback | `sslmode = "prefer"`, server asserts exact `[0, 0, 0, 8, 4, 210, 22, 47]`, replies `b'N'` | Client sends `StartupMessage` in plaintext on same socket and commits `Ready`. |
| Enabled modes + `S` TLS upgrade | `sslmode` in `["prefer", "require", "verify-ca", "verify-full"]`, server replies `b'S'` and completes `tokio-rustls` handshake with `rcgen` CA/leaf (`SAN = "localhost"`) | Client upgrades to TLS, completes trust startup inside TLS, and commits `Ready`. |
| Strict modes + `N` or unknown byte / EOF | `sslmode` in `["require", "verify-ca", "verify-full"]` + `b'N'`, or `Prefer` + `b'?'` / immediate EOF | Fails with `PostgresError::Transport` / `Protocol`; server observes zero `StartupMessage` bytes and socket EOF. |
| No plaintext fallback after `S` | `sslmode = "prefer"`, server replies `b'S'` then sends garbage TLS bytes | Fails with `PostgresError::Transport`; listener accepts only `1` connection (no second plaintext dial). |
| Hostname & `server_name` verification | Leaf SAN `"db.internal"`, connecting to `"127.0.0.1"`: (a) `verify-ca`, (b) `verify-full` without `server_name`, (c) `verify-full` with `server_name = Some("db.internal")`, (d) expired leaf or wrong CA, (e) `require` with and without `ca_pem` | (a) succeeds; (b) fails; (c) succeeds; (d) fails; (e) `require` without `ca_pem` succeeds on self-signed cert, `require` with wrong `ca_pem` fails. |
| Client mTLS identity & deadline expiry | TLS server with `WebPkiClientVerifier`: valid client cert/key vs missing client cert vs wrong-CA client cert; plus server stalling after `b'S'` past `connect_timeout` | Valid mTLS commits `Ready`; missing/wrong client cert fails `PostgresError::Transport`; stalled TLS fails at deadline and closes socket + `SessionHandle`. |

## Verification

Write the `rcgen` / `tokio-rustls` test suite in `src/rs/vertebra-postgres/tests/postgres_tls_wire_tests.rs` first (confirming Red), implement `tls.rs` and `session.rs` integration, and verify exit `0`:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_tls_wire_tests
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
```

Commit as `feat(postgres): negotiate protocol TLS with shared transport`.

## Acceptance Criteria

- [x] `tls::connect` emits exact `[0, 0, 0, 8, 4, 210, 22, 47]` `SSLRequest` frames for enabled TLS modes and upgrades via `TransportStream::upgrade_tls` using the shared `ring` provider (R4).
- [x] `Disable`, `Prefer` (`S`/`N`), `Require` (with/without `ca_pem`), `VerifyCa`, `VerifyFull` (`server_name` override), and client mTLS behave strictly without post-`S` plaintext downgrade (R4).
- [x] `SSLRequest` and TLS handshakes share `ConnectAttempt`'s single outer deadline and transition `SessionHandle` to `Closed` on failure or expiry (R3, R4).

## Notes & Discoveries

- `DialedTransport { stream, tls_server_end_point }` reserves the X.509 endpoint-binding seam for `ch03:t02`; its binding field remains `None` until that chapter extracts the verified peer digest from `TransportStream::TlsTcp`.
- Generic startup/send/EOF peer helpers accept both TCP and TLS streams. The publication-deadline test releases its peer only after clock resumption so its EOF watchdog cannot expire during an intentional clock advance.
- The TLS peer matrix verifies enabled modes, refusal, identity policy, client mTLS, deadlines, and terminal handshake failure over actual TCP/TLS streams. Deadline cases use paused Tokio time and explicit SSLRequest/ClientHello/EOF barriers; a runnable barrier waiter prevents automatic clock advancement during socket I/O. Both cases remain pending at virtual 249 ms and fail terminally at 251 ms for the reserved 250 ms deadline, accounting for Tokio timer rounding without a wall-clock threshold. EOF watchdogs start only after clock resumption.

[^rs-upgrade-tls]: `src/rs/vertebra-db/src/connection.rs::TransportStream` — Shared `upgrade_tls` and `ring` TLS verifier.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Direct TLS mode and SNI override contracts.
[^proto-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — `SSLRequest` and `rustls` peer transport analysis.
