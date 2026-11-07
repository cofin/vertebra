---
type: Task
id: "native-postgres-driver:ch02:t02"
chapter_id: ch02-wire-codec-and-session-startup
title: Establish authenticated trust sessions with SessionHandle and PendingSession publication
description: Implement PostgresConnection, SessionHandle, ConnectAttempt, and PendingSession enforcing strict Protocol 3.0 startup ordering, total deadline budgets, cancellation rollback, and bounded Terminate close.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch02:t01"
read_set:
  - src/rs/vertebra-db/src/connection.rs
  - src/rs/vertebra-postgres/src/config.rs
  - src/rs/vertebra-postgres/src/errors.rs
  - src/rs/vertebra-postgres/src/protocol.rs
  - src/rs/vertebra-postgres/tests/postgres_wire_tests.rs
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/protocol.rs
tests:
  - src/rs/vertebra-postgres/tests/support/mod.rs
  - src/rs/vertebra-postgres/tests/postgres_wire_tests.rs
estimated_loc: 240
plan_revision: 5
plan_commit: 399e068c
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
updated_at: "2026-11-07T05:00:00Z"
commit: null
sources:
  - id: rs-transport
    resource: src/rs/vertebra-db/src/connection.rs::TransportStream
    title: Shared TCP TransportStream and ConnectOptions
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Session lifecycle, total connection deadline, and publication invariants
  - id: proto-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: Protocol 3.0 startup frame sequence and BackendKeyData ownership
tags:
  - postgres
  - session
  - lifecycle
  - startup
---

# Task t02: Establish Authenticated Trust Sessions with `SessionHandle` and `PendingSession` Publication

## Objective & Scope Boundary

Implement `PostgresConnection`, `SessionHandle`, `ConnectAttempt`, and `PendingSession` in `src/rs/vertebra-postgres/src/session.rs` to establish direct TCP `TlsMode::Disable` trust sessions with strict startup ordering (`AuthenticationOk` -> `ParameterStatus` with `client_encoding = "UTF8"` + optional single `BackendKeyData` -> `ReadyForQuery(b'I')`), a single outer `connect_timeout` deadline, armed rollback-to-`Closed` guards on drop/cancellation, and best-effort `<= 100ms` `Terminate` shutdown [^rs-transport] [^pg-contracts] [^proto-research]. `SSLRequest`/TLS negotiation (`ch02:t03`), password/MD5/SCRAM auth (`ch02:t04`, `ch03`), Unix socket projection (`ch06:t03`), and shared `Connection` trait ping/cancel integration (`ch16:t03`) are out of scope ([../spec.md](../spec.md)).

## Bounded Read Manifest

- `src/rs/vertebra-db/src/connection.rs::{TransportStream, ConnectOptions, EndpointTarget, TlsMode}` — Plain TCP dial primitives [^rs-transport].
- `src/rs/vertebra-postgres/src/config.rs` — `PostgresConfig` fields and timeout duration.
- `src/rs/vertebra-postgres/src/errors.rs` — `PostgresError` categories and SQLSTATE server error constructor.
- `src/rs/vertebra-postgres/src/protocol.rs` — `read_message`, `encode_startup_frame`, and `TERMINATE_FRAME` from `ch02:t01`.
- `src/rs/vertebra-postgres/tests/postgres_wire_tests.rs` — Canonical wire integration test file initialized in `ch01:t04`.
- `.agents/bundles/specs/native-postgres-driver/contracts.md` — Session ownership and connection deadline semantics ([../../../contracts.md](../../../contracts.md)).

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/lib.rs`:
  - Export `pub mod session;` on the `rlib` seam.
- `src/rs/vertebra-postgres/src/protocol.rs`:
  - Remove the three producer-stage `expect(dead_code)` annotations from `read_message`, `encode_startup_frame`, and `TERMINATE_FRAME` as session startup/close establishes their real consumers. Leave private visibility and all-target `-D warnings` unchanged.
- `src/rs/vertebra-postgres/src/session.rs`:
  - `#[derive(Clone, Copy, Debug, PartialEq, Eq)] pub enum TransactionStatus { Idle, InTransaction, Failed }` with `pub fn from_wire_byte(byte: u8) -> Result<Self, PostgresError>` (`b'I' => Idle`, `b'T' => InTransaction`, `b'E' => Failed`, other => `Err(PostgresError::Protocol(_))`) and `pub const fn as_wire_byte(self) -> u8`.
  - `#[derive(Clone, Copy, Debug, PartialEq, Eq)] pub struct BackendKey { pub process_id: i32, pub secret_key: i32 }` (Rust-only; never exposed as Python client attributes).
  - `pub struct PostgresConnection`:
    - Owns `stream: TransportStream`, `parameters: HashMap<String, String>`, `backend_key: Option<BackendKey>`, `transaction_status: TransactionStatus`, and `tls_server_end_point: Option<Vec<u8>>`.
    - Public getters `parameters(&self) -> &HashMap<String, String>`, `backend_key(&self) -> Option<BackendKey>`, `transaction_status(&self) -> TransactionStatus`.
  - `#[derive(Clone)] pub struct SessionHandle`:
    - Owns `config: Arc<PostgresConfig>` and `state: Arc<std::sync::Mutex<SessionState>>` (`Configured`, `Connecting`, `Ready { conn: PostgresConnection, params_snapshot: HashMap<String, String> }`, `Closed { params_snapshot: HashMap<String, String> }`) — never hold the `std::sync::Mutex` across `.await` or Python callbacks.
    - `pub fn new(config: PostgresConfig) -> Self`
    - `pub fn config(&self) -> &PostgresConfig`
    - `pub fn is_closed(&self) -> bool` (`true` in `Configured`, `Connecting`, and `Closed`; `false` only in `Ready`).
    - `pub fn parameters(&self) -> HashMap<String, String>` (empty until `PendingSession::commit()` succeeds; retained after `close()`).
    - `pub fn begin_connect(&self) -> Result<BeginConnect, PostgresError>`:
      - `Configured`: transitions to `Connecting`, records `deadline = tokio::time::Instant::now() + self.config.connect_timeout`, and returns `Ok(BeginConnect::Attempt(ConnectAttempt))` armed with a rollback-to-`Closed` drop guard.
      - `Ready`: returns `Ok(BeginConnect::AlreadyReady)` (owns no guard or socket; dropping `AlreadyReady` leaves `Ready` intact).
      - `Connecting` or `Closed`: returns `Err(PostgresError::Interface(_))`.
    - `pub async fn close(&self) -> Result<(), PostgresError>`:
      - `Configured` or `Closed`: transitions/remains `Closed` and returns `Ok(())` idempotently.
      - `Connecting`: returns `Err(PostgresError::Interface(_))`.
      - `Ready`: atomically takes `PostgresConnection` and sets `Closed { params_snapshot }` under the short mutex before any I/O, then writes `protocol::TERMINATE_FRAME` + `flush()` + `shutdown()` under a `tokio::time::timeout(Duration::from_millis(100), ...)` best-effort budget and drops the stream.
  - `pub enum BeginConnect { Attempt(ConnectAttempt), AlreadyReady }`; `ConnectAttempt` and `PendingSession` each own `handle: SessionHandle`, `deadline: tokio::time::Instant`, and `armed: bool`; `PendingSession` additionally owns `conn: Option<PostgresConnection>`. These guards and the `run()` future satisfy `Send + 'static` with no borrowed caller state or leaked allocations.
  - `impl ConnectAttempt`:
    - `pub async fn run(self) -> Result<PendingSession, PostgresError>` driving dial + startup + auth inside a single `tokio::time::timeout_at(self.deadline, ...)`:
      - Initially accepts direct TCP (`unix_socket.is_none()`, `cloud_sql_instance.is_none()`, `alloydb_instance_uri.is_none()`, `!enable_iam_auth`) with `sslmode == TlsMode::Disable` and `AuthenticationOk` (trust); explicitly returns `UnsupportedFeature` / `UnsupportedAuth` for TLS/password/cloud modes until subsequent tasks.
      - Startup state machine enforces: (1) exactly one `AuthenticationOk` before any `ParameterStatus`, `BackendKeyData`, or `ReadyForQuery`; (2) `ParameterStatus` frames recorded in `parameters` (last value wins per key, valid UTF-8); (3) at most one `BackendKeyData`; (4) `ReadyForQuery` status parsed via `TransactionStatus::from_wire_byte(body.status())?` must equal `TransactionStatus::Idle` (`b'I'`) only after `parameters.get("client_encoding") == Some("UTF8")`.
      - Consumes `NoticeResponse` silently; maps `ErrorResponse` (`S`/`V` severity, `C` SQLSTATE, `M` message, and `(field.type_byte(), value)` `diagnostics` via `PostgresError::server(...)`) to `PostgresError::Server(ServerError)`.
  - `impl PendingSession`:
    - `pub fn commit(mut self) -> Result<(), PostgresError>`: rechecks `tokio::time::Instant::now() <= self.deadline` (failing `PostgresError::Transport` and rolling back to `Closed` if elapsed), atomically publishes `SessionState::Ready`, and disarms the rollback guard.
- `src/rs/vertebra-postgres/tests/support/mod.rs`:
  - Reusable loopback `TcpListener` harness, deterministic `tokio::sync::{Barrier, oneshot, Notify}` helpers, and raw backend frame builders (`AuthenticationOk`, `ParameterStatus`, `BackendKeyData`, `ReadyForQuery`, `ErrorResponse`) imported via `mod support;` across sibling wire test files.

## Edge Cases & Error Semantics

- A retained producer-stage expectation becomes unfulfilled when the session consumes its symbol and fails the existing `-D warnings` gate; remove all three annotations in this task.

- Reject `ReadyForQuery(b'T')` (`TransactionStatus::InTransaction`), `ReadyForQuery(b'E')` (`TransactionStatus::Failed`), unknown `ReadyForQuery` status bytes, missing or non-`"UTF8"` `client_encoding`, duplicate `AuthenticationOk`, duplicate `BackendKeyData`, `ParameterStatus` before `AuthenticationOk`, or unexpected query frames (`RowDescription`, `DataRow`, `CommandComplete`) with `PostgresError::Protocol`.
- Dropping `ConnectAttempt` mid-handshake or dropping `PendingSession` without calling `commit()` transitions `SessionHandle` to terminal `Closed` (`begin_connect()` thereafter returns `PostgresError::Interface`) and closes the TCP socket so the server observes `EOF`.
- Calling `begin_connect()` on an already `Ready` `SessionHandle` returns `BeginConnect::AlreadyReady`; dropping or cancelling that `AlreadyReady` value does not disturb the live connection.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/rs/vertebra-postgres/tests/postgres_wire_tests.rs` with socket/barrier helpers in `src/rs/vertebra-postgres/tests/support/mod.rs` (zero `sleep` polling).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `owned_guard_spawn` | Reserve an attempt from a retained `SessionHandle`, move `attempt.run()` into `tokio::spawn`, and obtain an owned `PendingSession` from the join handle against the trust loopback server | Both worker future and output compile as `Send + 'static`; `pending.commit()` publishes into the original handle, and dropping an uncommitted returned pending session instead closes that handle and the socket. |
| `startup_trust` | Loopback server receives Protocol 3.0 startup, sends `AuthenticationOk`, `ParameterStatus("client_encoding", "UTF8")`, `ParameterStatus("server_version", "16.0")`, `ParameterStatus("server_version", "18.0")`, `BackendKeyData(4242, 9999)`, `ReadyForQuery(b'I')` | `commit()` succeeds; `!handle.is_closed()`; `parameters()["server_version"] == "18.0"`; `transaction_status() == TransactionStatus::Idle`; `close()` sends exact `b"X\0\0\0\x04"` + EOF; post-close `is_closed() == true` and `parameters()["server_version"] == "18.0"`. |
| `startup_requires_auth_utf8_idle` | Sub-cases: missing `client_encoding`, `client_encoding = "SQL_ASCII"`, `ParameterStatus` before `AuthenticationOk`, or `ReadyForQuery(b'T')` | Each returns `Err(PostgresError::Protocol(_))`, server observes socket EOF, and `handle.is_closed() == true`. |
| `startup_error_sqlstate` | Server sends `ErrorResponse` with `S="FATAL"`, `C="3D000"`, `M="database \"missing\" does not exist"`, `D="secret detail"` | Returns `Err(PostgresError::Server(err))` with `err.severity == "FATAL"`, `err.sqlstate == "3D000"`, `err.diagnostics` retaining `(b'D', "secret detail".into())`, and formatted `{err}` excluding `"secret detail"`. |
| `total_deadline` | Server accepts TCP and holds barrier past `connect_timeout = 50ms` (or pauses Tokio time) | `attempt.run()` fails with `PostgresError::Transport(_)`, server observes EOF, and `handle` transitions to `Closed`. |
| `duplicate_auth_rejected` | Server sends two `AuthenticationOk` frames (or two `BackendKeyData` frames) before `ReadyForQuery(b'I')` | Returns `Err(PostgresError::Protocol(_))` and closes socket. |
| `repeated_connect_and_close` | `Ready` handle calls `begin_connect()`, then `close()` twice, then `begin_connect()` again | Second `begin_connect()` returns `Ok(BeginConnect::AlreadyReady)` keeping socket alive; both `close()` calls return `Ok(())`; post-close `begin_connect()` returns `Err(PostgresError::Interface(_))`. |
| `overlapping_connect_fails` | First `ConnectAttempt` held at server barrier while second `begin_connect()` or `close()` runs | Concurrent `begin_connect()` and `close()` both return `Err(PostgresError::Interface(_))` immediately. |
| `dropped_attempt_closes` & `dropped_pending_session_closes` | Drop in-flight `ConnectAttempt` future at barrier; separately complete `run()` into `PendingSession` and drop `PendingSession` without `commit()` | In both cases server observes socket EOF, `handle.is_closed() == true`, `parameters().is_empty()`, and subsequent `begin_connect()` returns `Err(PostgresError::Interface(_))`. |

## Verification

Write the ten canonical loopback tests in `src/rs/vertebra-postgres/tests/postgres_wire_tests.rs` first (confirming Red), implement `session.rs`, and verify exit `0`:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_wire_tests
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
```

Commit as `feat(postgres): own startup session lifecycle and publication`.

## Acceptance Criteria

- [ ] `SessionHandle` enforces `Configured -> Connecting -> Ready | Closed` under a short non-async mutex with `BeginConnect::AlreadyReady` idempotency and `<= 100ms` `Terminate` shutdown (R3, R7).
- [ ] `ConnectAttempt` validates strict startup frame ordering, `client_encoding = "UTF8"`, `ReadyForQuery(b'I')`, sanitized `ErrorResponse` SQLSTATEs, and one outer `connect_timeout` deadline (R3).
- [ ] Dropping `ConnectAttempt` or uncommitted `PendingSession` rolls `SessionHandle` to `Closed` and closes the underlying socket deterministically across all ten loopback tests (R7).

## Notes & Discoveries

- [2026-11-07 05:00] Keeping reusable loopback frame/barrier helpers in `src/rs/vertebra-postgres/tests/support/mod.rs` allows `postgres_wire_tests.rs`, `postgres_tls_wire_tests.rs`, and `postgres_auth_wire_tests.rs` to share `mod support;` without cross-importing test binaries.

[^rs-transport]: `src/rs/vertebra-db/src/connection.rs::TransportStream` — Shared TCP transport stream.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Session lifecycle and deadline contracts.
[^proto-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — Protocol 3.0 startup frame sequence.
