---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch02-wire-codec-and-session-startup
title: "Chapter 02: Bounded Protocol 3.0 Framing, Session Lifecycle, SSLRequest TLS, and Password/MD5 Auth"
description: Implement bounded 16 MiB PostgreSQL Protocol 3.0 framing, SessionHandle/PendingSession startup publication ownership, SSLRequest rustls negotiation, and cleartext/MD5 startup authentication.
status: stable
state: active
planning_status: complete
depends_on_chapters:
  - ch01-wheel-scaffold-and-config
replan_trigger: null
current_task: "native-postgres-driver:ch02:t02"
plan_revision: 6
plan_commit: 8833b064
state_revision: 2
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-07T21:57:00Z"
generated:
  by: flow-revise
  at: "2026-11-07T05:00:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-07T05:00:00Z"
    commit: dd432f18
sources:
  - id: rs-transport
    resource: src/rs/vertebra-db/src/connection.rs::TransportStream
    title: Shared TCP and rustls upgrade_tls transport primitives
  - id: proto-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL Protocol 3.0 framing, SSLRequest, and authentication state machine research
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Connection deadlines, TLS mode semantics, and session publication contracts
tags:
  - postgres
  - protocol
  - tls
  - auth
  - session
---

# Chapter 02: Bounded Protocol 3.0 Framing, Session Lifecycle, SSLRequest TLS, and Password/MD5 Auth

<!-- truth: start -->
- **Bounded 16 MiB Wire Decoder**: `protocol::read_message` reads the 5-byte `[tag, i32_be_len]` header, validates `4 <= len <= 16_777_216` before reserving capacity, and parses the exact frame slice via `postgres_protocol::message::backend::Message::parse` with zero trailing bytes or unbounded `read_to_end` allocations [^proto-research].
- **Single-Deadline `PendingSession` Publication**: `SessionHandle` transitions `Configured -> Connecting -> Ready(PostgresConnection) | Closed` under a short `std::sync::Mutex` never held across `.await`, enforcing one absolute `connect_timeout` deadline across dial, `SSLRequest`, TLS upgrade, startup, and auth, and rolling back to `Closed` if `ConnectAttempt` or `PendingSession` drops before `commit()` [^pg-contracts].
- **No-Downgrade `SSLRequest` & Strict Auth Ordering**: Direct TCP sends `[0, 0, 0, 8, 4, 210, 22, 47]` when TLS is enabled, upgrades via `TransportStream::upgrade_tls` on `b'S'` without plaintext fallback on failure, permits `b'N'` only in `TlsMode::Prefer`, and requires `Authentication::Complete`, `client_encoding = "UTF8"`, optional single `BackendKeyData`, and `ReadyForQuery(b'I')` in strict order [^rs-transport] [^proto-research].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

A native PostgreSQL session must frame backend Protocol 3.0 packets safely against untrusted or truncated sockets, negotiate PostgreSQL's pre-TLS `SSLRequest` preamble before `rustls` handshakes, enforce strict startup message ordering (`Authentication` -> `ParameterStatus` / `BackendKeyData` -> `ReadyForQuery(I)`), and guarantee cancellation-safe connection publication under a single total deadline [^rs-transport] [^proto-research] [^pg-contracts]. Chapter 02 delivers the crate-private bounded wire codec (`protocol.rs`), `SessionHandle` / `PostgresConnection` / `PendingSession` lifecycle (`session.rs`), `SSLRequest` + `rustls` identity verification (`tls.rs`), and trust/cleartext/MD5 authentication (`auth.rs`).

### 1.2 Non-Goals

- `SCRAM-SHA-256`, X.509 `tls-server-end-point` channel binding, and `SCRAM-SHA-256-PLUS` (owned by Chapter 03).
- Cloud SQL and AlloyDB control-plane certificate discovery and IAM login tokens (owned by Chapters 04–06).
- Unix domain socket startup projection and TCP keepalive socket options (owned by Chapter 06).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md). Public contracts: [contracts.md](../../contracts.md). Research: [protocol-and-execution_20261105.md](../../research/protocol-and-execution_20261105.md). Skills: [runtime-ipc](../../../../../skills/runtime-ipc/SKILL.md) and [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md). Knowledge: [database-drivers-and-dialers.md](../../../../knowledge/domains/data/database-drivers-and-dialers.md) and [ffi-and-gil-discipline.md](../../../../knowledge/patterns/rust/ffi-and-gil-discipline.md).

| Existing Seam | Verified Consequence |
| --- | --- |
| `src/rs/vertebra-db/src/connection.rs::TransportStream` [^rs-transport] | `TransportStream::connect` immediately starts TLS when `tls_mode.requires_immediate_tls()` is true; `tls.rs` dials raw TCP with `TlsMode::Disable`, exchanges `SSLRequest`, and calls `TransportStream::upgrade_tls` with the caller's original `TlsMode` on `b'S'`. |
| `postgres_protocol = "=0.6.12"` [^proto-research] | `frontend::startup_message`, `frontend::ssl_request`, `frontend::password_message`, `authentication::md5_hash`, and `backend::Message::parse` handle NUL checks and wire encoding without custom crypto. |

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R3 (`ch02:t01`, `ch02:t02`)**: Implement `protocol::read_message` (`4..=16_777_216` header length guard before allocation) and `protocol::encode_startup_frame` (`196608` Protocol 3.0 with `client_encoding = "UTF8"`), plus `SessionHandle`, `ConnectAttempt`, `PendingSession`, and `PostgresConnection` enforcing strict startup phase ordering, `BackendKeyData` retention, total connection deadline, and best-effort `<= 100ms` `Terminate` on close.
- **R4 (`ch02:t03`)**: Implement `tls::connect` negotiating PostgreSQL `SSLRequest` (`[0, 0, 0, 8, 4, 210, 22, 47]`), `b'S'` / `b'N'` handling across `Disable`, `Prefer`, `Require`, `VerifyCa`, and `VerifyFull`, `server_name` SNI/hostname override, and client mTLS identity via `TransportStream::upgrade_tls` (`ring` crypto provider).
- **R5 (`ch02:t04`)**: Implement `auth::Authentication` (`Initial`, `PasswordSent`, `Complete`) supporting trust (`AuthenticationOk`), cleartext (`AuthenticationCleartextPassword`), and salted MD5 (`AuthenticationMd5Password`) exchanges, rejecting duplicate challenges, missing passwords, and unsupported GSS/SSPI/Kerberos challenges without same-stream retry.

### 2.2 Concrete Type & Signature Contracts

Task 01 applies `#[cfg_attr(not(test), expect(dead_code, reason = "Framing precedes session integration"))]` only to `read_message` and `encode_startup_frame`, and `#[expect(dead_code, reason = "Termination precedes session integration")]` only to `TERMINATE_FRAME`. Task 02 owns removal of all three annotations when its real session consumers land. Unfulfilled expectations remain fatal under the unchanged all-target `-D warnings` gate.

- Rust (`src/rs/vertebra-postgres/src/protocol.rs`, crate-private `pub(crate)`):
  - `pub(crate) const MAX_BACKEND_FRAME_LENGTH: i32 = 16_777_216;`
  - `pub(crate) const PROTOCOL_VERSION_3_0: i32 = 196_608;`
  - `pub(crate) async fn read_message<R: AsyncRead + Unpin>(reader: &mut R) -> Result<postgres_protocol::message::backend::Message, PostgresError>`
  - `pub(crate) fn encode_startup_frame(config: &PostgresConfig) -> Result<BytesMut, PostgresError>`
  - `pub(crate) const TERMINATE_FRAME: [u8; 5] = *b"X\0\0\0\x04";`
- Rust (`src/rs/vertebra-postgres/src/session.rs`):
  - `#[derive(Clone, Copy, Debug, PartialEq, Eq)] pub enum TransactionStatus { Idle, InTransaction, Failed }` with `from_wire_byte(u8) -> Result<Self, PostgresError>` and `as_wire_byte(self) -> u8`
  - `#[derive(Clone, Copy, Debug, PartialEq, Eq)] pub struct BackendKey { pub process_id: i32, pub secret_key: i32 }`
  - `pub struct PostgresConnection` owning `stream: TransportStream`, `parameters: HashMap<String, String>`, `backend_key: Option<BackendKey>`, `transaction_status: TransactionStatus`, and `tls_server_end_point: Option<Vec<u8>>`
  - `pub enum BeginConnect { Attempt(ConnectAttempt), AlreadyReady }`
  - `#[derive(Clone)] pub struct SessionHandle` with `SessionHandle::new(config: PostgresConfig) -> Self`, `begin_connect(&self) -> Result<BeginConnect, PostgresError>`, `close(&self) -> impl Future<Output = Result<(), PostgresError>> + Send`, `is_closed(&self) -> bool`, `parameters(&self) -> HashMap<String, String>`
  - `impl ConnectAttempt { pub async fn run(self) -> Result<PendingSession, PostgresError> }`
  - `SessionHandle` clones share owned `Arc<PostgresConfig>` and `Arc<std::sync::Mutex<SessionState>>`; `ConnectAttempt` and `PendingSession` own a cloned `SessionHandle`, deadline, and armed rollback state. Both guards and the `run()` future are `Send + 'static` without borrowing the caller, holding a mutex guard across `.await`, or leaking allocations.
  - `impl PendingSession { pub fn commit(self) -> Result<(), PostgresError> }`
  - `PendingSession::connection(&self) -> &PostgresConnection` provides read-only prepublication inspection; `PostgresConnection::tls_server_end_point(&self) -> Option<&[u8]>` provides the negotiated channel-binding digest. Backend keys remain Rust-only.
  - Diagnostic fields use the direct `fallible_iterator::FallibleIterator` dependency and `ErrorField::{type_, value_bytes}` APIs.
- Rust (`src/rs/vertebra-postgres/src/tls.rs`, crate-private `pub(crate)`):
  - `pub(crate) struct DialedTransport { pub stream: TransportStream, pub tls_server_end_point: Option<Vec<u8>> }`
  - `pub(crate) async fn connect(config: &PostgresConfig, deadline: tokio::time::Instant) -> Result<DialedTransport, PostgresError>`
- Rust (`src/rs/vertebra-postgres/src/auth.rs`, crate-private `pub(crate)`):
  - `pub(crate) struct Authentication` (`Initial`, `PasswordSent`, `Complete`) with `Authentication::new() -> Self`, `Authentication::handle_message(&mut self, msg: &postgres_protocol::message::backend::Message, config: &PostgresConfig, out: &mut BytesMut) -> Result<bool, PostgresError>`, and `Authentication::is_complete(&self) -> bool`

### 2.3 Caller -> Seam -> Collaborator Integration Map

```text
SessionHandle::begin_connect()
  -> PendingSession::connection() -> PostgresConnection metadata getters (Rust adoption checks)
  -> ConnectAttempt::run() [single timeout_at(deadline)]
       -> tls::connect() -> frontend::ssl_request + TransportStream::upgrade_tls()
       -> protocol::encode_startup_frame() -> frontend::startup_message()
       -> protocol::read_message() -> backend::Message::parse()
       -> auth::Authentication::handle_message() -> frontend::password_message() / md5_hash()
  -> PendingSession::commit() -> SessionHandle::Ready(PostgresConnection)
```

### 2.4 Edge-Case & Alternatives Summary

| Edge Case / Failure Mode | Required Behavior |
| --- | --- |
| Backend sends length `< 4` or `> 16_777_216` | `read_message` fails with `PostgresError::Protocol` before allocating payload buffer. |
| Server sends `ParameterStatus`, `BackendKeyData`, or `ReadyForQuery` before `AuthenticationOk`, or sends `ReadyForQuery('T' \| 'E')` | Reject with `PostgresError::Protocol` and drop socket; `SessionHandle` transitions to `Closed`. |
| `client_encoding` missing or not `"UTF8"` at `ReadyForQuery(I)` | Reject with `PostgresError::Protocol` and close socket. |
| `ConnectAttempt` or `PendingSession` dropped (e.g. cancelled task or expired deadline before `commit()`) | Armed RAII guard rolls `SessionHandle` to terminal `Closed` and drops `TransportStream` (server observes EOF). |
| Server replies `b'N'` to `SSLRequest` under `Require` / `VerifyCa` / `VerifyFull`, or TLS handshake fails after `b'S'` | Fail terminally with `PostgresError::Transport`; never downgrade or reconnect in plaintext. |
| Server challenges cleartext/MD5 when `config.password` is `None`, or sends GSS/SSPI/Kerberos | Missing password returns `PostgresError::Config`; unsupported mechanism returns `PostgresError::UnsupportedAuth` without writing credentials. |

- **Alternatives Considered**: Publishing `Ready(PostgresConnection)` directly inside `ConnectAttempt::run` was rejected because an async caller cancelled between task completion and Python/caller adoption would leak an orphaned `Ready` session; two-phase `PendingSession::commit()` guarantees atomic ownership transfer.

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in ch02]` (all new symbols in `protocol.rs`, `tls.rs`, and `auth.rs` are crate-private, and `session.rs` `rlib` rustdoc comments land inline; user-facing TLS/auth documentation and OKF chapters are updated in `ch30:t01` and `ch30:t02`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test / Gate |
| --- | --- | --- | --- |
| R3 (bounded framing) | [t01](tasks/t01-bounded-codec.md) | `behavior_tdd` | `src/rs/vertebra-postgres/src/protocol.rs` (`protocol::tests`) |
| R3, R7 (session lifecycle & publication) | [t02](tasks/t02-trust-session-lifecycle.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/postgres_wire_tests.rs` |
| R4 (`SSLRequest` & TLS verification) | [t03](tasks/t03-sslrequest-and-tls-identity.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/postgres_tls_wire_tests.rs` |
| R5 (cleartext & MD5 auth) | [t04](tasks/t04-cleartext-and-md5-auth.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/postgres_auth_wire_tests.rs` |

## 5. Implementation Plan

- [x] [a00c3248] native-postgres-driver:ch02:t01 — Implement bounded PostgreSQL Protocol 3.0 startup encoder and backend frame decoder (tasks/t01-bounded-codec.md)
- [!] native-postgres-driver:ch02:t02 — Establish authenticated trust sessions with SessionHandle and PendingSession publication (tasks/t02-trust-session-lifecycle.md)
- [ ] native-postgres-driver:ch02:t03 — Negotiate PostgreSQL SSLRequest and verify rustls peer TLS identities (tasks/t03-sslrequest-and-tls-identity.md)
- [ ] native-postgres-driver:ch02:t04 — Authenticate cleartext password and MD5 startup exchanges (tasks/t04-cleartext-and-md5-auth.md)

**Chapter Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: Codec task 01 is complete at `a00c3248`; chapter `state: active`, `planning_status: complete`.
- **Task State**: Session task 02 is blocked on the direct diagnostic-iterator prerequisite and fresh chapter 01 review; TLS task 03 and password/MD5 task 04 remain pending.
- **Shared Contracts**: The session uses owned Arc-backed guards and read-only metadata getters; private codec expectations are removed when session consumers land.
- **Documentation**: `[0/0 files documented in ch02]`; full user and domain guidance remains assigned to chapter 30.
- **Resume**: Complete and review the existing chapter 01 prerequisite owners, then resume task 02.

[^rs-transport]: `src/rs/vertebra-db/src/connection.rs::TransportStream` — Shared TCP and `rustls` TLS upgrade stream primitives.
[^proto-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — Protocol 3.0 framing, `SSLRequest`, and auth state machine analysis.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Public connection constructor, TLS mode, and deadline contracts.
