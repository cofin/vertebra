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
current_task: null
plan_revision: 6
plan_commit: 8833b064
state_revision: 5
last_verified_checkpoint: ad8be7b6
last_reviewed_commit: null
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-07T23:44:42Z"
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
- **Bounded 16 MiB Wire Decoder**: `protocol::read_message` reads the 5-byte `[tag, i32_be_len]` header, validates `4 <= len <= 16_777_216` before reserving capacity, and parses the exact frame envelope via `postgres_protocol::message::backend::Message::parse` without trailing frame bytes or unbounded allocations; session consumers exhaust lazy notice/error fields with checked UTF8 [^proto-research].
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

`read_message`, `encode_startup_frame`, and `TERMINATE_FRAME` have real session consumers and retain crate-private visibility without dead-code allowances or expectations. The all-target `-D warnings` gate applies unchanged.

- Rust (`src/rs/vertebra-postgres/src/protocol.rs`, crate-private `pub(crate)`):
  - `pub(crate) const MAX_BACKEND_FRAME_LENGTH: i32 = 16_777_216;`
  - `pub(crate) const MIN_BACKEND_FRAME_LENGTH: i32 = 4;`; the pinned upstream startup encoder emits Protocol 3.0 (`196_608`).
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
  - `PendingSession::connection(&self) -> &PostgresConnection` provides read-only prepublication inspection; `PostgresConnection::tls_server_end_point(&self) -> Option<&[u8]>` returns `None` until `ch03:t02` extracts the verified peer digest. Backend keys remain Rust-only.
  - Diagnostic fields use the direct `fallible_iterator::FallibleIterator` dependency and `ErrorField::{type_, value_bytes}` APIs.
  - `NoticeResponse` fields are fully validated before discard at every startup phase. `ErrorResponse` fields are fully validated before typed ownership; free text masks the non-empty configured password and actual submitted MD5 token while structured `S`/`V`/`C`/`P`/`p`/`L` values remain exact. Empty secrets leave text unchanged.
- Rust (`src/rs/vertebra-postgres/src/tls.rs`, crate-private `pub(crate)`):
  - `pub(crate) struct DialedTransport { pub stream: TransportStream, pub tls_server_end_point: Option<Vec<u8>> }`
  - `pub(crate) async fn connect(config: &PostgresConfig, deadline: tokio::time::Instant) -> Result<DialedTransport, PostgresError>`
- Rust (`src/rs/vertebra-postgres/src/auth.rs`, crate-private `pub(crate)`):
  - `pub(crate) struct Authentication { phase: AuthPhase, md5_response: Option<String> }` (`Initial`, `PasswordSent`, `Complete`) with `Authentication::new() -> Self`, `Authentication::handle_message(&mut self, msg: &postgres_protocol::message::backend::Message, config: &PostgresConfig, out: &mut BytesMut) -> Result<bool, PostgresError>`, and `Authentication::is_complete(&self) -> bool`
  - `Authentication::redact_server_text(&self, text: &str, config: &PostgresConfig) -> String` masks the actual MD5 response before the non-empty configured password. The private credential-bearing authentication object has no `Debug` implementation.

### 2.3 Caller -> Seam -> Collaborator Integration Map

```text
SessionHandle::begin_connect()
  -> ConnectAttempt::run() [single timeout_at(deadline)]
       -> tls::connect() -> frontend::ssl_request + TransportStream::upgrade_tls()
       -> protocol::encode_startup_frame() -> frontend::startup_message()
       -> protocol::read_message() -> backend::Message::parse()
       -> auth::Authentication::handle_message() -> frontend::password_message() / md5_hash()
  -> PendingSession::connection() -> PostgresConnection metadata getters (Rust adoption checks)
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
| Malformed lazy notice/error field body | Session consumer exhausts the pinned fallible iterator and validates UTF8; malformed terminators/trailing bytes fail with `PostgresError::Protocol` and socket closure. |
| Server reflects configured password or submitted MD5 token | Human/free-text diagnostics are redacted before Display/Debug ownership; exact structured protocol fields retain coincidental low-entropy characters. |

- **Alternatives Considered**: Publishing `Ready(PostgresConnection)` directly inside `ConnectAttempt::run` was rejected because an async caller cancelled between task completion and Python/caller adoption would leak an orphaned `Ready` session; two-phase `PendingSession::commit()` guarantees atomic ownership transfer.

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in ch02]` (all new symbols in `protocol.rs`, `tls.rs`, and `auth.rs` are crate-private, and `session.rs` `rlib` rustdoc comments are inline; user-facing TLS/auth guides and repository documentation belong to `ch30:t01`–`t02`, and durable OKF consolidation belongs to `ch30:t03`–`t04`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test / Gate |
| --- | --- | --- | --- |
| R3 (bounded framing) | [t01](tasks/t01-bounded-codec.md) | `behavior_tdd` | `src/rs/vertebra-postgres/src/protocol.rs` (`protocol::tests`) |
| R3, R7 (session lifecycle & publication) | [t02](tasks/t02-trust-session-lifecycle.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/postgres_wire_tests.rs` |
| R4 (`SSLRequest` & TLS verification) | [t03](tasks/t03-sslrequest-and-tls-identity.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/postgres_tls_wire_tests.rs` |
| R5 (cleartext & MD5 auth) | [t04](tasks/t04-cleartext-and-md5-auth.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/postgres_auth_wire_tests.rs` |

## 5. Implementation Plan

- [x] [a00c3248] native-postgres-driver:ch02:t01 — Implement bounded PostgreSQL Protocol 3.0 startup encoder and backend frame decoder (tasks/t01-bounded-codec.md)
- [x] [81791de2] native-postgres-driver:ch02:t02 — Establish authenticated trust sessions with SessionHandle and PendingSession publication (tasks/t02-trust-session-lifecycle.md)
- [x] [ad8be7b6] native-postgres-driver:ch02:t03 — Negotiate PostgreSQL SSLRequest and verify rustls peer TLS identities (tasks/t03-sslrequest-and-tls-identity.md)
- [x] [c5173f1b] native-postgres-driver:ch02:t04 — Authenticate cleartext password and MD5 startup exchanges (tasks/t04-cleartext-and-md5-auth.md)

**Chapter Recovery Checkpoint:** `flock /tmp/vertebra-flow-shm-tests.lock cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --locked && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets --locked -- -D warnings` exits `0` at `ad8be7b6`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: Checkpoint `ad8be7b6` passes all 54 Rust tests (`9` codec, `13` auth, `10` TLS, `22` wire/config/error) under the shared SHM test lock and locked all-target Clippy with `-D warnings`; upstream chapter 01 remains complete and reviewed at `3d68c853`.
- **Task State**: All four tasks are complete at `a00c3248`, `81791de2`, `ad8be7b6`, and `c5173f1b`. The chapter remains `active` with `current_task: null` and `last_reviewed_commit: null` pending renewed correctness and fresh independent quality review.
- **Shared Contracts**: Bounded framing, fully consumed lazy diagnostic bodies, owned session publication, mode-specific TLS verification, and credential-safe trust/password/MD5 authentication retain their declared phase and error behavior. Controlled Tokio time, peer barriers, and independent stage watchdogs prove the reserved TLS deadline remains pending at 249 ms and terminal at 251 ms, without a wall-clock threshold.
- **Documentation**: `[0/0 files documented in ch02]`; chapter 30 retains full driver guides and knowledge synchronization.
- **Next Action**: Run `/flow:review native-postgres-driver`; advance to chapter 03 only after correctness and fresh independent quality review pass.

[^rs-transport]: `src/rs/vertebra-db/src/connection.rs::TransportStream` — Shared TCP and `rustls` TLS upgrade stream primitives.
[^proto-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — Protocol 3.0 framing, `SSLRequest`, and auth state machine analysis.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Public connection constructor, TLS mode, and deadline contracts.
