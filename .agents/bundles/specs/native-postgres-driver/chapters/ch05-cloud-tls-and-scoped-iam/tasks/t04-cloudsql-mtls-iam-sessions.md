---
type: Task
id: "native-postgres-driver:ch05:t04"
chapter_id: ch05-cloud-tls-and-scoped-iam
title: Authenticate automatic Cloud SQL mTLS endpoints with IAM tokens
description: Implement pre-startup dial_cloudsql returning DialedCloudTransport over automatic Cloud SQL :3307 mTLS, enforce pre-credential ScopedLoginSnapshot::ensure_usable guards in auth.rs and unified startup in session.rs, and verify IAM vs built-in password startup without SSLRequest.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch05:t03"
read_set:
  - src/rs/vertebra-postgres/src/cloud/mod.rs
  - src/rs/vertebra-postgres/src/cloud/material.rs
  - src/rs/vertebra-postgres/src/auth.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-db/src/connection.rs
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - src/rs/vertebra-postgres/src/cloud/mod.rs
  - src/rs/vertebra-postgres/src/auth.rs
  - src/rs/vertebra-postgres/src/session.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs
estimated_loc: 230
plan_revision: 4
plan_commit: e8901de4
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
commit: null
sources:
  - id: db-conn
    resource: src/rs/vertebra-db/src/connection.rs::TransportStream
    title: Shared TransportStream::connect for EndpointTarget::CloudSql on port 3307
  - id: cloud-research
    resource: .agents/bundles/specs/native-postgres-driver/research/cloud-material_20261105.md
    title: Cloud SQL automatic mTLS :3307 and sqlservice.login authentication flow
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Cloud SQL constructor, IAM password, and pre-transmission token guard contracts
tags:
  - postgres
  - cloudsql
  - mtls
  - iam
---

# Task t04: Authenticate Automatic Cloud SQL mTLS Endpoints with IAM Tokens

## Objective & Scope Boundary

Wire automatic Cloud SQL `:3307` mTLS session establishment across `src/rs/vertebra-postgres/src/cloud/mod.rs`, `src/rs/vertebra-postgres/src/auth.rs`, and `src/rs/vertebra-postgres/src/session.rs` [^db-conn] [^cloud-research] [^pg-contracts]. `cloud::dial_cloudsql` prepares ephemeral certificate and scoped IAM login material via `CloudConnector::prepare`, connects over immediate TLS 1.3 (`EndpointTarget::CloudSql` without emitting PostgreSQL `SSLRequest` and without falling back to direct `:5432`), and returns a pre-startup `DialedCloudTransport` to `session.rs` **before** emitting PostgreSQL `StartupMessage` (so `session.rs` runs `StartupMessage` and authentication uniformly across direct and cloud streams, enabling `ch06:t02` to apply `set_tcp_keepalive` at one seam without touching `cloud/mod.rs`). `auth.rs` enforces `ScopedLoginSnapshot::ensure_usable(clock.now_ms())` immediately before any password or SASL credential response is written on the wire, reaching `TransactionStatus::Idle` inside the caller's single `absolute_deadline` and `PendingSession` guard.

## Bounded Read Manifest

- `src/rs/vertebra-postgres/src/cloud/mod.rs` — Cloud connector entrypoints and Cloud SQL discovery integration from `ch04:t03`.
- `src/rs/vertebra-postgres/src/cloud/material.rs` — `CloudConnector::prepare`, `PreparedCloudMaterial`, and `ScopedTokenProvider` wiring from `ch05:t01` and `ch05:t03`.
- `src/rs/vertebra-postgres/src/auth.rs` — Cleartext, MD5, `SCRAM-SHA-256`, and `SCRAM-SHA-256-PLUS` authentication state machine from `ch02:t04` and `ch03:t01..t03`.
- `src/rs/vertebra-postgres/src/session.rs` — `SessionHandle`, `ConnectAttempt`, `PendingSession`, `TransactionStatus::Idle`, and startup decoder from `ch02:t02..t03`.
- `src/rs/vertebra-db/src/connection.rs` — `TransportStream::connect` and `EndpointTarget::CloudSql` (`:3307` immediate mTLS) [^db-conn].
- `.agents/bundles/specs/native-postgres-driver/contracts.md` — Chapter [spec.md](../spec.md) and [contracts.md](../../../contracts.md) Cloud SQL IAM and pre-transmission lifetime contracts [^pg-contracts].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/auth.rs`:
  - Accept an optional pre-resolved `&ScopedLoginSnapshot` + `&dyn Clock` in startup credential challenge handling (`AuthenticationCleartextPassword`, `AuthenticationMD5Password`, `AuthenticationSASL`).
  - Immediately before encoding and writing the first password or SASL `client-first-message` frame, invoke `login_snapshot.ensure_usable(clock.now_ms())?`. If the token crossed `now + refresh_skew_ms >= expires_at_unix_ms` while TLS or server greeting was held, abort immediately with `PostgresError::Auth` (`0` credential frames written).
- `src/rs/vertebra-postgres/src/cloud/mod.rs`:
  - Define `pub(crate) struct DialedCloudTransport { pub stream: TransportStream, pub tls_server_end_point: Option<Vec<u8>>, pub login_snapshot: Option<ScopedLoginSnapshot> }`.
  - `pub(crate) async fn dial_cloudsql(config: &PostgresConfig, connector: &CloudConnector, absolute_deadline: tokio::time::Instant) -> Result<DialedCloudTransport, PostgresError>`:
    - Obtain `PreparedCloudMaterial` via `connector.prepare(cloud_config, absolute_deadline).await?`.
    - Dial `EndpointTarget::CloudSql` on resolved `:3307` via `tokio::time::timeout_at(absolute_deadline, TransportStream::connect(&connect_opts))` (remaining budget passed in `connect_opts.connect_timeout`). Mandatory TLS 1.3 and `CloudPeerIdentity::CloudSql` verification from `ch05:t01` apply regardless of `config.sslmode`.
    - Compute `tls_server_end_point` from `stream.peer_leaf_der()` for `SCRAM-SHA-256-PLUS` binding (`ch03:t02..t03`) and return `DialedCloudTransport` **before** sending PostgreSQL `StartupMessage` (and **never** sending the 8-byte `SSLRequest` `80877103`).
    - When `config.enable_iam_auth` is `true`, populate `login_snapshot: Some(snapshot)` (`sqlservice.login`); when `enable_iam_auth` is `false`, populate `login_snapshot: None` without fetching any `sqlservice.login` token.
- `src/rs/vertebra-postgres/src/session.rs`:
  - Route `ConnectAttempt::run` to `cloud::dial_cloudsql` whenever `config.cloud_sql_instance` is `Some(_)`, retaining one `CloudConnector` per `SessionHandle` (shared only within `vertebra-postgres`, never across `cdylib` boundaries), then run unified PostgreSQL `StartupMessage` (`3.0`), `auth::Authentication` (using `dialed.login_snapshot` when `Some`, else `config.password`), and `ReadyForQuery` (`TransactionStatus::Idle`) verification over `DialedCloudTransport` into an armed `PendingSession` under the single `absolute_deadline`.

## Edge Cases & Error Semantics

- Cloud SQL Admin failure, TLS 1.3 peer-identity failure, or `:3307` connection refusal fails terminally and **never** falls back or reroutes to direct TCP `host:5432`.
- Server `ErrorResponse` `28P01` / `28000` during Cloud SQL IAM authentication transitions session to `Closed` and returns `PostgresError::Server(err)` (`err.sqlstate == "28P01"`, mapped to `DatabaseError`), invalidating the cached token generation in Rust without retrying a second token on the same stream.
- A valid static or dynamic login snapshot that crosses `now + refresh_skew_ms` while TLS or server startup greeting is held at a test barrier fails via `ScopedLoginSnapshot::ensure_usable` with `PostgresError::Auth` (`GoogleCloudAuthError`) and writes zero `PasswordMessage`/`SASLInitialResponse` bytes.
- Total `connect_timeout` spans Admin `connectSettings`, `:generateEphemeralCert`, `:3307` TCP+TLS 1.3 handshake, and PostgreSQL `ReadyForQuery` (`TransactionStatus::Idle`) together without resetting per stage.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs` (`cloudsql_mtls_*` and `static_login_lifetime_and_scopes` wire tests).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Automatic Cloud SQL IAM mTLS startup & zero `SSLRequest` (`cloudsql_mtls_iam_startup`) | `cloud_sql_instance = "proj:us-central1:inst"`, `enable_iam_auth = true`, mock SQL Admin + `:3307` TLS 1.3 PostgreSQL listener | First decrypted server bytes equal `StartupMessage` (`0x00030000`, not `SSLRequest` `80877103`); server receives `sqlservice.login` token as password and reaches `TransactionStatus::Idle` (`ReadyForQuery` `b'I'`). |
| Cloud SQL built-in password mode skips login token (`cloudsql_mtls_builtin_password`) | `cloud_sql_instance = "proj:us-central1:inst"`, `enable_iam_auth = false`, `password = Some("db-pass")` | SQL Admin `:generateEphemeralCert` body omits `access_token`; `0` `sqlservice.login` token requests occur; server authenticates `"db-pass"` and reaches `TransactionStatus::Idle`. |
| Login token crosses skew during held TLS handshake (`static_login_lifetime_and_scopes`) | Valid static `iam_auth_options` (`expires_at_unix_ms = 1_300_005`, `skew = 300_000`, `now = 1_000_000`); hold TLS/greeting barrier and advance clock to `1_000_005` | Fails with `PostgresError::Auth` before credential response; server observes EOF with `0` `PasswordMessage`/`SASLInitialResponse` bytes. |
| Incorrect Cloud SQL peer identity & Admin HTTP 403 (`cloudsql_mtls_peer_and_admin_errors`) | Mock `:3307` leaf CN `"proj:wrong-inst"` (no matching SAN), or SQL Admin returning HTTP 403 | Fails terminally (`Transport` / `Auth`) with `0` connections attempted against `:5432`. |
| Server `28P01` auth failure & total deadline (`cloudsql_mtls_auth_error_and_deadline`) | `:3307` listener replies `ErrorResponse` `28P01` (or stalls past `connect_timeout`) | Returns `PostgresError::Server(err)` with `err.sqlstate == "28P01"` and `0` same-stream retries (or `PostgresError::Transport` timeout within single `absolute_deadline`). |

## Verification

- Initial Red baseline:
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_cloud_wire_tests cloudsql_mtls_`
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_cloud_wire_tests static_login_`
- Final Green + Clippy gate:
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_cloud_wire_tests cloudsql_mtls_`
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_cloud_wire_tests static_login_`
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres`
  - `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings`

## Acceptance Criteria

- [ ] `dial_cloudsql` prepares automatic ephemeral certificates and scoped tokens via `CloudConnector::prepare`, connects to `:3307` over verified TLS 1.3 without sending `SSLRequest` or falling back to direct TCP, and returns pre-startup `DialedCloudTransport` to `session.rs` (R5, R9).
- [ ] `session.rs` and `auth.rs` drive `StartupMessage`, enforce `ScopedLoginSnapshot::ensure_usable` immediately before password/SASL transmission (`0` credential bytes emitted on expiry), and reach `TransactionStatus::Idle` (R5, R9).
- [ ] Failed server authentication (`PostgresError::Server(err)` with `28xxx` `sqlstate`) is terminal without same-stream token retry, total `absolute_deadline` spans Admin + mTLS + startup, and commit `feat(postgres): connect automatic Cloud SQL IAM endpoints` passes all canonical wire tests (R5, R9).

## Notes & Discoveries

- None.

[^db-conn]: `src/rs/vertebra-db/src/connection.rs::TransportStream` — Shared Cloud SQL `:3307` mTLS transport.
[^cloud-research]: `.agents/bundles/specs/native-postgres-driver/research/cloud-material_20261105.md` — Cloud SQL automatic mTLS and IAM startup protocol.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Cloud SQL IAM password, error mapping, and deadline contracts.
