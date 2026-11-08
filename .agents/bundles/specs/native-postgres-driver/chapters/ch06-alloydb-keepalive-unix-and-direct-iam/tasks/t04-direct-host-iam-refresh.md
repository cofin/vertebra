---
type: Task
id: "native-postgres-driver:ch06:t04"
chapter_id: ch06-alloydb-keepalive-unix-and-direct-iam
title: Refresh direct-host IAM passwords without changing transport policy
description: Resolve fresh scoped login tokens via ScopedTokenProvider for direct TCP and Unix sessions when enable_iam_auth is true without cloud URIs, superseding config.password while preserving direct TLS/Unix transport policy.
status: stable
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on:
  - "native-postgres-driver:ch06:t03"
read_set:
  - src/rs/vertebra-postgres/src/config.rs
  - src/rs/vertebra-postgres/src/credentials.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/auth.rs
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - src/rs/vertebra-postgres/src/config.rs
  - src/rs/vertebra-postgres/src/credentials.rs
  - src/rs/vertebra-postgres/src/session.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_auth_wire_tests.rs
estimated_loc: 205
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
updated_at: "2026-11-07T05:00:00Z"
commit: null
sources:
  - id: core-gcp-auth
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache
    title: Shared GCP token refresh cache and scope handling
  - id: pg-config
    resource: src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig
    title: Direct-host IAM configuration and scope override semantics
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Direct-host enable_iam_auth default sqlservice.login and explicit alloydb.login contract
tags:
  - postgres
  - iam
  - auth
  - credentials
---

# Task t04: Refresh Direct-Host IAM Passwords Without Changing Transport Policy

## Objective & Scope Boundary

Enable direct-host IAM password authentication (`enable_iam_auth = true` with `cloud_sql_instance = None` and `alloydb_instance_uri = None`) across `src/rs/vertebra-postgres/src/config.rs`, `src/rs/vertebra-postgres/src/credentials.rs`, and `src/rs/vertebra-postgres/src/session.rs` [^core-gcp-auth] [^pg-config] [^pg-contracts]. Before ordinary direct TCP (`SSLRequest`/`sslmode`) or Unix domain socket startup, `SessionHandle` lazily resolves `ScopedTokenProvider` inside the original `ConnectAttempt` `absolute_deadline` (defaulting to `https://www.googleapis.com/auth/sqlservice.login` unless overridden by explicit `iam_auth_options["scopes"]` such as `https://www.googleapis.com/auth/alloydb.login`), feeds `snapshot.access_token` (guarded by `ScopedLoginSnapshot::ensure_usable`) into the existing cleartext/MD5/SCRAM engine in place of `config.password`, and never contacts Cloud SQL/AlloyDB Admin certificate APIs or alters configured transport TLS policy.

## Bounded Read Manifest

- `src/rs/vertebra-postgres/src/config.rs` — Direct-host IAM option normalization from `ch06:t03`.
- `src/rs/vertebra-postgres/src/credentials.rs` — `ScopedTokenProvider`, `ScopedLoginSnapshot`, and `CLOUDSQL_LOGIN_SCOPE` from `ch05:t02..t03`.
- `src/rs/vertebra-postgres/src/session.rs` — `SessionHandle`, `ConnectAttempt`, and direct TCP/Unix session connect flows.
- `src/rs/vertebra-postgres/src/auth.rs` — Pre-credential `ScopedLoginSnapshot::ensure_usable` hook from `ch05:t04`.
- `.agents/bundles/specs/native-postgres-driver/contracts.md` — Chapter [spec.md](../spec.md) and [contracts.md](../../../contracts.md) `Connection constructors` direct-host IAM paragraph [^pg-contracts].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/config.rs`:
  - Expose direct-host IAM policy from `NormalizedPostgresConfig`: when `enable_iam_auth` is `true` and neither `cloud_sql_instance` nor `alloydb_instance_uri` is set, compute default required login scope `[CLOUDSQL_LOGIN_SCOPE.to_string()]` unless `iam_auth_options` supplies an explicit nonempty `scopes` list (parsed with standard GCP comma/whitespace semantics, e.g. `https://www.googleapis.com/auth/alloydb.login` for direct VPC/Auth-Proxy AlloyDB targets), in which case those explicit scopes are authoritative.
- `src/rs/vertebra-postgres/src/credentials.rs`:
  - Add `pub(crate) async fn resolve_direct_login(&self, default_scopes: &[String], absolute_deadline: tokio::time::Instant) -> Result<ScopedLoginSnapshot, PostgresError>` on `ScopedTokenProvider`:
    - Uses `iam_auth_options` when supplied (routing static tokens through `StaticLoginCredential` and dynamic sources through `GcpTokenCache::from_scoped_options` with authoritative `iam_auth_options["scopes"]` or `default_scopes`).
    - When `iam_auth_options` is `None`, derives the login cache from `cloud_auth_options` with `default_scopes` (`sqlservice.login`), rejecting opaque static `cloud_auth_options` tokens with `PostgresError::Auth`.
    - Performs ADC discovery / cache initialization lazily inside `connect` under `timeout_at(absolute_deadline, ...)`, reusing the same `ScopedTokenProvider` across reconnects on the owning `SessionHandle` (and later `PostgresPool`) within `vertebra-postgres`.
- `src/rs/vertebra-postgres/src/session.rs`:
  - When direct-host IAM is active on `NormalizedEndpoint::Tcp` or `NormalizedEndpoint::Unix`:
    1. Resolve `ScopedLoginSnapshot` via `provider.resolve_direct_login(..., absolute_deadline).await?` **before** dialing the PostgreSQL listener so grant/consent/refresh failures abort before opening a database socket.
    2. Proceed through unchanged direct transport (`ConnectOptions::tcp` + configured `SSLRequest`/`sslmode`/PEMs, or `ConnectOptions::unix` plaintext without adding TLS). Zero Cloud SQL/AlloyDB Admin discovery, ephemeral certificate, or AlloyDB metadata calls occur.
    3. Pass `&ScopedLoginSnapshot` as the effective password to `auth.rs` (superseding `config.password` without mutating `PostgresConfig`), enforcing `ensure_usable(clock.now_ms())` immediately before password/SASL frame emission.
    4. On server `ErrorResponse` `28P01` / `28xxx`, invalidate the cached generation in `ScopedTokenProvider`, close the session terminally without retrying a second token on that stream, and redact both the IAM token and any configured `config.password` from error messages.

## Edge Cases & Error Semantics

- Failed token acquisition (HTTP 400/403 `invalid_scope`, malformed JSON, or `connect_timeout` expiry during OAuth refresh) rolls back the `ConnectAttempt` reservation to `Closed` after runtime cleanup, makes `0` TCP/Unix accepts on the PostgreSQL listener, and maps `PostgresError::Auth` -> `GoogleCloudAuthError` (or timeout -> `ConnectionFailure`).
- `enable_iam_auth = true` on `unix_socket = Some(...)` with `sslmode = "disable"` resolves the IAM token and authenticates over the Unix domain socket without attempting TLS.
- Valid cached token is reused across sequential connects while `now + refresh_skew_ms < expires_at_unix_ms`, and refreshes automatically before a new physical connect once inside skew or invalidated.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/postgres_auth_wire_tests.rs` (`direct_iam_refresh`, `direct_iam_tls_expiry_before_credentials`, `direct_iam_grant_failure`, `direct_iam_authentication_failure`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Default `sqlservice.login` reuse, expiry refresh & password override (`direct_iam_refresh`) | Direct TCP (`sslmode = "disable"` and `"require"`), `enable_iam_auth = true`, `password = Some("dsn-pass-ignored")`, mock OAuth server + injected clock | OAuth POST requests `sqlservice.login`; PostgreSQL SCRAM/cleartext listener receives `"iam-tok-1"` (never `"dsn-pass-ignored"`) and reaches `TransactionStatus::Idle`; second connect while valid reuses `"iam-tok-1"` (`1` OAuth call); advancing clock into skew fetches `"iam-tok-2"` on next connect; `0` Admin API calls. |
| Explicit `alloydb.login` scope override & Unix transport | `enable_iam_auth = true`, `iam_auth_options = {"scopes": "https://www.googleapis.com/auth/alloydb.login", ...}` (plus Unix socket variant with `sslmode = "disable"`) | OAuth/metadata request carries exact `https://www.googleapis.com/auth/alloydb.login` scope; Unix variant connects plaintext with zero `SSLRequest` bytes and reaches `TransactionStatus::Idle`. |
| Held direct-host TLS expiry before credentials (`direct_iam_tls_expiry_before_credentials`) | Valid login snapshot at dial time (`expires_at_unix_ms = 1_400_000`, `skew = 300_000`, initial `now = 1_050_000`) on direct TCP `sslmode = "require"`; hold `SSLRequest` + TLS handshake at barrier and advance clock to `1_100_000` (`now + skew >= expires_at_unix_ms`) before password/SASL challenge | `ScopedLoginSnapshot::ensure_usable` aborts with `PostgresError::Auth` (`GoogleCloudAuthError`), transitions handle to `Closed`, and server observes EOF with `0` `PasswordMessage`/`SASLInitialResponse` credential bytes emitted. |
| Grant/consent failure & refresh timeout (`direct_iam_grant_failure`) | Mock OAuth server returns HTTP 400 `{"error":"invalid_scope"}`, malformed JSON, or stalls past `connect_timeout` | Fails before PostgreSQL listener receives any connection (`accept_count == 0`), transitions handle to `Closed`, and maps grant failure to `PostgresError::Auth`. |
| Server `28P01` terminal failure & redaction (`direct_iam_authentication_failure`) | Direct listener responds to SCRAM/password with `ErrorResponse` `28P01` (`"password authentication failed"`) | Returns `PostgresError::Server(err)` with `err.sqlstate == "28P01"` (`DatabaseError`), performs `0` same-stream retries, transitions to `Closed`, and error text contains neither `"iam-tok-1"` nor `"dsn-pass-ignored"`. |

## Verification

- Initial Red baseline:
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_auth_wire_tests direct_iam_`
- Final Green + Clippy gate:
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_auth_wire_tests direct_iam_`
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres`
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --test it explicit_login_scopes`
  - `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings`

## Acceptance Criteria

- [ ] Direct-host `enable_iam_auth = true` resolves fresh `sqlservice.login` (or authoritative explicit `iam_auth_options["scopes"]`) tokens lazily during `connect` under `absolute_deadline` and supersedes `config.password` without mutating `PostgresConfig` (R5).
- [ ] Direct TCP `SSLRequest`/`sslmode` and Unix plaintext transport policies remain unchanged with zero Cloud Admin or certificate API calls (R5).
- [ ] Grant/expiry failures abort before database dial (`PostgresError::Auth`), server `28xxx` failures close terminally without same-stream retry or secret leakage (`DatabaseError`), and commit `feat(postgres): preserve direct-host IAM token authentication` passes all tests (R5).

## Notes & Discoveries

- None.

[^core-gcp-auth]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache` — Shared GCP OAuth cache and explicit scope requests.
[^pg-config]: `src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig` — Direct-host IAM configuration precedents.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Direct-host IAM scope, password precedence, and non-retry contracts.
