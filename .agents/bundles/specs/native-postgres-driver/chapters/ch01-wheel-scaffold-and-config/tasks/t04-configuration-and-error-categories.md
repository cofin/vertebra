---
type: Task
id: "native-postgres-driver:ch01:t04"
chapter_id: ch01-wheel-scaffold-and-config
title: Implement pure PostgresConfig validation and sanitized PostgresError categories
description: Declare the complete owned PostgresConfig option record matching contracts.md, enforce pure scalar/port/timeout/NUL/PEM/five-spelling TlsMode validation with secret redaction, and define ServerError and PostgresError categories.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch01:t03"
read_set:
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-db/src/connection.rs
  - src/rs/vertebra-spanner/src/errors.rs
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
files:
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/src/config.rs
  - src/rs/vertebra-postgres/src/errors.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_wire_tests.rs
estimated_loc: 235
plan_revision: 4
plan_commit: 45ff9563
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
updated_at: "2026-11-07T16:40:00Z"
commit: null
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Connection constructor option inventory, defaults, and redaction rules
  - id: db-conn-tls
    resource: src/rs/vertebra-db/src/connection.rs::TlsMode
    title: Shared TlsMode enum and transport SQLSTATE error categories
  - id: spanner-err
    resource: src/rs/vertebra-spanner/src/errors.rs::spanner_error_to_py
    title: Reference native-to-Python database error mapping boundaries
tags:
  - postgres
  - config
  - errors
  - validation
---

# Task t04: Implement Pure `PostgresConfig` Validation and Sanitized `PostgresError` Categories

## Objective & Scope Boundary

Declare the complete owned `PostgresConfig` option record matching every constructor field and default in [contracts.md](../../../contracts.md), implement pure in-memory scalar/port/timeout/NUL/PEM/TLS-mode validation without DNS, file, or network I/O, and implement `ServerError` plus `PostgresError` (`PostgresError::Server(ServerError)`) with 5-byte ASCII SQLSTATE retention, diagnostic field storage, and secret redaction [^pg-contracts] [^db-conn-tls]. Advanced cloud/Unix cross-field projection lands in `ch04`–`ch06` (`ch06:t03`) and PyO3 exception resolution lands in `ch07:t02` ([../spec.md](../spec.md)).

## Bounded Read Manifest

- `src/rs/vertebra-postgres/src/lib.rs` — Crate root module declarations from `ch01:t01`.
- `src/rs/vertebra-db/src/connection.rs::TlsMode` — Shared `TlsMode` variants (`Disable`, `Prefer`, `Require`, `VerifyCa`, `VerifyFull`) [^db-conn-tls].
- `src/rs/vertebra-spanner/src/errors.rs` — Sanitized error boundary conventions before Python conversion [^spanner-err].
- `.agents/bundles/specs/native-postgres-driver/contracts.md` — Exact `Connection constructors` signature, defaults, five-spelling `sslmode`, and secret redaction rules [^pg-contracts].
- `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Driver configuration ownership and clean-break DSN rules.

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/lib.rs`:
  - Export `pub mod config;` and `pub mod errors;` on the `rlib` seam (do not register partial Python classes in `_postgres`).
- `src/rs/vertebra-postgres/src/config.rs`:
  - `#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)] pub enum ChannelBindingPolicy { Disable, #[default] Prefer, Require }` with `ChannelBindingPolicy::parse(raw: &str) -> Result<Self, PostgresError>` accepting exact `"disable"`, `"prefer"`, `"require"`.
  - `pub fn parse_strict_sslmode(raw: &str) -> Result<TlsMode, PostgresError>` accepting ONLY exact lowercase `"disable"`, `"prefer"`, `"require"`, `"verify-ca"`, `"verify-full"`.
  - `#[derive(Clone)] pub struct RawPostgresOptions` with `Default` matching `contracts.md` defaults (`host = "localhost"`, `port = 5432`, `sslmode = "verify-full"`, `channel_binding = "prefer"`, `connect_timeout = 10.0`, `application_name = "vertebra"`, `keepalives = true`, `cloud_sql_ip_type = "PRIVATE"`, `alloydb_ip_type = "PRIVATE"`, `enable_iam_auth = false`, plus `user`, `database`, `password`, `unix_socket`, `ca_pem`, `client_cert_pem`, `client_key_pem`, `server_name`, `session_gucs`, `keepalives_idle`, `keepalives_interval`, `keepalives_count`, `cloud_sql_instance`, `alloydb_instance_uri`, `cloud_auth_options`, `iam_auth_options`).
  - `#[derive(Clone, PartialEq, Eq)] pub struct PostgresConfig` owning validated fields (`port: u16`, `sslmode: TlsMode`, `channel_binding: ChannelBindingPolicy`, `connect_timeout: Duration`, copied `HashMap<String, String>` maps, and owned `Vec<u8>` PEM buffers, with zero public mutating setters).
  - `impl PostgresConfig { pub fn validate(raw: RawPostgresOptions) -> Result<Self, PostgresError> }`.
  - `impl fmt::Debug for PostgresConfig` redacting `password` (`"<redacted>"` when `Some`), `client_key_pem` (`"<redacted>"` when `Some`), `cloud_auth_options`, and `iam_auth_options`.
- `src/rs/vertebra-postgres/src/errors.rs`:
  - `#[derive(Debug, Clone, PartialEq, Eq)] pub struct ServerError { pub severity: String, pub sqlstate: String, pub message: String, pub diagnostics: Vec<(u8, String)> }` with `Display` formatting only sanitized `severity`, `sqlstate`, and primary `message` (omitting `diagnostics` such as `b'D'`, `b'H'`, `b'W'` from default error text).
  - `#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)] pub enum PostgresError`:
    - `Config(String)`
    - `Interface(String)`
    - `Data(String)`
    - `UnsupportedAuth(String)`
    - `UnsupportedFeature(String)`
    - `Transport(String)`
    - `Protocol(String)`
    - `Server(ServerError)`
  - `impl PostgresError`:
    - `pub fn server(severity: impl Into<String>, sqlstate: &str, message: impl Into<String>, diagnostics: Vec<(u8, String)>) -> Result<Self, Self>` validating exact 5-byte ASCII alphanumeric `sqlstate` (returning `PostgresError::Protocol` if malformed) and constructing `PostgresError::Server(ServerError { severity: severity.into(), sqlstate: sqlstate.to_owned(), message: message.into(), diagnostics })`.
    - `pub fn sqlstate(&self) -> Option<&str>` returning `Some(err.sqlstate.as_str())` for `PostgresError::Server(err)` and `None` otherwise.

## Edge Cases & Error Semantics

- Reject empty or NUL-containing (`'\0'`) `user`, `database`, `host`, or `server_name` (`Some("")` or `Some("...\0...")`) with `PostgresError::Config`.
- Reject NUL bytes inside `password` or `application_name`, and reject empty `ca_pem`, `client_cert_pem`, or `client_key_pem` (`Some(vec![])`).
- Require `client_cert_pem` and `client_key_pem` to be supplied together (`Some`/`Some` or `None`/`None`); mismatched pairs return `PostgresError::Config` without formatting key bytes.
- Validate signed `port: i64` lies in `1..=65535` before casting to `u16`; validate `connect_timeout: f64` is finite (`is_finite()`), `> 0.0`, and convertible via `Duration::try_from_secs_f64` without overflow.
- Reject TCP `host` values that contain `"://"` (URIs) or `'/'` Unix socket path separators, keeping DSN/URI parsing exclusively in the Python adapter (`ch25:t01`).
- Formatting `PostgresConfig` via `{:?}` or `PostgresError` via `{}` must never emit `password`, `client_key_pem`, auth option token values, or unredacted `diagnostics` detail strings.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/postgres_wire_tests.rs` importing `_postgres::config::{ChannelBindingPolicy, PostgresConfig, RawPostgresOptions}` and `_postgres::errors::{PostgresError, ServerError}`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Default option validation | `RawPostgresOptions { user: "app".into(), database: "prod".into(), ..Default::default() }` | `Ok(cfg)` with `host == "localhost"`, `port == 5432`, `sslmode == TlsMode::VerifyFull`, `channel_binding == ChannelBindingPolicy::Prefer`, `connect_timeout == Duration::from_secs(10)`, `application_name == "vertebra"`, `keepalives == true`, `cloud_sql_ip_type == "PRIVATE"`, `alloydb_ip_type == "PRIVATE"`. |
| Missing/empty/NUL identifiers | `user = ""`, `database = "db\0x"`, `host = ""`, `server_name = Some("sni\0".into())`, `application_name = "vtb\0"` | Each returns `Err(PostgresError::Config(_))`. |
| URI or path in TCP `host` | `host = "postgresql://localhost/db"` or `host = "/var/run/postgresql"` | Returns `Err(PostgresError::Config(_))` without DNS lookup. |
| Signed port & timeout bounds | `port = 0`, `port = -1`, `port = 65536`; `connect_timeout = 0.0`, `-5.0`, `f64::NAN`, `f64::INFINITY` | Each returns `Err(PostgresError::Config(_))`. |
| Mismatched client mTLS PEM pair | `client_cert_pem = Some(b"CERT".to_vec())`, `client_key_pem = None` (and inverse with secret `"PRIVATE-KEY-SECRET"`) | Returns `Err(PostgresError::Config(msg))` where `!msg.contains("PRIVATE-KEY-SECRET")`. |
| Strict five-spelling `sslmode` | Valid: `"disable"`, `"prefer"`, `"require"`, `"verify-ca"`, `"verify-full"`; Invalid: `"allow"`, `"true"`, `"verify_full"`, `"PREFER"` | Valid inputs map to `TlsMode`; invalid inputs return `Err(PostgresError::Config(_))`. |
| `ServerError` diagnostics & secret redaction | `password = Some("s3cr3t-pw\0".into())` error + valid `PostgresConfig` `{:?}` + `PostgresError::server("FATAL", "28P01", "password authentication failed", vec![(b'D', "secret detail".into())])` | `{:?}` and `{}` exclude `"s3cr3t-pw"` and `"secret detail"` (while `ServerError.diagnostics` retains `(b'D', "secret detail")`); `err.sqlstate() == Some("28P01")` and invalid `"28"` SQLSTATE returns `Err(PostgresError::Protocol(_))`. |

## Verification

Write the failing tests in `src/rs/vertebra-postgres/tests/postgres_wire_tests.rs` first (confirming Red), implement `config.rs` and `errors.rs`, and verify exit `0`:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_wire_tests --locked
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --locked
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets --locked -- -D warnings
```

Commit as `feat(postgres): validate owned connection options and error categories`.

## Acceptance Criteria

- [ ] `PostgresConfig` owns every constructor field and default specified in `contracts.md` without exposing partial mutating setters or reparsing DSNs (R2, R3).
- [ ] Pure validation rejects empty/NUL strings, out-of-range ports, non-finite/non-positive timeouts, mismatched PEM pairs, URI/path hosts, and non-canonical `sslmode` / `channel_binding` values without I/O (R2, R3).
- [ ] `ServerError` (`severity`, `sqlstate`, `message`, `diagnostics`) and `PostgresError::Server(ServerError)` retain validated 5-byte ASCII SQLSTATE codes and structured diagnostic tuples upfront while redacting secrets and `D`/`H`/`W` details from formatted messages (R2, R3).

## Notes & Discoveries

- [2026-11-07 16:40] Defining `ServerError` (`severity`, `sqlstate`, `message`, `diagnostics: Vec<(u8, String)>`) and `PostgresError::Server(ServerError)` upfront in `ch01:t04` ensures `ch09:t02` simple-query error projections require zero breaking variant changes across `ch02`–`ch08`.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Public constructor fields, defaults, and redaction contract.
[^db-conn-tls]: `src/rs/vertebra-db/src/connection.rs::TlsMode` — Shared transport TLS verification modes.
[^spanner-err]: `src/rs/vertebra-spanner/src/errors.rs::spanner_error_to_py` — Reference native database error sanitization seam.
