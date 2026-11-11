---
type: Task
id: "native-postgres-driver:ch06:t03"
chapter_id: ch06-alloydb-keepalive-unix-and-direct-iam
title: Project complete connection options and establish Unix domain socket startup
description: Normalize all 25 constructor options into canonical endpoint, credential, keepalive, and startup GUC policies, project application_name and session_gucs into StartupMessage, and establish Unix socket sessions without SSLRequest.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch06:t02"
read_set:
  - src/rs/vertebra-postgres/src/config.rs
  - src/rs/vertebra-postgres/src/protocol.rs
  - src/rs/vertebra-postgres/src/tls.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-db/src/connection.rs
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - src/rs/vertebra-postgres/src/config.rs
  - src/rs/vertebra-postgres/src/protocol.rs
  - src/rs/vertebra-postgres/src/session.rs
tests:
  - src/rs/vertebra-postgres/src/config.rs
  - src/rs/vertebra-postgres/tests/postgres_wire_tests.rs
estimated_loc: 240
plan_revision: 5
plan_commit: 936daa9f
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
commit: 2bcade62
sources:
  - id: db-conn
    resource: src/rs/vertebra-db/src/connection.rs::ConnectOptions
    title: ConnectOptions::unix and TransportStream::connect Unix domain socket path
  - id: pg-config
    resource: src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig
    title: Existing build_session_gucs application_name and session_gucs precedence
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Complete 25-option constructor normalization, Unix exclusions, and startup GUC rules
tags:
  - postgres
  - config
  - unix
  - startup
---

# Task t03: Project Complete Connection Options and Establish Unix Domain Socket Startup

## Objective & Scope Boundary

Complete pure validation and normalization of all 25 public constructor parameters in `src/rs/vertebra-postgres/src/config.rs`, project `application_name` and overlaid `session_gucs` directly into `StartupMessage` in `src/rs/vertebra-postgres/src/protocol.rs`, and wire local Unix domain socket session establishment (`ConnectOptions::unix` without `SSLRequest` or TCP keepalive calls) in `src/rs/vertebra-postgres/src/session.rs` [^db-conn] [^pg-config] [^pg-contracts]. Every field in `contracts.md::Connection constructors` becomes reachable and validated before the Python `SyncPostgresClient` and `AsyncPostgresClient` facades land in Chapter 07. Direct-host IAM token execution at connect time is completed in `ch06:t04`.

## Bounded Read Manifest

- `src/rs/vertebra-postgres/src/config.rs` — Scalar/TLS validation (`ch01:t04`) and keepalive validation (`ch06:t02`).
- `src/rs/vertebra-postgres/src/protocol.rs` — `StartupMessage` encoder and bounded backend frame codec (`ch02:t01`).
- `src/rs/vertebra-postgres/src/tls.rs` — Direct TCP `SSLRequest` and `channel_binding` rules (read-only context confirming Unix bypasses TLS entirely).
- `src/rs/vertebra-postgres/src/session.rs` — `SessionHandle`, `ConnectAttempt`, and `PendingSession` startup state machine.
- `src/rs/vertebra-db/src/connection.rs` — `ConnectOptions::unix` and `TransportStream::Unix` [^db-conn].
- `.agents/bundles/specs/native-postgres-driver/contracts.md` — Chapter [spec.md](../spec.md) and [contracts.md](../../../contracts.md) `Connection constructors` [^pg-contracts].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/config.rs`:
  - Normalize the complete 25-field `PostgresConfig` once (purely, with zero DNS, file, network, or credential-refresh side effects) into canonical endpoint, TLS/binding, keepalive, IAM/Admin credential, and startup GUC structures suitable for both direct clients and pool `validate_configuration` equality:
    - **Endpoint mutual exclusion**: `unix_socket`, `cloud_sql_instance`, and `alloydb_instance_uri` are pairwise mutually exclusive (`> 1` set -> `PostgresError::Config`).
    - **`unix_socket` rules**: exact nonempty NUL-free socket file path (never a directory `.s.PGSQL.<port>` expansion here; adapter DSN bridge owns directory-to-socket conversion in `ch25:t01`). Requires `sslmode == TlsMode::Disable`, `ca_pem.is_none()`, `client_cert_pem.is_none()`, `client_key_pem.is_none()`, `server_name.is_none()`, `channel_binding != ChannelBindingPolicy::Require`, and no explicit keepalive tuning (`keepalives_idle.is_none() && keepalives_interval.is_none() && keepalives_count.is_none()`; default `keepalives == true` is a no-op on Unix).
    - **Direct `host` rules**: nonempty NUL-free IPv4, IPv6, or DNS hostname — reject URI schemes (`"://"`) and Unix path prefixes (`host.starts_with('/')`) on `host` (`unix_socket` must be used for Unix sockets).
    - **Cloud rules**: validate `CloudSqlInstanceUri::parse` / `AlloyDbClusterUri::parse` and `IpType` (`"PUBLIC"`, `"PRIVATE"`, `"PSC"` exact case-insensitive normalized spellings); mandatory cloud TLS 1.3 peer identity applies independently of `sslmode`; reject opaque static `access_token`/`bearer_token` in `cloud_auth_options` when login credentials are required (Cloud SQL with `enable_iam_auth = true`, or any AlloyDB mode) and `iam_auth_options` is `None`.
    - **Channel binding**: exact `"disable"`, `"prefer"`, `"require"` spellings (`sslmode == Disable` + `channel_binding == Require` rejected).
    - **Direct-host IAM**: `enable_iam_auth == true` with no cloud resource is valid on both direct TCP and Unix (`iam_auth_options` or derived `cloud_auth_options`, rejecting opaque static `cloud_auth_options` without `iam_auth_options`).
    - **Startup parameters (`application_name` & `session_gucs`)**: copy `session_gucs` into an owned deterministic map (`BTreeMap<String, String>`); reject empty or NUL-containing keys/values; reject reserved startup keys `"user"`, `"database"`, `"options"`, and `"replication"` (ASCII case-insensitive check for protocol keys where applicable, exact key check); if `"client_encoding"` is present in `session_gucs`, require normalized `"UTF8"` (rejecting `"SQL_ASCII"`, `"LATIN1"`, etc.) and deduplicate with the mandatory `"client_encoding" = "UTF8"` startup parameter; seed `"application_name"` from `config.application_name` first and overlay `session_gucs` on top so an explicit `session_gucs["application_name"]` wins, matching `PostgreSQLConfig.build_session_gucs` [^pg-config].
- `src/rs/vertebra-postgres/src/protocol.rs`:
  - Update `StartupMessage` encoding to emit `"user"`, `"database"`, `"client_encoding" => "UTF8"`, and the normalized startup GUC map (`application_name` + overlaid `session_gucs`) verbatim as NUL-terminated key/value pairs without SQL `SET` interpolation or `-c` splitting.
- `src/rs/vertebra-postgres/src/session.rs`:
  - On `#[cfg(unix)]`, connect `NormalizedEndpoint::Unix { path }` via `tokio::time::timeout_at(absolute_deadline, TransportStream::connect(&ConnectOptions::unix(path, remaining)))`, skip `SSLRequest` and `set_tcp_keepalive`, and run the shared startup/auth engine (`channel_binding` unavailable -> plain SCRAM/MD5/cleartext/trust) into `PendingSession`.
  - On `#[cfg(not(unix))]`, return `Err(PostgresError::UnsupportedFeature("Unix domain sockets are not supported on this platform".into()))` (mapped to `vertebra._vertebra.NotSupportedError`).

## Edge Cases & Error Semantics

- Invalid mapping key/value Python types (`int`, `None`, `bytes`) are rejected as `TypeError` at FFI extraction (Chapter 07), while semantic option conflicts in `PostgresConfig` return `PostgresError::Config` -> `ValueError`.
- Custom dotted GUCs and values containing spaces (e.g. `"app.tenant_id" => "org 42"`, `"search_path" => "tenant_a, public"`) are preserved byte-for-byte in `StartupMessage`.
- Unix listener startup sends `0x00030000` (`StartupMessage`) as the very first 4 payload bytes after `0x000000..` length prefix — never `SSLRequest` (`80877103`).

## Public-Seam Test Blueprint

- **Canonical test files**: `src/rs/vertebra-postgres/src/config.rs` (`complete_option_validation`) and `src/rs/vertebra-postgres/tests/postgres_wire_tests.rs` (`unix_*` and `startup_session_gucs`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Endpoint & Unix TLS/keepalive/binding exclusions (`complete_option_validation`) | `unix_socket = Some("/tmp/.s.PGSQL.5432")` with `sslmode = "prefer"`, or `ca_pem = Some(...)`, or `keepalives_idle = Some(10)`, or `channel_binding = "require"`, or `cloud_sql_instance = Some("p:r:i")`; or both `cloud_sql_instance` and `alloydb_instance_uri` set | Every conflicting combination returns `Err(PostgresError::Config(_))` purely with zero I/O. |
| Cloud IP types, binding modes & opaque static Admin guard | `PUBLIC`/`PRIVATE`/`PSC`, `disable`/`prefer`/`require`, plus `cloud_auth_options = {"access_token": "opaque"}` with `iam_auth_options = None` on Cloud SQL IAM / AlloyDB | Valid spellings normalize cleanly; opaque Admin token without `iam_auth_options` fails purely (`Config`/`Auth`). |
| Reserved & non-UTF8 `session_gucs` vs `application_name` precedence | `session_gucs = {"client_encoding": "LATIN1"}` or `{"replication": "database"}` vs `application_name = "vtb-base", session_gucs = {"application_name": "vtb-override", "app.role": "reader one"}` | `"LATIN1"` and `"replication"` fail with `Err(PostgresError::Config(_))`; valid overlay encodes single `"client_encoding"="UTF8"`, `"application_name"="vtb-override"`, and `"app.role"="reader one"`. |
| Unix domain socket startup, EOF & deadline (`#[cfg(unix)]` `unix_*`) | Local `tokio::net::UnixListener` in temp dir, `unix_socket = Some(sock_path)`, `sslmode = "disable"` | First received message is `StartupMessage` (no `SSLRequest`), negotiates `UTF8` + `TransactionStatus::Idle` (`ReadyForQuery` `b'I'`), sends `Terminate` on `close()`, and enforces `connect_timeout` when listener stalls. |
| Wire `startup_session_gucs` inspection | Loopback listener inspecting raw `StartupMessage` key/value pairs | Receives exact `"application_name" => "vtb-override"` and `"app.tenant" => "acme corp"` with `client_encoding` emitted once as `"UTF8"`. |

## Verification

- Initial Red baseline:
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --lib complete_option_validation`
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_wire_tests unix_`
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_wire_tests startup_session_gucs`
- Final Green + Clippy gate:
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --lib complete_option_validation`
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_wire_tests unix_`
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_wire_tests startup_session_gucs`
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres`
  - `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings`

## Acceptance Criteria

- [x] All 25 constructor fields in `contracts.md` are validated purely and normalized into owned endpoint, TLS/binding, keepalive, IAM, and GUC policies without duplicate DSN parsing or I/O (R2, R3).
- [x] `session_gucs` overlays `application_name`, enforces `UTF8` `client_encoding`, rejects reserved keys and NUL bytes, and encodes directly into `StartupMessage` (R2, R3).
- [x] Unix domain socket connections establish without `SSLRequest` under `absolute_deadline` on Unix (`UnsupportedFeature` -> `NotSupportedError` on non-Unix), and commit `feat(postgres): project full options into Unix and startup sessions` passes all verification commands (R2, R3).

## Notes & Discoveries

- None.

[^db-conn]: `src/rs/vertebra-db/src/connection.rs::ConnectOptions` — Shared Unix domain socket `ConnectOptions::unix` dialer.
[^pg-config]: `src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig` — Authoritative `application_name` and `session_gucs` precedence rules.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Native constructor option inventory, Unix exclusions, and startup parameter contracts.
