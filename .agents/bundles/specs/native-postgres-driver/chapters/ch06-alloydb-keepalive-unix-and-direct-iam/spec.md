---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch06-alloydb-keepalive-unix-and-direct-iam
title: "Chapter 06: AlloyDB Split Metadata Sessions, TCP Keepalive, Unix Startup, and Direct-Host IAM"
description: Split AlloyDB TLS 1.3 and MetadataExchangeRequest stages to guard scoped login expiry before metadata emission, configure physical socket2 TCP keepalive parameters, project complete connection options and Unix domain socket startup, and refresh direct-host IAM tokens without altering transport policy.
status: stable
state: planned
planning_status: complete
depends_on_chapters:
  - ch05-cloud-tls-and-scoped-iam
replan_trigger: null
current_task: "native-postgres-driver:ch06:t01"
plan_revision: 3
plan_commit: dd432f18
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-07T05:00:00Z"
generated:
  by: flow-revise
  at: "2026-11-07T05:00:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-07T05:00:00Z"
    commit: dd432f18
sources:
  - id: db-conn
    resource: src/rs/vertebra-db/src/connection.rs::TransportStream
    title: Shared TCP, Unix, Cloud SQL, and AlloyDB TransportStream implementation
  - id: db-dialer
    resource: src/rs/vertebra-db/src/dialer.rs::exchange_alloydb_metadata_over_stream
    title: AlloyDB length-prefixed protobuf MetadataExchangeRequest codec
  - id: pg-config
    resource: src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig
    title: Existing PostgreSQL configuration, session_gucs precedence, and IAM options
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Complete 25-option native constructor, AlloyDB split TLS/metadata, Unix, and direct IAM contracts
tags:
  - postgres
  - alloydb
  - keepalive
  - unix
  - iam
  - rust
---

# Chapter 06: AlloyDB Split Metadata Sessions, TCP Keepalive, Unix Startup, and Direct-Host IAM

<!-- truth: start -->
- **Split AlloyDB TLS & Metadata Guard**: `TransportStream::connect_alloydb_tls` and `finish_alloydb_metadata` split AlloyDB `:5433` TLS 1.3 verification from `MetadataExchangeRequest` under one `absolute_deadline` so `dial_alloydb` verifies `ScopedLoginSnapshot::ensure_usable` immediately after TLS handshake (dropping expired/inside-skew streams with `0` metadata bytes written), completes `finish_alloydb_metadata`, and returns pre-startup `DialedCloudTransport` to `session.rs` before `StartupMessage` [^db-conn] [^db-dialer] [^pg-contracts].
- **Validated Physical TCP Keepalive**: `TransportStream::set_tcp_keepalive(TcpKeepaliveConfig)` configures `SO_KEEPALIVE`, idle, interval, and retry counts via `socket2` (`=0.6.5`) on underlying `PlainTcp` and `TlsTcp` sockets, applied once by `session.rs` before `StartupMessage` across direct TCP, direct TLS, Cloud SQL, and AlloyDB streams while failing closed with `TransportError::InvalidConfig` on unsupported platform knobs or disabled-with-tuning conflicts [^db-conn] [^pg-contracts].
- **Complete Option Normalization & Unix Startup**: `PostgresConfig` validates all 25 constructor parameters purely without I/O (`unix_socket` exact NUL-free file path requiring `sslmode="disable"`, excluding cloud URIs, PEMs, `server_name`, keepalive tuning, and `channel_binding="require"`), projects `application_name` + `session_gucs` directly into `StartupMessage`, and connects Unix sockets without `SSLRequest` (`UnsupportedFeature` -> `NotSupportedError` on non-Unix platforms) [^pg-config] [^pg-contracts].
- **Direct-Host IAM Password Refresh**: With `enable_iam_auth = true` and no cloud resource URI, session connect resolves fresh scoped login credentials (`sqlservice.login` by default or authoritative `iam_auth_options["scopes"]` such as `alloydb.login`), superseding `password` while keeping direct TCP `SSLRequest`/`sslmode` or Unix plaintext policy untouched and making zero Admin API calls [^pg-contracts].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`TransportStream::connect` currently couples AlloyDB TLS 1.3 negotiation and `exchange_alloydb_metadata_over_stream` inside one uninterruptible call, preventing `vertebra-postgres` from checking whether a scoped `alloydb.login` token crossed its expiry/skew boundary during a slow TLS handshake before transmitting token-bearing metadata [^db-conn] [^db-dialer]. In addition, physical PostgreSQL sessions still require OS-level TCP keepalive application (`keepalives`, `keepalives_idle`, `keepalives_interval`, `keepalives_count`), complete pure validation and `StartupMessage` projection across all 25 public constructor parameters (including Unix domain sockets and `session_gucs` overlay rules), and direct-host IAM password refresh (`enable_iam_auth=True` without Cloud SQL or AlloyDB proxy discovery) before the Python `SyncPostgresClient` and `AsyncPostgresClient` facades are exposed in Chapter 07 [^pg-config] [^pg-contracts].

### 1.2 Non-Goals

- Shared `py_async.rs` setup-abort guard and Python `SyncPostgresClient` / `AsyncPostgresClient` FFI wrappers (owned by Chapter 07).
- Adapter-owned URL/libpq DSN string parsing and `.s.PGSQL.<port>` directory expansion (owned by `ch25:t01` in `vertebra.db.adapters.postgres`).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md) and [contracts.md](../../contracts.md). Research: [cloud-material_20261105.md](../../research/cloud-material_20261105.md). Skills: [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md) and [runtime-ipc](../../../../../skills/runtime-ipc/SKILL.md). Knowledge: [database-drivers-and-dialers.md](../../../../knowledge/domains/data/database-drivers-and-dialers.md) and [ffi-and-gil-discipline.md](../../../../knowledge/patterns/rust/ffi-and-gil-discipline.md). `socket2 = { version = "=0.6.5", features = ["all"] }` is declared in `src/rs/vertebra-db/Cargo.toml` and locked in `src/rs/Cargo.lock` by `ch01:t02`, and Windows MSVC `cargo check` CI verification lands in `ch08:t04` where `.github/workflows/release.yaml` is owned.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R5, R9 (`ch06:t01`)**: Extract `TransportStream::connect_alloydb_tls(&ConnectOptions)` and `finish_alloydb_metadata(&mut TransportStream, &ConnectOptions, tokio::time::Instant)` in `src/rs/vertebra-db/src/connection.rs`, refactor `TransportStream::connect` to compose both under one deadline, and implement `dial_alloydb` in `src/rs/vertebra-postgres/src/cloud/mod.rs` + `session.rs` (`vertebra_db::dialer::AuthType::Password` and `AuthType::Iam`) checking `login_snapshot.ensure_usable(clock.now_ms())` immediately after TLS, completing `finish_alloydb_metadata`, and returning pre-startup `DialedCloudTransport` to `session.rs` before `StartupMessage`.
- **R2, R3 (`ch06:t02`)**: Define `TcpKeepaliveConfig` and `TransportStream::set_tcp_keepalive(&self, config: TcpKeepaliveConfig) -> Result<(), TransportError>` in `src/rs/vertebra-db/src/connection.rs` over `PlainTcp` and `TlsTcp.get_ref().0`, validate `keepalives*` fields in `src/rs/vertebra-postgres/src/config.rs`, and apply keepalives once at `session.rs`'s unified pre-`StartupMessage` seam across all direct TCP, direct TLS, Cloud SQL, and AlloyDB streams without touching `cloud/mod.rs`.
- **R2, R3 (`ch06:t03`)**: Complete pure normalization of all 25 constructor options in `src/rs/vertebra-postgres/src/config.rs`, encode `application_name` + overlaid `session_gucs` (with `client_encoding = "UTF8"` enforcement and reserved-key rejection) in `src/rs/vertebra-postgres/src/protocol.rs`, and establish Unix domain socket sessions without `SSLRequest` in `src/rs/vertebra-postgres/src/session.rs` (`PostgresError::UnsupportedFeature` -> `NotSupportedError` on `#[cfg(not(unix))]`).
- **R5 (`ch06:t04`)**: Implement direct-host `enable_iam_auth = true` (TCP or Unix without cloud resource URIs) across `config.rs`, `credentials.rs`, and `session.rs`, resolving `ScopedTokenProvider` lazily under `absolute_deadline` (`sqlservice.login` default or explicit `iam_auth_options["scopes"]` such as `alloydb.login`), superseding `config.password` while leaving direct transport/TLS policy untouched.

### 2.2 Concrete Type & Signature Contracts

- **Caller -> Seam -> Collaborator Map**:
  - `ConnectAttempt::run` (`session.rs`) -> dispatches normalized `PostgresConfig` (`config.rs`) across `cloud::dial_cloudsql`, `cloud::dial_alloydb` (`TransportStream::connect_alloydb_tls` -> `ScopedLoginSnapshot::ensure_usable` -> `finish_alloydb_metadata` -> `DialedCloudTransport`), direct TCP/TLS (`tls::connect`), or Unix (`ConnectOptions::unix`) -> applies `TransportStream::set_tcp_keepalive` once before `StartupMessage` across all TCP/TLS streams (direct + Cloud SQL + AlloyDB) -> resolves direct IAM via `ScopedTokenProvider` when `enable_iam_auth && !is_cloud` -> encodes `StartupMessage` (`protocol.rs`) -> reaches `TransactionStatus::Idle`.
- **Rust (`src/rs/vertebra-db/src/connection.rs`)**:
  - `pub async fn connect_alloydb_tls(options: &ConnectOptions) -> Result<TransportStream, TransportError>`
  - `pub async fn finish_alloydb_metadata(stream: &mut TransportStream, options: &ConnectOptions, absolute_deadline: tokio::time::Instant) -> Result<(), TransportError>`
  - `#[derive(Clone, Copy, Debug, PartialEq, Eq)] pub struct TcpKeepaliveConfig { pub enabled: bool, pub idle: Option<Duration>, pub interval: Option<Duration>, pub retries: Option<u32> }`
  - `pub fn set_tcp_keepalive(&self, config: TcpKeepaliveConfig) -> Result<(), TransportError>`
- **Rust (`src/rs/vertebra-postgres/src/{config.rs,protocol.rs,cloud/mod.rs,session.rs}`)**
  - `pub(crate) async fn dial_alloydb(config: &PostgresConfig, connector: &CloudConnector, absolute_deadline: tokio::time::Instant) -> Result<DialedCloudTransport, PostgresError>`
  - `PostgresConfig::validate_and_normalize(...) -> Result<NormalizedPostgresConfig, PostgresError>` (pure, zero I/O, canonical structural equality for later pool `validate_configuration`)
  - `protocol::encode_startup_message(user: &str, database: &str, application_name: &str, session_gucs: &BTreeMap<String, String>, buf: &mut BytesMut) -> Result<(), PostgresError>`

### 2.3 Edge-Case & Failure-Mode Matrix

| Condition | Behavior & Error Mapping |
| --- | --- |
| AlloyDB `alloydb.login` token valid at dial time crosses `now + skew >= expiry` while TLS 1.3 handshake is held | `connect_alloydb_tls` completes, `ensure_usable` fails with `PostgresError::Auth` (`GoogleCloudAuthError`), stream drops immediately, server sees EOF with `0` `MetadataExchangeRequest` bytes. |
| `keepalives = false` combined with `keepalives_idle = Some(30)` (or `<= 0` / `u32` overflow keepalive values) | Rejected in `PostgresConfig` / `set_tcp_keepalive` with `PostgresError::Config` (`ValueError`) / `TransportError::InvalidConfig`. |
| `unix_socket` combined with `cloud_sql_instance`, `alloydb_instance_uri`, `sslmode != "disable"`, `ca_pem`, `server_name`, keepalive tuning, or `channel_binding = "require"` | Rejected purely during `PostgresConfig` validation with `PostgresError::Config` (`ValueError`). |
| `session_gucs` contains NUL byte, reserved key (`user`, `database`, `options`, `replication`), or `client_encoding != "UTF8"` | Rejected purely with `PostgresError::Config` (`ValueError`); `session_gucs["application_name"]` overrides top-level `application_name`. |
| Unix socket connection requested on `#[cfg(not(unix))]` (Windows) | Fails cleanly with `PostgresError::UnsupportedFeature` -> `vertebra._vertebra.NotSupportedError`. |
| Direct-host `enable_iam_auth = true` with `password = Some("ignored")` and server returns `28P01` | Sends fresh IAM token (never `"ignored"`), makes `0` Cloud Admin requests, closes terminally on `PostgresError::Server(err)` (`err.sqlstate == "28P01"` -> `DatabaseError`) without same-stream token retry, redacts both token and `"ignored"`. |

### 2.4 Alternatives Comparison

| Option | Decision & Rationale |
| --- | --- |
| Explicit `TransportStream::set_tcp_keepalive` method vs mandatory new field on `ConnectOptions` | **Chosen: Explicit `set_tcp_keepalive` method**. Preserves all existing `ConnectOptions` struct literals across `vertebra-db` consumers while applying `socket2` options once before startup. |
| Inject token-lifetime policy callback into `vertebra-db` vs splitting `connect_alloydb_tls` and `finish_alloydb_metadata` | **Chosen: Split transport functions**. Keeps `vertebra-db` engine-agnostic and avoids a second credential/clock policy registry while `TransportStream::connect` composes both functions for legacy callers [^pg-contracts]. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in ch06]` (public docs and OKF data-domain chapters synchronize in `ch30:t01` and `ch30:t02`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R5, R9 | [t01](tasks/t01-alloydb-metadata-and-iam.md) | `behavior_tdd` | `src/rs/vertebra-db/src/connection.rs`, `src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs` |
| R2, R3 | [t02](tasks/t02-tcp-keepalive-controls.md) | `behavior_tdd` | `src/rs/vertebra-db/src/connection.rs`, `src/rs/vertebra-postgres/tests/postgres_wire_tests.rs` |
| R2, R3 | [t03](tasks/t03-unix-socket-and-options-projection.md) | `behavior_tdd` | `src/rs/vertebra-postgres/src/config.rs`, `src/rs/vertebra-postgres/tests/postgres_wire_tests.rs` |
| R5 | [t04](tasks/t04-direct-host-iam-refresh.md) | `regression_tdd` | `src/rs/vertebra-postgres/tests/postgres_auth_wire_tests.rs` |

## 5. Implementation Plan

- [ ] native-postgres-driver:ch06:t01 — Authenticate automatic AlloyDB metadata and IAM sessions across split TLS and metadata stages (tasks/t01-alloydb-metadata-and-iam.md)
- [ ] native-postgres-driver:ch06:t02 — Apply validated TCP keepalive controls to physical sessions (tasks/t02-tcp-keepalive-controls.md)
- [ ] native-postgres-driver:ch06:t03 — Project complete connection options and establish Unix domain socket startup (tasks/t03-unix-socket-and-options-projection.md)
- [ ] native-postgres-driver:ch06:t04 — Refresh direct-host IAM passwords without changing transport policy (tasks/t04-direct-host-iam-refresh.md)

**Chapter Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --lib && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --test it explicit_login_scopes` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `HEAD @ dd432f18`; chapter `state: planned`, `planning_status: complete`.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03`, `t04` (`4/4` pending).
- **Downstream Dependents**: Unblocks `ch07-async-bridge-and-client-sessions`.

[^db-conn]: `src/rs/vertebra-db/src/connection.rs::TransportStream` — Shared TCP, Unix, Cloud SQL, and AlloyDB transport stream.
[^db-dialer]: `src/rs/vertebra-db/src/dialer.rs::exchange_alloydb_metadata_over_stream` — AlloyDB `:5433` `MetadataExchangeRequest` / `MetadataExchangeResponse` wire helper.
[^pg-config]: `src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig` — Existing PostgreSQL configuration and `session_gucs` overlay rules.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Native PostgreSQL constructor, AlloyDB metadata guard, Unix, and direct IAM contracts.
