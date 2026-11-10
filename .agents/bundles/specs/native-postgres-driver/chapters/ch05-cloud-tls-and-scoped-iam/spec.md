---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch05-cloud-tls-and-scoped-iam
title: "Chapter 05: Cloud TLS 1.3 Peer Identities, Scoped IAM Caches, and Cloud SQL Sessions"
description: Enforce cloud TLS 1.3 and service-specific SAN/CN peer verification, validate static IAM login provenance and actual lifetime, propagate requested OAuth scopes across GCP token caches, and establish automatic Cloud SQL mTLS + IAM PostgreSQL sessions.
status: stable
state: completed
planning_status: complete
depends_on_chapters:
  - ch04-cloud-material-and-admin-discovery
replan_trigger: null
current_task: null
plan_revision: 5
plan_commit: 73c8a6ff
state_revision: 0
last_verified_checkpoint: eb48155b
last_reviewed_commit: eb48155b
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-08T02:05:22Z"
generated:
  by: flow-revise
  at: "2026-11-07T05:00:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-07T05:00:00Z"
    commit: 73c8a6ff
sources:
  - id: db-conn
    resource: src/rs/vertebra-db/src/connection.rs::TransportStream
    title: Shared TCP, TLS, Cloud SQL, and AlloyDB transport and peer certificate verifier
  - id: core-gcp-auth
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache
    title: Shared pure-Rust GCP Application Default Credentials token cache
  - id: cloud-research
    resource: .agents/bundles/specs/native-postgres-driver/research/cloud-material_20261105.md
    title: Primary-source Cloud SQL, AlloyDB, and scoped GCP OAuth research
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Native PostgreSQL connection constructor, static login, and error contracts
tags:
  - postgres
  - cloudsql
  - alloydb
  - tls
  - iam
  - rust
---

# Chapter 05: Cloud TLS 1.3 Peer Identities, Scoped IAM Caches, and Cloud SQL Sessions

<!-- truth: start -->
- **Mandatory Cloud TLS 1.3 & Peer Identity**: Shared cloud endpoints (`EndpointTarget::CloudSql` and `EndpointTarget::AlloyDb`) negotiate TLS 1.3 exclusively (`&rustls::version::TLS13`), retaining the verified peer leaf DER for `tls-server-end-point` SCRAM binding while verifying AlloyDB IP/PSC DNS SANs strictly (no CN or CA-only fallback) and Cloud SQL DNS SAN (with normalized trailing-dot retry) before exact `project:instance` CN fallback [^db-conn] [^cloud-research].
- **Strict Static Login Provenance & Actual Expiry**: Designated static `iam_auth_options` require canonical unsigned decimal `expires_at_unix_ms` (`parsed.to_string() == raw`) and explicit `scopes` covering every required login scope, constructing `GcpAuthSource::Static { expires_at_unix_ms: Some(supplied_expiry), .. }` without fabricating TTLs or `u64::MAX` lifetimes [^core-gcp-auth] [^pg-contracts].
- **Scope-Keyed Admin vs Login Caches**: `ScopedTokenProvider` isolates Admin (`cloud-platform`) and login (`sqlservice.login` or `alloydb.login`) caches inside `vertebra-postgres`, extending `vertebra_core::gcp_auth::GcpTokenCache` with explicit requested scopes across AuthorizedUser, GcloudCli (`auth application-default print-access-token --scopes=...`), MetadataServer, ServiceAccount, and ImpersonatedServiceAccount without unscoped downgrades [^core-gcp-auth] [^cloud-research].
- **Automatic Cloud SQL `:3307` Sessions**: `dial_cloudsql` acquires `PreparedCloudMaterial`, connects immediately over verified `:3307` mTLS without emitting PostgreSQL `SSLRequest` or falling back to `:5432`, and returns pre-startup `DialedCloudTransport` to `session.rs`, which drives `StartupMessage` and `ScopedLoginSnapshot::ensure_usable` before password/SASL transmission while mapping provider failures to exact `vertebra._vertebra.GoogleCloudAuthError` [^db-conn] [^pg-contracts].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`src/rs/vertebra-db/src/connection.rs` currently verifies AlloyDB server certificates with `expected_cn: None` (CA-only without SAN matching), permits TLS 1.2 on cloud proxy dials, and checks Cloud SQL peer certificates by CN alone without DNS SAN priority [^db-conn] [^cloud-research]. Meanwhile `src/rs/vertebra-core/src/gcp_auth.rs` lacks explicit scope requests on `AuthorizedUser` OAuth refreshes and `GcloudCli` invocations and treats `Static` tokens without expiry as `u64::MAX` [^core-gcp-auth]. Chapter 05 repairs cloud TLS 1.3 and peer identity verification, establishes the strict `StaticLoginCredential` and `ScopedTokenProvider` contracts (plus `PostgresError` -> `vertebra._vertebra` exception resolution), binds cloud material lifetime to login token expiry, and wires automatic Cloud SQL `:3307` mTLS + IAM PostgreSQL startup sessions [^pg-contracts].

### 1.2 Non-Goals

- Split AlloyDB TLS vs `MetadataExchangeRequest` stage boundaries and automatic AlloyDB `:5433` session startup (owned by `ch06:t01`).
- Direct-host IAM password refresh without cloud proxy endpoints (owned by `ch06:t04`).
- Python `SyncPostgresClient` and `AsyncPostgresClient` PyO3 class exposure (owned by `ch07:t02` and `ch07:t03`).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md) and [contracts.md](../../contracts.md). Research: [cloud-material_20261105.md](../../research/cloud-material_20261105.md). Skills: [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md) and [runtime-ipc](../../../../../skills/runtime-ipc/SKILL.md). Knowledge: [database-drivers-and-dialers.md](../../../../knowledge/domains/data/database-drivers-and-dialers.md) and [ffi-and-gil-discipline.md](../../../../knowledge/patterns/rust/ffi-and-gil-discipline.md). Note that `src/rs/vertebra-storage/src/gcp_auth.rs` already delegates to `vertebra_core::gcp_auth::GcpTokenCache` and constructs `GcpAuthSource::AuthorizedUser` struct literals directly; `ch05:t03` keeps `GcpAuthSource` variant fields backwards-compatible so `vertebra-storage` requires zero edits.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R4, R9 (`ch05:t01`)**: Add `CloudPeerIdentity` (`CloudSql { server_name: Option<String>, expected_cn: String }` and `AlloyDb { expected_san: String }`) to `EndpointTarget::{CloudSql, AlloyDb}` in `src/rs/vertebra-db/src/connection.rs` (`socket2 = { version = "=0.6.5", features = ["all"] }` is already registered on `vertebra-db` in `ch01:t02`); restrict `build_cloud_mtls_config` to `&[&rustls::version::TLS13]`; retain verified peer leaf DER on `TransportStream` for `tls-server-end-point` SCRAM binding; wire `src/rs/vertebra-postgres/src/cloud/material.rs` (consuming `CloudSqlDiscovery` and `AlloyDbDiscovery` from `ch04:t03`–`t04` as the sole constructor of `EndpointTarget::{CloudSql, AlloyDb}` and `PreparedCloudMaterial`) into `CloudPeerIdentity`.
- **R5, R9 (`ch05:t02`)**: Add `PostgresError::Auth(String)` and `postgres_error_to_py(py, err)` (`PyOnceLock` cache of `vertebra._vertebra` `DatabaseError`, `InterfaceError`, `DataError`, `IntegrityError`, `ProgrammingError`, `NotSupportedError`, `ConnectionFailure`, `QueryCancelledError`, `SerializationFailure`, and `GoogleCloudAuthError`) in `src/rs/vertebra-postgres/src/errors.rs`; implement `StaticLoginCredential::from_options`, `ensure_usable`, and `into_token_cache` in `src/rs/vertebra-postgres/src/credentials.rs`.
- **R5, R9 (`ch05:t03`)**: Add backwards-compatible requested-scope support (`GcpTokenCache::from_scoped_options` and `GcpTokenCache::with_requested_scopes`) in `src/rs/vertebra-core/src/gcp_auth.rs` covering `AuthorizedUser` (`scope` form parameter + response `scope` grant check, zero CLI fallback), `GcloudCli` (`auth application-default print-access-token --scopes=...`, zero `auth print-access-token` fallback), `MetadataServer` (`?scopes=...`), `ServiceAccount`, and `ImpersonatedServiceAccount`; implement `ScopedTokenProvider` and `ScopedLoginSnapshot` in `credentials.rs` and wire `CloudMaterialCache` in `cloud/material.rs` to bound `effective_expiry_ms = min(signed_cert_not_after_ms, login_snapshot.snapshot.expires_at_unix_ms)`.
- **R5, R9 (`ch05:t04`)**: Define `DialedCloudTransport` and implement `dial_cloudsql(config, connector, absolute_deadline)` in `src/rs/vertebra-postgres/src/cloud/mod.rs` (`:3307` immediate mTLS returning pre-startup `DialedCloudTransport`, zero `SSLRequest`, zero fallback to `:5432`), enforce pre-credential `ScopedLoginSnapshot::ensure_usable` in `auth.rs` and unified `StartupMessage` + `TransactionStatus::Idle` session startup in `session.rs` under `PendingSession` and `absolute_deadline`.

### 2.2 Concrete Type & Signature Contracts

- **Caller -> Seam -> Collaborator Map**:
  - `SessionHandle::begin_connect` -> `ConnectAttempt::run` -> `cloud::dial_cloudsql` -> `CloudConnector::prepare` (`ScopedTokenProvider` + `CloudMaterialCache`) -> `TransportStream::connect` (`CloudPeerIdentity` + TLS 1.3) -> returns `DialedCloudTransport` -> `session.rs` (`StartupMessage` + `auth::Authentication` with `ScopedLoginSnapshot::ensure_usable` -> `TransactionStatus::Idle`).
- **Rust (`src/rs/vertebra-db/src/connection.rs`)**:
  - `pub enum CloudPeerIdentity { CloudSql { server_name: Option<String>, expected_cn: String }, AlloyDb { expected_san: String } }`
  - `TransportStream::peer_leaf_der(&self) -> Option<&[u8]>`
- **Rust (`src/rs/vertebra-core/src/gcp_auth.rs`)**:
  - `pub fn from_scoped_options(options: Option<&HashMap<String, String>>, required_scopes: &[String]) -> Result<GcpTokenCache, String>`
  - `pub fn with_requested_scopes(self, requested_scopes: Vec<String>) -> Self`
- **Rust (`src/rs/vertebra-postgres/src/{errors.rs,credentials.rs,cloud/mod.rs}`)**
  - `PostgresError::Auth(String)` and `pub(crate) fn postgres_error_to_py(py: Python<'_>, err: PostgresError) -> PyErr`
  - `pub(crate) fn StaticLoginCredential::from_options(options: &HashMap<String, String>, required_scopes: &[String], now_ms: u64, refresh_skew_ms: u64) -> Result<Self, PostgresError>`
  - `pub(crate) struct ScopedTokenProvider` (`resolve_admin_token`, `resolve_login_snapshot` using `tokio::task::spawn_blocking` under `tokio::time::timeout_at(absolute_deadline, ...)`)
  - `pub(crate) fn ScopedLoginSnapshot::ensure_usable(&self, now_ms: u64) -> Result<(), PostgresError>`
  - `pub(crate) struct DialedCloudTransport { pub stream: TransportStream, pub tls_server_end_point: Option<Vec<u8>>, pub login_snapshot: Option<ScopedLoginSnapshot> }`
  - `pub(crate) async fn dial_cloudsql(config: &PostgresConfig, connector: &CloudConnector, absolute_deadline: tokio::time::Instant) -> Result<DialedCloudTransport, PostgresError>`

### 2.3 Edge-Case & Failure-Mode Matrix

| Condition | Behavior & Error Mapping |
| --- | --- |
| AlloyDB leaf signed by valid CA with matching CN but wrong/missing IP or DNS SAN | Handshake fails with `TransportError::Tls`; no CN or CA-only fallback. |
| Cloud SQL leaf has metadata DNS SAN with trailing dot (`db.example.internal.`) vs normalized `server_name` | Matches DNS SAN via normalized trailing-dot retry without falling through to CN mismatch. |
| Cloud SQL leaf lacks matching DNS SAN and CN != `project:instance` | Handshake fails with `TransportError::Tls`. |
| Cloud proxy negotiates TLS 1.2 | Rejected during `rustls` handshake (`&[&rustls::version::TLS13]`). |
| Static `iam_auth_options` has `01700000000000` or `+1700000000000` or `now + skew == expires_at_unix_ms` | Rejected before network I/O with `PostgresError::Auth` -> `GoogleCloudAuthError` (`sqlstate = None`). |
| Admin `cloud_auth_options` has opaque static `access_token` and `iam_auth_options` is `None` | Rejected with `PostgresError::Auth` rather than assuming opaque Admin token carries `sqlservice.login`. |
| Login snapshot valid at dial time crosses `now + skew >= expires_at_unix_ms` during held TLS handshake | `ScopedLoginSnapshot::ensure_usable` fails before password/SASL frame emission; `0` credential bytes sent. |
| Cloud SQL server returns PostgreSQL `ErrorResponse` `28P01` / `28000` | Terminal `PostgresError::Server(err)` (`err.sqlstate == "28P01"`) -> `DatabaseError` without same-stream token retry. |

### 2.4 Alternatives Comparison

| Option | Decision & Rationale |
| --- | --- |
| Store `requested_scopes` on `GcpTokenCache` vs adding fields directly onto `GcpAuthSource::AuthorizedUser` | **Chosen: `GcpTokenCache` field (`from_scoped_options` / `with_requested_scopes`)**. Preserves full struct-literal compatibility with `vertebra-storage/src/gcp_auth.rs` while enforcing strict scope requests and grant checks for PostgreSQL caches. |
| Extract `PyRef<SyncGcpTokenCache>` across `cdylib` boundary vs primitive options into `ScopedTokenProvider` | **Chosen: Owned primitive options (`HashMap<String, String>`)**. Separate `cdylib` extensions do not share Rust type layouts or static allocators [^pg-contracts]. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in ch05]` (public user docs and OKF data-domain chapters synchronize in `ch30:t01` and `ch30:t02` once the full native driver surface is complete).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R4, R9 | [t01](tasks/t01-cloud-tls13-identities.md) | `behavior_tdd` | `src/rs/vertebra-db/src/connection.rs`, `src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs` |
| R5, R9 | [t02](tasks/t02-static-login-token-contract.md) | `regression_tdd` | `src/rs/vertebra-postgres/src/credentials.rs` |
| R5, R9 | [t03](tasks/t03-cloud-scoped-auth-caches.md) | `behavior_tdd` | `src/rs/vertebra-core/tests/it/gcp_auth.rs`, `src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs` |
| R5, R9 | [t04](tasks/t04-cloudsql-mtls-iam-sessions.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs` |

## 5. Implementation Plan

- [x] [eb48155b] native-postgres-driver:ch05:t01 — Enforce cloud TLS 1.3 and service-specific peer identities (tasks/t01-cloud-tls13-identities.md)
- [x] [eb48155b] native-postgres-driver:ch05:t02 — Validate designated static login scope, actual lifetime, and core auth error mapping (tasks/t02-static-login-token-contract.md)
- [x] [eb48155b] native-postgres-driver:ch05:t03 — Create scope-correct cloud Admin and login credential caches (tasks/t03-cloud-scoped-auth-caches.md)
- [x] [eb48155b] native-postgres-driver:ch05:t04 — Authenticate automatic Cloud SQL mTLS endpoints with IAM tokens (tasks/t04-cloudsql-mtls-iam-sessions.md)

**Chapter Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --test it gcp_auth && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage --lib gcp_auth && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --lib cloud_peer_identities && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `plan_commit: 73c8a6ff`; chapter `state: completed` at `eb48155b`, `planning_status: complete`.
- **Remaining Tasks in Chapter**: None (`4/4` completed at `eb48155b`).
- **Downstream Dependents**: Unblocks `ch06-alloydb-keepalive-unix-and-direct-iam`.

[^db-conn]: `src/rs/vertebra-db/src/connection.rs::TransportStream` — Shared TCP/TLS/cloud mTLS transport and certificate verification.
[^core-gcp-auth]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache` — Pure-Rust GCP ADC token cache and OAuth refresh sources.
[^cloud-research]: `.agents/bundles/specs/native-postgres-driver/research/cloud-material_20261105.md` — Primary-source Cloud SQL, AlloyDB, and scoped OAuth verification rules.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Native PostgreSQL constructor, static login provenance, and error contracts.
