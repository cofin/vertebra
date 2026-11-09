---
type: Spec
parent_flow: native-oracle-driver
chapter_id: ch02-connect-auth-and-error-hierarchy
title: "Chapter 02: TNS/Wallet/Token Dialer, Worker Bridge, and SQLSTATE Error Mapping"
description: Implement NativeOracleConfig validation and ORA-xxxxx to SQLSTATE DbExceptionTypes error mapping, fix OracleConfig.connect_descriptor() precedence, build the TNS/Wallet mTLS and IAM token dialer, and wire dedicated per-connection worker threads over SyncCompletionQueue and AsyncCompletionQueue with out-of-band break_execution() cancellation.
status: stable
state: planned
planning_status: complete
depends_on_chapters:
  - ch01-crate-and-packaging
replan_trigger: null
current_task: "native-oracle-driver:ch02:t01"
plan_revision: 3
plan_commit: 769302bc
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-09T01:28:30Z"
generated:
  by: flow-plan
  at: "2026-11-09T01:15:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-09T01:28:30Z"
    commit: 769302bc
sources:
  - id: ora-config-py
    resource: src/py/vertebra/db/adapters/oracle/config.py::OracleConfig
    title: Python OracleConfig, OracleWallet, OracleConnectDescriptor, and OracleTopologyConfig
  - id: ora-rs-tns
    resource: src/rs/vertebra-db/src/oracle/tns.rs::parse_tnsnames_ora
    title: Shared Rust TNS Connect Descriptor parser and alias resolver
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr
    title: Shared AsyncCompletionQueue and independent event-loop drainer
  - id: sync-queue-rs
    resource: src/rs/vertebra-core/src/notify.rs::SyncCompletionQueue
    title: Cross-platform synchronous completion queue primitive
  - id: gcp-auth-rs
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpAuthSource
    title: Shared pure-Rust IAM/OAuth2 bearer token cache
  - id: py-errors-rs
    resource: src/rs/vertebra-py/src/errors.rs::DbExceptionTypes
    title: Canonical 13-class database exception hierarchy exported by vertebra.db.driver and vertebra._vertebra
  - id: ora-contracts
    resource: .agents/bundles/specs/native-oracle-driver/contracts.md
    title: Public connection constructor options, ORA-xxxxx SQLSTATE table, and worker-thread contracts
  - id: crate-research
    resource: .agents/bundles/specs/native-data-platform/research/rust-driver-crates_20261103.md
    title: Official pure-Rust oracledb =26.0.0-beta.5 synchronous connection and cancellation semantics
tags:
  - oracle
  - rust
  - dialer
  - wallet
  - errors
  - cancellation
---

# Chapter 02: TNS/Wallet/Token Dialer, Worker Bridge, and SQLSTATE Error Mapping

<!-- truth: start -->
- **Pure `NativeOracleConfig` Validation & Canonical `DbExceptionTypes` Mapping (`ch02:t01`)**: `src/rs/vertebra-oracle/src/config.rs` validates ports, timeouts, NUL bytes, `auth_mode`, and DRCP `purity` (`"SELF"` | `"NEW"`) purely in memory with secret redaction, while `src/rs/vertebra-oracle/src/errors.rs` (`OracleError::to_py_err`) imports the canonical `DbExceptionTypes` classes from `vertebra.db.driver` (`IntegrityError`, `SerializationFailure`, `OperationalError`, `ConnectionFailure`, `QueryCancelledError`, `NotFoundError`, `DataError`, `ProgrammingError`, `InterfaceError`, `DatabaseError`) and attaches `.sqlstate`, `.vendor_code`, and `.is_transient` [^py-errors-rs] [^ora-contracts].
- **Descriptor Precedence, Self-Contained `OracleWalletMaterial`, & IAM Token Dialer (`ch02:t02`)**: `OracleConfig.connect_descriptor()` prioritizes explicit `connect_descriptor`/wallet aliases, `topology` (`OracleTopologyConfig.to_tns_descriptor()`), explicit non-default `dsn`, and explicit `host`/`port`/`service_name` over the class-default `dsn="oracle://localhost:1521/FREEPDB1"`, while `src/rs/vertebra-oracle/src/dialer.rs` defines a self-contained `OracleWalletMaterial` struct for in-memory `.zip` / directory Autonomous Database Wallets (`ewallet.pem`, `cwallet.sso`, `tnsnames.ora`), reuses `vertebra_db::oracle::tns::{OracleConnectDescriptor, parse_tnsnames_ora, resolve_tns_alias}`, and resolves O5LOGON or IAM/OAuth2 bearer tokens via `vertebra_core::gcp_auth::GcpTokenCache` (`GcpAuthSource::from_ADC()`) [^ora-config-py] [^ora-rs-tns] [^gcp-auth-rs].
- **Dedicated Worker Threads & Out-of-Band `break_execution()` (`ch02:t03`)**: Each `oracledb::Connection` runs on a dedicated Rust worker thread in `src/rs/vertebra-oracle/src/client.rs` bridged via `SyncCompletionQueue` (`py.detach`) and `AsyncCompletionQueue` (`spawn_rust_db_task_with_attr` with `_vertebra_oracle_completion_drainer`), exposing `SyncOracleClient` and `AsyncOracleClient` connection/transaction lifecycle methods and out-of-band `connection.break_execution()` cancellation [^py-async-rs] [^sync-queue-rs] [^crate-research].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

In baseline `src/py/vertebra/db/adapters/oracle/config.py`, `OracleConfig.dsn` defaults to `"oracle://localhost:1521/FREEPDB1"` and `connect_descriptor()` checks `if self.dsn:` before `self.host`/`self.port`/`self.service_name`, causing `OracleConfig(host="db.internal", port=1530, service_name="ORCLPDB1")` to silently ignore the caller's endpoint overrides [^ora-config-py]. Furthermore, `oracledb = "=26.0.0-beta.5"` exposes a synchronous socket API (`oracledb::Connection`), requiring a dedicated worker-thread bridge over `SyncCompletionQueue` and `AsyncCompletionQueue` (`_vertebra_oracle_completion_drainer`) with out-of-band `connection.break_execution()` cancellation and deterministic `ORA-xxxxx` to SQLSTATE mapping into the canonical `DbExceptionTypes` classes exported by `vertebra.db.driver` [^py-async-rs] [^sync-queue-rs] [^py-errors-rs] [^ora-contracts] [^crate-research].

### 1.2 Non-Goals

- `OracleColumn`, `OracleQueryResult`, scalar/LOB/`OSON`/`VECTOR` codecs, positional/named bind lowering, `DML RETURNING INTO`, and crate-local `arrow 59` to `VTBARROW` SHM serialization (owned by [ch03-types-bind-and-arrow-shm](../ch03-types-bind-and-arrow-shm/spec.md), where `OracleColumn` and `OracleQueryResult` are defined once in `src/rs/vertebra-oracle/src/types.rs` in `ch03:t01`).
- Native `SyncOraclePool` and `AsyncOraclePool` pooling, DRCP connection tagging on checkout, and TTC session attribute piggyback (owned by `ch04-pools-drcp-and-session-piggyback`).
- Delayed-commit `SyncOraclePipeline` / `AsyncOraclePipeline`, VPD tenancy fields on `OracleConfig` (`context_namespace`, `context_package_proc`, `context_clear_proc`, owned by `ch05:t02`), and SQLSpec / APEX integration (owned by `ch05-pipelines-tenancy-and-apex`).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md). Public interface contracts: [contracts.md](../../contracts.md). Upstream chapter: [ch01-crate-and-packaging](../ch01-crate-and-packaging/spec.md). Skills: [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md), [runtime-ipc](../../../../../skills/runtime-ipc/SKILL.md), and [python-framework](../../../../../skills/python-framework/SKILL.md). Knowledge: [database-drivers-and-dialers.md](../../../../knowledge/domains/data/database-drivers-and-dialers.md) and [ffi-and-gil-discipline.md](../../../../knowledge/patterns/rust/ffi-and-gil-discipline.md).

| Existing Seam | Verified Consequence |
| :--- | :--- |
| `src/rs/vertebra-py/src/errors.rs::DbExceptionTypes` [^py-errors-rs] | `DbExceptionTypes` defines the 13 canonical classes exported by `vertebra.db.driver` and `vertebra._vertebra` (`DatabaseError`, `InterfaceError`, `DataError`, `NotFoundError`, `OperationalError`, `ConnectionFailure`, `PoolExhaustedError`, `QueryCancelledError`, `SerializationFailure`, `IntegrityError`, `InternalError`, `ProgrammingError`, `NotSupportedError`). `OracleError::to_py_err` imports from `vertebra.db.driver` and sets `.sqlstate`, `.vendor_code`, and `.is_transient`. |
| `src/py/vertebra/db/adapters/oracle/config.py::OracleConfig` [^ora-config-py] | Fixing `connect_descriptor()` priority (using `OracleTopologyConfig.to_tns_descriptor()`) and adding `OracleDrcpConfig` (`cclass`, `purity`, `connection_tag`) in `ch02:t02` resolves endpoint overrides while leaving VPD tenancy fields (`context_namespace`, `context_package_proc`, `context_clear_proc`) to `ch05:t02`. |
| `src/rs/vertebra-db/src/oracle/tns.rs::parse_tnsnames_ora` [^ora-rs-tns] | Importing `OracleConnectDescriptor`, `parse_tnsnames_ora`, and `resolve_tns_alias` from `vertebra_db::oracle::tns` while defining `OracleWalletMaterial` directly inside `src/rs/vertebra-oracle/src/dialer.rs` avoids depending on the dead `vertebra-db` `OracleWalletMaterial` stub removed by `driver-fail-loud` `ch07`. |
| `src/rs/vertebra-core/src/gcp_auth.rs::GcpAuthSource` [^gcp-auth-rs] | Using `vertebra_core::gcp_auth::{GcpAuthSource, GcpTokenCache}` (`GcpAuthSource::from_ADC()`) provides pure-Rust IAM/OAuth2 bearer token caching for `auth_mode="token"`. |
| `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` [^py-async-rs] | Using `ORACLE_COMPLETION_DRAINER_ATTR = "_vertebra_oracle_completion_drainer"` attaches an isolated `AsyncCompletionQueue` drainer to the running `asyncio` loop without colliding with `_vertebra`, `_bigquery`, `_spanner`, or `_postgres`. |

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R2.1 & R4.1 (`ch02:t01`)**: Implement pure in-memory `NativeOracleConfig` validation with secret redaction in `src/rs/vertebra-oracle/src/config.rs`, implement `OracleError` and `OracleError::to_py_err(self, py: Python<'_>) -> PyErr` mapping `ORA-xxxxx` codes to the canonical `DbExceptionTypes` classes imported from `vertebra.db.driver` in `src/rs/vertebra-oracle/src/errors.rs`, and wire `pub mod config; pub mod errors;` in `src/rs/vertebra-oracle/src/lib.rs` so `cargo test -p vertebra-oracle --locked` compiles and runs unit tests in `config.rs` and `errors.rs`.
- **R2.2 (`ch02:t02`)**: Fix `OracleConfig.connect_descriptor()` precedence (`OracleTopologyConfig.to_tns_descriptor()`) and add `OracleDrcpConfig` in `src/py/vertebra/db/adapters/oracle/config.py`, implement `src/rs/vertebra-oracle/src/dialer.rs` with self-contained `OracleWalletMaterial` (`.zip` / `ewallet.pem` / `cwallet.sso` in memory), `vertebra_db::oracle::tns::{OracleConnectDescriptor, parse_tnsnames_ora, resolve_tns_alias}`, and `vertebra_core::gcp_auth::{GcpAuthSource, GcpTokenCache}` (`GcpAuthSource::from_ADC()`), and wire `pub mod dialer;` in `src/rs/vertebra-oracle/src/lib.rs`.
- **R3.1 & R4.2 (`ch02:t03`)**: Implement `src/rs/vertebra-oracle/src/client.rs` with a dedicated Rust worker thread per `oracledb::Connection`, `SyncCompletionQueue` (`py.detach`) and `AsyncCompletionQueue` (`spawn_rust_db_task_with_attr` with `_vertebra_oracle_completion_drainer`), out-of-band `connection.break_execution()` cancellation (`ORA-01013` -> `QueryCancelledError`), and export `SyncOracleClient` and `AsyncOracleClient` connection/transaction lifecycle methods (`connect`, `close`, `ping`, `cancel`, `begin`, `commit`, `rollback`, `savepoint`, `rollback_to_savepoint`, `execute`, `closed`, `in_transaction`) in `src/rs/vertebra-oracle/src/lib.rs` and `src/py/vertebra/_oracle.pyi`.

### 2.2 Concrete Type & Signature Contracts

- Rust (`src/rs/vertebra-oracle/src/config.rs` & `errors.rs` in `ch02:t01`):
  - `#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)] pub enum OracleAuthMode { #[default] Password, Token }`
  - `#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)] pub enum DrcpPurity { #[default] SelfPurity, NewPurity }`
  - `#[derive(Clone, PartialEq, Eq)] pub struct NativeOracleConfig` + `NativeOracleConfig::validate(raw: RawOracleOptions) -> Result<Self, OracleError>` + redacted `fmt::Debug`
  - `#[derive(Debug, Clone, Copy, PartialEq, Eq)] pub enum OracleErrorKind { Integrity, SerializationFailure, Operational, ConnectionFailure, QueryCancelled, NotFound, Data, Programming, Interface }`
  - `impl OracleError { pub fn to_py_err(self, py: Python<'_>) -> PyErr }` importing `IntegrityError`, `SerializationFailure`, `OperationalError`, `ConnectionFailure`, `QueryCancelledError`, `NotFoundError`, `DataError`, `ProgrammingError`, `InterfaceError`, and `DatabaseError` from `vertebra.db.driver` and setting `.sqlstate`, `.vendor_code`, and `.is_transient`.
- Python & Rust (`src/py/vertebra/db/adapters/oracle/config.py` & `src/rs/vertebra-oracle/src/dialer.rs` in `ch02:t02`):
  - `class OracleDrcpConfig(FrozenBaseStruct, frozen=True, gc=False)` (`cclass: str | None = None`, `purity: str = "SELF"`, `connection_tag: str | None = None`)
  - `OracleConfig` adds `sid: str | None = None`, `connect_descriptor_str: str | None = None`, `drcp: OracleDrcpConfig = msgspec.field(default_factory=OracleDrcpConfig)`, `auth_mode: str = "password"`, `access_token: str | None = None`, `token_auth_options: dict[str, str] | None = None`
  - `OracleConfig.connect_descriptor(self) -> OracleConnectDescriptor` using `self.topology.to_tns_descriptor()` and enforcing strict 4-tier priority
  - `#[derive(Clone, PartialEq, Eq)] pub struct OracleWalletMaterial` defined directly in `src/rs/vertebra-oracle/src/dialer.rs` (`from_zip_bytes`, `from_location`, `from_raw_buffers`)
  - `pub fn resolve_endpoint(cfg: &NativeOracleConfig) -> Result<ResolvedOracleEndpoint, OracleError>` using `vertebra_db::oracle::tns::{OracleConnectDescriptor, parse_tnsnames_ora, resolve_tns_alias}` and `vertebra_core::gcp_auth::{GcpAuthSource, GcpTokenCache}` (`GcpAuthSource::from_ADC()`)
- Rust & Stub (`src/rs/vertebra-oracle/src/client.rs`, `src/lib.rs`, & `src/py/vertebra/_oracle.pyi` in `ch02:t03`):
  - `#[pyclass(name = "SyncOracleClient", module = "vertebra._oracle", frozen, immutable_type)] pub struct SyncOracleClient`
  - `#[pyclass(name = "AsyncOracleClient", module = "vertebra._oracle", frozen, immutable_type)] pub struct AsyncOracleClient`
  - Connection and transaction lifecycle methods: `connect`, `close`, `ping`, `cancel`, `begin`, `commit`, `rollback`, `savepoint`, `rollback_to_savepoint`, `execute`, and properties `closed: bool`, `in_transaction: bool` (`OracleColumn` and `OracleQueryResult` are added in `ch03:t01`–`t02`).

### 2.3 Caller -> Seam -> Collaborator Integration Map

```text
OracleConfig.connect_descriptor() (src/py/vertebra/db/adapters/oracle/config.py)
  -> SyncOracleClient / AsyncOracleClient.__new__ (src/rs/vertebra-oracle/src/client.rs)
  -> NativeOracleConfig::validate (src/rs/vertebra-oracle/src/config.rs)
  -> dialer::resolve_endpoint (OracleWalletMaterial + vertebra_db::oracle::tns + vertebra_core::gcp_auth::GcpTokenCache)
  -> Worker Thread (oracledb::Connection + break_execution handle)
  -> SyncCompletionQueue (py.detach) / AsyncCompletionQueue (_vertebra_oracle_completion_drainer)
  -> OracleError::to_py_err -> vertebra.db.driver DbExceptionTypes (src/rs/vertebra-oracle/src/errors.rs)
```

### 2.4 Edge-Case & Failure-Mode Matrix

| Edge Case / Failure Mode | Required Behavior |
| :--- | :--- |
| `OracleConfig(host="db.internal", port=1530, service_name="ORCLPDB1")` with default `dsn` | `connect_descriptor()` detects `dsn == "oracle://localhost:1521/FREEPDB1"` alongside non-default `host`/`port`/`service_name` and returns `OracleConnectDescriptor(host="db.internal", port=1530, service_name="ORCLPDB1")`. |
| Invalid `purity` (`"SHARED"`), invalid `auth_mode`, port outside `1..=65535`, non-finite `connect_timeout`, or NUL bytes | `NativeOracleConfig::validate` rejects synchronously in `__new__` with `InterfaceError` / `ImproperConfigurationError` without performing DNS, filesystem, or socket I/O. |
| Canonical `ORA-xxxxx` to `DbExceptionTypes` mapping | `ORA-00001` (`23505`), `ORA-02291`/`02292` (`23503`), `ORA-01400`/`01407` (`23502`), `ORA-02290` (`23514`) -> `IntegrityError` (`is_transient=False`); `ORA-08177` (`40001`), `ORA-00060` (`40P01`) -> `SerializationFailure` (`is_transient=True`); `ORA-00054`/`30006` (`55P03`) -> `OperationalError` (`is_transient=True`); `ORA-01013` (`57014`) -> `QueryCancelledError`; `ORA-01017`/`28000` (`28P01`, `is_transient=False`), `ORA-12154` (`08006`, `is_transient=False`), `ORA-12170`/`12514`/`12541`/`12545` (`08006`, `is_transient=True`) -> `ConnectionFailure`; `ORA-01403` (`02000`) -> `NotFoundError` (`is_transient=False`); `ORA-01438`/`01476`/`01722`/`01830`..`01899`/`12899` (`22000`) -> `DataError` (`is_transient=False`); `ORA-00900`..`00999`/`06550` (`42000`) -> `ProgrammingError` (`is_transient=False`); `ORA-03113`/`03114`/`03135`/`04061`/`04068`/`12537` (`08006`) -> `OperationalError` (`is_transient=True`). |
| Out-of-band `cancel()` or `asyncio.Future.cancel()` during active query | Invokes `connection.break_execution()` without waiting on the worker thread's command channel; `ORA-01013` maps to `QueryCancelledError` (`sqlstate="57014"`, `vendor_code=1013`) and the worker thread stays alive for subsequent queries. |

### 2.5 Mandatory Alternatives Comparison

| Seam | Chosen Design | Rejected Alternative | Rationale |
| :--- | :--- | :--- | :--- |
| Blocking `oracledb::Connection` concurrency model | Dedicated Rust worker thread per connection + out-of-band `break_execution()` handle | Wrapping `oracledb::Connection` in `Arc<Mutex<Connection>>` on `tokio::task::spawn_blocking` | A mutex held across blocking socket reads prevents `cancel()` from invoking `break_execution()` until the read completes; a dedicated worker thread keeps the socket owner isolated while exposing a lock-free break handle. |
| Autonomous Wallet material ownership | Self-contained `OracleWalletMaterial` inside `src/rs/vertebra-oracle/src/dialer.rs` using `zip = "2"` and `rustls-pemfile` in memory | Importing `OracleWalletMaterial` from `vertebra_db::oracle::tns` or extracting to `/tmp` | `driver-fail-loud` `ch07` deletes `OracleWalletMaterial` from `vertebra-db`, and in-memory extraction avoids temporary private-key files on disk. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/6 files documented]` (`src/py/vertebra/_oracle.pyi` is updated in `ch02:t03` with `SyncOracleClient` and `AsyncOracleClient`; all `6` user documentation and OKF v0.2 knowledge files in the root roadmap are owned by `ch07:t03`–`t04`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test / Gate |
| :--- | :--- | :--- | :--- |
| R2, R4 (`NativeOracleConfig` validation, `ORA-xxxxx` -> `DbExceptionTypes` mapping, `lib.rs` module wiring) | [t01](tasks/t01-descriptor-precedence-and-error-hierarchy.md) | `behavior_tdd` | `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle --locked` |
| R2 (`OracleConfig.connect_descriptor()` precedence, `OracleWalletMaterial` mTLS, `GcpTokenCache` dialer) | [t02](tasks/t02-tns-wallet-and-token-dialer.md) | `behavior_tdd` | `src/py/tests/unit/db/test_oracle_adapter.py`, `cargo test -p vertebra-oracle --locked` |
| R3, R4 (worker-thread completion bridge, `SyncOracleClient`/`AsyncOracleClient`, `break_execution()` cancellation) | [t03](tasks/t03-worker-bridge-and-break-cancellation.md) | `behavior_tdd` | `src/py/tests/unit/db/test_oracle_adapter.py`, `mypy.stubtest vertebra._oracle` |

## 5. Implementation Plan

- [ ] native-oracle-driver:ch02:t01 — Implement NativeOracleConfig validation and ORA-xxxxx to DbExceptionTypes SQLSTATE error mapping (tasks/t01-descriptor-precedence-and-error-hierarchy.md)
- [ ] native-oracle-driver:ch02:t02 — Fix OracleConfig.connect_descriptor() precedence and implement TNS, Autonomous Wallet mTLS, and GcpTokenCache dialer (tasks/t02-tns-wallet-and-token-dialer.md)
- [ ] native-oracle-driver:ch02:t03 — Implement per-connection worker bridge, SyncOracleClient/AsyncOracleClient lifecycle, and out-of-band break_execution cancellation (tasks/t03-worker-bridge-and-break-cancellation.md)

**Chapter Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle --locked && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-oracle --all-targets --locked -- -D warnings && make dev && uv run --no-sync python -m mypy.stubtest vertebra._oracle && uv run --no-sync pytest src/py/tests/unit/db/test_oracle_adapter.py -v` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Plan Baseline**: `HEAD @ 769302bc`; chapter `state: planned`, `planning_status: complete`, `plan_revision: 3`, `plan_commit: 769302bc`.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03` (`3/3` pending).
- **Documentation & OKF Status**: `[0/6 files documented]`; `_oracle.pyi` updated in `t03`, and full docs/OKF scheduled in `ch07:t03`–`t04`.
- **Downstream Dependents**: Unblocks [ch03-types-bind-and-arrow-shm](../ch03-types-bind-and-arrow-shm/spec.md) (`ch03:t01` depends on `ch02:t03`).

[^ora-config-py]: `src/py/vertebra/db/adapters/oracle/config.py::OracleConfig` — Python `OracleConfig`, `OracleWallet`, `OracleConnectDescriptor`, and `OracleTopologyConfig`.
[^ora-rs-tns]: `src/rs/vertebra-db/src/oracle/tns.rs::parse_tnsnames_ora` — Shared Rust TNS Connect Descriptor parser and alias resolver.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Shared `AsyncCompletionQueue` and independent event-loop drainer.
[^sync-queue-rs]: `src/rs/vertebra-core/src/notify.rs::SyncCompletionQueue` — Cross-platform synchronous completion queue primitive.
[^gcp-auth-rs]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpAuthSource` — Shared pure-Rust IAM/OAuth2 bearer token cache.
[^py-errors-rs]: `src/rs/vertebra-py/src/errors.rs::DbExceptionTypes` — Canonical 13-class database exception hierarchy exported by `vertebra.db.driver` and `vertebra._vertebra`.
[^ora-contracts]: `.agents/bundles/specs/native-oracle-driver/contracts.md` — Public connection constructor options, `ORA-xxxxx` SQLSTATE table, and worker-thread contracts.
[^crate-research]: `.agents/bundles/specs/native-data-platform/research/rust-driver-crates_20261103.md` — Official pure-Rust `oracledb =26.0.0-beta.5` synchronous connection and cancellation semantics.
