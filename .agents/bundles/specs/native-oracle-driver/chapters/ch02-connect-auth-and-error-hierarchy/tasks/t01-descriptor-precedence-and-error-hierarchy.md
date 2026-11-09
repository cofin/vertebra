---
type: Task
id: "native-oracle-driver:ch02:t01"
chapter_id: ch02-connect-auth-and-error-hierarchy
title: Implement NativeOracleConfig validation and ORA-xxxxx to DbExceptionTypes SQLSTATE error mapping
description: Implement pure NativeOracleConfig validation with secret redaction in src/rs/vertebra-oracle/src/config.rs, map ORA-xxxxx codes deterministically to canonical DbExceptionTypes classes imported from vertebra.db.driver in src/rs/vertebra-oracle/src/errors.rs, and register pub mod config; pub mod errors; in src/rs/vertebra-oracle/src/lib.rs.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-oracle-driver:ch01:t03"
read_set:
  - src/rs/vertebra-oracle/src/lib.rs
  - src/rs/vertebra-py/src/errors.rs
  - src/rs/vertebra-spanner/src/errors.rs
  - .agents/bundles/specs/native-oracle-driver/contracts.md
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
files:
  - src/rs/vertebra-oracle/src/config.rs
  - src/rs/vertebra-oracle/src/errors.rs
  - src/rs/vertebra-oracle/src/lib.rs
tests: []
estimated_loc: 230
plan_revision: 3
plan_commit: 769302bc
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-09T00:55:00Z"
updated_at: "2026-11-09T01:28:30Z"
commit: null
sources:
  - id: py-errors-rs
    resource: src/rs/vertebra-py/src/errors.rs::DbExceptionTypes
    title: Canonical 13-class database exception hierarchy exported by vertebra.db.driver and vertebra._vertebra
  - id: spanner-err-rs
    resource: src/rs/vertebra-spanner/src/errors.rs::spanner_error_to_py
    title: Reference PyO3 exception resolution across optional extension wheels
  - id: ora-contracts
    resource: .agents/bundles/specs/native-oracle-driver/contracts.md
    title: Public connection constructor options and ORA-xxxxx SQLSTATE table
tags:
  - oracle
  - config
  - errors
  - sqlstate
---

# Task t01: Implement `NativeOracleConfig` Validation and `ORA-xxxxx` to `DbExceptionTypes` SQLSTATE Error Mapping

## Objective & Scope Boundary

Implement pure in-memory `NativeOracleConfig` validation with secret redaction in `src/rs/vertebra-oracle/src/config.rs`, implement `OracleError` and `OracleError::to_py_err(self, py: Python<'_>) -> PyErr` in `src/rs/vertebra-oracle/src/errors.rs` mapping `ORA-xxxxx` codes to the canonical `DbExceptionTypes` classes imported from `vertebra.db.driver`, and register `pub mod config; pub mod errors;` in `src/rs/vertebra-oracle/src/lib.rs` so `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle --locked` compiles and executes the Rust unit tests in `config.rs` and `errors.rs` [^py-errors-rs] [^spanner-err-rs] [^ora-contracts]. Python `OracleConfig.connect_descriptor()` precedence and TNS/Wallet/Token dialing belong to `ch02:t02`, and worker-thread execution belongs to `ch02:t03` ([../spec.md](../spec.md)).

## Bounded Read Manifest

- `src/rs/vertebra-oracle/src/lib.rs` — Crate root module declarations from `ch01:t01`.
- `src/rs/vertebra-py/src/errors.rs` — `DbExceptionTypes` (`DbExceptionTypes::get(py)`) defining the 13 canonical classes (`DatabaseError`, `InterfaceError`, `DataError`, `NotFoundError`, `OperationalError`, `ConnectionFailure`, `PoolExhaustedError`, `QueryCancelledError`, `SerializationFailure`, `IntegrityError`, `InternalError`, `ProgrammingError`, `NotSupportedError`) and `.sqlstate` / `.vendor_code` / `.is_transient` attributes [^py-errors-rs].
- `src/rs/vertebra-spanner/src/errors.rs` — Cross-extension exception resolution pattern [^spanner-err-rs].
- `.agents/bundles/specs/native-oracle-driver/contracts.md` — Constructor kwargs and `ORA-xxxxx` SQLSTATE table ([../../../contracts.md](../../../contracts.md)) [^ora-contracts].
- `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Driver configuration and error classification conventions.

## Symbol & Signature Contracts

- `src/rs/vertebra-oracle/src/lib.rs`:
  - Wire `pub mod config;` and `pub mod errors;` at the crate root so `cargo test -p vertebra-oracle --locked` compiles and runs unit tests in `config.rs` and `errors.rs`.
- `src/rs/vertebra-oracle/src/config.rs`:
  - `#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)] pub enum OracleAuthMode { #[default] Password, Token }`
  - `#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)] pub enum DrcpPurity { #[default] SelfPurity, NewPurity }`
  - `#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)] pub enum OracleProtocol { #[default] Tcp, Tcps }`
  - `#[derive(Clone)] pub struct RawOracleOptions` matching all 31 constructor parameters and defaults in `contracts.md`.
  - `#[derive(Clone, PartialEq, Eq)] pub struct NativeOracleConfig` with `pub fn validate(raw: RawOracleOptions) -> Result<Self, OracleError>` and custom `fmt::Debug` redacting `password`, `wallet_password`, `pem_cert_data`, `cwallet_sso_data`, `access_token`, and `token_auth_options`.
- `src/rs/vertebra-oracle/src/errors.rs`:
  - `#[derive(Debug, Clone, Copy, PartialEq, Eq)] pub enum OracleErrorKind { Integrity, SerializationFailure, Operational, ConnectionFailure, QueryCancelled, NotFound, Data, Programming, Interface }`
  - `#[derive(Debug, Clone, PartialEq, Eq)] pub struct OracleErrorClassification { pub kind: OracleErrorKind, pub sqlstate: &'static str, pub vendor_code: Option<i32>, pub is_transient: bool }`
  - `#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)] pub enum OracleError { Config(String), Interface(String), Transport { code: Option<i32>, message: String }, Server { code: i32, message: String }, Cancelled(String) }`
  - `pub fn extract_ora_code(message: &str) -> Option<i32>` and `pub fn classify_oracle_error(vendor_code: Option<i32>, message: &str) -> OracleErrorClassification`
  - `impl OracleError { pub fn to_py_err(self, py: Python<'_>) -> PyErr }` importing from `py.import("vertebra.db.driver")` (`IntegrityError`, `SerializationFailure`, `OperationalError`, `ConnectionFailure`, `QueryCancelledError`, `NotFoundError`, `DataError`, `ProgrammingError`, `InterfaceError`, `DatabaseError`) and attaching `.sqlstate`, `.vendor_code`, and `.is_transient` on every raised exception instance [^py-errors-rs].

## Edge Cases & Error Semantics

- Reject `port` outside `1..=65535`, non-finite or `<= 0.0` `connect_timeout`, `stmtcachesize < 0`, `prefetchrows == 0`, `arraysize == 0`, or NUL bytes in string parameters with `OracleError::Config` (mapped to `InterfaceError`).
- Reject `purity` values other than `"SELF"` or `"NEW"` and `auth_mode` values other than `"password"` or `"token"` with `OracleError::Config`.
- Enforce the exact `DbExceptionTypes` class (`vertebra.db.driver`), `sqlstate`, and `is_transient` mappings:
  - `ORA-00001` (`"23505"`), `ORA-02291`/`02292` (`"23503"`), `ORA-01400`/`01407` (`"23502"`), `ORA-02290` (`"23514"`) -> `IntegrityError` (`is_transient=False`)
  - `ORA-08177` (`"40001"`), `ORA-00060` (`"40P01"`) -> `SerializationFailure` (`is_transient=True`, note no `Error` suffix)
  - `ORA-00054`/`30006` (`"55P03"`) -> `OperationalError` (`is_transient=True`)
  - `ORA-01013` (`"57014"`) -> `QueryCancelledError` (`is_transient=True` / `False` per classification)
  - `ORA-01017`/`28000` (`"28P01"`) -> `ConnectionFailure` (`is_transient=False`)
  - `ORA-12154` (`"08006"`, `is_transient=False`) and `ORA-12170`/`12514`/`12541`/`12545` (`"08006"`, `is_transient=True`) -> `ConnectionFailure`
  - `ORA-01403` (`"02000"`) -> `NotFoundError` (`is_transient=False`)
  - `ORA-01438`/`01476`/`01722`/`01830`..`01899`/`12899` (`"22000"`) -> `DataError` (`is_transient=False`)
  - `ORA-00900`..`00999`/`06550` (`"42000"`) -> `ProgrammingError` (`is_transient=False`)
  - `ORA-03113`/`03114`/`03135`/`04061`/`04068`/`12537` (`"08006"`) -> `OperationalError` (`is_transient=True`)

## Public-Seam Test Blueprint

- **Canonical gate**: Rust unit tests in `src/rs/vertebra-oracle/src/config.rs` and `src/rs/vertebra-oracle/src/errors.rs` executed via `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle --locked` (`tests: []`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| :--- | :--- | :--- |
| `NativeOracleConfig` validation & defaults | Default `RawOracleOptions` vs `purity="INVALID"`, `port=0`, `connect_timeout=-1.0`, or NUL byte in `user` | Default options succeed; invalid options return `Err(OracleError::Config(_))`. |
| Secret redaction in `Debug` | `NativeOracleConfig` with `password=Some("s3cr3t".into())`, `access_token=Some("tok-123".into())`, `pem_cert_data=Some(vec![1, 2, 3])` | `format!("{cfg:?}")` contains `"<redacted>"` and never contains `"s3cr3t"` or `"tok-123"`. |
| Deterministic `ORA-xxxxx` to `DbExceptionTypes` classification | `classify_oracle_error` across `1`, `2291`, `1400`, `2290`, `8177`, `60`, `54`, `1013`, `1017`, `1403`, `1722`, `942`, `12154`, `12541`, `3113` | Maps to exact `(OracleErrorKind, sqlstate, vendor_code, is_transient)` entries. |
| `OracleError::to_py_err` attribute attachment | `Python::attach(\|py\| ...)` converting `OracleError::Server` for `ORA-00001`, `ORA-08177`, `ORA-12154`, `ORA-12541`, and `ORA-01013` | Instantiates `IntegrityError`, `SerializationFailure`, `ConnectionFailure`, and `QueryCancelledError` from `vertebra.db.driver` with `.sqlstate`, `.vendor_code`, and `.is_transient` populated. |

## Verification

Run the locked Rust unit test suite and Clippy gate for `vertebra-oracle`:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle --locked
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-oracle --all-targets --locked -- -D warnings
```

Commit as `feat(oracle): implement NativeOracleConfig and ORA SQLSTATE DbExceptionTypes mapping`.

## Acceptance Criteria

- [ ] `src/rs/vertebra-oracle/src/lib.rs` registers `pub mod config; pub mod errors;` so `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle --locked` compiles and runs all tests in `config.rs` and `errors.rs` (R2, R4).
- [ ] `NativeOracleConfig::validate` enforces port, timeout, NUL-byte, `auth_mode`, and DRCP `purity` (`"SELF"` | `"NEW"`) invariants in memory and redacts secrets in `Debug` output (R2).
- [ ] `OracleError::to_py_err` imports the canonical `DbExceptionTypes` classes (`IntegrityError`, `SerializationFailure`, `OperationalError`, `ConnectionFailure`, `QueryCancelledError`, `NotFoundError`, `DataError`, `ProgrammingError`, `InterfaceError`, `DatabaseError`) from `vertebra.db.driver` and attaches `.sqlstate`, `.vendor_code`, and `.is_transient` on every raised exception instance (R4).

## Notes & Discoveries

- [2026-11-09 01:15] Registering `pub mod config; pub mod errors;` in `src/rs/vertebra-oracle/src/lib.rs` during `ch02:t01` ensures `cargo test -p vertebra-oracle --locked` compiles and executes the new Rust unit tests immediately in `ch02:t01`.

[^py-errors-rs]: `src/rs/vertebra-py/src/errors.rs::DbExceptionTypes` — Canonical 13-class database exception hierarchy exported by `vertebra.db.driver` and `vertebra._vertebra`.
[^spanner-err-rs]: `src/rs/vertebra-spanner/src/errors.rs::spanner_error_to_py` — Reference PyO3 exception resolution across optional extension wheels.
[^ora-contracts]: `.agents/bundles/specs/native-oracle-driver/contracts.md` — Public connection constructor options and `ORA-xxxxx` SQLSTATE table.
