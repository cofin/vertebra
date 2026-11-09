---
type: Task
id: "native-oracle-driver:ch02:t02"
chapter_id: ch02-connect-auth-and-error-hierarchy
title: Fix OracleConfig.connect_descriptor() precedence and implement TNS, Autonomous Wallet mTLS, and GcpTokenCache dialer
description: Fix OracleConfig.connect_descriptor() priority (using OracleTopologyConfig.to_tns_descriptor()) and add OracleDrcpConfig in src/py/vertebra/db/adapters/oracle/config.py, implement src/rs/vertebra-oracle/src/dialer.rs with self-contained OracleWalletMaterial, vertebra_db::oracle::tns alias resolution, and vertebra_core::gcp_auth::GcpTokenCache token auth, and register pub mod dialer; in src/rs/vertebra-oracle/src/lib.rs.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-oracle-driver:ch02:t01"
read_set:
  - src/py/vertebra/db/adapters/oracle/config.py
  - src/rs/vertebra-oracle/src/config.rs
  - src/rs/vertebra-oracle/src/errors.rs
  - src/rs/vertebra-db/src/oracle/tns.rs
  - src/rs/vertebra-core/src/gcp_auth.rs
  - .agents/bundles/specs/native-oracle-driver/contracts.md
files:
  - src/py/vertebra/db/adapters/oracle/config.py
  - src/rs/vertebra-oracle/src/dialer.rs
  - src/rs/vertebra-oracle/src/lib.rs
tests:
  - src/py/tests/unit/db/test_oracle_adapter.py
estimated_loc: 235
plan_revision: 3
plan_commit: 769302bc
state_revision: 2
claimed_by: agent:executor
claimed_at: "2026-11-09T03:21:00Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-09T03:25:00Z"
last_verified_commit: 1ad8a82b
verification_evidence: "cargo test -p vertebra-oracle --locked --offline (9 passed), cargo clippy -p vertebra-oracle --all-targets --locked --offline -- -D warnings (0 warnings), pytest src/py/tests/unit/db/test_oracle_adapter.py -v (6 passed)"
created_at: "2026-11-09T00:55:00Z"
updated_at: "2026-11-09T03:25:00Z"
commit: null
sources:
  - id: ora-config-py
    resource: src/py/vertebra/db/adapters/oracle/config.py::OracleConfig
    title: Python OracleConfig, OracleWallet, OracleConnectDescriptor, and OracleTopologyConfig
  - id: ora-rs-tns
    resource: src/rs/vertebra-db/src/oracle/tns.rs::parse_tnsnames_ora
    title: Shared Rust TNS Connect Descriptor parser and alias resolver
  - id: gcp-auth-rs
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpAuthSource
    title: Shared pure-Rust IAM/OAuth2 bearer token cache
  - id: ora-contracts
    resource: .agents/bundles/specs/native-oracle-driver/contracts.md
    title: Public descriptor precedence, TNS, Autonomous Wallet mTLS, and token authentication contracts
tags:
  - oracle
  - config
  - dialer
  - tns
  - wallet
  - mtls
  - iam
---

# Task t02: Fix `OracleConfig.connect_descriptor()` Precedence and Implement TNS, Autonomous Wallet mTLS, and `GcpTokenCache` Dialer

## Objective & Scope Boundary

Fix `OracleConfig.connect_descriptor()` precedence in `src/py/vertebra/db/adapters/oracle/config.py` so explicit `host`, `port`, and `service_name` (or `sid`) override the class-default `dsn="oracle://localhost:1521/FREEPDB1"`, add `OracleDrcpConfig` to `OracleConfig`, implement `src/rs/vertebra-oracle/src/dialer.rs` with a self-contained `OracleWalletMaterial` struct for in-memory Autonomous Database Wallets (`.zip`, `ewallet.pem`, `cwallet.sso`), TNS alias resolution via `vertebra_db::oracle::tns::{OracleConnectDescriptor, parse_tnsnames_ora, resolve_tns_alias}`, and IAM/OAuth2 bearer token resolution via `vertebra_core::gcp_auth::{GcpAuthSource, GcpTokenCache}` (`GcpAuthSource::from_ADC()`), and register `pub mod dialer;` in `src/rs/vertebra-oracle/src/lib.rs` [^ora-config-py] [^ora-rs-tns] [^gcp-auth-rs] [^ora-contracts]. VPD tenancy fields (`context_namespace`, `context_package_proc`, `context_clear_proc`) on `OracleConfig` belong exclusively to `ch05:t02`, and worker-thread execution belongs to `ch02:t03` ([../spec.md](../spec.md)).

## Bounded Read Manifest

- `src/py/vertebra/db/adapters/oracle/config.py` — `OracleConfig`, `OracleConnectDescriptor`, `OracleWallet`, and `OracleTopologyConfig.to_tns_descriptor()` [^ora-config-py].
- `src/rs/vertebra-oracle/src/config.rs` — `NativeOracleConfig`, `OracleAuthMode`, `DrcpPurity`, and `OracleProtocol` from `ch02:t01`.
- `src/rs/vertebra-oracle/src/errors.rs` — `OracleError` and `classify_oracle_error` from `ch02:t01`.
- `src/rs/vertebra-db/src/oracle/tns.rs` — `OracleConnectDescriptor`, `parse_tnsnames_ora`, and `resolve_tns_alias` [^ora-rs-tns].
- `src/rs/vertebra-core/src/gcp_auth.rs` — `GcpAuthSource` (`GcpAuthSource::from_ADC()`) and `GcpTokenCache` [^gcp-auth-rs].
- `.agents/bundles/specs/native-oracle-driver/contracts.md` — 4-tier descriptor precedence, wallet mTLS, and token auth contracts ([../../../contracts.md](../../../contracts.md)) [^ora-contracts].

## Symbol & Signature Contracts

- `src/py/vertebra/db/adapters/oracle/config.py`:
  - `DEFAULT_ORACLE_DSN: Final[str] = "oracle://localhost:1521/FREEPDB1"`
  - `class OracleDrcpConfig(FrozenBaseStruct, frozen=True, gc=False)` with `cclass: str | None = None`, `purity: str = "SELF"`, `connection_tag: str | None = None`.
  - `OracleConfig` adds `sid: str | None = None`, `connect_descriptor_str: str | None = None`, `drcp: OracleDrcpConfig = msgspec.field(default_factory=OracleDrcpConfig)`, `auth_mode: str = "password"`, `access_token: str | None = None`, `token_auth_options: dict[str, str] | None = None` (do not add `context_package_proc` or `context_clear_proc` here; `ch05:t02` is their sole owner).
  - `OracleConfig.connect_descriptor(self) -> OracleConnectDescriptor`:
    1. Explicit `connect_descriptor_str` or wallet alias (`wallet_zip` / `wallet_path` via `OracleWallet.resolve_alias`).
    2. Explicit `topology` (`OracleConnectDescriptor.from_tns_descriptor(self.topology.to_tns_descriptor())`).
    3. Explicit non-default `dsn` (`self.dsn != DEFAULT_ORACLE_DSN` or when `self.host == "localhost"`, `self.port == 1521`, `self.service_name == "FREEPDB1"`, and `self.sid is None`).
    4. Explicit `host`, `port`, and `service_name` / `sid` when `self.dsn == DEFAULT_ORACLE_DSN` and any of `host`, `port`, `service_name`, or `sid` differs from defaults.
- `src/rs/vertebra-oracle/src/lib.rs`:
  - Add `pub mod dialer;` alongside `pub mod config; pub mod errors;`.
- `src/rs/vertebra-oracle/src/dialer.rs`:
  - Import only `OracleConnectDescriptor`, `parse_tnsnames_ora`, and `resolve_tns_alias` from `vertebra_db::oracle::tns` (do not import `OracleWalletMaterial` from `vertebra_db::oracle::tns`).
  - Define `#[derive(Clone, PartialEq, Eq)] pub struct OracleWalletMaterial` directly inside `src/rs/vertebra-oracle/src/dialer.rs` owning `tnsnames_ora: String`, `sqlnet_ora: String`, `ewallet_pem: Vec<u8>`, `cwallet_sso: Vec<u8>`, `cert_der_count: usize`, `has_private_key: bool`, and `aliases: HashMap<String, OracleConnectDescriptor>` with redacted `fmt::Debug`.
  - `impl OracleWalletMaterial`:
    - `pub fn from_zip_bytes(zip_bytes: &[u8]) -> Result<Self, OracleError>` using `zip::ZipArchive` in memory (zero temporary files).
    - `pub fn from_location(path: &Path) -> Result<Self, OracleError>` supporting both a `.zip` file path and an extracted directory containing `tnsnames.ora` / `ewallet.pem` / `cwallet.sso`.
    - `pub fn from_raw_buffers(tnsnames_ora: Option<&str>, pem_data: Option<&[u8]>, sso_data: Option<&[u8]>) -> Result<Self, OracleError>`.
  - `#[derive(Clone, PartialEq, Eq)] pub struct ResolvedOracleEndpoint` (`connect_string: String`, `protocol: OracleProtocol`, `ssl_server_dn_match: bool`, `ssl_server_cert_dn: Option<String>`, `wallet: Option<OracleWalletMaterial>`, `bearer_token: Option<String>`).
  - `pub fn resolve_endpoint(cfg: &NativeOracleConfig) -> Result<ResolvedOracleEndpoint, OracleError>` using `vertebra_core::gcp_auth::{GcpAuthSource, GcpTokenCache}` (`GcpAuthSource::from_ADC()`) when `auth_mode == OracleAuthMode::Token` and no static `access_token` is supplied.
  - `pub fn connect_oracledb(cfg: &NativeOracleConfig) -> Result<oracledb::Connection, OracleError>`.

## Edge Cases & Error Semantics

- When `OracleConfig(host="db.internal", port=1530, service_name="ORCLPDB1")` is constructed without overriding `dsn`, `connect_descriptor()` returns `OracleConnectDescriptor(host="db.internal", port=1530, service_name="ORCLPDB1")` instead of `"localhost:1521/FREEPDB1"`.
- `OracleWalletMaterial` extracts `tnsnames.ora`, `sqlnet.ora`, `ewallet.pem`, and `cwallet.sso` strictly in memory via `std::io::Cursor` + `zip::ZipArchive` without writing any file to `/tmp` or the workspace, and validates PEM certificates/keys with `rustls_pemfile` without echoing private key bytes on error.
- If an explicit TNS alias is not found in `tnsnames.ora`, `resolve_endpoint` returns `OracleError::Server { code: 12154, message: ... }` (`ORA-12154` -> `ConnectionFailure`, `sqlstate="08006"`, `is_transient=False`).

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_oracle_adapter.py` and `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle --locked`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| :--- | :--- | :--- |
| Explicit `host`/`port`/`service_name` override default `dsn` | `OracleConfig(host="db.internal", port=1530, service_name="ORCLPDB1").connect_descriptor()` | `OracleConnectDescriptor(host="db.internal", port=1530, service_name="ORCLPDB1")`. |
| Explicit non-default `dsn` and `topology.to_tns_descriptor()` | `OracleConfig(dsn="oracle://rac.internal:1525/PROD")` and `OracleConfig(topology=OracleTopologyConfig(hosts=("n1", "n2"), port=1522, service_name="SVC"))` | Resolves `rac.internal:1525/PROD` and `n1`/`n2` topology respectively. |
| Self-contained `OracleWalletMaterial` `.zip` extraction & alias lookup | Synthetic `.zip` buffer containing `tnsnames.ora` (`mydb_high = (DESCRIPTION=...)`) and valid self-signed `ewallet.pem` | `OracleWalletMaterial::from_zip_bytes` parses `MYDB_HIGH`, `cert_der_count >= 1`, `has_private_key == true`, and `resolve_endpoint` resolves `"mydb_high"` to the wallet's TCPS descriptor. |
| Missing TNS alias in wallet / `tns_admin` | `dsn = Some("unknown_alias")` with a wallet whose `tnsnames.ora` only defines `mydb_high` | Returns `Err(OracleError::Server { code: 12154, .. })` which maps to `ConnectionFailure` (`sqlstate="08006"`, `vendor_code=12154`, `is_transient=False`). |
| Token auth resolution (`access_token` & `GcpTokenCache`) | `auth_mode = "token"` with `access_token = Some("ya29-test-token")` vs empty credentials | First resolves `bearer_token == Some("ya29-test-token")`; second returns `Err(OracleError::Config(_))`. |

## Verification

Run the Rust crate test suite, Clippy, and Python adapter unit tests:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle --locked
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-oracle --all-targets --locked -- -D warnings
uv run --no-sync pytest src/py/tests/unit/db/test_oracle_adapter.py -v
```

Commit as `feat(oracle): fix descriptor precedence and implement TNS, wallet mTLS, and token dialer`.

## Acceptance Criteria

- [x] `OracleConfig.connect_descriptor()` returns `OracleConnectDescriptor(host="db.internal", port=1530, service_name="ORCLPDB1")` when `host="db.internal"`, `port=1530`, `service_name="ORCLPDB1"` are supplied without overriding `dsn`, and uses `self.topology.to_tns_descriptor()` when `topology` is set (R2).
- [x] `src/rs/vertebra-oracle/src/dialer.rs` defines a self-contained `OracleWalletMaterial` struct that extracts `.zip` archives and wallet directories in memory and validates PEM material via `rustls-pemfile` without importing `OracleWalletMaterial` from `vertebra_db::oracle::tns` (R2).
- [x] `resolve_endpoint` resolves `tnsnames.ora` aliases via `vertebra_db::oracle::tns`, supports O5LOGON password and `vertebra_core::gcp_auth::GcpTokenCache` (`GcpAuthSource::from_ADC()`) bearer token auth, and raises `ORA-12154` (`ConnectionFailure`, `is_transient=False`) on unknown TNS aliases (R2, R4).

## Notes & Discoveries

- [2026-11-09 01:15] Defining `OracleWalletMaterial` directly inside `src/rs/vertebra-oracle/src/dialer.rs` decouples `vertebra-oracle` from the dead `OracleWalletMaterial` stub removed from `vertebra-db` by `driver-fail-loud` `ch07`.

[^ora-config-py]: `src/py/vertebra/db/adapters/oracle/config.py::OracleConfig` — Python `OracleConfig`, `OracleWallet`, `OracleConnectDescriptor`, and `OracleTopologyConfig`.
[^ora-rs-tns]: `src/rs/vertebra-db/src/oracle/tns.rs::parse_tnsnames_ora` — Shared Rust TNS Connect Descriptor parser and alias resolver.
[^gcp-auth-rs]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpAuthSource` — Shared pure-Rust IAM/OAuth2 bearer token cache.
[^ora-contracts]: `.agents/bundles/specs/native-oracle-driver/contracts.md` — Public descriptor precedence, TNS, Autonomous Wallet mTLS, and token authentication contracts.
