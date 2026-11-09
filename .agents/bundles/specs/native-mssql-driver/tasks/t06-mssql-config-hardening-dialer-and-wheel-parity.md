---
type: Task
id: t06
flow_id: native-mssql-driver
title: "Harden in-memory config validation, Cloud SQL :3307 dialer wiring, secret redaction, and wheel/GIL test parity"
description: "Enforce port bounds (1..=65535), NUL-byte rejection, encryption/PEM validation, and password redaction in Rust MssqlClientConfig and Python MSSQLConfig, wire SyncCloudSQLDialer / AsyncCloudSQLDialer through MSSQLCore into the native client connection path, and add mssql-test / mssql-wheel-check Makefile targets plus free-threaded GIL and wheel isolation unit tests."
status: stable
state: completed
verification_strategy: tdd
depends_on:
  - t05
plan_revision: 1
state_revision: 2
commit_sha: d6e15596
claimed_at: "2026-11-09T16:12:00Z"
completed_at: "2026-11-09T16:31:00Z"
created_at: "2026-11-09T15:36:00Z"
updated_at: "2026-11-09T16:31:00Z"
reads:
  - src/rs/vertebra-mssql/src/config.rs
  - src/rs/vertebra-mssql/src/lib.rs
  - src/py/vertebra/db/adapters/mssql/config.py
  - src/py/vertebra/db/adapters/mssql/core.py
  - src/py/vertebra/db/adapters/mssql/driver.py
  - Makefile
  - src/py/tests/unit/test_freethreaded_gil.py
targets:
  - src/rs/vertebra-mssql/src/config.rs
  - src/rs/vertebra-mssql/src/lib.rs
  - src/py/vertebra/db/adapters/mssql/config.py
  - src/py/vertebra/db/adapters/mssql/core.py
  - src/py/vertebra/db/adapters/mssql/driver.py
  - Makefile
  - src/py/tests/unit/test_freethreaded_gil.py
  - src/py/tests/unit/db/test_freethreaded_gil.py
  - src/py/tests/unit/db/test_optional_driver_wheels.py
  - src/py/tests/unit/db/test_mssql_driver.py
sources:
  - id: mssql-config-rs
    resource: src/rs/vertebra-mssql/src/config.rs
    title: Rust MssqlClientConfig DSN parser, validation, and TLS/Cloud SQL options
  - id: mssql-config-py
    resource: src/py/vertebra/db/adapters/mssql/config.py
    title: Python MSSQLConfig and Cloud SQL dialer integration
---

# Task t06: Harden in-memory config validation, Cloud SQL `:3307` dialer wiring, secret redaction, and wheel/GIL test parity

<!-- truth: start -->
- **In-Memory Validation & Secret Redaction**: `MssqlClientConfig` and `MSSQLConfig` validate port bounds (`1..=65535`), reject NUL (`\0`) bytes in `dsn`, `host`, `database`, `user`, `application_name`, and `server_name`, validate `encryption` (`optional`, `login_only`, `required`, `not_supported`, `strict`) and PEM inputs (`ca_pem`, `client_cert_pem`, `client_key_pem`), and redact passwords (`[REDACTED]`) in `Debug`, `Display`, and `__repr__` [^mssql-config-rs] [^mssql-config-py].
- **Cloud SQL `:3307` Dialer Wiring**: Passing `SyncCloudSQLDialer` or `AsyncCloudSQLDialer` to `SyncMSSQLDriver(..., dialer=...)` or `AsyncMSSQLDriver(..., dialer=...)` forwards the dialer's `CloudSQLConfig` (`instance_connection_name`, `ip_type`, `auth_type`) through `MSSQLCore.create_sync_rust_client` / `create_async_rust_client` into the native client connection path [^mssql-config-py].
- **Makefile, Free-Threaded GIL & Wheel Isolation Parity**: `Makefile` defines `mssql-test` and `mssql-wheel-check`, and unit suites (`test_freethreaded_gil.py`, `test_optional_driver_wheels.py`) verify `vertebra._mssql` (`#[pymodule(gil_used = false)]`) and wheel isolation.
<!-- truth: end -->

## Context

Completes full parity across configuration hardening, secret redaction, Cloud SQL `:3307` dialer forwarding, `Makefile` targets (`mssql-test`, `mssql-wheel-check`), free-threaded Python 3.14t/3.15t `Py_mod_gil` verification, and optional driver wheel isolation tests.

## Public Seam Contract

- `MssqlClientConfig::validate(&self) -> Result<(), MssqlError>` and custom `Debug` / `Display` redacting `password`.
- `MSSQLConfig` port/NUL/encryption validation and password-redacted `__repr__`.
- `MSSQLCore.create_sync_rust_client(config, dialer=None)` and `create_async_rust_client(config, dialer=None)` wiring `dialer.config` into `SyncMSSQLClient` / `AsyncMSSQLClient`.
- `Makefile` targets `mssql-test` and `mssql-wheel-check`.

## Implementation Blueprint

1. Add `MssqlClientConfig::validate`, NUL-byte checks, PEM header validation (`-----BEGIN `), `optional` encryption alias, and custom `Debug` / `Display` with `password: "[REDACTED]"` in `src/rs/vertebra-mssql/src/config.rs`.
2. Add `__post_init__` validation and password-redacting `__repr__` to `MSSQLConfig` in `src/py/vertebra/db/adapters/mssql/config.py`, and forward `dialer` in `src/py/vertebra/db/adapters/mssql/core.py` and `driver.py`.
3. Add `mssql-test` and `mssql-wheel-check` to `Makefile`, update `src/py/tests/unit/test_freethreaded_gil.py`, and add `src/py/tests/unit/db/test_freethreaded_gil.py` and `src/py/tests/unit/db/test_optional_driver_wheels.py`.

## Acceptance Criteria

- [x] `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-mssql --locked` and `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-mssql --all-targets --locked -- -D warnings` pass.
- [x] `pytest src/py/tests/unit/db/test_mssql_driver.py src/py/tests/unit/test_freethreaded_gil.py src/py/tests/unit/db/test_freethreaded_gil.py src/py/tests/unit/db/test_optional_driver_wheels.py` passes.
- [x] Wheel build into `/tmp/vtb-wheels-mssql` and `review_setup validate` pass.

[^mssql-config-rs]: `src/rs/vertebra-mssql/src/config.rs` — Rust MssqlClientConfig DSN parser, validation, and TLS/Cloud SQL options.
[^mssql-config-py]: `src/py/vertebra/db/adapters/mssql/config.py` — Python MSSQLConfig and Cloud SQL dialer integration.
