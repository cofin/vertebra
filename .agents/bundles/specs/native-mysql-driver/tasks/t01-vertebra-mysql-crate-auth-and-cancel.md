---
type: Task
flow_id: native-mysql-driver
task_id: t01
title: "Scaffold vertebra-mysql crate, Handshake v10 auth, TLS, Cloud SQL IAM, and KILL QUERY cancellation"
description: "Create standalone PyO3 extension crate src/rs/vertebra-mysql (vertebra._mysql), register workspace and optional wheel metadata, delete obsolete vertebra_db::mysql, and implement SyncMySQLClient and AsyncMySQLClient connection management, Handshake v10, caching_sha2_password, mysql_native_password, rustls TLS, Cloud SQL :3307 mTLS/IAM auth, and KILL QUERY cancellation."
status: stable
state: in_progress
verification_strategy: tdd
Verification-Exempt-Reason: null
dependencies: []
files_to_modify:
  - src/rs/Cargo.toml
  - src/rs/Cargo.lock
  - src/rs/vertebra-mysql/Cargo.toml
  - src/rs/vertebra-mysql/pyproject.toml
  - src/rs/vertebra-mysql/build.rs
  - src/rs/vertebra-mysql/src/lib.rs
  - src/rs/vertebra-mysql/src/config.rs
  - src/rs/vertebra-mysql/src/errors.rs
  - src/rs/vertebra-mysql/src/protocol.rs
  - src/rs/vertebra-mysql/src/client.rs
  - src/rs/vertebra-db/src/lib.rs
  - src/rs/vertebra-db/src/mysql/mod.rs
  - src/py/vertebra/_mysql.pyi
  - src/rs/vertebra-bigquery/pyproject.toml
  - src/rs/vertebra-bundler/pyproject.toml
  - src/rs/vertebra-spanner/pyproject.toml
  - src/rs/vertebra-storage/pyproject.toml
  - pyproject.toml
  - Makefile
  - .agents/skills/review-core/references/areas.toml
  - src/py/tests/unit/build/test_packaging_and_pgo.py
read_set:
  - .agents/bundles/specs/native-mysql-driver/spec.md
  - .agents/bundles/specs/native-data-platform/research/rust-driver-crates_20261103.md
  - src/rs/vertebra-db/src/mysql/mod.rs
  - src/rs/vertebra-db/src/py_async.rs
  - src/rs/vertebra-db/src/dialer.rs
  - src/rs/vertebra-db/src/connection.rs
  - src/rs/vertebra-spanner/Cargo.toml
  - src/rs/vertebra-spanner/pyproject.toml
  - src/py/tests/unit/build/test_packaging_and_pgo.py
commands_to_run:
  - "cargo test --manifest-path src/rs/Cargo.toml -p vertebra-mysql -p vertebra-db --locked"
  - "cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-mysql -p vertebra-db --all-targets --locked -- -D warnings"
  - "uv run python -m mypy.stubtest vertebra._mysql"
  - "uv run pytest src/py/tests/unit/build/test_packaging_and_pgo.py -q"
  - "uv run python -m tools.scripts.review_setup validate"
commit_sha: null
created_at: "2026-11-09T03:25:00Z"
updated_at: "2026-11-09T03:25:00Z"
sources:
  - id: mysql-spec
    resource: .agents/bundles/specs/native-mysql-driver/spec.md
    title: Native MySQL driver specification
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs
    title: Shared AsyncCompletionQueue + Notifier PyO3 bridge
  - id: dialer-rs
    resource: src/rs/vertebra-db/src/dialer.rs
    title: Cloud SQL Admin API ephemeral mTLS dialer
---

# Task t01: Scaffold `vertebra-mysql` crate, Handshake v10 auth, TLS, Cloud SQL IAM, and `KILL QUERY` cancellation

<!-- truth: start -->
- `src/rs/vertebra-mysql` compiles as `vertebra._mysql` (`#[pymodule(gil_used = false)]`) with `mysql_async =0.37.1` (`default-rustls-ring`) and `arrow =60.1.0`, while `src/rs/vertebra-db/src/mysql/mod.rs` is deleted [^mysql-spec].
- `SyncMySQLClient` and `AsyncMySQLClient` implement Handshake v10, `caching_sha2_password` (fast path `0x03`, TLS cleartext `0x04`, and RSA OAEP SHA-1 `0x02`), `mysql_native_password`, `rustls` TLS modes (`disable`, `prefer`, `require`, `verify-ca`, `verify-full`), Cloud SQL `:3307` mTLS / `:3306` IAM token refresh, and out-of-band `KILL QUERY <conn_id>` cancellation [^mysql-spec] [^py-async-rs] [^dialer-rs].
<!-- truth: end -->

## Context

`native-mysql-driver` isolates MySQL 8.4/9 and MariaDB wire execution into `src/rs/vertebra-mysql/` (`vertebra._mysql`, packaged as `vertebra-mysql` under `vertebra-core[mysql]`) and removes the legacy `vertebra_db::mysql` module from `src/rs/vertebra-db/` [^mysql-spec].

## Public Seam & TDD Plan

1. **Red**:
   - Write Rust unit and loopback wire tests in `src/rs/vertebra-mysql/` verifying:
     - Handshake v10 parsing, capability negotiation (`CLIENT_PROTOCOL_41`, `CLIENT_SECURE_CONNECTION`, `CLIENT_PLUGIN_AUTH`, `CLIENT_MULTI_RESULTS`, `CLIENT_MULTI_STATEMENTS`, `CLIENT_DEPRECATE_EOF`, `CLIENT_SSL`), and sequence number tracking across TLS upgrade.
     - `caching_sha2_password` scramble computation, fast-auth hit (`0x03`), full-auth over TLS (`0x04` -> cleartext null-terminated password), and full-auth over plaintext (`0x04` -> `0x02` RSA public key request -> RSA-OAEP-SHA1 encrypted `password\0 XOR nonce_20`).
     - `mysql_native_password` scramble computation (`SHA1(password) XOR SHA1(nonce || SHA1(SHA1(password)))`) and `AuthSwitchRequest` (`0xFE`) handling.
     - `rustls` + `ring` TLS modes (`disable`, `prefer`, `require`, `verify-ca`, `verify-full`).
     - Cloud SQL `:3307` mTLS dialer config and direct `:3306` IAM access-token injection via `GcpTokenCache` when `enable_iam_auth=True`.
     - Out-of-band `KILL QUERY <conn_id>` cancellation over a dedicated control socket that interrupts an in-flight query without dropping the session connection.
2. **Green**:
   - Create `src/rs/vertebra-mysql/` (`Cargo.toml`, `pyproject.toml`, `build.rs`, `src/lib.rs`, `src/config.rs`, `src/errors.rs`, `src/protocol.rs`, `src/client.rs`).
   - Delete `src/rs/vertebra-db/src/mysql/mod.rs` and remove `pub mod mysql;` / `pub use mysql::*;` from `src/rs/vertebra-db/src/lib.rs`.
   - Register `vertebra-mysql` in `src/rs/Cargo.toml`, `src/rs/Cargo.lock`, root `pyproject.toml`, sibling `pyproject.toml` exclusion globs, `src/py/vertebra/_mysql.pyi`, `Makefile`, `.agents/skills/review-core/references/areas.toml`, and `src/py/tests/unit/build/test_packaging_and_pgo.py`.
3. **Refactor & Verify**:
   - Run `cargo test`, `cargo clippy`, `mypy.stubtest vertebra._mysql`, wheel isolation check (`maturin build`), `pytest src/py/tests/unit/build/test_packaging_and_pgo.py`, and `review_setup validate`.

## Acceptance Criteria

- [ ] `src/rs/vertebra-db/src/mysql/mod.rs` is deleted and `cargo test -p vertebra-db --locked` passes.
- [ ] `cargo test -p vertebra-mysql --locked` and `cargo clippy -p vertebra-mysql --all-targets --locked -- -D warnings` pass with zero warnings.
- [ ] `mypy.stubtest vertebra._mysql` passes with zero errors against `src/py/vertebra/_mysql.pyi`.
- [ ] Wheel built via `maturin build --manifest-path src/rs/vertebra-mysql/Cargo.toml` contains `vertebra/_mysql*.so` and `vertebra/_mysql.pyi` and zero `vertebra/*.py` files.
- [ ] `pytest src/py/tests/unit/build/test_packaging_and_pgo.py` and `python -m tools.scripts.review_setup validate` pass.

[^mysql-spec]: `.agents/bundles/specs/native-mysql-driver/spec.md` — Native MySQL driver specification.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs` — Shared AsyncCompletionQueue + Notifier PyO3 bridge.
[^dialer-rs]: `src/rs/vertebra-db/src/dialer.rs` — Cloud SQL Admin API ephemeral mTLS dialer.
