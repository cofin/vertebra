---
type: Task
id: t01
flow_id: native-mssql-driver
title: "Scaffold vertebra-mssql crate, TDS PRELOGIN/TLS/LOGIN7, and Cloud SQL :3307 mTLS"
description: "Create standalone PyO3 crate src/rs/vertebra-mssql (vertebra._mssql + vertebra-core[mssql]), clean up legacy TDS framing helpers in vertebra-db/vertebra-py, and implement SyncMSSQLClient/AsyncMSSQLClient connection, PRELOGIN, TLS negotiation, LOGIN7 SQL auth, ENVCHANGE routing, and Cloud SQL :3307 mTLS."
status: stable
state: completed
verification_strategy: tdd
depends_on: []
plan_revision: 1
state_revision: 2
commit_sha: null
claimed_at: "2026-11-09T03:20:00Z"
completed_at: "2026-11-09T14:58:00Z"
created_at: "2026-11-09T03:20:00Z"
updated_at: "2026-11-09T14:58:00Z"
reads:
  - src/rs/Cargo.toml
  - src/rs/vertebra-spanner/Cargo.toml
  - src/rs/vertebra-spanner/pyproject.toml
  - src/rs/vertebra-db/src/mssql/mod.rs
  - src/rs/vertebra-db/src/py_async.rs
  - src/rs/vertebra-py/src/db.rs
  - src/py/vertebra/db/adapters/mssql/core.py
  - src/py/vertebra/db/adapters/mssql/config.py
targets:
  - src/rs/Cargo.toml
  - src/rs/Cargo.lock
  - src/rs/vertebra-mssql/Cargo.toml
  - src/rs/vertebra-mssql/pyproject.toml
  - src/rs/vertebra-mssql/build.rs
  - src/rs/vertebra-mssql/src/lib.rs
  - src/rs/vertebra-mssql/src/config.rs
  - src/rs/vertebra-mssql/src/transport.rs
  - src/rs/vertebra-mssql/src/client.rs
  - src/rs/vertebra-mssql/src/errors.rs
  - src/rs/vertebra-db/src/mssql/mod.rs
  - src/rs/vertebra-db/src/lib.rs
  - src/rs/vertebra-py/src/db.rs
  - src/py/vertebra/_vertebra.pyi
  - src/py/vertebra/_mssql.pyi
  - src/rs/vertebra-bigquery/pyproject.toml
  - src/rs/vertebra-bundler/pyproject.toml
  - src/rs/vertebra-spanner/pyproject.toml
  - src/rs/vertebra-storage/pyproject.toml
  - pyproject.toml
  - Makefile
  - .agents/skills/review-core/references/areas.toml
  - src/py/vertebra/db/adapters/mssql/core.py
  - src/py/vertebra/db/adapters/mssql/config.py
  - src/py/vertebra/db/adapters/mssql/__init__.py
  - src/py/tests/unit/build/test_packaging_and_pgo.py
sources:
  - id: mssql-rs
    resource: src/rs/vertebra-db/src/mssql/mod.rs::rewrite_mssql_placeholders
    title: Surviving @P1 placeholder rewrite helper in vertebra-db
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs
    title: Shared AsyncCompletionQueue + Notifier PyO3 bridge
---

# Task t01: Scaffold `vertebra-mssql` crate, TDS `PRELOGIN`/TLS/`LOGIN7`, and Cloud SQL `:3307` mTLS

<!-- truth: start -->
- **Standalone PyO3 Extension (`vertebra._mssql`)**: `src/rs/vertebra-mssql` compiles `cdylib` + `rlib` with `#[pymodule(gil_used = false)] fn _mssql`, built on `tiberius =0.13.0` (`tds73`, `rustls`, `chrono`, `rust_decimal`) and `tokio-util` (`compat`), registered in `src/rs/Cargo.toml`, `pyproject.toml`, `Makefile`, and `.agents/skills/review-core/references/areas.toml` [^py-async-rs].
- **Legacy TDS Framing Removal**: `src/rs/vertebra-db/src/mssql/mod.rs` retains only `rewrite_mssql_placeholders` [^mssql-rs]; `encode_tds_packet`, `decode_tds_packet`, `TdsPacketType`, `MSSQL_DIALECT_TAG`, `MssqlPool`, and `encode_tds_sql_batch` are removed from Rust and Python.
- **Connection, TLS & Cloud SQL `:3307` mTLS**: `SyncMSSQLClient` and `AsyncMSSQLClient` (`_vertebra_mssql_completion_drainer`) parse `mssql://` / `sqlserver://` DSNs and keyword parameters, negotiate TDS `PRELOGIN` / TLS (`EncryptionLevel`), authenticate via `LOGIN7`, follow `ENVCHANGE` routing redirects, and dial Cloud SQL for SQL Server `:3307` mTLS tunnels.
<!-- truth: end -->

## Context

`vertebra-mssql` provides the pure-Rust TDS 7.4/8.0 client extension `vertebra._mssql` (`vertebra-core[mssql]`) with zero `msodbcsql18` or `unixODBC` dependencies. Legacy fake TDS framing helpers in `vertebra-db` and `vertebra-py` are removed so `tiberius` is the sole TDS wire implementation while `vertebra_db::mssql::rewrite_mssql_placeholders` remains the shared `@P1` placeholder rewriter [^mssql-rs].

## Public Seam Contract

- `vertebra._mssql`:
  - `SyncMSSQLClient(dsn=None, *, host=None, port=1433, user=None, password=None, database="master", instance_name=None, application_name="vertebra", encrypt="required", trust_server_certificate=False, ssl_ca_pem=None, ssl_client_cert_pem=None, ssl_client_key_pem=None, hostname_in_certificate=None, read_only_intent=False, multi_subnet_failover=False, connect_timeout_ms=10000, command_timeout_ms=30000, packet_size=4096, pool_min_size=1, pool_max_size=10, cloud_sql_instance=None, cloud_sql_ip_type="PUBLIC", cloud_sql_access_token=None)`
  - `AsyncMSSQLClient(..., loop=None)` using `AsyncCompletionQueue` + `Notifier` (`"_vertebra_mssql_completion_drainer"`) [^py-async-rs].
- `src/py/vertebra/_mssql.pyi` type stub matching the extension module surface.

## Implementation Blueprint

1. Create `src/rs/vertebra-mssql/` (`Cargo.toml`, `pyproject.toml`, `build.rs`, `src/lib.rs`, `src/config.rs`, `src/transport.rs`, `src/client.rs`, `src/errors.rs`).
2. Register `"vertebra-mssql"` in `src/rs/Cargo.toml`, update `src/rs/Cargo.lock`, add `src/py/vertebra/_mssql.pyi`, exclude `_mssql.pyi` in sibling optional wheels' `pyproject.toml`, add `mssql` extra in root `pyproject.toml`, and register `vertebra-mssql` in `Makefile` and `.agents/skills/review-core/references/areas.toml`.
3. Clean up `src/rs/vertebra-db/src/mssql/mod.rs` to keep only `rewrite_mssql_placeholders`, remove `encode_tds_sql_batch` from `src/rs/vertebra-py/src/db.rs` and `src/py/vertebra/_vertebra.pyi`, and remove `encode_tds_sql_batch` from `src/py/vertebra/db/adapters/mssql/`.
4. Implement TDS `PRELOGIN`, `EncryptionLevel` TLS negotiation (`required`, `on`, `off`, `not_supported`, `strict`), `LOGIN7` SQL authentication, `ENVCHANGE` routing redirect handling, and Cloud SQL `:3307` mTLS outer tunnel support.

## Acceptance Criteria

- [x] `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-mssql -p vertebra-db --locked` passes.
- [x] `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-mssql --all-targets --locked -- -D warnings` passes.
- [x] `mypy.stubtest vertebra._mssql` and `mypy.stubtest vertebra._vertebra` pass.
- [x] `pytest src/py/tests/unit/build/test_packaging_and_pgo.py` and `review_setup validate` pass.

[^mssql-rs]: `src/rs/vertebra-db/src/mssql/mod.rs::rewrite_mssql_placeholders` — Surviving `@P1` placeholder rewrite helper in vertebra-db.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs` — Shared AsyncCompletionQueue + Notifier PyO3 bridge.
