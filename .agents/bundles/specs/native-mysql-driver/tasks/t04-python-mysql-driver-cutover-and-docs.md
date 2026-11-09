---
type: Task
flow_id: native-mysql-driver
task_id: t04
title: "Cut Python SyncMySQLDriver and AsyncMySQLDriver over to vertebra._mysql and implement SyncMySQLPipeline and AsyncMySQLPipeline"
description: "Cut src/py/vertebra/db/adapters/mysql/driver.py, config.py, and core.py over to vertebra._mysql (SyncMySQLClient, AsyncMySQLClient) with zero pymysql, asyncmy, or memory:// paths, implement SyncMySQLPipeline and AsyncMySQLPipeline, enforce strict ArrowCursor and NotFoundError semantics in _mysql.pyi and vertebra-mysql, and validate MySQLConfig bounds and secret redaction."
status: stable
state: completed
verification_strategy: tdd
Verification-Exempt-Reason: null
dependencies:
  - t03
files_to_modify:
  - src/py/vertebra/_mysql.pyi
  - src/py/vertebra/db/arrow.py
  - src/py/vertebra/db/adapters/mysql/__init__.py
  - src/py/vertebra/db/adapters/mysql/config.py
  - src/py/vertebra/db/adapters/mysql/core.py
  - src/py/vertebra/db/adapters/mysql/driver.py
  - src/py/vertebra/db/adapters/mysql/pipeline.py
  - src/rs/vertebra-mysql/src/client.rs
  - src/rs/vertebra-mysql/src/config.rs
  - src/rs/vertebra-mysql/src/errors.rs
  - src/rs/vertebra-mysql/src/loopback.rs
read_set:
  - .agents/bundles/specs/native-mysql-driver/spec.md
  - src/py/vertebra/_mysql.pyi
  - src/py/vertebra/db/arrow.py
  - src/py/vertebra/db/adapters/mysql/config.py
  - src/py/vertebra/db/adapters/mysql/core.py
  - src/py/vertebra/db/adapters/mysql/driver.py
  - src/py/vertebra/db/driver/capabilities.py
commands_to_run:
  - "cargo test --manifest-path src/rs/Cargo.toml -p vertebra-mysql --locked"
  - "uv run ruff check src/py/vertebra/db/adapters/mysql src/py/vertebra/db/arrow.py src/py/vertebra/_mysql.pyi"
  - "uv run mypy src/py/vertebra/db/adapters/mysql src/py/vertebra/db/arrow.py src/py/vertebra/_mysql.pyi"
  - "uv run python -m mypy.stubtest vertebra._mysql"
commit_sha: 71c7513c
created_at: "2026-11-09T03:25:00Z"
updated_at: "2026-11-09T16:50:00Z"
sources:
  - id: mysql-spec
    resource: .agents/bundles/specs/native-mysql-driver/spec.md
    title: Native MySQL driver specification
  - id: mysql-py
    resource: src/py/vertebra/db/adapters/mysql/driver.py
    title: Python SyncMySQLDriver and AsyncMySQLDriver target cutover module
  - id: capabilities-py
    resource: src/py/vertebra/db/driver/capabilities.py
    title: SyncPipelineCapable and AsyncPipelineCapable protocols
---

# Task t04: Cut Python `SyncMySQLDriver` and `AsyncMySQLDriver` over to `vertebra._mysql` and implement `SyncMySQLPipeline` and `AsyncMySQLPipeline`

<!-- truth: start -->
- `SyncMySQLDriver` and `AsyncMySQLDriver` load `vertebra._mysql` via `ensure_module("vertebra._mysql", package="vertebra-core[mysql]", extra="mysql")` and delegate all operations directly to `SyncMySQLClient` and `AsyncMySQLClient` with zero `pymysql`, `asyncmy`, `asyncio.to_thread`, or `memory://` fallback paths [^mysql-spec] [^mysql-py].
- `SyncMySQLPipeline` and `AsyncMySQLPipeline` implement `SyncPipeline` and `AsyncPipeline` with automatic `SAVEPOINT` nesting when executed inside an active transaction [^capabilities-py].
- `src/py/vertebra/_mysql.pyi` and `src/rs/vertebra-mysql/` type `select_to_arrow` and `stream_arrow_batches` as `ArrowCursor`, raise `NotFoundError` from `fetch_one` when `0` rows match, preserve column names in `execute_with_description` on empty result sets, and enforce port bounds, NUL rejection, SSL mode validation, and secret redaction [^mysql-spec].
<!-- truth: end -->

## Context

Task `t04` cuts the Python MySQL adapter package (`src/py/vertebra/db/adapters/mysql/`) over to `vertebra._mysql`, adds `SyncMySQLPipeline` and `AsyncMySQLPipeline`, tightens `_mysql.pyi` and `vertebra-mysql` `NotFoundError` / `ArrowCursor` / empty-result `execute_with_description` semantics, and enforces pure in-memory config validation and secret redaction [^mysql-spec] [^mysql-py] [^capabilities-py].

## Public Seam & TDD Plan

1. **Red**:
   - Assert `SyncMySQLDriver` and `AsyncMySQLDriver` satisfy `SyncPipelineCapable` and `AsyncPipelineCapable`, raise `NotFoundError` on `select_one` / `select_value` / `fetch_one` with `0` rows, preserve column descriptions on empty `execute_with_description`, and reject invalid ports, NUL bytes, invalid SSL modes, and `memory://` DSNs while redacting passwords and IAM tokens in `repr`.
2. **Green**:
   - Implement `src/py/vertebra/db/adapters/mysql/{config,core,driver,pipeline,__init__}.py`, update `src/py/vertebra/_mysql.pyi` and `src/py/vertebra/db/arrow.py`, and update `src/rs/vertebra-mysql/src/{client,config,errors,loopback}.rs`.
3. **Refactor & Verify**:
   - Run `cargo test`, `ruff check`, `mypy`, and `mypy.stubtest vertebra._mysql`.

## Acceptance Criteria

- [x] `src/py/vertebra/db/adapters/mysql/` contains zero `pymysql`, `asyncmy`, `asyncio.to_thread`, or `memory://` execution paths.
- [x] `SyncMySQLPipeline` and `AsyncMySQLPipeline` satisfy `SyncPipeline` and `AsyncPipeline` and nest via `SAVEPOINT` when already inside a transaction.
- [x] `fetch_one` raises `NotFoundError` on `0` rows and `_mysql.pyi` types `select_to_arrow` and `stream_arrow_batches` with `ArrowCursor`.
- [x] `MySQLConfig` and `SyncMySQLClient` / `AsyncMySQLClient` validate port bounds (`1..=65535`), NUL bytes, SSL modes, and pool bounds and redact passwords/tokens in `repr`.

[^mysql-spec]: `.agents/bundles/specs/native-mysql-driver/spec.md` — Native MySQL driver specification.
[^mysql-py]: `src/py/vertebra/db/adapters/mysql/driver.py` — Python SyncMySQLDriver and AsyncMySQLDriver target cutover module.
[^capabilities-py]: `src/py/vertebra/db/driver/capabilities.py` — SyncPipelineCapable and AsyncPipelineCapable protocols.
