---
type: Task
id: "native-duckdb-driver:t04"
title: "Add lossless 0-row column metadata, savepoints, config validation hardening, and secret redaction"
description: "Expose execute_with_description (preserving column metadata on empty result sets), execute_tuples, and savepoint/rollback_to_savepoint/release_savepoint on SyncDuckDBClient and AsyncDuckDBClient, bind full vertebra._vertebra DatabaseError hierarchy with sqlstate/vendor_code/is_transient, enforce NUL-byte and max_connections validation, fail-loud ExtensionSpec, GcpTokenCacheCapsule extraction, and secret redaction."
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-duckdb-driver:t03"
read_set:
  - src/rs/vertebra-duckdb/src/lib.rs
  - src/rs/vertebra-duckdb/src/errors.rs
  - src/py/vertebra/_duckdb.pyi
  - src/py/vertebra/db/adapters/duckdb/config.py
files:
  - src/rs/vertebra-duckdb/src/lib.rs
  - src/rs/vertebra-duckdb/src/errors.rs
  - src/py/vertebra/_duckdb.pyi
  - src/py/vertebra/db/adapters/duckdb/config.py
tests:
  - src/rs/vertebra-duckdb/src/lib.rs
  - src/py/tests/unit/db/test_duckdb_driver.py
estimated_loc: 400
plan_revision: 4
plan_commit: 88c30004
state_revision: 1
claimed_by: agent
claimed_at: "2026-11-09T15:50:00Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-09T16:31:00Z"
last_verified_commit: 9c549ed1
verification_evidence: "cargo test -p vertebra-duckdb --locked && python -m mypy.stubtest vertebra._duckdb --concise"
created_at: "2026-11-09T15:46:00Z"
updated_at: "2026-11-09T16:34:00Z"
commit: 8869f7c1
sources:
  - id: duckdb-rs
    resource: src/rs/vertebra-db/src/duckdb/mod.rs::DuckdbConfig
    title: DuckDB configuration and bootstrap pragma primitives in vertebra-db
  - id: duckdb-config-py
    resource: src/py/vertebra/db/adapters/duckdb/config.py::DuckDBConfig
    title: DuckDBConfig immutable configuration and cgroup-clamped memory settings
tags:
  - rust
  - duckdb
  - pyo3
  - metadata
  - savepoint
---

# Task t04: Add Lossless 0-Row Column Metadata, Savepoints, Config Validation Hardening, and Secret Redaction

## Objective & Scope Boundary

Enhance `src/rs/vertebra-duckdb/src/lib.rs`, `src/rs/vertebra-duckdb/src/errors.rs`, `src/py/vertebra/_duckdb.pyi`, and `src/py/vertebra/db/adapters/duckdb/config.py` so `SyncDuckDBClient` and `AsyncDuckDBClient` expose `execute_with_description(sql, parameters=None) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]` (preserving column names from the statement/Arrow schema even when `0` rows match), `execute_tuples(sql, parameters=None) -> list[tuple[Any, ...]]`, and transactional savepoint methods (`savepoint(name: str)`, `rollback_to_savepoint(name: str)`, `release_savepoint(name: str)`), bind all canonical `vertebra._vertebra` `DatabaseError` subclasses (`InterfaceError`, `NotFoundError`, `SerializationFailure`, `QueryCancelledError`, `IntegrityError`, `ProgrammingError`, `DataError`, `OperationalError`, `PoolExhaustedError`, `NotSupportedError`) with `.sqlstate`, `.vendor_code`, and `.is_transient` attributes, enforce NUL-byte rejection (`'\0'`), `max_connections >= 1`, fail-loud `ExtensionSpec` (`INSTALL <ext> [FROM <repo>]; LOAD <ext>`), support `GcpTokenCacheCapsule`, and redact bearer tokens and secrets in `Debug`, `Display`, and `__repr__` [^duckdb-rs] [^duckdb-config-py].

## Symbol & Signature Contracts

- `src/rs/vertebra-duckdb/src/lib.rs` & `src/py/vertebra/_duckdb.pyi`:
  - `SyncDuckDBClient.execute_tuples(sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> list[tuple[Any, ...]]`
  - `SyncDuckDBClient.execute_with_description(sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]`
  - `SyncDuckDBClient.savepoint(name: str) -> None`
  - `SyncDuckDBClient.rollback_to_savepoint(name: str) -> None`
  - `SyncDuckDBClient.release_savepoint(name: str) -> None`
  - `AsyncDuckDBClient.execute_tuples(...) -> Awaitable[list[tuple[Any, ...]]]`
  - `AsyncDuckDBClient.execute_with_description(...) -> Awaitable[tuple[list[tuple[Any, ...]], tuple[str, ...]]]`
  - `AsyncDuckDBClient.savepoint(name: str) -> Awaitable[None]`
  - `AsyncDuckDBClient.rollback_to_savepoint(name: str) -> Awaitable[None]`
  - `AsyncDuckDBClient.release_savepoint(name: str) -> Awaitable[None]`
- `src/py/vertebra/db/adapters/duckdb/config.py`:
  - `DuckDBSecretConfig.__repr__() -> str` (redacts `secret` when non-None).

## Acceptance Criteria

- [x] `execute_with_description` returns `(rows, column_names)` and preserves `column_names` even when `0` rows are returned.
- [x] `savepoint`, `rollback_to_savepoint`, and `release_savepoint` validate identifiers, require an active transaction, and support partial rollback within a transaction.
- [x] `DuckDBExceptionTypes` resolves canonical `vertebra._vertebra` exception classes and sets `.sqlstate`, `.vendor_code`, and `.is_transient` on raised exception instances.
- [x] NUL bytes in paths/strings/identifiers and `max_connections < 1` raise `InterfaceError`, missing extensions fail loudly with `OperationalError`, and secrets/tokens are redacted in `Debug`, `Display`, and `__repr__`.
- [x] `mypy.stubtest vertebra._duckdb` passes with 0 errors.

[^duckdb-rs]: `src/rs/vertebra-db/src/duckdb/mod.rs::DuckdbConfig` — DuckDB configuration and bootstrap pragma primitives in `vertebra-db`.
[^duckdb-config-py]: `src/py/vertebra/db/adapters/duckdb/config.py::DuckDBConfig` — `DuckDBConfig` immutable configuration and cgroup-clamped memory settings.
