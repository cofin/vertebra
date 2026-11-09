---
type: Task
id: t02
flow_id: native-mssql-driver
title: "Implement MSSQL SQL batch, sp_executesql RPC, transactions, ATTENTION cancellation, and DatabaseError mapping"
description: "Implement SQL batch and sp_executesql RPC execution with @P1 parameter rewriting, execute/execute_many/fetch_all/fetch_one/fetch_optional/select_value, transaction lifecycle and savepoints, out-of-band ATTENTION cancellation, and TDS error number to DatabaseError hierarchy mapping."
status: stable
state: in_progress
verification_strategy: tdd
depends_on:
  - t01
plan_revision: 1
state_revision: 1
commit_sha: null
claimed_at: "2026-11-09T14:58:00Z"
completed_at: null
created_at: "2026-11-09T03:20:00Z"
updated_at: "2026-11-09T14:58:00Z"
reads:
  - src/rs/vertebra-mssql/src/lib.rs
  - src/rs/vertebra-mssql/src/client.rs
  - src/rs/vertebra-mssql/src/errors.rs
  - src/rs/vertebra-db/src/mssql/mod.rs
  - src/rs/vertebra-py/src/errors.rs
targets:
  - src/rs/vertebra-mssql/src/client.rs
  - src/rs/vertebra-mssql/src/errors.rs
  - src/rs/vertebra-mssql/src/params.rs
  - src/rs/vertebra-mssql/src/lib.rs
  - src/rs/vertebra-mssql/tests/tds_wire_loopback.rs
  - src/py/vertebra/_mssql.pyi
sources:
  - id: mssql-rs
    resource: src/rs/vertebra-db/src/mssql/mod.rs::rewrite_mssql_placeholders
    title: Surviving @P1 placeholder rewrite helper in vertebra-db
  - id: py-errors
    resource: src/rs/vertebra-py/src/errors.rs
    title: Python DatabaseError exception hierarchy in vertebra._vertebra
---

# Task t02: Implement MSSQL SQL batch, `sp_executesql` RPC, transactions, `ATTENTION` cancellation, and `DatabaseError` mapping

<!-- truth: start -->
- **SQL Batch & `sp_executesql` RPC**: Parameterless statements execute via TDS `SQLBatch` (`simple_query`) and parameterized statements rewrite `?`/`$1` to `@P1..@Pn` via `vertebra_db::mssql::rewrite_mssql_placeholders` [^mssql-rs] and execute via `sp_executesql` RPC (`execute`, `execute_many`, `fetch_all`, `fetch_one`, `fetch_optional`, `select_value`).
- **Transactions & Savepoints**: `begin(isolation_level=None, read_only=False)`, `commit()`, `rollback()`, `savepoint(name)`, `rollback_to_savepoint(name)`, and `release_savepoint(name)` manage TDS Transaction Manager (`0x0E`) descriptors and `SAVE TRANSACTION` / `ROLLBACK TRANSACTION <name>` semantics.
- **`ATTENTION` Cancellation & Error Mapping**: `cancel()` sends a TDS `ATTENTION` (`0x06`) packet to abort in-flight queries cleanly, and TDS `TokenError` codes (`2601`, `2627`, `547`, `515` -> `IntegrityError`; `1205` -> `SerializationFailure`; `1222`/timeout -> `QueryCancelledError`; syntax/object errors -> `ProgrammingError`; login/connection failures -> `OperationalError`) map onto the `vertebra._vertebra.DatabaseError` hierarchy [^py-errors].
<!-- truth: end -->

## Context

Builds on the `t01` `vertebra-mssql` scaffold to implement the full query execution, transaction management, out-of-band `ATTENTION` cancellation, and TDS error number mapping contract for both `SyncMSSQLClient` and `AsyncMSSQLClient`.

## Public Seam Contract

- `SyncMSSQLClient` and `AsyncMSSQLClient` methods:
  - `execute(sql, params=None) -> int`
  - `execute_many(sql, param_sets) -> int`
  - `fetch_all(sql, params=None) -> list[dict[str, Any]]`
  - `fetch_one(sql, params=None) -> dict[str, Any]`
  - `fetch_optional(sql, params=None) -> dict[str, Any] | None`
  - `select_value(sql, params=None) -> Any`
  - `begin(isolation_level=None, read_only=False) -> None`
  - `commit() -> None`, `rollback() -> None`
  - `savepoint(name) -> None`, `rollback_to_savepoint(name) -> None`, `release_savepoint(name) -> None`
  - `cancel() -> None`, `ping() -> bool`, `close() -> None`
- `map_tds_error_to_pyerr` mapping TDS error numbers to `vertebra._vertebra` exception subclasses (`IntegrityError`, `SerializationFailure`, `QueryCancelledError`, `ProgrammingError`, `DataError`, `OperationalError`, `DatabaseError`).

## Implementation Blueprint

1. Implement parameter conversion (`PyAny` -> owned `MssqlParam` implementing `tiberius::ToSql`) and `@P1..@Pn` query normalization via `vertebra_db::mssql::rewrite_mssql_placeholders`.
2. Implement `execute`, `execute_many`, `fetch_all`, `fetch_one`, `fetch_optional`, and `select_value` on `SyncMSSQLClient` and `AsyncMSSQLClient`.
3. Implement transaction lifecycle (`begin` with optional isolation level, `commit`, `rollback`, `savepoint`, `rollback_to_savepoint`, `release_savepoint` no-op per T-SQL semantics).
4. Implement out-of-band `ATTENTION` cancellation (`cancel()`) and comprehensive TDS error code -> `DatabaseError` subclass mapping.
5. Add golden wire/loopback and unit tests in `src/rs/vertebra-mssql/tests/tds_wire_loopback.rs`.

## Acceptance Criteria

- [ ] `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-mssql --locked` passes including TDS loopback tests for `PRELOGIN`/`LOGIN7`, `sp_executesql` RPC `@P1` binding, transactions, `ATTENTION` cancellation, and error mapping.
- [ ] `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-mssql --all-targets --locked -- -D warnings` passes.
- [ ] `mypy.stubtest vertebra._mssql` passes.

[^mssql-rs]: `src/rs/vertebra-db/src/mssql/mod.rs::rewrite_mssql_placeholders` — Surviving `@P1` placeholder rewrite helper in vertebra-db.
[^py-errors]: `src/rs/vertebra-py/src/errors.rs` — Python DatabaseError exception hierarchy in vertebra._vertebra.
