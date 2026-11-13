---
type: Spec
flow_id: native-sqlite-driver
chapter_id: ch02-wheel-isolation-and-core-config
title: "Chapter 02: Sibling Wheel Payload Isolation, Client Config, and Extended Error Mapping"
description: "Exclude _sqlite.pyi across all 9 sibling PyO3 wheels, expand optional driver wheel tests, and implement Rust SqliteClientConfig, WAL pragma initialization, and extended SQLite error mapping."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
updated_at: "2026-11-12T23:45:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:45:00Z"
    commit: 18baadcd
tags:
  - sqlite
  - wheel
  - errors
  - pragmas
sources:
  - id: duckdb-pyproj
    resource: src/rs/vertebra-duckdb/pyproject.toml
    title: "Maturin standalone wheel isolation manifest precedent"
  - id: wheel-tests
    resource: src/py/tests/unit/db/test_optional_driver_wheels.py
    title: "Sibling PyO3 wheel isolation test gate"
  - id: sqlite-rs
    resource: src/rs/vertebra-db/src/sqlite/mod.rs::SqliteConfig
    title: "Shared SQLite pragma helper in vertebra-db"
  - id: vertebra-py-db
    resource: src/rs/vertebra-py/src/db.rs::sqlite_initialization_pragmas
    title: "Legacy sqlite_initialization_pragmas binding in vertebra-py"
---

# Chapter 02: Sibling Wheel Payload Isolation, Client Config, and Extended Error Mapping

<!-- truth: start -->
- Every sibling maturin `pyproject.toml` (`bigquery`, `bundler`, `duckdb`, `mssql`, `mysql`, `oracle`, `postgres`, `spanner`, `storage`) excludes `"**/py/vertebra/_sqlite.pyi"` [^duckdb-pyproj] [^wheel-tests].
- `test_optional_driver_wheels.py` enforces reciprocal `.pyi` isolation across `_oracle.pyi`, `_postgres.pyi`, and `_sqlite.pyi` [^wheel-tests].
- `SqliteClientConfig` computes WAL, `synchronous`, `mmap_size`, `busy_timeout`, `temp_store = MEMORY`, and `foreign_keys` pragmas and detects `:memory:` vs shared-URI vs file paths [^sqlite-rs].
- `errors.rs` maps extended `rusqlite::ffi::Error` codes (`2067`, `1555`, `2579`, `787`, `1299`, `275`, `5`, `6`, `9`, `1`, `20`) to typed `vertebra._vertebra.DatabaseError` subclasses with `vendor_code`, `sqlstate`, and `is_transient` [^vertebra-py-db].
<!-- truth: end -->

## 1. Specification, Context & Provenance

Maturin workspace wheels share `src/py` as `python-source`, so every sibling wheel must exclude `_sqlite.pyi` to avoid file collisions on installation [^duckdb-pyproj] [^wheel-tests]. Inside `vertebra-sqlite`, `config.rs` and `errors.rs` provide deterministic WAL pragma generation and extended SQLite constraint/transient error classification before worker threads land in `ch03`.

**Non-Goals:** Worker thread pool and `VTBARROW` builder (`ch03`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Wheel isolation | `src/rs/vertebra-*/pyproject.toml`, `test_optional_driver_wheels.py` [^wheel-tests] | Disjoint `.pyi` wheel archives |
| SQLite config & errors | `src/rs/vertebra-sqlite/src/{config,errors,lib}.rs` [^sqlite-rs] | `SqliteClientConfig` + extended FFI code -> `DatabaseError` |

**Knowledge Anchors:** [ADR-004 Optional Driver Wheels and SHM Ownership](../../../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md), [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R5a**: Exclude `_sqlite.pyi` in `bigquery`, `bundler`, `duckdb`, `mssql`, `mysql`, `oracle`, `postgres`, `spanner`, and `storage` `pyproject.toml` manifests and enforce in `test_optional_driver_wheels.py` (`t01`–`t03`).
- **R5b**: Implement `SqliteClientConfig` and extended `rusqlite::Error` -> `vertebra._vertebra` exception mapping (`UniqueViolationError`, `ForeignKeyViolationError`, `NotNullViolationError`, `CheckViolationError`, `OperationalError` `is_transient`, `ProgrammingError`, `DataError`) (`t04`).

```rust
pub struct SqliteClientConfig {
    pub path: String,
    pub wal_mode: bool,
    pub synchronous: SqliteSynchronous,
    pub mmap_size: u64,
    pub busy_timeout_ms: u32,
    pub max_readers: usize,
    pub foreign_keys: bool,
    pub enable_jsonb: bool,
}
// Extended SQLite error codes -> (Python exception class, sqlstate, is_transient):
// 2067 | 1555 | 2579 -> UniqueViolationError ("23505", false)
// 787                -> ForeignKeyViolationError ("23503", false)
// 1299               -> NotNullViolationError ("23502", false)
// 275                -> CheckViolationError ("23514", false)
// 5 | 261 | 517 | 6  -> OperationalError ("55P03", true)
// 9 (SQLITE_INTERRUPT)-> OperationalError ("57014", true)
```

**Integration Flow:**
- `make sqlite-wheel-check` + `pytest test_optional_driver_wheels.py` validate wheel payload boundaries.
- `worker.rs` (`ch03`) consumes `SqliteClientConfig::initialization_pragmas()` and `map_rusqlite_error(py, err)`.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Invalid synchronous mode | `synchronous = "BOGUS"` | Raises `ValueError` immediately at client construction | SQL injection-free pragma validation |
| Constraint violation | Extended code `2067` (`SQLITE_CONSTRAINT_UNIQUE`) | `UniqueViolationError` (`vendor_code=2067`, `sqlstate="23505"`) | Dialect-neutral ORM error handling |
| Busy / interrupt | `SQLITE_BUSY` (`5`) or `SQLITE_INTERRUPT` (`9`) | `OperationalError` (`is_transient=True`) | Retryable lock/cancel classification |

## 3. Documentation & OKF Knowledge Plan

Pragma defaults and error mappings are documented in `ch05`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R5a` | `t01`–`t03` | `src/rs/vertebra-*/pyproject.toml`, `test_optional_driver_wheels.py` | `make sqlite-wheel-check && pytest test_optional_driver_wheels.py` |
| `R5b` | `t04` | `src/rs/vertebra-sqlite/src/{config,errors,lib}.rs` | `cargo test -p vertebra-sqlite` |

## 5. Implementation Plan

- [ ] [t01-sibling-exclusions-part1.md](tasks/t01-sibling-exclusions-part1.md) — Add **/py/vertebra/_sqlite.pyi to [tool.maturin].exclude in bigquery, bundler, and duckdb pyproject.toml files.
- [ ] [t02-sibling-exclusions-part2.md](tasks/t02-sibling-exclusions-part2.md) — Add **/py/vertebra/_sqlite.pyi to [tool.maturin].exclude in mssql, mysql, and oracle pyproject.toml files.
- [ ] [t03-sibling-exclusions-part3-and-wheel-gate.md](tasks/t03-sibling-exclusions-part3-and-wheel-gate.md) — Exclude _sqlite.pyi in postgres, spanner, and storage and enforce oracle/postgres/sqlite wheel isolation in test_optional_driver_wheels.py.
- [ ] [t04-sqlite-config-pragmas-and-errors.md](tasks/t04-sqlite-config-pragmas-and-errors.md) — Implement SqliteClientConfig, WAL/pragma builder, :memory:/URI detection, and extended rusqlite error mapping.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`18baadcd`)**: `state: planned`; `0/4` tasks completed.
- **Active Invariants**: Zero sibling wheels ship `_sqlite.pyi`; every extended SQLite constraint code maps to its specific `IntegrityError` subclass.
- **Fast Chapter Gate (`< 60s`)**: `make sqlite-wheel-check && uv run --no-sync pytest src/py/tests/unit/db/test_optional_driver_wheels.py && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-sqlite`

[^duckdb-pyproj]: `src/rs/vertebra-duckdb/pyproject.toml` — Maturin standalone wheel isolation manifest precedent.
[^wheel-tests]: `src/py/tests/unit/db/test_optional_driver_wheels.py` — Sibling PyO3 wheel isolation test gate.
[^sqlite-rs]: `src/rs/vertebra-db/src/sqlite/mod.rs::SqliteConfig` — Shared SQLite pragma helper in vertebra-db.
[^vertebra-py-db]: `src/rs/vertebra-py/src/db.rs::sqlite_initialization_pragmas` — Legacy sqlite_initialization_pragmas binding in vertebra-py.
