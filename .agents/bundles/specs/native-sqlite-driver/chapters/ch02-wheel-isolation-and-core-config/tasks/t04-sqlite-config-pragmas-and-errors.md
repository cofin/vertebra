---
type: Task
id: "native-sqlite-driver:ch02:t04"
flow_id: native-sqlite-driver
chapter_id: ch02-wheel-isolation-and-core-config
title: "Rust SqliteClientConfig, Topology Detection, and Extended Error Mapping"
description: "Implement SqliteClientConfig, WAL/pragma builder, :memory:/URI detection, and extended rusqlite error mapping."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "native-sqlite-driver:ch01:t02"
files:
  - src/rs/vertebra-sqlite/src/config.rs
  - src/rs/vertebra-sqlite/src/errors.rs
  - src/rs/vertebra-sqlite/src/lib.rs
tests:
  - src/rs/vertebra-sqlite/tests/sqlite_client.rs
read_set:
  - src/rs/vertebra-db/src/sqlite/mod.rs
  - src/rs/vertebra-py/src/db.rs
  - src/rs/vertebra-duckdb/src/lib.rs
estimated_loc: 225
tags:
  - sqlite
  - pragmas
  - errors
sources:
  - id: sqlite-rs
    resource: src/rs/vertebra-db/src/sqlite/mod.rs::SqliteConfig
    title: "Shared SQLite pragma helper in vertebra-db"
  - id: vertebra-py-db
    resource: src/rs/vertebra-py/src/db.rs::sqlite_initialization_pragmas
    title: "Legacy sqlite_initialization_pragmas binding in vertebra-py"
  - id: duckdb-lib
    resource: src/rs/vertebra-duckdb/src/lib.rs
    title: "Embedded worker-thread and VTBARROW PyO3 extension precedent"
---

# Task `t04`: Rust SqliteClientConfig, Topology Detection, and Extended Error Mapping

## Objective & Scope Boundary

Implement `SqliteClientConfig` (`config.rs`) and extended SQLite error mapping (`errors.rs`) per [`../spec.md` §2](../spec.md) [^sqlite-rs] [^vertebra-py-db] [^duckdb-lib].

## Bounded Read Manifest

- `src/rs/vertebra-db/src/sqlite/mod.rs` — `SqliteConfig::initialization_pragmas`.
- `src/rs/vertebra-py/src/db.rs` — Database exception construction helpers.
- `src/rs/vertebra-duckdb/src/lib.rs` — Error translation precedent.

## Contracts & Edge Cases

- Validate `synchronous` in `{"OFF", "NORMAL", "FULL", "EXTRA"}`, `max_readers >= 1`, and distinguish private `:memory:` (single worker) vs shared URI (`file:...?mode=memory&cache=shared`) vs filesystem WAL.
- Map `rusqlite::Error::SqliteFailure(ffi_err, msg)` extended codes (`2067`, `1555`, `2579`, `787`, `1299`, `275`, `5`, `6`, `9`, `1`, `20`) to typed `vertebra._vertebra` `DatabaseError` subclasses with `vendor_code`, `sqlstate`, and `is_transient`.
- **Edge Case**: WAL pragma (`PRAGMA journal_mode = WAL`) is omitted on plain `:memory:` while `foreign_keys`, `busy_timeout`, and `temp_store = MEMORY` still apply.

## Test Matrix

- Rust tests verify pragma lists across file/URI/`:memory:`, reject invalid `synchronous`, and verify every extended error code classification.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-sqlite
```

## Acceptance Criteria

- [ ] `SqliteClientConfig` and `errors.rs` cover all pragma and extended SQLite error contracts.
- [ ] Zero regressions in the chapter fast verification gate.

[^sqlite-rs]: `src/rs/vertebra-db/src/sqlite/mod.rs::SqliteConfig` — Shared SQLite pragma helper in vertebra-db.
[^vertebra-py-db]: `src/rs/vertebra-py/src/db.rs::sqlite_initialization_pragmas` — Legacy sqlite_initialization_pragmas binding in vertebra-py.
[^duckdb-lib]: `src/rs/vertebra-duckdb/src/lib.rs` — Embedded worker-thread and VTBARROW PyO3 extension precedent.
