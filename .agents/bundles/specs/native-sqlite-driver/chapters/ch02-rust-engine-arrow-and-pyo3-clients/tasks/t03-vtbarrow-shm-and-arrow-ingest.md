---
type: Task
id: native-sqlite-driver:ch02-rust-engine-arrow-and-pyo3-clients:t03
flow_id: native-sqlite-driver
chapter_id: ch02-rust-engine-arrow-and-pyo3-clients
title: Implement VTBARROW Arrow IPC Streaming and Bulk Arrow Table Ingestion
description: Stream SQLite result sets into VTBARROW Arrow IPC frames with ShmArena spill and ingest Arrow IPC streams via prepared transactions.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 104fd6fa
depends_on:
- native-sqlite-driver:ch02-rust-engine-arrow-and-pyo3-clients:t02
files:
- src/rs/vertebra-sqlite/src/arrow_bridge.rs
- src/rs/vertebra-sqlite/src/worker.rs
tests:
- src/rs/vertebra-sqlite/tests/sqlite_client.rs
read_set:
- src/rs/vertebra-db/src/arrow_shm.rs
- src/rs/vertebra-duckdb/src/lib.rs
- src/rs/vertebra-sqlite/src/worker.rs
estimated_loc: 235
tags:
- sqlite
- arrow
- shm
sources:
- id: arrow-shm
  resource: src/rs/vertebra-db/src/arrow_shm.rs
  title: ShmArrowStreamWriter and VTBARROW ShmCapsule constructor
- id: duckdb-lib
  resource: src/rs/vertebra-duckdb/src/lib.rs
  title: Embedded worker-thread and VTBARROW PyO3 extension precedent
---

# Task `t03`: Implement VTBARROW Arrow IPC Streaming and Bulk Arrow Table Ingestion

## Objective & Scope Boundary

Implement `arrow_bridge.rs` for zero-copy `VTBARROW` `ShmCapsule` `select_to_arrow` and transactional prepared `load_from_arrow` per [`../spec.md` §2](../spec.md) [^arrow-shm] [^duckdb-lib].

## Bounded Read Manifest

- `src/rs/vertebra-db/src/arrow_shm.rs` — `ShmArrowStreamWriter` and `construct_py_arrow_cursor`.
- `src/rs/vertebra-duckdb/src/lib.rs` — Arrow IPC batch streaming and ingestion.
- `src/rs/vertebra-sqlite/src/worker.rs` — Worker statement execution loop from `t01`.

## Contracts & Edge Cases

- Infer `arrow_schema::Schema` (`Int64`, `Float64`, `Boolean`, `Utf8`, `Binary`) from declared column types + first-row `ValueRef`, chunk at `batch_size` (`8192`), and write into `ShmArrowStreamWriter`.
- Ingest Arrow IPC stream bytes in `load_from_arrow` inside a single writer transaction / savepoint with quoted identifiers.
- **Edge Case**: 0-row queries emit a valid empty `ArrowCursor` preserving column names and declared schema.
- **Edge Case**: Null values across all scalar/binary affinities preserve Arrow validity bitmaps.

## Test Matrix

- Round-trip 20,000 multi-type rows (`Int64`, `Float64`, `Boolean`, `Utf8`, `Binary`, `NULL`) via `load_from_arrow` -> `select_to_arrow` and verify zero-copy `VTBARROW` header.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-sqlite
```

## Acceptance Criteria

- [ ] `select_to_arrow` builds `VTBARROW` SHM regions directly from `rusqlite::Row::get_ref` without Python tuple allocation.
- [ ] Zero regressions in the fast verification gate.

[^arrow-shm]: `src/rs/vertebra-db/src/arrow_shm.rs` — ShmArrowStreamWriter and VTBARROW ShmCapsule constructor.
[^duckdb-lib]: `src/rs/vertebra-duckdb/src/lib.rs` — Embedded worker-thread and VTBARROW PyO3 extension precedent.
