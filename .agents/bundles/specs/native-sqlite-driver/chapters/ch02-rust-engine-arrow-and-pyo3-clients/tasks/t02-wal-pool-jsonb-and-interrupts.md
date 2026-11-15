---
type: Task
id: native-sqlite-driver:ch02-rust-engine-arrow-and-pyo3-clients:t02
flow_id: native-sqlite-driver
chapter_id: ch02-rust-engine-arrow-and-pyo3-clients
title: Implement 1-Writer/N-Reader WAL Pool, JSONB, Backup API, and Interrupt Registry
description: Build dedicated writer/reader worker threads, JSONB blob detection, online backup/restore, and InterruptHandle cancellation.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 473e1f2e
depends_on:
  - native-sqlite-driver:ch02-rust-engine-arrow-and-pyo3-clients:t01
files:
  - src/rs/vertebra-sqlite/src/value.rs
  - src/rs/vertebra-sqlite/src/worker.rs
  - src/rs/vertebra-sqlite/src/lib.rs
tests:
  - src/rs/vertebra-sqlite/tests/sqlite_client.rs
read_set:
  - src/rs/vertebra-sqlite/src/config.rs
  - src/rs/vertebra-sqlite/src/errors.rs
  - src/rs/vertebra-duckdb/src/lib.rs
estimated_loc: 245
tags:
  - sqlite
  - wal
  - jsonb
  - backup
sources:
  - id: duckdb-lib
    resource: src/rs/vertebra-duckdb/src/lib.rs
    title: Embedded worker-thread and VTBARROW PyO3 extension precedent
  - id: sqlite-rs
    resource: src/rs/vertebra-db/src/sqlite/mod.rs::SqliteConfig
    title: Shared SQLite pragma helper in vertebra-db
verification_strategy: regression_tdd
---

# Task `t02`: Implement 1-Writer/N-Reader WAL Pool, JSONB, Backup API, and Interrupt Registry

## Objective & Scope Boundary

Implement `value.rs` and `worker.rs` in `vertebra-sqlite` covering the WAL writer + reader pool, SQLite 3.45+ `JSONB`, `SAVEPOINT` nesting, `InterruptHandle` cancellation, and the `rusqlite` online `Backup` API snapshots per [`../spec.md` §2](../spec.md) [^duckdb-lib] [^sqlite-rs].

## Bounded Read Manifest

- `src/rs/vertebra-sqlite/src/config.rs` — `SqliteClientConfig` and pragma SQL.
- `src/rs/vertebra-sqlite/src/errors.rs` — Extended `rusqlite` error mapping.
- `src/rs/vertebra-duckdb/src/lib.rs` — Worker thread lifecycle and shutdown channel pattern.

## Contracts & Edge Cases

- Open 1 writer + `max_readers` read-only WAL workers for file/shared-URI paths and 1 serialized worker for `:memory:`, caching prepared statements per worker.
- Step `RETURNING` statements to completion, round-trip `JSONB` blobs and `BOOLEAN`/`TIMESTAMP` declared affinities, track per-op `InterruptHandle`, and expose `backup_to` / `restore_from` via the `rusqlite` online `Backup` API.
- **Edge Case**: Idle `cancel()` is an idempotent no-op; active `cancel()` interrupts only the targeted in-flight operation.
- **Edge Case**: `execute_pipeline` runs batched operations atomically inside `BEGIN IMMEDIATE` or a nested `SAVEPOINT`.

## Test Matrix

- Concurrent WAL readers execute without blocking while writer commits; `:memory:` DDL/DML is visible across subsequent `fetch_all` calls.
- `backup_to` produces a byte-consistent snapshot of a live WAL database and `restore_from` hydrates a target database.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-sqlite
```

## Acceptance Criteria

- [ ] WAL worker pool, `JSONB`, `SAVEPOINT`, `InterruptHandle`, and `backup_to`/`restore_from` pass Rust tests.
- [ ] Zero regressions in the fast verification gate.

[^duckdb-lib]: `src/rs/vertebra-duckdb/src/lib.rs` — Embedded worker-thread and VTBARROW PyO3 extension precedent.
[^sqlite-rs]: `src/rs/vertebra-db/src/sqlite/mod.rs::SqliteConfig` — Shared SQLite pragma helper in vertebra-db.
