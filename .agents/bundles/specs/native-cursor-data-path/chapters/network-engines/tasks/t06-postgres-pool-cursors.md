---
type: Task
id: "native-cursor-data-path:network:t06"
chapter_id: network-engines
title: "Pool-backed PostgreSQL cursors over the one-shot path"
description: "SyncPostgresPool.cursor() and AsyncPostgresPool.cursor() return cursors whose execute runs on the pooled one-shot query path."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:network:t05"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - "src/rs/vertebra-postgres/src/python_pool.rs:180-200"
  - "src/rs/vertebra-postgres/src/python_pool.rs:321-363"
  - "src/rs/vertebra-postgres/src/python_pool.rs:521-535"
  - "src/rs/vertebra-postgres/src/python_pool.rs:656-706"
  - "src/rs/vertebra-postgres/src/python_pool.rs:860-867"
  - "src/rs/vertebra-postgres/src/cursor.rs"
  - "src/py/vertebra/_postgres.pyi:442-462"
  - "src/py/vertebra/_postgres.pyi:552-572"
files:
  - "src/rs/vertebra-postgres/src/cursor.rs"
  - "src/rs/vertebra-postgres/src/python_pool.rs"
  - "src/py/vertebra/_postgres.pyi"
tests:
  - "src/py/tests/unit/db/test_postgres_native_pools.py"
estimated_loc: 200
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
created_at: 2026-11-20T15:30:00Z
commit: null
sources:
  - id: chapter-spec
    resource: .agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md
    title: "Chapter contract for the network engine cursors"
  - id: pg-pool
    resource: "src/rs/vertebra-postgres/src/python_pool.rs::PySyncPostgresPool"
    title: "Pool with the one-shot query path"
tags: [postgres, rust, pool, performance]
---

# Pool-backed PostgreSQL cursors over the one-shot path

## Objective & Scope Boundary

Autocommit statements run through a cursor without a Python-visible connection checkout. `pool.cursor().execute(sql, params)` uses the pool's one-shot path, which is the path the PostgreSQL acceptance rows are measured on.

Out of scope:

- Python driver changes (t07, t08).
- Removing `pool.query`. Task t09 removes it after its last Python caller is gone.
- Broker leases. A pool-backed cursor never takes one; see the lease contract in the chapter spec.

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `src/rs/vertebra-postgres/src/python_pool.rs:333-363` is the sync one-shot `query`. `:675-706` is the async form.
- `src/rs/vertebra-postgres/src/python_pool.rs:321-332` and `:656-674` are `acquire`, used by `executemany`.
- `src/rs/vertebra-postgres/src/cursor.rs` after t05 holds both cursor classes.

## Contracts & Edge Cases

- Both pool `cursor` methods take keyword-only `lazy_begin: bool = False`. A pool cursor has no session, so `lazy_begin=True` raises `InterfaceError` with the message `a pool cursor has no session; lazy_begin needs a client cursor`. Test row: that call raises and no checkout happens.
- Both cursor classes hold `enum CursorSource { Session(...), Pool(Arc<...>) }` in place of the bare session handle.
- `SyncPostgresPool.cursor(self) -> SyncPostgresCursor` and `AsyncPostgresPool.cursor(self) -> AsyncPostgresCursor` take no arguments and do no I/O.
- With `Pool` on `CursorSource`, `execute` calls `pool.query_oneshot` with the same parameters and budget as the one-shot `query` method.
- With `Pool` on `CursorSource`, `executemany` acquires one pooled connection, runs every parameter set on it, and returns it. On error the connection is evicted the way the pool evicts a failed one-shot connection.
- Results, errors, and edge cases are the same as for a session-backed cursor.
- `execute` on a cursor whose pool is closed raises `InterfaceError`.
- Stub: add `def cursor(self) -> SyncPostgresCursor: ...` and `def cursor(self) -> AsyncPostgresCursor: ...` on the two pool classes.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| One-shot select | `pool.cursor().execute('SELECT 1').fetchall()` | `[(1,)]` |
| Connection returned | 100 sequential executes on a pool of size 1 | All succeed |
| executemany one connection | Three sets inserting `pg_backend_pid()` | One distinct backend pid |
| Error evicts | Statement that fails, then a select | Select succeeds |
| Closed pool | `pool.close()` then `execute` | `InterfaceError` |
| Async one-shot | `await pool.cursor().execute(...)` | Same results as sync |

## Verification

- `make dev`
- `uv run --no-sync python -m mypy.stubtest vertebra._postgres`
- `uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_pools.py -q`
- `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings`

## Acceptance Criteria

- [ ] New tests named `test_pool_cursor_*` cover every matrix case for both pool classes.
- [ ] The stub diff adds two methods and no class.
