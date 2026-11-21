---
type: Task
id: "native-cursor-data-path:network:t09"
chapter_id: network-engines
title: "Remove the superseded PostgreSQL pool query methods"
description: "Delete SyncPostgresPool.query and AsyncPostgresPool.query, which only the removed Python query path called, with their stub entries."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: characterization
depends_on:
  - "native-cursor-data-path:network:t08"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - "src/rs/vertebra-postgres/src/python_pool.rs:333-363"
  - "src/rs/vertebra-postgres/src/python_pool.rs:675-706"
  - "src/py/vertebra/_postgres.pyi:442-660"
  - "src/py/tests/unit/db/test_postgres_native_pools.py"
files:
  - "src/rs/vertebra-postgres/src/python_pool.rs"
  - "src/py/vertebra/_postgres.pyi"
tests:
  - "src/py/tests/unit/db/test_postgres_native_pools.py"
estimated_loc: 120
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
    title: "Pool classes that lose the one-shot query method"
tags: [postgres, rust, cleanup]
---

# Remove the superseded PostgreSQL pool query methods

## Objective & Scope Boundary

The pool classes expose one way to run an autocommit statement: `pool.cursor()`. The older `query` method on both pool classes is deleted because its last Python caller moved to the cursor in t08.

Out of scope:

- `SyncPostgresClient.query` and `AsyncPostgresClient.query`. Pipelines and multi-statement helpers still call them.
- The Rust `query_oneshot` pool function. The pool cursor calls it.

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `src/rs/vertebra-postgres/src/python_pool.rs:333-363` and `:675-706` are the two methods to delete.
- `src/py/vertebra/_postgres.pyi:442-660` holds the two pool class stubs.

## Contracts & Edge Cases

- First run `git grep -n 'pool.query(' -- src/py tools` and `git grep -n '_pool.query(' -- src/py tools`. If either finds a caller outside tests, stop and mark the task blocked with the path. Otherwise continue.
- Delete `query` on `PySyncPostgresPool` and `query` on `PyAsyncPostgresPool` and their stub entries.
- Tests that called the removed methods are rewritten to `pool.cursor().execute(...)` and assert the same rows. Tests whose only purpose was the removed method's return type are deleted.
- Benchmark code under `tools/benchmarks` that calls `pool.query` is out of this task's files. If the first grep finds such a call, the task is blocked as stated above and the path is reported for the cleanup chapter.
- Imports left unused in `python_pool.rs` are removed.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| Method gone | `hasattr(pool, 'query')` on both pool classes | `False` |
| Cursor path | `pool.cursor().execute('SELECT 1').fetchall()` | `[(1,)]` |
| Stub | `mypy.stubtest vertebra._postgres` | Passes with no allow-list change |

## Verification

- `make dev`
- `uv run --no-sync python -m mypy.stubtest vertebra._postgres`
- `uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_pools.py -q`
- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres python_pool`
- `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings`

## Acceptance Criteria

- [ ] Neither pool class has a `query` method in Rust or in the stub.
- [ ] `mypy.stubtest vertebra._postgres` passes.
