---
type: Task
id: spanner-queue-and-rw-transactions:t02
flow_id: spanner-queue-and-rw-transactions
title: Wire SyncSpannerDriver / AsyncSpannerDriver Interactive Transactions & Aborted Closure Retries
description: Route SyncSpannerTransaction, AsyncSpannerTransaction, run_in_transaction, and transaction() through interactive ReadWrite handles with SpannerAbortedError retry backoff.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 1b191388
depends_on:
- spanner-queue-and-rw-transactions:t01
files:
- src/py/vertebra/db/adapters/spanner/driver.py
- src/py/vertebra/db/adapters/spanner/__init__.py
tests:
- src/py/tests/unit/db/test_spanner.py
read_set:
- src/py/vertebra/db/adapters/spanner/driver.py
- src/py/vertebra/db/adapters/spanner/__init__.py
- src/py/vertebra/_spanner.pyi
estimated_loc: 225
tags:
- spanner
- driver
- transactions
sources:
- id: spn-driver
  resource: src/py/vertebra/db/adapters/spanner/driver.py::SyncSpannerTransaction
  title: Current SyncSpannerTransaction and AsyncSpannerTransaction delegating to non-atomic single-shot RPCs
- id: spn-init
  resource: src/py/vertebra/db/adapters/spanner/__init__.py
  title: Public Spanner adapter barrel
---

# Task `t02`: Wire SyncSpannerDriver / AsyncSpannerDriver Interactive Transactions & Aborted Closure Retries

## Objective & Scope Boundary

Wire `SyncSpannerTransaction`, `AsyncSpannerTransaction`, `run_in_transaction`, and `transaction()` context managers in `src/py/vertebra/db/adapters/spanner/driver.py` onto interactive `ReadWrite` handles per [`../spec.md`](spec.md) [^spn-driver] [^spn-init].

## Bounded Read Manifest

- `src/py/vertebra/db/adapters/spanner/driver.py` — `SyncSpannerTransaction`, `AsyncSpannerTransaction`, `run_in_transaction`, and `transaction()`.
- `src/py/vertebra/_spanner.pyi` — `SyncSpannerReadWriteTransaction` and `AsyncSpannerReadWriteTransaction`.

## Contracts & Edge Cases

- Bind `SyncSpannerTransaction` and `AsyncSpannerTransaction` (`select*`, `execute`, `execute_batch_dml`, `buffer_mutation`) to one underlying `SyncSpannerReadWriteTransaction` / `AsyncSpannerReadWriteTransaction` handle instead of independent `driver.select` / `driver.execute` RPCs.
- `run_in_transaction(fn, *, max_attempts=..., transaction_tag=...)` commits buffered mutations with the active transaction on closure return, issues `rollback()` on unhandled exception, and retries `fn(tx)` from a fresh handle when `SpannerAbortedError` is raised—sleeping for server `retry_delay_ms` (or bounded jittered exponential backoff when absent) and passing `previous_transaction_id` to preserve wound-wait priority.
- `with driver.transaction():` / `async with driver.transaction():` opens a single-attempt interactive `ReadWrite` context manager (committing on clean exit, rolling back on error, propagating `SpannerAbortedError` directly because a `with` body cannot be re-entered automatically).

## Test Matrix

- Parametrized sync/async tests proving `tx.select('... FOR UPDATE')` + `tx.execute('UPDATE ...')` + `tx.insert(...)` share one `transaction_id` and `commit()`, unhandled errors trigger `rollback()` without committing prior DML, and `SpannerAbortedError` retries `run_in_transaction(fn)` honoring `retry_delay_ms` and forwarding `previous_transaction_id`.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-spanner && uv run --no-sync ruff check src/py/vertebra/_spanner.pyi src/py/vertebra/db/adapters/spanner && uv run --no-sync mypy src/py/vertebra/db/adapters/spanner && uv run --no-sync pytest src/py/tests/unit/db/test_spanner.py
```

## Acceptance Criteria

- [ ] `SyncSpannerDriver.run_in_transaction` and `AsyncSpannerDriver.run_in_transaction` execute multi-statement reads, DML, and mutations atomically and retry `SpannerAbortedError` with server backoff.
- [ ] Zero regressions in the fast verification gate.

[^spn-driver]: `src/py/vertebra/db/adapters/spanner/driver.py::SyncSpannerTransaction` — Current SyncSpannerTransaction and AsyncSpannerTransaction delegating to non-atomic single-shot RPCs.
[^spn-init]: `src/py/vertebra/db/adapters/spanner/__init__.py` — Public Spanner adapter barrel.
