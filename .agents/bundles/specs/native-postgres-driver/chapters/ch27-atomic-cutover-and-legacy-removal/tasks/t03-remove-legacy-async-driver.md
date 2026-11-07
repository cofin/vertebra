---
type: Task
id: "native-postgres-driver:ch27:t03"
chapter_id: ch27-atomic-cutover-and-legacy-removal
title: Remove unreachable _LegacyAsyncPostgreSQLDriver implementation from driver.py
description: Delete the unreferenced private _LegacyAsyncPostgreSQLDriver class and its unused async/legacy imports from vertebra.db.adapters.postgres.driver while preserving canonical native behavior under characterization gates.
status: stable
state: pending
priority: P1
verification_strategy: characterization
depends_on:
  - "native-postgres-driver:ch27:t02"
read_set:
  - src/py/vertebra/db/adapters/postgres/driver.py
  - src/py/vertebra/db/adapters/postgres/__init__.py
  - src/py/tests/unit/db/test_postgres_driver.py
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - src/py/vertebra/db/adapters/postgres/driver.py
tests:
  - src/py/tests/unit/db/test_postgres_driver.py
  - src/py/tests/unit/db/test_postgres_native_adapters.py
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
estimated_loc: 200
plan_revision: 3
plan_commit: dd432f18
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-07T16:45:00Z"
commit: null
sources:
  - id: pg-driver-mod
    resource: src/py/vertebra/db/adapters/postgres/driver.py::AsyncPostgreSQLDriver
    title: Public PostgreSQL driver module containing temporary _LegacyAsyncPostgreSQLDriver remnant
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Final activation and dead-body removal invariants
tags:
  - postgres
  - cutover
  - cleanup
  - async
---

# Task t03: Remove Unreachable `_LegacyAsyncPostgreSQLDriver` Implementation from `driver.py`

## Objective & Scope Boundary

Delete the unreachable private `_LegacyAsyncPostgreSQLDriver` class and all imports referenced exclusively by it (`asyncio`, `inspect`, `anyio.CancelScope`, `AsyncIterator`, `Awaitable`, `AsyncDatabaseDriver`, etc.) from `src/py/vertebra/db/adapters/postgres/driver.py` after `ch27:t01`–`t02` complete public cutover [^pg-driver-mod] [^pg-contracts].

Out of scope: Deleting `_LegacySyncPostgreSQLDriver` (`ch27:t04`) or modifying `_async.py`, `_sync.py`, `core.py`, or any test file.

## Bounded Read Manifest

- [src/py/vertebra/db/adapters/postgres/driver.py](../../../../../../../src/py/vertebra/db/adapters/postgres/driver.py) — Barrel exports plus `_LegacyAsyncPostgreSQLDriver` body slated for deletion [^pg-driver-mod].
- [src/py/vertebra/db/adapters/postgres/__init__.py](../../../../../../../src/py/vertebra/db/adapters/postgres/__init__.py) — Package export map confirming zero `_LegacyAsyncPostgreSQLDriver` references.
- [src/py/tests/unit/db/test_postgres_driver.py](../../../../../../../src/py/tests/unit/db/test_postgres_driver.py) — Canonical PostgreSQL unit suite used as characterization baseline.
- [src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py](../../../../../../../src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py) — SQLSpec and SQLAlchemy PostgreSQL regression suite.
- [.agents/bundles/specs/native-postgres-driver/contracts.md](../../../contracts.md) — Delivered barrel cleanliness contract [^pg-contracts].

## Symbol & Signature Contracts

- `src/py/vertebra/db/adapters/postgres/driver.py`:
  - Retain `from vertebra.db.adapters.postgres._async import AsyncPostgreSQLDriver`, `from vertebra.db.adapters.postgres._sync import SyncPostgreSQLDriver`, `__all__ = ("AsyncPostgreSQLDriver", "SyncPostgreSQLDriver")`, and temporary `_LegacySyncPostgreSQLDriver` (plus only the imports still needed by `_LegacySyncPostgreSQLDriver` until `ch27:t04`).
  - Zero references to `_LegacyAsyncPostgreSQLDriver`, `_async_executor`, or `import_optional("asyncpg")` remain anywhere in `src/py/vertebra/`.

## Edge Cases & Error Semantics

- Removing `_LegacyAsyncPostgreSQLDriver` must not alter `AsyncPostgreSQLDriver` or `SyncPostgreSQLDriver` class identity, MRO, `__module__`, or runtime error mapping.
- All characterization tests must pass unchanged before and after deletion without relaxing assertions.

## Public-Seam Test Blueprint

- **Canonical characterization files**: `src/py/tests/unit/db/test_postgres_driver.py`, `src/py/tests/unit/db/test_postgres_native_adapters.py`, `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Public barrel & package symbol check | `import vertebra.db.adapters.postgres.driver as drv; hasattr(drv, "_LegacyAsyncPostgreSQLDriver")` | `False`, while `drv.AsyncPostgreSQLDriver` and `drv.SyncPostgreSQLDriver` remain bound to `._async` and `._sync`. |
| Full PostgreSQL unit & extension characterization | Run pre-deletion and post-deletion pytest + Ruff + Mypy across PostgreSQL adapter, pipeline, Arrow, event, SQLSpec, and SQLAlchemy suites | Identical `0`-exit pass counts with zero unused-import warnings (`F401`) or Mypy errors. |

## Verification

- Pre-deletion characterization baseline: `uv run --no-sync pytest src/py/tests/unit/db/test_postgres_driver.py src/py/tests/unit/db/test_postgres_native_adapters.py src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py src/py/tests/unit/db/test_postgres_native_adapter_arrow.py src/py/tests/unit/db/test_postgres_event_hooks.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py -q`
- Post-deletion characterization + lint + typecheck gate: `uv run --no-sync pytest src/py/tests/unit/db/test_postgres_driver.py src/py/tests/unit/db/test_postgres_native_adapters.py src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py src/py/tests/unit/db/test_postgres_native_adapter_arrow.py src/py/tests/unit/db/test_postgres_event_hooks.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py -q && uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres && uv run --no-sync mypy src/py/vertebra/db/adapters/postgres src/py/vertebra/extensions`

## Acceptance Criteria

- [ ] `_LegacyAsyncPostgreSQLDriver` and all imports used exclusively by it are deleted from `src/py/vertebra/db/adapters/postgres/driver.py` (R24).
- [ ] Baseline and post-deletion characterization, Ruff, and Mypy commands pass with zero test modifications or behavioral drift (R24).

## Notes & Discoveries

- [2026-11-07 16:45] Leave `_LegacySyncPostgreSQLDriver` untouched in this commit so `ch27:t04` removes it cleanly in its own bounded characterization commit.

[^pg-driver-mod]: `src/py/vertebra/db/adapters/postgres/driver.py::AsyncPostgreSQLDriver` — Public PostgreSQL driver module containing temporary `_LegacyAsyncPostgreSQLDriver` remnant.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Final activation and dead-body removal invariants.
