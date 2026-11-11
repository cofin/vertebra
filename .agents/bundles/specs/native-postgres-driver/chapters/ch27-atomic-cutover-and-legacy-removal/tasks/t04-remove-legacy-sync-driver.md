---
type: Task
id: "native-postgres-driver:ch27:t04"
chapter_id: ch27-atomic-cutover-and-legacy-removal
title: Remove unreachable _LegacySyncPostgreSQLDriver and update PostgreSQLQueueBackend dependency guard
description: Delete _LegacySyncPostgreSQLDriver and import asyncio from driver.py leaving a minimal public barrel, and switch PostgreSQLQueueBackend.__init__ in framework/queues/postgres.py from psycopg to vertebra._postgres.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch27:t03"
read_set:
  - src/py/vertebra/db/adapters/postgres/driver.py
  - src/py/vertebra/db/adapters/postgres/core.py
  - src/py/vertebra/db/adapters/postgres/__init__.py
  - src/py/vertebra/framework/queues/postgres.py
  - src/py/tests/unit/framework/test_distributed_queues.py
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - src/py/vertebra/db/adapters/postgres/driver.py
  - src/py/vertebra/framework/queues/postgres.py
tests:
  - src/py/tests/unit/db/test_postgres_driver.py
  - src/py/tests/unit/db/test_postgres_native_adapters.py
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
  - src/py/tests/unit/framework/test_distributed_queues.py
estimated_loc: 230
plan_revision: 6
plan_commit: 8e9b324e
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
updated_at: "2026-11-10T14:45:00Z"
commit: null
verified:
  - by: agent:plan-generator
    at: "2026-11-10T14:45:00Z"
    commit: 8e9b324e
sources:
  - id: pg-driver-sync
    resource: src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver
    title: Public PostgreSQL driver barrel module targeted for final legacy sync and import asyncio removal
  - id: pg-queues-py
    resource: src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend
    title: PostgreSQLQueueBackend.__init__ dependency and executor guard at lines 147-159
  - id: pg-core-py
    resource: src/py/vertebra/db/adapters/postgres/core.py::PostgreSQLCore
    title: Retained PostgreSQL CPU codecs and tenant SQL helpers
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Final barrel cleanliness, vertebra-core[postgres] dependency metadata, and CPU helper preservation contracts
tags:
  - postgres
  - cutover
  - cleanup
  - queues
---

# Task t04: Remove Unreachable `_LegacySyncPostgreSQLDriver` and Update `PostgreSQLQueueBackend` Dependency Guard

## Objective & Scope Boundary

Delete the unreachable private `_LegacySyncPostgreSQLDriver` class, `import asyncio`, and all remaining legacy imports from `src/py/vertebra/db/adapters/postgres/driver.py` leaving a minimal permanent barrel [^pg-driver-sync] [^pg-contracts], and update `PostgreSQLQueueBackend.__init__` (`lines 147`–`159`) in `src/py/vertebra/framework/queues/postgres.py` from `ensure_module("psycopg", ...)` to `ensure_module("vertebra._postgres", package="vertebra-core[postgres]", install_package="vertebra-core[postgres]", extra="postgres")` while keeping `PostgreSQLQueueBackend` strictly caller-`PostgreSQLQueueExecutor`-injected (built-in native queue executor shipping belongs to `queues-tasks-and-channels`) and verifying missing-wheel vs missing-executor behavior in `src/py/tests/unit/framework/test_distributed_queues.py` [^pg-queues-py].

Out of scope: Modifying `src/py/vertebra/db/adapters/postgres/core.py` (`PostgreSQLCore` CPU helpers stay intact) [^pg-core-py], `src/py/vertebra/db/dialer/` (standalone cloud dialer helper shims stay outside PostgreSQL adapter cutover), `src/rs/vertebra-db/`, or `src/py/vertebra/_vertebra.pyi`.

## Bounded Read Manifest

- [src/py/vertebra/db/adapters/postgres/driver.py](../../../../../../../src/py/vertebra/db/adapters/postgres/driver.py) — Barrel module containing `_LegacySyncPostgreSQLDriver` and `import asyncio` [^pg-driver-sync].
- [src/py/vertebra/db/adapters/postgres/core.py](../../../../../../../src/py/vertebra/db/adapters/postgres/core.py) — Shared `PostgreSQLCore` CPU helpers preserved across `_sync.py`, `_async.py`, and tests [^pg-core-py].
- [src/py/vertebra/db/adapters/postgres/__init__.py](../../../../../../../src/py/vertebra/db/adapters/postgres/__init__.py) — Package export barrel.
- [src/py/vertebra/framework/queues/postgres.py](../../../../../../../src/py/vertebra/framework/queues/postgres.py) — `PostgreSQLQueueBackend.__init__` (`lines 147`–`159`) `ensure_module` and `ImproperConfigurationError` guard [^pg-queues-py].
- [src/py/tests/unit/framework/test_distributed_queues.py](../../../../../../../src/py/tests/unit/framework/test_distributed_queues.py) — `PostgreSQLQueueBackend` missing-dependency and missing-executor unit tests.
- [.agents/bundles/specs/native-postgres-driver/contracts.md](../../../contracts.md) — Delivered barrel and `vertebra-core[postgres]` error metadata contract [^pg-contracts].

## Symbol & Signature Contracts

- Exact final contents of `src/py/vertebra/db/adapters/postgres/driver.py` (deleting `_LegacySyncPostgreSQLDriver`, `import asyncio`, and all remaining legacy imports):

```python
"""Synchronous and asynchronous native PostgreSQL drivers."""

from __future__ import annotations

from vertebra.db.adapters.postgres._async import AsyncPostgreSQLDriver
from vertebra.db.adapters.postgres._sync import SyncPostgreSQLDriver

__all__ = (
    "AsyncPostgreSQLDriver",
    "SyncPostgreSQLDriver",
)
```

- `src/py/vertebra/framework/queues/postgres.py` (`PostgreSQLQueueBackend.__init__`, `lines 147`–`159`, plus docstring `Raises:`):
  - Keep `PostgreSQLQueueBackend` strictly caller-`PostgreSQLQueueExecutor`-injected (no built-in default executor ships in this spec; built-in native queue executor delivery belongs to `queues-tasks-and-channels`, and `PostgreSQLQueueBackend` never accepts raw `AsyncPostgreSQLDriver` without an `executor=` adapter implementing `PostgreSQLQueueExecutor`).
  - When `executor is None`, call `ensure_module("vertebra._postgres", package="vertebra-core[postgres]", install_package="vertebra-core[postgres]", extra="postgres")` (so absent `vertebra._postgres` raises `MissingDependencyError` with `package == "vertebra-core[postgres]"` and `"vertebra-core[postgres]"` in `str(exc)`), then raise `ImproperConfigurationError("PostgreSQLQueueBackend requires a PostgreSQLQueueExecutor; pass executor= implementing PostgreSQLQueueExecutor (for example wrapping AsyncPostgreSQLDriver and PostgresNotificationSource)")`.
- Scoped post-cutover legacy-import invariant: zero `psycopg`, `psycopg_pool`, `asyncpg`, `_in_memory_rows`, `_executor`, or `_Legacy*` symbols exist anywhere in `src/py/vertebra/db/adapters/postgres/`, `src/py/vertebra/extensions/{sqlspec,sqlalchemy,alchemy}/`, and `src/py/vertebra/framework/queues/postgres.py` (`src/py/vertebra/db/dialer/` helper shims remain separate).

## Edge Cases & Error Semantics

- Static helpers (`strip_jsonb_header`, `encode_jsonb`, `encode_pgcopy_binary_rows`, and `parse_pgcopy_binary_rows`) called on `SyncPostgreSQLDriver` (`strip_jsonb_header` and `encode_jsonb` on `AsyncPostgreSQLDriver`) continue delegating cleanly through `_sync.py` / `_async.py` / `core.py::PostgreSQLCore`.
- Passing an explicit `executor=...` implementing `PostgreSQLQueueExecutor` to `PostgreSQLQueueBackend` continues working unchanged without touching `vertebra._postgres` when mocked in unit tests; omitting `executor` checks `vertebra._postgres` first, then raises the exact `ImproperConfigurationError` string when `vertebra._postgres` is installed.
- Neither `pyproject.toml` nor `uv.lock` alters runtime dependencies here (CPython 3.12 benchmark comparator pins land under `[dependency-groups].postgres-bench` in `ch29:t02`).

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/unit/framework/test_distributed_queues.py`, `src/py/tests/unit/db/test_postgres_driver.py`, `src/py/tests/unit/db/test_postgres_native_adapters.py`, `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `PostgreSQLQueueBackend(executor=None)` guard (`test_distributed_queues.py`) | (a) Mask `vertebra._postgres` absent and construct `PostgreSQLQueueBackend()`; (b) with `vertebra._postgres` present, construct `PostgreSQLQueueBackend()` | (a) Raises `MissingDependencyError` with `exc.package == "vertebra-core[postgres]"` and `"vertebra-core[postgres]"` in `str(exc)`; (b) raises `ImproperConfigurationError` with exact message `"PostgreSQLQueueBackend requires a PostgreSQLQueueExecutor; pass executor= implementing PostgreSQLQueueExecutor (for example wrapping AsyncPostgreSQLDriver and PostgresNotificationSource)"`. |
| Clean barrel & scoped legacy-symbol audit | Inspect `driver.py` AST/namespace + scan `src/py/vertebra/db/adapters/postgres/`, `src/py/vertebra/extensions/{sqlspec,sqlalchemy,alchemy}/`, and `src/py/vertebra/framework/queues/postgres.py` | `driver.py` exports only `AsyncPostgreSQLDriver`, `SyncPostgreSQLDriver`, and `__all__` (zero `asyncio` import, zero `_Legacy*`); `SyncPostgreSQLDriver.parse_pgcopy_binary_rows` remains callable; zero `psycopg`/`psycopg_pool`/`asyncpg` strings exist across those scoped paths. |
| Full adapter, queue, dataplane, & extension gate | Run full PostgreSQL unit, queue, pipeline, Arrow, event, embedding, dataplane, SQLSpec, tenant, and SQLAlchemy suites + docstring scan + Ruff + Mypy + `uv lock --check` | `0` failures, `0` lint/type/docstring issues, and clean lockfile state. |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/framework/test_distributed_queues.py -q -k postgres`
- Final Green + Characterization + Lint + Typecheck + Lock gate: `make dev && PYTHONPATH="src/py:." uv run --no-sync python -m tools.docs.scan_public_docstrings --check-leaks --check-overloads --check-typeddicts src/py/vertebra/db/adapters/postgres src/py/vertebra/framework/queues/postgres.py && uv run --no-sync pytest src/py/tests/unit/framework/test_distributed_queues.py src/py/tests/unit/db/test_postgres_driver.py src/py/tests/unit/db/test_postgres_native_adapters.py src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py src/py/tests/unit/db/test_postgres_native_adapter_arrow.py src/py/tests/unit/db/test_postgres_event_hooks.py src/py/tests/unit/embeddings/test_google_embedders.py src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/db/test_enterprise_olap_drivers.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py -q && uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres src/py/vertebra/framework/queues/postgres.py src/py/tests/unit/framework/test_distributed_queues.py && uv run --no-sync mypy src/py/vertebra/db/adapters/postgres src/py/vertebra/extensions src/py/vertebra/framework/queues/postgres.py && uv lock --check`

## Acceptance Criteria

- [ ] `_LegacySyncPostgreSQLDriver`, `import asyncio`, and all remaining legacy state/imports are removed from `driver.py`, preserving `SyncPostgreSQLDriver.parse_pgcopy_binary_rows` and leaving only the minimal public barrel (R24).
- [ ] `PostgreSQLQueueBackend.__init__` keeps `PostgreSQLQueueBackend` strictly caller-`PostgreSQLQueueExecutor`-injected, checks `ensure_module("vertebra._postgres", package="vertebra-core[postgres]", install_package="vertebra-core[postgres]", extra="postgres")` when `executor is None`, and raises the caller-injected `PostgreSQLQueueExecutor` `ImproperConfigurationError`, leaving zero `psycopg`/`asyncpg` references across `postgres/`, `extensions/{sqlspec,sqlalchemy,alchemy}/`, and `framework/queues/postgres.py` (R24).
- [ ] All PostgreSQL adapter, queue, dataplane, embedding, SQLSpec, tenant, and SQLAlchemy tests plus docstring scan, Ruff, Mypy, and `uv lock --check` exit `0` (R24).

## Notes & Discoveries

- Completing `ch27:t04` closes the Python adapter, extension, and queue cutover boundary (`R24`) and unblocks live Docker PG14–18 conformance in `ch28`.

[^pg-driver-sync]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — Public PostgreSQL driver barrel module targeted for final legacy sync and `import asyncio` removal.
[^pg-queues-py]: `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend` — `PostgreSQLQueueBackend.__init__` dependency and executor guard at lines `147`–`159`.
[^pg-core-py]: `src/py/vertebra/db/adapters/postgres/core.py::PostgreSQLCore` — Retained PostgreSQL CPU codecs and tenant SQL helpers.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Final barrel cleanliness, `vertebra-core[postgres]` dependency metadata, and CPU helper preservation contracts.
