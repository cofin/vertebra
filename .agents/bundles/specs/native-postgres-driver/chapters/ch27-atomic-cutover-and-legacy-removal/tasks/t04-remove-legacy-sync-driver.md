---
type: Task
id: "native-postgres-driver:ch27:t04"
chapter_id: ch27-atomic-cutover-and-legacy-removal
title: Remove unreachable _LegacySyncPostgreSQLDriver implementation leaving clean public barrel
description: Delete the unreferenced private _LegacySyncPostgreSQLDriver class and all remaining legacy state from vertebra.db.adapters.postgres.driver so driver.py is a minimal permanent public barrel.
status: stable
state: pending
priority: P1
verification_strategy: characterization
depends_on:
  - "native-postgres-driver:ch27:t03"
read_set:
  - src/py/vertebra/db/adapters/postgres/driver.py
  - src/py/vertebra/db/adapters/postgres/core.py
  - src/py/vertebra/db/adapters/postgres/__init__.py
  - src/py/tests/unit/db/test_postgres_driver.py
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - src/py/vertebra/db/adapters/postgres/driver.py
tests:
  - src/py/tests/unit/db/test_postgres_driver.py
  - src/py/tests/unit/db/test_postgres_native_adapters.py
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
estimated_loc: 210
plan_revision: 8
plan_commit: 8c540809
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
  - id: pg-driver-sync
    resource: src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver
    title: Public PostgreSQL driver barrel module targeted for final legacy sync removal
  - id: pg-core-py
    resource: src/py/vertebra/db/adapters/postgres/core.py::PostgreSQLCore
    title: Retained PostgreSQL CPU codecs and tenant SQL helpers
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Final barrel cleanliness and CPU helper preservation contracts
tags:
  - postgres
  - cutover
  - cleanup
  - sync
---

# Task t04: Remove Unreachable `_LegacySyncPostgreSQLDriver` Implementation Leaving Clean Public Barrel

## Objective & Scope Boundary

Delete the unreachable private `_LegacySyncPostgreSQLDriver` class and all remaining legacy imports (`os`, `Buffer`, `Callable`, `Iterator`, `Mapping`, `Sequence`, `Any`, `ClassVar`, `PostgreSQLConfig`, `PostgreSQLCore`, `ARROW_SHM_THRESHOLD_ROWS`, `ArrowCursor`, `_table_to_rows_and_cols`, `POSTGRES_DIALECT_TAG`, `SyncDatabaseDriver`, `ensure_module`, `import_optional`, `quote_dialect_identifier`, `validate_identifier`) from `src/py/vertebra/db/adapters/postgres/driver.py`, leaving `driver.py` as a pure 11-line permanent barrel [^pg-driver-sync] [^pg-contracts].

Out of scope: Modifying `src/py/vertebra/db/adapters/postgres/core.py` (`PostgreSQLCore` CPU helpers remain intact) [^pg-core-py], `src/rs/vertebra-db/` (`PostgresPool` SHM admission metadata and CPU frame helpers remain shared), or `src/py/vertebra/_vertebra.pyi`.

## Bounded Read Manifest

- [src/py/vertebra/db/adapters/postgres/driver.py](../../../../../../../src/py/vertebra/db/adapters/postgres/driver.py) — Barrel module containing the final `_LegacySyncPostgreSQLDriver` remnant [^pg-driver-sync].
- [src/py/vertebra/db/adapters/postgres/core.py](../../../../../../../src/py/vertebra/db/adapters/postgres/core.py) — Shared `PostgreSQLCore` CPU helpers preserved across `_sync.py`, `_async.py`, and tests [^pg-core-py].
- [src/py/vertebra/db/adapters/postgres/__init__.py](../../../../../../../src/py/vertebra/db/adapters/postgres/__init__.py) — Package export barrel.
- [src/py/tests/unit/db/test_postgres_driver.py](../../../../../../../src/py/tests/unit/db/test_postgres_driver.py) — Canonical PostgreSQL unit test suite.
- [.agents/bundles/specs/native-postgres-driver/contracts.md](../../../contracts.md) — Delivered barrel contract [^pg-contracts].

## Symbol & Signature Contracts

- Exact final contents of `src/py/vertebra/db/adapters/postgres/driver.py`:

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

- Zero `psycopg`, `psycopg_pool`, `asyncpg`, `_in_memory_rows`, `_executor`, or `_Legacy*` symbols remain anywhere under `src/py/vertebra/`.

## Edge Cases & Error Semantics

- Static helpers (`strip_jsonb_header`, `encode_jsonb`, `encode_pgcopy_binary_rows`, `decode_pgcopy_binary_rows`) called on `SyncPostgreSQLDriver` or `PostgreSQLCore` by existing dataplane tests continue delegating cleanly through `_sync.py::SyncPostgreSQLDriver` / `core.py::PostgreSQLCore`.
- Neither `pyproject.toml` nor `uv.lock` requires runtime dependency removal here (`psycopg` and `asyncpg` were never runtime `pyproject.toml` dependencies; bench-only group pins are added separately in `ch29:t02`).

## Public-Seam Test Blueprint

- **Canonical characterization files**: `src/py/tests/unit/db/test_postgres_driver.py`, `src/py/tests/unit/db/test_postgres_native_adapters.py`, `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Clean barrel inspection | Inspect `src/py/vertebra/db/adapters/postgres/driver.py` AST and runtime namespace | Contains only `AsyncPostgreSQLDriver`, `SyncPostgreSQLDriver`, and `__all__`; zero `psycopg`/`asyncpg`/`_Legacy*` strings across `src/py/vertebra/`. |
| Full adapter, dataplane, & extension characterization | Run full PostgreSQL unit, pipeline, Arrow, event, dataplane, SQLSpec, and SQLAlchemy suites + Ruff + Mypy + `uv lock --check` | `0` failures, `0` lint/type issues, and clean lockfile state. |

## Verification

- Pre-deletion characterization baseline: `uv run --no-sync pytest src/py/tests/unit/db/test_postgres_driver.py src/py/tests/unit/db/test_postgres_native_adapters.py src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py src/py/tests/unit/db/test_postgres_native_adapter_arrow.py src/py/tests/unit/db/test_postgres_event_hooks.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py -q`
- Post-deletion characterization + lint + typecheck + lock gate: `uv run --no-sync pytest src/py/tests/unit/db/test_postgres_driver.py src/py/tests/unit/db/test_postgres_native_adapters.py src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py src/py/tests/unit/db/test_postgres_native_adapter_arrow.py src/py/tests/unit/db/test_postgres_event_hooks.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py -q && uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres && uv run --no-sync mypy src/py/vertebra/db/adapters/postgres src/py/vertebra/extensions && uv lock --check`

## Acceptance Criteria

- [ ] `_LegacySyncPostgreSQLDriver` and all remaining legacy state/imports are removed from `src/py/vertebra/db/adapters/postgres/driver.py`, leaving only the permanent public barrel (R24).
- [ ] All PostgreSQL adapter, dataplane, SQLSpec, and SQLAlchemy regression tests plus Ruff, Mypy, and `uv lock --check` exit `0` without weakening tests (R24).

## Notes & Discoveries

- [2026-11-07 16:45] Completing `ch27:t04` closes the Python adapter cutover boundary (`R24`) and unblocks live Docker PG14–18 conformance in `ch28`.

[^pg-driver-sync]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — Public PostgreSQL driver barrel module targeted for final legacy sync removal.
[^pg-core-py]: `src/py/vertebra/db/adapters/postgres/core.py::PostgreSQLCore` — Retained PostgreSQL CPU codecs and tenant SQL helpers.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Final barrel cleanliness and CPU helper preservation contracts.
