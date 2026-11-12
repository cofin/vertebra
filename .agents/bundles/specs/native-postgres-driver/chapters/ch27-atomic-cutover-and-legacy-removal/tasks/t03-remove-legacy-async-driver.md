---
type: Task
id: "native-postgres-driver:ch27:t03"
chapter_id: ch27-atomic-cutover-and-legacy-removal
title: Remove the legacy async driver and dialer psycopg/asyncpg connect helpers
description: Delete the unreferenced private _LegacyAsyncPostgreSQLDriver and its async-only imports from driver.py (retaining import asyncio for _LegacySyncPostgreSQLDriver until ch27:t04) and delete the psycopg/asyncpg connect factories from the Cloud SQL and AlloyDB dialers.
status: stable
state: pending
priority: P1
verification_strategy: characterization
depends_on:
  - "native-postgres-driver:ch27:t02"
read_set:
  - src/py/vertebra/db/adapters/postgres/driver.py
  - src/py/vertebra/db/dialer/cloudsql.py
  - src/py/vertebra/db/dialer/alloydb.py
  - src/py/tests/unit/db/test_postgres_driver.py
  - src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - src/py/vertebra/db/adapters/postgres/driver.py
  - src/py/vertebra/db/dialer/cloudsql.py
  - src/py/vertebra/db/dialer/alloydb.py
tests:
  - src/py/tests/unit/db/test_postgres_driver.py
  - src/py/tests/unit/db/test_postgres_native_adapters.py
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
  - src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
estimated_loc: 240
plan_revision: 8
plan_commit: d90d4625
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
updated_at: "2026-11-12T04:25:15Z"
commit: null
generated:
  by: flow-revise
  at: "2026-11-12T01:30:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T01:30:00Z"
    commit: c8722c76
sources:
  - id: pg-driver-mod
    resource: src/py/vertebra/db/adapters/postgres/driver.py::AsyncPostgreSQLDriver
    title: Public PostgreSQL driver module containing the temporary _LegacyAsyncPostgreSQLDriver remnant after ch27:t01
  - id: dialer-cloudsql
    resource: src/py/vertebra/db/dialer/cloudsql.py::SyncCloudSQLDialer
    title: Cloud SQL dialers carrying create_psycopg_connect_func and create_asyncpg_connect_func
  - id: dialer-alloydb
    resource: src/py/vertebra/db/dialer/alloydb.py::SyncAlloyDBDialer
    title: AlloyDB dialers carrying create_psycopg_connect_func and create_asyncpg_connect_func
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Final activation and dead-body removal invariants
tags:
  - postgres
  - cutover
  - cleanup
  - dialers
---

# Task t03: Remove the Legacy Async Driver and Dialer psycopg/asyncpg Connect Helpers

## Objective & Scope Boundary

After `ch27:t01`–`t02` switch every public path to the native drivers:

1. Delete `_LegacyAsyncPostgreSQLDriver` and the imports used only by it (`inspect`, `anyio.CancelScope`, `AsyncIterator`, `Awaitable`, `AsyncDatabaseDriver`) from `src/py/vertebra/db/adapters/postgres/driver.py`, keeping `import asyncio` because `_LegacySyncPostgreSQLDriver._publish_notification` uses it until `ch27:t04` [^pg-driver-mod] [^pg-contracts].
2. Delete `SyncCloudSQLDialer.create_psycopg_connect_func` and `AsyncCloudSQLDialer.create_asyncpg_connect_func` from `src/py/vertebra/db/dialer/cloudsql.py`, and `SyncAlloyDBDialer.create_psycopg_connect_func` and `AsyncAlloyDBDialer.create_asyncpg_connect_func` from `src/py/vertebra/db/dialer/alloydb.py`, together with any import used only by them (for example `import_optional` if no other caller remains) [^dialer-cloudsql] [^dialer-alloydb].

Out of scope: `_LegacySyncPostgreSQLDriver`, `import asyncio`, and `PostgreSQLQueueBackend` (`ch27:t04`); native dialer helpers (`CloudSQLConfig`, `AlloyDBConfig`, ephemeral certificate and `MetadataExchange` helpers); `_async.py`, `_sync.py`, `core.py`; user documentation (`ch30:t03`).

## Bounded Read Manifest

- `src/py/vertebra/db/adapters/postgres/driver.py::{_LegacyAsyncPostgreSQLDriver, _LegacySyncPostgreSQLDriver}` — bodies and imports after `t01` [^pg-driver-mod].
- `src/py/vertebra/db/dialer/cloudsql.py::{SyncCloudSQLDialer.create_psycopg_connect_func, AsyncCloudSQLDialer.create_asyncpg_connect_func}` [^dialer-cloudsql].
- `src/py/vertebra/db/dialer/alloydb.py::{SyncAlloyDBDialer.create_psycopg_connect_func, AsyncAlloyDBDialer.create_asyncpg_connect_func}` [^dialer-alloydb].
- `src/py/tests/unit/db/test_postgres_driver.py::test_database_registry_attaches_cloudsql_and_alloydb_dialers_to_postgres_drivers` — dialer attachment characterization.
- `src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py` — Cloud SQL / AlloyDB + SQLAlchemy characterization.
- [contracts.md](../../../contracts.md) — barrel cleanliness [^pg-contracts].

## Symbol & Signature Contracts

- `driver.py` keeps the permanent re-exports, `__all__ = ("AsyncPostgreSQLDriver", "SyncPostgreSQLDriver")`, `_LegacySyncPostgreSQLDriver`, `import asyncio`, and only the imports `_LegacySyncPostgreSQLDriver` still uses; zero `_LegacyAsyncPostgreSQLDriver`, `asyncpg`, or `CancelScope` references remain.
- `cloudsql.py` and `alloydb.py` keep every other public method on `SyncCloudSQLDialer`, `AsyncCloudSQLDialer`, `SyncAlloyDBDialer`, and `AsyncAlloyDBDialer`; `git grep -n -E "create_(psycopg|asyncpg)_connect_func" -- src/py` prints nothing.
- Docstrings on the dialer classes stop mentioning `psycopg`/`asyncpg` (verified by `tools.docs.scan_public_docstrings`).

## Edge Cases & Error Semantics

- Retaining `import asyncio` keeps Ruff `F821` and Mypy clean inside `_LegacySyncPostgreSQLDriver._publish_notification` between `t03` and `t04`.
- Removing `_LegacyAsyncPostgreSQLDriver` leaves `AsyncPostgreSQLDriver` identity, MRO, `__module__`, and error mapping unchanged.
- A caller of a deleted dialer factory gets `AttributeError`; no alias or deprecation shim is added.
- Characterization tests pass unchanged before and after deletion; if any test referenced a deleted factory, it is removed rather than rewritten against a shim.

## Public-Seam Test Blueprint

- **Canonical characterization files**: `src/py/tests/unit/db/test_postgres_driver.py`, `src/py/tests/unit/db/test_postgres_native_adapters.py`, `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py`, `src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Barrel namespace | `import vertebra.db.adapters.postgres.driver as drv; hasattr(drv, "_LegacyAsyncPostgreSQLDriver")` | `False`; `drv.AsyncPostgreSQLDriver.__module__ == "vertebra.db.adapters.postgres._async"`. |
| Dialer surface | `hasattr(SyncCloudSQLDialer, "create_psycopg_connect_func")`, `hasattr(AsyncCloudSQLDialer, "create_asyncpg_connect_func")`, `hasattr(SyncAlloyDBDialer, "create_psycopg_connect_func")`, `hasattr(AsyncAlloyDBDialer, "create_asyncpg_connect_func")` (parametrized in `test_postgres_driver.py`) | Each `False`. |
| Registry dialer attachment | `test_database_registry_attaches_cloudsql_and_alloydb_dialers_to_postgres_drivers` | Passes unchanged. |
| Full characterization | Pre- and post-deletion pytest + Ruff + Mypy across PostgreSQL adapter, pipeline, Arrow, event, embedding, SQLSpec, tenant, SQLAlchemy, and GCP adapter suites | Identical pass counts, zero `F401`/`F821`, zero Mypy errors. |

## Verification

- Pre-deletion baseline: `uv run --no-sync pytest src/py/tests/unit/db/test_postgres_driver.py src/py/tests/unit/db/test_postgres_native_adapters.py src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py src/py/tests/unit/db/test_postgres_native_adapter_arrow.py src/py/tests/unit/db/test_postgres_event_hooks.py src/py/tests/unit/embeddings/test_google_embedders.py src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/db/test_enterprise_olap_drivers.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py -q`
- Post-deletion gate: the same pytest command, then `PYTHONPATH="src/py:." uv run --no-sync python -m tools.docs.scan_public_docstrings --check-leaks --check-overloads --check-typeddicts src/py/vertebra/db/adapters/postgres src/py/vertebra/db/dialer && uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres src/py/vertebra/db/dialer && uv run --no-sync mypy src/py/vertebra/db src/py/vertebra/extensions && test -z "$(git grep -n -E 'create_(psycopg|asyncpg)_connect_func|_LegacyAsyncPostgreSQLDriver' -- src/py)"`

## Acceptance Criteria

- [ ] `_LegacyAsyncPostgreSQLDriver` and its exclusive imports are deleted from `driver.py` with `import asyncio` retained for `t04` (R24).
- [ ] The four dialer `psycopg`/`asyncpg` connect factories and their exclusive imports are deleted without shims (R24).
- [ ] Characterization, docstring scan, Ruff, and Mypy pass with no relaxed assertions (R24).
- [ ] Commit message follows `refactor(postgres): remove legacy async driver and third-party dialer factories` once verified.

## Notes & Discoveries

- No repository code, test, or doc calls the dialer factories at `c8722c76` (`git grep -n create_psycopg_connect_func` matches only their definitions).

[^pg-driver-mod]: `src/py/vertebra/db/adapters/postgres/driver.py::AsyncPostgreSQLDriver` — Public PostgreSQL driver module containing the temporary `_LegacyAsyncPostgreSQLDriver` remnant after `ch27:t01`.
[^dialer-cloudsql]: `src/py/vertebra/db/dialer/cloudsql.py::SyncCloudSQLDialer` — Cloud SQL dialers carrying `create_psycopg_connect_func` and `create_asyncpg_connect_func`.
[^dialer-alloydb]: `src/py/vertebra/db/dialer/alloydb.py::SyncAlloyDBDialer` — AlloyDB dialers carrying `create_psycopg_connect_func` and `create_asyncpg_connect_func`.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Final activation and dead-body removal invariants.
