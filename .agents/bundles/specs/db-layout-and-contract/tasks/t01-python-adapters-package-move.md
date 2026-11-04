---
type: Task
id: db-layout-and-contract:t01
title: Relocate Python engine packages to vertebra/db/adapters/<engine> and remove DataFusion SQLite fallback
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on: []
files:
  - src/py/vertebra/db/__init__.py
  - src/py/vertebra/db/arrow.py
  - src/py/vertebra/db/registry.py
  - src/py/vertebra/db/adapters/__init__.py
  - src/py/vertebra/db/adapters/bigquery/__init__.py
  - src/py/vertebra/db/adapters/bigquery/config.py
  - src/py/vertebra/db/adapters/bigquery/core.py
  - src/py/vertebra/db/adapters/bigquery/driver.py
  - src/py/vertebra/db/adapters/datafusion/__init__.py
  - src/py/vertebra/db/adapters/datafusion/config.py
  - src/py/vertebra/db/adapters/datafusion/driver.py
  - src/py/vertebra/db/adapters/duckdb/__init__.py
  - src/py/vertebra/db/adapters/duckdb/config.py
  - src/py/vertebra/db/adapters/duckdb/core.py
  - src/py/vertebra/db/adapters/duckdb/driver.py
  - src/py/vertebra/db/adapters/mssql/__init__.py
  - src/py/vertebra/db/adapters/mssql/config.py
  - src/py/vertebra/db/adapters/mssql/core.py
  - src/py/vertebra/db/adapters/mssql/driver.py
  - src/py/vertebra/db/adapters/mysql/__init__.py
  - src/py/vertebra/db/adapters/mysql/config.py
  - src/py/vertebra/db/adapters/mysql/core.py
  - src/py/vertebra/db/adapters/mysql/driver.py
  - src/py/vertebra/db/adapters/oracle/__init__.py
  - src/py/vertebra/db/adapters/oracle/config.py
  - src/py/vertebra/db/adapters/oracle/core.py
  - src/py/vertebra/db/adapters/oracle/driver.py
  - src/py/vertebra/db/adapters/postgres/__init__.py
  - src/py/vertebra/db/adapters/postgres/config.py
  - src/py/vertebra/db/adapters/postgres/core.py
  - src/py/vertebra/db/adapters/postgres/driver.py
  - src/py/vertebra/db/adapters/redis/__init__.py
  - src/py/vertebra/db/adapters/redis/config.py
  - src/py/vertebra/db/adapters/redis/core.py
  - src/py/vertebra/db/adapters/redis/driver.py
  - src/py/vertebra/db/adapters/spanner/__init__.py
  - src/py/vertebra/db/adapters/spanner/config.py
  - src/py/vertebra/db/adapters/spanner/core.py
  - src/py/vertebra/db/adapters/spanner/driver.py
  - src/py/vertebra/db/adapters/sqlite/__init__.py
  - src/py/vertebra/db/adapters/sqlite/config.py
  - src/py/vertebra/db/adapters/sqlite/core.py
  - src/py/vertebra/db/adapters/sqlite/driver.py
  - src/py/vertebra/db/adapters/valkey/__init__.py
  - src/py/vertebra/db/adapters/valkey/config.py
  - src/py/vertebra/db/adapters/valkey/core.py
  - src/py/vertebra/db/adapters/valkey/driver.py
  - src/py/vertebra/db/channels.py
  - src/py/vertebra/db/driver/_async.py
  - src/py/vertebra/db/driver/_common.py
  - src/py/vertebra/db/driver/_sync.py
  - src/py/vertebra/db/mcp_tools.py
  - src/py/vertebra/extensions/alchemy/cache.py
  - src/py/vertebra/extensions/apex/bridge.py
  - src/py/vertebra/extensions/django/db/backends/__init__.py
  - src/py/vertebra/extensions/fastapi/__init__.py
  - src/py/vertebra/extensions/flask/__init__.py
  - src/py/vertebra/extensions/litestar/__init__.py
  - src/py/vertebra/extensions/litestar/store.py
  - src/py/vertebra/extensions/sqlalchemy/alembic.py
  - src/py/vertebra/extensions/sqlalchemy/dialects.py
  - src/py/vertebra/extensions/sqlspec/adapters.py
  - src/py/vertebra/extensions/sqlspec/bigquery.py
  - src/py/vertebra/extensions/sqlspec/spanner.py
  - src/py/vertebra/extensions/sqlspec/storage.py
  - src/py/vertebra/extensions/starlette/__init__.py
  - src/py/vertebra/extensions/starlette/store.py
  - src/py/vertebra/framework/queues/postgres.py
  - src/py/vertebra/framework/stores.py
  - docs/examples/storage_and_db.py
  - docs/reference/db.md
  - docs/usage/analytics-and-cdc.md
  - docs/usage/databases.md
  - docs/usage/query-pipelines.md
  - .agents/bundles/knowledge/domains/data/broker-and-shm-leases.md
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  - .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
tests:
  - src/py/tests/integration/conftest.py
  - src/py/tests/unit/db/test_bigquery.py
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
  - src/py/tests/unit/db/test_mysql_driver.py
  - src/py/tests/unit/db/test_native_db_drivers.py
  - src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
  - src/py/tests/unit/db/test_pg_gateway.py
  - src/py/tests/unit/db/test_postgres_driver.py
  - src/py/tests/unit/db/test_redis_valkey_driver.py
  - src/py/tests/unit/db/test_redis_valkey_resp3.py
  - src/py/tests/unit/db/test_spanner.py
  - src/py/tests/unit/db/test_spanner_queue_and_cdc.py
  - src/py/tests/unit/db/test_vector_wire_codecs.py
  - src/py/tests/unit/docs/test_examples.py
  - src/py/tests/unit/extensions/test_alchemy_cache_backend.py
  - src/py/tests/unit/extensions/test_data_storage_integrations.py
  - src/py/tests/unit/extensions/test_django_plugin.py
  - src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
  - src/py/tests/unit/extensions/test_sqlspec_bigquery.py
  - src/py/tests/unit/extensions/test_sqlspec_spanner.py
  - src/py/tests/unit/extensions/test_starlette_plugin.py
  - src/py/tests/unit/framework/test_distributed_queues.py
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
plan_revision: 4
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: 2026-11-03T21:40:00Z
updated_at: 2026-11-04T01:05:00Z
commit: null
tags: [db, layout, adapters, datafusion, clean-break]
---

## Objective

Move all eleven Python database, cache, and federated query adapters under `src/py/vertebra/db/adapters/<engine>/`, rename `_arrow.py` to `arrow.py` and `_registry.py` to `registry.py`, convert `vertebra/db/__init__.py` to curated lazy exports over engine-agnostic names only, remove the `sqlite3` fallback from `DataFusionEngine`, and update all in-tree imports, doc Python code fences, and OKF Python `sources:` entries in lockstep.

## Context

- **Verification strategy (`behavior_tdd`)**: Package layout boundaries, lazy top-level imports, absence of legacy module paths, and fail-closed `DataFusionEngine` behavior are directly observable through Python import and execution tests at the public `vertebra.db` and `vertebra.db.adapters.*` seams.
- **Architectural invariants**:
  - Clean-break rule (`.agents/bundles/product/product-guidelines.md`): leave zero compatibility shims at `vertebra.db.<engine>`, `vertebra.db._arrow`, or `vertebra.db._registry`, and export zero engine-specific classes from `vertebra.db`.
  - Lazy package exports (`.agents/bundles/knowledge/patterns/python/module-and-class-layout.md`): use `vertebra.utils.module_loader.lazy_exports` in `src/py/vertebra/db/__init__.py` so `import vertebra.db` imports zero adapter subpackages.
  - Because `make lint-all` runs `make review-check` (`tools/scripts/review_setup.py::_check_bundles`) and `uv run pytest src/py/tests/unit` runs `src/py/tests/unit/docs/test_examples.py`, relocated Python file paths in `.agents/bundles/knowledge/domains/data/{broker-and-shm-leases,database-drivers-and-dialers,query-pipeline-cdc-and-tenancy}.md` and Python import fences in `docs/usage/{databases,analytics-and-cdc,query-pipelines}.md` must update inside `t01`.
- **Canonical test targets**: `src/py/tests/unit/db/test_native_db_drivers.py`, `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py`, and `src/py/tests/unit/db/test_pg_gateway.py`.

## Steps

1. **Write failing layout, lazy-export, and DataFusion fail-closed tests (RED)**:
   - In `src/py/tests/unit/db/test_native_db_drivers.py`, add parameterized `@pytest.mark.parametrize` tests asserting:
     - Every adapter package `vertebra.db.adapters.<engine>` (`postgres`, `mysql`, `sqlite`, `oracle`, `mssql`, `duckdb`, `bigquery`, `spanner`, `redis`, `valkey`, `datafusion`) imports cleanly, while `importlib.import_module(f"vertebra.db.{engine}")` plus `vertebra.db._arrow` and `vertebra.db._registry` raise `ModuleNotFoundError`.
     - `vertebra.db.__all__` exposes only curated engine-agnostic names (`ARROW_SHM_THRESHOLD_ROWS`, `AlloyDBAuthType`, `AlloyDBClusterURI`, `AlloyDBConfig`, `ArrowCursor`, `AsyncAlloyDBDialer`, `AsyncCloudSQLDialer`, `AsyncDatabaseDriver`, `BaseDatabaseConfig`, `CDCConfig`, `ChangeEvent`, `ChangeFeed`, `CloudSQLConfig`, `CloudSQLInstanceURI`, `CommonDriverMixin`, `DatabaseRegistry`, `SyncAlloyDBDialer`, `SyncCloudSQLDialer`, `SyncDatabaseDriver`, `TenantPredicate`, `TenantScopeConfig`, `ThreadOffloadAsyncDatabaseDriver`, plus `provide_bigquery_driver` and `provide_spanner_driver` until `t06` replaces them with `provide_driver`), and `import vertebra.db` in a fresh `sys.modules` state loads zero `vertebra.db.adapters.*` modules.
   - In `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py`, `src/py/tests/unit/db/test_pg_gateway.py`, and `src/py/tests/unit/db/test_bigquery.py`, update `DataFusionEngine` tests to import from `vertebra.db.adapters.datafusion`, assert that `DataFusionEngine().execute_dicts("SELECT 1")` raises `MissingDependencyError` when `datafusion` is unavailable and no `ctx=` is supplied (simulated via `monkeypatch.setitem(sys.modules, "datafusion", None)` + `reset_dependency_cache("datafusion")`), and pass a deterministic `SessionContext` test double via `DataFusionEngine(ctx=...)` (when `import_optional("datafusion") is None`) that executes against registered normalized PyArrow tables and raises on invalid SQL instead of falling back to `sqlite3`.

2. **Move packages, split `datafusion`, rename `arrow.py`/`registry.py`, and update all imports (GREEN)**:
   - Create `src/py/vertebra/db/adapters/__init__.py` (`"""Database, cache, and analytical engine adapters."""` with `__all__ = ()`).
   - Move `src/py/vertebra/db/{postgres,mysql,sqlite,oracle,mssql,duckdb,bigquery,spanner,redis,valkey}/` to `src/py/vertebra/db/adapters/<engine>/` (`git mv`) and update intra-adapter imports (`vertebra.db.adapters.<engine>.*`, `vertebra.db.arrow`, `vertebra.db.driver`), including `valkey/{config,core,driver}.py` importing `vertebra.db.adapters.redis.*`.
   - Split `src/py/vertebra/db/datafusion.py` into `src/py/vertebra/db/adapters/datafusion/{__init__,config,driver}.py` (`git rm` `datafusion.py` after moving):
     - `config.py`: `DataFusionMode`, `TableRegistration(FrozenBaseStruct, frozen=True, gc=False, kw_only=True)`, and `DataFusionConfig(FrozenBaseStruct, frozen=True, gc=False, kw_only=True)` (reparented onto `BaseDatabaseConfig` in `t06`) with PEP 257 inline field docstrings on every field of `TableRegistration` and `DataFusionConfig`.
     - `driver.py`: `ARROW_STREAM_MEDIA_TYPE` and `DataFusionEngine(config: DataFusionConfig | None = None, *, ctx: Any = None)`; delete `import sqlite3` and remove `_FallbackArrowTable` and the `sqlite3.connect(":memory:")` fallback in `DataFusionEngine.execute_dicts` and `execute_arrow`, normalizing `ArrowCursor` (`.to_pyarrow()`), PyArrow tables, and duck-typed `.to_pylist()` inputs into a `pyarrow.Table` inside `_coerce_arrow_table`, registering tables on `self._ctx` without `contextlib.suppress(Exception)`, calling `ensure_module("datafusion", package="datafusion")` when `self._ctx is None`, and propagating DataFusion exceptions unchanged from `self._ctx.sql(sql)`.
     - `__init__.py`: re-export `ARROW_STREAM_MEDIA_TYPE`, `DataFusionConfig`, `DataFusionEngine`, `DataFusionMode`, `TableRegistration`.
   - Rename `src/py/vertebra/db/_arrow.py` to `src/py/vertebra/db/arrow.py` and `src/py/vertebra/db/_registry.py` to `src/py/vertebra/db/registry.py`, updating internal `vertebra.db.adapters.*` imports in `registry.py`, `driver/{_common,_sync,_async}.py`, `channels.py`, `mcp_tools.py`, and `src/py/vertebra/extensions/{fastapi,flask,litestar,starlette}/__init__.py` (`from vertebra.db.registry import provide_bigquery_driver, provide_spanner_driver`).
   - Rewrite `src/py/vertebra/db/__init__.py` with `TYPE_CHECKING` imports and `__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)` over only the curated engine-agnostic public symbols.
   - Update all repository callers and tests (including `src/py/tests/integration/conftest.py`, `test_pg_dataplane_and_datafusion.py` and `test_pg_gateway.py` importing `CTEStep`, `SyncQueryPipeline`, `AsyncQueryPipeline`, `RelOverlay` from `vertebra.db.pipeline`, `test_native_db_drivers.py` importing `DatabaseChannelBackend` from `vertebra.db.channels`, `test_spanner_queue_and_cdc.py` importing `SyncSpannerChangeStreamSource`, `AsyncSpannerChangeStreamSource`, `SpannerChildPartition`, `SpannerChangeStreamRecord` from `vertebra.db.cdc`, `test_gcp_adapters_and_sqlalchemy.py` importing `GCPDatabaseToolPolicy`, `is_mutating_sql`, `register_gcp_database_tools` from `vertebra.db.mcp_tools`, and replacing `_load_fake_spanner_client()`'s hardcoded `lines[41:598]` `exec` slice in `test_sqlspec_spanner.py` and `test_gcp_adapters_and_sqlalchemy.py` with `from tests.unit.db.test_spanner import FakeSpannerClient`).
   - Update `docs/examples/storage_and_db.py`, `docs/usage/databases.md`, `docs/usage/analytics-and-cdc.md`, `docs/usage/query-pipelines.md` (`from vertebra.db.pipeline import AsyncQueryPipeline, RelOverlay, SyncQueryPipeline`), `docs/reference/db.md` (`::: vertebra.db.registry.DatabaseRegistry`, `::: vertebra.db.adapters.<engine>.*`), and relocated Python `sources:` / file links in `.agents/bundles/knowledge/domains/data/{broker-and-shm-leases,database-drivers-and-dialers,query-pipeline-cdc-and-tenancy}.md`.

3. **Refactor & verify**:
   - Ensure zero `vertebra.db._arrow`, `vertebra.db._registry`, or `vertebra.db.<engine>` references remain across `src/py/`, `docs/usage/`, `docs/reference/`, `docs/examples/`, and `.agents/bundles/knowledge/`.
   - Run Ruff, Mypy, `make review-check`, unit tests, and `make docs` to verify clean imports and Griffe autodoc rendering.

## Verification

- **Initial (RED)**:
  - `uv run pytest src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py` -> fails before the move (`ModuleNotFoundError: No module named 'vertebra.db.adapters'`).
- **Final (GREEN)**:
  - `uv run pytest src/py/tests/unit` -> exits `0`.
  - `make lint-all` -> exits `0`.
  - `make docs` -> exits `0`.

## Acceptance Criteria

- [ ] R1: All eleven engine packages live under `src/py/vertebra/db/adapters/<engine>/`, `arrow.py` and `registry.py` replace `_arrow.py` and `_registry.py`, `vertebra/db/__init__.py` uses `lazy_exports` for engine-agnostic symbols only, and legacy `vertebra.db.<engine>` paths raise `ModuleNotFoundError`.
- [ ] R2: `DataFusionEngine` contains zero `sqlite3` imports or fallbacks, calls `ensure_module("datafusion", package="datafusion")`, and propagates `MissingDependencyError` / DataFusion errors directly.
- [ ] All unit tests (including `test_examples.py`), `make lint-all` (including `make review-check`), and `make docs` pass with exit code `0`.

## Notes & Discoveries

<!-- Append timestamped implementation findings here: - [YYYY-MM-DD HH:MM] ... -->
