---
type: Task
id: db-layout-and-contract:t07
title: Add parameterized pytest-databases driver conformance suite and update docs and OKF chapters
state: pending
priority: P2
verification_strategy: integration_acceptance
depends_on:
  - db-layout-and-contract:t06
files:
  - src/py/tests/integration/conftest.py
  - src/py/tests/integration/db/__init__.py
  - src/py/tests/integration/db/test_driver_conformance.py
  - docs/examples/storage_and_db.py
  - docs/reference/db.md
  - docs/usage/analytics-and-cdc.md
  - docs/usage/databases.md
  - .agents/bundles/knowledge/domains/data/broker-and-shm-leases.md
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  - .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
tests:
  - src/py/tests/integration/db/test_driver_conformance.py
  - src/py/tests/unit/docs/test_examples.py
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
updated_at: 2026-11-04T01:08:00Z
commit: null
tags: [db, conformance, integration, docs, okf]
---

## Objective

Create the cross-engine driver conformance suite `src/py/tests/integration/db/test_driver_conformance.py` parameterized across all adapters via `@pytest.mark.parametrize` and `pytest-databases` (fixing `MySQLConfig`, `RedisConfig`, and `ValkeyConfig` fixture constructors in `src/py/tests/integration/conftest.py`), and synchronize `docs/` and `.agents/bundles/knowledge/domains/data/{database-drivers-and-dialers,broker-and-shm-leases,query-pipeline-cdc-and-tenancy}.md` with the new `vertebra.db.adapters.<engine>` layout, `vertebra._bigquery` / `vertebra._spanner` optional wheels, driver contract, `DatabaseError` hierarchy, `Sync*`/`Async*` capability protocols, and `provide_driver`.

## Context

- **Verification strategy (`integration_acceptance`)**: The conformance suite is the acceptance contract for `native-data-platform` Chapters 1–6: `sqlite` passes the full SQL/Arrow driver contract end-to-end immediately without external containers (using `t04`'s `_translate_sqlite_error` `DatabaseError` mapping and `Decimal`/`datetime`/JSON bind normalization), `datafusion`, `redis`, and `valkey` verify registry/config/engine conformance, and SQL engines awaiting native Rust cutovers carry `pytest.mark.xfail(strict=True, reason="...")` naming their owning chapter so CI fails the moment an engine starts passing without removing its `xfail` marker.
- **Architectural invariants**:
  - Use `@pytest.mark.parametrize` and parameterized `@pytest.fixture(params=...)` exclusively — never iterate over engine specs or test cases with `for` loops inside test functions.
  - None of the 8 SQL/analytical adapter pairs may skip mandatory `SyncDatabaseDriver` / `AsyncDatabaseDriver` contract assertions; capability-based skips apply strictly to optional `capabilities.py` protocols (`SyncPipelineCapable` / `AsyncPipelineCapable`).
  - Keep `<!-- truth: start -->` blocks in OKF knowledge chapters `<= 40` lines (`make review-check`).

## Steps

1. **Fix `src/py/tests/integration/conftest.py` and create parameterized conformance suite `src/py/tests/integration/db/test_driver_conformance.py`**:
   - In `src/py/tests/integration/conftest.py`, update `vtb_mysql_config` and `vtb_mariadb_config` to construct `MySQLConfig.from_url(f"mysql://{user}:{password}@{host}:{port}/{database}")`, `vtb_redis_config` to construct `RedisConfig.from_url(f"redis://{host}:{port}/0")`, and `vtb_valkey_config` to construct `ValkeyConfig.from_url(f"valkey://{host}:{port}/0")` instead of passing unsupported `host=` / `port=` keyword arguments.
   - Add `src/py/tests/integration/db/__init__.py` and define `EngineConformanceSpec(FrozenBaseStruct, frozen=True, gc=False)` in `test_driver_conformance.py` covering all 8 SQL/analytical adapter pairs: `sqlite` (live local execution, sync + async), `postgres` (`native-postgres-driver`), `oracle` (`native-oracle-driver`), `mysql` / `mssql` (`native-mysql-mssql-drivers`), `duckdb` (`native-embedded-and-cache-drivers`), and `bigquery` / `spanner` (live when `pytest-databases` / emulator or credentials are configured, `pytest.skip` only when external service env is absent), plus parameterized registry/config/execution conformance tests for `DataFusionConfig` / `DataFusionEngine` (`pool_key`, `provide_driver`, `execute_arrow` / `execute_dicts` fail-closed behavior) and `RedisConfig` / `ValkeyConfig`.
   - Parameterize SQL/analytical conformance test functions via `@pytest.fixture(params=ENGINE_SPECS, ids=...)` and `@pytest.mark.parametrize` across all mandatory `SyncDatabaseDriver` / `AsyncDatabaseDriver` contract methods (forbidding skips on core contract methods):
     - Scalar and structured types: `NULL`, 64-bit `int`, `float`, exact `Decimal`, `bool`, non-ASCII UTF-8 `str`, `bytes`, timezone-aware `datetime`, and JSON payloads.
     - Execution contract: `execute` (`int` rowcount), `execute_many` (`int` rowcount), `execute_tuples`, `execute_with_description` (`(rows, col_names)` 2-tuple), `select`, `select_one` (`NotFoundError` on 0 rows), `select_one_or_none` (`None` on 0 rows), and `select_value` (`NotFoundError` on 0 rows, `None` on 1-row `SQL NULL`).
     - Transactions: `with driver.transaction():` / `async with driver.transaction():` commit persistence and exception-triggered rollback.
     - Errors: duplicate primary key / constraint violation raising `IntegrityError` (`DatabaseError`) and invalid SQL raising `ProgrammingError` / `OperationalError` (`DatabaseError`).
     - Arrow data plane: `select_to_arrow` -> `ArrowCursor`, `stream_arrow_batches`, and `load_from_arrow` round-trip.
     - Cancellation & optional pipelining: `cancel()` idempotency when idle, and `driver.pipeline()` batch execution gated strictly on optional `isinstance(driver, SyncPipelineCapable)` (sync) / `isinstance(driver, AsyncPipelineCapable)` (async).
   - Apply `pytest.mark.xfail(strict=True, reason="Owned by <flow_id>")` to non-native SQL engines whose current Python drivers still use stand-in/legacy backends so every parameterized entry runs deterministically (`passed`, `xfailed`, or `skipped` when external service env is absent).

2. **Update documentation and OKF knowledge chapters**:
   - Update `docs/usage/databases.md`, `docs/usage/analytics-and-cdc.md`, `docs/reference/db.md`, and `docs/examples/storage_and_db.py` to document `vertebra.db.adapters.<engine>`, optional per-engine install extras (`vertebra-core[bigquery]`, `vertebra-core[spanner]`), the unified `SyncDatabaseDriver` / `AsyncDatabaseDriver` contract, `DatabaseError` / `NotFoundError`, capability protocols (`SyncPipelineCapable`, `AsyncPipelineCapable`, `NotificationSource`, `SyncChangeSource`, `AsyncChangeSource`, `QueueStore`, `EmbeddingPushdown`), and `provide_driver`.
   - Update `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/bundles/knowledge/domains/data/broker-and-shm-leases.md`, and `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` (`sources:` frontmatter, `<= 40`-line `<!-- truth: start -->` blocks, and prose) to reflect `src/py/vertebra/db/adapters/<engine>/`, `src/rs/vertebra-{bigquery,spanner}/`, `src/rs/vertebra-db/src/<engine>/mod.rs`, `src/rs/vertebra-db/src/{connection,py_async}.rs`, `src/py/vertebra/db/driver/capabilities.py`, `BaseDatabaseConfig.pool_key()`, and `provide_driver`.

3. **Run full verification gates**:
   - Execute `uv run pytest src/py/tests/integration/db/test_driver_conformance.py`, `uv run pytest src/py/tests/unit/docs/test_examples.py`, `make lint-all`, `make docs`, and `make review-check`.

## Verification

- **Initial**:
  - `uv run pytest src/py/tests/integration/db/test_driver_conformance.py` -> fails before `test_driver_conformance.py` exists.
- **Final (GREEN)**:
  - `uv run pytest src/py/tests/integration/db/test_driver_conformance.py` -> exits `0` (`sqlite`, `datafusion`, and `redis`/`valkey` conformance tests pass; pending native SQL engines report strict `xfail` or `skip` when external containers are absent).
  - `make lint-all && make rs-test && make py-test && make docs && make review-check` -> exits `0`.

## Acceptance Criteria

- [ ] R9: `src/py/tests/integration/db/test_driver_conformance.py` covers all 8 SQL/analytical adapter pairs plus `DataFusion` and `Redis`/`Valkey` using `@pytest.mark.parametrize` and parameterized fixtures with zero `for` loops over test cases and zero skips on mandatory core driver methods.
- [ ] `docs/usage/databases.md`, `docs/usage/analytics-and-cdc.md`, `docs/reference/db.md`, `docs/examples/storage_and_db.py`, and all three OKF data chapters build and validate cleanly (`make docs` and `make review-check` exit `0`).

## Notes & Discoveries

<!-- Append timestamped implementation findings here: - [YYYY-MM-DD HH:MM] ... -->
