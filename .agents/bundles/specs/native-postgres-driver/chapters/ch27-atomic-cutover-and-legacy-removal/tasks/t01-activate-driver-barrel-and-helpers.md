---
type: Task
id: "native-postgres-driver:ch27:t01"
chapter_id: ch27-atomic-cutover-and-legacy-removal
title: Activate the permanent native driver barrel, package exports, and numeric embedding pushdown, and replace legacy test doubles
description: Switch vertebra.db.adapters.postgres.driver and package __init__ to export the permanent _sync, _async, types, events, and change_stream contracts atomically, update PostgresEmbeddingPushdown to numeric binds with verified Google-style docstrings, and replace the SQLite-backed asyncpg pool double, psycopg pool mocks, and psycopg converter ports with native-shaped postgres_ports.py tests over canonical public imports.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch26:t04"
read_set:
  - src/py/vertebra/db/adapters/postgres/driver.py
  - src/py/vertebra/db/adapters/postgres/__init__.py
  - src/py/vertebra/db/adapters/postgres/embeddings.py
  - src/py/vertebra/db/adapters/postgres/config.py
  - src/py/tests/unit/db/test_postgres_driver.py
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - src/py/vertebra/db/adapters/postgres/driver.py
  - src/py/vertebra/db/adapters/postgres/__init__.py
  - src/py/vertebra/db/adapters/postgres/embeddings.py
tests:
  - src/py/tests/unit/db/test_postgres_driver.py
  - src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
  - src/py/tests/unit/embeddings/test_google_embedders.py
  - src/py/tests/unit/db/test_native_db_drivers.py
  - src/py/tests/unit/db/test_postgres_native_adapters.py
  - src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py
  - src/py/tests/unit/db/test_postgres_native_adapter_arrow.py
estimated_loc: 245
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
  - id: pg-driver-py
    resource: src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver
    title: Public PostgreSQL driver module defining psycopg_pool and asyncio.to_thread classes on main
  - id: pg-init-py
    resource: src/py/vertebra/db/adapters/postgres/__init__.py
    title: Lazy package export map for vertebra.db.adapters.postgres including PG_UNCHANGED_TOAST and PgUnchangedToast
  - id: pg-embeddings-py
    resource: src/py/vertebra/db/adapters/postgres/embeddings.py::PostgresEmbeddingPushdown
    title: AlloyDB SQL embedding pushdown and pgvector similarity search helper
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Frozen public contracts for native PostgreSQL activation
  - id: pg-driver-tests
    resource: src/py/tests/unit/db/test_postgres_driver.py::test_asyncpg_pool_transaction_and_affected_rows
    title: AsyncPostgreSQLDriver tests over a SQLite-backed asyncpg pool double
tags:
  - postgres
  - cutover
  - driver
  - embeddings
---

# Task t01: Activate the Permanent Native Driver Barrel, Package Exports, and Numeric Embedding Pushdown, and Replace Legacy Test Doubles

## Objective & Scope Boundary

Atomically switch `src/py/vertebra/db/adapters/postgres/driver.py` and `src/py/vertebra/db/adapters/postgres/__init__.py` to select the completed `._sync.SyncPostgreSQLDriver` and `._async.AsyncPostgreSQLDriver` classes alongside canonical `types`, `pipeline`, `events`, and `change_stream` lazy exports (explicitly preserving `PG_UNCHANGED_TOAST` and `PgUnchangedToast`) [^pg-driver-py] [^pg-init-py], update `PostgresEmbeddingPushdown` in `embeddings.py` to emit unconditional `$1`/`$2` numeric binds across both native drivers with complete Google-style docstrings [^pg-embeddings-py], and migrate core PostgreSQL driver, dataplane, embedding, and registry/pool unit tests to canonical public imports over `postgres_ports.py` (created in `ch25:t03`) [^pg-contracts]. On `main`, `test_postgres_driver.py` exercises `AsyncPostgreSQLDriver` through `_SQLiteAsyncPGPool` / `_SQLiteAsyncPGConnection` / `_SQLiteAsyncPGTransaction` (real SQLite semantics behind an asyncpg-shaped port) and `SyncPostgreSQLDriver` through `MockConnectionPool` / `MockConnection` / `MockCursor`; this task deletes those doubles and the tests bound to them and re-expresses the same transaction, rollback, cancellation, affected-count, GUC, and prepared-statement behaviors against native-shaped ports [^pg-driver-tests].

Out of scope: `extensions/sqlspec/adapters.py` and `extensions/sqlalchemy/dialects.py` activation (`ch27:t02`), and physical deletion of private `_LegacyAsyncPostgreSQLDriver` (`ch27:t03`) and `_LegacySyncPostgreSQLDriver` + queue `psycopg` guard (`ch27:t04`).

## Bounded Read Manifest

- [src/py/vertebra/db/adapters/postgres/driver.py](../../../../../../../src/py/vertebra/db/adapters/postgres/driver.py) — Existing `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` definitions to rename private while importing `._sync` and `._async` [^pg-driver-py].
- [src/py/vertebra/db/adapters/postgres/__init__.py](../../../../../../../src/py/vertebra/db/adapters/postgres/__init__.py) — `__all__`, `__lazy_modules__`, and `_EXPORT_MAP` package exports [^pg-init-py].
- [src/py/vertebra/db/adapters/postgres/embeddings.py](../../../../../../../src/py/vertebra/db/adapters/postgres/embeddings.py) — `PostgresEmbeddingPushdown.embed_documents`, `similarity_search`, and `_uses_dollar_binds` [^pg-embeddings-py].
- [src/py/vertebra/db/adapters/postgres/config.py](../../../../../../../src/py/vertebra/db/adapters/postgres/config.py) — `PostgreSQLConfig`, `DatabaseRegistry` resolution, and native primitive option projection.
- [src/py/tests/unit/db/test_postgres_driver.py](../../../../../../../src/py/tests/unit/db/test_postgres_driver.py) — Existing legacy port tests (`_SQLiteAsyncPGPool`, `MockConnectionPool`, `executor=...`) to migrate to `postgres_ports.py` (alongside inspecting `test_pg_dataplane_and_datafusion.py`, `test_google_embedders.py`, and `test_native_db_drivers.py`).
- [.agents/bundles/specs/native-postgres-driver/contracts.md](../../../contracts.md) — Final activation, value record, and constructor invariants [^pg-contracts].

## Symbol & Signature Contracts

- `src/py/vertebra/db/adapters/postgres/driver.py`:
  - Rename existing legacy classes in-place to `_LegacySyncPostgreSQLDriver` and `_LegacyAsyncPostgreSQLDriver`, updating `_LegacyAsyncPostgreSQLDriver.__init__` and any internal `strip_jsonb_header` / `encode_jsonb` static references to target `_LegacySyncPostgreSQLDriver` so the module stays statically valid until `t03`/`t04` delete them.
  - Import `from vertebra.db.adapters.postgres._async import AsyncPostgreSQLDriver` and `from vertebra.db.adapters.postgres._sync import SyncPostgreSQLDriver` at top-level grouped imports, keeping `__all__ = ("AsyncPostgreSQLDriver", "SyncPostgreSQLDriver")`. Zero runtime flags, environment toggles, or fallback branches select `_Legacy*`.
  - Maintain complete Google-style module and public symbol docstrings across `driver.py`, `__init__.py`, and `embeddings.py` (including PEP 257 attribute docstrings on `msgspec.Struct` fields) verified by `tools.docs.scan_public_docstrings`.
- `src/py/vertebra/db/adapters/postgres/__init__.py`:
  - Keep `"AsyncPostgreSQLDriver"` and `"SyncPostgreSQLDriver"` mapped to `vertebra.db.adapters.postgres.driver`, `"PoolerMode"` and `"PostgreSQLConfig"` mapped to `vertebra.db.adapters.postgres.config`, `"PostgreSQLCore"` alongside retained `core` helpers (`"PGCOPY_BINARY_SIGNATURE"`, `"decode_pgcopy_rows"`, `"encode_pg_jsonb"`, `"encode_pgcopy_rows"`, `"strip_pg_jsonb_header"`) mapped to `vertebra.db.adapters.postgres.core`, `"PostgresEmbeddingPushdown"` mapped to `vertebra.db.adapters.postgres.embeddings`, `"PostgresNotificationSource"` and `"PostgresNotifyConfig"` mapped to `vertebra.db.adapters.postgres.notify`, `"AsyncPostgresPipeline"` and `"SyncPostgresPipeline"` mapped to `vertebra.db.adapters.postgres.pipeline`, and `"ARROW_SHM_THRESHOLD_ROWS"`, `"ArrowCursor"`, `"read_arrow_shm"`, and `"write_arrow_shm"` re-exported from `vertebra.db.arrow` across `__lazy_modules__`, `__all__`, `_EXPORT_MAP`, and `if TYPE_CHECKING:` so `extensions/django/db/backends/__init__.py`, `test_examples.py`, `test_enterprise_olap_drivers.py`, and `test_postgres_driver.py` continue resolving cleanly.
  - Explicitly preserve `"PG_UNCHANGED_TOAST"`, `"PgUnchangedToast"`, and `"PostgresReplicationRecord"` alongside `"PostgresArrowResult"`, `"PostgresReplicationIdentity"`, `"PostgresColumn"`, `"PostgresQueryResult"`, `"PgInterval"`, `"PgArray"`, `"PgComposite"`, `"PgTemporal"`, `"PgJson"`, `"PgBitString"`, `"PgVector"`, `"PgParameter"`, `"PostgresErrorInfo"`, `"PostgresPipelineOperation"`, and `"PostgresPipelineOutcome"` mapped to `vertebra.db.adapters.postgres.types` (introducing zero phantom Arrow stream codec names and keeping `EventSessionRegistry` adapter-internal out of `__init__.py`).
  - Map `"PostgresNotificationDriver"` and `"PostgresReplicationDriver"` to `vertebra.db.adapters.postgres.events`.
  - Map `"PostgresChangeConfig"`, `"PostgresChangeSource"`, `"PostgresCheckpoint"`, `"PostgresCheckpointStore"`, `"MemoryPostgresCheckpointStore"`, `"PostgresCheckpointConflictError"`, and `"PostgresReplicationGapError"` to `vertebra.db.adapters.postgres.change_stream`.
- `src/py/vertebra/db/adapters/postgres/embeddings.py`:
  - Remove `PostgresEmbeddingPushdown._uses_dollar_binds()` and emit `("$1", "$2")` unconditionally in `embed_documents(...)` (`SELECT embedding($1, t.txt)::vector FROM unnest($2::text[]) WITH ORDINALITY AS t(txt, idx) ORDER BY t.idx`) and `similarity_search(...)` (`SELECT *, <col> <op> $1::vector AS distance FROM <table> ORDER BY distance ASC LIMIT $2`) for both `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver`.
  - Extract decoded `PgVector` (`row_val.values`), `VectorView`, binary `bytes`, or text pgvector payloads cleanly in `_extract_vector` while preserving `VectorBatchView.from_f32_rows` normalization.
- Test migrations to canonical public APIs over `postgres_ports.py`:
  - In `test_postgres_driver.py`, delete `_SQLiteAsyncPGTransaction`, `_SQLiteAsyncPGConnection`, `_SQLiteAsyncPGPool`, `MockCursor`, `MockConnection`, `MockConnectionPool`, `test_asyncpg_pool_transaction_and_affected_rows`, `test_asyncpg_pool_rollback_and_cancel_release_connection`, `test_asyncpg_pool_failed_begin_releases_connection`, `test_sync_postgres_driver_pool_gucs_and_prepared_statements`, and `test_postgres_sync_and_async_guards_name_psycopg_pool`, and add `test_native_driver_transaction_and_affected_rows`, `test_native_driver_rollback_and_cancel_release_lease`, `test_native_driver_failed_begin_releases_lease`, `test_native_driver_pool_gucs_and_statement_cache`, and `test_native_drivers_name_postgres_extra_when_wheel_missing` (parametrized over sync/async drivers) over `postgres_ports.py`; replace `_PsycopgConnectionPort` and `@requires_psycopg` in `test_pg_dataplane_and_datafusion.py` (keeping non-PostgreSQL and `PostgreSQLCore` CPU codec cases intact); update sync/async `$1`/`$2` pushdown SQL assertions in `src/py/tests/unit/embeddings/test_google_embedders.py`; update PostgreSQL pool validation, `dialer`/`pool` checks, and `MissingDependencyError` (`vertebra-core[postgres]`) assertions over `postgres_ports.py` in `src/py/tests/unit/db/test_native_db_drivers.py`; switch `test_postgres_native_adapters.py`, `test_postgres_native_adapter_pipeline.py`, and `test_postgres_native_adapter_arrow.py` from `._sync`/`._async` to `vertebra.db.adapters.postgres`.

## Edge Cases & Error Semantics

- Between `ch27:t01` and `ch27:t02`, pre-`t02` SQLSpec/SQLAlchemy sync callers emitting `%s` pyformat placeholders continue passing because `SyncPostgreSQLDriver._prepare_native_query` lowers `%s` to `$N` via `PostgreSQLCore.prepare_native_query`.
- Uninstalled `vertebra._postgres` raises `MissingDependencyError` with `package == "vertebra-core[postgres]"` and `"vertebra-core[postgres]"` in `str(exc)`; legacy `executor=` raises `TypeError`; `dsn="memory://"` (any casing) raises `ImproperConfigurationError(match="memory://")` synchronously in `__init__`; invalid external `pool=` raises `ImproperConfigurationError`.
- `PostgresEmbeddingPushdown.embed_documents` raises `DataError` when the returned row count disagrees with `len(texts)` and `ValueError` on empty `texts` or nonpositive `dimensions`.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/unit/db/test_postgres_driver.py`, `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py`, `src/py/tests/unit/embeddings/test_google_embedders.py`, `src/py/tests/unit/db/test_native_db_drivers.py`, `src/py/tests/unit/db/test_postgres_native_adapters.py`, `src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py`, `src/py/tests/unit/db/test_postgres_native_adapter_arrow.py`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Canonical barrel & `_EXPORT_MAP` resolution | Import every symbol in `vertebra.db.adapters.postgres.__all__` (including `PoolerMode`, `PostgresNotifyConfig`, `PostgresReplicationRecord`, `PGCOPY_BINARY_SIGNATURE`/`decode_pgcopy_rows`/`encode_pg_jsonb`/`encode_pgcopy_rows`/`strip_pg_jsonb_header`, `ARROW_SHM_THRESHOLD_ROWS`/`ArrowCursor`/`read_arrow_shm`/`write_arrow_shm`, `PG_UNCHANGED_TOAST`, `PgUnchangedToast`, `PostgresArrowResult`, `PostgresReplicationIdentity`, `PostgresChangeSource`, `SyncPostgreSQLDriver`, `AsyncPostgreSQLDriver`) | Exact `._sync` / `._async` classes, preserved `config`/`notify`/`core`/`vertebra.db.arrow`/`types` symbols, valid lazy resolution for every `__all__` item, and zero `_Legacy*` or `EventSessionRegistry` leaks in `__all__`. |
| Missing wheel, `memory://`, & `test_native_db_drivers.py` registry checks | Absent `vertebra._postgres`, `dsn="MEMORY://db"`, invalid external `pool=`, `DatabaseRegistry` dialer/pool attachment over `postgres_ports.py` | Missing wheel raises `MissingDependencyError` (`package == "vertebra-core[postgres]"` in `str(exc)`); `MEMORY://` and bad `pool=` raise `ImproperConfigurationError`; `driver.dialer` and validated shared pool attach cleanly. |
| Native replacements for SQLite/psycopg doubles (`test_postgres_driver.py`) | Parametrized sync/async driver over `postgres_ports.py`: `CREATE`/`INSERT` (`execute == 1`), `execute_many` of `2` rows inside `transaction()`; `ValueError("abort")` inside `transaction()`; `asyncio` task cancelled after `INSERT`; `begin()` with the port's begin failing once | Affected counts `1`, `2`, and `3` for the `UPDATE`; aborted and cancelled transactions leave `0` rows and `acquired == released`; failed `begin()` releases the lease and a retry succeeds; zero `sqlite3`, `psycopg`, or `asyncpg` names remain in the module. |
| Sync & async embedding pushdown (`test_google_embedders.py`) | `PostgresEmbeddingPushdown(sync_driver, ...)` and `PostgresEmbeddingPushdown(async_driver, ...)` running `embed_documents(["alpha", "beta"])` and `similarity_search(...)` | Both sync and async drivers receive unconditional `$1`/`$2` placeholders and return normalized `VectorBatchView` (`len == 2`) and ranked rows. |
| Dataplane (`test_pg_dataplane_and_datafusion.py`) & docstring scan | Replace `_PsycopgConnectionPort` + `@requires_psycopg` with `postgres_ports.py`; preserve `parse_pgcopy_binary_rows`/`encode_pgcopy_binary_rows`/`strip_jsonb_header`/`encode_jsonb` + DataFusion tests; run `tools.docs.scan_public_docstrings` | Zero `@requires_psycopg` skips remain on PostgreSQL unit paths, `in_use == 0`, and docstring scan exits `0`. |

## Verification

- Initial Red baseline: `make dev && uv run --no-sync pytest src/py/tests/unit/db/test_postgres_driver.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/embeddings/test_google_embedders.py src/py/tests/unit/db/test_native_db_drivers.py -q`
- Final Green + docstring + lint + typecheck gate: `make dev && PYTHONPATH="src/py:." uv run --no-sync python -m tools.docs.scan_public_docstrings --check-leaks --check-overloads --check-typeddicts src/py/vertebra/db/adapters/postgres && uv run --no-sync pytest src/py/tests/unit/db/test_postgres_driver.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/embeddings/test_google_embedders.py src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/db/test_postgres_native_adapters.py src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py src/py/tests/unit/db/test_postgres_native_adapter_arrow.py -q && uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres src/py/tests/unit/db src/py/tests/unit/embeddings/test_google_embedders.py && uv run --no-sync mypy src/py/vertebra/db/adapters/postgres && test -z "$(git grep -n -E 'sqlite3|psycopg|asyncpg|_SQLiteAsyncPG' -- src/py/tests/unit/db/test_postgres_driver.py)"`

## Acceptance Criteria

- [ ] `driver.py` and `postgres/__init__.py` export permanent `._sync.SyncPostgreSQLDriver` and `._async.AsyncPostgreSQLDriver` while preserving `PoolerMode`, `PostgresNotifyConfig`, `PostgresReplicationRecord`, `PG_UNCHANGED_TOAST`, `PgUnchangedToast`, retained `core` helpers, `vertebra.db.arrow` re-exports, and exact `types`/`pipeline`/`events`/`change_stream` lazy targets without phantom Arrow helpers or `EventSessionRegistry` leaks (R24).
- [ ] `PostgresEmbeddingPushdown` emits unconditional `$1`/`$2` numeric binds across both sync and async drivers, and `src/py/vertebra/db/adapters/postgres` passes `tools.docs.scan_public_docstrings` (R24).
- [ ] `test_postgres_driver.py` contains no SQLite-backed asyncpg double or psycopg pool mock, `test_pg_dataplane_and_datafusion.py` drops `_PsycopgConnectionPort` + `@requires_psycopg`, and `test_google_embedders.py`, `test_native_db_drivers.py`, and native adapter unit suites pass over canonical public imports and `postgres_ports.py` (R24).

## Notes & Discoveries

- Notification source naming: this worksheet executes before `queues-tasks-and-channels` t01 splits `NotificationSource` into `SyncNotificationSource` / `AsyncNotificationSource` and renames `PostgresNotificationSource` to `AsyncPostgresNotificationSource` alongside a new `SyncPostgresNotificationSource` (see [native-data-platform](../../../../native-data-platform/spec.md) Seam 2). It keeps the HEAD names, adds no new unprefixed notification protocol, class, or alias, and leaves the rename of every symbol, doc, and OKF reference it touches to that task.
- Preserve `PostgreSQLCore` CPU helpers (`strip_jsonb_header`, `encode_jsonb`, `encode_pgcopy_rows`, `decode_pgcopy_rows`), driver static `parse_pgcopy_binary_rows`, and all non-PostgreSQL cases in `test_pg_dataplane_and_datafusion.py` and `test_native_db_drivers.py`.

[^pg-driver-py]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — Public PostgreSQL driver module defining `psycopg_pool` and `asyncio.to_thread` classes on `main`.
[^pg-init-py]: `src/py/vertebra/db/adapters/postgres/__init__.py` — Lazy package export map for `vertebra.db.adapters.postgres` including `PG_UNCHANGED_TOAST` and `PgUnchangedToast`.
[^pg-embeddings-py]: `src/py/vertebra/db/adapters/postgres/embeddings.py::PostgresEmbeddingPushdown` — AlloyDB SQL embedding pushdown and `pgvector` similarity search helper.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Frozen public contracts for native PostgreSQL activation.
[^pg-driver-tests]: `src/py/tests/unit/db/test_postgres_driver.py::test_asyncpg_pool_transaction_and_affected_rows` — `AsyncPostgreSQLDriver` tests over a SQLite-backed asyncpg pool double.
