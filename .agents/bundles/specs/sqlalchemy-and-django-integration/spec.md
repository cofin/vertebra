---
type: Spec
flow_id: sqlalchemy-and-django-integration
title: SQLAlchemy, Advanced Alchemy, and Django DB Integration
description: Split the multi-engine SQLAlchemy dialect module, base GCP dialects on the official SQLAlchemy dialects, share one DB-API module, replace the custom AA cache region with a dogpile backend, and make Django database backends real.
state: draft
parent_prd: integration-remediation
research:
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 0
plan_commit: null
state_revision: 0
current_task: null
last_verified_checkpoint: null
created_at: 2026-11-03T03:30:00Z
updated_at: 2026-11-03T03:30:00Z
tags: [sqlalchemy, advanced-alchemy, django, dialects, dbapi]
---

# Flow Charter: SQLAlchemy, Advanced Alchemy, and Django DB Integration

## Charter

SQLAlchemy and Advanced Alchemy are the primary user path. Vertebra's dialects and Django backends must behave like full dialects and backends (compilers, type compilers, identifier quoting, reflection, schema editing) while executing through vertebra's drivers.

## Scope

1. **DB-API module.** Extract the PEP 249 shim (`DBAPIModule`, `Connection`, `Cursor`, async variants, error hierarchy) from `extensions/sqlalchemy/dialects.py` into `extensions/sqlalchemy/dbapi.py`. The SQLAlchemy dialects and the Django backends both use it.
2. **Dialects.**
   - Split into `extensions/sqlalchemy/dialects/<engine>.py` and `extensions/sqlalchemy/engine.py` (`SyncEngineAdapter`, `attach_serverless_disconnect_guard`).
   - `BigQueryDialect` subclasses `sqlalchemy_bigquery.BigQueryDialect`, `SpannerDialect` subclasses `google.cloud.sqlalchemy_spanner.SpannerDialect`, and `DuckDBDialect` subclasses `duckdb_engine.Dialect`. Each is an optional extra and overrides only `import_dbapi`, `get_pool_class`, and `create_connect_args`.
   - The Postgres/MySQL/Oracle/MSSQL/SQLite dialects subclass the SQLAlchemy built-ins in the same way.
   - Remove the helpers only tests call (`compile_merge_upsert`, `compile_insert_or_update`, `compile_interleave_clause`, `extract_interleave_options`), unless a dialect compiler adopts them.
   - Split `sqlalchemy/alembic.py` into one Impl module per dialect.
3. **Advanced Alchemy.**
   - Replace `alchemy/cache.py::SQLAlchemyCacheBackend` with a dogpile `CacheBackend` registered as `vertebra.valkey` / `vertebra.redis`, so AA's stock `CacheManager` works. Keep `AsyncCacheManager` only for native async L2.
   - Remove the unused `shm_name`, the dead `TYPE_CHECKING` block, and the URL-as-backend-name bug in `create_cache_config` (prefix-scoped `SCAN` + `UNLINK` and millisecond `PX` TTLs on `SQLAlchemyCacheBackend` landed in `critical-correctness-fixes`).
   - Delete the `sqlalchemy/cache.py` re-export shim and `alchemy/broker.py::StatementCache`.
   - Replace the `alchemy/storage.py` `storages.get` monkeypatch with AA's registration API.
   - `alchemy/plugin.py::normalize_alchemy_url` reads schemes from the config classes (`url_schemes` from native-data-platform `db-layout-and-contract`).
4. **Django backends** *(user decision)*.
   - `extensions/django/db/backends/{postgresql,mysql,oracle,sqlite3}/base.py::DatabaseWrapper` subclass Django's built-in backends and swap in the vertebra DB-API module.
   - `spanner/base.py::DatabaseWrapper` subclasses `django_spanner.base.DatabaseWrapper` (googleapis `django-google-spanner`, Django 5.2/6.0) and swaps its `google.cloud.spanner_dbapi` connection for the vertebra DB-API.
   - The BigQuery Django backend is removed.
   - Vendor config resolution moves from `backends/__init__.py::DatabaseWrapperMixin._build_native_config` into each `<vendor>/base.py`.
5. **Remaining fakes.**
   - `extensions/sqlalchemy/dbapi.py::DBAPIConnection` raises on URL parse errors instead of falling back to `SQLiteConfig(":memory:")`.
   - The `extensions/alchemy/cache.py` `shm://` scheme is either backed by a real Rust SHM slab (reusing the `broker.rs`/`vector.rs` seqlock), with a two-process cache-hit test, or removed.
6. **Docs.** Correct `domains/extensions/sqlspec-alchemy-and-dialects.md` (false claims: a `ShmRegion` L1 and "real" GCP dialects).

## Out of Scope

- sqlspec adapters (Chapter 4) and Django tasks (Chapter 5).

## Public Seams

Dialect entry points (`<engine>+vtb`, `<engine>+vtb_async`, `spangres+vtb`), `create_cache_config`, the dogpile backend names, and the Django `ENGINE` paths.

## Dependencies

native-data-platform `db-layout-and-contract` (DB-API over native drivers) and `broker-pool-sharing` (broker SQLAlchemy pool).

## Acceptance Boundary

- SQLAlchemy compile and reflection tests pass per dialect (GoogleSQL backtick quoting, `STRING` types).
- Alembic autogenerate works on Postgres and Spanner.
- Django `migrate` runs on Postgres and the Spanner emulator.
- `make lint-all` and `make py-test` pass.

## Draft Task Outline

- t01 Extract the DB-API module.
- t02 Split the dialect modules + built-in bases.
- t03 GCP dialects on the official bases.
- t04 Alembic Impl split.
- t05 AA dogpile backend + cache cleanup.
- t06 AA storage registration + URL scheme lookup.
- t07 Django built-in backends.
- t08 Django Spanner backend on `django_spanner` + BigQuery removal + docs.
