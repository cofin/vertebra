---
type: Spec
flow_id: sqlalchemy-and-django-integration
chapter_id: ch01-dbapi-hardening-and-dialect-base
title: "Chapter 01: DB-API Hardening, Engine-Neutral Config Metadata & Dialect Foundation"
description: "Harden shared vertebra.db.dbapi over native drivers without stdlib sqlite3 or default URLs, replace concrete config isinstance checks with BaseDatabaseConfig metadata, and build dialect mixins and SQLAlchemy types."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
updated_at: "2026-11-12T23:45:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:45:00Z"
    commit: 18baadcd
tags:
  - sqlalchemy
  - dbapi
  - dialects
sources:
  - id: pyproject-deps
    resource: pyproject.toml
    title: "Project optional dependencies and entry points"
  - id: sa-dialects
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py
    title: "Current monolithic SQLAlchemy dialect and DB-API module"
  - id: sa-types
    resource: src/py/vertebra/extensions/sqlalchemy/types.py
    title: "SQLAlchemy custom vector and JSON types"
  - id: db-common
    resource: src/py/vertebra/db/driver/_common.py
    title: "BaseDatabaseConfig contract"
---

# Chapter 01: DB-API Hardening, Engine-Neutral Config Metadata & Dialect Foundation

<!-- truth: start -->
- **Baseline (`18baadcd`)**: `SyncDBAPIConnection` and `AsyncDBAPIConnection` currently open a second stdlib `sqlite3` connection (`_sqlite_conn`), default `url` to `sqlite+vtb:///:memory:`, and check `isinstance(config, (BigQueryConfig, SpannerConfig, SQLiteConfig))` [^sa-dialects] [^db-common].
- **Target Architecture**: Shared `vertebra.db.dbapi` (extracted in `broker-pool-sharing:ch07`) requires an explicit URL (`InterfaceError` when omitted), executes SQLite exclusively via `SyncSQLiteDriver`/`AsyncSQLiteDriver`, and reads engine-neutral `BaseDatabaseConfig.requires_statement_autocommit` and `effective_direct_dsn` [^pyproject-deps].
- **Scope**: `pyproject.toml` extras (`dogpile.cache>=1.3.3`, `django>=6.0`), `db/dbapi/{_sync,_async}.py` hardening, `extensions/sqlalchemy/{async_adapter,engine,types}.py`, and `dialects/_base.py` [^sa-types].
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter hardens the shared PEP 249 package (`vertebra.db.dbapi`) after `broker-pool-sharing:ch07` extracts it, eliminates concrete adapter `isinstance` checks via `BaseDatabaseConfig` metadata, isolates SQLAlchemy greenlet adaptation in `extensions/sqlalchemy/async_adapter.py`, extracts `engine.py`, and builds `_SyncDialectMixin` / `_AsyncDialectMixin` plus custom vector/JSON SQLAlchemy types [^sa-dialects] [^sa-types].

## 2. Requirements, Concrete Contracts & Architecture

- **R1 (`db/dbapi` Purity & URL Requirement)**: `SyncDBAPIModule.connect(url)` and `AsyncDBAPIModule.connect(url)` raise `InterfaceError` on empty/missing URLs, delete `_sqlite_conn` and stdlib `sqlite3`, and import zero SQLAlchemy, Django, framework, or engine-adapter modules.
- **R2 (Engine-Neutral Config Metadata)**: `BaseDatabaseConfig` declares `requires_statement_autocommit: ClassVar[bool] = False` and `effective_direct_dsn -> str`.
- **R3 (`_SyncDialectMixin` / `_AsyncDialectMixin` & `types.py`)**: Mixins wire `import_dbapi`, full-URL `create_connect_args`, broker-aware `get_pool_class`, and `is_disconnect`; `types.py` implements `PgVector`, `HalfVector`, `SparseVector`, `OracleVector`, `SpannerVector`, `BigQueryGeography`, and `MsgspecJSON`.

```python
class BaseDatabaseConfig(msgspec.Struct, frozen=True):
    requires_statement_autocommit: ClassVar[bool] = False

    @property
    def effective_direct_dsn(self) -> str: ...

class SyncDBAPIModule:
    paramstyle: ClassVar[str] = "qmark"
    threadsafety: ClassVar[int] = 2
    apilevel: ClassVar[str] = "2.0"

    @staticmethod
    def connect(url: str, *, autocommit: bool | None = None, **kwargs: Any) -> SyncDBAPIConnection: ...

class AsyncDBAPIModule:
    @staticmethod
    async def connect(url: str, *, autocommit: bool | None = None, **kwargs: Any) -> AsyncDBAPIConnection: ...
```

## 3. Documentation & OKF Knowledge Plan

Public documentation and OKF updates for DB-API hardening and custom SQLAlchemy types land in `ch07-sqlalchemy-django-docs-and-okf`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R1` DB-API hardening & extras | `t01` | `pyproject.toml`, `src/py/vertebra/db/dbapi/{_sync,_async}.py` | `pytest test_sqlalchemy_and_sqlspec_vtb.py` |
| `R2` Config metadata & async adapter | `t02` | `src/py/vertebra/db/driver/_common.py`, `extensions/sqlalchemy/{async_adapter,engine}.py` | `pytest test_sqlalchemy_and_sqlspec_vtb.py` |
| `R3` Dialect mixins & native types | `t03` | `extensions/sqlalchemy/dialects/_base.py`, `extensions/sqlalchemy/types.py` | `pytest test_sqlalchemy_and_sqlspec_vtb.py` |

## 5. Implementation Plan

- [ ] [t01-dependencies-and-dbapi-hardening.md](tasks/t01-dependencies-and-dbapi-hardening.md) — Add `dogpile.cache>=1.3.3` / `django>=6.0` extras and harden `vertebra.db.dbapi` (explicit URL requirement, zero stdlib `sqlite3`).
- [ ] [t02-config-metadata-and-async-adapter.md](tasks/t02-config-metadata-and-async-adapter.md) — Add engine-neutral `BaseDatabaseConfig` metadata and harden `extensions/sqlalchemy/{async_adapter,engine}.py`.
- [ ] [t03-dialect-base-and-native-types.md](tasks/t03-dialect-base-and-native-types.md) — Create `_SyncDialectMixin` / `_AsyncDialectMixin` in `dialects/_base.py` and complete vector/JSON types in `types.py`.

## 6. Current State & Remaining Plan Snapshot

Chapter `ch01-dbapi-hardening-and-dialect-base` is `planned` behind upstream [`../../spec.md`](../../spec.md) prerequisites (`broker-pool-sharing` and `native-sqlite-driver`).

Fast Chapter Gate (`< 60s` offline):

```bash
uv run --no-sync ruff check src/py/vertebra/db/driver src/py/vertebra/extensions/sqlalchemy && uv run --no-sync mypy src/py/vertebra/db/driver src/py/vertebra/extensions/sqlalchemy && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/db/test_optional_driver_wheels.py
```

[^pyproject-deps]: `pyproject.toml` — Project optional dependencies and entry points.
[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py` — Current monolithic SQLAlchemy dialect and DB-API module.
[^sa-types]: `src/py/vertebra/extensions/sqlalchemy/types.py` — SQLAlchemy custom vector and JSON types.
[^db-common]: `src/py/vertebra/db/driver/_common.py` — BaseDatabaseConfig contract.
