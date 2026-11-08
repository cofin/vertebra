---
type: Task
id: "driver-fail-loud:ch02:t02"
chapter_id: ch02-duckdb-sqlalchemy-queue-fail-loud
title: SQLAlchemy DB-API connections raise InterfaceError on unparsable URLs
description: Replace the SQLiteConfig parse fallback in SyncDBAPIConnection and AsyncDBAPIConnection with the PEP 249 InterfaceError chained from the parse ValueError.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - src/py/vertebra/extensions/sqlalchemy/dialects.py
  - src/py/vertebra/db/registry.py
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
files:
  - src/py/vertebra/extensions/sqlalchemy/dialects.py
tests:
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
estimated_loc: 70
plan_revision: 1
plan_commit: 0acdf576
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-07T00:00:00Z"
updated_at: "2026-11-07T00:00:00Z"
commit: null
sources:
  - id: sqla-dbapi
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIConnection
    title: Sync DB-API connection with SQLite parse fallback
  - id: sqla-async-dbapi
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::AsyncDBAPIConnection
    title: Async DB-API connection with SQLite parse fallback
  - id: sqla-errors
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::InterfaceError
    title: PEP 249 InterfaceError exposed on SyncDBAPIModule and AsyncDBAPIModule
  - id: registry-parse
    resource: src/py/vertebra/db/registry.py::DatabaseRegistry.parse_url
    title: URL parser raising ValueError for unsupported schemes
tags:
  - sqlalchemy
  - dbapi
  - fail-loud
---

# Task t02: SQLAlchemy DB-API Connections Raise `InterfaceError` on Unparsable URLs

## Objective & Scope Boundary

`SyncDBAPIConnection.__init__` and `AsyncDBAPIConnection.__init__` stop substituting `SQLiteConfig(database=database or ":memory:")` when `DatabaseRegistry.parse_url` fails and raise the module's PEP 249 `InterfaceError` (`vertebra.extensions.sqlalchemy.dialects::InterfaceError`, also exposed as `SyncDBAPIModule.InterfaceError` / `AsyncDBAPIModule.InterfaceError` and re-exported from `vertebra.extensions.sqlalchemy`) chained `from` the `ValueError`.[^sqla-dbapi][^sqla-async-dbapi][^sqla-errors][^registry-parse]

Out of scope: the raw `sqlite3` connection (`_sqlite_conn`) opened for genuine SQLite URLs, the `import sqlite3` and `SQLiteConfig` imports that path still uses, the `connect` default URLs in `SyncDBAPIModule` / `AsyncDBAPIModule`, and DB-API error mapping for execution failures (all owned by `sqlalchemy-and-django-integration`).

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlalchemy/dialects.py` — `InterfaceError`, `Error`, `SyncDBAPIConnection.__init__`, `SyncDBAPIModule.connect`, `AsyncDBAPIConnection.__init__`, `AsyncDBAPIModule.connect`.[^sqla-dbapi][^sqla-async-dbapi][^sqla-errors]
- `src/py/vertebra/db/registry.py` — `DatabaseRegistry.parse_url` (raises `ValueError(f"Unsupported database URL scheme: {scheme!r}")`).[^registry-parse]
- `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` — `test_sync_dbapi_connection_driver_ownership_and_broker_isolation`, `test_async_dbapi_connection_driver_ownership_broker_isolation_and_close_settlement` (placement and style).

## Symbol & Signature Contracts

- `SyncDBAPIConnection.__init__(self, url: str = "sqlite+vtb:///:memory:", *, driver: Any = None, broker: Any = None, share_pool: bool = True, database: str | None = None, **kwargs: Any) -> None` — signature unchanged. The parse block becomes:

```python
try:
    self._config: Any = (
        DatabaseRegistry.parse_url(resolved_url, broker=self._broker)
        if self._broker is not None
        else DatabaseRegistry.parse_url(resolved_url)
    )
except ValueError as exc:
    msg = f"Cannot parse Vertebra database URL: {exc}"
    raise InterfaceError(msg) from exc
```

- `self._config: Any = None` before the `try` is deleted; `self._config` is assigned only by the successful parse, and that assignment carries the `Any` annotation in both constructors. Reason: without it mypy infers the `AnyDatabaseConfig` union from `DatabaseRegistry.parse_url` and reports `union-attr` on the later `self._config.build_sync_driver(...)` / `self._config.build_async_driver(...)` calls.
- `AsyncDBAPIConnection.__init__(self, url: str = "sqlite+vtb_async:///:memory:", ...)` — identical parse block, including `self._config: Any = (...)` inside the `try`.
- Both docstrings become Google style: summary line, `Args:` (`url`, `driver`, `broker`, `share_pool`, `database`, `kwargs`), `Raises: InterfaceError: When url cannot be parsed into a Vertebra database configuration.`
- `except ValueError` covers `ImproperConfigurationError` (a `ValueError` subclass) raised by `from_url`; other exception types propagate unchanged.

## Edge Cases & Error Semantics

- Unsupported scheme `"nonsense+vtb://host/db"`: `InterfaceError("Cannot parse Vertebra database URL: Unsupported database URL scheme: 'nonsense+vtb'")`; `exc.__cause__` is a `ValueError` with `str(...) == "Unsupported database URL scheme: 'nonsense+vtb'"`.
- Async unsupported scheme `"nonsense+vtb_async://host/db"`: same, with scheme `'nonsense+vtb_async'`.
- Empty URL `""` with `database=None`: `parse_url("")` raises `ValueError` (`Unsupported database URL scheme: ''`), so the constructor raises `InterfaceError`.
- Empty URL `""` with `database="app.db"`: resolves to `sqlite+vtb:///app.db` and parses; unchanged.
- `driver=` supplied with an unparsable URL: still raises `InterfaceError`; the injected driver is not closed or modified by the constructor.
- `InterfaceError` is an `Error` subclass, so `except dialects.Error` and SQLAlchemy's `dbapi.Error` handling both catch it.
- The raised message contains only the scheme from `parse_url`, never credentials from the URL.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py`, next to the DB-API ownership tests; `InterfaceError` is imported from `vertebra.extensions.sqlalchemy` (add it to the existing import block).
- **Dependency category**: in-process (`DatabaseRegistry.parse_url`); no fakes.
- **New tests** (both plain `def`; the async constructor is synchronous):
  - `test_sync_dbapi_connection_rejects_unparsable_url() -> None`
  - `test_async_dbapi_connection_rejects_unparsable_url() -> None`
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Sync unsupported scheme | `SyncDBAPIConnection("nonsense+vtb://host/db", share_pool=False)` | `pytest.raises(InterfaceError, match=r"Cannot parse Vertebra database URL: Unsupported database URL scheme: 'nonsense\+vtb'")`; `isinstance(exc.value.__cause__, ValueError)` |
| Sync via module connect | `SyncDBAPIModule().connect("nonsense+vtb://host/db")` | `InterfaceError`; `SyncDBAPIModule.InterfaceError is InterfaceError` |
| Sync empty URL | `SyncDBAPIConnection("", share_pool=False)` | `InterfaceError`; `str(exc.value.__cause__) == "Unsupported database URL scheme: ''"` |
| Sync injected driver | `driver = SyncSQLiteDriver()`; `SyncDBAPIConnection("nonsense+vtb://host/db", driver=driver, share_pool=False)`; `driver.close()` in `finally` | `InterfaceError` |
| Async unsupported scheme | `AsyncDBAPIConnection("nonsense+vtb_async://host/db", share_pool=False)` | `pytest.raises(InterfaceError, match=r"'nonsense\+vtb_async'")`; `isinstance(exc.value.__cause__, ValueError)` |
| Async via module connect | `dialects.AsyncDBAPIModule().connect("nonsense+vtb_async://host/db")` | `InterfaceError`; `dialects.AsyncDBAPIModule.InterfaceError is InterfaceError` |
| Genuine SQLite URL unchanged | Existing `test_sqlite_vtb_sync_engine_ddl_dml_and_rollback` | Passes unchanged |

## Verification

- Initial Red baseline (tests written, constructors unchanged; both fail because a SQLite connection is returned): `uv run pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py -k "rejects_unparsable_url" -q`
- Final Green:
  - `uv run pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py -q`
  - `uv run pytest src/py/tests/unit/db/test_enterprise_olap_drivers.py -k "sqlalchemy" -q`
  - `uv run ruff check src/py/vertebra/extensions/sqlalchemy/dialects.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py`
  - `uv run mypy src/py/vertebra/extensions/sqlalchemy/dialects.py`
  - `! grep -n 'SQLiteConfig(database=database or ":memory:"' src/py/vertebra/extensions/sqlalchemy/dialects.py`

## Acceptance Criteria

- [ ] `SyncDBAPIConnection` and `AsyncDBAPIConnection` raise `InterfaceError` with a `ValueError` `__cause__` for unparsable URLs (R4).
- [ ] An injected `driver=` does not bypass URL parsing (R4).
- [ ] Genuine SQLite URLs still open `_sqlite_conn`; existing SQLAlchemy tests pass unchanged (R4).
- [ ] Both constructors carry Google docstrings with `Raises: InterfaceError`; no inline `#` comments in touched Python files.

## Notes & Discoveries

- [2026-11-07 00:00] `create_engine("vertebra+vtb://...")` never reaches the DB-API: `vertebra` is not a registered SQLAlchemy dialect name, so SQLAlchemy raises `NoSuchModuleError` first. The public seams that reach the parse are the connection constructors and `SyncDBAPIModule.connect` / `AsyncDBAPIModule.connect`.

[^sqla-dbapi]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIConnection` — Sync DB-API connection with SQLite parse fallback.
[^sqla-async-dbapi]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::AsyncDBAPIConnection` — Async DB-API connection with SQLite parse fallback.
[^sqla-errors]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::InterfaceError` — PEP 249 `InterfaceError` exposed on `SyncDBAPIModule` and `AsyncDBAPIModule`.
[^registry-parse]: `src/py/vertebra/db/registry.py::DatabaseRegistry.parse_url` — URL parser raising `ValueError` for unsupported schemes.
