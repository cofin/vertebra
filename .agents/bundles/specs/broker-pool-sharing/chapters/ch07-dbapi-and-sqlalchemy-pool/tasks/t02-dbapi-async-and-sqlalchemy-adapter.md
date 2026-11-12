---
type: Task
id: "broker-pool-sharing:ch07:t02"
chapter_id: ch07-dbapi-and-sqlalchemy-pool
title: Extract async DB-API classes and the SQLAlchemy async adapter
description: Move AsyncDBAPICursor and AsyncDBAPIConnection into vertebra.db.dbapi._async, export them from vertebra.db.dbapi, and create extensions/sqlalchemy/async_adapter.py owning AsyncDBAPIModule and the AsyncAdapt_dbapi_connection wrapping.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "broker-pool-sharing:ch07:t01"
read_set:
  - src/py/vertebra/extensions/sqlalchemy/dialects.py
  - src/py/vertebra/db/dbapi/__init__.py
  - src/py/vertebra/db/dbapi/_sync.py
  - src/py/tests/unit/db/test_dbapi.py
  - .agents/bundles/specs/broker-pool-sharing/chapters/ch07-dbapi-and-sqlalchemy-pool/spec.md
files:
  - src/py/vertebra/db/dbapi/_async.py
  - src/py/vertebra/db/dbapi/__init__.py
  - src/py/vertebra/extensions/sqlalchemy/async_adapter.py
tests:
  - src/py/tests/unit/db/test_dbapi.py
estimated_loc: 230
plan_revision: 3
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
created_at: "2026-11-12T01:40:00Z"
updated_at: "2026-11-12T01:40:00Z"
commit: null
sources:
  - id: async-cursor
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::AsyncDBAPICursor
    title: Async cursor reporting execution metrics to AsyncAdapt
  - id: async-conn
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::AsyncDBAPIConnection
    title: Async connection (URL/driver resolution, autocommit)
  - id: async-module
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::AsyncDBAPIModule
    title: Async module returning AsyncAdapt_dbapi_connection
tags:
  - dbapi
  - sqlalchemy
  - async
---

# Task t02: Extract Async DB-API Classes and the SQLAlchemy Async Adapter

## Objective & Scope Boundary

1. `src/py/vertebra/db/dbapi/_async.py`: `AsyncDBAPICursor` and `AsyncDBAPIConnection` moved from `dialects.py`, importing `is_row_returning_sql` and `STATEMENT_AUTOCOMMIT_DIALECTS` from `_sync.py` and errors from `_errors.py` [^async-cursor] [^async-conn]. Engine `isinstance` checks become dialect ClassVar checks as in `t01`.
2. `src/py/vertebra/db/dbapi/__init__.py`: add `AsyncDBAPICursor`, `AsyncDBAPIConnection` to imports and `__all__`.
3. `src/py/vertebra/extensions/sqlalchemy/async_adapter.py`: `AsyncDBAPIModule` (PEP 249 globals plus error attributes from `vertebra.db.dbapi`) whose `connect` returns `AsyncAdapt_dbapi_connection(self, AsyncDBAPIConnection(url, **kwargs))` [^async-module]. This is the only module in the package graph that imports `sqlalchemy.connectors.asyncio`.

`dialects.py` is untouched; `t03` rewires it.

## Bounded Read Manifest

- `dialects.py` — `AsyncDBAPICursor`, `AsyncDBAPIConnection`, `AsyncDBAPIModule` [^async-cursor] [^async-conn] [^async-module].
- `db/dbapi/__init__.py`, `db/dbapi/_sync.py` — from `t01`.
- `test_dbapi.py` — boundary test from `t01`.

## Symbol & Signature Contracts

```python
# vertebra.db.dbapi._async
class AsyncDBAPICursor:
    rowcount: int
    lastrowid: int | None
    async def execute(self, operation: str, parameters: Any = None) -> AsyncDBAPICursor: ...
    async def executemany(self, operation: str, seq_of_parameters: Sequence[Any]) -> AsyncDBAPICursor: ...
class AsyncDBAPIConnection:
    def __init__(self, url: str = "sqlite+vtb_async:///:memory:", *, driver: Any = None, broker: Any = None, share_pool: bool = True, database: str | None = None, **kwargs: Any) -> None: ...

# vertebra.extensions.sqlalchemy.async_adapter
class AsyncDBAPIModule:
    apilevel: str = "2.0"
    threadsafety: int = 2
    paramstyle: str
    def __init__(self, *, paramstyle: str = "qmark") -> None: ...
    def connect(self, *args: Any, **kwargs: Any) -> AsyncAdapt_dbapi_connection: ...
```

## Edge Cases & Error Semantics

- Async cursor rowcount semantics follow HEAD: DML → affected rows; row-returning → `len(rows)`; unknown driver result shape → `-1`.
- Unknown attribute on `AsyncDBAPIModule` → `AttributeError` naming the attribute (HEAD `__getattr__` behavior).
- `connect()` with no URL → `sqlite+vtb_async:///:memory:`.

## Public-Seam Test Blueprint

| Test | Input | Expected |
| --- | --- | --- |
| `test_async_cursor_select_rowcount` | `AsyncDBAPIConnection("sqlite+vtb_async:///:memory:")`, `select 1 as x` | `rowcount == 1`, `description[0][0] == "x"` |
| `test_async_cursor_dml_rowcount` | create + insert 2 rows via `executemany` | `rowcount == 2` |
| `test_async_autocommit_rejected[bigquery]` | config with `dialect == "bigquery"`, `autocommit=False` | `NotSupportedError` |
| `test_async_adapter_module_connect_wraps` | `AsyncDBAPIModule().connect()` | instance of `AsyncAdapt_dbapi_connection` |
| `test_dbapi_import_boundary` (extended) | same subprocess check after `t02` | still `[]` |

## Verification

- `make dev && uv run --no-sync pytest src/py/tests/unit/db/test_dbapi.py -q`
- `uv run --no-sync mypy src/py/vertebra/db/dbapi src/py/vertebra/extensions/sqlalchemy/async_adapter.py`

## Acceptance Criteria

- `vertebra.db.dbapi._async` has no `sqlalchemy` import; `async_adapter.py` holds the only `AsyncAdapt_dbapi_connection` import outside `dialects.py` (which `t03` removes).
- Matrix tests pass.

## Notes & Discoveries

- At HEAD the async adaptation lives in `dialects.py::AsyncDBAPIModule.connect` (verified with `git grep -n AsyncAdapt_dbapi_connection src/py/vertebra`); no other module imports it.
