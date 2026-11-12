---
type: Task
id: "broker-pool-sharing:ch07:t01"
chapter_id: ch07-dbapi-and-sqlalchemy-pool
title: Extract sync DB-API classes and errors into vertebra.db.dbapi
description: Create vertebra.db.dbapi with the PEP 249 error hierarchy, module globals, SyncDBAPICursor, SyncDBAPIConnection, SyncDBAPIModule, and is_row_returning_sql, replacing engine-config isinstance checks with the dialect ClassVar, plus canonical behavior and import-boundary tests.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - src/py/vertebra/extensions/sqlalchemy/dialects.py
  - src/py/vertebra/db/registry.py
  - src/py/vertebra/db/driver/_common.py
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
  - .agents/bundles/specs/broker-pool-sharing/chapters/ch07-dbapi-and-sqlalchemy-pool/spec.md
files:
  - src/py/vertebra/db/dbapi/__init__.py
  - src/py/vertebra/db/dbapi/_errors.py
  - src/py/vertebra/db/dbapi/_sync.py
tests:
  - src/py/tests/unit/db/test_dbapi.py
estimated_loc: 250
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
  - id: sync-cursor
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPICursor
    title: Sync cursor (execute, executemany, rowcount, lastrowid)
  - id: sync-conn
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIConnection
    title: Sync connection (URL/driver resolution, autocommit, sqlite3 fast path)
  - id: row-returning
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::_is_row_returning_sql
    title: Statement-kind classifier
  - id: registry
    resource: src/py/vertebra/db/registry.py::DatabaseRegistry
    title: URL parsing and shared driver resolution
tags:
  - dbapi
  - pep249
---

# Task t01: Extract Sync DB-API Classes and Errors into `vertebra.db.dbapi`

## Objective & Scope Boundary

1. `src/py/vertebra/db/dbapi/_errors.py`: `Warning`, `Error`, `InterfaceError`, `DatabaseError`, `DataError`, `OperationalError`, `IntegrityError`, `InternalError`, `ProgrammingError`, `NotSupportedError`, and `Binary`, moved verbatim from `dialects.py`.
2. `src/py/vertebra/db/dbapi/_sync.py`: `SyncDBAPICursor`, `SyncDBAPIConnection`, `SyncDBAPIModule`, `is_row_returning_sql` (public rename of `_is_row_returning_sql`), and `ROW_RETURNING_LEADING_KEYWORDS` moved from `dialects.py` [^sync-cursor] [^sync-conn] [^row-returning]. Replace `isinstance(self._config, (BigQueryConfig, SpannerConfig))` with `type(self._config).dialect in STATEMENT_AUTOCOMMIT_DIALECTS` (`frozenset({"bigquery", "spanner"})`) and `isinstance(self._config, SQLiteConfig)` with `type(self._config).dialect == "sqlite"`.
3. `src/py/vertebra/db/dbapi/__init__.py`: module docstring, PEP 249 globals (`apilevel = "2.0"`, `threadsafety = 2`, `paramstyle = "qmark"`), `connect(url, **kwargs)`, and `__all__` exporting the errors and sync classes.
4. `src/py/tests/unit/db/test_dbapi.py` (new canonical test module) with behavior and import-boundary tests.

`dialects.py` keeps its embedded copies until `t03` deletes them; this task does not touch it.

Out of scope: async classes (`t02`), dialect rewiring (`t03`).

## Bounded Read Manifest

- `dialects.py` — error classes, `Binary`, `SyncDBAPICursor`, `SyncDBAPIConnection`, `SyncDBAPIModule`, `_is_row_returning_sql`, `_ROW_RETURNING_LEADING_KEYWORDS` [^sync-cursor] [^sync-conn] [^row-returning].
- `db/registry.py` — `DatabaseRegistry.parse_url`, `default_broker`, `get_shared_sync_driver` [^registry].
- `db/driver/_common.py` — `BaseDatabaseConfig`; adapter `dialect: ClassVar[str]` values (`bigquery`, `spanner`, `sqlite`).
- `test_sqlalchemy_and_sqlspec_vtb.py` — existing DB-API behavior tests to mirror.

## Symbol & Signature Contracts

```python
STATEMENT_AUTOCOMMIT_DIALECTS: Final[frozenset[str]] = frozenset({"bigquery", "spanner"})
ROW_RETURNING_LEADING_KEYWORDS: Final[frozenset[str]]
def is_row_returning_sql(operation: str) -> bool: ...
class SyncDBAPICursor:
    rowcount: int
    lastrowid: int | None
    description: Sequence[tuple[Any, ...]] | None
    def execute(self, operation: str, parameters: Any = None) -> SyncDBAPICursor: ...
    def executemany(self, operation: str, seq_of_parameters: Sequence[Any]) -> SyncDBAPICursor: ...
class SyncDBAPIConnection:
    def __init__(self, url: str = "sqlite+vtb:///:memory:", *, driver: Any = None, broker: Any = None, share_pool: bool = True, database: str | None = None, **kwargs: Any) -> None: ...
class SyncDBAPIModule:
    def connect(self, *args: Any, **kwargs: Any) -> SyncDBAPIConnection: ...
def connect(url: str, **kwargs: Any) -> SyncDBAPIConnection: ...
```

## Edge Cases & Error Semantics

Contract decisions recorded from HEAD behavior (kept unchanged):

- Statement kind: first non-comment, non-literal identifier token (skipping tokens prefixed by `:`, `@`, `$`) in `ROW_RETURNING_LEADING_KEYWORDS` → row-returning; otherwise a later standalone `RETURNING` token → row-returning; else DML.
- `rowcount`: DML → driver-reported affected rows from `execute_dml`; row-returning → `len(rows)`; `executemany` DML → sum of affected rows; raw sqlite3 cursor path → `cursor.rowcount`.
- `lastrowid`: forwarded from the raw sqlite3 cursor only; native driver paths leave `None`.
- `autocommit=False` on a statement-autocommit dialect raises `NotSupportedError` naming the dialect.

## Public-Seam Test Blueprint

| Test | Input | Expected |
| --- | --- | --- |
| `test_is_row_returning_sql[select]` | `"select 1"` | `True` |
| `test_is_row_returning_sql[comment_prefix]` | `"-- x\n/* y */ WITH a AS (select 1) select * from a"` | `True` |
| `test_is_row_returning_sql[dml_returning]` | `"insert into t values (1) returning id"` | `True` |
| `test_is_row_returning_sql[literal_returning]` | `"insert into t values ('returning')"` | `False` |
| `test_is_row_returning_sql[bind_named_returning]` | `"update t set a = :returning"` | `False` |
| `test_is_row_returning_sql[update]` | `"update t set a = 1"` | `False` |
| `test_sqlite_cursor_rowcount_and_lastrowid` | `connect("sqlite+vtb:///:memory:")`, create + insert | `rowcount == 1`, `lastrowid == 1` |
| `test_select_description_and_rowcount` | `select 1 as x` | `description[0][0] == "x"`, `rowcount == 1` |
| `test_dbapi_import_boundary` | subprocess `python -c "import vertebra.db.dbapi, sys; print(sorted(m for m in sys.modules if m.startswith(('sqlalchemy','vertebra.extensions','vertebra.db.adapters'))))"` | prints `[]` |
| `test_module_globals` | `vertebra.db.dbapi` | `apilevel == "2.0"`, `threadsafety == 2`, `paramstyle == "qmark"` |

## Verification

- `make dev && uv run --no-sync pytest src/py/tests/unit/db/test_dbapi.py -q`
- `uv run --no-sync mypy src/py/vertebra/db/dbapi && uv run --no-sync ruff check src/py/vertebra/db/dbapi src/py/tests/unit/db/test_dbapi.py`

## Acceptance Criteria

- `vertebra.db.dbapi` imports without loading `sqlalchemy`, `vertebra.extensions`, or `vertebra.db.adapters.*`.
- All matrix tests pass.

## Notes & Discoveries

- If `vertebra.db.registry` itself imports `vertebra.db.adapters.*` at module top level, the boundary test restricts its assertion to modules imported by `vertebra.db.dbapi` beyond those already loaded by `import vertebra.db.registry`; the executor records which form applies.
