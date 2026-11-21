---
type: Task
id: "native-cursor-data-path:cleanup:t18"
chapter_id: cleanup-and-checks
title: "Typed doubles in the SQLAlchemy, DB-API, and SQL Server tests"
description: "Clear seventeen mypy errors in three test files: cursor doubles that declare attributes twice, one override with a narrower argument type, the Oracle Var attribute read on the DB-API module, an unfrozen config subclass, and an async broker double that overrides a sync method."
worksheet_format: compact
status: draft
state: completed
priority: P2
verification_strategy: static_validation
depends_on: []
read_set:
  - "src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py:1268-1350"
  - "src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py:1470-1565"
  - "src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py:2110-2135"
  - "src/py/tests/unit/db/oracle_test_doubles.py::_StubSyncOracleClient.execute"
  - "src/py/tests/unit/db/test_dbapi_standalone.py::test_dbapi_oracle_module_exposes_native_out_bind_var"
  - "src/py/tests/unit/db/test_dbapi_standalone.py::test_sync_dbapi_connect_maps_improper_configuration_to_interface_error"
  - "src/py/tests/unit/db/test_dbapi_standalone.py:596-606"
  - "src/py/vertebra/db/dbapi/_sync.py::_apply_dialect_metadata"
  - "src/py/tests/unit/db/test_mssql_driver.py::_RecordingBroker"
  - "src/py/tests/unit/db/test_mssql_driver.py::_AsyncRecordingBroker"
  - "src/py/tests/unit/db/test_mssql_driver.py::test_async_mssql_driver_checkout_lease_lifecycle"
files: []
tests:
  - src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py
  - src/py/tests/unit/db/test_dbapi_standalone.py
  - src/py/tests/unit/db/test_mssql_driver.py
estimated_loc: 80
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-21T16:23:03Z
sources:
  - id: sqlalchemy-tests
    resource: src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py
    title: "SQLAlchemy dialect and DB-API tests with Oracle cursor doubles"
  - id: dbapi-tests
    resource: src/py/tests/unit/db/test_dbapi_standalone.py::test_dbapi_oracle_module_exposes_native_out_bind_var
    title: "DB-API module test for the Oracle Var attribute"
  - id: dbapi-metadata
    resource: src/py/vertebra/db/dbapi/_sync.py::_apply_dialect_metadata
    title: "Sets Var on the module instance for the oracle dialect"
  - id: mssql-broker-double
    resource: src/py/tests/unit/db/test_mssql_driver.py::_AsyncRecordingBroker
    title: "Async broker double that subclasses the sync one"
tags: [testing, typing, sqlalchemy, dbapi, mssql]
---

# t18 — Typed doubles in the SQLAlchemy, DB-API, and SQL Server tests

## Objective & Scope Boundary

`mypy` reports no error in `test_sqlalchemy_vtb_dialects_and_dbapi.py`, `test_dbapi_standalone.py`, and `test_mssql_driver.py`. It reports seventeen there today.[^sqlalchemy-tests][^dbapi-tests][^mssql-broker-double]

Every fix is in a test file. No test changes what it asserts. No production file changes.

## Bounded Read Manifest

- Lines `1268-1350`, `1470-1565`, and `2110-2135` of the SQLAlchemy test file: four cursor doubles and one stub client subclass.
- `_StubSyncOracleClient.execute` in the doubles file for the signature the subclass overrides. Read only; t17 owns that file.
- The two DB-API tests named in `read_set`, and lines `596-606` of the same file for a config subclass that is already declared frozen.
- `_apply_dialect_metadata` for how `Var` reaches the module object.[^dbapi-metadata]
- `_RecordingBroker`, `_AsyncRecordingBroker`, and the async lease test that takes both as a parameter.

## Contracts & Edge Cases

### `test_sqlalchemy_vtb_dialects_and_dbapi.py`, thirteen errors

Twelve `no-redef` errors, three in each of four cursor doubles nested in tests: `_SyncOracleVersionCursor`, `_AsyncOracleVersionCursor`, `_SyncOracleReturningCursor`, `_AsyncOracleReturningCursor`.

Each class annotates `_rows`, `description`, and `rowcount` inside `__init__`. Each class also names a driver class that is defined later in the same function, so `mypy` checks `execute` first and then rejects the annotations in `__init__`. Declare the three attributes once, at class level, in all four classes:

```python
class _SyncOracleVersionCursor:
    _rows: list[tuple[Any, ...]]
    description: tuple[tuple[Any, ...], ...] | None
    rowcount: int

    def __init__(self, driver: _SyncOracleVersionDriver) -> None:
        self._driver = driver
        self._rows = []
        self.description = None
        self.rowcount = -1
```

The other three classes get the same three class-level lines and the same three plain assignments. Their other methods stay.

One `override` error on `_OutBindBlockClient.execute`. Change its parameter to `enable_lowercase_column_names: bool | None = None`, as `_StubSyncOracleClient.execute` declares it. The body discards the value.

### `test_dbapi_standalone.py`, three errors

1. Two `attr-defined` errors in `test_dbapi_oracle_module_exposes_native_out_bind_var`. `Var` is set on the module instance, only for the `oracle` dialect, so the class has no such attribute. Read the instance attribute through `vars`:

```python
assert vars(sync_oracle_mod)["Var"] is oracle_ext.OracleOutBindVar
assert vars(async_oracle_mod)["Var"] is oracle_ext.OracleOutBindVar
```

2. One `misc` error on `_BadConfig` in `test_sync_dbapi_connect_maps_improper_configuration_to_interface_error`. The class header becomes `class _BadConfig(driver_common.BaseDatabaseConfig, frozen=True):`, the form `_FailingConfig` in the same file already uses.

### `test_mssql_driver.py`, one error

`_AsyncRecordingBroker` subclasses `_RecordingBroker` and overrides the sync `acquire` with a coroutine. Make it a separate class that holds a sync broker:

```python
class _AsyncRecordingBroker:
    """Async recording broker double whose `acquire` is a coroutine."""

    def __init__(self, *, refuse: bool = False) -> None: ...

    @property
    def acquire_calls(self) -> int: ...

    @property
    def release_calls(self) -> int: ...

    async def acquire(self, pid: int, *, timeout_ms: int = 5000, bind_key_hash: int = 0) -> _RecordingLeaseGrant: ...

    def release(self, slot_index: int, generation: int) -> None: ...
```

- `__init__` stores `self._inner = _RecordingBroker(refuse=refuse)`.
- The two properties return the inner broker's counters.
- `acquire` awaits `asyncio.sleep(0)` and returns `self._inner.acquire(pid, timeout_ms=timeout_ms, bind_key_hash=bind_key_hash)`.
- `release` calls `self._inner.release(slot_index, generation)`.
- Each method keeps a one-line docstring.

In `test_async_mssql_driver_checkout_lease_lifecycle`, the parameter annotation becomes `broker_cls: type[_RecordingBroker] | type[_AsyncRecordingBroker]`. The `parametrize` list and the body stay.

Edge cases:

- This task runs before any `single-call-paths` task starts, so the three files are as the planning tree has them.
- When the body of the async lease test reads an attribute the new class lacks, add a read-only property for it that returns the inner broker's value. Do not restore the inheritance.
- When `mypy` reports a further error in these three files after the edits, fix it with an annotation in the test file. A fix that needs a production change stops the task with the error text.
- No `# type: ignore` is added.

## Test Matrix

| Check | Expected |
| --- | --- |
| `mypy` | No error in the three files |
| `pytest` on the three files | Same pass count as before the task; no new failure |
| `test_async_mssql_driver_checkout_lease_lifecycle` | All eight cases pass |
| `test_dbapi_oracle_module_exposes_native_out_bind_var` | Passes, or skips when `vertebra._oracle` is not built |
| `ruff check` on the three files | No finding |

## Verification

```bash
uv run --no-sync --frozen mypy | grep "test_sqlalchemy_vtb_dialects_and_dbapi.py\|test_dbapi_standalone.py\|test_mssql_driver.py"
uv run --no-sync --frozen pytest src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py src/py/tests/unit/db/test_dbapi_standalone.py src/py/tests/unit/db/test_mssql_driver.py -p no:xdist -q
uv run --no-sync --frozen ruff check src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py src/py/tests/unit/db/test_dbapi_standalone.py src/py/tests/unit/db/test_mssql_driver.py
git diff --stat -- src/py/vertebra src/rs
```

The `grep` prints nothing. `pytest` reports no failure. The last command prints nothing.

## Acceptance Criteria

- [ ] `mypy` reports no error in the three files.
- [ ] The four cursor doubles declare `_rows`, `description`, and `rowcount` once each.
- [ ] `_AsyncRecordingBroker` does not inherit from `_RecordingBroker`, and the async lease test passes for both broker classes.
- [ ] No `# type: ignore`, and no production file has a diff from this task.

[^sqlalchemy-tests]: `src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py`
[^dbapi-tests]: `src/py/tests/unit/db/test_dbapi_standalone.py::test_dbapi_oracle_module_exposes_native_out_bind_var`
[^dbapi-metadata]: `src/py/vertebra/db/dbapi/_sync.py::_apply_dialect_metadata`
[^mssql-broker-double]: `src/py/tests/unit/db/test_mssql_driver.py::_AsyncRecordingBroker`
