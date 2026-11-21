---
type: Task
id: "native-cursor-data-path:embedded:t01"
chapter_id: embedded-cursors
title: "DB-API exception names bind to the native classes"
description: "Replace the DB-API module's own exception class statements with bindings to the vertebra._vertebra classes, so one hierarchy serves the native drivers, the DB-API module, and both wrappers."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - "src/py/vertebra/db/dbapi/_sync.py:14-23"
  - "src/py/vertebra/db/dbapi/_sync.py:365-402"
  - "src/py/vertebra/db/dbapi/_sync.py:570-632"
  - "src/py/vertebra/db/dbapi/_sync.py:1526-1545"
  - "src/py/vertebra/_vertebra.pyi:1702-1746"
  - "src/py/tests/unit/db/test_dbapi_standalone.py:20-55"
  - "src/py/tests/unit/db/test_dbapi_standalone.py:868-881"
files:
  - src/py/vertebra/db/dbapi/_sync.py
tests:
  - src/py/tests/unit/db/test_dbapi_standalone.py
estimated_loc: 90
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-20T15:22:45Z
sources:
  - id: dbapi-errors
    resource: src/py/vertebra/db/dbapi/_sync.py::Error
    title: "DB-API exception names"
  - id: native-errors
    resource: src/py/vertebra/_vertebra.pyi::DatabaseError
    title: "Native DatabaseError hierarchy"
tags: [dbapi, errors]
---

# DB-API exception names bind to the native classes

## Objective & Scope Boundary

After this task, `vertebra.db.dbapi.IntegrityError` is the class `vertebra._vertebra.IntegrityError`, and the same holds for the other PEP 249 names. An error raised by an extension is caught by `except dbapi.<Name>` with no translation. The native cursors in later tasks rely on this.

In scope, all in `src/py/vertebra/db/dbapi/_sync.py`:

- The ten class statements at lines 365-402.
- The function `_translate_native_error` and the one branch of `_ErrorTranslator.__exit__` that calls it.

Out of scope: `_ErrorTranslator` itself, `_translate_vertebra_error`, `_TRANSLATE_ERRORS`, and `_translate_errors`. They still map configuration errors and stay until `network-engines` removes their last caller. `src/py/vertebra/db/dbapi/_async.py` and `src/py/vertebra/db/dbapi/__init__.py` import the names from this module and need no edit.

## Bounded Read Manifest

- `src/py/vertebra/db/dbapi/_sync.py:14-23` — `_native` is already imported as `vertebra._vertebra`.
- `src/py/vertebra/db/dbapi/_sync.py:365-402` — the ten class statements.
- `src/py/vertebra/db/dbapi/_sync.py:570-632` — `_translate_native_error`, `_translate_vertebra_error`, `_ErrorTranslator`, and `_translate_errors`.
- `src/py/vertebra/db/dbapi/_sync.py:1526-1545` — `SyncDBAPIModule` re-binds the names as class attributes.
- `src/py/vertebra/_vertebra.pyi:1702-1746` — the native hierarchy and its `sqlstate`, `vendor_code`, and `is_transient` attributes.
- `src/py/tests/unit/db/test_dbapi_standalone.py:20-55`, `868-881` — imports of both hierarchies and the connect-time mapping test.

## Contracts & Edge Cases

Replace the ten class statements with these bindings, in this order. `Warning` keeps its class statement and docstring.

```python
Error = _native.DatabaseError
DatabaseError = _native.DatabaseError
InterfaceError = _native.InterfaceError
DataError = _native.DataError
OperationalError = _native.OperationalError
IntegrityError = _native.IntegrityError
InternalError = _native.InternalError
ProgrammingError = _native.ProgrammingError
NotSupportedError = _native.NotSupportedError


class Warning(Exception):
    """PEP 249 DBAPI warning exception."""
```

The module docstring gains one sentence: the PEP 249 exception names are the `vertebra._vertebra` classes, so `InterfaceError` is a subclass of `DatabaseError` and `Error` is `DatabaseError`.

Translator changes:

- Delete `_translate_native_error`. Its only caller is the branch below.
- In `_ErrorTranslator.__exit__`, delete the two lines `if isinstance(exc, _native.DatabaseError):` and `raise _translate_native_error(exc) from exc`. The first check in that method already returns for every native error, because `Error` is now `_native.DatabaseError`.
- `_translate_vertebra_error` keeps its body. Its return annotation `Error` now names the native class.
- The `VertebraError` and `ValueError`/`TypeError` branches of `_ErrorTranslator.__exit__` are unchanged.

Sites that construct one of these exceptions from Python. Each passes one message string, which the native classes accept, so none needs an edit. Verified against the built extension: every class constructs with zero, one, or two positional arguments, accepts attribute assignment, and reads `sqlstate`, `vendor_code` as `None` and `is_transient` as `False` by default.

| File | Function | Class |
| --- | --- | --- |
| `_sync.py` | `_translate_vertebra_error` | `InterfaceError`, `DatabaseError` |
| `_sync.py` | `_ErrorTranslator.__exit__` | `InterfaceError` |
| `_sync.py` | `_validate_connect_kwargs` (two sites) | `InterfaceError` |
| `_sync.py` | `_resolve_dbapi_config` (two sites) | `InterfaceError` |
| `_sync.py` | `_build_driver` | `InterfaceError` |
| `_sync.py` | `SyncDBAPICursor.fetchmany` | `ProgrammingError` |
| `_sync.py` | `SyncDBAPICursor._require_open` | `ProgrammingError`, `InterfaceError` |
| `_sync.py` | `SyncDBAPIConnection.autocommit` setter | `NotSupportedError` |
| `_sync.py` | `SyncDBAPIConnection._require_open` | `InterfaceError` |
| `_sync.py` | `SyncDBAPIModule.connect`, `connect` | `InterfaceError` |
| `_async.py` | `_build_async_driver` | `InterfaceError` |
| `_async.py` | `AsyncDBAPICursor.fetchmany` | `ProgrammingError` |
| `_async.py` | `AsyncDBAPICursor._require_open` | `ProgrammingError`, `InterfaceError` |
| `_async.py` | `AsyncDBAPIConnection.autocommit` setter | `NotSupportedError` |
| `_async.py` | `AsyncDBAPIConnection._require_open` | `InterfaceError` |
| `_async.py` | `AsyncDBAPIModule.connect`, `async_connect` | `InterfaceError` |

Edge cases:

- `SyncDBAPIModule` and `AsyncDBAPIModule` re-bind the names as class attributes (`Error = Error` and the rest). They pick up the native classes with no edit.
- An error that passes through `_ErrorTranslator` keeps its own class, its `vendor_code`, and its traceback. `NotFoundError`, `QueryCancelledError`, `SerializationFailure`, `ConnectionFailure`, and `PoolExhaustedError` are no longer flattened to their PEP 249 parent.
- A translated error no longer has a `__cause__` that points at a second exception object.
- `Warning` is not part of the native hierarchy. `issubclass(Warning, Error)` is `False`, as before.
- If Mypy or Pyright rejects a binding, fix the annotation that uses it. Do not add a suppression comment.
- The file gets shorter. Later worksheets locate their ranges in this file by symbol name.

## Test Matrix

Add `test_dbapi_exception_names_are_native_classes` to `src/py/tests/unit/db/test_dbapi_standalone.py`. No existing test asserts that `InterfaceError` sits outside `DatabaseError`, so none is rewritten.

| Case | Input | Expected |
| --- | --- | --- |
| Identity | each of `InterfaceError`, `DataError`, `OperationalError`, `IntegrityError`, `InternalError`, `ProgrammingError`, `NotSupportedError`, `DatabaseError` | `getattr(dbapi, name) is getattr(vertebra._vertebra, name)` |
| `Error` | `dbapi.Error` | `is vertebra._vertebra.DatabaseError` |
| Recorded deviation | `InterfaceError` | `issubclass(dbapi.InterfaceError, dbapi.DatabaseError)` is `True` |
| `Warning` | `dbapi.Warning` | subclass of `Exception`, not of `dbapi.Error` |
| Module objects | `SyncDBAPIModule().IntegrityError`, `AsyncDBAPIModule().IntegrityError` | both are `vertebra._vertebra.IntegrityError` |
| Python construction | `dbapi.ProgrammingError("x")` | `str(exc) == "x"`, `exc.sqlstate is None`, `exc.is_transient is False` |
| Native error passes through | `with _translate_errors(): raise vertebra._vertebra.QueryCancelledError("x")` | the same object is raised; it is a `dbapi.OperationalError` |
| Configuration error | `with _translate_errors(): raise ImproperConfigurationError("x")` | `dbapi.InterfaceError` |
| Removed name | `hasattr(vertebra.db.dbapi._sync, "_translate_native_error")` | `False` |
| Connect-time mapping | existing assertions at lines 868-881 | pass unchanged |

## Verification

- Red then green: `uv run --no-sync pytest src/py/tests/unit/db/test_dbapi_standalone.py -q -k native_classes`
- File suites: `uv run --no-sync pytest src/py/tests/unit/db/test_dbapi_standalone.py src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py -q`
- Removal check, must print nothing: `git grep -n "_translate_native_error" -- src/py`
- Types and lint: `uv run --no-sync ruff check src/py/vertebra/db/dbapi && uv run --no-sync mypy src/py/vertebra/db/dbapi && uv run --no-sync pyright src/py/vertebra/db/dbapi`

## Acceptance Criteria

- [ ] The nine PEP 249 error names are bindings to the `vertebra._vertebra` classes, and `Warning` is the only exception class the module defines.
- [ ] `_translate_native_error` is gone and `_ErrorTranslator.__exit__` has no native-error branch.
- [ ] The new test passes and the two listed test files pass.
- [ ] `git diff --stat` shows only the two files above.

## Notes & Discoveries

- The test matrix asks for `hasattr(_sync, "_translate_native_error")` and the removal check requires `git grep "_translate_native_error" -- src/py` to print nothing. The test builds the name as `"_translate_native" + "_error"` so both hold.
- The matrix is covered by two tests that match `-k native_classes`: the parametrized `test_dbapi_exception_names_are_native_classes` (identity on the package, `_sync`, and both module objects) and `test_dbapi_native_classes_keep_hierarchy_and_pass_through_translator` (hierarchy, construction, translator, removed name).
- `make lint` was not run. It runs `ruff --fix --unsafe-fixes`, `cargo fmt --all`, and `cargo clippy --fix` across the whole repository, and `src/rs/vertebra-sqlite/tests/sqlite_client.rs` carried another writer's uncommitted edits during this job. Ruff check, Ruff format check, Mypy, and Pyright ran on `src/py/vertebra/db/dbapi` and the test file only. The TypeScript, Rust, `check_extras`, and stubtest parts of `make lint` did not run.
- `uv run` needs `UV_NO_CACHE=1` in the worker sandbox because `~/.cache/uv` is read-only there.
- The caveats are not yet copied into `.agents/bundles/knowledge/domains/data/database-driver-limitations.md`: `dbapi.Error is dbapi.DatabaseError`, `InterfaceError` is a `DatabaseError` subclass (PEP 249 places it beside `DatabaseError`), and native errors keep their subclass instead of being flattened to the PEP 249 parent.
