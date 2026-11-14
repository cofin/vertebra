---
type: Spec
flow_id: sqlspec-adapter-parity
chapter_id: ch01-sqlspec-core-driver-and-artifact-ref
title: 'Chapter 01: SQLSpec Dispatch Foundation, VTBARROW Provider & Storage ArtifactReference'
description: Bump sqlspec>=0.71.1 + adk extra, implement MyPyC-safe BaseSyncDriver / BaseAsyncDriver dispatch in _driver.py, cut provider.py over to VTBARROW ArrowCursor, and define vertebra.storage.ArtifactReference.
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 1b191388
updated_at: '2026-11-14T18:50:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-14T18:50:00Z'
verified:
- by: agent:plan-generator
  at: '2026-11-14T18:50:00Z'
  commit: 1b191388
tags:
- sqlspec
- driver
- arrow
- storage
sources:
- id: pyproject-sqlspec
  resource: pyproject.toml
  title: sqlspec floor and optional adk extra
- id: sqlspec-adapters
  resource: src/py/vertebra/extensions/sqlspec/adapters.py
  title: Current monolithic SQLSpec driver module
- id: sqlspec-provider
  resource: src/py/vertebra/extensions/sqlspec/provider.py
  title: Broker SQLSpec provider
- id: storage-store
  resource: src/py/vertebra/storage/_store.py
  title: Storage value objects
---

# Chapter 01: SQLSpec Dispatch Foundation, VTBARROW Provider & Storage ArtifactReference

<!-- truth: start -->
- **Baseline (`18baadcd`)**: `extensions/sqlspec/adapters.py` overrides `execute`/`select`, `provider.py` defines `SharedArrowBuffer`/`SharedArrowTable`, and `spanner.py` imports BigQuery `ObjectRef` [^sqlspec-adapters] [^sqlspec-provider].
- **Target Architecture**: `sqlspec>=0.71.1` natively resolves `adapter_name` and explicit `*_store_class` attributes with zero `_compat.py` monkeypatching; `_driver.py` implements `dispatch_execute*`; `provider.py` emits `VTBARROW` `ArrowCursor` frames; `vertebra.storage.ArtifactReference` provides engine-neutral artifact metadata [^pyproject-sqlspec] [^storage-store].
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter establishes the shared foundation for all 8 SQLSpec adapter packages: `sqlspec>=0.71.1` + optional `adk = ["google-adk>=1.0.0"]` (`uv.lock` already resolves `sqlspec==0.71.1`), uniform `(rows, column_names)` `query_with_columns` metadata on native drivers (delegating to existing `execute_with_description` + `hydrate_rows`), `_driver.py` template-method dispatch bases, `VTBARROW` `ArrowCursor` provider framing, and storage-owned `ArtifactReference` [^sqlspec-adapters] [^sqlspec-provider].

## 2. Requirements, Concrete Contracts & Architecture

- **R1 (`sqlspec>=0.71.1` & Zero Monkeypatching)**: Declare `sqlspec>=0.71.1` and `adk = ["google-adk>=1.0.0"]`; `_compat.py` must not exist.
- **R2 (`_driver.py` Dispatch Contract)**: Inherit `execute`/`select*` from `SyncDriverAdapterBase`/`AsyncDriverAdapterBase`, implement `dispatch_execute*`, `execute_stack` pipelining, non-mutating `StatementConfig` merge, `TypedParameter` unwrapping, `DatabaseError` mapping, and refcounted shared-pool `close()`.
- **R3 (`VTBARROW` & `ArtifactReference`)**: Replace `SharedArrowBuffer`/`SharedArrowTable` with `ArrowCursor`, remove `gc=False` on container structs, and export `vertebra.storage.ArtifactReference`.

```python
class ArtifactReference(msgspec.Struct, frozen=True):
    uri: str
    version: int
    content_type: str | None = None
    size: int | None = None
    updated: datetime | None = None
    signed_url: str | None = None

class BaseSyncDriver(SyncDriverAdapterBase):
    def with_cursor(self, connection: Any) -> AbstractContextManager[Any]: ...
    def handle_database_exceptions(self) -> AbstractContextManager[None]: ...
    def dispatch_execute(self, cursor: Any, statement: SQL) -> ExecutionResult: ...
    def dispatch_execute_many(self, cursor: Any, statement: SQL) -> ExecutionResult: ...
    def dispatch_execute_script(self, cursor: Any, statement: SQL) -> ExecutionResult: ...
    def execute_stack(self, stack: StatementStack, *, continue_on_error: bool = False) -> tuple[StackResult, ...]: ...
```

## 3. Documentation & OKF Knowledge Plan

Foundation APIs are documented in `ch04-sqlspec-cutover-storage-events-docs`.

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Bump sqlspec>=0.71.1 + adk Extra and Add Driver query_with_columns Metadata Hook | `pyproject.toml`, `src/py/vertebra/db/driver/_sync.py`, `src/py/vertebra/db/driver/_async.py` | `src/py/tests/unit/db/test_native_db_drivers.py` |
| `t02` | Implement _driver.py Template-Method Dispatch Bases and Pipeline Stack Execution | `src/py/vertebra/extensions/sqlspec/_driver.py` | `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` |
| `t03` | Cut SQLSpec Provider Over to VTBARROW ArrowCursor and Add vertebra.storage.ArtifactReference | `src/py/vertebra/extensions/sqlspec/provider.py`, `src/py/vertebra/storage/_store.py`, `src/py/vertebra/storage/__init__.py` | `src/py/tests/unit/extensions/test_sqlspec_shm_proxy.py`, `src/py/tests/unit/storage/test_storage_store.py` |

## 5. Implementation Plan

- [ ] [t01-driver-column-metadata-and-deps.md](tasks/t01-driver-column-metadata-and-deps.md) — Raise sqlspec>=0.71.1, add adk = ['google-adk>=1.0.0'], and expose query_with_columns delegating to execute_with_description on base drivers.
- [ ] [t02-sqlspec-driver-dispatch-base.md](tasks/t02-sqlspec-driver-dispatch-base.md) — Implement extensions/sqlspec/_driver.py template-method bases, execute_stack pipelining, and refcounted pool close.
- [ ] [t03-vtbarrow-provider-and-artifact-ref.md](tasks/t03-vtbarrow-provider-and-artifact-ref.md) — Replace SharedArrowBuffer/SharedArrowTable with VTBARROW ArrowCursor and add vertebra.storage.ArtifactReference.

## 6. Current State & Remaining Plan Snapshot

Chapter `ch01-sqlspec-core-driver-and-artifact-ref` is `planned` behind [`../../spec.md`](../../spec.md) prerequisites (`broker-pool-sharing`, `native-sqlite-driver`, `native-redis-driver`).

Fast Chapter Gate (`< 60s` offline):

```bash
uv run --no-sync ruff check src/py/vertebra/db/driver src/py/vertebra/extensions/sqlspec src/py/vertebra/storage && uv run --no-sync mypy src/py/vertebra/db/driver src/py/vertebra/extensions/sqlspec src/py/vertebra/storage && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/extensions/test_sqlspec_shm_proxy.py src/py/tests/unit/storage/test_storage_store.py
```

[^pyproject-sqlspec]: `pyproject.toml` — sqlspec floor and optional adk extra.
[^sqlspec-adapters]: `src/py/vertebra/extensions/sqlspec/adapters.py` — Current monolithic SQLSpec driver module.
[^sqlspec-provider]: `src/py/vertebra/extensions/sqlspec/provider.py` — Broker SQLSpec provider.
[^storage-store]: `src/py/vertebra/storage/_store.py` — Storage value objects.
