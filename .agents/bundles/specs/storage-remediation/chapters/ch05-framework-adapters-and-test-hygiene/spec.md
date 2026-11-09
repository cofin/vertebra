---
type: Spec
flow_id: storage-remediation
parent_flow: storage-remediation
chapter_id: ch05-framework-adapters-and-test-hygiene
title: "Chapter 05: Framework Storage Adapters and Pytest Hygiene"
description: Clean up SQLSpec, Advanced Alchemy, and Django storage adapters (remove _IPCBuffer and duplicate re-exports, enforce public-before-private method ordering, raise ImproperConfigurationError on missing DuckDB GCS tokens, check supports_signing, forward signing and token_cache on StorageConfig, and avoid extra bytes copies in DjangoStorage.save), drop open_readable_file and _get_native_module shims in _store.py, and enforce pytest top-level imports, parametrize, and anyio across storage/extension/db tests.
status: stable
state: completed
planning_status: complete
depends_on_chapters:
  - ch04-streaming-arrow-parquet-and-shm
replan_trigger: null
current_task: null
plan_revision: 1
plan_commit: fd571e3a
state_revision: 1
last_verified_checkpoint: 41d6d8c1
last_reviewed_commit: 41d6d8c1
created_at: "2026-11-08T20:55:00Z"
updated_at: "2026-11-09T01:54:00Z"
generated:
  by: flow/0.28.0
  at: "2026-11-08T20:55:00Z"
verified:
  - by: agent:code-reviewer
    at: "2026-11-09T01:54:00Z"
    commit: 41d6d8c1
sources:
  - id: py-sqlspec-storage
    resource: src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend
    title: SQLSpec ObjectStoreBackend and build_duckdb_gcs_secret_sql
  - id: py-alchemy-storage
    resource: src/py/vertebra/extensions/alchemy/storage.py::ObjectStoreBackend
    title: Advanced Alchemy ObjectStoreBackend adapter
  - id: py-django-storage
    resource: src/py/vertebra/extensions/django/storage.py::DjangoStorage
    title: Django Storage backend over paired native object stores
  - id: py-django-init
    resource: src/py/vertebra/extensions/django/__init__.py
    title: Public vertebra.extensions.django package exports
  - id: py-store
    resource: src/py/vertebra/storage/_store.py::SyncObjectStore
    title: SyncObjectStore and AsyncObjectStore deprecated alias cleanup
tags:
  - storage
  - extensions
  - sqlspec
  - alchemy
  - django
  - tests
---

# Chapter 05: Framework Storage Adapters and Pytest Hygiene

<!-- truth: start -->
- **SQLSpec & Advanced Alchemy Storage Adapters**: `extensions/sqlspec/storage.py` deletes `_IPCBuffer`, groups all public methods strictly above private `_...` helpers on `ObjectStoreBackend`, checks `self._sync_store.supports_signing` consistently, propagates native signing errors unchanged, rejects `signing`/`token_cache` alongside prebuilt stores, and renders `build_duckdb_gcs_secret_sql(secret_name, config, *, bearer_token) -> str` as a quote-escaped literal (`ImproperConfigurationError` on a missing/blank token, `ValueError` on control characters); `extensions/alchemy/storage.py` forwards `signing=` and `token_cache=` when constructing `SyncObjectStore` / `AsyncObjectStore` from `StorageConfig` and rejects them alongside prebuilt stores [^py-sqlspec-storage] [^py-alchemy-storage].
- **Clean-Break Django Storage & `_store.py` Debloat (`PY-07`)**: `extensions/django/storage.py` and `extensions/django/__init__.py` drop re-exports of `vertebra.storage` symbols (`GCSConfig`, `LocalConfig`, `MemoryConfig`, `S3Config`, `SyncObjectStore`, `AsyncObjectStore`) while preserving `DjangoStorage`, `SyncStorageRegistry`, and `AsyncStorageRegistry`, and pass bytes/buffers directly in `DjangoStorage.save` without redundant `ContentFile(bytes(content))` copying; `storage/_store.py` deletes `open_readable_file` and `_get_native_module` [^py-django-storage] [^py-django-init] [^py-store].
- **Pytest Top-Level Imports & Parameterization (`TST-01`..`TST-06`)**: `test_storage_store.py`, `test_signing.py`, `test_gcs_token_refresh.py`, `test_litestar_plugin.py`, and `test_pg_dataplane_and_datafusion.py` use module top-level imports, `@pytest.mark.parametrize` instead of subcase `for` loops, and `@pytest.mark.anyio` instead of `asyncio.run` [^py-store].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Zero-context code and quality reviews flagged adapter drift and test-hygiene violations (`CR-05`..`CR-08`, `QUAL-005`..`QUAL-008`):
1. `extensions/sqlspec/storage.py` retains a trivial `_IPCBuffer` wrapper, interleaves private `_...` methods with public `*_async` methods (`PY-01`), silently generates broken `CREATE OR REPLACE SECRET ... BEARER_TOKEN ''` SQL when `build_duckdb_gcs_secret_sql` receives no token, and inspects `_signing is not None` instead of `supports_signing` [^py-sqlspec-storage].
2. `extensions/alchemy/storage.py` drops `signing=` and `token_cache=` when `store` is a `StorageConfig` instance [^py-alchemy-storage].
3. `extensions/django/storage.py` and `extensions/django/__init__.py` re-export `vertebra.storage` config/store classes (`QUAL-006`) and copy `bytes(raw)` unnecessarily in `DjangoStorage.save`, while `storage/_store.py` keeps deprecated `open_readable_file` and dead `_get_native_module` helpers (`PY-07`) [^py-django-storage] [^py-django-init] [^py-store].
4. Storage, Litestar, and PG DataFusion unit test modules contain function-scoped imports, `for` subcase loops, and ad-hoc `asyncio.run(...)` calls.

**Non-Goals**:
- Django ORM database backends (owned by `sqlalchemy-and-django-integration`).

### 1.2 Upstream Execution Baseline

- Depends on `ch04-streaming-arrow-parquet-and-shm`; parent spec [`../../spec.md`](../../spec.md).

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| SQLSpec & Alchemy Adapters | `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` [^py-sqlspec-storage], `src/py/vertebra/extensions/alchemy/storage.py::ObjectStoreBackend` [^py-alchemy-storage] | Public framework storage backend seams. |
| Django Storage & `_store.py` | `src/py/vertebra/extensions/django/storage.py::DjangoStorage` [^py-django-storage], `src/py/vertebra/extensions/django/__init__.py` [^py-django-init], `src/py/vertebra/storage/_store.py::SyncObjectStore` [^py-store] | Public Django `Storage` subclass and clean-break export surface. |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [`object-storage.md`](../../../../knowledge/domains/storage/object-storage.md), [`litestar.md`](../../../../knowledge/domains/extensions/litestar.md), [`module-and-class-layout.md`](../../../../knowledge/patterns/python/module-and-class-layout.md), [`review-tests`](../../../../../skills/review-tests/SKILL.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R5.1 (`t01`)**: Remove `_IPCBuffer` in `extensions/sqlspec/storage.py`, place all public sync and async methods strictly above private `_...` helpers on `ObjectStoreBackend`, check `self._sync_store.supports_signing`, raise `ImproperConfigurationError` when `build_duckdb_gcs_secret_sql` resolves an empty/missing token, and forward `signing=signing, token_cache=token_cache` across `ObjectStoreBackend.__init__` (preserving `key: str = "default", fs: ... = None` Advanced Alchemy compatibility) and `register_storage_backend` in `extensions/alchemy/storage.py`.
- **R5.2 (`t02`)**: Remove `GCSConfig`, `LocalConfig`, `MemoryConfig`, `S3Config`, `SyncObjectStore`, and `AsyncObjectStore` re-exports from `extensions/django/storage.py` and `extensions/django/__init__.py` (preserving `DjangoStorage`, `SyncStorageRegistry`, and `AsyncStorageRegistry`), write `Buffer` / `str` payloads directly to `self._store.put` after `get_available_name` in `DjangoStorage.save` (streaming Django `File` objects via `self._store.open_writer` / `put_stream` in `_save`), and delete `open_readable_file` and `_get_native_module` in `storage/_store.py`.
- **R5.3 (`t03`)**: Hoist function-scoped imports to module top-level, replace subcase `for` loops with `@pytest.mark.parametrize`, and replace `asyncio.run(...)` with `@pytest.mark.anyio` across `test_storage_store.py`, `test_signing.py`, `test_gcs_token_refresh.py`, `test_litestar_plugin.py`, and `test_pg_dataplane_and_datafusion.py`.

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

- `extensions/sqlspec/storage.py::build_duckdb_gcs_secret_sql(secret_name: str = "vertebra_gcs", config: GCSConfig | None = None, *, bearer_token: str | None = None) -> str` resolves the token from `bearer_token` or `config.resolve_adc_token()`, doubles `'` in the literal, raises `ImproperConfigurationError` when no non-empty token resolves, and raises `ValueError` on NUL/CR/LF.
- `extensions/django/storage.py::__all__ = ("AsyncStorageRegistry", "DjangoStorage", "SyncStorageRegistry")` (and `extensions/django/__init__.py` exports those three storage symbols without `vertebra.storage` re-exports).

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `ObjectStoreBackend("default", GCSConfig(...), signing=..., token_cache=...)` / `register_storage_backend(..., signing=..., token_cache=...)` (`alchemy/storage.py`) -> `SyncObjectStore(target, signing=signing, token_cache=token_cache)` / `AsyncObjectStore(...)`.
- `DjangoStorage.save(name, content)` -> `self._store.put` directly for `Buffer` / `str` without `ContentFile(bytes(...))`, or `self._store.open_writer` / `put_stream` for chunked Django `File` objects.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| DuckDB GCS secret with no token | `build_duckdb_gcs_secret_sql("gcs_sec")` with `config=None, bearer_token=None` | Raises `ImproperConfigurationError` | Fail-loud credential validation. |
| Auth options with a prebuilt store | `ObjectStoreBackend(SyncObjectStore(...), signing=..., token_cache=...)` (SQLSpec or Alchemy) | Raises `ImproperConfigurationError` without mutating the caller's store | No silently ignored credentials. |
| `ObjectStoreBackend(GCSConfig(...), signing=..., token_cache=...)` | Alchemy `ObjectStoreBackend` initialized with `StorageConfig` | Both sync and async stores receive `signing` and `token_cache` | Consistent auth/signing across ORM attachments. |
| Deprecated aliases accessed | `hasattr(SyncObjectStore, "open_readable_file")` or `from vertebra.extensions.django import S3Config` | `False` / `ImportError` | Clean-break `PY-07` surface. |

### 2.5 Acceptance Scenarios (Given / When / Then)

1. **Given** `ObjectStoreBackend(S3Config(bucket="b", access_key_id="k", secret_access_key="s", endpoint="http://127.0.0.1:9000"), signing=SigningConfig(...))` in `vertebra.extensions.alchemy.storage`, **When** `sign_sync` and `sign_async` run, **Then** both use the forwarded `SigningConfig`.
2. **Given** the updated test modules, **When** `pytest` and AST checks run, **Then** zero function-scoped imports or subcase `for` loops remain in the remediated test modules and all tests pass.

### 2.6 Alternatives Considered

- **Chosen**: Clean-break removal of `open_readable_file`, `_get_native_module`, and `vertebra.extensions.django` storage re-exports (`PY-07`).
- **Rejected**: Retaining deprecated re-exports/wrappers (violates repository `PY-07` clean-break rules).

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/0 files documented in ch05]` (consolidated in Chapter 06 `ch06:t01` and `ch06:t02`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test |
| --- | --- | --- | --- |
| R5.1 | `ch05 / t01` | `behavior_tdd` | `src/py/tests/unit/extensions/test_data_storage_integrations.py` |
| R5.2 | `ch05 / t02` | `behavior_tdd` | `src/py/tests/unit/extensions/test_django_plugin.py`, `src/py/tests/unit/storage/test_storage_store.py` |
| R5.3 | `ch05 / t03` | `static_validation` | `src/py/tests/unit/storage/test_storage_store.py`, `src/py/tests/unit/extensions/test_litestar_plugin.py`, `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py` |

## 5. Implementation Plan

- [x] [3b24b0af] storage-remediation:ch05:t01 — Clean up SQLSpec and Advanced Alchemy `ObjectStoreBackend` method ordering, DuckDB token validation, and `StorageConfig` forwarding (tasks/t01-sqlspec-and-alchemy-storage-adapters.md)
- [x] [a04970be] storage-remediation:ch05:t02 — Enforce clean-break `vertebra.extensions.django` storage exports, stream `DjangoStorage.save`, and remove `_store.py` shims (tasks/t02-django-storage-and-store-alias-cleanup.md)
- [x] [cd7f66bf] storage-remediation:ch05:t03 — Hoist nested imports and convert subcase `for` loops and `asyncio.run` to `@pytest.mark.parametrize` and `@pytest.mark.anyio` (tasks/t03-pytest-imports-and-parametrize-hygiene.md)
- **Chapter / Phase Recovery Checkpoint**: `uv run --no-sync ruff check src/py/vertebra src/py/tests && uv run --no-sync mypy src/py/vertebra && uv run --no-sync pytest src/py/tests/unit/storage src/py/tests/unit/extensions src/py/tests/unit/db -q`

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline (`HEAD`)**: `41d6d8c1` — `3/3` tasks completed (`3b24b0af`, `a04970be`, `cd7f66bf`) and the zero-context review findings remediated: native signing errors propagate, auth options with prebuilt stores raise `ImproperConfigurationError` in both adapters, `build_duckdb_gcs_secret_sql` renders a quote-escaped literal, Litestar tests run against real Litestar primitives, and optional dependencies are resolved at module level.
- **Remaining Tasks in Chapter**: None.
- **Remaining Chapters**: `ch06`.

[^py-sqlspec-storage]: `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` — SQLSpec `ObjectStoreBackend` and `build_duckdb_gcs_secret_sql`.
[^py-alchemy-storage]: `src/py/vertebra/extensions/alchemy/storage.py::ObjectStoreBackend` — Advanced Alchemy `ObjectStoreBackend` adapter.
[^py-django-storage]: `src/py/vertebra/extensions/django/storage.py::DjangoStorage` — Django `Storage` backend over paired native object stores.
[^py-django-init]: `src/py/vertebra/extensions/django/__init__.py` — Public `vertebra.extensions.django` package exports.
[^py-store]: `src/py/vertebra/storage/_store.py::SyncObjectStore` — `SyncObjectStore` and `AsyncObjectStore` deprecated alias cleanup.
