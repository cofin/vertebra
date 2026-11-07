---
type: Spec
parent_flow: storage-completeness
chapter_id: ch02-orm-and-sql-storage-contracts
title: "Chapter 02: SQLSpec, Advanced Alchemy, and Django Storage Contracts"
description: Implement fail-closed SQLSpec ObjectStoreProtocol, Advanced Alchemy StorageBackend registration without monkeypatching, and @deconstructible Django Storage subclass over SyncObjectStore and AsyncObjectStore.
status: stable
state: active
planning_status: complete
depends_on_chapters:
  - ch01-native-engine-and-auth
replan_trigger: null
current_task: "storage-completeness:ch02:t02"
plan_revision: 3
plan_commit: 0acdf576
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-07T04:15:00Z"
updated_at: "2026-11-07T20:00:00Z"
generated:
  by: flow-setup
  at: "2026-11-07T04:15:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T04:15:00Z"
    commit: cdb978d3
sources:
  - id: sqlspec-storage
    resource: src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend
    title: SQLSpec ObjectStoreBackend over SyncObjectStore and AsyncObjectStore
  - id: alchemy-storage
    resource: src/py/vertebra/extensions/alchemy/storage.py::StorageBackend
    title: Advanced Alchemy StorageBackend and StorageRegistry helper
  - id: django-storage
    resource: src/py/vertebra/extensions/django/storage.py::DjangoStorage
    title: Django storage backend integration
tags:
  - storage
  - sqlspec
  - advanced-alchemy
  - django
  - extensions
---

# Chapter 02: SQLSpec, Advanced Alchemy, and Django Storage Contracts

<!-- truth: start -->
- **Fail-Closed Extension Constructors**: `ObjectStoreBackend`, Advanced Alchemy `ObjectStoreBackend(StorageBackend)` / `register_storage_backend`, and `DjangoStorage` raise `ImproperConfigurationError` when no store, config, or URI is supplied instead of defaulting to `"memory://"` [^sqlspec-storage] [^alchemy-storage] [^django-storage].
- **Zero Monkeypatching in Advanced Alchemy**: `src/py/vertebra/extensions/alchemy/storage.py` registers backends exclusively via `StorageRegistry.register_backend` without mutating `storages.get` [^alchemy-storage].
- **Real Django Storage Inheritance**: `DjangoStorage` subclasses `django.core.files.storage.Storage` with `@deconstructible`, returns `django.core.files.base.File` from `_open`/`open`/`aopen`, and implements full sync/async file and metadata methods [^django-storage].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Across Vertebra's ORM and SQL extensions, `sqlspec/storage.py::ObjectStoreBackend` defaults `uri=None` to `"memory://"` and lacks streaming/paginated protocol parity [^sqlspec-storage]; `alchemy/storage.py` monkeypatches `setattr(storages, "get", storages.get_backend)` at import time and falls back to `SyncObjectStore("memory://")` [^alchemy-storage]; and `django/storage.py::DjangoStorage` does not subclass `django.core.files.storage.Storage` (`@deconstructible`), reaches into `_store._memory`, and returns raw `bytes` from `open()` [^django-storage]. Chapter 02 aligns all three adapters with their upstream framework contracts on top of Chapter 01's `SyncObjectStore` and `AsyncObjectStore`.

### 1.2 Non-Goals

- Relocating `build_duckdb_gcs_secret_sql` out of `extensions/sqlspec/storage.py` (deferred to `native-duckdb-driver`).
- Batch-streaming `_iter_arrow_async` Parquet/Arrow cursor integration (owned by Chapter 04 `ch04:t02`).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md); depends on [ch01-native-engine-and-auth](../ch01-native-engine-and-auth/spec.md). Skills: [python-framework](../../../../../skills/python-framework/SKILL.md) (plus external `sqlspec` and `advanced-alchemy` plugin skills). Knowledge: [sqlspec-alchemy-and-dialects.md](../../../../knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R4 (`ch02:t01`)**: Remove implicit `"memory://"` fallback in `vertebra.extensions.sqlspec.storage.ObjectStoreBackend`; preserve canonical `ObjectStoreProtocol` `*_sync` / `*_async` methods and unsupported-signing `NotImplementedError`, forward conditional write options, and add `list_page_sync` / `list_page_async`, `get_stream_sync` / `get_stream_async`, `put_stream_sync` / `put_stream_async`, and `open_sync` / `open_async` extensions delegating to `SyncObjectStore` / `AsyncObjectStore`; preserve `build_duckdb_gcs_secret_sql`.
- **R5 (`ch02:t02`)**: Delete `setattr(storages, _storage_get_attr, storages.get_backend)` and `"memory://"` fallbacks from `vertebra.extensions.alchemy.storage`; require an explicit store target (`SyncObjectStore | AsyncObjectStore | StorageConfig | str`) in `ObjectStoreBackend(StorageBackend)` and `register_storage_backend(store_or_key, *, store, key, prefix, set_default, registry)`, preserving existing helper call forms while rejecting a key-only target; implement `save_object`, `save_object_async`, `get_content`, `get_content_async`, `delete_object`, `delete_object_async`, `sign`, and `sign_async` without `asyncio.to_thread`.
- **R6 (`ch02:t03`)**: Refactor `vertebra.extensions.django.storage.DjangoStorage` into a `@deconstructible` subclass of `django.core.files.storage.Storage` (raising `MissingDependencyError` when `django` is absent and `ImproperConfigurationError` when `config` and `store` are omitted), returning Django `File` over `SyncReadableFile` from sync `_open`/`open` and an async-loaded `io.BytesIO` from `aopen` and implementing `_save`, `save`, `asave`, `delete`, `adelete`, `exists`, `aexists`, `size`, `asize`, `listdir`, `url`, `aurl`, `path`, `get_available_name`, `generate_filename`, `get_valid_name`, `get_accessed_time`, `get_created_time`, `get_modified_time`, and `get_alternative_name`.

`ObjectStoreBackend` accepts `uri=None` so omitted configuration raises `ImproperConfigurationError`; an explicit `store=` takes precedence. It constructs one primary native store immediately and lazily obtains the shared-engine companion on first opposite-side use. Protocol consumers call canonical `*_sync` / `*_async` methods; native streaming, opaque-cursor pagination, and readable-file handles remain separately named extensions. Relative keys, URI keys, and `base_path` reject traversal segments before I/O.

### 2.2 Edge-Case & Failure-Mode Matrix

| Boundary | Deterministic Behavior |
| --- | --- |
| Unsigned `ObjectStoreBackend.sign_sync` / `sign_async` | Raises `NotImplementedError`, matching installed SQLSpec `ObjectStoreProtocol`. |
| `ObjectStoreBackend()`, `StorageBackend()`, `register_storage_backend("default")`, or `DjangoStorage()` with no target | Raises `ImproperConfigurationError`. |
| `DjangoStorage.path(name)` on remote backend (`gs`, `s3`, `r2`, `oci`, `az`, `memory`) | Raises `NotImplementedError` per Django's remote-storage contract; returns resolved local filesystem path on `LocalConfig`. |
| `DjangoStorage._save(name, content)` when `name` already exists and `max_length` is set | Resolves a non-colliding filename via `Storage.get_available_name(name, max_length=max_length)` before uploading. |

### 2.3 Alternatives Comparison

| Decision | Chosen Seam | Rejected Alternative |
| --- | --- | --- |
| Django base class | Subclass `django.core.files.storage.Storage` directly (lazy stub fallback only when `django` is uninstalled so module import raises `MissingDependencyError` on instantiation) | Duck-typed standalone class — fails `isinstance(storage, Storage)` and Django migration `deconstruct()` serialization. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in ch02]` (documented in `ch04:t03` across `docs/usage/storage.md` and `.agents/bundles/knowledge/domains/storage/object-storage.md`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R4 | [t01](tasks/t01-sqlspec-object-store-protocol.md) | `behavior_tdd` | `src/py/tests/unit/extensions/test_data_storage_integrations.py` |
| R5 | [t02](tasks/t02-advanced-alchemy-storage-backend.md) | `behavior_tdd` | `src/py/tests/unit/extensions/test_data_storage_integrations.py` |
| R6 | [t03](tasks/t03-django-deconstructible-storage.md) | `behavior_tdd` | `src/py/tests/unit/extensions/test_django_plugin.py`, `src/py/tests/unit/extensions/test_data_storage_integrations.py` |

## 5. Implementation Plan

- [x] [dd3c9c08] storage-completeness:ch02:t01 — Complete fail-closed SQLSpec ObjectStoreProtocol over SyncObjectStore and AsyncObjectStore (tasks/t01-sqlspec-object-store-protocol.md)
- [ ] storage-completeness:ch02:t02 — Remove storages.get monkeypatch and harden Advanced Alchemy StorageBackend (tasks/t02-advanced-alchemy-storage-backend.md)
- [ ] storage-completeness:ch02:t03 — Subclass Django Storage (@deconstructible) with File handles and full metadata contract (tasks/t03-django-deconstructible-storage.md)

**Chapter Recovery Checkpoint:** `uv run --no-sync pytest src/py/tests/unit/extensions/test_data_storage_integrations.py src/py/tests/unit/extensions/test_django_plugin.py -q && uv run --no-sync ruff check src/py/vertebra/extensions/{sqlspec,alchemy,django}/storage.py` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `HEAD @ cdb978d3`; `depends_on_chapters: [ch01-native-engine-and-auth]`, `state: planned`.
- **Verified Task**: `t01` completes the SQLSpec protocol and native extensions at `dd3c9c08`.
- **Remaining Tasks in Chapter**: `t02`, `t03` (`2/3` pending).
- **Active Invariants**: Zero `asyncio.to_thread` in `extensions/alchemy/storage.py`; zero `storages.get` monkeypatching; `build_duckdb_gcs_secret_sql` preserved intact.

[^sqlspec-storage]: `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` — SQLSpec object storage adapter.
[^alchemy-storage]: `src/py/vertebra/extensions/alchemy/storage.py::StorageBackend` — Advanced Alchemy storage backend and registry integration.
[^django-storage]: `src/py/vertebra/extensions/django/storage.py::DjangoStorage` — Django file storage adapter.
