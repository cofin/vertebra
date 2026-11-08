---
type: Task
id: "storage-remediation:ch05:t02"
chapter_id: ch05-framework-adapters-and-test-hygiene
title: Enforce clean-break vertebra.extensions.django storage exports, stream DjangoStorage.save, and remove _store.py shims
description: Drop vertebra.storage config/store re-exports from extensions/django/storage.py and __init__.py, stream multi-chunk files and avoid redundant bytes copies in DjangoStorage.save, and delete open_readable_file and _get_native_module from storage/_store.py.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "storage-remediation:ch05:t01"
read_set:
  - src/py/vertebra/extensions/django/storage.py
  - src/py/vertebra/extensions/django/__init__.py
  - src/py/vertebra/storage/_store.py
  - src/py/tests/unit/extensions/test_django_plugin.py
  - src/py/tests/unit/storage/test_storage_store.py
  - src/py/tests/unit/storage/test_signing.py
files:
  - src/py/vertebra/extensions/django/storage.py
  - src/py/vertebra/extensions/django/__init__.py
  - src/py/vertebra/storage/_store.py
tests:
  - src/py/tests/unit/extensions/test_django_plugin.py
  - src/py/tests/unit/storage/test_storage_store.py
  - src/py/tests/unit/storage/test_signing.py
estimated_loc: 120
plan_revision: 1
plan_commit: fd571e3a
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-08T20:55:00Z"
updated_at: "2026-11-08T20:55:00Z"
commit: null
generated:
  by: flow/0.28.0
  at: "2026-11-08T20:55:00Z"
verified: []
sources:
  - id: py-django-storage
    resource: src/py/vertebra/extensions/django/storage.py::DjangoStorage
    title: Django Storage adapter
  - id: py-django-init
    resource: src/py/vertebra/extensions/django/__init__.py
    title: Public vertebra.extensions.django exports
  - id: py-store
    resource: src/py/vertebra/storage/_store.py::SyncObjectStore
    title: SyncObjectStore and AsyncObjectStore deprecated shims
tags:
  - storage
  - django
  - debloat
  - tdd
---

# Task t02: Clean-Break Django Storage Exports, Streaming `DjangoStorage.save`, and `_store.py` Shim Removal

## Objective & Scope Boundary

Update `src/py/vertebra/extensions/django/storage.py` [^py-django-storage], `src/py/vertebra/extensions/django/__init__.py` [^py-django-init], and `src/py/vertebra/storage/_store.py` [^py-store] (`3` files):
1. Remove duplicate re-exports of `AsyncObjectStore`, `GCSConfig`, `LocalConfig`, `MemoryConfig`, `S3Config`, and `SyncObjectStore` from `extensions/django/storage.py` and `extensions/django/__init__.py` while preserving `DjangoStorage`, `SyncStorageRegistry`, and `AsyncStorageRegistry` in `__all__` (updating test imports in `test_django_plugin.py` to import storage configs/stores from `vertebra.storage`).
2. In `DjangoStorage.save` / `_save`: when `content` is `Buffer | str`, validate and allocate `name = self.get_available_name(self._validate_name(name), max_length=max_length)` directly in `save()` and write via `self._store.put(self._qualified_name(name), content.encode("utf-8") if isinstance(content, str) else content)` without wrapping in `ContentFile(bytes(content))`; in `_save`, stream chunked Django `File` objects through `self._store.open_writer` / `self._store.put_stream(key, self._file_chunks(file))`.
3. Delete `open_readable_file` on `SyncObjectStore` and `AsyncObjectStore` and delete `_get_native_module` in `storage/_store.py` (updating `test_signing.py` and `test_storage_store.py`).

Out of scope: Test-wide nested import and loop parameterization sweep (`t03`).

## Bounded Read Manifest

- `src/py/vertebra/extensions/django/storage.py` — `DjangoStorage`, `SyncStorageRegistry`, `AsyncStorageRegistry`, and `__all__` [^py-django-storage].
- `src/py/vertebra/extensions/django/__init__.py` — package exports [^py-django-init].
- `src/py/vertebra/storage/_store.py` — `open_readable_file` and `_get_native_module` [^py-store].
- `src/py/tests/unit/extensions/test_django_plugin.py` — Django storage tests.
- `src/py/tests/unit/storage/test_storage_store.py` — `open_reader` tests.
- `src/py/tests/unit/storage/test_signing.py` — `_get_native_module` callers.

## Symbol & Signature Contracts

- `vertebra.extensions.django.storage.__all__ = ("AsyncStorageRegistry", "DjangoStorage", "SyncStorageRegistry")`
- `SyncObjectStore` and `AsyncObjectStore` expose `open_reader` exclusively (zero `open_readable_file` method).

## Edge Cases & Error Semantics

- `DjangoStorage` saving a multi-chunk `SimpleUploadedFile` / `UploadedFile` (`multiple_chunks() == True`) streams chunks via `open_writer` without concatenating the full upload in memory first.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/unit/extensions/test_django_plugin.py` and `src/py/tests/unit/storage/test_storage_store.py`.
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Clean-break Django exports | `vertebra.extensions.django.__all__` and `vertebra.extensions.django.storage.__all__` | Exports `DjangoStorage` without `S3Config`/`GCSConfig`/`SyncObjectStore` duplicates. |
| Multi-chunk `DjangoStorage.save` | Chunked file object with `multiple_chunks() -> True` yielding `3` chunks | Streams into store and reads back exact concatenated bytes. |
| Removed `_store.py` shims | `hasattr(SyncObjectStore, "open_readable_file")` and `hasattr(_store, "_get_native_module")` | Both evaluate to `False`. |

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/django src/py/vertebra/storage/_store.py src/py/tests/unit/extensions/test_django_plugin.py src/py/tests/unit/storage/test_storage_store.py src/py/tests/unit/storage/test_signing.py
uv run --no-sync mypy src/py/vertebra/extensions/django src/py/vertebra/storage/_store.py
uv run --no-sync pytest src/py/tests/unit/extensions/test_django_plugin.py src/py/tests/unit/storage/test_storage_store.py src/py/tests/unit/storage/test_signing.py -q
```

## Acceptance Criteria

- [ ] `extensions/django` exports `DjangoStorage` cleanly without duplicating `vertebra.storage`, and `DjangoStorage.save` streams multi-chunk uploads without redundant `bytes(...)` copies (R5.2).
- [ ] `open_readable_file` and `_get_native_module` are deleted from `storage/_store.py` (R5.2).

## Notes & Discoveries

- [2026-11-08 20:55] Ensure no docs or internal callers reference `open_readable_file` (`git grep open_readable_file`).

[^py-django-storage]: `src/py/vertebra/extensions/django/storage.py::DjangoStorage` — Django `Storage` adapter.
[^py-django-init]: `src/py/vertebra/extensions/django/__init__.py` — Public `vertebra.extensions.django` exports.
[^py-store]: `src/py/vertebra/storage/_store.py::SyncObjectStore` — `SyncObjectStore` and `AsyncObjectStore` deprecated shims.
