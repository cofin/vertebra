---
type: Task
id: "storage-integrity-and-streaming:ch02:t02"
flow_id: storage-integrity-and-streaming
chapter_id: ch02-streaming-arrow-writes-and-integrations
title: "Add Streaming Arrow Saves and Digest Forwarding to Alchemy and Django Storage"
description: "Add save_arrow and save_arrow_async plus expected-digest forwarding to Advanced Alchemy ObjectStoreBackend, and forward content.{sha256,crc32c,md5} in DjangoStorage._save."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "storage-integrity-and-streaming:ch02:t01"
files:
  - src/py/vertebra/extensions/alchemy/storage.py
  - src/py/vertebra/extensions/django/storage.py
tests:
  - src/py/tests/unit/extensions/test_data_storage_integrations.py
read_set:
  - src/py/vertebra/storage/_arrow.py
  - src/py/vertebra/extensions/alchemy/storage.py
  - src/py/vertebra/extensions/django/storage.py
  - src/py/tests/unit/extensions/test_data_storage_integrations.py
estimated_loc: 165
tags:
  - storage
  - arrow
  - advanced-alchemy
  - django
sources:
  - id: aa-backend
    resource: src/py/vertebra/extensions/alchemy/storage.py::ObjectStoreBackend
    title: "Advanced Alchemy ObjectStoreBackend save_object and save_object_async"
  - id: django-storage
    resource: src/py/vertebra/extensions/django/storage.py::DjangoStorage
    title: "Django Storage _save implementation"
---

# Task `t02`: Add Streaming Arrow Saves and Digest Forwarding to Alchemy and Django Storage

## Objective & Scope Boundary

Add streaming Arrow/Parquet multipart methods (`save_arrow` / `save_arrow_async`) and `checksum` / `expected_{sha256,crc32c,md5}` forwarding to Advanced Alchemy `ObjectStoreBackend`, and opt-in `content.{sha256,crc32c,md5}` forwarding to `DjangoStorage._save` per [`../spec.md` §2](../spec.md) [^aa-backend] [^django-storage]. Out of scope: Criterion Rust benchmark (`t03`).

## Bounded Read Manifest

- `src/py/vertebra/storage/_arrow.py` — `write_batches_sync` and `write_batches_async` from `t01`.
- `src/py/vertebra/extensions/alchemy/storage.py` — `ObjectStoreBackend.save_object` and `save_object_async` [^aa-backend].
- `src/py/vertebra/extensions/django/storage.py` — `DjangoStorage._save` [^django-storage].
- `src/py/tests/unit/extensions/test_data_storage_integrations.py` — Alchemy and Django storage tests.

## Contracts & Edge Cases

- Advanced Alchemy `ObjectStoreBackend`: accept keyword-only `checksum: bool = False, expected_sha256: str | None = None, expected_crc32c: str | None = None, expected_md5: str | None = None` on `save_object` and `save_object_async` (forwarding to `put` / `put_stream` over native Tokio async/sync stores with zero `asyncio.to_thread`), and add `save_arrow` / `save_arrow_async` delegating to `write_batches_sync` / `write_batches_async`.
- `DjangoStorage._save(name, content)`: preserve Django's fixed `(name, content)` signature while reading `getattr(content, "sha256", None)`, `getattr(content, "crc32c", None)`, and `getattr(content, "md5", None)` when set on the uploaded `File` / `ContentFile` object, forwarding them as `expected_*` to `put` / `put_stream`.
- Mismatched digests raise `vertebra.typing.IntegrityError` unchanged and preserve prior objects; omitted digests keep existing behavior.

## Test Matrix

- Parametrized tests in `src/py/tests/unit/extensions/test_data_storage_integrations.py` across `("sqlspec", "alchemy", "django")`: valid `expected_sha256` succeeds; mismatched `expected_sha256` (`ContentFile` `.sha256` on Django) raises `IntegrityError` and preserves prior object bytes; Alchemy `save_arrow` / `save_arrow_async` stream multi-batch Parquet/Arrow tables with `checksum=True` returning matching `PutResult.sha256`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/alchemy/storage.py src/py/vertebra/extensions/django/storage.py src/py/tests/unit/extensions/test_data_storage_integrations.py && uv run --no-sync mypy src/py/vertebra/extensions/alchemy/storage.py src/py/vertebra/extensions/django/storage.py && uv run --no-sync pytest src/py/tests/unit/extensions/test_data_storage_integrations.py -q
```

## Acceptance Criteria

- [ ] Advanced Alchemy and Django storage adapters forward expected digests, stream Arrow/Parquet writes on Alchemy without `to_thread`, and surface `vertebra.typing.IntegrityError` (`R4`).
- [ ] Zero regressions in `test_data_storage_integrations.py` and `make review-check` (`PYO3-01`).

[^aa-backend]: `src/py/vertebra/extensions/alchemy/storage.py::ObjectStoreBackend` — Advanced Alchemy `ObjectStoreBackend` `save_object` and `save_object_async`.
[^django-storage]: `src/py/vertebra/extensions/django/storage.py::DjangoStorage` — Django `Storage` `_save` implementation.
