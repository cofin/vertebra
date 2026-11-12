---
type: Task
id: "storage-integrity-and-streaming:ch03:t02"
chapter_id: ch03-arrow-streaming-and-integrations
title: Forward digest options through SQLSpec, Advanced Alchemy, and Django storage
description: Pass checksum and expected_sha256 through SQLSpec write_bytes_sync/async and read_bytes, Advanced Alchemy save_object/save_object_async, and DjangoStorage._save so integrations surface receipts and IntegrityError.
status: draft
state: pending
priority: P2
verification_strategy: tdd
depends_on:
  - "storage-integrity-and-streaming:ch03:t01"
read_set:
  - src/py/vertebra/extensions/sqlspec/storage.py
  - src/py/vertebra/extensions/alchemy/storage.py
  - src/py/vertebra/extensions/django/storage.py
  - src/py/tests/unit/extensions/test_data_storage_integrations.py
files:
  - src/py/vertebra/extensions/alchemy/storage.py
  - src/py/vertebra/extensions/django/storage.py
  - src/py/tests/unit/extensions/test_data_storage_integrations.py
tests:
  - src/py/tests/unit/extensions/test_data_storage_integrations.py::test_integration_sha256_forwarding
estimated_loc: 150
plan_revision: 1
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
created_at: "2026-11-12T01:30:00Z"
updated_at: "2026-11-12T01:30:00Z"
commit: null
sources:
  - id: sqlspec-backend
    resource: src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend
    title: write_bytes_sync/async (already forwards **kwargs)
  - id: aa-backend
    resource: src/py/vertebra/extensions/alchemy/storage.py::ObjectStoreBackend
    title: save_object and save_object_async
  - id: django-storage
    resource: src/py/vertebra/extensions/django/storage.py::DjangoStorage
    title: _save
tags:
  - storage
  - integrity
  - django
  - advanced-alchemy
---

# Task t02: Forward Digest Options Through SQLSpec, Advanced Alchemy, and Django Storage

## Objective & Scope Boundary

1. SQLSpec `ObjectStoreBackend.write_bytes_sync/async` forward `**kwargs` through `_write_options` to `put` (the pass-through for `checksum` / `expected_sha256` lands in ch03/t01); this task adds the SQLSpec test rows only [^sqlspec-backend].
2. Advanced Alchemy `ObjectStoreBackend.save_object` / `save_object_async`: accept `expected_sha256: str | None = None` keyword-only and forward to `put` / `put_stream` [^aa-backend].
3. `DjangoStorage._save`: read `expected_sha256` from `getattr(content, "sha256", None)` when the uploaded content object carries it (Django `File` subclasses may set it) and forward; no change when absent [^django-storage].

## Bounded Read Manifest

- The three integration modules and the integration test file.

## Symbol & Signature Contracts

- `save_object(self, path: str, data: ..., *, expected_sha256: str | None = None, ...) -> ...` (existing parameters unchanged).
- Django: `content.sha256` attribute is the opt-in channel because `Storage._save(name, content)` has a fixed signature.

## Edge Cases & Error Semantics

- Mismatch raises `vertebra.typing.IntegrityError` unchanged through each integration; the prior object stays.
- No digest supplied: behavior identical to HEAD.

## Public-Seam Test Blueprint

- **Canonical gate**: `uv run pytest src/py/tests/unit/extensions/test_data_storage_integrations.py -k sha256 -v`, parametrized over `("sqlspec", "alchemy", "django")`.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Good digest | write `b"payload"` with `expected_sha256=hashlib.sha256(b"payload").hexdigest()` | object stored |
| Bad digest | prior `b"old"`; write `b"new"` with `expected_sha256="0" * 64` | `IntegrityError`; stored bytes `b"old"` |
| Django opt-in | `ContentFile(b"x")` with `.sha256 = "0" * 64` | `IntegrityError` |
| Absent | no digest | stored, no error |

## Verification

- `uv run pytest src/py/tests/unit/extensions/test_data_storage_integrations.py -k sha256 -v`
- `uv run mypy src/py/vertebra/extensions/alchemy/storage.py src/py/vertebra/extensions/django/storage.py`

## Acceptance Criteria

- [ ] Each integration forwards expected digests and surfaces `IntegrityError` (R5).

## Notes & Discoveries

- [2026-11-12 01:30] Django `Storage._save(name, content)` cannot take extra parameters, so the digest travels on the content object.

[^sqlspec-backend]: `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` — SQLSpec backend.
[^aa-backend]: `src/py/vertebra/extensions/alchemy/storage.py::ObjectStoreBackend` — Advanced Alchemy backend.
[^django-storage]: `src/py/vertebra/extensions/django/storage.py::DjangoStorage` — Django storage.
