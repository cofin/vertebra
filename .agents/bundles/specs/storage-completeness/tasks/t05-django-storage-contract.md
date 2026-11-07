---
type: Task
id: storage-completeness:t05
title: Subclass Django Storage (@deconstructible) and implement full file/metadata contract
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- storage-completeness:t04
files:
- src/py/vertebra/extensions/django/storage.py
tests:
- src/py/tests/unit/extensions/test_django_plugin.py
- src/py/tests/unit/extensions/test_data_storage_integrations.py
plan_revision: 1
plan_commit: "7657e8b7c2c96bdda6821aec70a2b603f438a127"
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: '2026-11-06T06:38:00Z'
updated_at: '2026-11-06T06:38:00Z'
commit: null
tags:
- storage
- django
requirements:
- R3
- R6
---

# Subclass Django Storage (@deconstructible) and implement full file/metadata contract

## Objective

Refactor `src/py/vertebra/extensions/django/storage.py::DjangoStorage` into a `@deconstructible` subclass of `django.core.files.storage.Storage` that fails closed when unconfigured, returns Django `File` handles from `_open`/`open`, signs URLs when configured, and implements all standard Django storage methods.

## Context

Read `../spec.md` and `t04-advanced-alchemy-storage-backend.md`. Guard `django` imports cleanly with `MissingDependencyError(..., extra="django")` when Django is absent.

## Steps

1. Write failing tests in `test_django_plugin.py` and `test_data_storage_integrations.py` verifying `issubclass(DjangoStorage, Storage)`, `deconstruct()` migration serialization, `open()` returning a `django.core.files.base.File`, `save` collision handling (`get_available_name` / `get_alternative_name`), `listdir` `(dirs, files)` splitting via `list_with_delimiter`, `get_modified_time` / `get_created_time` / `get_accessed_time`, `path()` `NotImplementedError` on remote stores vs filesystem path on `LocalConfig`, signed `url()`, and fail-closed error when no `config`/`store` is supplied.
2. Implement `@deconstructible class DjangoStorage(Storage)` in `src/py/vertebra/extensions/django/storage.py` delegating sync and async helpers over `SyncObjectStore` and `AsyncObjectStore`.
3. Run verification and commit `feat(django): subclass Django Storage with deconstructible file and metadata support`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/extensions/test_django_plugin.py src/py/tests/unit/extensions/test_data_storage_integrations.py -q
uv run --no-sync ruff check src/py/vertebra/extensions/django/storage.py
uv run --no-sync mypy src/py/vertebra/extensions/django/storage.py
```

## Acceptance Criteria

- [ ] R3, R6: `DjangoStorage` subclasses `django.core.files.storage.Storage` (`@deconstructible`), implements the complete Django storage API, and fails closed without `memory://` defaults.

## Notes & Discoveries

None.
