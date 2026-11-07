---
type: Task
id: storage-completeness:ch02:t03
chapter_id: ch02-orm-and-sql-storage-contracts
title: Subclass Django Storage (@deconstructible) with File handles and full metadata contract
description: Refactor vertebra.extensions.django.storage.DjangoStorage into a fail-closed @deconstructible subclass of django.core.files.storage.Storage
  returning Django File objects and implementing all sync/async storage and metadata methods.
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
- src/py/vertebra/extensions/django/storage.py
- src/py/vertebra/storage/_store.py
- src/py/tests/unit/extensions/test_django_plugin.py
- src/py/tests/unit/extensions/test_data_storage_integrations.py
- .agents/skills/python-framework/SKILL.md
files:
- src/py/vertebra/extensions/django/storage.py
tests:
- src/py/tests/unit/extensions/test_django_plugin.py
- src/py/tests/unit/extensions/test_data_storage_integrations.py
estimated_loc: 210
plan_revision: 4
plan_commit: null
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: '2026-11-07T20:37:34Z'
last_verified_commit: 8fccaed8
verification_evidence: 'Red: 5 failed, 7 passed. Green: 35 canonical integration tests; source mypy, Ruff check/format and
  diff check pass. Coordinator chapter checkpoint: 35 passed and Ruff across all three adapters.'
created_at: '2026-11-07T04:15:00Z'
updated_at: '2026-11-07T20:37:34Z'
commit: 8fccaed8
sources:
- id: django-storage-mod
  resource: src/py/vertebra/extensions/django/storage.py::DjangoStorage
  title: Django storage backend class
- id: readable-file-api
  resource: src/py/vertebra/storage/_store.py::SyncReadableFile
  title: Seekable SyncReadableFile and AsyncReadableFile handles
tags:
- storage
- django
- extensions
- python
---

# Task t03: Subclass Django `Storage` (`@deconstructible`) with `File` Handles and Full Metadata Contract

## Objective & Scope Boundary

Refactor `src/py/vertebra/extensions/django/storage.py::DjangoStorage` into a `@deconstructible` subclass of `django.core.files.storage.Storage` that fails closed when `config` and `store` are omitted, returns `django.core.files.base.File` (wrapping `SyncReadableFile`) from `_open`/`open` and async-loaded `aopen`, removes all private `_memory` dict access, and implements Django's complete file, directory, URL, path, and timestamp contract [^django-storage-mod] [^readable-file-api].

## Bounded Read Manifest

- `src/py/vertebra/extensions/django/storage.py` — Current standalone `DjangoStorage` implementation [^django-storage-mod].
- `src/py/vertebra/storage/_store.py` — `SyncObjectStore`, `AsyncObjectStore`, `SyncReadableFile`, `LocalConfig`, and `ObjectMeta` [^readable-file-api].
- `src/py/tests/unit/extensions/test_django_plugin.py` — Django extension tests.
- `src/py/tests/unit/extensions/test_data_storage_integrations.py` — Cross-framework storage contract tests.
- `.agents/skills/python-framework/SKILL.md` — Optional dependency loading (`MissingDependencyError`), typing, and docstring rules.

## Symbol & Signature Contracts

- `src/py/vertebra/extensions/django/storage.py`:
  - Guard `django.core.files.base.File`, `django.core.files.storage.Storage`, and `django.utils.deconstruct.deconstructible` via optional import so importing the module without `django` installed does not crash at import time, while instantiating `DjangoStorage` without `django` raises `MissingDependencyError("django", install_package="django", extra="django")`.
  - `@deconstructible` `class DjangoStorage(Storage)`:
    - `__init__(self, config: StorageConfig | str | None = None, *, store: SyncObjectStore | None = None, async_store: AsyncObjectStore | None = None, base_url: str | None = None, location: str = "", file_overwrite: bool = False, expires_in: int = 3600) -> None`: raise `ImproperConfigurationError` if `config`, `store`, and `async_store` are all `None` (or empty `""`).
    - Core hooks & sync/async operations:
      - `_open(self, name: str, mode: str = "rb") -> File` (wraps `SyncReadableFile` in `django.core.files.base.File(readable, name=name)`)
      - `_save(self, name: str, content: IO[bytes] | File | bytes) -> str` (streams via `put_stream` for multi-chunk `File.chunks()` or `put` for bytes)
      - `aopen` returning a Django `File` over an `io.BytesIO` payload obtained through awaited native `get` (Django `File` has synchronous reads; no blocking native I/O runs on the event loop), `asave`, `delete`, `adelete`, `exists`, `aexists`, `size`, `asize`
      - `listdir(self, path: str) -> tuple[list[str], list[str]]` (uses `store.list_with_delimiter` to return `(directories, filenames)` relative to `path`)
      - `url(self, name: str | None, *, expire: int | None = None) -> str` and `aurl` (uses `store.sign` when `supports_signing`, else `base_url` join, raising `ValueError` if neither is configured)
      - `path(self, name: str) -> str` (returns resolved local filesystem path when backed by `LocalConfig` / `file://`, otherwise raises `NotImplementedError`)
      - Inherited/overridden `Storage` helpers: `get_available_name` (honoring `file_overwrite` and `max_length`), `generate_filename`, `get_valid_name`, `get_alternative_name`, and UTC `datetime` `get_modified_time`, `get_created_time`, `get_accessed_time` parsed from `ObjectMeta.last_modified`.

## Edge Cases & Error Semantics

- `DjangoStorage()` with no `config` or `store` raises `ImproperConfigurationError` (never defaults to `MemoryConfig()`).
- `storage.save("docs/report.pdf", ContentFile(b"v2"))` when `"docs/report.pdf"` already exists and `file_overwrite=False` writes to an alternative non-colliding name (`"docs/report_<suffix>.pdf"`) via `get_available_name` / `get_alternative_name`.
- `storage.deconstruct()` returns `("vertebra.extensions.django.storage.DjangoStorage", args, kwargs)` that reconstructs an equivalent `DjangoStorage` instance for Django `makemigrations`.
- `storage.path("key")` on `MemoryConfig` / `S3Config` / `GCSConfig` raises `NotImplementedError`.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/unit/extensions/test_django_plugin.py` and `src/py/tests/unit/extensions/test_data_storage_integrations.py`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Subclass, `@deconstructible`, & fail-closed init | `DjangoStorage()` vs `s = DjangoStorage(f"file://{tmp_path}", base_url="/media/")` | `DjangoStorage()` raises `ImproperConfigurationError`; `isinstance(s, Storage)` is `True`; `path, args, kwargs = s.deconstruct()` reconstructs equal storage. |
| `save` collision + `open` returns `File` | Save `ContentFile(b"hello")` twice to `"notes/readme.txt"` (`file_overwrite=False`), then `with s.open(saved_name) as fh:` | Second `save` returns distinct `notes/readme_*.txt`; `isinstance(fh, File)` is `True`; `fh.read() == b"hello"`. |
| `listdir` & timestamps | Seed `"sub/a.txt"` and `"root.txt"`; call `s.listdir("")` and `s.get_modified_time("root.txt")` | `listdir("") == (["sub"], ["root.txt"])`; `get_modified_time` returns timezone-aware `datetime`. |
| `path()` local vs remote | `LocalConfig(tmp_path)` `s.path("root.txt")` vs `MemoryConfig()` `s_mem.path("root.txt")` | Local returns `str(tmp_path / "root.txt")`; remote/memory raises `NotImplementedError`. |
| Async `asave` / `aopen` / `aexists` / `adelete` | `name = await s.asave("async.bin", ContentFile(b"xyz"))` | `await s.aexists(name)` is `True`, `await s.asize(name) == 3`, `await s.adelete(name)` removes object. |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/extensions/test_django_plugin.py src/py/tests/unit/extensions/test_data_storage_integrations.py -k django -q`
- Final Green + lint + typecheck gate: `uv run --no-sync ruff check src/py/vertebra/extensions/django/storage.py src/py/tests/unit/extensions/test_django_plugin.py && uv run --no-sync mypy src/py/vertebra/extensions/django/storage.py && uv run --no-sync pytest src/py/tests/unit/extensions/test_django_plugin.py src/py/tests/unit/extensions/test_data_storage_integrations.py -q`

## Acceptance Criteria

- [x] `DjangoStorage` is a `@deconstructible` subclass of `django.core.files.storage.Storage` that fails closed when unconfigured (R3, R6).
- [x] `_open` / `open` / `aopen` return `django.core.files.base.File` handles and all required sync/async storage, `listdir`, `url`, `path`, naming, and timestamp methods pass contract tests without touching `_memory` (R6).

## Notes & Discoveries

- [2026-11-07 04:15] `ObjectMeta.last_modified` from `vertebra-storage` is RFC 3339 UTC; parse with `datetime.fromisoformat`.

[^django-storage-mod]: `src/py/vertebra/extensions/django/storage.py::DjangoStorage` — Django storage adapter.
[^readable-file-api]: `src/py/vertebra/storage/_store.py::SyncReadableFile` — Seekable object-store file reader.
