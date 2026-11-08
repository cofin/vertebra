---
type: Task
id: "storage-completeness:ch02:t03"
chapter_id: ch02-orm-and-sql-storage-contracts
title: Subclass Django Storage (@deconstructible) with File handles and full metadata contract
description: Refactor vertebra.extensions.django.storage.DjangoStorage into a fail-closed @deconstructible subclass of django.core.files.storage.Storage returning Django File objects and implementing all sync/async storage and metadata methods.
status: stable
state: pending
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
plan_revision: 3
plan_commit: e8901de4
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-07T04:15:00Z"
updated_at: "2026-11-08T02:09:14Z"
commit: null
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

# Task t03: Implement Deconstructible Django `Storage` With Real `File` Handles

## Objective & Scope Boundary

Make `DjangoStorage` a fail-closed `@deconstructible` subclass of Django `Storage`, return real seekable `File` handles, retain sync and async CRUD, and implement Django naming, collision, directory, URL, local-path, and timestamp contracts over the native paired store. Preserve both existing storage registries.[^django-storage-mod][^readable-file-api]

## Bounded Read Manifest

- `src/py/vertebra/extensions/django/storage.py` — `DjangoStorage` and public sync/async registries.[^django-storage-mod]
- `src/py/vertebra/storage/_store.py` — `SyncObjectStore`, `AsyncObjectStore`, `SyncReadableFile`, `LocalConfig`, and `ObjectMeta`.[^readable-file-api]
- `src/py/tests/unit/extensions/test_django_plugin.py` — Existing Django consumer and storage tests.
- `src/py/tests/unit/extensions/test_data_storage_integrations.py` — Cross-framework storage tests.
- `.agents/skills/python-framework/SKILL.md` — Optional dependency loading, typing, and docstrings.

**Installed upstream contract evidence:** Django `6.1.3`, `.venv/lib/python3.12/site-packages/django/core/files/storage/base.py::Storage`, `.venv/lib/python3.12/site-packages/django/core/files/base.py::File` / `ContentFile`, and `.venv/lib/python3.12/site-packages/django/utils/deconstruct.py::deconstructible`. Django `Storage.save` validates filenames and selects an available name before `_save`; its `open` delegates to `_open`. No native async storage base methods exist, so `a*` methods are Vertebra extensions.

## Symbol & Signature Contracts

- Load optional Django classes/decorator through `vertebra.utils.module_loader` so importing the module without Django succeeds, and constructing `DjangoStorage` raises `MissingDependencyError("django", install_package="django", extra="django")`. When installed, subclass the real `Storage` and apply the real `@deconstructible` decorator.
- Preserve `config` accepting existing `SyncObjectStore` / `AsyncObjectStore` objects as well as `StorageConfig | str | None = None`. Preserve `store: SyncObjectStore | AsyncObjectStore | None = None`; add `async_store: AsyncObjectStore | None = None`, `base_url: str | None = None`, `location: str = ""`, `file_overwrite: bool = False`, and `expires_in: int = 3600` keyword arguments. Select `store`, then `async_store`, then `config`; absent/blank selected target raises `ImproperConfigurationError`. URI strings construct a store directly, config structs use `from_config`, and all explicit handles derive a paired handle without recreating an engine.
- `_open(self, name: str, mode: str = "rb") -> File` returns `File(self.store.open_reader(qualified_name), name=name)`. `open` follows the real base contract. `async def aopen(self, name: str, mode: str = "rb") -> File` first awaits async metadata, then constructs a `File` over a `SyncReadableFile` with the known size; construction performs no synchronous storage I/O. Subsequent `File` reads follow Django's synchronous file interface. Unsupported write modes raise `ValueError` rather than pretending to support writable handles.
- `save(self, name: str | None, content: Any, max_length: int | None = None) -> str` delegates to Django filename validation/collision selection, wrapping byte buffers in `ContentFile` for the existing byte-input convenience. `_save(self, name: str, content: Any) -> str` only persists the chosen key: buffers use `put`; `File.chunks()` / file readers stream with `put_stream`.
- `async def asave(self, name: str | None, content: Any, max_length: int | None = None) -> str` applies equivalent filename validation/max-length/collision behavior with native `await exists`, and uploads via paired async put/stream without sync storage I/O or `asyncio.to_thread`. Async iterables remain incremental.
- Preserve `delete` / `adelete`, `exists` / `aexists`, `size` / `asize`, and store properties. `listdir(path: str) -> tuple[list[str], list[str]]` maps native `(list[ObjectMeta], common_prefixes)` from `list_with_delimiter` into Django `(directories, filenames)` relative to `path`, with the location prefix stripped and deterministic ordering.
- `url(self, name: str, *, expire: int | None = None) -> str` and `aurl` sign qualified keys when supported, using configured `expires_in` unless overridden; otherwise join URL-quoted name with `base_url`. Raise `ValueError` if neither signing nor a base URL is configured.
- `path(name: str) -> str` returns a resolved path under the explicit `LocalConfig` / `file://` root including `location`; other schemes raise `NotImplementedError`. Reject traversal for every operation before applying `location`.
- Inherit Django `get_valid_name`, `generate_filename`, `get_alternative_name`, and collision/max-length behavior; honor `file_overwrite` without bypassing Django path validation. `get_modified_time`, `get_created_time`, and `get_accessed_time` return UTC-aware `datetime` parsed from native `ObjectMeta.last_modified`. The adapter exposes the one available native modification timestamp for all three hooks; absent/unparsable metadata raises `NotImplementedError` rather than inventing a timestamp.
- Caller wiring: Django `FileField` / `Storage.save` → validated chosen name → `_save` → native paired store; Django `Storage.open` → `_open` → `File(SyncReadableFile)`; migration serialization → decorator `deconstruct()` → the same constructor path/args/kwargs.

## Edge Cases & Error Semantics

- `DjangoStorage()`, None/blank configuration with no explicit store, and unsupported read modes fail explicitly. Explicit `MemoryConfig()` is supported for tests.
- Save collision with `file_overwrite=False` returns an alternative filename; `file_overwrite=True` replaces contents under the requested valid name. `max_length` follows Django's truncation/error rules, preserving `SuspiciousFileOperation` on invalid traversal/names.
- Existing tests that compare `open()` / `aopen()` directly with bytes must read/close the returned real `File`; do not preserve a bytes-return alias.
- `deconstruct()` preserves constructor data suitable for `STORAGES`/migration serialization; reconstruct the class and prove file behavior, rather than comparing two unrelated store instances for identity.
- `listdir` returns direct children only; `path` on memory/cloud raises `NotImplementedError`. Expiry/signing errors propagate honestly.

## Public-Seam Test Blueprint

**Canonical modules:** `test_django_plugin.py` and `test_data_storage_integrations.py` under `src/py/tests/unit/extensions/`. Use `LocalConfig(root=tmp_path)`, Django `ContentFile`, and the native engine; keep existing byte-input convenience assertions after updating the consumer's `File.read` usage. Observe intended Red before production edits.

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Base/decorator/fail-closed | `DjangoStorage()`; `s = DjangoStorage(LocalConfig(root=tmp_path), base_url="/media/")` | Missing target raises `ImproperConfigurationError`; `isinstance(s, Storage)` is true; `s.deconstruct()` reconstructs a working adapter. |
| Save collision, overwrite, and File | Save `ContentFile(b"hello")` twice as `notes/readme.txt`; then open each | Distinct alternative name by default; handles are real `File` and read `b"hello"`; overwrite mode retains one chosen name with new bytes. |
| Naming/path safety | Invalid `../outside.txt`, filename exceeding `max_length`, and location `uploads` | Match Django validation/error behavior and keep native keys inside the chosen root/location. |
| Directory and time | Store `sub/a.txt`, `root.txt`; call `listdir("")` and all three timestamp methods | Return `(["sub"], ["root.txt"])`; all timestamp hooks return aware UTC `datetime` from native metadata. |
| Local/cloud path | Local `s.path("root.txt")`; explicit memory `s_mem.path("root.txt")` | Local path is `str(tmp_path / "root.txt")`; memory raises `NotImplementedError`. |
| Async lifecycle and File | `name = await s.asave("async.bin", ContentFile(b"xyz"))`; `fh = await s.aopen(name)` | `isinstance(fh, File)`, `fh.read() == b"xyz"`; async exists/size yield true/3; delete removes the object. |
| URL semantics | Base URL plus `"a b.txt"`; separate explicit HMAC-signed store | URL quotes space as `%20`; configured signing honors method/expiry and returns a verifiable signature. |

## Verification

- Initial Red: `uv run --no-sync pytest src/py/tests/unit/extensions/test_django_plugin.py src/py/tests/unit/extensions/test_data_storage_integrations.py -k django -q` after adding the focused regression.
- Final: `uv run --no-sync ruff check src/py/vertebra/extensions/django/storage.py src/py/tests/unit/extensions/test_django_plugin.py src/py/tests/unit/extensions/test_data_storage_integrations.py && uv run --no-sync mypy src/py/vertebra/extensions/django/storage.py && uv run --no-sync pytest src/py/tests/unit/extensions/test_django_plugin.py src/py/tests/unit/extensions/test_data_storage_integrations.py -q`.

## Acceptance Criteria

- [ ] Real deconstructible Django `Storage` subclass refuses absent configuration and supports migration reconstruction (R3, R6).
- [ ] `open` / `aopen` return real seekable `File` handles; sync/async save, naming/collisions, directory, URL, local path, and timestamp contracts pass public tests (R6).
- [ ] Both registries stay public; no private `_memory` access, no inline Python comments, and lint/type checks pass (R6).

## Notes & Discoveries

- Installed contract baseline: Django `6.1.3`; Django `save` owns validation/name allocation and `_save` owns persistence. Native metadata type is `ObjectMeta`, with one RFC 3339 modification timestamp.

[^django-storage-mod]: `src/py/vertebra/extensions/django/storage.py::DjangoStorage` — Django backend and registries.
[^readable-file-api]: `src/py/vertebra/storage/_store.py::SyncReadableFile` — Seekable native-store file handle and metadata contracts.
