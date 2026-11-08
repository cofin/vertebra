---
type: Task
id: "storage-completeness:ch02:t03"
chapter_id: ch02-orm-and-sql-storage-contracts
title: Subclass Django Storage (@deconstructible) with File handles and full metadata contract
description: Refactor vertebra.extensions.django.storage.DjangoStorage into a fail-closed @deconstructible subclass of django.core.files.storage.Storage returning Django File objects and implementing all sync/async storage and metadata methods.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - src/py/vertebra/extensions/django/storage.py
  - src/py/vertebra/storage/_store.py
  - src/py/tests/unit/extensions/test_django_plugin.py
  - src/py/tests/unit/extensions/test_data_storage_integrations.py
  - pyproject.toml
  - uv.lock
files:
  - src/py/vertebra/extensions/django/storage.py
  - pyproject.toml
  - uv.lock
tests:
  - src/py/tests/unit/extensions/test_django_plugin.py
  - src/py/tests/unit/extensions/test_data_storage_integrations.py
estimated_loc: 210
plan_revision: 7
plan_commit: a51ebcb6
state_revision: 3
claimed_by: agent:python-framework-specialist
claimed_at: "2026-11-08T14:32:16Z"
blocked_reason: null
unblock_condition: null
next_step: "Chapter correctness review, then fresh quality review"
last_verified_at: "2026-11-08T15:13:53Z"
last_verified_commit: 04be780b7901e76ea3a487dfa5ba519061a14a8c
verification_evidence:
  red: "uv run --no-sync pytest src/py/tests/unit/extensions/test_django_plugin.py src/py/tests/unit/extensions/test_data_storage_integrations.py -k django -q: exit 1, missing configuration regression fails"
  reflection_red: "UV_NO_SYNC=1 uv run --no-sync pytest src/py/tests/unit/extensions/test_django_plugin.py -k file_annotations -q --disable-warnings: exit 1, 2 failed with File not subscriptable"
  green: "UV_NO_SYNC=1 uv run --no-sync ruff check src/py/vertebra/extensions/django/storage.py src/py/tests/unit/extensions/test_django_plugin.py src/py/tests/unit/extensions/test_data_storage_integrations.py && UV_NO_SYNC=1 uv run --no-sync mypy src/py/vertebra/extensions/django/storage.py && UV_NO_SYNC=1 uv run --no-sync pytest src/py/tests/unit/extensions/test_django_plugin.py src/py/tests/unit/extensions/test_data_storage_integrations.py -q: exit 0, 136 passed"
  full_mypy: "UV_NO_SYNC=1 uv run --no-sync mypy: exit 0, 419 source files"
  format: "UV_NO_SYNC=1 uv run --no-sync ruff format --check src/py/vertebra/extensions/django/storage.py src/py/tests/unit/extensions/test_django_plugin.py src/py/tests/unit/extensions/test_data_storage_integrations.py: exit 0"
  lock: "uv lock --check: exit 0"
  consumer_red: "UV_NO_SYNC=1 uv run --no-sync pytest src/py/tests/unit/extensions/test_django_plugin.py -k 'reopens_through_native_store or migration_writer_round_trips_configuration' -q --disable-warnings: exit 1, 10 failed"
  broad: "UV_NO_SYNC=1 uv run --no-sync pytest src/py/tests/unit/extensions src/py/tests/unit/storage -q: exit 0, 592 passed"
  docs: "UV_NO_SYNC=1 make docs: exit 0, documentation artifacts verified"
created_at: "2026-11-07T04:15:00Z"
updated_at: "2026-11-08T15:13:53Z"
commit: 04be780b7901e76ea3a487dfa5ba519061a14a8c
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

**Installed upstream contract evidence:** Django `6.1.3`, `.venv/lib/python3.12/site-packages/django/core/files/storage/base.py::Storage`, `.venv/lib/python3.12/site-packages/django/core/files/base.py::File` / `ContentFile`, and `.venv/lib/python3.12/site-packages/django/utils/deconstruct.py::deconstructible`. Django `Storage.save` validates filenames and selects an available name before `_save`; its `open` delegates to `_open`. No native async storage base methods exist, so `a*` methods are Vertebra extensions.

- `pyproject.toml` and `uv.lock` — Add `django-stubs>=6.1.2` to the lint development dependency group and minimally resolve its lock entries. Load the Python framework skill as referenced guidance outside the six-file source manifest.

## Symbol & Signature Contracts

- Load optional Django classes/decorator through `vertebra.utils.module_loader` so importing the module without Django succeeds, and constructing `DjangoStorage` raises `MissingDependencyError("django", install_package="django", extra="django")`. When installed, subclass the real `Storage` and apply the real `@deconstructible` decorator.
- Preserve `config` accepting existing `SyncObjectStore` / `AsyncObjectStore` objects as well as `StorageConfig | str | None = None`. Preserve `store: SyncObjectStore | AsyncObjectStore | None = None`; add `async_store: AsyncObjectStore | None = None`, `base_url: str | None = None`, `location: str = ""`, `file_overwrite: bool = False`, and `expires_in: int = 3600` keyword arguments. Select `store`, then `async_store`, then `config`; absent/blank selected target raises `ImproperConfigurationError`. URI strings construct a store directly, config structs use `from_config`, and all explicit handles derive a paired handle without recreating an engine.
- `_open(self, name: str, mode: str = "rb") -> File` returns `File(self.store.open_reader(qualified_name), name=name)`. `open` follows the real base contract. `async def aopen(self, name: str, mode: str = "rb") -> File` first awaits async metadata, then constructs a `File` over a `SyncReadableFile` with the known size; construction performs no synchronous storage I/O. Subsequent `File` reads follow Django's synchronous file interface. Unsupported write modes raise `ValueError` rather than pretending to support writable handles.
- `save(self, name: str | None, content: Any, max_length: int | None = None) -> str` delegates to Django filename validation/collision selection, wrapping byte buffers in `ContentFile` for the existing byte-input convenience. `_save(self, name: str, content: Any) -> str` only persists the chosen key: buffers use `put`; `File.chunks()` / file readers stream with `put_stream`.
- `async def asave(self, name: str | None, content: Any, max_length: int | None = None) -> str` applies equivalent filename validation/max-length/collision behavior with native `await exists`, and uploads via paired async put/stream without sync storage I/O or `asyncio.to_thread`. Async iterables remain incremental.
- Preserve `delete` / `adelete`, `exists` / `aexists`, `size` / `asize`, and store properties. `listdir(path: str) -> tuple[list[str], list[str]]` maps native `(list[ObjectMeta], common_prefixes)` from `list_with_delimiter` into Django `(directories, filenames)` relative to `path`, with the location prefix stripped and deterministic ordering.
- `url(self, name: str, *, expire: int | None = None) -> str` and `aurl` sign qualified keys when supported, using configured `expires_in` unless overridden; otherwise join URL-quoted name with `base_url`. Raise `ValueError` if neither signing nor a base URL is configured.
- `path(name: str) -> str` returns a resolved path under the explicit `LocalConfig` / `file://` root including `location`; other schemes raise `NotImplementedError`. Reject traversal for every operation before applying `location`.
- Inherit Django `get_valid_name`, `generate_filename`, `get_alternative_name`, and collision/max-length behavior; honor `file_overwrite` without bypassing Django path validation. `get_modified_time` returns a UTC-aware `datetime` parsed from native `ObjectMeta.last_modified`; absent/unparsable modification metadata raises `NotImplementedError`. `get_created_time` and `get_accessed_time` raise `NotImplementedError` because native metadata supplies neither timestamp.
- Caller wiring: Django `FileField` / `Storage.save` → validated chosen name → `_save` → native paired store; Django `Storage.open` → `_open` → `File(SyncReadableFile)`; migration serialization → decorator `deconstruct()` → the same constructor path/args/kwargs.

The native store supplies modification time only. `get_modified_time` parses that value into an aware UTC datetime; `get_created_time` and `get_accessed_time` raise `NotImplementedError`, matching Django's unsupported metadata contract.

Use the real `django-stubs` type definitions for the Django `Storage` base. No handwritten replacement base or weakened MyPy checks. This is a development typing dependency only.

### Django File Reopen and Migration Serialization

- Both file factories return a Django `File` subclass whose `open(mode=None)` resets or recreates its reader through the configured native store. A stored object key never becomes a process-filesystem path, including after `FieldFile.close()` and `FieldFile.open()`.
- Configuration objects serialize through Django's documented custom-serializer registry, registered only for Vertebra's own `StorageConfig` types. `BaseSerializer.serialize(self) -> tuple[str, set[str]]` emits the public configuration constructor and recursively serialized field values. Storage constructor arguments and all config fields remain intact; no private state or config options are dropped. No upstream API monkeypatch is introduced.
- Test the real `MigrationWriter.serialize` output by importing its returned imports and evaluating its expression. `LocalConfig` and `MemoryConfig(namespace="migration-tests")` reconstruct equivalent configuration; include provider options with concrete non-secret fixture values.

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
| Directory and time | Store `sub/a.txt`, `root.txt`; call `listdir("")` and all three timestamp methods | Return `(["sub"], ["root.txt"])`; modification time is an aware UTC `datetime`; creation/access methods raise `NotImplementedError`. |
| Local/cloud path | Local `s.path("root.txt")`; explicit memory `s_mem.path("root.txt")` | Local path is `str(tmp_path / "root.txt")`; memory raises `NotImplementedError`. |
| Async lifecycle and File | `name = await s.asave("async.bin", ContentFile(b"xyz"))`; `fh = await s.aopen(name)` | `isinstance(fh, File)`, `fh.read() == b"xyz"`; async exists/size yield true/3; delete removes the object. |
| URL semantics | Base URL plus `"a b.txt"`; separate explicit HMAC-signed store | URL quotes space as `%20`; configured signing honors method/expiry and returns a verifiable signature. |

## Verification

- Initial Red: `uv run --no-sync pytest src/py/tests/unit/extensions/test_django_plugin.py src/py/tests/unit/extensions/test_data_storage_integrations.py -k django -q` after adding the focused regression.
- Final: `uv run --no-sync ruff check src/py/vertebra/extensions/django/storage.py src/py/tests/unit/extensions/test_django_plugin.py src/py/tests/unit/extensions/test_data_storage_integrations.py && uv run --no-sync mypy src/py/vertebra/extensions/django/storage.py && uv run --no-sync pytest src/py/tests/unit/extensions/test_django_plugin.py src/py/tests/unit/extensions/test_data_storage_integrations.py -q`.

Dependency verification: `uv lock --check` and the repository-wide configured MyPy gate pass with the new stubs installed; retain all existing optional extras in the environment.

Review regression verification: real `FieldFile` open/read/close/reopen returns the stored bytes for ordinary keys and `"pyproject.toml"` through sync/async-created handles. Real `MigrationWriter` round trips provider config constructors with all fields retained. Both cases demonstrate Red before the narrow repair.

## Acceptance Criteria

- [x] Real deconstructible Django `Storage` subclass refuses absent configuration and supports migration reconstruction (R3, R6).
- [x] `open` / `aopen` return real seekable `File` handles; sync/async save, naming/collisions, directory, URL, local path, and timestamp contracts pass public tests (R6).
- [x] Both registries stay public; no private `_memory` access, no inline Python comments, and lint/type checks pass (R6).

- [x] Django storage subclasses the real upstream typed base with `django-stubs` in the lint group; lock validation and full configured MyPy pass.

- [x] FieldFile reopen remains on object storage, and real migration serialization round trips LocalConfig and MemoryConfig/provider options.

## Notes & Discoveries

- Prior source evidence remains at `09a44f88baa87178271436a0228acf9fb3ddd5a5` and `6ed9661be17a6da446d436cbe056381784c8a475`, with 125 canonical tests before the consumer review cases.
- Django `FieldFile.open` delegates reopening to its cached `File`; plain `File.open` can read a same-named working-directory file after close. The storage-bound file reopens only its qualified native key and refreshes the cached size.
- Django's public `MigrationWriter.register_serializer` supports a `BaseSerializer` for each of the eight Vertebra configuration types. Every config field serializes recursively into public `vertebra.storage` constructors; LocalConfig, MemoryConfig namespace, and S3 provider options round-trip without cloud calls.
- Live native store handles remain nonportable migration constructor inputs; Django reports their unsupported serialization explicitly.

- Django's runtime `File` class is not generic despite its published stubs. A private type-only specialization resolves to the concrete `File` at runtime so `get_type_hints` succeeds without mutating Django.
- Commit hooks invoking `uv run` require `UV_NO_SYNC=1` after dependency manifest changes to retain installed optional extras and native editable artifacts.

- Native metadata exposes modification time only; creation/access hooks raise `NotImplementedError`.
- Django `File` accepts binary readers; `aopen` awaits metadata and supplies its size so constructing the synchronous reader performs no storage I/O.
- `ContentFile(str)` emits text chunks; uploads encode each chunk as UTF-8 without buffering the complete file.
- `django-stubs` `6.1.2` checks the real optional base and `File[bytes]`; full configured MyPy passes for 419 files with unchanged typing configuration.
- The focused initial regression fails because omitted configuration previously created a memory store. Both canonical modules pass 136 tests after implementation; Ruff, format, and lock validation pass.

- Installed contract baseline: Django `6.1.3`; Django `save` owns validation/name allocation and `_save` owns persistence. Native metadata type is `ObjectMeta`, with one RFC 3339 modification timestamp.

[^django-storage-mod]: `src/py/vertebra/extensions/django/storage.py::DjangoStorage` — Django backend and registries.
[^readable-file-api]: `src/py/vertebra/storage/_store.py::SyncReadableFile` — Seekable native-store file handle and metadata contracts.
