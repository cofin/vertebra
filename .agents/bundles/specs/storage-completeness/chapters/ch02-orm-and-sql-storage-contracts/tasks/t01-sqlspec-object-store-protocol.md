---
type: Task
id: "storage-completeness:ch02:t01"
chapter_id: ch02-orm-and-sql-storage-contracts
title: Complete fail-closed SQLSpec ObjectStoreProtocol over SyncObjectStore and AsyncObjectStore
description: Remove implicit memory:// defaults in vertebra.extensions.sqlspec.storage.ObjectStoreBackend and implement full sync/async SQLSpec ObjectStoreProtocol streaming, pagination, rename, and presigning methods.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - src/py/vertebra/extensions/sqlspec/storage.py
  - src/py/vertebra/storage/_store.py
  - src/py/tests/unit/extensions/test_data_storage_integrations.py
  - .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
  - .agents/skills/python-framework/SKILL.md
files:
  - src/py/vertebra/extensions/sqlspec/storage.py
tests:
  - src/py/tests/unit/extensions/test_data_storage_integrations.py
estimated_loc: 170
plan_revision: 2
plan_commit: cdb978d3
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
updated_at: "2026-11-07T04:15:00Z"
commit: null
sources:
  - id: sqlspec-storage-mod
    resource: src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend
    title: SQLSpec ObjectStoreBackend and build_duckdb_gcs_secret_sql
  - id: py-store-api
    resource: src/py/vertebra/storage/_store.py::SyncObjectStore
    title: SyncObjectStore and AsyncObjectStore streaming and signing API
tags:
  - storage
  - sqlspec
  - extensions
  - python
---

# Task t01: Complete Fail-Closed SQLSpec `ObjectStoreProtocol` Over `SyncObjectStore` and `AsyncObjectStore`

## Objective & Scope Boundary

Make `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` fail closed when no `uri` or `store` is supplied and implement complete sync and async SQLSpec `ObjectStoreProtocol` methods (`read_bytes`, `write_bytes`, `read_text`, `write_text`, `exists`, `delete`, `copy`, `move`, `list_objects`, `list_page`, `get_stream`, `put_stream`, `sign_sync`, `sign_async`, `open`, `get_metadata`) over `SyncObjectStore` and `AsyncObjectStore` while preserving `build_duckdb_gcs_secret_sql` intact [^sqlspec-storage-mod] [^py-store-api]. Streaming Arrow/Parquet `_iter_arrow_async` batch iteration is owned by `storage-completeness:ch04:t02`.

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/storage.py` — `ObjectStoreBackend` and `build_duckdb_gcs_secret_sql` [^sqlspec-storage-mod].
- `src/py/vertebra/storage/_store.py` — `SyncObjectStore`, `AsyncObjectStore`, `SyncReadableFile`, `AsyncReadableFile`, and `StorageConfig` [^py-store-api].
- `src/py/tests/unit/extensions/test_data_storage_integrations.py` — Canonical SQLSpec and Advanced Alchemy storage contract tests.
- `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` — SQLSpec extension integration rules.
- `.agents/skills/python-framework/SKILL.md` — Python typing, docstring, and zero-inline-comment rules.

## Symbol & Signature Contracts

- `src/py/vertebra/extensions/sqlspec/storage.py`:
  - `ObjectStoreBackend.__init__(self, uri: str | SyncObjectStore | AsyncObjectStore | StorageConfig, *, base_path: str = "", **options: Any) -> None`:
    - Raise `ImproperConfigurationError` if `uri` is `None` or an empty/whitespace `str`.
    - Construct paired `SyncObjectStore` and `AsyncObjectStore` lazily on demand when initialized from a `str` URI or `StorageConfig`, or bind directly when given an explicit store instance.
  - Sync & async protocol methods (`*_sync` / plain sync and `*_async` per SQLSpec `ObjectStoreProtocol`):
    - `read_bytes` / `read_bytes_async`, `write_bytes` / `write_bytes_async` (supporting `content_type`, `attributes`, `if_not_exists`)
    - `read_text` / `read_text_async`, `write_text` / `write_text_async`
    - `exists` / `exists_async`, `delete` / `delete_async`, `copy` / `copy_async`, `move` / `move_async` (delegating to `store.rename`)
    - `list_objects` / `list_objects_async`, `list_page` / `list_page_async`
    - `get_stream` / `get_stream_async`, `put_stream` / `put_stream_async`
    - `open_sync` (`SyncReadableFile`) / `open_async` (`AsyncReadableFile`)
    - `get_metadata` / `get_metadata_async`, `sign_sync` / `sign_async`
  - Keep `build_duckdb_gcs_secret_sql` public signature and behavior unchanged until `native-duckdb-driver`.

## Edge Cases & Error Semantics

- `ObjectStoreBackend(None)` (via `Any`) or `ObjectStoreBackend("")` raises `ImproperConfigurationError` instead of opening `"memory://"`.
- Path normalization with `base_path="tenant-a"` prefixes keys cleanly without leading `/` duplication and rejects `..` path traversal segments with `ValueError`.
- Calling `sign_sync` / `sign_async` on a backend without signing credentials raises `NotSupportedError` / `ImproperConfigurationError` from the underlying store.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/extensions/test_data_storage_integrations.py` (exercising real `LocalConfig(tmp_path)` and `sqlspec` `ObjectStoreProtocol` `isinstance` conformance).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Fail-closed constructor | `ObjectStoreBackend("")` and `ObjectStoreBackend(None)` | Both raise `ImproperConfigurationError`. |
| `ObjectStoreProtocol` sync/async CRUD + `move` | `backend = ObjectStoreBackend(f"file://{tmp_path}", base_path="sql")`; `await backend.write_bytes_async("a.bin", b"123")`; `await backend.move_async("a.bin", "b.bin")` | `isinstance(backend, ObjectStoreProtocol)` is `True`; `exists_async("a.bin")` is `False`; `await backend.read_bytes_async("b.bin") == b"123"`. |
| Streaming & cursor pagination | `await backend.put_stream_async("big.bin", [b"chunk1-", b"chunk2"])`, then `list_page_async(limit=1)` and `get_stream_async("big.bin")` | Stream chunks concatenate to `b"chunk1-chunk2"`; `list_page_async` returns `(["b.bin"], cursor)` then `(["big.bin"], None)`. |
| Path traversal guard | `backend.read_bytes("../secret.txt")` | Raises `ValueError`. |
| DuckDB GCS secret helper preserved | `build_duckdb_gcs_secret_sql(...)` | Emits expected `CREATE SECRET` SQL unchanged. |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/extensions/test_data_storage_integrations.py -k sqlspec -q`
- Final Green + lint + typecheck gate: `uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/storage.py src/py/tests/unit/extensions/test_data_storage_integrations.py && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/storage.py && uv run --no-sync pytest src/py/tests/unit/extensions/test_data_storage_integrations.py -q`

## Acceptance Criteria

- [ ] `ObjectStoreBackend` fails closed on omitted/empty URI and conforms to SQLSpec `ObjectStoreProtocol` across sync and async operations (R3, R4).
- [ ] `build_duckdb_gcs_secret_sql` remains intact for `native-duckdb-driver` (R4).
- [ ] Zero inline `#` comments in touched Python files.

## Notes & Discoveries

- [2026-11-07 04:15] Consult the `sqlspec` plugin skill conventions when checking upstream `ObjectStoreProtocol` method names.

[^sqlspec-storage-mod]: `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` — SQLSpec storage backend implementation.
[^py-store-api]: `src/py/vertebra/storage/_store.py::SyncObjectStore` — Underlying Vertebra object store classes.
