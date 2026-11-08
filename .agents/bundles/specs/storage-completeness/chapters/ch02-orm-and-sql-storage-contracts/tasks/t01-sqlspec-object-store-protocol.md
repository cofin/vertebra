---
type: Task
id: "storage-completeness:ch02:t01"
chapter_id: ch02-orm-and-sql-storage-contracts
title: Complete fail-closed SQLSpec ObjectStoreProtocol over SyncObjectStore and AsyncObjectStore
description: Remove implicit memory:// defaults in vertebra.extensions.sqlspec.storage.ObjectStoreBackend and implement full sync/async SQLSpec ObjectStoreProtocol streaming, pagination, rename, and presigning methods.
status: stable
state: completed
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
plan_revision: 4
plan_commit: 1e1f028a
state_revision: 1
claimed_by: agent:sqlspec_storage
claimed_at: "2026-11-08T02:25:35.891222+00:00"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-08T02:37:24.610413+00:00"
last_verified_commit: 7e13257bd58739520c11ca7a2c3e012867f660d9
verification_evidence:
  red: "uv run --no-sync pytest src/py/tests/unit/extensions/test_data_storage_integrations.py -k sqlspec -q: exit 1; omitted-target regression fails (1 failed, 5 passed)."
  green: "uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/storage.py src/py/tests/unit/extensions/test_data_storage_integrations.py && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/storage.py && uv run --no-sync pytest src/py/tests/unit/extensions/test_data_storage_integrations.py -q: exit 0; 49 passed."
  format: "uv run --no-sync ruff format src/py/vertebra/extensions/sqlspec/storage.py src/py/tests/unit/extensions/test_data_storage_integrations.py: exit 0."
created_at: "2026-11-07T04:15:00Z"
updated_at: "2026-11-08T02:37:24.610413+00:00"
commit: 7e13257bd58739520c11ca7a2c3e012867f660d9
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

# Task t01: Complete Fail-Closed SQLSpec `ObjectStoreProtocol` Over Shared Storage Handles

## Objective & Scope Boundary

Make `ObjectStoreBackend` reject omitted/empty targets, retain SQLSpec's exact `*_sync` / `*_async` protocol, and add native streaming, pagination, and seekable-reader extensions. Share one engine between sync and async handles; moves use native `rename`. Preserve `build_duckdb_gcs_secret_sql` and all Arrow methods; Arrow/Parquet `_iter_arrow_async` changes belong to `storage-completeness:ch04:t02`.[^sqlspec-storage-mod][^py-store-api]

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/storage.py` — `ObjectStoreBackend`, `register_storage_alias`, and `build_duckdb_gcs_secret_sql`.[^sqlspec-storage-mod]
- `src/py/vertebra/storage/_store.py` — `SyncObjectStore`, `AsyncObjectStore`, `SyncReadableFile`, `AsyncReadableFile`, `ObjectMeta`, and `PutResult`.[^py-store-api]
- `src/py/tests/unit/extensions/test_data_storage_integrations.py` — SQLSpec protocol, pipelines, signing, and Arrow regression coverage.
- `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` — SQLSpec extension integration rules.
- `.agents/skills/python-framework/SKILL.md` — Python typing, docstrings, and module rules.

**Installed upstream contract evidence:** SQLSpec `0.71.1`, `.venv/lib/python3.12/site-packages/sqlspec/protocols.py::ObjectStoreProtocol`; inspect that class for the complete signature inventory. Protocol byte/text/CRUD/metadata names use `*_sync` and `*_async`. `get_stream_*`, `put_stream_*`, `list_page_*`, and `open_*` below are Vertebra adapter extensions, not SQLSpec protocol members.

## Symbol & Signature Contracts

- Preserve the existing constructor's `store`, `base_path`, `prefix`, `options`, `signing`, `token_cache`, and `**kwargs` keywords. Change `uri` to `str | SyncObjectStore | AsyncObjectStore | StorageConfig | None = None`. Select explicit `store` before `uri`; an omitted/None/blank selected target raises `ImproperConfigurationError`. Construct one `SyncObjectStore` from a URI/config and derive `.to_async()`; explicit stores derive their paired handle via `.to_sync()` / `.to_async()`.
- Preserve `resolve_uri`, `read_bytes_sync`, `write_bytes_sync`, `read_text_sync`, `write_text_sync`, `exists_sync`, `delete_sync`, `copy_sync`, `move_sync`, `list_objects_sync`, `glob_sync`, `is_object_sync`, `is_path_sync`, `get_metadata_sync`, all corresponding existing async methods, Arrow methods, and overloaded `sign_sync` / `sign_async` signatures. Do not introduce plain-sync compatibility aliases.
- `write_bytes_sync(path: str | Path, data: bytes, **kwargs: Any) -> None` and its async counterpart forward `content_type`, `attributes`, `if_not_exists`, `if_match`, and `if_none_match` to native `put`; text writes pass these options through. `move_sync(source: str | Path, destination: str | Path, **kwargs: Any) -> None` and `move_async` use paired store `rename` with the existing default overwrite behavior.
- Add `get_stream_sync(path: str | Path, *, chunk_size: int | None = None) -> Iterator[StorageBuffer]` and `get_stream_async(...) -> AsyncIterator[StorageBuffer]` as iterator factories delegating to native `get_stream`. `StorageBuffer` is a type-only import from `vertebra._storage`. Keep SQLSpec `stream_read_sync(...) -> Iterator[bytes]` and `async def stream_read_async(...) -> AsyncIterator[bytes]`; callers await the latter before iterating and receive byte chunks. No full-object buffering.
- Add `put_stream_sync(path: str | Path, chunks: Iterable[Buffer], **kwargs: Any) -> PutResult` and `async def put_stream_async(path: str | Path, chunks: AsyncIterable[Buffer] | Iterable[Buffer], **kwargs: Any) -> PutResult`, forwarding the same conditional/metadata write options to paired `put_stream`.
- Add `list_page_sync(prefix: str = "", *, cursor: str | None = None, limit: int = 1000) -> tuple[list[str], str | None]` and the async counterpart. Qualify the prefix, unqualify each returned `ObjectMeta.path`, preserve the native cursor opaquely, and retain native `limit >= 1` validation.
- Add `open_sync(path: str | Path, *, size: int | None = None, chunk_size: int = 65536) -> SyncReadableFile` and `open_async(...) -> AsyncReadableFile` as reader factories using the paired stores' `open_reader`; both factories are synchronous. The sync reader fetches native metadata immediately when `size=None`; supplying a size avoids that lookup. The async reader remains lazy until reading. Missing-object errors propagate at the native lookup/read boundary.
- Caller wiring: SQLSpec `SyncStoragePipeline` / `AsyncStoragePipeline` → exact protocol methods → the shared paired native store; adapter extensions → native streaming/listing/reader seams. All key-bearing operations reject `..` segments before namespace qualification.

## Edge Cases & Error Semantics

- `ObjectStoreBackend()`, `ObjectStoreBackend(None)`, `ObjectStoreBackend("")`, and `ObjectStoreBackend("  ")` raise `ImproperConfigurationError`; `ObjectStoreBackend(store=explicit_store)` succeeds. Explicit `MemoryConfig()` remains supported.
- `base_path="tenant-a"` prefixes relative keys exactly once. Any `..` segment in object keys or configured base paths raises `ValueError`, including memory and cloud targets.
- Unsupported signing retains the existing adapter's `NotImplementedError` contract. Configured HMAC signing retains GET/PUT and scalar/list return behavior.
- Conditional-write failures propagate from the native store. A failed stream upload does not leave a completed object. Cursor continuation returns only the requested namespace.

## Public-Seam Test Blueprint

**Canonical module:** `src/py/tests/unit/extensions/test_data_storage_integrations.py`. Use real local/native storage, the real SQLSpec protocol and pipeline, and concrete bytes. Add one behavioral test at a time and observe the intended Red failure before editing production code.

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Fail-closed constructor | Omitted, None, empty, whitespace target; explicit `store=SyncObjectStore.from_local(tmp_path)` | First four raise `ImproperConfigurationError`; explicit store works and `isinstance(backend, ObjectStoreProtocol)` is true. |
| Sync/async writes and rename | `write_bytes_sync("a.bin", b"123")`; `await move_async("a.bin", "b.bin")` | `exists_sync("a.bin")` is false and `await read_bytes_async("b.bin") == b"123"`. |
| Conditional write + metadata | Write `"b.bin"` with `if_not_exists=True`; write `"meta.bin"` with `content_type="text/plain", attributes={"tenant": "a"}` | Existing object is retained on failure; metadata is visible from the paired underlying store. |
| Streaming and pagination | `await put_stream_async("big.bin", [b"chunk1-", b"chunk2"])`; `list_page_sync(limit=1)` then `await list_page_async(cursor=cursor, limit=1)` | `get_stream_sync` and async stream concatenate to `b"chunk1-chunk2"`; isolated seed `b.bin,big.bin` pages return `["b.bin"]` then `["big.bin"]`. |
| Seekable readers | `open_sync("big.bin")` and `open_async("big.bin")` | Seek/read reproduce `b"chunk1-chunk2"` and close cleanly. |
| Namespace refusal | `read_bytes_sync("../secret.txt")` and equivalent async/native extensions | Raise `ValueError` before reading outside the prefix. |
| Existing pipelines, signing, Arrow, secret helper | Existing canonical SQLSpec and DuckDB-secret tests | Preserve their byte/table/signature/SQL assertions. |

## Verification

- Initial Red: `uv run --no-sync pytest src/py/tests/unit/extensions/test_data_storage_integrations.py -k sqlspec -q` after adding the focused regression.
- Final: `uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/storage.py src/py/tests/unit/extensions/test_data_storage_integrations.py && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/storage.py && uv run --no-sync pytest src/py/tests/unit/extensions/test_data_storage_integrations.py -q`.

## Acceptance Criteria

- [x] Omitted/blank storage configuration raises `ImproperConfigurationError`; real SQLSpec protocol and pipeline behavior remains conformant (R3, R4).
- [x] Streaming, pagination, readers, conditional metadata writes, and rename delegate to one shared native engine without full-object buffering (R1, R4).
- [x] Namespace traversal is rejected; configured and unsupported signing retain their public contracts (R4).
- [x] Arrow methods and `build_duckdb_gcs_secret_sql` remain intact for their owning downstream flows (R4).
- [x] Touched Python passes lint/type checks with zero inline comments.

## Notes & Discoveries

- Native `SyncReadableFile.__init__` performs `store.head(key)` for omitted size; explicit size avoids the lookup. `AsyncReadableFile` defers metadata until reading. The adapter preserves both native behaviors under approved plan revision 4.
- Real local and memory stores verify shared sync/async handles, byte and text write conditions, metadata, stream options, failed-source upload abort, opaque cursor continuation, seek/read/close, traversal refusal, and unsupported signing. Existing SQLSpec pipelines, Arrow, HMAC signing, and DuckDB-secret assertions pass unchanged.
- Explicit stores retain their canonical address when the separate URI is omitted or blank; blank URI strings no longer override `store.url`.
- Focused Red commands for `sqlspec_shared_store or sqlspec_write_metadata`, `sqlspec_native_streams`, `sqlspec_seekable`, `sqlspec and traversal`, and `sqlspec_explicit_store` each expose the corresponding missing behavior before its repair. The final gate runs against implementation commit `7e13257b`.

- Installed contract baseline: SQLSpec `0.71.1`; metadata is `ObjectMeta`. Sync protocol names have `_sync`; native iterator/page/reader helpers are explicit adapter extensions.

[^sqlspec-storage-mod]: `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` — SQLSpec storage backend and public helper implementation.
[^py-store-api]: `src/py/vertebra/storage/_store.py::SyncObjectStore` — Paired native store, streaming, metadata, and reader signatures.
