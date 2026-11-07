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
  - .venv/lib/python3.12/site-packages/sqlspec/protocols.py
files:
  - src/py/vertebra/extensions/sqlspec/storage.py
tests:
  - src/py/tests/unit/extensions/test_data_storage_integrations.py
estimated_loc: 170
plan_revision: 3
plan_commit: 0acdf576
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-07T20:10:17Z"
last_verified_commit: dd3c9c08
verification_evidence: "Red: omitted URI/conditional writes/prefix traversal fail; Green: focused SQLSpec 9 passed, full integration module 16 passed; mypy source, Ruff check/format, diff check exit 0."
created_at: "2026-11-07T04:15:00Z"
updated_at: "2026-11-07T20:00:00Z"
commit: dd3c9c08
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

Make `ObjectStoreBackend` reject omitted, empty, or whitespace URI/store configuration with `ImproperConfigurationError`; preserve the installed SQLSpec `ObjectStoreProtocol` `*_sync` / `*_async` surface and unsupported-signing `NotImplementedError` contract; add native streaming, cursor pagination, and readable-file extensions. Keep `build_duckdb_gcs_secret_sql` unchanged. Arrow/Parquet batch streaming belongs to `storage-completeness:ch04:t02` [^sqlspec-storage-mod] [^py-store-api] [^upstream-protocol].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` — Existing protocol implementation and DuckDB helper [^sqlspec-storage-mod].
- `src/py/vertebra/storage/_store.py::{SyncObjectStore,AsyncObjectStore,SyncReadableFile,AsyncReadableFile}` — Native operations, metadata, shared engine conversion [^py-store-api].
- `src/py/tests/unit/extensions/test_data_storage_integrations.py` — Canonical public protocol and storage-pipeline tests.
- `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` — Extension ownership rules.
- `.agents/skills/python-framework/SKILL.md` — Python typing and layout rules.
- Installed `sqlspec.protocols::ObjectStoreProtocol` — Authoritative upstream suffixes, signatures, and unsupported-signing contract [^upstream-protocol].

## Symbol & Signature Contracts

- Constructor: `ObjectStoreBackend.__init__(self, uri: str | SyncObjectStore | AsyncObjectStore | StorageConfig | None = None, *, store: SyncObjectStore | AsyncObjectStore | StorageConfig | None = None, base_path: str = "", prefix: str = "", options: dict[str, str] | None = None, signing: SigningConfig | None = None, token_cache: SyncGcpTokenCache | AsyncGcpTokenCache | None = None, **kwargs: Any) -> None`. Explicit `store` takes precedence. Reject absent/blank targets before construction. Construct the primary native store immediately to preserve missing-wheel and invalid-configuration failures; obtain its shared-engine sync/async companion only when that side is first used.
- Keep canonical `read_bytes_sync`, `write_bytes_sync`, `read_text_sync`, `write_text_sync`, `exists_sync`, `delete_sync`, `copy_sync`, `move_sync`, `list_objects_sync`, `get_metadata_sync`, `sign_sync` and existing async counterparts. Do not add plain synchronous aliases. `write_bytes_sync` / `write_bytes_async` forward `content_type`, `attributes`, `if_not_exists`, `if_match`, and `if_none_match` to native `put`; text writes reuse them.
- Native extensions (synchronous methods use `_sync`):
  - `list_page_sync(self, prefix: str = "", *, cursor: str | None = None, limit: int = 1000) -> tuple[list[str], str | None]` and async `list_page_async` return backend-relative keys and the opaque native cursor.
  - `get_stream_sync(self, path: str | Path, *, chunk_size: int | None = None) -> Iterator[StorageBuffer]` and async-generator `get_stream_async(...) -> AsyncIterator[StorageBuffer]` delegate to native chunk streams.
  - `put_stream_sync(self, path: str | Path, chunks: Iterable[Buffer], **kwargs: Any) -> None` and async `put_stream_async(self, path: str | Path, chunks: AsyncIterable[Buffer] | Iterable[Buffer], **kwargs: Any) -> None` forward write options to native incremental multipart upload.
  - `open_sync(self, path: str | Path, *, size: int | None = None, chunk_size: int = 65_536) -> SyncReadableFile` and `open_async(...) -> AsyncReadableFile` return the corresponding context-managed readable-file handle directly; the latter is not a coroutine.
- `move_sync` / `move_async` delegate to native `rename`. Signing preserves SQLSpec's scalar/list overloads and `NotImplementedError` for unsupported signing [^upstream-protocol].
- Caller map: SQLSpec registry/pipeline -> canonical protocol methods -> paired native object stores. Vertebra callers -> streaming/page/open extensions -> the same native engine. `base_path` prefixes and returned keys remain consistent on both surfaces.
- Preserve `build_duckdb_gcs_secret_sql` public signature and behavior.

## Edge Cases & Error Semantics

- `ObjectStoreBackend()`, `ObjectStoreBackend(None)`, empty/whitespace URI, or invalid target type raises `ImproperConfigurationError`; explicit memory URI/config remains valid.
- `base_path="tenant-a"` prefixes keys once; reject `..` segments in relative keys, URI keys, and `base_path` with `ValueError` before native I/O.
- Unsupported native signing raises `NotImplementedError` on both protocol methods, matching SQLSpec. Configuration errors unrelated to unsupported signing propagate.
- `list_page_*` preserves an opaque native cursor; native `limit < 1` validation raises `ValueError`. Returned paths exclude `base_path`.
- Empty chunk streams create an empty object; an input stream error propagates after native upload abort. Conditional write collision raises native `FileExistsError`.

## Public-Seam Test Blueprint

Canonical module: `src/py/tests/unit/extensions/test_data_storage_integrations.py`, using real `LocalConfig` / explicit memory stores and runtime `ObjectStoreProtocol` conformance.

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Fail-closed constructor | Omitted URI, `None`, `""`, `"  "` | `ImproperConfigurationError` before native creation. |
| Protocol CRUD and move | Backend over `file://{tmp_path}`, `base_path="sql"`; async write `"a.bin"`, `b"123"`; move to `"b.bin"` | Runtime protocol conformance; old key absent; new bytes `b"123"`; equivalent sync CRUD works. |
| Conditional bytes and text writes | Write existing key with `if_not_exists=True` | `FileExistsError` through sync/async bytes/text methods. |
| Native streams and pagination | `put_stream_async("big.bin", [b"chunk1-", b"chunk2"])`; `list_page_async(limit=1)` twice | Relative keys `["b.bin"]` then `["big.bin"]`, final cursor `None`; joined native chunks equal `b"chunk1-chunk2"`; equivalent sync stream/page works. |
| Readable-file handles | `open_sync("b.bin")` / `async with open_async("b.bin")` | File reads return `b"123"`; handles close on context exit. |
| Traversal refusal | `read_bytes_sync("../secret.txt")`, nested traversal, invalid `base_path` | `ValueError` before I/O. |
| Unsupported signing | Unsigned local backend `sign_sync("b.bin")` / `await sign_async("b.bin")` | `NotImplementedError` matching upstream protocol. |
| DuckDB helper | Existing `build_duckdb_gcs_secret_sql` test | Exact SQL/params remain unchanged. |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/extensions/test_data_storage_integrations.py -k sqlspec -q`
- Final Green + lint + typecheck gate: `uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/storage.py src/py/tests/unit/extensions/test_data_storage_integrations.py && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/storage.py && uv run --no-sync pytest src/py/tests/unit/extensions/test_data_storage_integrations.py -q`

## Acceptance Criteria

- [x] `ObjectStoreBackend` fails closed on omitted/empty URI and conforms to SQLSpec `ObjectStoreProtocol` across sync and async operations (R3, R4).
- [x] `build_duckdb_gcs_secret_sql` remains intact for `native-duckdb-driver` (R4).
- [x] Zero inline `#` comments in touched Python files.

## Notes & Discoveries

- Native streaming/page/readable-file extensions, conditional puts, fail-closed construction, shared-engine lazy companions, and native rename pass focused and full integration tests at `dd3c9c08`; existing Arrow and DuckDB helper behavior remains green.

- Installed `sqlspec.protocols::ObjectStoreProtocol` uses `*_sync` exclusively and requires `NotImplementedError` for unsupported URL signing. Streaming/page/open additions are Vertebra extensions rather than upstream protocol members; the existing focused baseline passes (`2 passed`, exit `0`).

[^sqlspec-storage-mod]: `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` — SQLSpec storage backend implementation.
[^py-store-api]: `src/py/vertebra/storage/_store.py::SyncObjectStore` — Underlying native object store facades.
[^upstream-protocol]: Installed `sqlspec.protocols::ObjectStoreProtocol` — Synchronous suffixes and unsupported-signing error contract.
