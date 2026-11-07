---
type: Task
id: "storage-completeness:ch01:t02"
chapter_id: ch01-native-engine-and-auth
title: Wire Python streaming/conditional/paginated storage facade and _storage.pyi stubs
description: Update _storage.pyi and vertebra.storage._store.{SyncObjectStore,AsyncObjectStore,SyncReadableFile,AsyncReadableFile} for streaming reads/uploads, conditional puts, cursor pagination, delimiter listing, and rename.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "storage-completeness:ch01:t01"
read_set:
  - src/rs/vertebra-storage/src/lib.rs
  - src/py/vertebra/_storage.pyi
  - src/py/vertebra/storage/_store.py
  - src/py/tests/unit/storage/test_multicloud_storage.py
  - src/py/tests/unit/storage/test_storage_store.py
  - .agents/skills/python-framework/SKILL.md
files:
  - src/rs/vertebra-storage/src/lib.rs
  - src/py/vertebra/_storage.pyi
  - src/py/vertebra/storage/_store.py
tests:
  - src/py/tests/unit/storage/test_multicloud_storage.py
  - src/py/tests/unit/storage/test_storage_store.py
estimated_loc: 210
plan_revision: 2
plan_commit: cdb978d3
state_revision: 2
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-07T16:44:30Z"
last_verified_commit: cb6b0647
verification_evidence: "Red: uv run --no-sync pytest src/py/tests/unit/storage/test_storage_store.py src/py/tests/unit/storage/test_multicloud_storage.py -q (exit 1, 8 failed); uv run --no-sync python -m mypy.stubtest vertebra._storage (exit 1, 27 errors). Green: cargo fmt --manifest-path src/rs/Cargo.toml --all -- --check (exit 0); uv run --no-sync cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-storage --all-targets -- -D warnings (exit 0); uv run --no-sync cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage (exit 0, 20 passed); make dev (exit 0); uv run --no-sync python -m mypy.stubtest vertebra._storage (exit 0); uv run --no-sync ruff check src/py/vertebra/_storage.pyi src/py/vertebra/storage src/py/tests/unit/storage (exit 0); uv run --no-sync mypy src/py/vertebra/storage (exit 0); uv run --no-sync pytest src/py/tests/unit/storage -q (exit 0, 41 passed)."
created_at: "2026-11-07T04:15:00Z"
updated_at: "2026-11-07T16:44:30Z"
commit: cb6b0647
sources:
  - id: storage-pyi
    resource: src/py/vertebra/_storage.pyi::SyncObjectStore
    title: Type stubs for the vertebra._storage PyO3 extension
  - id: py-store-facade
    resource: src/py/vertebra/storage/_store.py::AsyncObjectStore
    title: Python SyncObjectStore, AsyncObjectStore, SyncReadableFile, and AsyncReadableFile
tags:
  - storage
  - python
  - streaming
  - typing
---

# Task t02: Wire Python Streaming, Conditional, and Paginated Storage Facade and `_storage.pyi` Stubs

## Objective & Scope Boundary

Expose the new Rust streaming, conditional-put, multipart-session, paginated-listing, and rename capabilities from `ch01:t01` through `src/py/vertebra/_storage.pyi` and `src/py/vertebra/storage/_store.py` (`SyncObjectStore`, `AsyncObjectStore`, `SyncReadableFile`, `AsyncReadableFile`) with zero `asyncio.to_thread` calls [^storage-pyi] [^py-store-facade]. Removing duplicate GCP token caches and `self._memory` fallbacks is scoped to `storage-completeness:ch01:t04`.

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/lib.rs` — Exact PyO3 class and method signatures added in `ch01:t01`.
- `src/py/vertebra/_storage.pyi` — Stub definitions verified by `mypy.stubtest vertebra._storage` [^storage-pyi].
- `src/py/vertebra/storage/_store.py` — `SyncObjectStore`, `AsyncObjectStore`, `SyncReadableFile`, `AsyncReadableFile`, and `ObjectMetadata` [^py-store-facade].
- `src/py/tests/unit/storage/test_multicloud_storage.py` — Multi-backend storage unit tests.
- `src/py/tests/unit/storage/test_storage_store.py` — Store facade, readable-file, and zero-`to_thread` AST tests.
- `.agents/skills/python-framework/SKILL.md` — PEP 585/604 typing, `msgspec.Struct(frozen=True, gc=False)`, and zero-inline-comment rules.

## Symbol & Signature Contracts

- `src/py/vertebra/_storage.pyi`:
  - Add `SyncStreamReader`, `AsyncStreamReader`, `SyncMultipartUpload`, and `AsyncMultipartUpload` classes matching `lib.rs` verbatim; add `content_type: str | None` and `attributes: dict[str, str]` properties on `ObjectMeta`; update `SyncObjectStore` and `AsyncObjectStore` with `get_stream`, `create_multipart`, conditional `put(...)` kwargs, `list_page`, `list_with_delimiter`, and `rename`.
- `src/py/vertebra/storage/_store.py`:
  - Extend `ObjectMetadata` (`msgspec.Struct(frozen=True, gc=False)`) with `content_type: str | None = None` and `attributes: dict[str, str] = msgspec.field(default_factory=dict)`.
  - `SyncObjectStore` & `AsyncObjectStore`:
    - `put(self, key: str, data: bytes | bytearray | memoryview, *, content_type: str | None = None, attributes: Mapping[str, str] | None = None, if_not_exists: bool = False, if_match: str | None = None, if_none_match: str | None = None) -> ObjectMetadata` (awaitable on `AsyncObjectStore`)
    - `get_stream(self, key: str, *, chunk_size: int | None = None) -> Iterator[StorageBuffer]` / `AsyncIterator[StorageBuffer]`
    - `put_stream(self, key: str, chunks: Iterable[bytes | memoryview], *, content_type: str | None = None, attributes: Mapping[str, str] | None = None, if_not_exists: bool = False) -> ObjectMetadata` (and `AsyncIterable[bytes | memoryview] | Iterable[bytes | memoryview]` on `AsyncObjectStore`, driving `create_multipart` -> `write_part` -> `complete` with `abort` on exception)
    - `list_page(self, prefix: str = "", *, cursor: str | None = None, limit: int = 1000) -> tuple[list[ObjectMetadata], str | None]`
    - `list_with_delimiter(self, prefix: str = "") -> tuple[list[ObjectMetadata], list[str]]`
    - `rename(self, src: str, dst: str, *, overwrite: bool = True) -> None` (maps `overwrite=False` -> `if_not_exists=True`)
  - `SyncReadableFile` & `AsyncReadableFile`: ensure `__iter__` / `__aiter__` and `read(size)` stream over `get_range` / `get_stream` without loading the entire object when `size > 0`.

## Edge Cases & Error Semantics

- If the chunk iterator passed to `put_stream` raises mid-iteration, `SyncMultipartUpload.abort()` / `await AsyncMultipartUpload.abort()` executes in a `try ... except` cleanup block before re-raising the original exception.
- `put(..., if_not_exists=True)` raises `FileExistsError` when the target key already exists; `rename(..., overwrite=False)` raises `FileExistsError` when `dst` exists.
- `SyncReadableFile.read(0)` / `AsyncReadableFile.read(0)` returns `b""` without issuing a backend RPC; reading at or past `size` returns `b""`.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/unit/storage/test_storage_store.py` and `src/py/tests/unit/storage/test_multicloud_storage.py` (real `LocalConfig(tmp_path)` and explicit `MemoryConfig()` stores).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Async `put_stream` + `get_stream` round-trip | `AsyncObjectStore(LocalConfig(tmp_path)).put_stream("exports/data.bin", async_chunks)` with 4 chunks of `64 KiB`, then `[bytes(buf) async for buf in store.get_stream("exports/data.bin")]` | Uploaded size is `262144`, chunks are `StorageBuffer` instances (`memoryview` zero-copy readable), concatenated bytes match input. |
| Failed `put_stream` aborts upload | Generator yields `b"abc"` then raises `RuntimeError("boom")` into `store.put_stream("exports/partial.bin", gen())` | Raises `RuntimeError("boom")` and `store.exists("exports/partial.bin")` is `False`. |
| Conditional `put` & metadata round-trip | `store.put("cfg.json", b"{}", content_type="application/json", attributes={"env": "prod"}, if_not_exists=True)` followed by duplicate `if_not_exists=True` | First returns `ObjectMetadata(content_type="application/json", attributes={"env": "prod"})`; second raises `FileExistsError`. |
| Paginated & delimiter listing | Seed `"logs/2026/01.log"`, `"logs/2026/02.log"`, `"logs/root.log"`; call `list_with_delimiter("logs/")` and `list_page("logs/", limit=2)` | `list_with_delimiter` returns `([ObjectMetadata(path="logs/root.log")], ["logs/2026"])`; `list_page` returns 2 items + cursor, second call returns remaining 1 item + `None`. |
| Zero `asyncio.to_thread` AST check | AST scan of `src/py/vertebra/storage/_store.py` | Zero `to_thread` references; `mypy.stubtest vertebra._storage` exits `0`. |

## Verification

- Initial Red baseline: `make dev && uv run --no-sync pytest src/py/tests/unit/storage/test_storage_store.py src/py/tests/unit/storage/test_multicloud_storage.py -q`
- Final Green + stubtest + typecheck gate: `make dev && uv run --no-sync python -m mypy.stubtest vertebra._storage && uv run --no-sync mypy src/py/vertebra/storage && uv run --no-sync pytest src/py/tests/unit/storage/test_storage_store.py src/py/tests/unit/storage/test_multicloud_storage.py -q`

## Acceptance Criteria

- [x] `_storage.pyi` matches compiled `vertebra._storage` with zero `mypy.stubtest` discrepancies (R1).
- [x] `SyncObjectStore` and `AsyncObjectStore` support `get_stream`, `put_stream`, conditional `put`, `list_page`, `list_with_delimiter`, and `rename` with zero `asyncio.to_thread` calls (R1).
- [x] Zero inline `#` comments in touched Python files; all explanations live in Google-style docstrings.

## Notes & Discoveries

- [2026-11-07 04:15] Keep `import_optional("vertebra._storage")` lazy inside store/helper methods so CLI and doc imports succeed without `[storage]` installed.
- [2026-11-07 16:38] Guard `std::mem::replace` inside `AsyncStreamState::next_bytes` (`src/rs/vertebra-storage/src/lib.rs`) with `matches!(self.init, AsyncStreamInit::Pending { .. })` so `AsyncStreamInit::Open(Some(stream))` survives across multi-chunk `__anext__` / `next_chunk` calls on `LocalFileSystem` and network backends.
- [2026-11-07 16:40] `PutResult` subclasses `ObjectMeta` (`msgspec.Struct(frozen=True, gc=False, kw_only=True)`) so write results carry `last_modified`, `content_type`, and `attributes` while preserving `isinstance(..., ObjectMeta)` and `isinstance(..., PutResult)` compatibility.

[^storage-pyi]: `src/py/vertebra/_storage.pyi::SyncObjectStore` — Type stub contract for `vertebra._storage`.
[^py-store-facade]: `src/py/vertebra/storage/_store.py::AsyncObjectStore` — Python object storage facade.
