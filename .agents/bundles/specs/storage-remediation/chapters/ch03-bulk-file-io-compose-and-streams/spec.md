---
type: Spec
flow_id: storage-remediation
parent_flow: storage-remediation
chapter_id: ch03-bulk-file-io-compose-and-streams
title: "Chapter 03: Bulk File I/O, Server-Side Compose, Glob, and Read-Ahead/Writable Streams"
description: Stream Rust put_file (> 8 MiB multipart) and atomic get_file, drive bounded-concurrency get_files/put_files (download_many, upload_many, /dev/shm staged_download), add copy(..., overwrite=False), glob(), and server-side GCS compose() (<= 32-way tree fold + multipart fallback), pass immutable buffers directly without memoryview wrapping, and implement 4 MiB read-ahead open_reader plus streaming multipart open_writer (SyncWritableFile / AsyncWritableFile).
status: stable
state: completed
planning_status: complete
depends_on_chapters:
  - ch01-native-gcs-plane-and-signing
  - ch02-zero-copy-ffi-keys-and-backends
replan_trigger: null
current_task: null
plan_revision: 1
plan_commit: fd571e3a
state_revision: 3
last_verified_checkpoint: 46d39d95
last_reviewed_commit: 46d39d95
created_at: "2026-11-08T20:55:00Z"
updated_at: "2026-11-09T01:00:00Z"
generated:
  by: flow/0.28.0
  at: "2026-11-08T20:55:00Z"
verified:
  - by: agent:code-reviewer
    at: "2026-11-09T01:00:00Z"
    commit: 46d39d95
sources:
  - id: storage-store-rs
    resource: src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine
    title: Rust ObjectStoreEngine file I/O, conditional copy, and GCS compose
  - id: storage-lib-rs
    resource: src/rs/vertebra-storage/src/lib.rs::PySyncObjectStore
    title: PyO3 bindings for SyncObjectStore and AsyncObjectStore
  - id: storage-pyi
    resource: src/py/vertebra/_storage.pyi::SyncObjectStore
    title: Type stubs for vertebra._storage
  - id: py-store
    resource: src/py/vertebra/storage/_store.py::SyncObjectStore
    title: Python SyncObjectStore, AsyncObjectStore, SyncReadableFile, and AsyncReadableFile
  - id: py-storage-init
    resource: src/py/vertebra/storage/__init__.py
    title: Public vertebra.storage package exports
tags:
  - storage
  - gcs
  - streaming
  - bulk-io
  - compose
---

# Chapter 03: Bulk File I/O, Server-Side Compose, Glob, and Read-Ahead/Writable Streams

<!-- truth: start -->
- **Streaming File I/O & Bounded-Concurrency Batch Transfers**: `ObjectStoreEngine::put_file` streams `> 8 MiB` files in `8 MiB` multipart chunks (aborting on error); `get_file` streams `get_opts` payloads to a sibling `.part` temporary file, flushes, and renames atomically (unlinking on error); and `get_files` / `put_files` run on Tokio via `buffer_unordered(concurrency)` preserving input order and cleaning up partial downloads on failure [^storage-store-rs] [^storage-lib-rs].
- **Conditional `copy`, Prefix-Pushed `glob`, & Server-Side GCS `compose`**: `copy(src, dst, *, overwrite=False)` delegates to `copy_if_not_exists` (with atomic `hard_link` / `create_new` fallback on `file://` raising `FileExistsError`); `glob(pattern)` uses `head()` when wildcard-free and pushes down the literal prefix before `[*?[]` to `list(prefix)` + `fnmatchcase`; and `compose(sources, dst, *, content_type=None)` uses native GCS JSON `/storage/v1/b/<bucket>/o/<dst>/compose` with `<= 32`-way tree folding and guaranteed `.compose-*` temp-object cleanup on `gs://`, falling back to streaming multipart concatenation on all other backends [^storage-store-rs] [^py-store].
- **Direct Zero-Copy Buffer Passthrough, `4 MiB` Read-Ahead `open_reader`, & Streaming `open_writer`**: `SyncObjectStore` and `AsyncObjectStore` pass buffers directly to native `put` / `put_multipart` without `memoryview` wrapping so `PyBytes` stays zero-copy; `SyncReadableFile` and `AsyncReadableFile` cache `read_ahead: int = 4_194_304` range blocks; and `open_writer` returns `SyncWritableFile` / `AsyncWritableFile` (`part_size: int = 8_388_608`), exported alongside `PreconditionError` from `vertebra.storage` [^py-store] [^py-storage-init].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Analytical and pipeline workloads transferring multi-hundred-megabyte Parquet shards currently hit five bottlenecks in `vertebra.storage`:
1. `ObjectStoreEngine::put_file` reads entire files into RAM via `tokio::fs::read`, and there is no native streaming `get_file`, bounded-concurrency `get_files` / `put_files`, or `/dev/shm`-backed `staged_download` context manager [^storage-store-rs] [^py-store].
2. `copy` always overwrites destinations unconditionally, and there is no `glob()` helper with prefix pushdown or server-side GCS `compose()` (`<= 32` objects per `/compose` call, tree-folded for `> 32` shards) [^storage-store-rs] [^py-store].
3. `_store.py` wraps `data` in `memoryview(data).cast("B")` on `put` and `put_multipart`, masking `PyBytes` from `extract_buffer_bytes` (`PyBackedBytes` zero-copy), while `S3Config.virtual_hosted_style` and `R2Config.custom_domain` options are omitted in Python [^py-store].
4. `SyncReadableFile` / `AsyncReadableFile` issue an uncached `get_range` network RTT on every small Parquet footer/metadata read, and there is no `open_writer` (`SyncWritableFile` / `AsyncWritableFile`) for streaming binary/CSV/JSONL writers directly into multipart uploads [^py-store].

**Non-Goals**:
- `StreamingStorageResponse` and `_arrow.py` / `db/arrow.py` codecs (Chapter 04).

### 1.2 Upstream Execution Baseline

- Depends on `ch01-native-gcs-plane-and-signing` and `ch02-zero-copy-ffi-keys-and-backends`; parent spec [`../../spec.md`](../../spec.md).

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| Rust Bulk I/O & Compose | `src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine` [^storage-store-rs] | Local filesystem + loopback HTTP GCS `/compose` and multipart upload boundaries. |
| PyO3 Store Bindings | `src/rs/vertebra-storage/src/lib.rs::PySyncObjectStore` [^storage-lib-rs], `src/py/vertebra/_storage.pyi::SyncObjectStore` [^storage-pyi] | `py.detach` sync bindings and `CompletionQueue` `RustTaskAwaitable` async bindings. |
| Python Store & File Streams | `src/py/vertebra/storage/_store.py::SyncObjectStore` [^py-store], `src/py/vertebra/storage/__init__.py` [^py-storage-init] | Public `SyncObjectStore`, `AsyncObjectStore`, `SyncReadableFile`, `AsyncReadableFile`, `SyncWritableFile`, and `AsyncWritableFile` APIs. |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [`object-storage.md`](../../../../knowledge/domains/storage/object-storage.md), [`pyo3-bridge`](../../../../../skills/pyo3-bridge/SKILL.md), [`python-framework`](../../../../../skills/python-framework/SKILL.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R3.1 (`t01`)**: In `src/rs/vertebra-storage/src/store.rs`, stream `put_file` (`> 8 MiB` multipart chunks with `abort` on error), add atomic streaming `get_file`, bounded-concurrency `get_files` and `put_files` (`concurrency >= 1`, input-order results, partial-download cleanup on error), conditional `copy(src, dst, if_not_exists: bool)` (`copy_if_not_exists` + `file://` atomic fallback), and `compose(sources, dst, content_type)` (GCS JSON `/compose` `<= 32`-way tree fold with guaranteed temp-object cleanup + streaming multipart fallback for non-`gs` stores).
- **R3.2 (`t02`)**: Expose `get_file`, `get_files`, `put_files`, `copy(..., *, if_not_exists: bool = False)`, and `compose` on `PySyncObjectStore` and `PyAsyncObjectStore` in `src/rs/vertebra-storage/src/lib.rs` and `src/py/vertebra/_storage.pyi`.
- **R3.3 (`t03`)**: Implement `get_file`, `download_many`, `upload_many`, `staged_download` (`/dev/shm` default when writable directory exists else `tempfile.gettempdir()`, unlinking staged files in `finally`), `copy(..., *, overwrite: bool = True)`, `glob(pattern: str) -> list[ObjectMeta]`, and `compose(sources, dst, *, content_type: str | None = None) -> PutResult` on `SyncObjectStore` and `AsyncObjectStore`, and wire `S3Config.virtual_hosted_style` + `R2Config.custom_domain` in `_store.py`.
- **R3.4 (`t04`)**: Remove `memoryview(data).cast("B")` wrapping in `SyncObjectStore`/`AsyncObjectStore` `put` and `put_multipart`, add `read_ahead: int = 4_194_304` block caching to `SyncReadableFile` / `AsyncReadableFile` (`open_reader`), implement `SyncWritableFile(io.RawIOBase)` and `AsyncWritableFile` (`open_writer`, `part_size: int = 8_388_608`), and export `PreconditionError`, `SyncWritableFile`, and `AsyncWritableFile` in `vertebra.storage`.

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

- `SyncObjectStore` / `AsyncObjectStore`:
  - `get_file(self, path: str, file_path: str | Path) -> ObjectMeta`
  - `download_many(self, keys: Sequence[str], destination: str | Path, *, concurrency: int = 16) -> list[Path]`
  - `upload_many(self, paths: Sequence[str | Path], destination_prefix: str = "", *, concurrency: int = 16) -> list[PutResult]`
  - `staged_download(self, keys: Sequence[str], destination: str | Path | None = None, *, concurrency: int = 16)`
  - `copy(self, src: str, dst: str, *, overwrite: bool = True) -> None`
  - `glob(self, pattern: str) -> list[ObjectMeta]`
  - `compose(self, sources: Sequence[str], dst: str, *, content_type: str | None = None) -> PutResult`
  - `open_reader(self, path: str, *, size: int | None = None, chunk_size: int = 65536, read_ahead: int = 4_194_304) -> SyncReadableFile | AsyncReadableFile`
  - `open_writer(self, path: str, *, content_type: str | None = None, attributes: Mapping[str, str] | None = None, part_size: int = 8_388_608, if_not_exists: bool = False, if_match: str | None = None, if_none_match: str | None = None) -> SyncWritableFile | AsyncWritableFile`
- `SyncWritableFile(io.RawIOBase)` (`writable() -> True`, `write(b: Buffer) -> int`, `flush() -> None`, `close() -> None`, `abort() -> None`, `result: PutResult | None`, `__enter__`/`__exit__`) and `AsyncWritableFile` (`write(b: Buffer) -> int`, `flush() -> None`, `aclose() -> PutResult | None` (`None` after `abort()`), `abort() -> None`, `result: PutResult | None`, `__aenter__`/`__aexit__`).

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `store.download_many` / `store.staged_download` -> validates unique basenames -> `native.get_files(pairs, concurrency=concurrency)` -> `ObjectStoreEngine::get_files` -> atomic `.part` write + `rename`.
- `store.compose(sources, dst)` -> `native.compose(list(sources), dst, content_type=content_type)` -> `ObjectStoreEngine::compose` -> GCS JSON `/compose` tree fold (`gs://`) or multipart stream concatenation (other schemes).
- `with store.open_writer("out.bin", part_size=8_388_608) as wf:` -> `create_multipart` -> flushes `8 MiB` `write_part` chunks -> `complete()` on clean exit or `abort()` on exception.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| Empty `compose` sources | `store.compose([], "dst.bin")` | Raises `ValueError` | Rejects empty compose inputs cleanly. |
| GCS `compose` `> 32` sources & mid-fold error | `70` sources or `404` on shard `#50` | Folds in `<= 32`-object batches via `{dst}.compose-{idx:04d}` and deletes all temp objects even on error | Zero leaked temporary objects. |
| Duplicate basenames in `download_many` / `upload_many` | `download_many(["a/x.bin", "b/x.bin"], tmp)` | Raises `ValueError` before performing I/O | Prevents silent overwrite collisions. |
| `open_writer` unhandled exception inside `with` / `async with` | `RuntimeError` raised mid-stream | Calls `abort()` automatically and re-raises original exception (`result is None`) | Zero orphaned multipart uploads. |
| `open_reader` seek outside cached `4 MiB` window vs inside window | `seek` + `read` within `[buf_start, buf_end)` vs beyond `buf_end` | In-window read slices `_buf` with `0` extra `get_range` calls; out-of-window read fetches next `min(read_ahead, size - pos)` block (`read(n >= read_ahead)` bypasses cache directly) | Coalesces small Parquet footer/header reads without double-buffering large reads. |

### 2.5 Acceptance Scenarios (Given / When / Then)

1. **Given** `70` shard objects in a loopback GCS server, **When** `await store.compose(shards, "combined.bin", content_type="application/octet-stream")` completes, **Then** `"combined.bin"` holds the exact concatenation of all `70` shards in order and zero `.compose-*` objects remain.
2. **Given** `10` objects in `SyncObjectStore`, **When** `with store.staged_download(keys) as paths:` runs and raises inside the `with` block, **Then** all staged files and temporary staging directories under `/dev/shm` (or temp dir) are deleted on exit.

### 2.6 Alternatives Considered

- **Chosen**: Native Rust `buffer_unordered(concurrency)` for `get_files` / `put_files` + native GCS `/compose` tree fold in `store.rs`.
- **Rejected**: Python thread-pool / `asyncio.Semaphore` loops over single-file `get`/`put` calls (higher GIL traffic and `2x` client network transfer on GCS shard merges).

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/0 files documented in ch03]` (consolidated in Chapter 06 `ch06:t01` and `ch06:t02`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test |
| --- | --- | --- | --- |
| R3.1 | `ch03 / t01` | `behavior_tdd` | `src/rs/vertebra-storage/tests/storage_engine_tests.rs` |
| R3.2 | `ch03 / t02` | `behavior_tdd` | `src/py/tests/unit/storage/test_storage_store.py` |
| R3.3 | `ch03 / t03` | `behavior_tdd` | `src/py/tests/unit/storage/test_storage_store.py` |
| R3.4 | `ch03 / t04` | `behavior_tdd` | `src/py/tests/unit/storage/test_storage_store.py` |

## 5. Implementation Plan

- [x] [f8bf065c] storage-remediation:ch03:t01 — Implement Rust streaming `put_file`/`get_file`, bounded-concurrency `get_files`/`put_files`, conditional `copy`, and server-side GCS `compose` (tasks/t01-rust-bulk-file-io-copy-and-compose.md)
- [x] [62deb8c3] storage-remediation:ch03:t02 — Expose PyO3 bindings and `_storage.pyi` stubs for `get_file`, `get_files`, `put_files`, conditional `copy`, and `compose` (tasks/t02-pyo3-bulk-io-copy-and-compose-bindings.md)
- [x] [70c23f63] storage-remediation:ch03:t03 — Implement Python `get_file`, `download_many`, `upload_many`, `staged_download`, `copy(..., overwrite=...)`, `glob`, `compose`, and S3/R2 config options (tasks/t03-python-bulk-transfers-glob-and-compose.md)
- [x] [19dac0d8] storage-remediation:ch03:t04 — Pass buffers zero-copy without `memoryview` wrapping, add `4 MiB` read-ahead `open_reader`, and add streaming `open_writer` (`SyncWritableFile` / `AsyncWritableFile`) (tasks/t04-read-ahead-reader-writable-streams-and-exports.md)
- **Chapter / Phase Recovery Checkpoint**: `PATH="$PWD/.venv/bin:$PATH" PYO3_PYTHON="$PWD/.venv/bin/python" CARGO_TARGET_DIR=/tmp/vtb-cargo-target cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage && uv run --no-sync pytest src/py/tests/unit/storage -q`

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline (`HEAD`)**: `46d39d95` — `4/4` tasks completed and the zero-context review findings remediated (`081d0163` Python, `46d39d95` Rust): cancellation-safe `.part`/multipart/compose cleanup, failed-part writer abort, 8 MiB framed compose fallback, unique compose temporaries, XML-consistent compose metadata, scoped batch cleanup, and writer finalizers.
- **Remaining Tasks in Chapter**: None.
- **Remaining Chapters**: `ch04`–`ch06`.

[^storage-store-rs]: `src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine` — Rust `ObjectStoreEngine` file I/O, conditional `copy`, and GCS `compose`.
[^storage-lib-rs]: `src/rs/vertebra-storage/src/lib.rs::PySyncObjectStore` — PyO3 bindings for `SyncObjectStore` and `AsyncObjectStore`.
[^storage-pyi]: `src/py/vertebra/_storage.pyi::SyncObjectStore` — Type stubs for `vertebra._storage`.
[^py-store]: `src/py/vertebra/storage/_store.py::SyncObjectStore` — Python `SyncObjectStore`, `AsyncObjectStore`, `SyncReadableFile`, and `AsyncReadableFile`.
[^py-storage-init]: `src/py/vertebra/storage/__init__.py` — Public `vertebra.storage` package exports.
