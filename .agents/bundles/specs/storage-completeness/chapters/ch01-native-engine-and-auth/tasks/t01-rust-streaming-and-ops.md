---
type: Task
id: "storage-completeness:ch01:t01"
chapter_id: ch01-native-engine-and-auth
title: Implement streaming reads, incremental multipart sessions, conditional puts, pagination, and rename in vertebra-storage
description: Extend ObjectStoreEngine and PyO3 bindings in vertebra-storage with zero-copy stream readers, incremental multipart uploads, conditional put options, cursor pagination, and server-side rename.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - src/rs/vertebra-storage/src/store.rs
  - src/rs/vertebra-storage/src/r2.rs
  - src/rs/vertebra-storage/src/lib.rs
  - src/rs/vertebra-storage/tests/storage_engine_tests.rs
  - .agents/skills/pyo3-bridge/SKILL.md
  - .agents/bundles/knowledge/domains/storage/object-storage.md
files:
  - src/rs/vertebra-storage/src/store.rs
  - src/rs/vertebra-storage/src/r2.rs
  - src/rs/vertebra-storage/src/lib.rs
tests:
  - src/rs/vertebra-storage/tests/storage_engine_tests.rs
estimated_loc: 220
plan_revision: 2
plan_commit: cdb978d3
state_revision: 2
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-07T16:12:00Z"
last_verified_commit: beb6a972
verification_evidence: "uv run --no-sync cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage && uv run --no-sync cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-storage --all-targets -- -D warnings (exit 0)"
created_at: "2026-11-07T04:15:00Z"
updated_at: "2026-11-07T16:12:00Z"
commit: beb6a972
sources:
  - id: store-engine
    resource: src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine
    title: Native Rust ObjectStoreEngine and StoredObjectMeta
  - id: r2-multipart
    resource: src/rs/vertebra-storage/src/r2.rs::build_r2_store
    title: Cloudflare R2 multipart chunk uniformity constraints
  - id: pyo3-storage-lib
    resource: src/rs/vertebra-storage/src/lib.rs::PyAsyncObjectStore
    title: PyO3 SyncObjectStore and AsyncObjectStore completion bindings
tags:
  - storage
  - rust
  - pyo3
  - streaming
---

# Task t01: Implement Streaming Reads, Incremental Multipart Sessions, Conditional Puts, Pagination, and Rename in `vertebra-storage`

## Objective & Scope Boundary

Deliver true chunk-streaming reads, stateful incremental multipart uploads (without `Vec<Bytes>` whole-payload buffering or R2 `Vec<u8>` concatenation), conditional `put` options (`if_not_exists`, `if_match`, `if_none_match`, `content_type`, `attributes`), cursor-paginated `list_page` / `list_with_delimiter`, and `rename` in `src/rs/vertebra-storage` [^store-engine] [^r2-multipart] [^pyo3-storage-lib]. Python facade methods and `.pyi` stubs are out of scope and belong to `storage-completeness:ch01:t02`.

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine` — Existing `put`, `put_multipart`, `get`, `get_range`, `head`, `copy`, `list`, and `StoredObjectMeta` [^store-engine].
- `src/rs/vertebra-storage/src/r2.rs` — R2 uniform part-size constraints and builder options [^r2-multipart].
- `src/rs/vertebra-storage/src/lib.rs::{PySyncObjectStore,PyAsyncObjectStore,PyObjectMeta,StorageBuffer,RustTaskAwaitable}` — PyO3 classes and `RustCompletionRing` async completion bridge [^pyo3-storage-lib].
- `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — Canonical Rust engine integration test suite.
- `.agents/skills/pyo3-bridge/SKILL.md` — GIL release and `AsyncCompletionQueue` worker-thread rules.
- `.agents/bundles/knowledge/domains/storage/object-storage.md` — Storage zero-copy buffer invariants.

## Symbol & Signature Contracts

- `src/rs/vertebra-storage/src/store.rs`:
  - Extend `StoredObjectMeta` with `pub content_type: Option<String>` and `pub attributes: HashMap<String, String>`.
  - Add `pub struct PutConditionalOptions { pub if_not_exists: bool, pub if_match: Option<String>, pub if_none_match: Option<String>, pub content_type: Option<String>, pub attributes: HashMap<String, String> }`.
  - Add `pub struct MultipartUploadSession` wrapping `Box<dyn MultipartUpload>` plus an 8 MiB (`8 * 1024 * 1024`) rolling `BytesMut` re-chunker when `scheme` is `"r2"` or `"oci"` so non-trailing parts have identical byte lengths without buffering the full object in memory:
    - `pub async fn write_chunk(&mut self, chunk: Bytes) -> Result<(), ObjectStoreError>`
    - `pub async fn complete(&mut self) -> Result<PutResult, ObjectStoreError>`
    - `pub async fn abort(&mut self) -> Result<(), ObjectStoreError>`
  - Add methods on `ObjectStoreEngine`:
    - `pub async fn put_opts(&self, path: &str, data: Bytes, opts: PutConditionalOptions) -> Result<StoredObjectMeta, ObjectStoreError>`
    - `pub async fn get_stream(&self, path: &str) -> Result<BoxStream<'static, Result<Bytes, ObjectStoreError>>, ObjectStoreError>`
    - `pub async fn create_multipart(&self, path: &str, opts: PutConditionalOptions) -> Result<MultipartUploadSession, ObjectStoreError>`
    - `pub async fn list_page(&self, prefix: Option<&str>, offset: Option<&str>, limit: usize) -> Result<(Vec<StoredObjectMeta>, Option<String>), ObjectStoreError>`
    - `pub async fn list_with_delimiter(&self, prefix: Option<&str>) -> Result<(Vec<StoredObjectMeta>, Vec<String>), ObjectStoreError>`
    - `pub async fn rename(&self, src: &str, dst: &str, if_not_exists: bool) -> Result<(), ObjectStoreError>` (`rename_if_not_exists` vs `rename` on `dyn ObjectStore`).
- `src/rs/vertebra-storage/src/lib.rs`:
  - Expose `content_type` and `attributes` getters on `PyObjectMeta`.
  - Add `PySyncStreamReader` (`__iter__`, `__next__ -> StorageBuffer`), `PyAsyncStreamReader` (`__aiter__`, `__anext__ -> RustTaskAwaitable`), `PySyncMultipartUpload` (`write_part`, `complete`, `abort`), and `PyAsyncMultipartUpload` (`write_part`, `complete`, `abort` returning `RustTaskAwaitable`), plus `get_stream`, `create_multipart`, `put` optional keyword args (`if_not_exists`, `if_match`, `if_none_match`, `content_type`, `attributes`), `list_page`, `list_with_delimiter`, and `rename` on `PySyncObjectStore` and `PyAsyncObjectStore`.

## Edge Cases & Error Semantics

- `put_opts` with `if_not_exists: true` on an existing key maps `object_store::Error::AlreadyExists` to `PyFileExistsError` (Python) and typed `ObjectStoreError::AlreadyExists` (Rust).
- `put_opts` with `if_match: Some(etag)` when remote ETag differs maps `object_store::Error::Precondition` to `PyPermissionError` / precondition `OSError` with ETag context.
- `MultipartUploadSession::abort()` is idempotent if called after an error or before any part is flushed; `complete()` flushes the trailing `< 8 MiB` buffer as the final part (or uploads an empty body when `0` bytes were written).
- `list_page` with `limit == 0` raises `PyValueError("limit must be >= 1")`; when fewer than `limit` items remain after `offset`, `next_cursor` is `None`.
- Never call `Python::attach` inside Tokio futures spawned for `PyAsyncStreamReader` or `PyAsyncMultipartUpload`; deliver `Result<Option<Bytes>, ...>` via `RustCompletionRing`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-storage/tests/storage_engine_tests.rs` (real `LocalFileSystem` and `InMemory` `ObjectStoreEngine` seams, zero mocks).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Streaming chunk read | Write `3 * 65536` deterministic bytes to `"blobs/stream.bin"`, read via `engine.get_stream("blobs/stream.bin")` | Concatenated stream chunks equal exact `196608`-byte payload with `>= 1` `Bytes` frames. |
| Incremental R2-mode multipart | `create_multipart` (`scheme = "r2"`) fed 5 uneven chunks (`3 MiB`, `3 MiB`, `3 MiB`, `1 MiB`, `512 KiB`), then `complete()` | Flushes one exact `8 MiB` part + one `2.5 MiB` final part without whole-object concatenation; `get` returns exact `10.5 MiB` payload. |
| Multipart abort cleanup | `create_multipart("blobs/aborted.bin")`, `write_chunk(b"part1")`, `abort()` | `engine.exists("blobs/aborted.bin")` returns `false`. |
| Conditional `if_not_exists` & `if_match` | First `put_opts("locks/a", b"v1", if_not_exists=true)`, second `put_opts("locks/a", b"v2", if_not_exists=true)`, third `put_opts("locks/a", b"v2", if_match=Some("wrong-etag"))` | First succeeds returning `etag1`; second returns `AlreadyExists`; third returns `Precondition`; `put_opts` with `if_match=etag1` updates to `b"v2"`. |
| Cursor pagination & delimiter listing | Seed `"dir/a.txt"`, `"dir/b.txt"`, `"dir/sub/c.txt"`; call `list_page(Some("dir/"), None, 2)` and `list_with_delimiter(Some("dir/"))` | Page 1 returns `["dir/a.txt", "dir/b.txt"]` + `Some("dir/b.txt")`; Page 2 returns `["dir/sub/c.txt"]` + `None`; `list_with_delimiter` returns objects `["dir/a.txt", "dir/b.txt"]` and prefixes `["dir/sub"]`. |
| Atomic rename & conflict | `rename("dir/a.txt", "dir/moved.txt", true)` then `rename("dir/b.txt", "dir/moved.txt", true)` | First moves object (`exists("dir/a.txt") == false`, `get("dir/moved.txt") == b"..."`); second returns `AlreadyExists` leaving `"dir/b.txt"` intact. |

## Verification

- Initial Red baseline: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage --test storage_engine_tests`
- Final Green + Clippy gate: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-storage --all-targets -- -D warnings`

## Acceptance Criteria

- [x] `StoredObjectMeta` and `PyObjectMeta` expose `content_type` and `attributes` (R1).
- [x] `ObjectStoreEngine`, `PySyncObjectStore`, and `PyAsyncObjectStore` support `get_stream`, `create_multipart` (`SyncMultipartUpload` / `AsyncMultipartUpload`), conditional `put`, `list_page`, `list_with_delimiter`, and `rename` without `Vec<Bytes>` full-payload buffering (R1).
- [x] All async PyO3 operations deliver completions via `RustTaskAwaitable` without `Python::attach` on Tokio worker threads (R1).

## Notes & Discoveries

- [2026-11-07 04:15] `r2.rs` currently documents R2's uniform non-trailing part size requirement; `MultipartUploadSession` enforces an 8 MiB part buffer incrementally for `r2` and `oci`.
- [2026-11-07 16:12] Downstream `ch01:t02` contract notes (`beb6a972`): `AsyncObjectStore.create_multipart(path, *, content_type=None, attributes=None, if_not_exists=False, if_match=None, if_none_match=None)` returns a `RustTaskAwaitable` resolving to `AsyncMultipartUpload`, whereas `AsyncObjectStore.get_stream(path, *, chunk_size=None)` returns `AsyncStreamReader` directly (opening lazily on the first `__anext__` / `next_chunk` Tokio task). `SyncStreamReader` exposes `__iter__`, `__next__`, `next_chunk`, and `close`; `AsyncStreamReader` exposes `__aiter__`, `__anext__`, `next_chunk`, and `aclose`. `AlreadyExists` maps to `FileExistsError`, `Precondition` maps to `PermissionError`, and `head` populates `content_type` and `attributes` via `get_opts(GetOptions { head: true, .. })`.

[^store-engine]: `src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine` — Native Rust storage engine.
[^r2-multipart]: `src/rs/vertebra-storage/src/r2.rs::build_r2_store` — R2 multipart upload constraints.
[^pyo3-storage-lib]: `src/rs/vertebra-storage/src/lib.rs::PyAsyncObjectStore` — PyO3 storage extension bindings.
