---
type: Task
id: "storage-remediation:ch03:t01"
chapter_id: ch03-bulk-file-io-compose-and-streams
title: Implement Rust streaming put_file and get_file, bounded-concurrency get_files and put_files, conditional copy, and server-side GCS compose
description: Stream put_file (> 8 MiB multipart) and atomic .part get_file, add buffer_unordered get_files and put_files, add conditional copy(..., if_not_exists), and implement native GCS JSON /compose (<= 32-way tree fold with temp cleanup) plus multipart fallback in store.rs.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - src/rs/vertebra-storage/src/store.rs
  - src/rs/vertebra-storage/src/lib.rs
  - src/rs/vertebra-storage/tests/storage_engine_tests.rs
files:
  - src/rs/vertebra-storage/src/store.rs
  - src/rs/vertebra-storage/src/lib.rs
tests:
  - src/rs/vertebra-storage/tests/storage_engine_tests.rs
estimated_loc: 245
plan_revision: 1
plan_commit: fd571e3a
state_revision: 2
claimed_by: executor
claimed_at: "2026-11-09T00:23:00Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-09T00:33:00Z"
last_verified_commit: f8bf065c
verification_evidence: "cargo test -p vertebra-storage (3 unit + 33 integration passed), clippy -D warnings, rustfmt --check, review-check clean"
created_at: "2026-11-08T20:55:00Z"
updated_at: "2026-11-09T00:33:00Z"
commit: f8bf065c
generated:
  by: flow/0.28.0
  at: "2026-11-08T20:55:00Z"
verified:
  - by: executor
    at: "2026-11-09T00:33:00Z"
    commit: f8bf065c
sources:
  - id: storage-store-rs
    resource: src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine
    title: ObjectStoreEngine file I/O, copy, and GCS compose implementation
  - id: storage-lib-rs
    resource: src/rs/vertebra-storage/src/lib.rs::PySyncObjectStore
    title: Existing copy call sites in lib.rs
tags:
  - storage
  - gcs
  - bulk-io
  - compose
  - tdd
---

# Task t01: Rust Streaming File I/O, Batch Transfers, Conditional `copy`, and Server-Side GCS `compose`

## Objective & Scope Boundary

Implement five core engine capabilities in `src/rs/vertebra-storage/src/store.rs` [^storage-store-rs] (updating existing `engine.copy(src, dst, false)` call sites in `lib.rs` [^storage-lib-rs] so the workspace compiles cleanly):
1. `put_file(&self, path: &str, file_path: &str) -> Result<StoredObjectMeta, StorageError>`: inspect `tokio::fs::metadata(file_path)`; for `<= 8 MiB` (`8 * 1024 * 1024`), read and call `self.put`; for `> 8 MiB`, stream `8 MiB` chunks via `tokio::io::AsyncReadExt` into `create_multipart` -> `write_part` -> `complete_with_meta`, calling `abort().await` on any read or upload error.
2. `get_file(&self, path: &str, file_path: &str) -> Result<StoredObjectMeta, StorageError>`: validate `path`, open `self.inner.get(&location).await?`, build `StoredObjectMeta`, create parent directories of `file_path`, stream `GetResult::into_stream()` chunks to a sibling `.{filename}.{pid}.{seq}.part` file via `BufWriter`, `flush()`, and `tokio::fs::rename` to `file_path`, unlinking the `.part` file on error.
3. `get_files` and `put_files(&self, pairs: Vec<(String, String)>, concurrency: usize) -> Result<Vec<StoredObjectMeta>, StorageError>`: validate `concurrency >= 1` (`InvalidInput`), run `futures::stream::iter(pairs.into_iter().enumerate()).map(...).buffer_unordered(concurrency)`, restore input order, and on `get_files` error unlink any destination files created during that batch.
4. `copy(&self, src: &str, dst: &str, if_not_exists: bool) -> Result<(), StorageError>`: when `if_not_exists` is `true`, invoke `self.inner.copy_if_not_exists(&from, &to).await` (with `file://` `NotImplemented` fallback using `tokio::fs::hard_link` / `OpenOptions::new().write(true).create_new(true)` mapping `AlreadyExists` -> `StorageError::AlreadyExists`).
5. `compose(&self, sources: Vec<String>, dst: &str, content_type: Option<String>) -> Result<StoredObjectMeta, StorageError>`: reject empty `sources` (`InvalidInput`), validate all source and destination keys. On `gs://` (`GcsComposeContext`), call GCS JSON `POST {base}/storage/v1/b/{bucket}/o/{encoded_dst}/compose` (`sourceObjects: [{"name": ...}]`, `destination: {"contentType": ...}`) using a pooled `reqwest::Client` (`Bearer` token from `credential_cache` / `access_token` unless `skip_signature`, plus `x-goog-user-project`), folding `> 32` sources in layers via `{dst}.compose-{idx:04d}` and deleting every temporary object in cleanup even when a fold fails; on non-`gs` stores, stream each source's `get_stream` chunks sequentially into `create_multipart` -> `complete_with_meta` (aborting on error).

Out of scope: New PyO3 methods and `_storage.pyi` stubs (`t02`).

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/store.rs` — `ObjectStoreEngine` and `BackendConfig::Gcs` [^storage-store-rs].
- `src/rs/vertebra-storage/src/lib.rs` — `engine.copy` call sites [^storage-lib-rs].
- `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — Rust integration tests.

## Symbol & Signature Contracts

- `ObjectStoreEngine::get_file(&self, path: &str, file_path: &str) -> Result<StoredObjectMeta, StorageError>`
- `ObjectStoreEngine::get_files(&self, pairs: Vec<(String, String)>, concurrency: usize) -> Result<Vec<StoredObjectMeta>, StorageError>`
- `ObjectStoreEngine::put_files(&self, pairs: Vec<(String, String)>, concurrency: usize) -> Result<Vec<StoredObjectMeta>, StorageError>`
- `ObjectStoreEngine::copy(&self, src: &str, dst: &str, if_not_exists: bool) -> Result<(), StorageError>`
- `ObjectStoreEngine::compose(&self, sources: Vec<String>, dst: &str, content_type: Option<String>) -> Result<StoredObjectMeta, StorageError>`

## Edge Cases & Error Semantics

- `concurrency == 0` in `get_files` / `put_files` returns `StorageError::InvalidInput("concurrency must be >= 1".into())`.
- `compose` with `1` source copies/composes cleanly into `dst` with optional `content_type`.
- GCS `/compose` `401` invalidates `SharedGcpTokenCacheHandle` at the observed generation and retries once with a refreshed token.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-storage/tests/storage_engine_tests.rs`.
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Streaming `> 8 MiB` `put_file` & atomic `get_file` | `10 MiB` temp file uploaded via `put_file` and downloaded via `get_file` | Round-trips exact `10 MiB` payload with zero `.part` leftover files. |
| Bounded `get_files` / `put_files` + error cleanup | `8` pairs at `concurrency = 3`, plus batch where item `#6` is `NotFound` | Valid batch returns ordered metadata; failing `get_files` unlinks already-written files from that call. |
| Conditional `copy(..., true)` across `memory://` and `file://` | `copy("a", "b", true)` when `"b"` already exists | Returns `Err(StorageError::AlreadyExists { .. })` (`FileExistsError`). |
| GCS `compose` `70`-shard tree fold & error cleanup | Loopback GCS server handling `/compose` and `DELETE`; `70` shards + failure case on shard `#50` | `70`-shard compose produces exact concatenated bytes and deletes all `.compose-*` temporaries; error case also deletes all created `.compose-*` temporaries. |

## Verification

```bash
PATH="$PWD/.venv/bin:$PATH" PYO3_PYTHON="$PWD/.venv/bin/python" CARGO_TARGET_DIR=/tmp/vtb-cargo-target cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage
```

## Acceptance Criteria

- [x] [f8bf065c] `put_file`, `get_file`, `get_files`, `put_files`, `copy(..., if_not_exists)`, and `compose` (`gs://` `/compose` tree fold + multipart fallback) pass Rust integration tests (R3.1).

## Notes & Discoveries

- [2026-11-08 20:55] Percent-encode object names in `/storage/v1/b/{bucket}/o/{encoded_dst}/compose` with `crate::signing::rfc3986_encode(dst, true)` so `/` in `dst` is encoded as `%2F` per the GCS JSON API contract without adding new crate dependencies.
- [2026-11-09 00:33] `GcsComposeContext` carries the built store's `GcpCredentialProvider` (covering static tokens, service-account keys, and ADC) alongside the shared token cache, which is preferred so `401` invalidate-and-retry works. The `file://` `copy(..., if_not_exists=true)` fallback is a create-only put because `LocalFileSystem` already implements `copy_if_not_exists` via `hard_link`; the fallback serves only custom stores registered through `with_store(.., "file")`. `get_files`/`put_files` stop launching new items after the first failure and let in-flight items finish so no cancelled future leaves a `.part` file.

[^storage-store-rs]: `src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine` — `ObjectStoreEngine` file I/O, `copy`, and GCS `compose` implementation.
[^storage-lib-rs]: `src/rs/vertebra-storage/src/lib.rs::PySyncObjectStore` — Existing `copy` call sites in `lib.rs`.
