---
type: Task
id: "storage-integrity-and-streaming:ch01:t01"
chapter_id: ch01-create-only-copy-and-quality
title: Configure S3/B2 and R2 create-only copy with mock-S3 tests
description: Build S3/B2 stores with S3CopyIfNotExists::Multipart and R2 stores with the cf-copy-destination-if-none-match header strategy, normalize destination conflicts to StorageError::AlreadyExists, and prove conflict and success against loopback mock S3 servers.
status: stable
state: in_progress
priority: P1
verification_strategy: regression_tdd
depends_on: []
read_set:
  - src/rs/vertebra-storage/src/store.rs
  - src/rs/vertebra-storage/tests/storage_engine_tests.rs
  - src/rs/vertebra-storage/Cargo.toml
  - .agents/bundles/specs/storage-integrity-and-streaming/chapters/ch01-create-only-copy-and-quality/spec.md
files:
  - src/rs/vertebra-storage/src/store.rs
  - src/rs/vertebra-storage/tests/storage_engine_tests.rs
tests:
  - src/rs/vertebra-storage/tests/storage_engine_tests.rs::test_copy_if_not_exists_s3_compatible
estimated_loc: 220
plan_revision: 2
plan_commit: c368cb00
state_revision: 0
claimed_by: agent:bulk-storage
claimed_at: "2026-11-12T03:14:29Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-12T03:14:29Z"
last_verified_commit: null
verification_evidence: "Red: create-only S3 copy returns NotSupported; Green: focused copy_if_not_exists test passes for memory/file/s3/b2/r2, cargo storage suite 43 passed, clippy all-targets exits 0; focused storage extension rebuild succeeds."
created_at: "2026-11-12T01:30:00Z"
updated_at: "2026-11-12T01:30:00Z"
commit: null
sources:
  - id: store-rs
    resource: src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine
    title: BackendConfig::S3 and BackendConfig::R2 builder arms, copy, rename
  - id: engine-tests
    resource: src/rs/vertebra-storage/tests/storage_engine_tests.rs::test_copy_if_not_exists_s3_compatible
    title: Existing memory/local create-only test and TcpListener mock-server pattern
tags:
  - storage
  - s3
  - r2
---

# Task t01: Configure S3/B2 and R2 Create-Only Copy with Mock-S3 Tests

## Objective & Scope Boundary

In `store.rs::ObjectStoreEngine::new` [^store-rs]:

1. `BackendConfig::S3` arm: add `builder = builder.with_copy_if_not_exists(S3CopyIfNotExists::Multipart);` before `builder.build()`.
2. `BackendConfig::R2` arm: add `.with_copy_if_not_exists(S3CopyIfNotExists::HeaderWithStatus("cf-copy-destination-if-none-match".to_string(), "*".to_string(), reqwest::StatusCode::PRECONDITION_FAILED))`.
3. `ObjectStoreEngine::copy` and `::rename` with `if_not_exists=true`: map `object_store::Error::AlreadyExists { .. }` and `object_store::Error::Precondition { .. }` returned by `copy_if_not_exists` / `rename_if_not_exists` to `StorageError::AlreadyExists { path: dst.to_string(), message }`. Keep the `file` fallback. Any other error (including `NotSupported`) propagates unchanged.
4. Update the `copy` doc comment to list the S3/B2 and R2 strategies.

Replace `test_copy_if_not_exists_s3_compatible` with a table-driven `test_copy_if_not_exists_s3_compatible` covering memory, local, a mock S3 server, and a mock R2 server (rename covered in the same test).

Out of scope: Azure, GCS, OCI (native support unchanged); Python facade.

## Bounded Read Manifest

- `store.rs` — `BackendConfig`, `ObjectStoreEngine::new` S3/R2 arms, `copy`, `rename`, `StorageError` [^store-rs].
- `storage_engine_tests.rs` — `test_copy_if_not_exists_s3_compatible`, `test_incremental_s3_uniform_multipart_session`, `tokio::net::TcpListener` mock patterns [^engine-tests].
- `Cargo.toml` — `object_store 0.13.2` features (`aws`) and `reqwest`/`http` availability for `StatusCode`.

## Symbol & Signature Contracts

- `use object_store::aws::S3CopyIfNotExists;`
- `S3CopyIfNotExists::Multipart`: object_store issues `CreateMultipartUpload`, `UploadPartCopy`, then `CompleteMultipartUpload` with `If-None-Match: *`; `412` maps to `object_store::Error::AlreadyExists`.
- `S3CopyIfNotExists::HeaderWithStatus(k, v, 412)`: object_store sends `PUT` copy with header `k: v`; status `412` maps to `AlreadyExists`.
- Engine signatures unchanged: `copy(&self, src: &str, dst: &str, if_not_exists: bool) -> Result<(), StorageError>`, `rename(..) -> Result<(), ObjectStoreError>`.

## Edge Cases & Error Semantics

- Mock returns `412`: `StorageError::AlreadyExists`, destination bytes unchanged.
- Mock returns `501 NotImplemented` for the conditional header: error propagates (no silent overwrite).
- Rename success on S3: source deleted after create-only copy (object_store default `rename_if_not_exists`).
- Rename conflict: source still present.

## Public-Seam Test Blueprint

- **Canonical gate**: `cargo test -p vertebra-storage copy_if_not_exists`.
- Mock S3 server: `TcpListener` on `127.0.0.1:0` holding an in-memory `HashMap<String, Vec<u8>>`; handles `PUT /bucket/key` (store), `GET`, `HEAD`, `DELETE`, `POST ?uploads` (return `UploadId`), `PUT ?partNumber&uploadId` with `x-amz-copy-source` (return `CopyPartResult`), `POST ?uploadId` (return `412` when `If-None-Match: *` and the key exists, else store and return `CompleteMultipartUploadResult`), and R2 `PUT` with `x-amz-copy-source` plus `cf-copy-destination-if-none-match: *` (return `412` when the key exists). Engines built with `BackendConfig::S3 { endpoint: Some(mock), allow_http: true, .. }` and `BackendConfig::R2 { endpoint: Some(mock), allow_http: true, .. }`.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Conflict copy | `put a=alpha`, `put b=bravo`, `copy("a","b",true)` | `StorageError::AlreadyExists`; `get b == b"bravo"` |
| Success copy | `copy("a","c",true)` | `Ok`; `get c == b"alpha"` |
| Conflict rename | `rename("a","b",true)` | `AlreadyExists`; `exists a == true` |
| Success rename | `rename("a","d",true)` | `Ok`; `exists a == false`; `get d == b"alpha"` |
| Header sent (R2) | inspect recorded request | contains `cf-copy-destination-if-none-match: *` |

Each row runs for `memory`, `file`, `s3`, `r2` engines.

## Verification

- `cd src/rs && cargo test -p vertebra-storage copy_if_not_exists`
- `cd src/rs && cargo clippy -p vertebra-storage --all-targets -- -D warnings`

## Acceptance Criteria

- [x] S3/B2 and R2 engines perform create-only copy and rename with `FileExistsError` semantics (R1).
- [x] Mock-server tests cover conflict and success on all four schemes (R1).

## Notes & Discoveries

- [2026-11-12 01:30] Cloudflare R2 documents `cf-copy-destination-if-none-match` for conditional `CopyObject`; object_store documents `HeaderWithStatus` for this case.

[^store-rs]: `src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine` — Builders, copy, rename.
[^engine-tests]: `src/rs/vertebra-storage/tests/storage_engine_tests.rs::test_copy_if_not_exists_s3_compatible` — Existing test and mock patterns.
