---
type: Task
id: "storage-integrity-and-streaming:ch01:t01"
flow_id: storage-integrity-and-streaming
chapter_id: ch01-integrity-receipts-and-verified-reads
title: "Add Rust CRC32C, MD5, and SHA-256 Write Receipts and Verified Streaming Reads"
description: "Implement opt-in CRC32C/MD5/SHA-256 receipts, abort-before-publish digest checks, and get_verified/get_file_verified/get_stream_verified in vertebra-storage."
status: stable
state: completed
commit: 85849b32
last_verified_commit: 85849b32
last_verified_at: "2026-11-13T02:21:00Z"
verification_evidence: "cargo test -p vertebra-storage (47 passed) + clippy + stubtest + mypy + pytest (485 passed, 1 skipped) + review-check (exit 0)"
claimed_by: null
claimed_at: null
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: []
files:
  - src/rs/Cargo.lock
  - src/rs/vertebra-storage/Cargo.toml
  - src/rs/vertebra-storage/src/store.rs
tests:
  - src/rs/vertebra-storage/tests/storage_engine_tests.rs
read_set:
  - src/rs/vertebra-storage/Cargo.toml
  - src/rs/Cargo.lock
  - src/rs/vertebra-storage/src/store.rs
  - src/rs/vertebra-storage/tests/storage_engine_tests.rs
estimated_loc: 245
tags:
  - storage
  - integrity
  - rust
  - checksums
sources:
  - id: store-rs
    resource: src/rs/vertebra-storage/src/store.rs::PutConditionalOptions
    title: "PutConditionalOptions, StoredObjectMeta, StorageError, put_opts, put_file, get, get_file, and get_stream"
  - id: session-rs
    resource: src/rs/vertebra-storage/src/store.rs::MultipartUploadSession
    title: "MultipartUploadSession write_chunk, complete_with_meta, and abort"
---

# Task `t01`: Add Rust CRC32C, MD5, and SHA-256 Write Receipts and Verified Streaming Reads

## Objective & Scope Boundary

Add `md-5 = "0.10"` (already present in `src/rs/Cargo.lock`) to `vertebra-storage/Cargo.toml` alongside `sha2` and a pure-Rust Castagnoli CRC32C accumulator, and implement write receipts, expected-digest enforcement, and verified reads in `src/rs/vertebra-storage/src/store.rs` per [`../spec.md` §2](../spec.md) [^store-rs] [^session-rs]. Out of scope: PyO3 bindings (`t02`) and Python facades (`t03`).

## Bounded Read Manifest

- `src/rs/vertebra-storage/Cargo.toml` and `src/rs/Cargo.lock` — locked `md-5 = "0.10"` and `sha2` versions.
- `src/rs/vertebra-storage/src/store.rs` — `PutConditionalOptions`, `StoredObjectMeta`, `MultipartUploadSession`, `PartFileGuard`, `put_opts`, `put_file`, `get`, `get_file`, `get_stream` [^store-rs] [^session-rs].
- `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — memory/local and loopback mock-S3 harnesses.

## Contracts & Edge Cases

- Validate `expected_sha256` (`64` lowercase hex), `expected_crc32c` (`8` lowercase hex), and `expected_md5` (`32` lowercase hex) before network/file I/O, returning `StorageError::InvalidInput`.
- When `checksum=false` and all `expected_*` are `None`, allocate zero hashers (`sha256`, `crc32c`, `md5` default to `None`). When `checksum=true`, populate all three receipts (`sha256`, `crc32c`, `md5`) on `StoredObjectMeta`.
- Single-shot `put_opts` checks digests before sending `PutPayload`; `MultipartUploadSession` updates hashers in `write_chunk` and checks in `complete_with_meta` before `complete()`, calling `self.abort().await` (marking `had_error` so `Drop` never double-aborts) and returning `StorageError::Integrity` while leaving prior objects intact; `put_file` threads options across both single-shot and multipart branches.
- `get_verified`, `get_file_verified` (unlinking `.part` before rename on mismatch while preserving any pre-existing destination file), and `get_stream_verified` (`VerifyingStream` yielding `Err(StorageError::Integrity)` on the final poll; early consumer drop raises no error) enforce `expected_sha256`.

## Test Matrix

- Add integration tests in `src/rs/vertebra-storage/tests/storage_engine_tests.rs`: single-shot, empty-payload, file, and multi-chunk (`> 8 MiB` mock-S3) `checksum=true` `sha256`/`crc32c`/`md5` receipts; single-shot and multipart digest mismatches preserving prior object bytes and recording mock-S3 `DELETE ?uploadId`; malformed hex `InvalidInput`; `get_verified`, `get_file_verified` (preserving pre-existing target file), and `get_stream_verified` good/corrupted/early-drop cases.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage --locked && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-storage --all-targets -- -D warnings
```

## Acceptance Criteria

- [x] `ObjectStoreEngine` computes CRC32C/MD5/SHA-256 receipts, aborts mismatched uploads before publish, and verifies byte, file, and stream reads (`R1`, `R2`).
- [x] Zero regressions in `vertebra-storage` tests and Clippy.

[^store-rs]: `src/rs/vertebra-storage/src/store.rs::PutConditionalOptions` — `PutConditionalOptions`, `StoredObjectMeta`, `StorageError`, `put_opts`, `put_file`, `get`, `get_file`, and `get_stream`.
[^session-rs]: `src/rs/vertebra-storage/src/store.rs::MultipartUploadSession` — `MultipartUploadSession` `write_chunk`, `complete_with_meta`, and `abort`.
