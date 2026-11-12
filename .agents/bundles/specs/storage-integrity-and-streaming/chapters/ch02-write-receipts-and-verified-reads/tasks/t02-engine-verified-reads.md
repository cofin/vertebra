---
type: Task
id: "storage-integrity-and-streaming:ch02:t02"
chapter_id: ch02-write-receipts-and-verified-reads
title: Engine verified reads
description: Add get_verified, get_file_verified, and get_stream_verified to ObjectStoreEngine so corrupted reads raise before bytes are returned or files are published.
status: draft
state: pending
priority: P1
verification_strategy: tdd
depends_on:
  - "storage-integrity-and-streaming:ch02:t01"
read_set:
  - src/rs/vertebra-storage/src/store.rs
  - src/rs/vertebra-storage/tests/storage_engine_tests.rs
  - .agents/bundles/specs/storage-integrity-and-streaming/chapters/ch02-write-receipts-and-verified-reads/spec.md
files:
  - src/rs/vertebra-storage/src/store.rs
  - src/rs/vertebra-storage/tests/storage_engine_tests.rs
tests:
  - src/rs/vertebra-storage/tests/storage_engine_tests.rs::test_get_verified
estimated_loc: 170
plan_revision: 1
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T01:30:00Z"
updated_at: "2026-11-12T01:30:00Z"
commit: null
sources:
  - id: store-rs
    resource: src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine
    title: get, get_file, get_stream
tags:
  - storage
  - integrity
---

# Task t02: Engine Verified Reads

## Objective & Scope Boundary

1. `ObjectStoreEngine::get_verified(&self, path: &str, expected_sha256: &str) -> Result<Bytes, StorageError>`: validate digest, fetch via `get`, hash, return `Integrity` on mismatch [^store-rs].
2. `get_file_verified(&self, path: &str, dest: &str, expected_sha256: &str) -> Result<StoredObjectMeta, StorageError>`: same streaming as `get_file`, hashing each chunk while writing the `.part` file; on mismatch delete the `.part` file and return `Integrity` before the rename. Implement by adding a private `expected: Option<&str>` parameter to the shared `get_file` body.
3. `get_stream_verified(&self, path: &str, expected_sha256: &str) -> Result<BoxStream<'static, Result<Bytes, StorageError>>, StorageError>`: wraps `get_stream` in a private `VerifyingStream` that updates a hasher per chunk and, at end of inner stream, yields `Err(Integrity)` instead of `None` on mismatch.
Tests append `test_get_verified` to `storage_engine_tests.rs`. Out of scope: the checksum benchmark (ch03/t02), PyO3 (t03).

## Bounded Read Manifest

- `store.rs` — `get`, `get_file`, `get_stream`, `PartFileGuard` [^store-rs].
- `storage_engine_tests.rs` — engine construction helpers; chapter spec edge-case matrix.

## Symbol & Signature Contracts

- Signatures listed above; existing `get`, `get_file`, `get_stream` unchanged.
- `struct VerifyingStream { inner: BoxStream<'static, Result<Bytes, StorageError>>, hasher: Option<Sha256>, expected: String, path: String }` implementing `Stream`.

## Edge Cases & Error Semantics

- Malformed digest: `InvalidInput` before any request.
- Missing object: existing `NotFound` mapping.
- Mismatch in `get_file_verified`: destination file absent afterwards; pre-existing destination file untouched.
- Stream consumer stops early: no verification error (never reached end).

## Public-Seam Test Blueprint

- **Canonical gate**: `cargo test -p vertebra-storage verified`.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Good read | `put k=b"hello"`; `get_verified("k", sha256(b"hello"))` | `b"hello"` |
| Bad read | `get_verified("k", sha256(b"x"))` | `Integrity` |
| File mismatch | `get_file_verified("k", tmp/"out", wrong)` | `Integrity`; `tmp/out` and `tmp/out.part` absent |
| File pre-existing dest | `tmp/out` contains `b"keep"`; mismatch | `tmp/out == b"keep"` |
| Stream mismatch | collect `get_stream_verified("k", wrong)` | last item `Err(Integrity)` |
| Stream good | collect with correct digest | concatenation `b"hello"`, no error |

## Verification

- `cd src/rs && cargo test -p vertebra-storage verified`
- `cd src/rs && cargo clippy -p vertebra-storage --all-targets -- -D warnings`

## Acceptance Criteria

- [ ] Corrupted reads raise before bytes are returned or files are published (R4).

## Notes & Discoveries

- [2026-11-12 01:30] `get_file` already writes a `.part` sibling and renames it into place, so verification slots in before the rename.

[^store-rs]: `src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine` — Read paths.
