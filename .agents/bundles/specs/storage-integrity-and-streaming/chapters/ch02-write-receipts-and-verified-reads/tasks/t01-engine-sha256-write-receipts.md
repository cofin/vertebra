---
type: Task
id: "storage-integrity-and-streaming:ch02:t01"
chapter_id: ch02-write-receipts-and-verified-reads
title: Engine SHA-256 write receipts and expected-digest enforcement
description: Add checksum and expected_sha256 to PutConditionalOptions, a StoredObjectMeta.sha256 receipt, StorageError::Integrity, and hashing in put_opts, MultipartUploadSession, and put_file with abort-before-publish on mismatch.
status: draft
state: pending
priority: P1
verification_strategy: tdd
depends_on:
  - "storage-integrity-and-streaming:ch01:t01"
read_set:
  - src/rs/vertebra-storage/src/store.rs
  - src/rs/vertebra-storage/tests/storage_engine_tests.rs
  - src/rs/vertebra-storage/Cargo.toml
  - .agents/bundles/specs/storage-integrity-and-streaming/chapters/ch02-write-receipts-and-verified-reads/spec.md
files:
  - src/rs/vertebra-storage/src/store.rs
  - src/rs/vertebra-storage/tests/storage_engine_tests.rs
tests:
  - src/rs/vertebra-storage/tests/storage_engine_tests.rs::test_put_sha256_receipts
  - src/rs/vertebra-storage/tests/storage_engine_tests.rs::test_put_sha256_mismatch_preserves_prior_object
estimated_loc: 240
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
    resource: src/rs/vertebra-storage/src/store.rs::PutConditionalOptions
    title: PutConditionalOptions, StoredObjectMeta, StorageError, put_opts, put_file, create_multipart
  - id: session-rs
    resource: src/rs/vertebra-storage/src/store.rs::MultipartUploadSession
    title: write_chunk, complete_with_meta, abort
tags:
  - storage
  - integrity
---

# Task t01: Engine SHA-256 Write Receipts and Expected-Digest Enforcement

## Objective & Scope Boundary

In `store.rs` [^store-rs] [^session-rs]:

1. `PutConditionalOptions`: add `pub checksum: bool` and `pub expected_sha256: Option<String>` (Default stays derivable).
2. `StoredObjectMeta`: add `pub sha256: Option<String>`; `From<object_store::ObjectMeta>` sets `None`.
3. `StorageError::Integrity { path: String, expected: String, actual: String }` with message `integrity check failed for '{path}': expected sha256 {expected}, got {actual}`.
4. Private helper `fn validate_sha256_hex(value: &str) -> Result<(), StorageError>` on `ObjectStoreEngine` (associated fn): 64 chars of `[0-9a-f]`, else `InvalidInput("expected_sha256 must be 64 lowercase hex characters")`.
5. `ObjectStoreEngine::put_opts`: when `checksum || expected_sha256.is_some()`, compute `hex::encode(Sha256::digest(&data))` before building `PutPayload`; mismatch returns `Integrity` without any request; set the receipt on the returned meta.
6. `MultipartUploadSession`: new field `hasher: Option<Sha256>` and `expected_sha256: Option<String>`, populated by `create_multipart` from options. `write_chunk` calls `hasher.update(&chunk)` before buffering. `complete_with_meta` finalizes before the final `complete()`; on mismatch it calls `self.abort().await` and returns `Integrity`. Receipt stored on returned meta.
7. `put_file`: thread the same options through its multipart path (single-shot path hashes the file bytes it reads).

Out of scope: reads (t02), PyO3 (t03).

## Bounded Read Manifest

- `store.rs` — the symbols above, `UniformPartAccumulator`, `put`, `put_multipart` [^store-rs].
- `storage_engine_tests.rs` — memory/local engine construction and the mock S3 server added in ch01/t01.
- `Cargo.toml` — `sha2`, `hex` already declared.

## Symbol & Signature Contracts

- `use sha2::{Digest, Sha256};`
- Signatures of `put_opts`, `create_multipart`, `put_file`, `complete_with_meta` unchanged; behavior keyed off new option fields.
- Receipt format: lowercase hex, 64 chars.

## Edge Cases & Error Semantics

- Zero-byte payload: receipt `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.
- Mismatch on multipart with prior object at path: abort leaves prior bytes readable.
- Abort itself fails: return `Integrity` (primary error), abort error discarded only after `had_error` set so `Drop` does not double-abort.
- `checksum=false`, no expected digest: `hasher` is `None`; no hashing.

## Public-Seam Test Blueprint

- **Canonical gate**: `cargo test -p vertebra-storage sha256`.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Single receipt | `put_opts("k", b"hello", checksum=true)` | `sha256 == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"` |
| Multipart receipt | session over chunks `[vec![1u8; 9 MiB], vec![2u8; 1 KiB]]`, `checksum=true` on mock S3 | receipt equals digest of concatenation |
| Empty | `put_opts("e", b"", checksum=true)` | empty-string digest above |
| Mismatch single | prior `k=old`; `put_opts("k", b"new", expected_sha256=sha256(b"x"))` | `Integrity`; `get k == b"old"` |
| Mismatch multipart | prior `k=old` on memory and mock S3; session writes `b"new"` with wrong digest | `Integrity`; `get k == b"old"`; mock records `DELETE ?uploadId` |
| Malformed | `expected_sha256="ABC"` | `InvalidInput` |
| Default | no options | `sha256 == None` |

## Verification

- `cd src/rs && cargo test -p vertebra-storage sha256`
- `cd src/rs && cargo clippy -p vertebra-storage --all-targets -- -D warnings`

## Acceptance Criteria

- [ ] Receipts match SHA-256 for single, multipart, and file uploads (R3).
- [ ] Mismatched digests never publish and leave prior objects intact (R3).

## Notes & Discoveries

- [2026-11-12 01:30] `sha2` and `hex` are existing workspace dependencies used by OCI signing.

[^store-rs]: `src/rs/vertebra-storage/src/store.rs::PutConditionalOptions` — Options, meta, errors, writes.
[^session-rs]: `src/rs/vertebra-storage/src/store.rs::MultipartUploadSession` — Multipart session.
