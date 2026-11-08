---
type: Task
id: storage-completeness:ch04:t04
chapter_id: ch04-arrow-shm-and-docs
title: Coalesce native S3 multipart chunks into valid bounded parts
description: Reuse the existing 8 MiB accumulator for native S3 uploads so incremental
  codec chunks complete successfully under S3 minimum-part rules, including B2 aliases
  normalized to S3.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
- storage-completeness:ch04:t03
read_set:
- src/rs/vertebra-storage/src/r2.rs
- src/rs/vertebra-storage/src/store.rs
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
- src/py/tests/unit/storage/test_storage_store.py
- src/py/vertebra/storage/_store.py
- src/py/vertebra/_storage.pyi
files:
- src/rs/vertebra-storage/src/r2.rs
- src/rs/vertebra-storage/src/store.rs
tests:
- src/rs/vertebra-storage/src/r2.rs
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
- src/py/tests/unit/storage/test_storage_store.py
estimated_loc: 80
plan_revision: 10
plan_commit: '78293774'
state_revision: 2
claimed_by: agent:s3-part-executor
claimed_at: '2026-11-08T17:23:21.915804Z'
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: '2026-11-08T17:39:10.695721Z'
last_verified_commit: 3f3958e3
verification_evidence:
  red:
    exit_code: 1
    result: 8 actual native S3 successful Complete cases reject non-final parts below5MiB
      with EntityTooSmall.
  green:
    rust_exit_code: 0
    rust_result: 3 unit+22 integration tests; S3/B2 normalization and S3/R2/OCI incremental8MiB
      demand pass.
    native_rebuild_exit_code: 0
    stubtest_exit_code: 0
    python_exit_code: 0
    python_result: 119 canonical store tests, including8 min-part-enforcing S3 codec
      Complete roundtrips.
    lint_typing_format_exit_code: 0
    lockfiles: uv.lock and Cargo.lock unchanged
  aggregate:
    rust_workspace_exit_code: 0
    python_suite_exit_code: 0
    python_result: 2563 passed,74 skipped,170 xfailed
    docs_exit_code: 0
created_at: '2026-11-08T17:23:21.915804Z'
updated_at: '2026-11-08T17:39:10.695721Z'
commit: 3f3958e3
sources:
- id: accumulator
  resource: src/rs/vertebra-storage/src/r2.rs::requires_uniform_part_size
  title: Existing bounded 8 MiB native accumulator policy
- id: session
  resource: src/rs/vertebra-storage/src/store.rs::MultipartUploadSession
  title: Native incremental multipart and normalized backend schemes
tags:
- storage
- arrow
- multipart
---

# Task t04: Coalesce native S3 multipart chunks into valid bounded parts

## Objective & Scope Boundary

Native S3 multipart uploads coalesce arbitrary codec and public put_stream chunks into bounded 8 MiB parts. The existing R2/OCI accumulator also serves S3; caller signatures, incremental producer demand, native abort, and output metadata remain stable. This repairs the R1/R8 successful cloud-publication contract rather than adding a new provider API [^accumulator] [^session].

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/r2.rs` — native policy, public consumer, or canonical test seam.
- `src/rs/vertebra-storage/src/store.rs` — native policy, public consumer, or canonical test seam.
- `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — native policy, public consumer, or canonical test seam.
- `src/py/tests/unit/storage/test_storage_store.py` — native policy, public consumer, or canonical test seam.
- `src/py/vertebra/storage/_store.py` — native policy, public consumer, or canonical test seam.
- `src/py/vertebra/_storage.pyi` — native policy, public consumer, or canonical test seam.

## Symbol & Signature Contracts

- Preserve `requires_uniform_part_size(scheme: &str) -> bool`, `R2_UNIFORM_PART_SIZE`, and `UniformPartAccumulator` APIs. Include normalized `s3` in the existing accumulator policy; retain r2/oci behavior and other backends.
- `ObjectStoreEngine::new(BackendConfig::S3)` records scheme `s3`; from_url accepts `s3` and `b2` and constructs BackendConfig::S3. Test actual alias normalization instead of inventing an independent B2 transport policy.
- MultipartUploadSession receives each producer chunk immediately, retains at most one unfinished 8 MiB part, submits complete parts through actual MultipartUpload.put_part, then flushes the final short part on complete. Preserve abort and PutResult.
- Update adjacent native documentation to describe S3 minimum-part compliance and R2/OCI uniformity. Do not change public Python codec output or accumulate a whole encoded object.

## Edge Cases & Error Semantics

S3 requires every non-final part to be at least 5 MiB; the last part is exempt. Small schema/codec output therefore completes as one final part. Large output forms 8 MiB non-final parts plus a final remainder. Failures and cancellation retain native abort semantics. B2 URL aliases normalize to the same S3 backend and policy.

## Public-Seam Test Blueprint

- Red: extend the real canonical S3 HTTP fixture to accept initiation and part uploads but reject CompleteMultipartUpload when any non-final captured body is below 5 MiB. Exercise actual public write_arrow/write_parquet sync/async with multiple small codec chunks; the current native policy must fail completion.
- Green: native 8 MiB coalescing makes those successful outputs complete. Join captured completed parts and decode with installed PyArrow; assert exact schema/values and actual native PutResult. Include a dataset crossing 8 MiB and verify every non-final captured part meets the minimum; one final short part remains valid.
- Retain existing real PUT cancellation/DELETE abort and native multipart completion ordering tests. Verify S3/B2 normalized policy and R2/OCI behavior with native unit/integration cases. No mocked System Under Test or permissive success server.

## Verification

Red: `UV_NO_SYNC=1 uv run --no-sync pytest src/py/tests/unit/storage/test_storage_store.py -k s3 -q` against the existing built extension.

Green: locked `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage`; all-target storage Clippy; all-five `UV_NO_SYNC=1 make dev` with project Python, repository target and root-filesystem TMPDIR; five-extension stubtest; canonical storage pytest; focused Ruff/format/diff. Use unique owned test artifacts, never broad SHM cleanup. Set `PYO3_PYTHON="$PWD/.venv/bin/python"` and `PYTHONPATH="$PWD/src/py:$PWD/.venv/lib/python3.12/site-packages"` for embedded Python Rust tests so the project interpreter resolves both source and installed dependencies.

## Acceptance Criteria

- [x] R1/R8 actual S3 successful multipart completion enforces 5 MiB non-final parts for public codec writers and arbitrary native producer chunks.
- [x] Bounded 8 MiB accumulation, B2 normalization, native metadata, incremental demand, and cancellation/abort remain preserved.

## Notes & Discoveries

The native session already coalesces R2/OCI while S3 directly forwards each raw codec chunk. Successful S3 Complete enforcement supplies the missing provider contract evidence. [AWS multipart upload limits](https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html) require a 5 MiB minimum for each non-final part.

[^accumulator]: `src/rs/vertebra-storage/src/r2.rs::requires_uniform_part_size` — Existing bounded accumulator policy.
[^session]: `src/rs/vertebra-storage/src/store.rs::MultipartUploadSession` — Native upload lifetime and S3/B2 normalization.
