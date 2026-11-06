---
type: Task
id: storage-completeness:t01
title: Implement streaming reads, multipart uploads, conditional puts, pagination, and rename in vertebra-storage
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on: []
files:
- src/rs/vertebra-storage/src/store.rs
- src/rs/vertebra-storage/src/r2.rs
- src/rs/vertebra-storage/src/lib.rs
- src/py/vertebra/_storage.pyi
- src/py/vertebra/storage/_store.py
tests:
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
- src/py/tests/unit/storage/test_multicloud_storage.py
- src/py/tests/unit/storage/test_storage_store.py
plan_revision: 1
plan_commit: "7657e8b7c2c96bdda6821aec70a2b603f438a127"
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: '2026-11-06T06:38:00Z'
updated_at: '2026-11-06T06:38:00Z'
commit: null
tags:
- storage
- rust
- streaming
requirements:
- R1
---

# Implement streaming reads, multipart uploads, conditional puts, pagination, and rename in vertebra-storage

## Objective

Complete `ObjectStoreEngine`, `SyncObjectStore`, and `AsyncObjectStore` (`src/rs/vertebra-storage` + `src/py/vertebra/storage/_store.py`) with zero-copy `get_stream` chunk iterators, byte-range reads, conditional `put` (`if_not_exists`, `if_match`, `if_none_match`), streaming multipart uploads without `Vec<Bytes>` whole-payload buffering, cursor-paginated `list` / `list_with_delimiter`, `rename`, and object attributes.

## Context

Read `../spec.md`, `.agents/bundles/knowledge/domains/storage/object-storage.md`, and `.agents/skills/pyo3-bridge/SKILL.md`. Never introduce `asyncio.to_thread` in `src/py/vertebra/storage/_store.py` and keep `vertebra._storage` loaded via `import_optional`.

## Steps

1. Write failing Rust and Python tests in `storage_engine_tests.rs`, `test_multicloud_storage.py`, and `test_storage_store.py` covering `get_stream` (`StorageBuffer` chunks), `get_range`, conditional `put(if_not_exists=True)` conflict errors, streaming multipart upload (`put_stream`), paginated `list_page` / `list_with_delimiter`, `rename`, and `content_type` / custom metadata round-trips across sync and async stores.
2. Implement streaming `get_stream`, `put_opts` (conditional `PutMode`), incremental `MultipartUpload` (`put_part` without buffering all parts in `Vec<Bytes>`), paginated listing, and `rename` in `src/rs/vertebra-storage/src/{store.rs,r2.rs,lib.rs}` and `_storage.pyi`.
3. Expose matching sync and async methods on `SyncObjectStore`, `AsyncObjectStore`, `SyncReadableFile`, and `AsyncReadableFile` in `src/py/vertebra/storage/_store.py`.
4. Run verification and commit `feat(storage): add streaming reads, multipart uploads, conditional puts, pagination, and rename`.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage
make dev
uv run --no-sync python -m mypy.stubtest vertebra._storage
uv run --no-sync pytest src/py/tests/unit/storage/test_multicloud_storage.py src/py/tests/unit/storage/test_storage_store.py -q
```

## Acceptance Criteria

- [ ] R1: `SyncObjectStore` and `AsyncObjectStore` expose full sync/async parity across `get_stream`, ranges, conditional `put`, streaming multipart uploads, paginated listing, `rename`, and metadata with zero `asyncio.to_thread` calls.

## Notes & Discoveries

None.
