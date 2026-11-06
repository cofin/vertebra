---
type: Task
id: storage-completeness:t07
title: Stream Arrow/Parquet across ObjectStore, emit valid IPC in ArrowCursor.from_rows, and enforce strict SHM allocation
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- storage-completeness:t05
- storage-completeness:t06
files:
- src/rs/vertebra-db/src/arrow_shm.rs
- src/py/vertebra/db/arrow.py
- src/py/vertebra/extensions/sqlspec/storage.py
- src/py/vertebra/storage/_store.py
- docs/usage/storage.md
- .agents/bundles/knowledge/domains/storage/object-storage.md
tests:
- src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
- src/py/tests/unit/extensions/test_data_storage_integrations.py
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
- arrow
- shm
- docs
requirements:
- R8
---

# Stream Arrow/Parquet across ObjectStore, emit valid IPC in ArrowCursor.from_rows, and enforce strict SHM allocation

## Objective

Implement batch-streaming Arrow/Parquet export and import helpers between `ArrowCursor` and `SyncObjectStore` / `AsyncObjectStore`, ensure `ArrowCursor.from_rows` always writes valid Arrow IPC streams (never JSON pseudo-Arrow), make `allocate_shm_region` return `Err(ArrowShmError)` when `ShmRegion::create_named` fails without falling back to `ShmRegion::create`, and update storage docs and OKF chapters.

## Context

Read `../spec.md`, `.agents/bundles/knowledge/decisions/adr-002-two-tier-shm-database-and-arrow-data-plane.md`, and `.agents/skills/runtime-ipc/SKILL.md`.

## Steps

1. Write failing Rust and Python tests in `arrow_shm.rs`, `test_pg_dataplane_and_datafusion.py`, `test_data_storage_integrations.py`, and `test_storage_store.py` verifying that `allocate_shm_region` returns `Err(ArrowShmError)` (`Result<ShmRegion, ArrowShmError>`) when `ShmRegion::create_named` fails without falling back to `ShmRegion::create`, `ArrowCursor.from_rows` produces valid Arrow IPC readable by `pyarrow.ipc.open_stream`, and `SyncObjectStore` / `AsyncObjectStore` + `ObjectStoreBackend._iter_arrow_async` stream multi-batch Arrow/Parquet payloads incrementally.
2. Remove the `ShmRegion::create` fallback from `src/rs/vertebra-db/src/arrow_shm.rs::allocate_shm_region`, update `src/py/vertebra/db/arrow.py::ArrowCursor.from_rows` and `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend._iter_arrow_async` batch streaming, and wire `write_arrow` / `read_arrow` / `write_parquet` / `read_parquet` streaming helpers on `SyncObjectStore` and `AsyncObjectStore`.
3. Update `docs/usage/storage.md` and `.agents/bundles/knowledge/domains/storage/object-storage.md` (`sources:` and `src/rs/vertebra-core/src/notify.rs` link) to document unified GCP token caches, fail-closed store construction, framework storage adapters, and Arrow/Parquet streaming.
4. Run full flow verification (`make lint-all`, `make rs-test`, `make py-test`, `make docs`) and commit `feat(storage,db): add streaming Arrow/Parquet I/O, strict IPC ArrowCursor.from_rows, and fail-closed SHM allocation`.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db -p vertebra-storage
make dev
uv run --no-sync pytest src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/storage src/py/tests/unit/extensions/test_data_storage_integrations.py -q
make lint-all
make docs
```

## Acceptance Criteria

- [ ] R8: `allocate_shm_region` returns `Err(ArrowShmError)` without `ShmRegion::create` fallback, `ArrowCursor.from_rows` emits valid Arrow IPC, streaming Arrow/Parquet storage helpers pass sync/async suites, and `make lint-all`, `make rs-test`, `make py-test`, and `make docs` exit 0.

## Notes & Discoveries

None.
