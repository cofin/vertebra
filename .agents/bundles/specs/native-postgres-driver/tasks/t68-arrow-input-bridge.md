---
type: Task
id: native-postgres-driver:t68
title: Stream Arrow input buffers into native COPY
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t67
files:
- src/rs/vertebra-postgres/src/copy.rs
- src/rs/vertebra-postgres/src/sync_client.rs
- src/rs/vertebra-postgres/src/async_client.rs
- src/py/vertebra/_postgres.pyi
- src/py/vertebra/db/adapters/postgres/arrow.py
- src/py/tests/unit/db/test_postgres_native_data.py
tests:
- src/py/tests/unit/db/test_postgres_native_data.py::test_native_load_arrow_streams_without_rows
- src/py/tests/unit/db/test_postgres_native_data.py::test_native_load_arrow_producer_error_rolls_back
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
created_at: '2026-11-05T18:25:07Z'
updated_at: '2026-11-05T20:03:00Z'
commit: null
tags:
- postgres
- native
- data-events
requirements:
- R20
---

# Stream Arrow input buffers into native COPY

## Objective

Stream Arrow input buffers into native COPY, with observable value, wire and ownership assertions through the named tests.

## Context

Read `../spec.md`, dependencies, protocol research, `.agents/skills/runtime-ipc/SKILL.md`, `.agents/skills/pyo3-bridge/SKILL.md`, database driver/CDC knowledge and ADR-004. One worksheet produces one signed conventional local commit on the complete driver's shared branch and PR. No implementation starts with `plan_commit: null`. Preserve other changes; listed files are owned only after predecessors complete.

Estimated handwritten changes including tests: 410 lines. If the work cannot fit roughly400 lines, use `/flow:revise native-postgres-driver` to split its boundary while preserving complete one-PR scope. Do not move implementation into acceptance tasks.

## Steps

1. Test Table/RecordBatch/RecordBatchReader/public ArrowCursor input with to_pylist/to_pydict raising. Cover columns/schema-only empty input, source error after first batch, malformed/oversize IPC, server error/cancel. A second-batch barrier proves first batch transmits before remaining input is read.
2. Extend native COPY input handles with write_arrow_ipc(buffer). Extract contiguous read-only buffer while attached; bounded complete IPC stream -> t67 plan in Rust. Per-call cap16MiB/per-batch8MiB; reject oversize before allocation. Async owns bytes or buffer lease whose Python disposal happens on loop; worker has no Python callbacks or foreign producer release functions.
3. Add adapter-local arrow.py::iter_arrow_ipc_batches/aiter_arrow_ipc_batches. Table.to_reader(max_chunksize), wrapped RecordBatch reader, ArrowCursor IPC buffer_view and PyArrow RecordBatchReader.from_stream providers feed one bounded batch at a time. Serialize each to a self-contained stream through BufferOutputStream/new_stream, not Python rows/dicts or whole-table to_pybytes. Split until row/byte limits; single oversized row DataError. Provider C callbacks remain on caller thread; no native Arrow C-stream ownership across .so required.
4. One in-flight native write, finish/abort in finally, close input reader on every exit. Missing PyArrow fails before COPY side effects. Expose `SyncPostgresClient.copy_from_arrow(table_name,first_batch_ipc,*,columns=None)->SyncPostgresCopyIn` and awaited async counterpart. Resolve target metadata, build import plan and prevalidate first batch before starting quoted COPY, write that batch and retain plan in returned handle. Python helpers then write remaining batches and finish; no opaque Python ArrowSchema or global plan registry crosses the seam. Empty input supplies schema-only IPC, validates target, opens/finishes zero-row COPY and returns0. t89 cutover calls these helpers. Commit `feat(postgres): stream Arrow input buffers to COPY`.

## Verification

Write the named failing tests first. Initial RED is the absent named public method/module or wrong value, bytes, state or cleanup explicitly described above; record the actual failing assertion. Then run:

```bash
make dev
uv run --no-sync python -m mypy.stubtest vertebra._postgres
uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_native_data.py -k load_arrow -q
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_arrow_import_tests
```

Require final exit0 and record actual command, exit and observable assertion evidence. These are planned commands, not passing evidence. Native rebuild precedes Python tests. No external writes, tags, push or hosted dispatch are authorized.

## Acceptance Criteria

- [ ] R20: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] Every Steps contract and named negative/lifetime assertion passes.
- [ ] Buffer/queue/lease limits hold; no Python row/dict roundtrip or duplicated standard backend parser enters the native data path.
- [ ] Only owned work enters the atomic local commit; task state and parent checklist synchronize after verification.

## Notes & Discoveries

None.
