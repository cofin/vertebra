---
type: Task
id: native-postgres-driver:t66
title: Serialize complete Arrow IPC messages into SHM
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t65
files:
- src/rs/vertebra-postgres/src/arrow.rs
- src/rs/vertebra-postgres/tests/postgres_arrow_tests.rs
- src/py/tests/unit/db/test_postgres_native_data.py
- src/py/vertebra/db/arrow.py
- src/py/vertebra/db/adapters/postgres/types.py
tests:
- src/rs/vertebra-postgres/tests/postgres_arrow_tests.rs::ipc_is_one_valid_stream
- src/rs/vertebra-postgres/tests/postgres_arrow_tests.rs::ipc_capsule_retains_views
- src/py/tests/unit/db/test_postgres_native_data.py::test_arrow_count_metadata
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

# Serialize complete Arrow IPC messages into SHM

## Objective

Serialize complete Arrow IPC messages into SHM, with observable value, wire and ownership assertions through the named tests.

## Context

Read `../spec.md`, dependencies, protocol research, `.agents/skills/runtime-ipc/SKILL.md`, `.agents/skills/pyo3-bridge/SKILL.md`, database driver/CDC knowledge and ADR-004. One worksheet produces one signed conventional local commit on the complete driver's shared branch and PR. No implementation starts with `plan_commit: null`. Preserve other changes; listed files are owned only after predecessors complete.

Estimated handwritten changes including tests: 420 lines. If the work cannot fit roughly400 lines, use `/flow:revise native-postgres-driver` to split its boundary while preserving complete one-PR scope. Do not move implementation into acceptance tasks.

## Steps

1. Decode real IPC after zero/multiple batches, unaligned values and forced SHM growth; assert schema/values/count/one EOS. Test threshold0/exact-boundary/above-boundary, inline bytes and promotion. Rust integration test invokes the production Arrow output converter into independently loaded core .so and retains a memoryview/PyArrow table after cursor release; values remain readable and producer mapping eventually drops. Python public generic cursor tests cover empty/inline/SHM count and conflicting/negative count metadata.
2. `PostgresArrowIpcWriter` owns ONE `StreamWriter<Vec<u8>>`, with storage mode Adaptive(threshold_rows) or AlwaysShm(stream batches). Complete aligned schema and dictionary+batch groups drain via get_mut, retaining dictionary tracker. Adaptive keeps IPC inline while total rows<=threshold; when exceeded promote already encoded complete groups to ShmArrowStreamWriter then append new groups. Validate total IPC cap(default256MiB) before allocation. Never feed arbitrary Write fragments because every SHM append pads8bytes. Finish once; inline retains its single EOS, SHM discards StreamWriter EOS and finish_unregistered supplies sole EOS/header.
3. Internal owned ArrowOutput is Inline(Vec<u8>) or Shm(name,len,row_count,Arc<ShmRegion>). Loop converter constructs adapter-owned frozen `PostgresArrowResult(buffer:bytes|core.ArrowCursor,row_count:int,columns:tuple[str,...],used_shm:bool)` in types.py; SHM uses construct_py_arrow_cursor C ABI producer callbacks. No private registry or cross-.so Arc/Box reconstruction; conversion/drop failure cleans producer mapping. Native client export follows t69, so this task tests converter from Rust instead of exposing a test-only Python factory.
4. Generic db.arrow.ArrowCursor gains keyword-only row_count:int|None=None and _row_count slot. Validate nonnegative integer and agreement with nonempty provided rows or known native SHM count; explicit0 versus nonzero native count conflicts. __len__/num_rows use explicit count without decoding rows. Keep generic module engine-agnostic with no PostgreSQL imports. Other constructor callers remain compatible. Test inline buffer count using valid IPC, not invented count; existing row/native-backed counts remain authoritative.
5. Commit `feat(postgres): preserve adaptive Arrow IPC output ownership`.

## Verification

Write the named failing tests first. Initial RED is the absent named public method/module or wrong value, bytes, state or cleanup explicitly described above; record the actual failing assertion. Then run:

```bash
make dev
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_arrow_tests ipc_
uv run --no-sync python -m mypy.stubtest vertebra._postgres
uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_native_data.py -k arrow_count -q
```

Require final exit0 and record actual command, exit and observable assertion evidence. These are planned commands, not passing evidence. Native rebuild precedes Python tests. No external writes, tags, push or hosted dispatch are authorized.

## Acceptance Criteria

- [ ] R20: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] Every Steps contract and named negative/lifetime assertion passes.
- [ ] Buffer/queue/lease limits hold; no Python row/dict roundtrip or duplicated standard backend parser enters the native data path.
- [ ] Only owned work enters the atomic local commit; task state and parent checklist synchronize after verification.

## Notes & Discoveries

None.
