---
type: Task
id: native-postgres-driver:t89
title: Complete native Arrow forwarding before public PostgreSQL activation
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t88
files:
- src/py/vertebra/db/adapters/postgres/_sync.py
- src/py/vertebra/db/adapters/postgres/_async.py
- src/py/tests/unit/db/test_postgres_native_adapter_arrow.py
tests:
- src/py/tests/unit/db/test_postgres_native_adapter_arrow.py
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
- cutover
- native
requirements:
- R20
- R24
---

# Complete native Arrow forwarding before public PostgreSQL activation

## Objective

Validate and complete native Arrow/COPY forwarding on the permanent implementations without changing helpers used by the public legacy drivers.

## Context

Read `../spec.md`, completed native contracts and `.agents/skills/python-framework/SKILL.md`, `.agents/skills/pyo3-bridge/SKILL.md`, database-driver knowledge and Python module layout. The permanent native implementations are `_sync.py::SyncPostgreSQLDriver` and `_async.py::AsyncPostgreSQLDriver`; `driver.py` is the canonical public export module after atomic t91 activation. Before t91 the unchanged public legacy pair remains coherent. New implementation tests import the final defining modules until t91 switches them to canonical public imports and identity assertions. No temporary implementation module, class alias, compatibility shim or second native engine is introduced.

One executor invocation produces one signed conventional local commit on the complete driver's feature branch; all work ships in one complete PR. Ownership is the listed files/symbols, shared files change sequentially and unrelated edits remain intact. No implementation starts with null plan_commit. Commands are planned, not evidence. Record actual public assertions and exits; rebuild native code with make dev before Python tests. If measured scope exceeds the stated near400-line estimate, revise the task boundary before implementation rather than leave a broken intermediate tree. No hosted write/push/tag action is authorized.

Estimated implementation plus tests/removals: 270–370; forwards exist from t85/t86, edge handling and native lifetime tests are owned here changed lines.

## Steps

1. Import the final implementation classes directly in new native Arrow tests, using shared native ports and completed actual native-data fixtures. Verify PostgresArrowResult inline/SHM threshold boundary, empty columns/row_count, explicit wrapper row_count, max_result_bytes default256MiB, negative limits preflight and oversize row/value8MiB errors. Forbid execute_with_description/from_rows/to_pylist fallback in native paths. Verify every returned public db.arrow.ArrowCursor retains the actual buffer/capsule owner.
2. Complete thin forwarder edge handling in `_sync.py`/`_async.py`: native select_to_arrow result wrapping, native Arrow stream->documented batch projection with pinned lease and early close/cancellation, and iter/aiter_arrow_ipc_batches plus copy_from_arrow(firstIPC) followed by remaining writes/finish/abort. Reuse arrow.py helpers already implemented; no new bulk engine, row INSERT loop or Python-row materialization. Schema-only empty import validates target and returns0. Caller column/schema options and native counter errors remain observable.
3. Add cancellation/source-error/second-batch barriers proving one in-flight native write, prompt portal/stream closure and balanced real lease ownership. Preserve original exception during failed abort/reset. Run CPU pgcopy/JSONB/vector-codec tests unchanged; their public helpers remain useful independently of socket client activation.
4. Do not edit embeddings.py or notify.py SQL markers or replace the existing psycopg-converter regression here. Their public driver still uses legacy psycopg until t91; sync helper numeric marker changes and corresponding test-port migration occur in the same t91 atomic activation. Commit test(postgres): complete native Arrow forwarding contracts.

## Verification

Write named behavioral RED cases first, then implement and require passing assertions.

```bash
make dev
uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_adapter_arrow.py src/py/tests/unit/db/test_postgres_native_data.py src/py/tests/unit/db/test_vector_wire_codecs.py -q
uv run --no-sync pytest src/py/tests/unit/db/test_postgres_driver.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py -q
uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres/_sync.py src/py/vertebra/db/adapters/postgres/_async.py src/py/tests/unit/db/test_postgres_native_adapter_arrow.py
uv run --no-sync mypy src/py/vertebra/db/adapters/postgres
```

## Acceptance Criteria

- [ ] R20, R24: named behavior, lifetime and negative cases pass with the intended publication boundary.
- [ ] The current public legacy pair remains green before t91; after t91 every public path resolves only to the permanent native classes.
- [ ] One bounded signed local commit and task-first checklist/state/evidence synchronization complete this invocation.

## Notes & Discoveries

None.
