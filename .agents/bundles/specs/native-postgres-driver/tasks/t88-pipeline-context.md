---
type: Task
id: "native-postgres-driver:t88"
title: "Bind permanent native driver pipeline factories to transaction ownership"
description: "Bind permanent native driver pipeline factories to transaction ownership for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:t87"
files:
  - src/py/vertebra/db/adapters/postgres/_sync.py
  - src/py/vertebra/db/adapters/postgres/_async.py
  - src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py
tests:
  - src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py
plan_revision: 2
plan_commit: cdb978d3
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-07T04:15:00Z"
commit: null
tags:
  - postgres
  - cutover
  - native
---

# Bind permanent native driver pipeline factories to transaction ownership

## Objective

Complete pipeline factories on the permanent native classes while public legacy exports remain unchanged.

## Context

Read `../spec.md`, completed native contracts and `.agents/skills/python-framework/SKILL.md`, `.agents/skills/pyo3-bridge/SKILL.md`, database-driver knowledge and Python module layout. The permanent native implementations are `_sync.py::SyncPostgreSQLDriver` and `_async.py::AsyncPostgreSQLDriver`; `driver.py` is the canonical public export module after atomic t91 activation. Before t91 the unchanged public legacy pair remains coherent. New implementation tests import the final defining modules until t91 switches them to canonical public imports and identity assertions. No temporary implementation module, class alias, compatibility shim or second native engine is introduced.

One executor invocation produces one signed conventional local commit on the complete driver's feature branch; all work ships in one complete PR. Ownership is the listed files/symbols, shared files change sequentially and unrelated edits remain intact. No implementation starts with null plan_commit. Commands are planned, not evidence. Record actual public assertions and exits; rebuild native code with make dev before Python tests. If measured scope exceeds the stated near400-line estimate, revise the task boundary before implementation rather than leave a broken intermediate tree. No hosted write/push/tag action is authorized.

Estimated implementation plus tests/removals: 230–330 changed lines.

## Steps

1. New tests import final `_sync.py`/`_async.py` classes directly and use shared native result/pool ports. Add RED cases for None-returning queue operations, ordered int/list[dict] results, result-count mismatch, hydration error, caught server failure, caller exception after results, second-results/queue-after-flush policy and nested context owner refusal. Assert observed store rollback/commit and exact lease lifetime, not only calls.
2. Add pipeline() methods only to the permanent native implementations, using completed pipeline.py::SyncPostgresPipeline/AsyncPostgresPipeline. Reuse driver._prepare_native_query; do not duplicate lexical bind lowering or batch protocol. A transient context owns one native lease and BEGIN; ambient context uses its owned lease/savepoint. results flushes without commit; successful exit alone finishes success. Native records/errors/skips and first canonical exception remain authoritative; one data Sync/logical flush excludes control exchanges.
3. Exercise actual SQLSpec BaseSyncDriver/BaseAsyncDriver.execute_stack over explicitly injected permanent native drivers with explicit native transport profiles from t87; no registered default changes. Keep strict zip/hydration inside with/async with, so cardinality, projection or SQLResult construction failure rolls back before release. Continue_on_error and nonexecute fallbacks retain behavior. Existing legacy SQLSpec/dialect tests run unchanged alongside these new tests.
4. Project native select outcomes through the established PostgreSQL public row helper, preserving JSON-null conventional output and duplicate-label dictionary behavior. Preserve exact SyncPipelineCapable/AsyncPipelineCapable signatures. No class implementation or factory body is added to driver.py. Commit feat(postgres): bind native pipeline factories to owned scopes.

## Verification

Write named behavioral RED cases first, then implement and require passing assertions.

```bash
make dev
uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py -q
uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres/_sync.py src/py/vertebra/db/adapters/postgres/_async.py src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py
uv run --no-sync mypy src/py/vertebra/db/adapters/postgres src/py/vertebra/extensions/sqlspec
```

## Acceptance Criteria

- [ ] R13, R17, R24: named behavior, lifetime and negative cases pass with the intended publication boundary.
- [ ] The current public legacy pair remains green before t91; after t91 every public path resolves only to the permanent native classes.
- [ ] One bounded signed local commit and task-first checklist/state/evidence synchronization complete this invocation.

## Notes & Discoveries

None.
