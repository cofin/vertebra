---
type: Task
id: native-postgres-driver:t92
state: pending
priority: P1
verification_strategy: characterization
depends_on:
- native-postgres-driver:t91
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
- R24
title: Remove unreachable legacy async PostgreSQL implementation
files:
- src/py/vertebra/db/adapters/postgres/driver.py
tests:
- src/py/tests/unit/db/test_postgres_driver.py
- src/py/tests/unit/db/test_postgres_native_adapters.py
- src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
---

# Remove unreachable legacy async PostgreSQL implementation

## Objective

Remove unreachable private legacy async code after the atomic public native activation, preserving the established canonical behavior.

## Context

Read `../spec.md` and t91 activation evidence. All public sync/async/config/registry/consumer exports already select final `_sync.py`/`_async.py` classes. These private bodies are unreferenced remnants within the same feature branch, not public compatibility APIs. One invocation/one signed conventional local commit; own only driver.py, preserve unrelated edits and do not mutate tests to justify removal. No implementation starts with null plan_commit. No external action is authorized.

Estimated changed lines including removals and newly unused import cleanup: 380–405; current old Async body has approximately368 lines. This is an honest deletion budget, not hidden work in a verification task.

## Steps

1. Run the canonical baseline commands below and record passing native public behavior/identity, SQLSpec/SQLAlchemy binds and existing PostgreSQL regression results. Search references to the exact private legacy class name and verify it is selected by no public/runtime path; this is a manual removal audit, not a new structural test that mirrors the deletion.
2. Delete the entire `_LegacyAsyncPostgreSQLDriver` class first, including its legacy synchronous-child construction and static helper reference. Remove imports made unused specifically by this deletion (inspect/Awaitable/async base/cancellation symbols as applicable), keeping grouped imports valid and Ruff/Mypy green. Leave the still-dead private sync body for the next bounded task; do not move it or select it.
3. Re-run exactly the same canonical baseline commands. Public imports remain identical class objects from their final defining modules and native port store mutations/count/rollback results remain unchanged. No fake RED, class rename, alternate native engine or public compatibility alias is added. Record before/after command exits and observed public assertions, then commit `refactor(postgres): remove dead legacy async driver`.

## Verification

Run before and after the deletion; all must exit0. These are planned checks, not execution evidence:

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_postgres_driver.py src/py/tests/unit/db/test_postgres_native_adapters.py src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py src/py/tests/unit/db/test_postgres_native_adapter_arrow.py src/py/tests/unit/db/test_postgres_event_hooks.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py -q
uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres
uv run --no-sync mypy src/py/vertebra/db/adapters/postgres src/py/vertebra/extensions
```

## Acceptance Criteria

- [ ] R24: the owned dead class and newly unused imports are removed with no public identity or runtime behavior change.
- [ ] Canonical baseline and final commands pass without weakening tests or adding fake RED.
- [ ] One bounded signed local commit and synchronized task-first evidence precede t93.

## Notes & Discoveries

None.
