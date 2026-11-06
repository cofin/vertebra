---
type: Task
id: native-postgres-driver:t93
state: pending
priority: P1
verification_strategy: characterization
depends_on:
- native-postgres-driver:t92
plan_revision: 2
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
created_at: '2026-11-05T18:25:07Z'
updated_at: '2026-11-06T06:30:00Z'
commit: null
tags:
- postgres
- cutover
- native
requirements:
- R24
title: Remove unreachable legacy sync PostgreSQL implementation
files:
- src/py/vertebra/db/adapters/postgres/driver.py
tests:
- src/py/tests/unit/db/test_postgres_driver.py
- src/py/tests/unit/db/test_postgres_native_adapters.py
- src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
---

# Remove unreachable legacy sync PostgreSQL implementation

## Objective

Remove unreachable private legacy sync code after the atomic public native activation, preserving the established canonical behavior.

## Context

Read `../spec.md` and t91 activation evidence. All public sync/async/config/registry/consumer exports already select final `_sync.py`/`_async.py` classes. These private bodies are unreferenced remnants within the same feature branch, not public compatibility APIs. One invocation/one signed conventional local commit; own only driver.py, preserve unrelated edits and do not mutate tests to justify removal. No implementation starts with null plan_commit. No external action is authorized.

Estimated changed lines including removals and newly unused import cleanup: 400–445; current old Sync body has approximately399 lines. This is an honest deletion budget, not hidden work in a verification task.

## Steps

1. Run the canonical baseline commands below and record passing native public behavior/identity, SQLSpec/SQLAlchemy binds and existing PostgreSQL regression results. Search references to the exact private legacy class name and verify it is selected by no public/runtime path; this is a manual removal audit, not a new structural test that mirrors the deletion.
2. Delete the entire `_LegacySyncPostgreSQLDriver` class and all remaining legacy-only imports/constants/state from driver.py. Leave only module docstring, grouped imports of final `._sync` and `._async` classes, and canonical __all__. The final source is the real permanent public barrel: no legacy class, memory/executor branch, dynamic fallback, optional Python PostgreSQL client import or temporary module survives. Preserve CPU codec exports in core.py and benchmark-only reference dependencies.
3. Re-run exactly the same canonical baseline commands. Public imports remain identical class objects from their final defining modules and native port store mutations/count/rollback results remain unchanged. No fake RED, class rename, alternate native engine or public compatibility alias is added. Record before/after command exits and observed public assertions, then commit `refactor(postgres): remove dead legacy sync driver`.

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
- [ ] One bounded signed local commit and synchronized task-first evidence precede native conformance and final complete-driver delivery.

## Notes & Discoveries

None.
