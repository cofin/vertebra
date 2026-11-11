---
type: Task
id: "native-postgres-driver:t87"
title: "Prepare native PostgreSQL compiler boundaries without changing public defaults"
description: "Prepare native PostgreSQL compiler boundaries without changing public defaults for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:t86"
files:
  - src/py/vertebra/extensions/sqlspec/adapters.py
  - src/py/vertebra/extensions/sqlalchemy/dialects.py
  - src/py/tests/unit/extensions/test_postgres_compiler_transport.py
tests:
  - src/py/tests/unit/extensions/test_postgres_compiler_transport.py
plan_revision: 2
plan_commit: 9af810f5
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

# Prepare native PostgreSQL compiler boundaries without changing public defaults

## Objective

Implement permanent pure prepared-statement transport helpers and profiles while legacy public consumers retain their existing parameter formats until t91.

## Context

Read `../spec.md`, completed native contracts and `.agents/skills/python-framework/SKILL.md`, `.agents/skills/pyo3-bridge/SKILL.md`, database-driver knowledge and Python module layout. The permanent native implementations are `_sync.py::SyncPostgreSQLDriver` and `_async.py::AsyncPostgreSQLDriver`; `driver.py` is the canonical public export module after atomic t91 activation. Before t91 the unchanged public legacy pair remains coherent. New implementation tests import the final defining modules until t91 switches them to canonical public imports and identity assertions. No temporary implementation module, class alias, compatibility shim or second native engine is introduced.

One executor invocation produces one signed conventional local commit on the complete driver's feature branch; all work ships in one complete PR. Ownership is the listed files/symbols, shared files change sequentially and unrelated edits remain intact. No implementation starts with null plan_commit. Commands are planned, not evidence. Record actual public assertions and exits; rebuild native code with make dev before Python tests. If measured scope exceeds the stated near400-line estimate, revise the task boundary before implementation rather than leave a broken intermediate tree. No hosted write/push/tag action is authorized.

Estimated implementation plus tests/removals: 300–380; pure helper/profile behavior plus tests, default activation deferred atomically to t91 changed lines.

## Steps

1. Add behavioral RED tests using actual SQLSpec SQL/StatementConfig/prepared objects and real SQLAlchemy compiler objects. Cover raw/prebound statements, caller config identity/question-mark compile, %s/named pyformat/native$N/colon inputs, TypedParameter conversion, repeated/out-of-order values, many/filter/Arrow/stack metadata and quotes/E/dollar/comments/JSON? operators. Assert original compilation/provenance remains unchanged and a separately requested native transport copy emits correct numeric SQL and ordered values.
2. Implement permanent private SQLSpec transport helper/profile in adapters.py using existing public SQL.copy(statement_config=original_config.replace(parameter_config=...NUMERIC...)) and prepare_driver_parameters on the transport copy. No regex rewrite, original config mutation or transformer double application. Non-idempotent statement transformer test proves configured transformations execute once semantically. A copied already-prepared SQL object retains filters, original SQLResult/StackResult.statement and explicit custom configuration authority. Helpers remain production-use functions selected during t91, not disposable test-only APIs.
3. Define/test the PostgreSQL numeric_dollar SQLAlchemy DBAPI/compiler profile through explicit constructor/config inputs; exercise SELECT/INSERT RETURNING/UPDATE, expanding IN, repeated names and executemany against a native-shaped port. Do not change registered Sync/AsyncPostgreSQLDialect dbapi/default_paramstyle or PostgreSQLSyncDriver._execution_parameter_style yet. Legacy sync public driver must still receive pyformat and current explicit question-mark port regressions remain green before t91.
4. Keep all native transport activation in t91: default NUMERIC sync/async SQLSpec profile, numeric_dollar registered dialect wiring and actual native execution paths. This task delivers complete pure helpers/profile tests, not partial activation or a runtime flag selecting two engines. Commit feat(postgres): prepare native consumer transport compilation.

## Verification

Write named behavioral RED cases first, then implement and require passing assertions.

```bash
uv run --no-sync pytest src/py/tests/unit/extensions/test_postgres_compiler_transport.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py -q
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters.py src/py/vertebra/extensions/sqlalchemy/dialects.py src/py/tests/unit/extensions/test_postgres_compiler_transport.py
uv run --no-sync mypy src/py/vertebra/extensions/sqlspec src/py/vertebra/extensions/sqlalchemy
```

## Acceptance Criteria

- [ ] R2, R24: named behavior, lifetime and negative cases pass with the intended publication boundary.
- [ ] The current public legacy pair remains green before t91; after t91 every public path resolves only to the permanent native classes.
- [ ] One bounded signed local commit and task-first checklist/state/evidence synchronization complete this invocation.

## Notes & Discoveries

None.
