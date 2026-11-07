---
type: Task
id: "native-postgres-driver:t85"
title: "Implement the permanent synchronous native PostgreSQL driver before activation"
description: "Implement the permanent synchronous native PostgreSQL driver before activation for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:t84"
files:
  - src/py/vertebra/db/adapters/postgres/_sync.py
  - src/py/tests/unit/db/postgres_ports.py
  - src/py/tests/unit/db/test_postgres_native_adapters.py
tests:
  - src/py/tests/unit/db/test_postgres_native_adapters.py
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

# Implement the permanent synchronous native PostgreSQL driver before activation

## Objective

Implement the final synchronous native facade without changing the still-coherent public legacy pair.

## Context

Read `../spec.md`, completed native contracts and `.agents/skills/python-framework/SKILL.md`, `.agents/skills/pyo3-bridge/SKILL.md`, database-driver knowledge and Python module layout. The permanent native implementations are `_sync.py::SyncPostgreSQLDriver` and `_async.py::AsyncPostgreSQLDriver`; `driver.py` is the canonical public export module after atomic t91 activation. Before t91 the unchanged public legacy pair remains coherent. New implementation tests import the final defining modules until t91 switches them to canonical public imports and identity assertions. No temporary implementation module, class alias, compatibility shim or second native engine is introduced.

One executor invocation produces one signed conventional local commit on the complete driver's feature branch; all work ships in one complete PR. Ownership is the listed files/symbols, shared files change sequentially and unrelated edits remain intact. No implementation starts with null plan_commit. Commands are planned, not evidence. Record actual public assertions and exits; rebuild native code with make dev before Python tests. If measured scope exceeds the stated near400-line estimate, revise the task boundary before implementation rather than leave a broken intermediate tree. No hosted write/push/tag action is authorized.

Estimated implementation plus tests/removals: 400–450; new class/forwarders and compact parameterized tests, no legacy-body deletion changed lines.

## Steps

1. Add final implementation module `_sync.py` defining public-named SyncPostgreSQLDriver(SyncDatabaseDriver[PostgreSQLConfig]). Add shared native-shaped client/pool test support in postgres_ports.py with actual observable storage, exact frozen result records, configuration validation and lease accounting; no cursor/fetchall/asyncpg APIs. New tests import this final module directly, not driver.py. Parameterized RED cases cover empty SELECT metadata, DDL0, UPDATE/DELETE zero/many, RETURNING count, JSON SQLNULL/JSONnull conventional high-level None versus rich query_multi PgJson(None), argument errors and query failure lease release. Existing test_postgres_driver.py remains unchanged and continues to exercise the legacy public pair until t91.
2. Implement lazy native SyncPostgresPool discovery through ensure_module("vertebra._postgres",package="vertebra-core[postgres]",extra="postgres") and completed config primitive bridge. validate_configuration compares complete credential/TLS/GUC/cloud/IAM/preparation identity before using supplied/shared pool. Driver-owned pool closes once; registry-shared pool retains registry lifetime; share_pool=False isolates ownership. Preserve broker admission/metadata leases separately from concrete socket ownership. Ordinary calls borrow/release one transient native client, never a permanent shared-driver socket.
3. Implement context-owned connect/begin/commit/rollback/transaction/close/cancel and query/description/count hooks with ContextVar and thread/task identity. Publish a pinned lease only after BEGIN succeeds; nested transaction contexts use native savepoints and restore parent scope. Reject inherited wrong-thread/task scope use. Failed begin, query, commit/rollback and close release/retire exactly once under native reset policy. Native query returns explicit affected_rows; use types.py::_project_public_row at conventional tuple/dict/hydration boundary, preserving rich native/query_multi records. `_prepare_native_query` delegates completed PostgreSQLCore.prepare_native_query and preserves raw $n/%s/named mapping contracts. query_multi returns every simple-query result with no parameters; parameterized multiSQL fails canonical42601 ProgrammingError. Add two-scope isolation and callback/cleanup-error cases. Forward the existing high-level `timeout` keyword from execute/execute_with_description/select paths to native `query(...,timeout=timeout)` and expose `query_multi(sql,*,timeout:float|None=None)` forwarding that exact option. Reuse completed t57: None leaves ordinary command I/O unbounded; finite positive timeout bounds one reserved command's parameter discovery/execute/read/metadata I/O, not pool acquisition, reset, CPU decoding or Python projection. execute_many forwards timeout separately to each command and never advertises a whole-batch budget. Add parameterized forwarding/default/error tests for single/multi/many calls, canonical client-expiry OperationalError(sqlstate=None), actual server57014 QueryCancelledError, no replay and normal cleanup/retirement. Do not add global command_timeout config or pretend pipeline/COPY/portal gained this option.
4. Add real thin select_to_arrow/stream_arrow_batches/load_from_arrow forwarders using already completed native selection/stream/COPY APIs and arrow.py batch helpers, with lease ownership through cursor/generator closure. Never inherit eager generic Arrow fallback or add placeholder methods. t89 extends edge-case integration and tests; it does not supply a missing class body. Missing extra errors name vertebra-core[postgres], and constructor does not accept old executor/memory modes. Do not edit driver.py, postgres.__init__, config driver names, SQLSpec defaults or consumer helper markers in this task.

5. Implement explicit driver.cancel selection using a driver-private active-operation registry guarded by a short lock, separate from transaction ContextVar scopes. Each registered record owns thread/task identity, monotonically scoped operation generation and the exact native client while actual wire work is pending; register before submission and remove in finally after drain/cleanup. Prefer an active operation belonging to the caller context; otherwise select exactly one active operation anywhere in THIS driver, including a child task/worker whose ContextVar changes are invisible to its parent. No active record means idle no-op; multiple candidates raise exact InterfaceError before any CancelRequest. A different driver sharing the same pool owns a different registry and is never a candidate. Task.cancel remains per-operation.
6. Select and claim a record under the short lock, then release that lock before native I/O/await. Retain its operation/lease ownership until bounded cancel claim finishes; query-finally cannot release/reassign that selected client or start later work on it during this window. Existing native SessionHandle/OperationGeneration fencing is authoritative for request send/retirement and rejects stale or completed generations; no new public generation kwargs or imaginary current-client API is added. Do not hold a Python threading lock across blocking/awaited native work. Actual request may-send still retires the physical connection, and failed ambient transaction stays failed until rollback.
7. Add coordinated public-behavior tests for cancel from parent context while one child/worker runs a query outside a transaction, current-context preference, idle no-op, two active operations producing InterfaceError with ZERO cancellation packets, another driver sharing that pool, query completion/lease-return racing selection, and subsequent borrower immunity. Barriers/events observe server CancelRequest and independent result state; registry-count assertions alone do not prove routing. These unit/native-wire cases feed later active-query conformance acceptance after canonical activation. Commit feat(postgres): implement permanent synchronous native driver.

## Verification

Write named behavioral RED cases first, then implement and require passing assertions.

```bash
make dev
uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_adapters.py -q -k 'sync_native or native_missing_extra'
uv run --no-sync pytest src/py/tests/unit/db/test_postgres_driver.py -q
uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres/_sync.py src/py/tests/unit/db/postgres_ports.py src/py/tests/unit/db/test_postgres_native_adapters.py
uv run --no-sync mypy src/py/vertebra/db/adapters/postgres
```

## Acceptance Criteria

- [ ] R13, R18, R24: named behavior, lifetime and negative cases pass with the intended publication boundary.
- [ ] The current public legacy pair remains green before t91; after t91 every public path resolves only to the permanent native classes.
- [ ] One bounded signed local commit and task-first checklist/state/evidence synchronization complete this invocation.

## Notes & Discoveries

None.
