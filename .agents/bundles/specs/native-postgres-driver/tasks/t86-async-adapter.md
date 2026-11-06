---
type: Task
id: native-postgres-driver:t86
title: Implement the permanent asynchronous native PostgreSQL driver before activation
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t85
files:
- src/py/vertebra/db/adapters/postgres/_async.py
- src/py/tests/unit/db/postgres_ports.py
- src/py/tests/unit/db/test_postgres_native_adapters.py
tests:
- src/py/tests/unit/db/test_postgres_native_adapters.py::test_async_native_transaction_ownership
- src/py/tests/unit/db/test_postgres_native_adapters.py::test_async_native_failed_begin_releases
- src/py/tests/unit/db/test_postgres_native_adapters.py::test_async_native_cancel_releases
- src/py/tests/unit/db/test_postgres_native_adapters.py::test_async_native_cancel_routing
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
- R7
- R13
- R24
- R18
---

# Implement the permanent asynchronous native PostgreSQL driver before activation

## Objective

Implement the final asynchronous native facade independently of the legacy Sync/Async constructor coupling.

## Context

Read `../spec.md`, completed native contracts and `.agents/skills/python-framework/SKILL.md`, `.agents/skills/pyo3-bridge/SKILL.md`, database-driver knowledge and Python module layout. The permanent native implementations are `_sync.py::SyncPostgreSQLDriver` and `_async.py::AsyncPostgreSQLDriver`; `driver.py` is the canonical public export module after atomic t91 activation. Before t91 the unchanged public legacy pair remains coherent. New implementation tests import the final defining modules until t91 switches them to canonical public imports and identity assertions. No temporary implementation module, class alias, compatibility shim or second native engine is introduced.

One executor invocation produces one signed conventional local commit on the complete driver's feature branch; all work ships in one complete PR. Ownership is the listed files/symbols, shared files change sequentially and unrelated edits remain intact. No implementation starts with null plan_commit. Commands are planned, not evidence. Record actual public assertions and exits; rebuild native code with make dev before Python tests. If measured scope exceeds the stated near400-line estimate, revise the task boundary before implementation rather than leave a broken intermediate tree. No hosted write/push/tag action is authorized.

Estimated implementation plus tests/removals: 400–450; final native async class and compact shared-port tests, no legacy-body deletion changed lines.

## Steps

1. Define AsyncPostgreSQLDriver(AsyncDatabaseDriver[PostgreSQLConfig]) permanently in `_async.py`, with no SyncPostgreSQLDriver construction or `_sync` fallback. Extend shared native-shaped pool/client ports and parameterized tests from t85; direct-import this defining module before activation. Preserve legacy async regression assertions using the new port: DDL0, execute_many count, commit/rollback visibility, cancelled-scope rollback, failed begin retry and balanced leases. Add event-loop responsiveness and two-task isolation. Existing driver.py and all public exports remain untouched.
2. Implement awaited AsyncPostgresPool creation/acquire and complete configuration identity validation using the same primitive config bridge. Native owns asynchronous IAM/cloud material/token refresh; no Python cross-extension cache. Ordinary operations release transient leases, explicit transactions/portals/pipelines pin one exact native client. ContextVar owner-task/thread checks prevent child-task inherited state or unrelated request tasks from sharing/overwriting one transaction socket. Nested scopes use native savepoints and bounded shielded cleanup, preserving original errors.
3. Implement async query/count/description/query_multi and `_prepare_native_query` with the exact t85 rich-versus-conventional projection policy. Cancellation routes to generation-fenced native cancel and shields independent native cleanup; every physical session possibly cancelled is retired. Cancelled ambient transaction remains failed until rollback and never acquires a replacement mid-scope. Outside a completed scope the same driver uses a fresh backend. Test active/cancel races and no leaked pool capacity. Sync cancellation already belongs to `_sync.py`; do not mutate legacy driver classes. Forward high-level `timeout` through async execute/description/select to native query and expose async query_multi(sql,*,timeout:float|None=None), using completed t57 command-only budget. Preserve None/unbounded default, finite positive native validation, OperationalError(sqlstate=None) client expiry and actual server57014 identity. execute_many applies timeout per command, not across the batch; acquisition/reset/CPU projection retain their separate contracts. Add async forwarding, default, expiry/no-replay/retirement and per-command-many tests; no new pipeline/COPY/portal timeout or global config is introduced.
4. Add complete thin native Arrow selection/stream/import forwarders using completed arrow.py/native APIs and async finally/aclose ownership. Native stream constructor returns stream directly; async iteration/close are awaited. Do not implement sync fallback, coroutine duck typing or fake methods pending a later class move. Record real method signatures and shared typed results.

5. Implement explicit driver.cancel selection using a driver-private active-operation registry guarded by a short lock, separate from transaction ContextVar scopes. Each registered record owns thread/task identity, monotonically scoped operation generation and the exact native client while actual wire work is pending; register before submission and remove in finally after drain/cleanup. Prefer an active operation belonging to the caller context; otherwise select exactly one active operation anywhere in THIS driver, including a child task/worker whose ContextVar changes are invisible to its parent. No active record means idle no-op; multiple candidates raise exact InterfaceError before any CancelRequest. A different driver sharing the same pool owns a different registry and is never a candidate. Task.cancel remains per-operation.
6. Select and claim a record under the short lock, then release that lock before native I/O/await. Retain its operation/lease ownership until bounded cancel claim finishes; query-finally cannot release/reassign that selected client or start later work on it during this window. Existing native SessionHandle/OperationGeneration fencing is authoritative for request send/retirement and rejects stale or completed generations; no new public generation kwargs or imaginary current-client API is added. Do not hold a Python threading lock across blocking/awaited native work. Actual request may-send still retires the physical connection, and failed ambient transaction stays failed until rollback.
7. Add coordinated public-behavior tests for cancel from parent context while one child/worker runs a query outside a transaction, current-context preference, idle no-op, two active operations producing InterfaceError with ZERO cancellation packets, another driver sharing that pool, query completion/lease-return racing selection, and subsequent borrower immunity. Barriers/events observe server CancelRequest and independent result state; registry-count assertions alone do not prove routing. These unit/native-wire cases feed later active-query conformance acceptance after canonical activation. Commit feat(postgres): implement permanent asynchronous native driver.

## Verification

Write named behavioral RED cases first, then implement and require passing assertions.

```bash
make dev
uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_adapters.py -q
uv run --no-sync pytest src/py/tests/unit/db/test_postgres_driver.py -q
uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres/_sync.py src/py/vertebra/db/adapters/postgres/_async.py src/py/tests/unit/db/postgres_ports.py src/py/tests/unit/db/test_postgres_native_adapters.py
uv run --no-sync mypy src/py/vertebra/db/adapters/postgres
```

## Acceptance Criteria

- [ ] R7, R13, R18, R24: named behavior, lifetime and negative cases pass with the intended publication boundary.
- [ ] The current public legacy pair remains green before t91; after t91 every public path resolves only to the permanent native classes.
- [ ] One bounded signed local commit and task-first checklist/state/evidence synchronization complete this invocation.

## Notes & Discoveries

None.
