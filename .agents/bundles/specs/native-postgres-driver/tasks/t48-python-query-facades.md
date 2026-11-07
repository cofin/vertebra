---
type: Task
id: native-postgres-driver:t48
title: Typed sync and async query transaction facades
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t47
files:
- src/rs/vertebra-postgres/src/sync_client.rs
- src/rs/vertebra-postgres/src/async_client.rs
- src/rs/vertebra-postgres/src/errors.rs
- src/rs/vertebra-postgres/src/lib.rs
- src/py/vertebra/_postgres.pyi
- src/py/tests/unit/db/test_postgres_native_queries.py
tests:
- src/py/tests/unit/db/test_postgres_native_queries.py
plan_revision: 2
plan_commit: "7657e8b7c2c96bdda6821aec70a2b603f438a127"
state_revision: 0
claimed_by: null
claimed_at: null
commit: null
verification_evidence: null
created_at: '2026-11-05T18:25:07Z'
updated_at: '2026-11-06T06:30:00Z'
requirements:
- R6
- R7
- R12
- R13
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
---

# Typed sync and async query transaction facades

## Objective

Expose typed owned query results and canonical exceptions through the established sync/async clients.

## Context

Read `../spec.md`, direct dependency worksheets and notes, `.agents/bundles/knowledge/domains/data/{database-drivers-and-dialers,query-pipeline-cdc-and-tenancy,broker-and-shm-leases}.md`, and applicable `.agents/skills/{pyo3-bridge,runtime-ipc}/SKILL.md`. New paths listed in this worksheet are intentional additions under the t01 scaffold; inspect existing parents and dependency APIs before creating them. This is one invocation and one signed conventional local commit on the same feature branch and in the complete-driver PR. Estimated 390 handwritten changed lines including tests; cap approximately400. Preserve unrelated edits. No external repository writes. If the contract/budget does not hold, route `/flow:revise native-postgres-driver` before changing scope. This task is pending and is not implementation approval.

## Steps

1. Write RED scripted native-client tests for query/query_multi results, DML count/command tags, repeated column names, typed NULL/explicit OID binds, all three transaction states, multi-result simple queries, wrong parameter counts, closed/busy clients, and canonical exception identity/diagnostic attrs. Add concurrent async calls proving per-session exclusion and loop responsiveness.

2. Extend existing sync_client.rs::PySyncPostgresClient and async_client.rs::PyAsyncPostgresClient (public Python names SyncPostgresClient and AsyncPostgresClient): query(sql:str,parameters:Sequence[PostgresValue|PgParameter]=(),*,parameter_oids:Sequence[int]|None=None)->PostgresQueryResult; query_multi(sql:str)->tuple[PostgresQueryResult,...]; begin(),commit(),rollback(),savepoint(token),release_savepoint(token),rollback_to_savepoint(token). Async variants return Awaitable of the same values via the existing dedicated completion drainer. Raw native parameters are ordered numeric bindings; adapter mapping/SQL lowering belongs to the cutover-owned final boundary. query_multi is the public name for the Rust simple_query multi-result engine; do not add duplicate public simple_query alias. query always uses one extended statement; multi-command SQL with parameters raises canonical ProgrammingError rather than silently selecting a result set.

3. Use python_values once, sync py.detach plus block_on_db and existing guarded async task registration. Shared SessionHandle operation guards own the connection while native work runs and publish results after locks release. This commit introduces the public query signatures using t47 production value conversion.

4. Implement/extend errors.rs::postgres_error_to_py resolving exact canonical _vertebra exception classes through PyOnceLock, attaching sqlstate/vendor_code/is_transient plus structured server diagnostic attrs. For t40 PostgresError::AfterCompletion, classify/map the underlying source to the same canonical shared exception, keep its SQLSTATE/diagnostics, and attach the original PostgresQueryResult as .completed_result; never replace it with catalog results or another exception class. Add a scripted metadata transport failure after a side-effecting completed query and assert original count/tag/rows plus one Execute. Python input type/shape failures remain deterministic InterfaceError/DataError as documented. No fallback duplicate DatabaseError hierarchy in installed runtime.

5. Update _postgres.pyi with exact argument defaults and result classes imported from the adapter types module. Maintain gil_used=false/immutable_type and detached blocking semantics. Commit feat(postgres): expose typed sync and async query clients.

## Verification

Run the focused commands after introducing the public behavior tests to capture RED, then again after implementation to capture GREEN. New API absence may be the initial compile RED; existing-defect tasks require a behavioral reproduction. Use independent server fixtures or scripted peers, not implementation text scans.

```bash
make dev
uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_queries.py -q
uv run --no-sync python -m mypy.stubtest vertebra._postgres
```

Run `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` and `git diff --check`. If Python tests load a changed extension, run `make dev` first, then `uv run --no-sync python -m mypy.stubtest vertebra._postgres` for changed public native surfaces. Record actual command/exit and assertion evidence; no command has run during planning. The parent acceptance task owns full native PG14–18, wheel, free-threaded, performance and aggregate acceptance; focused success does not close the charter.

## Acceptance Criteria

- [ ] R6, R7, R12, R13: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] Sync/async public signatures and result record shape agree with stubs and adapter consumers.
- [ ] Canonical DatabaseError subclasses retain SQLSTATE and diagnostics.
- [ ] Existing connection/authentication APIs still pass focused tests.
- [ ] Focused RED/GREEN evidence, relevant typing/lint and a clean task diff are recorded; exactly one signed local commit is linked.

## Notes & Discoveries

None.
