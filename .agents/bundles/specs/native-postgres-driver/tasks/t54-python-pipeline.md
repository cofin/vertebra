---
type: Task
id: native-postgres-driver:t54
title: Python native pipeline capability contexts
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t53
files:
- src/rs/vertebra-postgres/src/python_pipeline.rs
- src/rs/vertebra-postgres/src/sync_client.rs
- src/rs/vertebra-postgres/src/async_client.rs
- src/rs/vertebra-postgres/src/lib.rs
- src/py/vertebra/_postgres.pyi
- src/py/vertebra/db/adapters/postgres/pipeline.py
- src/py/vertebra/db/adapters/postgres/types.py
- src/py/tests/unit/db/test_postgres_native_pipeline.py
tests:
- src/py/tests/unit/db/test_postgres_native_pipeline.py
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
- R17
- R13
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
---

# Python native pipeline capability contexts

## Objective

Expose native batches through SyncPipeline/AsyncPipeline with ordered DML counts and dictionary row results.

## Context

Read `../spec.md`, direct dependency worksheets and notes, `.agents/bundles/knowledge/domains/data/{database-drivers-and-dialers,query-pipeline-cdc-and-tenancy,broker-and-shm-leases}.md`, and applicable `.agents/skills/{pyo3-bridge,runtime-ipc}/SKILL.md`. New paths listed in this worksheet are intentional additions under the t01 scaffold; inspect existing parents and dependency APIs before creating them. This is one invocation and one signed conventional local commit on the same feature branch and in the complete-driver PR. Estimated 420 handwritten changed lines including owned snapshots, persistent failure and parameterized context tests; retain this honest cohesive estimate near the approximate400-line heuristic. Preserve unrelated edits. No external repository writes. If the contract/budget does not hold, route `/flow:revise native-postgres-driver` before changing scope. This task is pending and is not implementation approval.

## Steps

1. Write RED sync/async context tests for execute/select queueing, ordered int/list[dict] results, duplicate labels matching ordinary driver.select behavior, rows with typed values, first canonical error plus attached ordered outcomes, skipped entries, repeated results, no-flush body error and conversion failure after results. Use scripted native clients for real wire/transaction behavior and a local injected callback probe for preparation contracts; no driver/Core helper exists at this task. Queue nested JSON/list/array parameters, mutate originals after execute/select and before results/await, and prove the server receives the enqueue-time snapshot. Queue INSERT then COMMIT/END and the complete t50 alias table with leading comments/empty semicolons; catch InterfaceError/NotSupportedError from results inside the with block, verify outcomes=()/failing_index, and prove __exit__/__aexit__ still rolls back with zero data-operation frames/no inserted rows. Also catch an injected preparation/snapshot validation error, and reject same-client raw commit/query before and after results; caller conversion errors still roll back.

2. Expose native clients pipeline_begin()->int scope_id; pipeline_results(scope_id:int,operations:Sequence[PostgresPipelineOperation])->tuple[PostgresPipelineOutcome,...]; pipeline_finish(scope_id:int,success:bool)->None with async Awaitable counterparts. Define immutable typed records in adapter types.py: PostgresPipelineOperation(sql:str,parameters:tuple[PostgresValue|PgParameter,...]=(),parameter_oids:tuple[int,...]|None=None,kind:Literal["execute","select"]="execute"); PostgresErrorInfo(sqlstate:str|None,message:str,severity:str|None,diagnostics:tuple[tuple[str,str],...]); PostgresPipelineOutcome(index:int,status:Literal["result","error","skipped"],result:PostgresQueryResult|None=None,error:PostgresErrorInfo|None=None,failed_index:int|None=None). Validate discriminant/payload consistency and update _postgres.pyi. On server-submitted failure raise the canonical first DatabaseError with ordered outcomes and failing_index; never return exception objects as rows. For native t50 preflight rejection retain the same canonical InterfaceError/NotSupportedError with failing_index and outcomes=(), denoting ALL operations unsubmitted, not synthetic server Skipped records.

3. Implement adapter-owned pipeline.py::{SyncPostgresPipeline,AsyncPostgresPipeline}. The constructor receives an explicit corresponding raw client (which may itself own a real pooled lease), a typed preparation callback and an optional typed managed-release callback; it never reads driver internals. Define PreparePostgresQuery as Callable[[str,Sequence[PostgresValue|PgParameter]|Mapping[str,PostgresValue|PgParameter]|None],tuple[str,tuple[PostgresValue|PgParameter,...]]]. The default callback accepts numeric $N SQL plus an ordered non-string sequence/None only, normalizes None to (), and rejects Mapping explicitly. It forwards numeric SQL unchanged; it never rewrites legacy placeholders. Active legacy placeholders fail through native Parse/bind validation, while quoted literal marker text remains unchanged. It does not implement the future SQL-aware compiler. Context entry begins native scope on that supplied client. Later driver factories inject their own already-normalized callback and managed lease release. execute/select invokes the callback immediately, then validates and recursively snapshots every permitted nested parameter (JSON dict/list, arrays/composites/wrappers and byte buffers) into owned/frozen queued payload before returning. No caller-owned mutable reference survives until results()/await. Every preparation, snapshot, enqueue-state or native batch guard exception sets a persistent Python failed flag and retains the first error before re-raising; catching it inside the body never clears that flag. Native preflight failure also persists in t53 scope state, so direct raw callers cannot bypass this protection. Reuse t47 supported value/ownership rules; reject cycles/unsupported values before enqueue. The private snapshot recursively copies allowed dict/list/tuple/wrapper contents and byte buffers while preserving their public value types (JSON lists remain JSON lists inside PgJson); it never exposes the owned queued copy back to the caller. Native extraction at flush sees only those owned snapshots, not original mutable inputs.

4. results() submits once, projects DML to affected int and selects through types.py::_project_public_row to list[dict[str,PostgresValue]], validates outcome cardinality and leaves the transaction open. __exit__/__aexit__ forwards success only when both actual exception state and persistent Python/native failed flags are clear. A caught guard exception therefore calls native finish with failure, even when exc_type is None. Further enqueue/results calls after failure cannot submit or retry work and preserve the first error. Forward the actual exception state and persistent failed-scope disposition to native finish before invoking the injected managed-release callback when one is supplied. Invoke managed release exactly once after native scope finish or retirement, preserving a primary canonical query/conversion error and chaining any cleanup error. Without that callback, the caller retains ownership of the raw client/lease; the context does not close it. Thus SQLSpec zip(strict=True) and SQLResult construction inside the with block can still trigger rollback. Explicit ambient scope never commits caller work.

5. The public constructors are SyncPostgresPipeline(client:SyncPostgresClient,*,prepare:PreparePostgresQuery|None=None,release:Callable[[],None]|None=None) and AsyncPostgresPipeline(client:AsyncPostgresClient,*,prepare:PreparePostgresQuery|None=None,release:Callable[[],Awaitable[None]]|None=None). Driver.pipeline() factories later supply prepared callbacks and managed clients; their future driver/Core helpers and live SQLSpec integration tests are not dependencies here. Preserve the driver factory callback boundary so the binding compiler is never duplicated. Add typed surface parity and commit feat(postgres): expose transactional Python pipeline contexts.

## Verification

Run the focused commands after introducing the public behavior tests to capture RED, then again after implementation to capture GREEN. New API absence may be the initial compile RED; existing-defect tasks require a behavioral reproduction. Use independent server fixtures or scripted peers, not implementation text scans.

```bash
make dev
uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_pipeline.py -q
uv run --no-sync python -m mypy.stubtest vertebra._postgres
```

Run `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` and `git diff --check`. If Python tests load a changed extension, run `make dev` first, then `uv run --no-sync python -m mypy.stubtest vertebra._postgres` for changed public native surfaces. Record actual command/exit and assertion evidence; no command has run during planning. The parent acceptance task owns full native PG14–18, wheel, free-threaded, performance and aggregate acceptance; focused success does not close the charter.

## Acceptance Criteria

- [ ] R17/R13: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] Capability results match SQLSpec int/list-of-dictionaries consumption.
- [ ] Canonical first failure carries ordered outcomes; skipped operations are not rows.
- [ ] A caught queue/preparation/snapshot/native guard exception still rolls back the context/savepoint; no first INSERT is emitted when a later transaction-control/COPY head is rejected.
- [ ] Same-owner raw commit/query cannot terminate or use the retained scope, including after results but before conversion exits.
- [ ] User/conversion/cardinality errors inside the context roll back before releasing the lease.
- [ ] Focused RED/GREEN evidence, relevant typing/lint and a clean task diff are recorded; exactly one signed local commit is linked.

## Notes & Discoveries

None.
