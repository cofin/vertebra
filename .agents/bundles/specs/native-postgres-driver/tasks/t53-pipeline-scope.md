---
type: Task
id: native-postgres-driver:t53
title: Pipeline transaction scope through caller result conversion
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t52
files:
- src/rs/vertebra-postgres/src/pipeline_scope.rs
- src/rs/vertebra-postgres/src/session.rs
- src/rs/vertebra-postgres/src/sync_client.rs
- src/rs/vertebra-postgres/src/async_client.rs
- src/rs/vertebra-postgres/src/transaction.rs
- src/rs/vertebra-postgres/src/lib.rs
- src/rs/vertebra-postgres/tests/pipeline_scope.rs
tests:
- src/rs/vertebra-postgres/tests/pipeline_scope.rs
plan_revision: 1
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
commit: null
verification_evidence: null
created_at: '2026-11-05T18:25:07Z'
updated_at: '2026-11-05T20:03:00Z'
requirements:
- R13
- R17
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
---

# Pipeline transaction scope through caller result conversion

## Objective

Keep a transaction or savepoint alive from pipeline context entry until caller result conversion exits cleanly.

## Context

Read `../spec.md`, direct dependency worksheets and notes, `.agents/bundles/knowledge/domains/data/{database-drivers-and-dialers,query-pipeline-cdc-and-tenancy,broker-and-shm-leases}.md`, and applicable `.agents/skills/{pyo3-bridge,runtime-ipc}/SKILL.md`. New paths listed in this worksheet are intentional additions under the t01 scaffold; inspect existing parents and dependency APIs before creating them. This is one invocation and one signed conventional local commit on the same feature branch and in the complete-driver PR. Estimated 400 handwritten changed lines including the central gate and parameterized scope tests; keep the approximate400-line heuristic. Preserve unrelated edits. No external repository writes. If the contract/budget does not hold, route `/flow:revise native-postgres-driver` before changing scope. This task is pending and is not implementation approval.

## Steps

1. Write RED native scope tests for outside/inside ambient transaction, successful results followed by caller conversion failure, results length mismatch, server error, user body error before flush, repeated results call, nested/concurrent scope refusal and close/drop. Prove rollback after a post-results exception by checking durable rows from a separate connection. From the CURRENT owning client/thread/task, exercise every already-implemented raw query/query_multi/simple_query, begin/commit/rollback, savepoint/release/rollback-to and portal/session reservation both before and after results; every ordinary entry is refused before bytes. No future COPY/portal-FFI method is pre-implemented just for this test. Their later worksheets/full acceptance call the same mandatory ordinary reservation gate and add actual API refusal tests when those APIs exist. Include queued INSERT then a forbidden head, catch its client error inside the context, and prove the scope still rolls back. Ambient work before the savepoint survives and is not committed by this scope.

2. Implement public pipeline_scope.rs::PipelineScopeToken and SessionHandle::{pipeline_begin,pipeline_results,pipeline_finish}. Begin creates an owned explicit transaction when I, or a savepoint when T; E refuses a new scope. Retain exclusive logical owner and real lease through response collection, Python projection and context exit. Extend the shared SessionHandle operation reservation gate with private SessionAccess::{Ordinary,Pipeline(token),Cancel,ForcedCleanup}; every ordinary raw API is forbidden while a pipeline token is active even for the same logical owner. Public ordinary methods do not accept or derive a privileged token. Dedicated pipeline_results/pipeline_finish validate the exact token internally; out-of-band cancel and bounded forced cleanup remain legitimate. BEGIN/savepoint and finish are control exchanges outside the measured N-op single-Sync data batch.

3. pipeline_results(token,operations) runs at most once, caches immutable outcomes for repeat reads and never commits. A t50 preflight validation error marks the native scope failed BEFORE it is returned, retains the first source/index, emits no data operations and requires no data drain. A raw-API reservation violation also cannot bypass scope ownership or make a later finish succeed. After a server-submitted pipeline error drain first and mark the scope failed; finish(false) at context exit rolls back the owned transaction or rolls back/releases only the savepoint. A caught client guard error or server error cannot turn a failed scope into success: finish(true) still rolls it back and reports the FIRST canonical failure. Guard failure diagnostics use outcomes=() and failing_index because no operation was submitted; server-submitted result/error/skipped records remain unchanged. Ambient transaction work before the scope remains intact; do not substitute another pooled session.

4. pipeline_finish(token,success) commits/releases only when server results, local decode/projection and enclosing caller context all exit cleanly. COMMIT failure is observable; failure cleanup preserves the primary error and chains rollback/close errors. Dropped scope schedules rollback/retirement and keeps lease unavailable until complete.

5. Route ALL existing/future raw query, multi/simple-query, transaction/savepoint, COPY and portal acquisition through the ordinary reservation gate; matching thread/task/client identity does not exempt them. Direct client close/release cannot return the pinned session underneath its scope; drop/forced shutdown uses the existing bounded rollback/retirement path. Dedicated finish/cancel remains allowed, and immutable no-I/O status snapshots need no transport reservation. Internal BEGIN/savepoint/finish SQL uses the trusted token path, never a public raw-query bypass. Preserve caller conversion exceptions as rollback triggers after results. Commit feat(postgres): retain pipeline transaction scope through context exit.

## Verification

Run the focused commands after introducing the public behavior tests to capture RED, then again after implementation to capture GREEN. New API absence may be the initial compile RED; existing-defect tasks require a behavioral reproduction. Use independent server fixtures or scripted peers, not implementation text scans.

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test pipeline_scope
```

Run `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` and `git diff --check`. If Python tests load a changed extension, run `make dev` first, then `uv run --no-sync python -m mypy.stubtest vertebra._postgres` for changed public native surfaces. Record actual command/exit and assertion evidence; no command has run during planning. The parent acceptance task owns full native PG14–18, wheel, free-threaded, performance and aggregate acceptance; focused success does not close the charter.

## Acceptance Criteria

- [ ] R13, R17: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] Post-results caller exceptions roll back the pipeline scope.
- [ ] Ambient transactions are preserved through savepoint ownership.
- [ ] Current-owner raw APIs are refused while active; no raw COMMIT/ROLLBACK/query/COPY/portal call can escape the pipeline token, before or after results.
- [ ] Caught preflight client guard failures persist until rollback/savepoint cleanup; finish(true) cannot commit them.
- [ ] Exactly one owner controls finish/release and results() never commits.
- [ ] Focused RED/GREEN evidence, relevant typing/lint and a clean task diff are recorded; exactly one signed local commit is linked.

## Notes & Discoveries

None.
