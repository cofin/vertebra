---
type: Task
id: "native-postgres-driver:t46"
title: "Rollback reset and draining pool shutdown"
description: "Rollback reset and draining pool shutdown for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:t45"
files:
  - src/rs/vertebra-postgres/src/pool.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/statements.rs
  - src/rs/vertebra-postgres/tests/pool_reset.rs
tests:
  - src/rs/vertebra-postgres/tests/pool_reset.rs
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
  - driver
  - rust
---

# Rollback reset and draining pool shutdown

## Objective

Release only fully drained, reset sessions and retire connections when cleanup cannot prove safety.

## Context

Read `../spec.md`, direct dependency worksheets and notes, `.agents/bundles/knowledge/domains/data/{database-drivers-and-dialers,query-pipeline-cdc-and-tenancy,broker-and-shm-leases}.md`, and applicable `.agents/skills/{pyo3-bridge,runtime-ipc}/SKILL.md`. New paths listed in this worksheet are intentional additions under the t01 scaffold; inspect existing parents and dependency APIs before creating them. This is one invocation and one signed conventional local commit on the same feature branch and in the complete-driver PR. Estimated 390 handwritten changed lines including tests; cap approximately400. Preserve unrelated edits. No external repository writes. If the contract/budget does not hold, route `/flow:revise native-postgres-driver` before changing scope. This task is pending and is not implementation approval.

## Steps

1. Write RED session-reuse tests with temp objects, custom/session GUCs, prepared statements, LISTEN registration, advisory locks, open/failed transaction, suspended portal and cancellation-in-progress. Reborrow and prove server state is reset. Inject EOF/rollback error/reset timeout/expired connection and prove replacement gets a different backend PID.

2. Implement PooledConnection::release and Drop return scheduling. Finish/drain any bounded in-flight response; rollback T/E; close owned portals; execute DISCARD ALL only after I, then restore the configured startup/session_gucs baseline. Invalidate real statement/type cache generations consistently. This conservative reset is the default: prepared-cache benefit is within retained leases/transactions/pipelines, and full acceptance benchmarks report reset throughput cost. Do not classify arbitrary SELECT as clean because server functions can mutate session state; no unproven SQL-text fast return is introduced. Direct/session pooling uses this complete reset; transaction poolers do not claim cleanup of unpinned backend session state and prohibit incompatible session-affine features unless direct_dsn is used.

3. A failed reset, uncertain query cancellation, unread COPY/replication stream, malformed protocol or disconnected session retires the socket and releases its permit exactly once. Never publish an idle lease before reset completes. Retained portal/transaction owners keep the lease pinned until their explicit cleanup; pool close cannot steal their connection.

4. Implement PostgresPool::close(deadline): reject new acquires, wake waiters, close idle sessions, await active returns up to the configured drain deadline, then mark remaining leases retire-on-return and report timeout without capacity leaks. Shutdown/drop performs best-effort native close without blocking the GIL. Stats distinguish connecting/idle/borrowed/resetting/retired.

5. Test concurrent close/return/drop/reset with barriers instead of sleeps and commit feat(postgres): reset and drain physical pool leases.

## Verification

Run the focused commands after introducing the public behavior tests to capture RED, then again after implementation to capture GREEN. New API absence may be the initial compile RED; existing-defect tasks require a behavioral reproduction. Use independent server fixtures or scripted peers, not implementation text scans.

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test pool_reset
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test physical_pool
```

Run `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` and `git diff --check`. If Python tests load a changed extension, run `make dev` first, then `uv run --no-sync python -m mypy.stubtest vertebra._postgres` for changed public native surfaces. Record actual command/exit and assertion evidence; no command has run during planning. The parent acceptance task owns full native PG14–18, wheel, free-threaded, performance and aggregate acceptance; focused success does not close the charter.

## Acceptance Criteria

- [ ] R14: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] No unread/failed/dirty session is returned to idle borrowers.
- [ ] Reset removes temporary/session state and restores configured baseline.
- [ ] Drain shutdown is deadline-bounded and leaked capacity is observable/prevented.
- [ ] Focused RED/GREEN evidence, relevant typing/lint and a clean task diff are recorded; exactly one signed local commit is linked.

## Notes & Discoveries

None.
