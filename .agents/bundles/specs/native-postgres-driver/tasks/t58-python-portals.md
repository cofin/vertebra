---
type: Task
id: native-postgres-driver:t58
title: Typed sync and async portal facades
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t57
files:
- src/rs/vertebra-postgres/src/python_portal.rs
- src/rs/vertebra-postgres/src/sync_client.rs
- src/rs/vertebra-postgres/src/async_client.rs
- src/rs/vertebra-postgres/src/lib.rs
- src/py/vertebra/_postgres.pyi
- src/py/tests/unit/db/test_postgres_native_portals.py
tests:
- src/py/tests/unit/db/test_postgres_native_portals.py
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
- R16
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
---

# Typed sync and async portal facades

## Objective

Expose bounded owned portal results without allowing a pooled session to escape its streaming transaction.

## Context

Read `../spec.md`, direct dependency worksheets and notes, `.agents/bundles/knowledge/domains/data/{database-drivers-and-dialers,query-pipeline-cdc-and-tenancy,broker-and-shm-leases}.md`, and applicable `.agents/skills/{pyo3-bridge,runtime-ipc}/SKILL.md`. New paths listed in this worksheet are intentional additions under the t01 scaffold; inspect existing parents and dependency APIs before creating them. This is one invocation and one signed conventional local commit on the same feature branch and in the complete-driver PR. Estimated 340 handwritten changed lines including tests; cap approximately400. Preserve unrelated edits. No external repository writes. If the contract/budget does not hold, route `/flow:revise native-postgres-driver` before changing scope. This task is pending and is not implementation approval.

## Steps

1. Write RED sync/async client tests opening a portal, fetching finite batches, stopping early, closing repeatedly, cancelling fetch, dropping the portal and attempting competing client/pool operations while it is active. Assert native backend row progression and pool lease exclusion; do not mirror private token layout.

2. Expose SyncPostgresClient.open_portal(sql,parameters=(),*,parameter_oids=None,batch_size=65536)->SyncPostgresPortal and AsyncPostgresClient.open_portal(...)->Awaitable[AsyncPostgresPortal]. Portal fetch(max_rows:int)->PostgresQueryResult/awaitable and close()->None/awaitable. Export read-only exhausted/closed status; no raw socket or backend cancellation secrets cross Python.

3. Portal facade retains the owning SessionHandle/real pooled lease and routes all native operations through the same completion/GIL discipline. Conversion uses t47 typed owned records. End-of-stream or explicit close completes the native transaction scope before client/lease reuse; early drop/cancel schedules independent native cleanup and retires uncertain sockets.

4. Update typed stubs and maintain native pyclass immutability/free-threaded safety. Arrow stream consumers may use the same concrete Rust Portal engine without materializing Python tuple rows. Commit feat(postgres): expose owned sync and async portal streams.

## Verification

Run the focused commands after introducing the public behavior tests to capture RED, then again after implementation to capture GREEN. New API absence may be the initial compile RED; existing-defect tasks require a behavioral reproduction. Use independent server fixtures or scripted peers, not implementation text scans.

```bash
make dev
uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_portals.py -q
uv run --no-sync python -m mypy.stubtest vertebra._postgres
```

Run `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` and `git diff --check`. If Python tests load a changed extension, run `make dev` first, then `uv run --no-sync python -m mypy.stubtest vertebra._postgres` for changed public native surfaces. Record actual command/exit and assertion evidence; no command has run during planning. The parent acceptance task owns full native PG14–18, wheel, free-threaded, performance and aggregate acceptance; focused success does not close the charter.

## Acceptance Criteria

- [ ] R6, R7, R16: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] Fetch/close semantics are typed and retain real session ownership.
- [ ] Drop/cancel/early-exit cannot release a still-active streaming lease.
- [ ] Arrow/COPY consumers retain access to the concrete Rust engine independently of Python rows.
- [ ] Focused RED/GREEN evidence, relevant typing/lint and a clean task diff are recorded; exactly one signed local commit is linked.

## Notes & Discoveries

None.
