---
type: Task
id: "native-postgres-driver:t49"
title: "Native pool facades carrying real session leases"
description: "Native pool facades carrying real session leases for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:t48"
files:
  - src/rs/vertebra-postgres/src/python_pool.rs
  - src/rs/vertebra-postgres/src/sync_client.rs
  - src/rs/vertebra-postgres/src/async_client.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/lib.rs
  - src/py/vertebra/_postgres.pyi
  - src/py/tests/unit/db/test_postgres_native_pools.py
tests:
  - src/py/tests/unit/db/test_postgres_native_pools.py
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
  - driver
  - rust
---

# Native pool facades carrying real session leases

## Objective

Make Python pools acquire real concrete Rust leases and preserve pinned transaction ownership.

## Context

Read `../spec.md`, direct dependency worksheets and notes, `.agents/bundles/knowledge/domains/data/{database-drivers-and-dialers,query-pipeline-cdc-and-tenancy,broker-and-shm-leases}.md`, and applicable `.agents/skills/{pyo3-bridge,runtime-ipc}/SKILL.md`. New paths listed in this worksheet are intentional additions under the t01 scaffold; inspect existing parents and dependency APIs before creating them. This is one invocation and one signed conventional local commit on the same feature branch and in the complete-driver PR. Estimated 390 handwritten changed lines including tests; cap approximately400. Preserve unrelated edits. No external repository writes. If the contract/budget does not hold, route `/flow:revise native-postgres-driver` before changing scope. This task is pending and is not implementation approval.

## Steps

1. Write RED SyncPostgresPool/AsyncPostgresPool tests with max_size1 proving acquire saturation, query on the actual leased socket, transaction-pinned reuse, second waiter exclusion, exception/drop release, cancellation during acquire, pool close/drain timeout and reset failure replacement. Check backend PIDs, not object names or SHM-only counters. Constructor tests enumerate all three pooler/preparation fields and five bounds/timeouts, including invalid types/ranges and validate_configuration preparation-policy mismatches without I/O.

2. Expose immutable native SyncPostgresPool/AsyncPostgresPool with the complete connection constructor options in contracts.md plus pooler_mode:str="direct", protocol_level_pooler_cache:bool=False, statement_cache_size:int=256, min_size:int=1, max_size:int=10, acquire_timeout:float=30.0, reset_timeout:float=10.0 and close_timeout:float=30.0. Pool options are validated before network I/O. acquire()->SyncPostgresClient or Awaitable[AsyncPostgresClient] transfers a real PooledConnection into SessionHandle::from_pooled, never creates an unrelated new client/session. Reuse the already-owned PostgresConfig and existing foundation constructor validator for every user/database/host/port/password/Unix/TLS/PEM/channel-binding/timeout/application_name/GUC/keepalive/cloud/Admin/login field. No later adapter DSN/cutover bridge is a prerequisite, and no second parser is introduced. No public caller-supplied pool key overrides this identity. Add validate_configuration(**full_connection_options)->None, reusing constructor validation/canonical identity and raising InterfaceError on mismatch without I/O; it includes Unix/TLS/channel-binding/GUC/keepalive/cloud/Admin/login identity plus preparation policy. No caller can override comparison with its own key.

3. Extend SessionHandle storage with explicit direct-session or concrete pooled-lease ownership. Client.close() terminates a direct connection and resets/releases a pooled one; no subsequent method can reuse the closed handle. Commit/rollback ends a transaction but does not silently release a caller-retained client lease. Pool acquisition and return release the GIL and use the existing async completion ring. The high-level driver owns a pool and borrows transient clients per ordinary operation; only transaction/portal/pipeline contexts pin them. A raw client is deliberately one physical session until close.

4. Pool facade close drains under its configured deadline and surfaces canonical pool exhaustion/operational timeout errors. Cleanup/drop remains native and releases permits after reset/retirement. A cancelled acquire that wins the race after a Python Future is cancelled still returns its acquired lease safely, including completion publication failure.

5. Export pool classes and typed stubs in _postgres, without generic db engine dispatch or cross-cdylib trait pointers. Commit feat(postgres): expose Python pools with native lease ownership.

## Verification

Run the focused commands after introducing the public behavior tests to capture RED, then again after implementation to capture GREEN. New API absence may be the initial compile RED; existing-defect tasks require a behavioral reproduction. Use independent server fixtures or scripted peers, not implementation text scans.

```bash
make dev
uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_pools.py -q
uv run --no-sync python -m mypy.stubtest vertebra._postgres
```

Run `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` and `git diff --check`. If Python tests load a changed extension, run `make dev` first, then `uv run --no-sync python -m mypy.stubtest vertebra._postgres` for changed public native surfaces. Record actual command/exit and assertion evidence; no command has run during planning. The parent acceptance task owns full native PG14–18, wheel, free-threaded, performance and aggregate acceptance; focused success does not close the charter.

## Acceptance Criteria

- [ ] R6, R7, R14: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] Every Python acquired client owns the actual PooledConnection tested by server PID.
- [ ] Transactions, cancellation and close preserve exclusive lease/permit ownership.
- [ ] Pool errors and public stubs are canonical and typed.
- [ ] Focused RED/GREEN evidence, relevant typing/lint and a clean task diff are recorded; exactly one signed local commit is linked.

## Notes & Discoveries

None.
