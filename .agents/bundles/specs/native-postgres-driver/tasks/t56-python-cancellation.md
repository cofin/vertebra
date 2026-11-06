---
type: Task
id: native-postgres-driver:t56
title: Explicit and asyncio cancellation with native cleanup
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t55
files:
- src/rs/vertebra-db/src/py_async.rs
- src/rs/vertebra-db/Cargo.toml
- src/rs/vertebra-postgres/src/sync_client.rs
- src/rs/vertebra-postgres/src/async_client.rs
- src/rs/vertebra-postgres/src/python_pipeline.rs
- src/rs/vertebra-postgres/src/lib.rs
- src/py/vertebra/_postgres.pyi
- src/rs/vertebra-db/tests/py_async_cancel_tests.rs
- src/py/tests/unit/db/test_postgres_native_cancel.py
tests:
- src/rs/vertebra-db/tests/py_async_cancel_tests.rs
- src/py/tests/unit/db/test_postgres_native_cancel.py
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
- R7
- R18
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
---

# Explicit and asyncio cancellation with native cleanup

## Objective

Wire driver/client cancellation and cancelled Python Futures to protocol cancellation without aborting required socket cleanup.

## Context

Read `../spec.md`, direct dependency worksheets and notes, `.agents/bundles/knowledge/domains/data/{database-drivers-and-dialers,query-pipeline-cdc-and-tenancy,broker-and-shm-leases}.md`, and applicable `.agents/skills/{pyo3-bridge,runtime-ipc}/SKILL.md`. New paths listed in this worksheet are intentional additions under the t01 scaffold; inspect existing parents and dependency APIs before creating them. This is one invocation and one signed conventional local commit on the same feature branch and in the complete-driver PR. Estimated 390 handwritten changed lines including tests; cap approximately400. Preserve unrelated edits. No external repository writes. If the contract/budget does not hold, route `/flow:revise native-postgres-driver` before changing scope. This task is pending and is not implementation approval.

## Steps

1. Write RED sync/async client tests for explicit cancel of active pg_sleep, asyncio.wait_for timeout, Future.cancel during write/read/queued operation, cancelled pipeline/portal, cancellation after result publication and repeated cancellation. The same Python driver remains usable after outside-transaction replacement; a cancelled ambient transaction rejects queries until explicit rollback.

2. Add shared py_async.rs::spawn_rust_db_task_with_attr_and_cancel with an owned cancellation callback/action while preserving existing spawn_rust_db_task_with_attr default AbortHandle behavior for other engines. Registration is protected by the foundation abort-on-setup-failure guard. PostgreSQL supplies a nonblocking action that signals the active OperationGeneration; after publication, Python cancellation does not abort the cleanup-owning worker.

3. Extend SyncPostgresClient.cancel()->None and AsyncPostgresClient.cancel()->Awaitable[None] with out-of-band handles that do not need the busy session execution lock. Explicit successful wire cancellation surfaces canonical QueryCancelledError on the query; Python Future cancellation preserves asyncio.CancelledError while native drain/retirement continues. Timeout/cancel before submission removes queued work without sending a stale request.

4. Keep a native cleanup owner alive until query drain/retirement and permit return even when the Python Future, loop or client is dropped. Mark active transaction failed and release only after rollback/close acknowledgement. Pool replacement after cancellation must prove a fresh backend PID; reuse of the same Python facade does not imply reuse of the old physical socket.

5. Declare the py_async_cancel_tests Cargo test target with required-features=["pyo3"] and require named tests cancel_callback_runs_without_aborting_cleanup, legacy_abort_cancels_worker and registration_failure_drops_worker. Do not hide the complete file behind cfg(feature="pyo3") so a default-feature run reports a misleading empty success. Prove unchanged abort-on-cancel behavior for other native database users and callback setup failure cleanup. Update stubs and commit feat(postgres): route asyncio cancellation through protocol cleanup.

## Verification

Run the focused commands after introducing the public behavior tests to capture RED, then again after implementation to capture GREEN. New API absence may be the initial compile RED; existing-defect tasks require a behavioral reproduction. Use independent server fixtures or scripted peers, not implementation text scans.

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --features pyo3 --test py_async_cancel_tests -- --list
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --features pyo3 --test py_async_cancel_tests
make dev
uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_cancel.py -q
uv run --no-sync python -m mypy.stubtest vertebra._postgres
```

Run `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` and `git diff --check`. If Python tests load a changed extension, run `make dev` first, then `uv run --no-sync python -m mypy.stubtest vertebra._postgres` for changed public native surfaces. Record actual command/exit and assertion evidence; no command has run during planning. The parent acceptance task owns full native PG14–18, wheel, free-threaded, performance and aggregate acceptance; focused success does not close the charter.

## Acceptance Criteria

- [ ] R7, R18: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] The --features pyo3 -- --list evidence contains all named tests, and execution reports at least 3 tests run; zero-test/cfg-empty success does not satisfy this task.
- [ ] Explicit/asyncio cancellation reaches the protocol rather than only AbortHandle.
- [ ] Cleanup finishes without a live Python Future and capacity/leases are recovered.
- [ ] Ambient rollback and outside-transaction replacement preserve future driver usability.
- [ ] Focused RED/GREEN evidence, relevant typing/lint and a clean task diff are recorded; exactly one signed local commit is linked.

## Notes & Discoveries

None.
