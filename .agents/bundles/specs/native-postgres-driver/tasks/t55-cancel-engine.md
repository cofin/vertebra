---
type: Task
id: native-postgres-driver:t55
title: Protocol cancellation and delayed-request retirement
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t54
files:
- src/rs/vertebra-postgres/src/cancel.rs
- src/rs/vertebra-postgres/src/session.rs
- src/rs/vertebra-postgres/src/pool.rs
- src/rs/vertebra-postgres/src/transaction.rs
- src/rs/vertebra-postgres/src/lib.rs
- src/rs/vertebra-postgres/tests/cancellation_protocol.rs
tests:
- src/rs/vertebra-postgres/tests/cancellation_protocol.rs
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
- R18
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
---

# Protocol cancellation and delayed-request retirement

## Objective

Send real protocol3 CancelRequest with generation fencing and prevent delayed cancellation from reaching later work.

## Context

Read `../spec.md`, direct dependency worksheets and notes, `.agents/bundles/knowledge/domains/data/{database-drivers-and-dialers,query-pipeline-cdc-and-tenancy,broker-and-shm-leases}.md`, and applicable `.agents/skills/{pyo3-bridge,runtime-ipc}/SKILL.md`. New paths listed in this worksheet are intentional additions under the t01 scaffold; inspect existing parents and dependency APIs before creating them. This is one invocation and one signed conventional local commit on the same feature branch and in the complete-driver PR. Estimated 390 handwritten changed lines including tests; cap approximately400. Preserve unrelated edits. No external repository writes. If the contract/budget does not hold, route `/flow:revise native-postgres-driver` before changing scope. This task is pending and is not implementation approval.

## Steps

1. Write RED coordinated scripted-server tests for idle cancel(no packet), active query, query finishing before cancel connect/send, cancel socket failure, original query EOF, cancel timeout, missing BackendKeyData, close racing cancel and repeated cancel calls. Hold the cancel connection behind a barrier while completing the query and trying a second pool borrower.

2. Implement cancel.rs::{PostgresCancelHandle,OperationGeneration,CancelDisposition} from Rust-only BackendKeyData and validated original endpoint/TLS/dialer configuration. CancelRequest uses upstream frontend::cancel_request(pid,key), fresh transport and no Startup/password authentication. Follow the supported endpoint cancellation transport route, including cloud outer mTLS/metadata exchange and PostgreSQL SSL negotiation where applicable; do not reuse the busy query socket or expose backend secrets to Python.

3. Only the active operation generation can claim cancellation before opening/sending. Once a CancelRequest may have been sent, mark that physical session retire-on-finish even if the query already completed; a fresh cancel socket has no success acknowledgement. Serialize send/claim with operation completion so the next operation cannot start on the old session while a delayed request exists.

4. Retain the original query reader and drain to ReadyForQuery under cleanup deadline; expect canonical SQLSTATE57014 when cancellation wins but allow completion when it loses. Retire after drain or close immediately on uncertainty/timeouts. Mark an ambient transaction logically failed and require explicit rollback to clear it; never silently reacquire mid-transaction. Outside finished scopes a later call can obtain a replacement pool session.

5. Implement shared Connection::cancel_handle/CancelHandle trait via the concrete native owner and keep cleanup independent of the caller future lifetime. Commit feat(postgres): cancel active queries and retire raced sessions.

## Verification

Run the focused commands after introducing the public behavior tests to capture RED, then again after implementation to capture GREEN. New API absence may be the initial compile RED; existing-defect tasks require a behavioral reproduction. Use independent server fixtures or scripted peers, not implementation text scans.

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test cancellation_protocol
```

Run `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` and `git diff --check`. If Python tests load a changed extension, run `make dev` first, then `uv run --no-sync python -m mypy.stubtest vertebra._postgres` for changed public native surfaces. Record actual command/exit and assertion evidence; no command has run during planning. The parent acceptance task owns full native PG14–18, wheel, free-threaded, performance and aggregate acceptance; focused success does not close the charter.

## Acceptance Criteria

- [ ] R18: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] Active cancellation sends the exact BackendKeyData CancelRequest on a separate connection.
- [ ] No delayed cancel can hit the next operation/borrower because cancel-sent sessions retire.
- [ ] Drain/transaction-failure/pool replacement behavior is explicit and leak-free.
- [ ] Focused RED/GREEN evidence, relevant typing/lint and a clean task diff are recorded; exactly one signed local commit is linked.

## Notes & Discoveries

None.
