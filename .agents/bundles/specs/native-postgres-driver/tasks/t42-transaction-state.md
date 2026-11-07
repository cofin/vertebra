---
type: Task
id: "native-postgres-driver:t42"
title: "Pinned transaction and savepoint command state"
description: "Pinned transaction and savepoint command state for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:t41"
files:
  - src/rs/vertebra-postgres/src/transaction.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/tests/transaction_state.rs
tests:
  - src/rs/vertebra-postgres/tests/transaction_state.rs
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

# Pinned transaction and savepoint command state

## Objective

Track actual I/T/E state and logical ownership for explicit transactions and nested operation savepoints.

## Context

Read `../spec.md`, direct dependency worksheets and notes, `.agents/bundles/knowledge/domains/data/{database-drivers-and-dialers,query-pipeline-cdc-and-tenancy,broker-and-shm-leases}.md`, and applicable `.agents/skills/{pyo3-bridge,runtime-ipc}/SKILL.md`. New paths listed in this worksheet are intentional additions under the t01 scaffold; inspect existing parents and dependency APIs before creating them. This is one invocation and one signed conventional local commit on the same feature branch and in the complete-driver PR. Estimated 350 handwritten changed lines including tests; cap approximately400. Preserve unrelated edits. No external repository writes. If the contract/budget does not hold, route `/flow:revise native-postgres-driver` before changing scope. This task is pending and is not implementation approval.

## Steps

1. Write RED connection tests for BEGIN/COMMIT/ROLLBACK, failure after BEGIN, PostgreSQL COMMIT returning ROLLBACK tag in a failed transaction, nested savepoints, release/rollback-to ordering, ownership conflicts and disconnect during finish. Assert real frames and final server transaction status rather than a Python boolean.

2. Implement transaction.rs::{TransactionOwner,TransactionScope,TransactionDisposition} and PostgresConnection::{begin,commit,rollback,savepoint,release_savepoint,rollback_to_savepoint}. Generate internal savepoint identifiers from bounded numeric tokens, never caller SQL text. Validate server tags and ReadyForQuery transitions. Reject begin/commit by a different active scope owner; operations in E may only recover through the proper rollback path.

3. SessionHandle retains transaction owner/generation while direct or pooled connections are pinned. An operation guard holds exclusive session execution without holding a Rust mutex across Python conversion or GIL reacquisition. Nested library operations take a savepoint in an existing transaction; they never commit an ambient transaction. Do not silently recover a user failed transaction by acquiring a different physical session.

4. Commit observes actual command tag and maps unexpected rollback to an explicit transaction failure. Rollback resets logical failed state only after confirmed recovery or closed-socket disposition. Leave transaction settings and session GUC policy to pool reset; commit feat(postgres): enforce transaction scope ownership.

## Verification

Run the focused commands after introducing the public behavior tests to capture RED, then again after implementation to capture GREEN. New API absence may be the initial compile RED; existing-defect tasks require a behavioral reproduction. Use independent server fixtures or scripted peers, not implementation text scans.

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test transaction_state
```

Run `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` and `git diff --check`. If Python tests load a changed extension, run `make dev` first, then `uv run --no-sync python -m mypy.stubtest vertebra._postgres` for changed public native surfaces. Record actual command/exit and assertion evidence; no command has run during planning. The parent acceptance task owns full native PG14–18, wheel, free-threaded, performance and aggregate acceptance; focused success does not close the charter.

## Acceptance Criteria

- [ ] R13: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] Transaction state follows server I/T/E and command tags.
- [ ] Nested scopes use savepoints and never commit ambient transactions.
- [ ] Concurrent logical owners cannot execute on or release the same session.
- [ ] Focused RED/GREEN evidence, relevant typing/lint and a clean task diff are recorded; exactly one signed local commit is linked.

## Notes & Discoveries

None.
