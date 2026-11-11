---
type: Task
id: "native-postgres-driver:t43"
title: "Bounded portals that retain session ownership"
description: "Bounded portals that retain session ownership for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:t42"
files:
  - src/rs/vertebra-postgres/src/portal.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/extended.rs
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/tests/portal_protocol.rs
tests:
  - src/rs/vertebra-postgres/tests/portal_protocol.rs
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

# Bounded portals that retain session ownership

## Objective

Expose a concrete Portal with row-limited Execute, suspension/resumption and deterministic transaction/lease ownership.

## Context

Read `../spec.md`, direct dependency worksheets and notes, `.agents/bundles/knowledge/domains/data/{database-drivers-and-dialers,query-pipeline-cdc-and-tenancy,broker-and-shm-leases}.md`, and applicable `.agents/skills/{pyo3-bridge,runtime-ipc}/SKILL.md`. New paths listed in this worksheet are intentional additions under the t01 scaffold; inspect existing parents and dependency APIs before creating them. This is one invocation and one signed conventional local commit on the same feature branch and in the complete-driver PR. Estimated 390 handwritten changed lines including tests; cap approximately400. Preserve unrelated edits. No external repository writes. If the contract/budget does not hold, route `/flow:revise native-postgres-driver` before changing scope. This task is pending and is not implementation approval.

## Steps

1. Write RED scripted portal tests for max_rows1/2, PortalSuspended versus CommandComplete, empty final chunk, close before exhaustion, decode/server errors, interruption, stale token, and concurrent ordinary query refusal. Verify each fetch preserves metadata and never buffers the complete result. Pause after one row before PortalSuspended, resume the same response later, and assert no second Execute appears before suspension.

2. Implement public portal.rs::Portal retaining Arc<SessionHandle>, a unique portal name, connection/transaction generation and a TransactionScope. SessionHandle::open_portal(sql,parameters,parameter_oids,batch_size)->Portal owns the operation token; the internal PostgresConnection creates named Bind+DescribePortal+Execute. Portal::fetch(max_rows)->QueryResult advances the same owner; Portal::close() closes/drains it. max_rows must be positive and fit the wire signed row limit. Expose Rust Portal::start_fetch(max_rows) and Portal::next_event()->PortalEvent::{Row(Vec<PgValue>),Suspended,Complete{command_tag,affected_rows}}. next_event retains a pending Execute response between calls; start_fetch refuses another Execute until suspension/completion. The QueryResult fetch convenience aggregates these events under both row and configured result-byte limits. Arrow can stop/resume after a row without first consuming the whole Execute result.

3. When opened outside a transaction, BEGIN an owned streaming transaction before creating the portal; Sync between fetches must leave T so the portal remains alive. Under an ambient transaction use a savepoint and retain the same session. Preserve the transaction until exhaustion/close cleanup; an operation error rolls back only the portal scope. Lease return and other client operations remain blocked until portal ownership is released.

4. Handle CloseComplete/PortalSuspended ordering, interleaved asynchronous frames and bounded drain. Explicit close performs rollback for early-abandoned owned streams or release on successful exhaustion; ambient scope behavior never commits caller work. Drop schedules bounded native cleanup without awaiting under the GIL; failed cleanup retires the socket. Expose portal status for future Arrow streaming, not a second socket reader.

5. Export concrete Portal from rlib and commit feat(postgres): stream bounded results through owned portals.

## Verification

Run the focused commands after introducing the public behavior tests to capture RED, then again after implementation to capture GREEN. New API absence may be the initial compile RED; existing-defect tasks require a behavioral reproduction. Use independent server fixtures or scripted peers, not implementation text scans.

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test portal_protocol
```

Run `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` and `git diff --check`. If Python tests load a changed extension, run `make dev` first, then `uv run --no-sync python -m mypy.stubtest vertebra._postgres` for changed public native surfaces. Record actual command/exit and assertion evidence; no command has run during planning. The parent acceptance task owns full native PG14–18, wheel, free-threaded, performance and aggregate acceptance; focused success does not close the charter.

## Acceptance Criteria

- [ ] R16: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] Fetch row limits bound memory and preserve one live portal/physical session.
- [ ] Portals survive intermediate Sync only under a retained transaction scope.
- [ ] Early close/drop/error cannot leak a transaction or return a busy lease.
- [ ] Focused RED/GREEN evidence, relevant typing/lint and a clean task diff are recorded; exactly one signed local commit is linked.

## Notes & Discoveries

None.
