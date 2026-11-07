---
type: Task
id: native-postgres-driver:t52
title: Ordered pipeline results errors and skipped outcomes
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t51
files:
- src/rs/vertebra-postgres/src/pipeline.rs
- src/rs/vertebra-postgres/src/query.rs
- src/rs/vertebra-postgres/src/errors.rs
- src/rs/vertebra-postgres/tests/pipeline_outcomes.rs
tests:
- src/rs/vertebra-postgres/tests/pipeline_outcomes.rs
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
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
---

# Ordered pipeline results errors and skipped outcomes

## Objective

Demultiplex cold/warm frame streams and retain the first canonical failure plus explicitly skipped operations.

## Context

Read `../spec.md`, direct dependency worksheets and notes, `.agents/bundles/knowledge/domains/data/{database-drivers-and-dialers,query-pipeline-cdc-and-tenancy,broker-and-shm-leases}.md`, and applicable `.agents/skills/{pyo3-bridge,runtime-ipc}/SKILL.md`. New paths listed in this worksheet are intentional additions under the t01 scaffold; inspect existing parents and dependency APIs before creating them. This is one invocation and one signed conventional local commit on the same feature branch and in the complete-driver PR. Estimated 390 handwritten changed lines including tests; cap approximately400. Preserve unrelated edits. No external repository writes. If the contract/budget does not hold, route `/flow:revise native-postgres-driver` before changing scope. This task is pending and is not implementation approval.

## Steps

1. Write RED golden scripts for heterogeneous row/DML/empty results, ParameterDescription/RowDescription/NoData sequencing, Bind/Parse failure before Execute, execute failure in the middle, async notices/status/notify, malformed ordering and ReadyForQuery I/T/E. Earlier complete results precede the failing op; all later server-discarded operations are skipped without fabricated ErrorResponse frames.

2. Implement public PipelineOutcome::{Result(QueryResult),Error(ServerError),Skipped{failed_index}} and PipelineFailure{first_error,outcomes,transaction_status}. Track each operation parse/bind/describe/execute phase so errors before execute belong to the correct operation. Ignore/discriminate statement Describe text formats and use portal Describe actual formats for rows. Retain command tags/counts and exact ordered result metadata.

3. After the first ErrorResponse synthesize skipped outcomes for remaining operations, continue bounded drain until the batch Sync ReadyForQuery and record I/T/E. Do not expect one error/result frame per discarded op. Decode failures retain the first relevant error while draining; protocol/drain errors retire the session. Never return Python exceptions as successful row data.

4. PostgresConnection::pipeline_results(batch)->Result<Vec<PipelineOutcome>,PipelineFailure> performs no commit, release-savepoint or pool return. Expose first canonical error and all ordered outcomes to later Python exception attrs. A transaction outcome is not inferred from earlier CommandComplete. Commit feat(postgres): preserve ordered pipeline failures and recovery state.

## Verification

Run the focused commands after introducing the public behavior tests to capture RED, then again after implementation to capture GREEN. New API absence may be the initial compile RED; existing-defect tasks require a behavioral reproduction. Use independent server fixtures or scripted peers, not implementation text scans.

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test pipeline_outcomes
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test pipeline_wire
```

Run `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` and `git diff --check`. If Python tests load a changed extension, run `make dev` first, then `uv run --no-sync python -m mypy.stubtest vertebra._postgres` for changed public native surfaces. Record actual command/exit and assertion evidence; no command has run during planning. The parent acceptance task owns full native PG14–18, wheel, free-threaded, performance and aggregate acceptance; focused success does not close the charter.

## Acceptance Criteria

- [ ] R17: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] Every submitted op has one result/error/skipped outcome in original order.
- [ ] First server error drains through exactly the trailing Sync and retains real transaction state.
- [ ] Result collection does not commit or release its owning scope.
- [ ] Focused RED/GREEN evidence, relevant typing/lint and a clean task diff are recorded; exactly one signed local commit is linked.

## Notes & Discoveries

None.
