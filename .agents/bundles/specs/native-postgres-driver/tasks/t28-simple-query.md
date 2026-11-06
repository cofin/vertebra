---
type: Task
id: native-postgres-driver:t28
title: Simple query completion and canonical SQLSTATE errors
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t27
files:
- src/rs/vertebra-postgres/src/query.rs
- src/rs/vertebra-postgres/src/session.rs
- src/rs/vertebra-postgres/src/errors.rs
- src/rs/vertebra-postgres/src/lib.rs
- src/rs/vertebra-postgres/tests/query_protocol.rs
tests:
- src/rs/vertebra-postgres/tests/query_protocol.rs
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
- R10
- R12
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
---

# Simple query completion and canonical SQLSTATE errors

## Objective

Run real simple-query exchanges, retain every result set and drain recoverable errors through ReadyForQuery.

## Context

Read `../spec.md`, direct dependency worksheets and notes, `.agents/bundles/knowledge/domains/data/{database-drivers-and-dialers,query-pipeline-cdc-and-tenancy,broker-and-shm-leases}.md`, and applicable `.agents/skills/{pyo3-bridge,runtime-ipc}/SKILL.md`. New paths listed in this worksheet are intentional additions under the t01 scaffold; inspect existing parents and dependency APIs before creating them. This is one invocation and one signed conventional local commit on the same feature branch and in the complete-driver PR. Estimated 390 handwritten changed lines including tests; cap approximately400. Preserve unrelated edits. No external repository writes. If the contract/budget does not hold, route `/flow:revise native-postgres-driver` before changing scope. This task is pending and is not implementation approval.

## Steps

1. Write RED loopback-script tests through public PostgresConnection::simple_query for SELECT, UPDATE/INSERT/DELETE/MERGE/COPY command counts, DDL count zero, empty query, multiple statements with heterogeneous schemas, notice/status interleaving, and a middle-statement error. Reuse foundation scripted-server fixture; assert exact emitted Query frame and final I/T/E status.

2. Implement query.rs::SimpleQueryCollector and PostgresConnection::simple_query(sql)->Result<Vec<QueryResult>,PostgresError>. Use upstream frontend::query and foundation protocol::read_message. Correlate RowDescription/DataRow/CommandComplete/EmptyQueryResponse; own rows before advancing. Until codec installation decode text cells to PgValue::Text and preserve NULL separately. Validate row width, field length, UTF8 and legal frame ordering. Parse the affected count only from documented command tags, with checked u64 conversion.

3. Extend foundation errors.rs::PostgresError::Server with a bounded structured ServerError preserving SQLSTATE, severity, message and diagnostic fields. Map canonical shared Python subclasses later via errors.rs::postgres_error_to_py, never define duplicate exception hierarchies. SQLSTATE class22 maps DataError,23 IntegrityError,42 ProgrammingError,08 ConnectionFailure,40001/40P01 SerializationFailure,57014 QueryCancelledError; retain unknown codes as DatabaseError. Transient flags reflect actual retry category; no implicit execution replay.

4. On ErrorResponse retain the first error, continue consuming asynchronous status/notice/notification frames and drain exactly through ReadyForQuery. Validate transaction status and record it even on error. EOF, malformed frames, impossible ordering and drain deadline permanently break the connection. A second ordinary query succeeds after a recoverable idle-state error; failed ambient transactions stay failed until rollback. Route notification frames through one central session dispatch hook for later event tasks.

5. Expose safe connection status and result-set consumption to later extended/pipeline/COPY code. Do not interpret CommandComplete as a successful commit. Commit feat(postgres): execute simple queries and preserve SQLSTATE failures.

## Verification

Run the focused commands after introducing the public behavior tests to capture RED, then again after implementation to capture GREEN. New API absence may be the initial compile RED; existing-defect tasks require a behavioral reproduction. Use independent server fixtures or scripted peers, not implementation text scans.

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test query_protocol
```

Run `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` and `git diff --check`. If Python tests load a changed extension, run `make dev` first, then `uv run --no-sync python -m mypy.stubtest vertebra._postgres` for changed public native surfaces. Record actual command/exit and assertion evidence; no command has run during planning. The parent acceptance task owns full native PG14–18, wheel, free-threaded, performance and aggregate acceptance; focused success does not close the charter.

## Acceptance Criteria

- [ ] R10, R12: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] Multi-result order/schema and affected counts survive native protocol exchange.
- [ ] Recoverable server errors drain to ReadyForQuery; protocol errors retire the socket.
- [ ] Canonical SQLSTATE data remains available without duplicate Python classes.
- [ ] Focused RED/GREEN evidence, relevant typing/lint and a clean task diff are recorded; exactly one signed local commit is linked.

## Notes & Discoveries

None.
