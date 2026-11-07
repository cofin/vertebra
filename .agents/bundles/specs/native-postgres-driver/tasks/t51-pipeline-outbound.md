---
type: Task
id: native-postgres-driver:t51
title: Single-Sync cold and warm pipeline submission
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t50
files:
- src/rs/vertebra-postgres/src/pipeline.rs
- src/rs/vertebra-postgres/src/session.rs
- src/rs/vertebra-postgres/src/extended.rs
- src/rs/vertebra-postgres/src/lib.rs
- src/rs/vertebra-postgres/tests/pipeline_wire.rs
tests:
- src/rs/vertebra-postgres/tests/pipeline_wire.rs
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
- R13
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
---

# Single-Sync cold and warm pipeline submission

## Objective

Encode and submit every pipeline operation in one synchronization group with one logical socket flush.

## Context

Read `../spec.md`, direct dependency worksheets and notes, `.agents/bundles/knowledge/domains/data/{database-drivers-and-dialers,query-pipeline-cdc-and-tenancy,broker-and-shm-leases}.md`, and applicable `.agents/skills/{pyo3-bridge,runtime-ipc}/SKILL.md`. New paths listed in this worksheet are intentional additions under the t01 scaffold; inspect existing parents and dependency APIs before creating them. This is one invocation and one signed conventional local commit on the same feature branch and in the complete-driver PR. Estimated 390 handwritten changed lines including tests; cap approximately400. Preserve unrelated edits. No external repository writes. If the contract/budget does not hold, route `/flow:revise native-postgres-driver` before changing scope. This task is pending and is not implementation approval.

## Steps

1. Write RED instrumented duplex/socket transcript tests for0/1/N operations, cold and warm statements, first-use DDL followed by dependent INSERT/SELECT, typed NULL hints, known/unknown OIDs, duplicate SQL, partial writes and backpressure with large responses. Count actual Sync frames and AsyncWrite::poll_flush completions separately; one flush does not imply one write/packet. Add an INSERT followed by each forbidden transaction-control alias/COPY, including leading comments and empty semicolons. Assert validation rejects before ANY operation Parse/Bind/Execute or data-batch flush, not just before the offending operation; separate context BEGIN/rollback control traffic from data-batch counters.

2. Implement public pipeline.rs::{PipelineOperation,PipelineBatch,PipelineLimits} and PostgresConnection::submit_pipeline(batch). Empty batches emit no frames. First validate the complete owned operation list through t50 pipeline_guard::validate_pipeline_commands, in original order, before encoding/emitting any operation frame or flushing the data batch. Reject even a later COMMIT/END/ROLLBACK/ABORT/BEGIN/START TRANSACTION/SAVEPOINT/RELEASE/PREPARE TRANSACTION/SET TRANSACTION/SET SESSION CHARACTERISTICS or COPY before an earlier INSERT can be sent. Preserve the first canonical client error with failing_index and immutable outcomes=() because ALL operations are unsubmitted. Never synthesize server Error/Skipped outcomes here. Only after every operation passes, for each cold op encode Parse+DescribeStatement+Bind(text parameters,all-text results)+DescribePortal+Execute; all N ops are encoded in order before exactly one trailing Sync and one logical flush. No metadata prefetch/Sync precedes the batch, including DDL-dependent names. Explicit type/OID hints feed Parse and typed NULLs.

3. Warm known statement/OID metadata uses per-parameter binary where supported and per-column binary/text selection, retaining explicit descriptions for correct demultiplexing. Cold text uses built-in known text handlers; undiscovered custom OIDs remain exact owned text. Snapshot TypeCatalog before send; never issue catalog queries from inside this batch.

4. Submit bounded frame segments with a single write owner while the same session owner services reads concurrently under write backpressure; retain bounded response buffers and do not write_all the entire batch before reading. Bound queued op count/SQL bytes/parameter bytes/result bytes. Rejection before send is clean; partial-send failure retires the connection. Do not append frontend Flush per statement.

5. t50 maps transaction-control SQL heads to canonical InterfaceError and COPY to NotSupportedError before writing. Reject non-extended replication/simple-query operation variants through existing NotSupportedError paths as well. The native guard runs before any future Python binding compiler and applies identically to direct Rust/FFI submissions; no client can bypass it by using the same pipeline owner or raw native API. Commit feat(postgres): submit cold and warm single-Sync pipelines.

## Verification

Run the focused commands after introducing the public behavior tests to capture RED, then again after implementation to capture GREEN. New API absence may be the initial compile RED; existing-defect tasks require a behavioral reproduction. Use independent server fixtures or scripted peers, not implementation text scans.

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test pipeline_wire
```

Run `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` and `git diff --check`. If Python tests load a changed extension, run `make dev` first, then `uv run --no-sync python -m mypy.stubtest vertebra._postgres` for changed public native surfaces. Record actual command/exit and assertion evidence; no command has run during planning. The parent acceptance task owns full native PG14–18, wheel, free-threaded, performance and aggregate acceptance; focused success does not close the charter.

## Acceptance Criteria

- [ ] R17: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] Nonempty cold/warm batches emit exactly one Sync and one logical flush.
- [ ] A later forbidden operation causes zero operation Parse/Bind/Execute frames and zero data-batch flushes for the entire owned batch; failing_index is correct and outcomes is the empty immutable tuple.
- [ ] DDL-dependent first-use operations are never described/executed outside the batch.
- [ ] Backpressured large requests/responses complete without unbounded memory or deadlock.
- [ ] Focused RED/GREEN evidence, relevant typing/lint and a clean task diff are recorded; exactly one signed local commit is linked.

## Notes & Discoveries

None.
