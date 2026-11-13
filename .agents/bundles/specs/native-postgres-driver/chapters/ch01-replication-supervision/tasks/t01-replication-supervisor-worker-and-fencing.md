---
type: Task
id: "native-postgres-driver:ch01-replication-supervision:t01"
flow_id: native-postgres-driver
chapter_id: ch01-replication-supervision
title: "Implement generation-fenced ReplicationSupervisor and Drop cancel"
description: "Add ReplicationSupervisor worker loop, generation-fenced SupervisedDelivery leases, monotonic acknowledge(lsn), and synchronous Drop cancellation."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: []
files: 
  - src/rs/vertebra-postgres/src/replication/supervisor.rs
  - src/rs/vertebra-postgres/src/replication/mod.rs
tests: 
  - src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
read_set:
  - src/rs/vertebra-postgres/src/replication/stream.rs
  - src/rs/vertebra-postgres/src/replication/feedback.rs
  - src/rs/vertebra-postgres/src/replication/mod.rs
  - .agents/bundles/specs/native-postgres-driver/contracts.md
estimated_loc: 220
tags:
  - rust
  - postgres
  - replication
sources:
  - id: pg-stream
    resource: "src/rs/vertebra-postgres/src/replication/stream.rs::ReplicationSession"
    title: "Single-generation session and delivery lease API"
---

# Task `t01`: Implement generation-fenced ReplicationSupervisor and Drop cancel

## Objective & Scope Boundary

Implement `ReplicationSupervisor` and `SupervisedDelivery` in `supervisor.rs` per `../spec.md` §2 [^pg-stream]. Out of scope: reconnect identity verification (`t02`) and PyO3 bindings (`ch02`).

## Bounded Read Manifest

- `replication/stream.rs` — `ReplicationSession` and `ReplicationDelivery`.
- `replication/feedback.rs` — commit-fenced LSN state.
- `replication/mod.rs` — module exports.

## Contracts & Edge Cases

- Fence every `SupervisedDelivery` by `(generation: u64, commit_end_lsn: Option<u64>)` so stale acknowledgements after a generation turnover never corrupt active session state.
- Dropping an unconsumed `SupervisedDelivery` restores the record ahead of the FIFO queue when the generation is still live; `Drop` on `ReplicationSupervisor` invokes synchronous `cancel()` without requiring a Tokio runtime.

## Test Matrix

- Scripted `CopyBoth` peer tests covering single-receiver reservation, dropped-lease FIFO restoration, monotonic `acknowledge(lsn)` forwarding, and synchronous `Drop` worker abort.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --features gcp replication::
```

## Acceptance Criteria

- [ ] `ReplicationSupervisor` fences stale generation acks and restores dropped leases cleanly.
- [ ] Synchronous `Drop` aborts background tasks without panicking outside Tokio.
- [ ] Zero regressions in `ch01-replication-supervision` fast verification gate.

[^pg-stream]: `src/rs/vertebra-postgres/src/replication/stream.rs::ReplicationSession` — Single-generation session and delivery lease API.
