---
type: Task
id: "native-postgres-driver:ch01-replication-supervision:t01"
flow_id: native-postgres-driver
chapter_id: ch01-replication-supervision
title: "Implement generation-fenced ReplicationSupervisor and Drop cancel"
description: "Add ReplicationSupervisor worker loop, generation-fenced ReplicationEventDelivery leases, monotonic acknowledge(generation, commit_end_lsn), and synchronous Drop cancellation."
status: stable
state: completed
commit: 7d50fd42
last_verified_commit: 7d50fd42
last_verified_at: "2026-11-13T01:52:00Z"
verification_evidence: "cargo test -p vertebra-postgres --test postgres_replication_wire_tests (56 passed; 0 failed) + cargo clippy -p vertebra-postgres --all-targets -- -D warnings (exit 0)"
claimed_by: null
claimed_at: null
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

Implement `ReplicationSupervisor`, `PostgresReplicationResume`, `ReplicationStreamEvent`, and `ReplicationEventDelivery` in `supervisor.rs` (re-exported from `replication/mod.rs`) per `../../contracts.md` and `../spec.md` §2 [^pg-stream]. Out of scope: reconnect identity verification (`t02`) and PyO3 bindings (`ch02`).

## Bounded Read Manifest

- `replication/stream.rs` — `ReplicationSession` and `ReplicationDelivery`.
- `replication/feedback.rs` — commit-fenced LSN state.
- `replication/mod.rs` — module exports.
- `contracts.md` — exact Rust signatures (`ReplicationSupervisor::open`, `next_event`, `resume`, `acknowledge`, `shutdown`, `cancel`, `is_closed`, `ReplicationEventDelivery::{event, publish}`).

## Contracts & Edge Cases

- Validate `PoolerMode::{Direct, Session}` and `queue_capacity` in `1..=1024` (`InterfaceError` before dialing), emit `ReplicationStreamEvent::Control` before `Record`s per generation, and fence every `ReplicationEventDelivery` by `generation: u64` so stale `publish`/`acknowledge` calls fail cleanly after generation turnover.
- Dropping an unconsumed `ReplicationEventDelivery` restores the event/record ahead of the FIFO stream when the generation is still live; `Drop` on `ReplicationSupervisor` invokes synchronous `cancel()` without requiring an ambient Tokio runtime.

## Test Matrix

- Scripted `CopyBoth` peer tests covering single-receiver reservation, dropped-lease FIFO restoration, monotonic `acknowledge(lsn)` forwarding, and synchronous `Drop` worker abort.

## Verification

```bash
PYO3_PYTHON="$(pwd)/.venv/bin/python" PYTHONPATH="$(pwd)/src/py:$(pwd)/.venv/lib/python3.12/site-packages" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --locked replication::
```

## Acceptance Criteria

- [x] `ReplicationSupervisor` fences stale generation acks and restores dropped leases cleanly.
- [x] Synchronous `Drop` aborts background tasks without panicking outside Tokio.
- [x] Zero regressions in `ch01-replication-supervision` fast verification gate.

[^pg-stream]: `src/rs/vertebra-postgres/src/replication/stream.rs::ReplicationSession` — Single-generation session and delivery lease API.
