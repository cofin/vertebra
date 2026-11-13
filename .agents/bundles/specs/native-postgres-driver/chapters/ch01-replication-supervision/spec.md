---
type: Spec
flow_id: native-postgres-driver
chapter_id: ch01-replication-supervision
title: "Chapter 01: Verified Replication Supervision and Resume"
description: "Build generation-fenced ReplicationSupervisor worker loops, monotonic LSN acknowledgements, and identity-verified connect_logical_verified reconnect policies."
status: stable
state: active
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
updated_at: "2026-11-13T00:25:00Z"
generated:
  by: flow-plan
  at: "2026-11-13T00:25:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-13T00:25:00Z"
    commit: 18baadcd
tags:
  - rust
  - postgres
  - replication
  - cdc
sources:
  - id: pg-stream
    resource: "src/rs/vertebra-postgres/src/replication/stream.rs::ReplicationSession"
    title: "Bounded single-generation CopyBoth session and delivery leases"
  - id: pg-feedback
    resource: "src/rs/vertebra-postgres/src/replication/feedback.rs::ReplicationFeedback"
    title: "Commit-fenced StandbyStatusUpdate tracking"
  - id: pg-repl-mod
    resource: src/rs/vertebra-postgres/src/replication/mod.rs
    title: "Replication connection identity probe and module exports"
---

# Chapter 01: Verified Replication Supervision and Resume

<!-- truth: start -->
- `ReplicationSupervisor` (`open`, `next_event`, `resume`, `acknowledge(generation, commit_end_lsn)`, `shutdown`, `cancel`, `is_closed`) wraps `ReplicationSession` across reconnect generations, emitting `ReplicationStreamEvent::{Control, Record}` via generation-fenced `ReplicationEventDelivery` leases (`event`, `publish`) [^pg-stream] [^pg-feedback].
- `PostgresReplicationConnection::connect_logical_verified(config, slot, publication, start_lsn, expected)` re-probes server `system_identifier`, database, timeline, slot, and publication fingerprint before resuming from `max(start_lsn, confirmed_flush_lsn)` [^pg-repl-mod].
- Bounded jittered backoff (`MAX_RAW_RECONNECT_ATTEMPTS = 8`, `raw_reconnect_delay(attempt, jitter_seed)` in `100ms..=5s`) retries frame-boundary disconnects and `SQLSTATE 55006` / active-slot teardown while failing immediately on missing slots/publications, auth failures, WAL gaps, or identity drift.
<!-- truth: end -->

## 1. Specification, Context & Provenance

Single-generation `ReplicationSession` landed in `18baadcd` [^pg-stream]. This chapter adds multi-generation `ReplicationSupervisor` lifecycle management and verified resume in `src/rs/vertebra-postgres/src/replication/supervisor.rs` and `src/rs/vertebra-postgres/src/replication/mod.rs` matching `contracts.md`.

## 2. Requirements, Concrete Contracts & Architecture

- `ReplicationSupervisor`: `open(config, slot, publication, start_lsn, queue_capacity, reconnect) -> Result<Self, PostgresError>`, `next_event(&self) -> Result<Option<ReplicationEventDelivery>, PostgresError>`, `resume(&self) -> PostgresReplicationResume`, `acknowledge(&self, generation: u64, commit_end_lsn: u64) -> Result<u64, PostgresError>`, `shutdown(&self) -> Result<(), PostgresError>`, `is_closed(&self) -> bool`, and synchronous `cancel(&self)` invoked on `Drop`.
- `ReplicationStreamEvent`, `ReplicationEventDelivery`, `PostgresReplicationResume`, `MAX_RAW_RECONNECT_ATTEMPTS = 8`, and `raw_reconnect_delay(attempt: u32, jitter_seed: u64) -> Duration`: emits `Control` before `Record`s per generation, fences stale generation leases on `publish`/`acknowledge`/`Drop`, and classifies transient (`08*` except `08P01`, `57P*`, `53300`, `55006` / slot active during teardown) vs permanent (`42704`, auth, WAL gap, identity mismatch, truncated frame) errors.
- `PostgresReplicationConnection::connect_logical_verified(config, slot, publication, start_lsn, expected)`: verifies `VerifiedReplicationIdentity` matches initial `system_identifier`, `database`, `slot`, and `publication_fingerprint` (without timeline regression), advancing resume LSN monotonically from acknowledged commits and server `confirmed_flush_lsn`.

## 3. Documentation & OKF Knowledge Plan

OKF updates are consolidated in `ch09` so `postgres-native-driver.md` and `postgres-limitations.md` are edited once after full replication and driver cutover.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R1` Generation-fenced supervisor | `t01` | `replication/{supervisor,mod}.rs` | `cargo test -p vertebra-postgres replication::` |
| `R1` Identity-verified resume & backoff | `t02` | `replication/{supervisor,mod}.rs` | `cargo test -p vertebra-postgres replication::` |

## 5. Implementation Plan

- [x] [7d50fd42] [t01-replication-supervisor-worker-and-fencing.md](tasks/t01-replication-supervisor-worker-and-fencing.md) — Add ReplicationSupervisor worker loop, generation-fenced ReplicationEventDelivery leases, monotonic acknowledge(generation, commit_end_lsn), and synchronous Drop cancellation.
- [x] [7d50fd42] [t02-verified-resume-and-reconnect-policy.md](tasks/t02-verified-resume-and-reconnect-policy.md) — Verify server/database/slot/publication identity across reconnects via connect_logical_verified, enforce confirmed_flush_lsn floors, and classify transient vs permanent errors.

## 6. Current State & Remaining Plan Snapshot

Implemented (`2/2` tasks at `7d50fd42`, `56` wire tests + Clippy clean), awaiting Zero-Context Chapter Review closeout stamp.

```bash
PYO3_PYTHON="$(pwd)/.venv/bin/python" PYTHONPATH="$(pwd)/src/py:$(pwd)/.venv/lib/python3.12/site-packages" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --locked replication:: && PYO3_PYTHON="$(pwd)/.venv/bin/python" PYTHONPATH="$(pwd)/src/py:$(pwd)/.venv/lib/python3.12/site-packages" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --locked --test postgres_replication_wire_tests && PYO3_PYTHON="$(pwd)/.venv/bin/python" cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
```

[^pg-stream]: `src/rs/vertebra-postgres/src/replication/stream.rs::ReplicationSession` — Bounded single-generation CopyBoth session and delivery leases.
[^pg-feedback]: `src/rs/vertebra-postgres/src/replication/feedback.rs::ReplicationFeedback` — Commit-fenced StandbyStatusUpdate tracking.
[^pg-repl-mod]: `src/rs/vertebra-postgres/src/replication/mod.rs` — Replication connection identity probe and module exports.
