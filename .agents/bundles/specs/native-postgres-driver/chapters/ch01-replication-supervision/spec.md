---
type: Spec
flow_id: native-postgres-driver
chapter_id: ch01-replication-supervision
title: "Chapter 01: Verified Replication Supervision and Resume"
description: "Build generation-fenced ReplicationSupervisor worker loops, monotonic LSN acknowledgements, and identity-verified connect_logical_verified reconnect policies."
status: stable
state: planned
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
- `ReplicationSupervisor` wraps `ReplicationSession` across reconnect generations, fencing stale leases and merging monotonic `acknowledge(lsn)` updates [^pg-stream] [^pg-feedback].
- `connect_logical_verified` re-probes server `system_identifier`, database OID, timeline, slot, and publication fingerprint before resuming from `max(start_lsn, confirmed_flush_lsn)` [^pg-repl-mod].
- Bounded jittered `ReconnectPolicy` retries frame-boundary disconnects and `SQLSTATE 55006` slot-in-use errors while failing immediately on missing slots/publications or identity drift.
<!-- truth: end -->

## 1. Specification, Context & Provenance

Single-generation `ReplicationSession` landed in `18baadcd` [^pg-stream]. This chapter adds multi-generation `ReplicationSupervisor` lifecycle management and verified resume in `src/rs/vertebra-postgres/src/replication/supervisor.rs`.

## 2. Requirements, Concrete Contracts & Architecture

- `ReplicationSupervisor`: `spawn(config, options, policy) -> Result<Self, PostgresError>`, `next(&self) -> Result<Option<SupervisedDelivery>, PostgresError>`, `acknowledge(&self, lsn: u64) -> Result<(), PostgresError>`, `shutdown(&self, timeout: Duration) -> Result<(), PostgresError>`, synchronous `cancel(&self)` on `Drop`.
- `ReconnectPolicy`: `max_attempts`, `initial_backoff`, `max_backoff`, jittered delay calculation, and transient (`08*`, `57P01`, `55006`) vs permanent (`42704`, identity mismatch, truncated frame) classification.
- `connect_logical_verified`: verifies `VerifiedReplicationIdentity` matches initial `system_identifier`, `database_oid`, `slot_name`, and `publication_fingerprint`, advancing resume LSN monotonically from acknowledged commits and server `confirmed_flush_lsn`.

## 3. Documentation & OKF Knowledge Plan

OKF updates are consolidated in `ch09` so `postgres-native-driver.md` and `postgres-limitations.md` are edited once after full replication and driver cutover.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R1` Generation-fenced supervisor | `t01` | `replication/{supervisor,mod}.rs` | `cargo test -p vertebra-postgres replication::` |
| `R1` Identity-verified resume & backoff | `t02` | `replication/{supervisor,mod}.rs` | `cargo test -p vertebra-postgres replication::` |

## 5. Implementation Plan

- [ ] [t01-replication-supervisor-worker-and-fencing.md](tasks/t01-replication-supervisor-worker-and-fencing.md) — Add ReplicationSupervisor worker loop, generation-fenced SupervisedDelivery leases, monotonic acknowledge(lsn), and synchronous Drop cancellation.
- [ ] [t02-verified-resume-and-reconnect-policy.md](tasks/t02-verified-resume-and-reconnect-policy.md) — Verify server/database/slot/publication identity across reconnects, enforce confirmed_flush_lsn floors, and classify transient vs permanent errors.

## 6. Current State & Remaining Plan Snapshot

Planned (`2` tasks). Fast offline Rust gate (`< 30s`).

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --features gcp replication:: && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets --features gcp -- -D warnings
```

[^pg-stream]: `src/rs/vertebra-postgres/src/replication/stream.rs::ReplicationSession` — Bounded single-generation CopyBoth session and delivery leases.
[^pg-feedback]: `src/rs/vertebra-postgres/src/replication/feedback.rs::ReplicationFeedback` — Commit-fenced StandbyStatusUpdate tracking.
[^pg-repl-mod]: `src/rs/vertebra-postgres/src/replication/mod.rs` — Replication connection identity probe and module exports.
