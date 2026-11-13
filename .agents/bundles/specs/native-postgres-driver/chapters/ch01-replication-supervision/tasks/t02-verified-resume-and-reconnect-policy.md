---
type: Task
id: "native-postgres-driver:ch01-replication-supervision:t02"
flow_id: native-postgres-driver
chapter_id: ch01-replication-supervision
title: "Add connect_logical_verified identity checks and ReconnectPolicy"
description: "Verify server/database/slot/publication identity across reconnects, enforce confirmed_flush_lsn floors, and classify transient vs permanent errors."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: 
  - "native-postgres-driver:ch01:t01"
files: 
  - src/rs/vertebra-postgres/src/replication/supervisor.rs
  - src/rs/vertebra-postgres/src/replication/mod.rs
tests: 
  - src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
read_set:
  - src/rs/vertebra-postgres/src/replication/supervisor.rs
  - src/rs/vertebra-postgres/src/replication/mod.rs
  - src/rs/vertebra-postgres/src/errors.rs
  - src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
estimated_loc: 205
tags:
  - rust
  - postgres
  - replication
sources:
  - id: pg-repl-mod
    resource: src/rs/vertebra-postgres/src/replication/mod.rs
    title: "probe_identity and connect_logical primitives"
---

# Task `t02`: Add connect_logical_verified identity checks and ReconnectPolicy

## Objective & Scope Boundary

Implement `connect_logical_verified` and `ReconnectPolicy` inside `supervisor.rs` per `../spec.md` §2 [^pg-repl-mod].

## Bounded Read Manifest

- `replication/supervisor.rs` — supervisor loop from `t01`.
- `replication/mod.rs` — `probe_identity` and `VerifiedReplicationIdentity`.
- `errors.rs` — SQLSTATE error classification.

## Contracts & Edge Cases

- Reject resume immediately with `InterfaceError` / `ProgrammingError` if `system_identifier`, `database_oid`, `slot_name`, or `publication_fingerprint` changed, or if timeline regressed.
- Retry frame-boundary EOF and `SQLSTATE 55006` (slot active during backend teardown) with bounded jittered backoff; fail immediately on mid-frame truncation or missing slot (`42704`).

## Test Matrix

- Parameterized Rust wire tests for frame-boundary reconnect, `55006` slot-in-use retry, `confirmed_flush_lsn` floor advancement, and fail-closed `system_identifier`/publication mismatch.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --features gcp --test postgres_replication_wire_tests && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets --features gcp -- -D warnings
```

## Acceptance Criteria

- [ ] Reconnect resumes without duplicate acknowledged commits or silent identity drift.
- [ ] Clippy `--all-targets --features gcp` passes with zero warnings.
- [ ] Zero regressions in `ch01-replication-supervision` fast verification gate.

[^pg-repl-mod]: `src/rs/vertebra-postgres/src/replication/mod.rs` — probe_identity and connect_logical primitives.
