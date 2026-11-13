---
type: Task
id: "native-postgres-driver:ch01-replication-supervision:t02"
flow_id: native-postgres-driver
chapter_id: ch01-replication-supervision
title: "Add connect_logical_verified identity checks and reconnect backoff"
description: "Verify server/database/slot/publication identity across reconnects, enforce confirmed_flush_lsn floors, and classify transient vs permanent errors."
status: stable
state: completed
commit: 7c4a3996
last_verified_commit: 7c4a3996
last_verified_at: "2026-11-13T01:52:00Z"
verification_evidence: "cargo test -p vertebra-postgres --test postgres_replication_wire_tests (56 passed; 0 failed) + cargo clippy -p vertebra-postgres --all-targets -- -D warnings (exit 0)"
claimed_by: null
claimed_at: null
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

# Task `t02`: Add connect_logical_verified identity checks and reconnect backoff

## Objective & Scope Boundary

Implement `PostgresReplicationConnection::connect_logical_verified`, `MAX_RAW_RECONNECT_ATTEMPTS = 8`, `raw_reconnect_delay(attempt, jitter_seed)`, and `reconnect=true` replacement generation recovery in `replication/{mod,supervisor}.rs` per `../../contracts.md` and `../spec.md` §2 [^pg-repl-mod].

## Bounded Read Manifest

- `replication/supervisor.rs` — supervisor loop from `t01`.
- `replication/mod.rs` — `probe_identity` and `VerifiedReplicationIdentity`.
- `errors.rs` — SQLSTATE error classification.
- `postgres_replication_wire_tests.rs` — scripted TCP wire peer helpers.

## Contracts & Edge Cases

- Reject resume immediately (`Operational` / `Interface` / `Protocol`) if `system_identifier`, `database`, `slot`, or `publication_fingerprint` changed, if timeline regressed, or if `restart_lsn` / WAL availability shows a gap beyond `max(start_lsn, confirmed_flush_lsn)`.
- When `reconnect=true`, retry frame-boundary EOF, transient transport/server errors (`08*` except `08P01`, `57P*`, `53300`), and `SQLSTATE 55006` / slot-active teardown races with cancellable `100ms..=5s` jittered backoff (`<= 8` attempts), resuming strictly from `acknowledged_lsn` (never unacknowledged commits); fail immediately on mid-frame truncation, auth errors, or missing slot/publication (`42704`).

## Test Matrix

- Parameterized Rust wire tests for frame-boundary reconnect, `55006` slot-in-use retry, `confirmed_flush_lsn` floor advancement, and fail-closed `system_identifier`/publication mismatch.

## Verification

```bash
PYO3_PYTHON="$(pwd)/.venv/bin/python" PYTHONPATH="$(pwd)/src/py:$(pwd)/.venv/lib/python3.12/site-packages" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --locked --test postgres_replication_wire_tests && PYO3_PYTHON="$(pwd)/.venv/bin/python" cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
```

## Acceptance Criteria

- [x] Reconnect resumes without duplicate acknowledged commits or silent identity drift.
- [x] Clippy `--all-targets` passes with zero warnings.
- [x] Zero regressions in `ch01-replication-supervision` fast verification gate.

[^pg-repl-mod]: `src/rs/vertebra-postgres/src/replication/mod.rs` — probe_identity and connect_logical primitives.
