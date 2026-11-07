---
type: Task
id: "native-postgres-driver:t74"
title: "Start dedicated CopyBoth logical replication sessions"
description: "Start dedicated CopyBoth logical replication sessions for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:t73"
files:
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/src/protocol.rs
  - src/rs/vertebra-postgres/src/replication/mod.rs
  - src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
plan_revision: 2
plan_commit: cdb978d3
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
  - native
  - data-events
---

# Start dedicated CopyBoth logical replication sessions

## Objective

Start dedicated CopyBoth logical replication sessions; verify returned events, wire boundaries and owner cleanup through the named tests.

## Context

Read `../spec.md`, predecessor worksheets, protocol research, database drivers/CDC knowledge, ADR-004 and local runtime-ipc/pyo3-bridge skills. One worksheet makes one signed conventional local commit on the complete driver's shared branch and PR. No implementation with `plan_commit: null`; preserve others' edits. Listed files are owned after dependencies complete.

Estimated handwritten changes including tests: 410 lines. If this exceeds roughly400 lines during execution, `/flow:revise native-postgres-driver` splits the boundary while retaining the full one-PR scope. Planned acceptance is never passing evidence.

## Steps

1. Add native scripted-server startup replication=database, IDENTIFY_SYSTEM row description/result, START_REPLICATION and CopyBothResponse. Assert no query-pool lease is held, startup completes before streaming and SQL options exactly proto_version1,binary=false,streaming=false,messages=true (no two-phase option). Cover missing slot/publication, wrong plugin/database, active slot, malformed LSN, requested LSN beyond server current and unsupported messages; preserve SQLSTATE.
2. Factor existing protocol reader into one bounded read_frame; ordinary read_message retains its signature and postgres-protocol parser. Add replication-only message wrapper for tag W CopyBothResponse, validating length/format/count and consuming exact payload; all other tags delegate the unchanged upstream parser. Do not duplicate standard message decoding. Replication requires overall format0 and zero columns; malformed W fails boundedly.
3. `replication/mod.rs::PostgresReplicationConnection` owns a dedicated direct/session PostgresConnection initialized with replication=database, not query pool. Validate/quote slot identifier/publication option literal and format u64 LSN as X/Y. Native START_REPLICATION SLOT ... LOGICAL start (proto_version '1',publication_names '...',binary 'false',streaming 'false',messages 'true') uses simple Query. Record IDENTIFY_SYSTEM system_identifier/timeline/database; no physical replication.
4. Existing caller-provisioned logical pgoutput slot/publication required. Preflight on separate ordinary short-lived session reads pg_replication_slots/publications using bound params; confirm database/plugin=pgoutput/slot inactive and start not before restart_lsn or behind confirmed_flush_lsn. No create/drop/alter slot/publication, no initial snapshot. Preserve explicit clear OperationalError on invalid resume; shutdown CopyDone/response drain or discard. Commit `feat(postgres): establish logical CopyBoth sessions`.

## Verification

Add the named failing tests before behavior changes; record initial RED at the absent public seam or the wrong event, LSN, state or cleanup assertion. Then run:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests copyboth
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests replication_identity
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
```

Require final exit0 with actual command/exit and asserted behavior recorded. Rebuild before Python tests; no stale-extension evidence. No external write, push, tags or hosted workflow dispatch is authorized.

## Acceptance Criteria

- [ ] R22: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] All listed normal, malformed, reconnect, backpressure and lifetime cases pass.
- [ ] Dedicated session ownership and bounded resources hold; feedback never acknowledges unprocessed data.
- [ ] One owned local commit is reviewable; worksheet state and spec checklist synchronize after verification.

## Notes & Discoveries

None.
