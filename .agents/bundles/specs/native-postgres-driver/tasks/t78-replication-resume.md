---
type: Task
id: "native-postgres-driver:t78"
title: "Reconnect logical streams from verified checkpoints"
description: "Reconnect logical streams from verified checkpoints for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:t77"
files:
  - src/rs/vertebra-postgres/src/replication/mod.rs
  - src/rs/vertebra-postgres/src/replication/feedback.rs
  - src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
plan_revision: 2
plan_commit: 9af810f5
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

# Reconnect logical streams from verified checkpoints

## Objective

Reconnect logical streams from verified checkpoints; verify returned events, wire boundaries and owner cleanup through the named tests.

## Context

Read `../spec.md`, predecessor worksheets, protocol research, database drivers/CDC knowledge, ADR-004 and local runtime-ipc/pyo3-bridge skills. One worksheet makes one signed conventional local commit on the complete driver's shared branch and PR. No implementation with `plan_commit: null`; preserve others' edits. Listed files are owned after dependencies complete.

Estimated handwritten changes including tests: 380 lines. If this exceeds roughly400 lines during execution, `/flow:revise native-postgres-driver` splits the boundary while retaining the full one-PR scope. Planned acceptance is never passing evidence.

## Steps

1. Test the standalone raw native API's optional autoreconnect after DML/before COMMIT, COMMIT/before ack, and ack feedback. Resume from its last protocol-acknowledged commit end, never sender wal_end; rebuild relation/type cache and discard partial prior transaction. These raw API tests do not claim durable checkpoint coordination.
2. PostgresReplicationResume retains verified identity(system_identifier,database,slot,publication fingerprint), acknowledged_lsn and generation. Every reconnect repeats IDENTIFY_SYSTEM/preflight and rejects changed identity/plugin/database/publication, unavailable WAL or checkpoint behind slot confirmed_flush_lsn; no adopt-newer skip or automatic slot recreation. Timeline change requires same system identity and valid slot/start.
3. Raw open_replication(reconnect=True) may retry recoverable transport failures only, exponential0.1..5s jitter,max8; terminal auth/permission/protocol/WAL/slot errors preserve SQLSTATE. Refresh IAM before reconnect and cancel backoff promptly. Old native generations cannot publish/ack the replacement. With reconnect=False, transport loss terminates and does not internally open another socket; deterministic server connection-count test proves it.
4. High-level PostgresChangeSource ALWAYS opens native reconnect=False. Its config.reconnect controls the Python source loop(t82), which closes old generation, reloads authoritative checkpoint and counter and explicitly reopens. Raw native autoreconnect never reads or changes the Python checkpoint store. Tail-only caller-provisioned slot/publication semantics remain unchanged; no initial snapshot/DDL. Commit `feat(postgres): resume raw logical streams with explicit ownership`.

## Verification

Add the named failing tests before behavior changes; record initial RED at the absent public seam or the wrong event, LSN, state or cleanup assertion. Then run:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests reconnect
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests checkpoint_
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
