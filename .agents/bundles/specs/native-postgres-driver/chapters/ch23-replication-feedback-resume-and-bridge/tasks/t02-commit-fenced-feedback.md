---
type: Task
id: "native-postgres-driver:ch23:t02"
chapter_id: ch23-replication-feedback-resume-and-bridge
title: "Fence acknowledgement to published Commit boundaries"
description: "Implement initialized feedback state and cumulative exact-boundary acknowledgements; defer all transport and publication-lease mechanics to ch34."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on: ["native-postgres-driver:ch23:t01"]
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - src/rs/vertebra-postgres/src/replication/feedback.rs
  - src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
files:
  - src/rs/vertebra-postgres/src/replication/feedback.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
estimated_loc: 165
plan_revision: 8
plan_commit: d90d4625
state_revision: 2
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
commit: null
created_at: "2026-11-12T05:00:00Z"
updated_at: "2026-11-12T04:25:15Z"
generated: {by: flow-revise, at: "2026-11-12T05:00:00Z"}
verified: []
sources:
  - id: contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Commit-fenced replication and cancellation contracts
  - id: wire
    resource: src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
    title: Canonical real-wire scripted PostgreSQL peer tests
tags: [postgres, replication, bounded, tdd]
---

# Fence acknowledgement to published Commit boundaries

## Objective & Scope Boundary

Implement initialized feedback state and cumulative exact-boundary acknowledgements; defer all transport and publication-lease mechanics to ch34. [^contracts]

Own only the production paths listed above and canonical wire tests; one executor invocation and signed atomic commit. New modules are intentional creation targets. Dependency-created modules are available after their owning tasks complete. No reconnect, Python handle, or durable checkpoint implementation belongs to this task.

## Bounded Read Manifest

- `.agents/bundles/specs/native-postgres-driver/contracts.md` — public contracts.
- `src/rs/vertebra-postgres/src/replication/feedback.rs` — owned seam or direct collaborator.
- `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs` — owned seam or direct collaborator.

The owning chapter spec and referenced domain skills/knowledge are contextual reads. The source read set has 3 files; request a plan revision if additional source exploration is needed.

## Symbol & Signature Contracts

In `replication/feedback.rs`:

```rust
pub struct ReplicationFeedback { /* private state */ }
impl ReplicationFeedback {
    pub fn new(initial_lsn: u64) -> Self;
    pub fn record_received_commit(&mut self, commit_end_lsn: u64) -> Result<(), PostgresError>;
    pub fn record_published_commit(&mut self, commit_end_lsn: u64) -> Result<(), PostgresError>;
    pub fn acknowledge(&mut self, commit_end_lsn: u64) -> Result<u64, PostgresError>;
    pub fn status_positions(&self) -> (u64, u64, u64);
    pub fn unacked_count(&self) -> usize;
}
```

Initialize received/acknowledged to `initial_lsn`, empty published deque. Received Commit ends advance monotonically (equal is idempotent); reject regression. Publication requires strictly increasing endLSN above acknowledged and <=received; caller owns proof that this is a decoded Commit. Reject duplicates and cap1024. `acknowledge` accepts initial/current acknowledged idempotently or an exact published boundary, cumulatively drains earlier published boundaries, and otherwise leaves state unchanged. `status_positions` is (max(received,acknowledged),acknowledged,acknowledged). No received advance comes from server wal_end, wal_start, payload byte count, or keepalive. Stream admission enforces one publication per consumed Commit.

Caller → declared public seam → real peer/decoder/feedback collaborator. Tests invoke public APIs from `postgres_replication_wire_tests.rs`; no private visibility expansion or mocked System Under Test. [^wire]

## Edge Cases & Error Semantics

Any invalid receive/publication/ack call returns PostgresError::Interface with unchanged fields and deque. Equal received and equal acknowledged calls are no-ops; publication duplicates are errors. Never infer durability from receipt.

## Public-Seam Test Blueprint

Canonical module: `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs`; remote-owned TCP peer is scripted at the protocol boundary. Use Tokio paused clock and explicit peer barriers, never real sleeps or timing guesses. Pure feedback cases call the public Rust type directly.

| Case | Input | Expected output / error |
| --- | --- | --- |
| Initial fence | new(0x200000100) | positions all0x200000100; unacked_count0 |
| Decoded but unpublished | receive0x200000200; acknowledge0x200000200 | Interface; flush/apply remain0x200000100 |
| Received monotonicity | new100;receive200;receive200;receive150 | equal receive succeeds idempotently; regression returns Interface; positions(200,100,100),count0 throughout refusal |
| Publication refusal atomicity | new100;receive400;publish200;publish200 duplicate;publish150 regression;publish500 above-received;publish100 at acknowledged;publish99 below acknowledged | each invalid publication returns Interface; positions remain(400,100,100),count1; ack200 still succeeds afterward and drains count0 |
| Cumulative boundary | receive350;publish200,publish350;ack250,ack999,ack350,ack350,ack200 | invalid calls unchanged;350 advances and drains;duplicate350ok;200regression rejected |
| Capacity | receive2048;publish1..1024 on new0 then publish1025 | count1024; overflow rejected unchanged; ack1024 frees all slots |
| Server ahead | no receive call; server wal_end0x200009000 | positions remain initial; codec has no reference to state |

## Verification

Red: add one canonical `feedback_state` behavioral case, then run the focused command and require failure for the specified missing behavior. A filtered run must execute at least one test.

```bash
PYO3_PYTHON="$PWD/.venv/bin/python" PYTHONPATH="$PWD/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests feedback_state
```

Green: repeat the same focused command, then run the full canonical module and fast native lint (all exit `0`):

```bash
PYO3_PYTHON="$PWD/.venv/bin/python" PYTHONPATH="$PWD/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests
PYO3_PYTHON="$PWD/.venv/bin/python" cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
cargo fmt --manifest-path src/rs/Cargo.toml --check
```

## Acceptance Criteria

- [ ] R22.4 only published complete Commit boundaries advance flush/apply.
- [ ] Cumulative acknowledgements, initialized positions, cap and no-mutation refusal all pass.

- [ ] Red, Green, and lint evidence is recorded, and task plus checklist synchronization is included in one signed conventional commit.

## Notes & Discoveries

- Decoded, queued, leased, published, and acknowledged records have distinct ownership boundaries. Only an actually published Commit becomes acknowledgeable. [^contracts]

[^contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Commit-fenced replication and cancellation contracts.
[^wire]: `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs` — Canonical real-wire scripted PostgreSQL peer tests.
