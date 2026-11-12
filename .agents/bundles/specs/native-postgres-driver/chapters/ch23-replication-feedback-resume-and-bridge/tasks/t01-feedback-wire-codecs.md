---
type: Task
id: "native-postgres-driver:ch23:t01"
chapter_id: ch23-replication-feedback-resume-and-bridge
title: "Decode replication CopyData and encode bounded status payloads"
description: "Implement strict public wire payload codecs and status interval computation only; session ownership, published-commit state, and I/O loops remain separate tasks."
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on: ["native-postgres-driver:ch22:t03"]
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
  - src/rs/vertebra-postgres/src/replication/mod.rs
  - src/rs/vertebra-postgres/src/protocol.rs
  - src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
files:
  - src/rs/vertebra-postgres/src/replication/feedback.rs
  - src/rs/vertebra-postgres/src/replication/mod.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
estimated_loc: 130
plan_revision: 8
plan_commit: 5eb75b2a
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
updated_at: "2026-11-12T05:00:00Z"
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

# Decode replication CopyData and encode bounded status payloads

## Objective & Scope Boundary

Implement strict public wire payload codecs and status interval computation only; session ownership, published-commit state, and I/O loops remain separate tasks. [^contracts]

Own only the production paths listed above and canonical wire tests; one executor invocation and signed atomic commit. New modules are intentional creation targets. Dependency-created modules are available after their owning tasks complete. No reconnect, Python handle, or durable checkpoint implementation belongs to this task.

## Bounded Read Manifest

- `.agents/bundles/specs/native-postgres-driver/contracts.md` — public contracts.
- `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — owned seam or direct collaborator.
- `src/rs/vertebra-postgres/src/replication/mod.rs` — owned seam or direct collaborator.
- `src/rs/vertebra-postgres/src/protocol.rs` — owned seam or direct collaborator.
- `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs` — owned seam or direct collaborator.

The owning chapter spec and referenced domain skills/knowledge are contextual reads. The source read set has 5 files; request a plan revision if additional source exploration is needed.

## Symbol & Signature Contracts

In `replication/feedback.rs`, exported through `pub mod feedback` in `replication/mod.rs`:

```rust
pub const MAX_UNACKED_COMMITS: usize = 1024;
pub const DEFAULT_STATUS_INTERVAL: Duration = Duration::from_secs(10);
pub enum ReplicationCopyData<'a> {
    XLogData { wal_start: u64, wal_end: u64, server_time_pg_us: i64, payload: &'a [u8] },
    PrimaryKeepalive { wal_end: u64, server_time_pg_us: i64, reply_requested: bool },
}
pub fn decode_replication_copy_data(frame: &[u8]) -> Result<ReplicationCopyData<'_>, PostgresError>;
pub fn encode_standby_status_update(write_lsn: u64, flush_lsn: u64, apply_lsn: u64, client_time_pg_us: i64, reply_requested: bool) -> [u8; 34];
pub fn compute_status_interval(wal_sender_timeout: Option<Duration>) -> Duration;
```

`w` requires at least25bytes; `k` exactly18bytes and final flag0or1. Encode network endian `r`+threeu64+i64+bool. The function emits payload only; the stream writer adds frontend `CopyData` framing. Interval: None/zero→10s, otherwise min(10s,max(100ms,timeout/3)).

Caller → declared public seam → real peer/decoder/feedback collaborator. Tests invoke public APIs from `postgres_replication_wire_tests.rs`; no private visibility expansion or mocked System Under Test. [^wire]

## Edge Cases & Error Semantics

Unknown tags, short w, non18-byte k, and flags outside0/1 return PostgresError::Protocol. Never index before checking lengths; negative PG-epoch timestamps round-trip as signedi64.

## Public-Seam Test Blueprint

Canonical module: `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs`; remote-owned TCP peer is scripted at the protocol boundary. Use Tokio paused clock and explicit peer barriers, never real sleeps or timing guesses. Pure feedback cases call the public Rust type directly.

| Case | Input | Expected output / error |
| --- | --- | --- |
| Empty XLogData | w payload exactly25bytes: wal_start=0x100000100, wal_end=0x200009000, time=-7, no trailing payload | XLogData preserves both LSN literals, signed timestamp -7 and empty borrowed slice |
| Nonempty XLogData | same header with trailing bytes [0,255,66] | Payload slice equals exactly [0,255,66], without copying header bytes or altering signed timestamp |
| Golden keepalive | k + wal_end=0x200001000 + time=-1 + flag1 | PrimaryKeepalive fields exactly match; no state mutation |
| Golden status | write=0x200000100,flush=apply=0x200000080,time=-1,flagfalse | 34bytes, r then exact network-endian fields, final0 |
| Malformed payload | w length24; k length17or19; k flag2; x | Protocol error for each |
| Intervals | None,0ms,150ms,9s,30s,90s | 10s,10s,100ms,3s,10s,10s |

## Verification

Red: add one canonical `feedback_wire` behavioral case, then run the focused command and require failure for the specified missing behavior. A filtered run must execute at least one test.

```bash
PYO3_PYTHON="$PWD/.venv/bin/python" PYTHONPATH="$PWD/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests feedback_wire
```

Green: repeat the same focused command, then run the full canonical module and fast native lint (all exit `0`):

```bash
PYO3_PYTHON="$PWD/.venv/bin/python" PYTHONPATH="$PWD/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests
PYO3_PYTHON="$PWD/.venv/bin/python" cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
cargo fmt --manifest-path src/rs/Cargo.toml --check
```

## Acceptance Criteria

- [ ] R22.4 framing and interval cases pass with no socket or feedback mutation.

- [ ] Red, Green, and lint evidence is recorded, and task plus checklist synchronization is included in one signed conventional commit.

## Notes & Discoveries

- Decoded, queued, leased, published, and acknowledged records have distinct ownership boundaries. Only an actually published Commit becomes acknowledgeable. [^contracts]

[^contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Commit-fenced replication and cancellation contracts.
[^wire]: `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs` — Canonical real-wire scripted PostgreSQL peer tests.
