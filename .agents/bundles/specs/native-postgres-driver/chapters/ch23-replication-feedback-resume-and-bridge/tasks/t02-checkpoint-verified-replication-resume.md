---
type: Task
id: "native-postgres-driver:ch23:t02"
chapter_id: ch23-replication-feedback-resume-and-bridge
title: Reconnect logical streams from verified checkpoints
description: Implement PostgresReplicationResume generation fencing, identity and slot re-verification on reconnect, IAM credential refresh, and explicit reconnect=False single-socket termination.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch23:t01"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
  - src/rs/vertebra-postgres/src/replication/mod.rs
  - src/rs/vertebra-postgres/src/replication/feedback.rs
  - src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
files:
  - src/rs/vertebra-postgres/src/replication/mod.rs
  - src/rs/vertebra-postgres/src/replication/feedback.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
estimated_loc: 230
plan_revision: 6
plan_commit: bf53faf3
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
updated_at: "2026-11-10T14:45:00Z"
commit: null
generated:
  by: flow-revise
  at: "2026-11-10T14:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-10T14:45:00Z"
    commit: bf53faf3
sources:
  - id: npd-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Raw native reconnect versus PostgresChangeSource reconnect=False ownership
  - id: npd-proto-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: Logical slot crash recovery, WAL availability, and replay semantics
  - id: pg-conn-rs
    resource: src/rs/vertebra-db/src/connection.rs::TransportStream
    title: Transient versus terminal transport error classification
tags:
  - postgres
  - replication
  - reconnect
  - checkpoints
---

# Task t02: Reconnect Logical Streams from Verified Checkpoints

## Objective & Scope Boundary

Implement `PostgresReplicationResume` and generation-fenced stream supervision in `src/rs/vertebra-postgres/src/replication/mod.rs` and `feedback.rs` so standalone raw streams with `reconnect=True` resume strictly from `acknowledged_commit_lsn` after re-verifying `(system_identifier, database, slot, publication_fingerprint)` and refreshing IAM/cloud credentials, while `reconnect=False` terminates after a single socket drop without dialing a second connection [^npd-contracts] [^npd-proto-research] [^pg-conn-rs]. PyO3 `AsyncPostgresReplication` exposure belongs to `ch23:t03`, and Python `PostgresChangeSource` checkpoint store coordination belongs to Chapter 24.

## Bounded Read Manifest

- [spec.md](../spec.md) & [contracts.md](../../../contracts.md) — Raw `reconnect` parameter contract, identity mismatch / WAL gap refusal, and `PostgresChangeSource` `reconnect=False` rule [^npd-contracts].
- [protocol-and-execution_20261105.md](../../../research/protocol-and-execution_20261105.md) — Logical slot replay and LSN resume guarantees [^npd-proto-research].
- `src/rs/vertebra-postgres/src/replication/mod.rs` — `PostgresReplicationConnection` and split stream loops from `ch22:t01` and `ch23:t01`.
- `src/rs/vertebra-postgres/src/replication/feedback.rs` — `ReplicationFeedback` state and `acknowledged_commit_lsn` tracking.
- `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs` — Replication wire test harness.

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/replication/mod.rs` & `feedback.rs`:
  - `pub const MAX_RAW_RECONNECT_ATTEMPTS: u32 = 8;` (`100ms` initial backoff doubling up to `5.0s` cap with bounded deterministic/seeded jitter).
  - `pub struct PostgresReplicationResume { pub identity: VerifiedReplicationIdentity, pub acknowledged_lsn: u64, pub generation: u64 }`
  - `pub enum ReplicationStreamEvent { Control { identity: VerifiedReplicationIdentity, acknowledged_lsn: u64, generation: u64 }, Record { record: ReplicationRecord, generation: u64 } }`
  - `pub struct ReplicationSupervisor`:
    - `pub async fn open(config: crate::config::PostgresConfig, slot: String, publication: String, start_lsn: Option<u64>, queue_capacity: usize, reconnect: bool) -> Result<Self, crate::errors::PostgresError>` — explicitly checks `config.pooler_mode` first and rejects `PoolerMode::Transaction`, `PoolerMode::Statement`, and `PoolerMode::Auto` (requiring explicit `PoolerMode::Direct | PoolerMode::Session`) with `PostgresError::Interface` before dialing any socket (matching `PostgresReplicationConnection::connect_logical` and Python `PostgreSQLConfig.derive_direct_config()`).
    - Maintains a `1`-slot generation-fenced unconsumed front buffer (`pub(crate) fn push_front_unconsumed(&self, event: ReplicationStreamEvent)` checked first by `next_event()`, cleared on generation advance or `shutdown()`) so `ch23:t03` can restore events from cancelled Python `next()` futures in exact wire order without advancing `ReplicationFeedback::record_published_commit`.
    - Emits `ReplicationStreamEvent::Control` at the start of generation `0` (and each replacement generation `1..` when `reconnect == true`) carrying verified identity and `acknowledged_lsn` before any `ReplicationStreamEvent::Record`.
    - On transient transport EOF/reset when `reconnect == true`:
      1. Fence out old generation (`generation += 1`) and clear any stale unconsumed front-slot entry so late completions or `acknowledge` calls targeting prior generations cannot publish or advance feedback.
      2. Reset `PgOutputDecoder` (`reset_on_disconnect()`), clear unacked `published_commits` from the dead generation while preserving `acknowledged_commit_lsn`, refresh IAM/cloud material via `crate::session::establish_standalone_connection` over `crate::config::PostgresConfig`, and wait under cancellable exponential backoff (`0.1s..5.0s`, max `8` attempts).
      3. Re-run `IDENTIFY_SYSTEM` + slot/publication preflight, verify `(system_identifier, database, slot, publication_fingerprint)` matches initial `identity` (allowing `timeline` increase only when `system_identifier` matches and `acknowledged_lsn >= restart_lsn` and `acknowledged_lsn >= confirmed_flush_lsn`), and issue `START_REPLICATION ... LOGICAL <acknowledged_lsn>`.
    - When `reconnect == false`, any transport loss immediately marks the supervisor terminal and returns the transport `OperationalError` / `ConnectionFailure` with zero follow-up socket opens.

## Edge Cases & Error Semantics

- Disconnect after `Begin` + `Insert` (before `Commit`), or after `Commit` is published (before `acknowledge`), resumes from the previous `acknowledged_commit_lsn` (or initial `start_lsn` / slot `confirmed_flush_lsn` when zero commits were acknowledged), discarding uncommitted partial state and replaying the unacknowledged transaction cleanly.
- Never resume from server `wal_end` or `received_complete_lsn`, never recreate or alter an invalidated slot, and never silently skip ahead to a newer `confirmed_flush_lsn` when another consumer advanced the slot past `acknowledged_lsn`.
- Terminal failures — authentication/SQLSTATE `28xxx`, permission `42501`, protocol corruption, changed `system_identifier`/`database`/`plugin`/`publication_fingerprint`, `acknowledged_lsn < restart_lsn` (WAL removed), or `8` exhausted transient attempts — abort immediately preserving exact SQLSTATE or `OperationalError` reason without further retries.
- Promptly cancel in-progress backoff sleep when `shutdown()` / `aclose()` is invoked.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Reconnect mid-transaction & post-commit-pre-ack (`reconnect=true`) | Tx1 (`0x1_0000_0200`) published & acknowledged; Tx2 (`0x1_0000_0300`) published without ack; Tx3 (`Begin`+`Insert`) truncated by TCP drop | Reconnects as `generation = 1`, emits `Control(acknowledged_lsn = 0x1_0000_0200, generation = 1)`, sends `START_REPLICATION ... LOGICAL 1/200` (never `1/300` or server `wal_end`), and cleanly replays Tx2 and Tx3 with fresh `RelationCache`. |
| Identity / publication / WAL-gap refusal on reconnect | On reconnect, peer reports different `system_identifier`, modified `publication_fingerprint`, or `restart_lsn = "1/500" > acknowledged_lsn = "1/200"` | Fails closed immediately with `OperationalError` (`identity_mismatch` / `wal_unavailable`) without `START_REPLICATION` or further retries. |
| Valid timeline switch on failover | Peer keeps identical `system_identifier`, valid slot `restart_lsn <= acknowledged_lsn`, and `timeline = 2` (was `1`) | Reconnect succeeds on `generation = 1` from `acknowledged_lsn`. |
| `reconnect=false` strict single-socket contract | Open with `reconnect = false`, drop peer socket after Tx1 | Stream returns transport `ConnectionFailure`; server listener observes exact connection count `1` (zero reconnect attempts). |
| IAM refresh, `PoolerMode` rejection, & backoff cancellation | `ReplicationSupervisor::open` with `PoolerMode::Transaction | Statement | Auto`; expired initial IAM token refreshed on reconnect; separately invoke `shutdown()` mid-backoff under paused clock | `Transaction`/`Statement`/`Auto` rejected before dialing with `PostgresError::Interface` (`0` replication socket accepts); reconnect uses refreshed IAM token; `shutdown()` cancels backoff immediately without waiting `5s`. | |

## Verification

- Initial Red baseline: `PYTHONPATH="$(pwd)/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests reconnect`
- Final Green + Clippy gate:
  ```bash
  PYTHONPATH="$(pwd)/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests reconnect
  PYTHONPATH="$(pwd)/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests checkpoint_
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
  ```

## Acceptance Criteria

- [ ] `ReplicationSupervisor` rejects `PoolerMode::Transaction | Statement | Auto` (requiring explicit `PoolerMode::Direct | Session`) before dialing, resumes strictly from `acknowledged_commit_lsn` (never `wal_end`), fences generations (including `push_front_unconsumed`), rebuilds `RelationCache`/`TypeCatalog`, and re-verifies `(system_identifier, database, slot, publication_fingerprint)` and WAL bounds on every reconnect (R22).
- [ ] `reconnect=false` opens zero replacement sockets on transport loss, leaving checkpoint store reload and retry exclusively to Python `PostgresChangeSource` (R22).
- [ ] Commit message follows `feat(postgres): resume raw logical streams with explicit ownership` once verified.

## Notes & Discoveries

- Raw `ReplicationSupervisor` never touches any Python `PostgresCheckpointStore`; high-level `PostgresChangeSource` always passes `reconnect=False` so checkpoint CAS reload precedes any new generation [^npd-contracts].

[^npd-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Raw native reconnect versus PostgresChangeSource reconnect=False ownership.
[^npd-proto-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — Logical slot crash recovery, WAL availability, and replay semantics.
[^pg-conn-rs]: `src/rs/vertebra-db/src/connection.rs::TransportStream` — Transient versus terminal transport error classification.
