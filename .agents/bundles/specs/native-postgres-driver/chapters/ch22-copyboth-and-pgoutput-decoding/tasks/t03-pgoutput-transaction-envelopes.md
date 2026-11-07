---
type: Task
id: "native-postgres-driver:ch22:t03"
chapter_id: ch22-copyboth-and-pgoutput-decoding
title: Order logical transactions and change envelopes
description: Complete pgoutput v1 transaction state tracking and bounded ReplicationRecord envelopes across Begin, Origin, Insert, Update, Delete, Truncate, Message, and Commit frames.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch22:t02"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
  - src/rs/vertebra-postgres/src/replication/pgoutput.rs
  - src/rs/vertebra-postgres/src/replication/mod.rs
  - src/rs/vertebra-postgres/tests/postgres_pgoutput_tests.rs
files:
  - src/rs/vertebra-postgres/src/replication/pgoutput.rs
  - src/rs/vertebra-postgres/src/replication/mod.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_pgoutput_tests.rs
estimated_loc: 235
plan_revision: 6
plan_commit: 8833b064
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
updated_at: "2026-11-07T05:00:00Z"
commit: null
sources:
  - id: npd-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Logical replication commit-boundary and event payload contracts
  - id: npd-proto-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: pgoutput v1 Begin, Commit, Origin, DML, Truncate, and Message wire formats
  - id: cdc-domain
    resource: .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
    title: Signed-int64 event IDs and deduplication coordinate invariants
tags:
  - postgres
  - replication
  - pgoutput
  - transactions
---

# Task t03: Order Logical Transactions and Change Envelopes

## Objective & Scope Boundary

Extend `PgOutputDecoder` and `src/rs/vertebra-postgres/src/replication/mod.rs` to validate transaction ordering and produce bounded owned `ReplicationRecord` envelopes across `Begin` (`'B'`), `Origin` (`'O'`), `Insert` (`'I'`), `Update` (`'U'` with `'K'`/`'O'` old and `'N'` new tuples), `Delete` (`'D'` with `'K'`/`'O'` tuple), `Truncate` (`'T'`), `Message` (`'M'`), and `Commit` (`'C'`), enforcing `16 MiB` single-record and `1024`-record / `8 MiB` queue limits with zero whole-transaction buffering [^npd-contracts] [^npd-proto-research] [^cdc-domain]. `StandbyStatusUpdate` LSN feedback and heartbeats belong to `ch23:t01`.

## Bounded Read Manifest

- [spec.md](../spec.md) & [contracts.md](../../../contracts.md) — Commit-boundary acknowledgement and deduplication coordinate invariants [^npd-contracts].
- [protocol-and-execution_20261105.md](../../../research/protocol-and-execution_20261105.md) — `pgoutput` v1 transaction, `Truncate` flags, `Message`, and Arrow/Parquet `int64` `event_id` constraints [^npd-proto-research].
- `src/rs/vertebra-postgres/src/replication/pgoutput.rs` — `PgOutputDecoder`, `RelationCache`, and `PgOutputTupleValue` from `ch22:t02`.
- `src/rs/vertebra-postgres/src/replication/mod.rs` — `PostgresReplicationConnection` and `VerifiedReplicationIdentity`.
- `src/rs/vertebra-postgres/tests/postgres_pgoutput_tests.rs` — `pgoutput` golden transcript test suite.

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/replication/pgoutput.rs`:
  - `pub const DEFAULT_MAX_QUEUE_RECORDS: usize = 1024;` and `pub const DEFAULT_MAX_QUEUE_BYTES: usize = 8 * 1024 * 1024;`
  - `pub struct ActiveLogicalTx { pub xid: u32, pub begin_final_lsn: u64, pub commit_time_pg_us: i64, pub origin_name: Option<String>, pub origin_lsn: Option<u64>, pub next_ordinal: u64 }`
  - `pub struct ReplicationDedupKey { pub system_identifier: String, pub slot: String, pub begin_final_lsn: u64, pub transaction_ordinal: u64 }`
  - `pub enum ReplicationRecord`:
    - `Begin { dedup: ReplicationDedupKey, xid: u32, begin_final_lsn: u64, commit_time_pg_us: i64 }`
    - `Insert { dedup: ReplicationDedupKey, xid: u32, relation: Arc<PgOutputRelationSchema>, new_tuple: Vec<PgOutputTupleValue> }`
    - `Update { dedup: ReplicationDedupKey, xid: u32, relation: Arc<PgOutputRelationSchema>, old_kind: Option<OldTupleKind>, old_tuple: Option<Vec<PgOutputTupleValue>>, new_tuple: Vec<PgOutputTupleValue> }` (`pub enum OldTupleKind { Key, Full }`)
    - `Delete { dedup: ReplicationDedupKey, xid: u32, relation: Arc<PgOutputRelationSchema>, old_kind: OldTupleKind, old_tuple: Vec<PgOutputTupleValue> }`
    - `Truncate { dedup: ReplicationDedupKey, xid: u32, relations: Vec<Arc<PgOutputRelationSchema>>, cascade: bool, restart_identity: bool }`
    - `Message { dedup: Option<ReplicationDedupKey>, xid: Option<u32>, transactional: bool, message_lsn: u64, prefix: String, content: Bytes }`
    - `Commit { dedup: ReplicationDedupKey, xid: u32, flags: u8, commit_lsn: u64, commit_end_lsn: u64, commit_time_pg_us: i64 }`
  - `pub struct BoundedReplicationQueue` enforcing `max_records` (`1024`) and `max_bytes` (`8 MiB`) with backpressure check `can_accept(approx_bytes: usize) -> bool` (never silently dropping records; always allowing at least `1` record `<= max_record_bytes` when empty so large valid rows up to `16 MiB` do not deadlock).
- `src/rs/vertebra-postgres/src/replication/mod.rs`:
  - Re-export `ReplicationRecord`, `OldTupleKind`, `ReplicationDedupKey`, and `BoundedReplicationQueue`, and wire transaction reset on disconnect (`reset_on_disconnect()` drops any uncommitted `ActiveLogicalTx`).

## Edge Cases & Error Semantics

- Reject `Insert` (`'I'`), `Update` (`'U'`), `Delete` (`'D'`), `Truncate` (`'T'`), `Origin` (`'O'`), `Commit` (`'C'`), or transactional `Message` (`'M'` with `flags == 1`) when `active_tx` is `None` (`ProtocolError`).
- Reject nested `Begin` (`'B'`) while `active_tx.is_some()`, nonzero reserved `Commit` flags (`flags != 0`), unknown `Truncate` option bits (`(options & !0x03) != 0`), `Message` flags outside `{0, 1}`, and unnegotiated v2–v4 streaming/two-phase tags (`'S'`, `'E'`, `'A'`, `'c'`, `'b'`, `'P'`, `'K'`, `'r'`) with `ProtocolError`.
- Non-transactional `Message` (`'M'` with `flags == 0`) is valid both inside and outside an active transaction: it emits immediately with `commit_end_lsn = None` (and `xid = active_tx.map(|t| t.xid)`) and never creates an acknowledgeable commit boundary on its own.
- `Commit` verifies `commit_lsn == active_tx.begin_final_lsn` and `commit_end_lsn >= commit_lsn`, clears `active_tx`, and exposes `commit_end_lsn` as the sole protocol-acknowledgeable LSN boundary.
- A single record exceeding `16 MiB` fails immediately with resource-limit `OperationalError` without acknowledging prior uncommitted records; mid-transaction socket EOF discards `active_tx` cleanly for replay.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/postgres_pgoutput_tests.rs`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Full multi-relation `Begin`..`Commit` envelope sequence | `'O'`/`'B'` (`xid = 701`, `final_lsn = 0x1_0000_0400`) -> `'I'` -> `'U'` (`'K'` key + `'N'` new with `'u'` TOAST) -> `'U'` (`'O'` full old + `'N'` new) -> `'D'` (`'K'`) -> `'T'` (`2` relations, `options = 0x03` cascade + restart identity) -> transactional `'M'` -> `'C'` (`commit_end_lsn = 0x1_0000_0440`) | Emits `7` ordered `ReplicationRecord`s with `transaction_ordinal` `0..=6`, exact `OldTupleKind::{Key, Full}`, `cascade == true`, `restart_identity == true`, and `Commit.commit_end_lsn == 0x1_0000_0440`; never buffers prior DML waiting for `'C'`. |
| Standalone non-transactional `Message` (`'M'`) | `'M'` with `flags = 0`, `lsn = 0x1_0000_0500`, `prefix = "hb"`, `content = b"\xde\xad"` outside `Begin` | Emits `ReplicationRecord::Message { transactional: false, xid: None, ... }` immediately with no commit boundary advancement. |
| Out-of-order DML, nested `Begin`, & v2 streaming tags | `'I'` before `'B'`; `'B'` followed by `'B'`; `'S'` (`StreamStart`) tag; `'C'` with `flags = 1` or mismatched `commit_lsn` | Each case returns `ProtocolError` immediately. |
| Oversized `16 MiB + 1` record & `1024` / `8 MiB` queue backpressure | Push `'M'` with `content.len() == 16 * 1024 * 1024 + 1`; fill `BoundedReplicationQueue` to `1024` records or `8 MiB` | Oversized record fails with `OperationalError`; full queue reports `can_accept == false` without dropping any queued record. |
| Mid-transaction disconnect reset | Emit `'B'` + `'I'` without `'C'`, call `reset_on_disconnect()`, then feed fresh `'B'` (`xid = 702`) + `'C'` | Uncommitted `xid = 701` state is discarded; `xid = 702` decodes cleanly from `ordinal = 0`. |

## Verification

- Initial Red baseline: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_pgoutput_tests`
- Final Green + Clippy gate:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_pgoutput_tests
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
  ```

## Acceptance Criteria

- [ ] `PgOutputDecoder` emits ordered `Begin`, `Insert`, `Update`, `Delete`, `Truncate`, `Message`, and `Commit` envelopes with `(system_identifier, slot, begin_final_lsn, transaction_ordinal)` dedup coordinates and zero whole-transaction buffering (R22).
- [ ] Out-of-order DML, nested `Begin`, nonzero reserved flags, unnegotiated v2+ tags, and `> 16 MiB` records fail explicitly, and `BoundedReplicationQueue` enforces `1024` records / `8 MiB` without silent drops (R22).
- [ ] Commit message follows `feat(postgres): order pgoutput transaction records` once verified.

## Notes & Discoveries

- Even though `streaming = 'false'` causes PostgreSQL to publish only committed transactions on the wire, network disconnects can still truncate an in-flight `Begin`..`Commit` stream; downstream consumers stage atomic application until `Commit` while Rust keeps memory bounded [^npd-proto-research].

[^npd-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Logical replication commit-boundary and event payload contracts.
[^npd-proto-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — pgoutput v1 Begin, Commit, Origin, DML, Truncate, and Message wire formats.
[^cdc-domain]: `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` — Signed-int64 event IDs and deduplication coordinate invariants.
