---
type: Task
id: "native-postgres-driver:ch22:t03"
chapter_id: ch22-copyboth-and-pgoutput-decoding
title: Order logical transactions and change envelopes
description: Extend PgOutputDecoder with pgoutput v1 Begin, Origin, Update, Delete, Truncate, Message, and Commit frames, a next_record transaction state machine emitting bounded ReplicationRecord envelopes, and a BoundedReplicationQueue.
status: draft
state: completed
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
  - src/rs/vertebra-postgres/src/result.rs
files:
  - src/rs/vertebra-postgres/src/replication/pgoutput.rs
  - src/rs/vertebra-postgres/src/replication/mod.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_pgoutput_tests.rs
estimated_loc: 245
plan_revision: 7
plan_commit: c8722c76
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-12T02:35:00Z"
last_verified_commit: 2947d015
verification_evidence: "Red: missing ReplicationRecord, BoundedReplicationQueue and next_record. Green: 20 pgoutput and replication wire tests; all-target Clippy -D warnings; diff --check exit 0. PYO3_PYTHON pinned to repository Python 3.12."
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-12T03:00:27Z"
commit: 2947d015
generated:
  by: flow-revise
  at: "2026-11-12T01:30:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T01:30:00Z"
    commit: c8722c76
sources:
  - id: npd-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Logical replication commit-boundary and event payload contracts
  - id: npd-proto-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: pgoutput v1 Begin, Commit, Origin, DML, Truncate, and Message wire formats
  - id: pgoutput-rs
    resource: src/rs/vertebra-postgres/src/replication/pgoutput.rs::PgOutputDecoder
    title: Existing decode_next_frame parser for Relation, Type, and Insert frames
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

`PgOutputDecoder::decode_next_frame` on `main` parses `Relation` (`'R'`), `Type` (`'Y'`), and `Insert` (`'I'`) and rejects every other tag [^pgoutput-rs]. This task extends `PgOutputFrame` and `decode_next_frame` with the remaining `pgoutput` v1 frames — `Begin` (`'B'`), `Origin` (`'O'`), `Update` (`'U'` with `'K'`/`'O'` old and `'N'` new tuples), `Delete` (`'D'`), `Truncate` (`'T'`), `Message` (`'M'`), and `Commit` (`'C'`) — and adds `PgOutputDecoder::next_record`, a transaction state machine that turns frames into bounded owned `ReplicationRecord` envelopes, plus `BoundedReplicationQueue` (`1024` records / `8 MiB`) with zero whole-transaction buffering [^npd-contracts] [^npd-proto-research] [^cdc-domain]. `replication/mod.rs` re-exports the new types.

Out of scope: `StandbyStatusUpdate` feedback and heartbeats (`ch23:t01`), Python record projection and the PyO3 replication handle (`ch23:t03`), and any Python file.

## Bounded Read Manifest

- [contracts.md](../../../contracts.md) — Commit-boundary acknowledgement and deduplication coordinate invariants [^npd-contracts].
- [protocol-and-execution_20261105.md](../../../research/protocol-and-execution_20261105.md) — `pgoutput` v1 transaction, `Truncate` flags, and `Message` layouts [^npd-proto-research].
- `src/rs/vertebra-postgres/src/replication/pgoutput.rs::{PgOutputDecoder, PgOutputFrame, PgOutputTupleValue, PgOutputRelationSchema}` — existing `decode_next_frame`, `FrameCursor`, `parse_insert_frame`, and `reset_on_disconnect` [^pgoutput-rs].
- `src/rs/vertebra-postgres/src/replication/mod.rs::VerifiedReplicationIdentity` — identity fields used for dedup coordinates.
- `src/rs/vertebra-postgres/tests/postgres_pgoutput_tests.rs` — existing golden helpers (`encode_relation_frame`, `encode_tuple_data`, `encode_insert_frame`).

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/replication/pgoutput.rs`:
  - `pub const DEFAULT_MAX_QUEUE_RECORDS: usize = 1024;` and `pub const DEFAULT_MAX_QUEUE_BYTES: usize = 8 * 1024 * 1024;`
  - Network-endian `proto.c` v1 layouts parsed by `decode_next_frame` (reusing `FrameCursor` and the existing tuple parser shared with `Insert`):
    - `'B'`: `final_lsn: u64`, `commit_time_pg_us: i64` (microseconds since `2000-01-01T00:00:00Z`), `xid: u32` -> `PgOutputFrame::Begin { final_lsn, commit_time_pg_us, xid }`.
    - `'O'`: `origin_lsn: u64`, `origin_name: cstring` -> `PgOutputFrame::Origin { origin_lsn, origin_name }`.
    - `'U'`: `rel_oid: u32`, optional `'K'` or `'O'` + `TupleData`, then `'N'` + `TupleData` -> `PgOutputFrame::Update { relation, old_kind, old_tuple, new_tuple }`.
    - `'D'`: `rel_oid: u32`, `'K'` or `'O'` + `TupleData` -> `PgOutputFrame::Delete { relation, old_kind, old_tuple }`.
    - `'T'`: `nrelids: u32`, `options: u8` (`0x01` cascade, `0x02` restart identity), `[rel_oid: u32; nrelids]` -> `PgOutputFrame::Truncate { relations, cascade, restart_identity }`.
    - `'M'`: `flags: u8`, `message_lsn: u64`, `prefix: cstring`, `content_len: u32`, `content` -> `PgOutputFrame::Message { transactional, message_lsn, prefix, content }`.
    - `'C'`: `flags: u8`, `commit_lsn: u64`, `commit_end_lsn: u64`, `commit_time_pg_us: i64` -> `PgOutputFrame::Commit { .. }`.
  - `pub enum OldTupleKind { Key, Full }`
  - `pub struct ActiveLogicalTx { pub xid: u32, pub begin_final_lsn: u64, pub commit_time_pg_us: i64, pub origin_name: Option<String>, pub origin_lsn: Option<u64>, pub next_ordinal: u64 }`
  - `pub struct ReplicationDedupKey { pub system_identifier: String, pub slot: String, pub begin_final_lsn: u64, pub transaction_ordinal: u64 }`
  - `pub enum ReplicationRecord`:
    - `Begin { dedup: ReplicationDedupKey, xid: u32, begin_final_lsn: u64, commit_time_pg_us: i64 }`
    - `Insert { dedup: ReplicationDedupKey, xid: u32, relation: Arc<PgOutputRelationSchema>, new_tuple: Vec<PgOutputTupleValue> }`
    - `Update { dedup: ReplicationDedupKey, xid: u32, relation: Arc<PgOutputRelationSchema>, old_kind: Option<OldTupleKind>, old_tuple: Option<Vec<PgOutputTupleValue>>, new_tuple: Vec<PgOutputTupleValue> }`
    - `Delete { dedup: ReplicationDedupKey, xid: u32, relation: Arc<PgOutputRelationSchema>, old_kind: OldTupleKind, old_tuple: Vec<PgOutputTupleValue> }`
    - `Truncate { dedup: ReplicationDedupKey, xid: u32, relations: Vec<Arc<PgOutputRelationSchema>>, cascade: bool, restart_identity: bool }`
    - `Message { dedup: Option<ReplicationDedupKey>, xid: Option<u32>, transactional: bool, message_lsn: u64, prefix: String, content: Bytes }`
    - `Commit { dedup: ReplicationDedupKey, xid: u32, flags: u8, commit_lsn: u64, commit_end_lsn: u64, commit_time_pg_us: i64, origin_name: Option<String>, origin_lsn: Option<u64> }`
    - `pub fn approx_bytes(&self) -> usize` and `pub fn commit_end_lsn(&self) -> Option<u64>`.
  - `impl PgOutputDecoder`:
    - `pub fn next_record(&mut self, identity: &VerifiedReplicationIdentity, catalog: &mut TypeCatalog, codec_ctx: &CodecContext) -> Result<Option<ReplicationRecord>, PostgresError>` — loops over `decode_next_frame`; `Relation`/`Type` frames update state and continue; `Origin` updates the active transaction and continues; every other frame returns one record.
    - `pub fn active_transaction(&self) -> Option<&ActiveLogicalTx>`.
    - `reset_on_disconnect` also clears the active transaction.
  - `pub struct BoundedReplicationQueue` — `new(max_records: usize, max_bytes: usize, max_record_bytes: usize) -> Self`, `can_accept(&self, approx_bytes: usize) -> bool`, `push(&mut self, record: ReplicationRecord) -> Result<(), PostgresError>` (returns `PostgresError::Operational` when full rather than dropping), `pop(&mut self) -> Option<ReplicationRecord>`, `len`, `is_empty`; an empty queue always admits one record `<= max_record_bytes`.
- `src/rs/vertebra-postgres/src/replication/mod.rs`: `pub use pgoutput::{ActiveLogicalTx, BoundedReplicationQueue, OldTupleKind, ReplicationDedupKey, ReplicationRecord, DEFAULT_MAX_QUEUE_BYTES, DEFAULT_MAX_QUEUE_RECORDS};`.
- Caller -> Seam -> Collaborator: `ch23:t01` read task -> `PgOutputDecoder::{push, next_record}` -> `decode_next_frame` + `ActiveLogicalTx` -> `BoundedReplicationQueue::push`.

## Edge Cases & Error Semantics

- `Insert`, `Update`, `Delete`, `Truncate`, `Origin`, `Commit`, or transactional `Message` with no active transaction -> `PostgresError::Protocol`.
- Nested `Begin`, `Commit` flags `> 0`, `commit_lsn != begin_final_lsn`, `commit_end_lsn < commit_lsn`, `Truncate` options `> 0x03` or `nrelids == 0`, `Message` flags outside `{0, 1}`, uncached relation OID in `Update`/`Delete`/`Truncate`, and v2–v4 tags (`'S'`, `'E'`, `'A'`, `'c'`, `'b'`, `'P'`, `'K'`, `'r'`) -> `PostgresError::Protocol`.
- Non-transactional `Message` emits immediately with `dedup = None`, `xid = active_tx.map(|t| t.xid)` and creates no commit boundary.
- `Commit` moves `origin_name`/`origin_lsn` from the active transaction into the record, clears the active transaction, and exposes `commit_end_lsn` as the only acknowledgeable boundary.
- A record `> 16 MiB` fails with `PostgresError::Operational`; mid-transaction EOF followed by `reset_on_disconnect()` discards the active transaction for replay.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/postgres_pgoutput_tests.rs`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Full multi-relation transaction | `'R'` (`rel_a`, `rel_b`) -> `'B'` (`xid = 701`, `final_lsn = 0x1_0000_0400`) -> `'O'` (`origin_lsn = 0x0_5000_0100`, `"peer_a"`) -> `'I'` -> `'U'` (`'K'` + `'N'` with `'u'` TOAST) -> `'U'` (`'O'` + `'N'`) -> `'D'` (`'K'`) -> `'T'` (`nrelids = 2`, `options = 0x03`) -> `'M'` (`flags = 1`) -> `'C'` (`commit_lsn = 0x1_0000_0400`, `commit_end_lsn = 0x1_0000_0440`) | `next_record` yields `8` records (`Begin`, `Insert`, `Update`, `Update`, `Delete`, `Truncate`, `Message`, `Commit`) with `transaction_ordinal` `0..=7`, `OldTupleKind::Key` then `Full`, `cascade && restart_identity`, `Commit.origin_lsn == Some(0x0_5000_0100)`, `Commit.origin_name.as_deref() == Some("peer_a")`, `commit_end_lsn() == Some(0x1_0000_0440)`; `Insert` is returned before `'C'` is pushed. |
| Standalone non-transactional `Message` | `'M'` (`flags = 0`, `message_lsn = 0x1_0000_0500`, `prefix = "hb"`, `content = b"\xde\xad"`) with no `Begin` | `ReplicationRecord::Message { transactional: false, xid: None, dedup: None, .. }`; `active_transaction()` stays `None`. |
| Ordering and flag violations | `'O'` before `'B'`; `'I'` before `'B'`; `'B'`,`'B'`; `'S'`; `'T'` with `options = 0x04`; `'C'` with `flags = 1`; `'C'` with mismatched `commit_lsn` | Each returns `PostgresError::Protocol`. |
| Size and queue bounds | `'M'` with `content.len() == 16 * 1024 * 1024 + 1`; `BoundedReplicationQueue::new(1024, 8 * 1024 * 1024, 16 * 1024 * 1024)` filled to `1024` records; then a fresh queue given one `9 MiB` record | Oversized `'M'` -> `PostgresError::Operational`; full queue `can_accept(1) == false` and `push` errors without dropping (`len() == 1024`); the empty queue admits the `9 MiB` record. |
| Disconnect reset | `'B'` (`xid = 701`) + `'I'`, `reset_on_disconnect()`, then `'R'` + `'B'` (`xid = 702`) + `'C'` | `active_transaction()` is `None` after reset; `xid = 702` records start at ordinal `0`. |

## Verification

- Initial Red baseline: `PYO3_PYTHON="$(pwd)/.venv/bin/python" PYTHONPATH="$(pwd)/src/py:$(pwd)/.venv/lib/python3.12/site-packages" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_pgoutput_tests`
- Final Green + Clippy gate:
  ```bash
  PYO3_PYTHON="$(pwd)/.venv/bin/python" PYTHONPATH="$(pwd)/src/py:$(pwd)/.venv/lib/python3.12/site-packages" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_pgoutput_tests --test postgres_replication_wire_tests
  PYO3_PYTHON="$(pwd)/.venv/bin/python" cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
  ```

## Acceptance Criteria

- [x] `decode_next_frame` parses exact `proto.c` v1 `'B'`, `'O'`, `'U'`, `'D'`, `'T'`, `'M'`, and `'C'` layouts and `next_record` emits ordered `ReplicationRecord`s carrying `(system_identifier, slot, begin_final_lsn, transaction_ordinal)` and `Commit` origin fields without whole-transaction buffering (R22).
- [x] Ordering violations, reserved flags, v2+ tags, and `> 16 MiB` records fail explicitly; `BoundedReplicationQueue` enforces `1024` records / `8 MiB` without drops; `reset_on_disconnect` clears the active transaction (R22).
- [x] Commit message follows `feat(postgres): order pgoutput transaction records` once verified.

## Notes & Discoveries

- Retained-byte accounting reads `result.rs` as the sixth manifest file. The full declared parser/state/queue contract requires substantially more code than the original 245-line estimate; coverage includes fragmented frames and unfinished-transaction EOF refusal.

- With `streaming = 'false'` PostgreSQL publishes only committed transactions, but a disconnect can still truncate an in-flight `Begin`..`Commit` stream; consumers stage application until `Commit` while Rust keeps memory bounded [^npd-proto-research].

[^npd-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Logical replication commit-boundary and event payload contracts.
[^npd-proto-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — pgoutput v1 Begin, Commit, Origin, DML, Truncate, and Message wire formats.
[^pgoutput-rs]: `src/rs/vertebra-postgres/src/replication/pgoutput.rs::PgOutputDecoder` — Existing `decode_next_frame` parser for `Relation`, `Type`, and `Insert` frames.
[^cdc-domain]: `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` — Signed-int64 event IDs and deduplication coordinate invariants.
