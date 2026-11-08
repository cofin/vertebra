---
type: Task
id: "native-postgres-driver:ch22:t01"
chapter_id: ch22-copyboth-and-pgoutput-decoding
title: Start dedicated CopyBoth logical replication sessions
description: Factor bounded CopyBothResponse framing in protocol.rs and establish dedicated replication=database PostgresReplicationConnection sessions with catalog preflight and IDENTIFY_SYSTEM validation.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch21:t03"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
  - src/rs/vertebra-postgres/src/protocol.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/lib.rs
  - .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
files:
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/src/protocol.rs
  - src/rs/vertebra-postgres/src/replication/mod.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
estimated_loc: 230
plan_revision: 7
plan_commit: 1c75af65
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
    title: Native PostgreSQL public contracts for dedicated replication sessions
  - id: npd-proto-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL Protocol 3.0 CopyBoth and pgoutput v1 startup constraints
  - id: pg-conn-rs
    resource: src/rs/vertebra-db/src/connection.rs::TransportStream
    title: Shared transport stream and connection lifecycle traits
tags:
  - postgres
  - replication
  - copyboth
  - rust
---

# Task t01: Start Dedicated CopyBoth Logical Replication Sessions

## Objective & Scope Boundary

Factor the bounded backend message reader in `src/rs/vertebra-postgres/src/protocol.rs` around one shared `read_frame` helper with a replication-only `'W'` (`CopyBothResponse`) wrapper, and implement `PostgresReplicationConnection` in `src/rs/vertebra-postgres/src/replication/mod.rs` to validate caller-provisioned logical `pgoutput` slots and publications, run `IDENTIFY_SYSTEM`, and enter `START_REPLICATION ... LOGICAL` on a dedicated `replication=database` socket without borrowing any query-pool lease [^npd-contracts] [^npd-proto-research] [^pg-conn-rs]. `pgoutput` payload decoding (`ch22:t02`–`t03`) and `XLogData` / `StandbyStatusUpdate` LSN feedback (`ch23:t01`) are out of scope.

## Bounded Read Manifest

- [spec.md](../spec.md) & [contracts.md](../../../contracts.md) — Dedicated replication session rules, slot/publication tail-only contract, and identity tuple invariants [^npd-contracts].
- [protocol-and-execution_20261105.md](../../../research/protocol-and-execution_20261105.md) — `CopyBothResponse` (`'W'`) framing and `pgoutput` v1 `START_REPLICATION` option list [^npd-proto-research].
- `src/rs/vertebra-postgres/src/protocol.rs` — Existing bounded `read_message` framing over `postgres-protocol`.
- `src/rs/vertebra-postgres/src/session.rs` — Physical `PostgresConnection` startup, TLS/auth, and simple/extended query primitives.
- `src/rs/vertebra-postgres/src/lib.rs` — Crate module exports and PyO3 module registration entrypoint.
- [query-pipeline-cdc-and-tenancy.md](../../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md) — Engine-isolated CDC boundaries.

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/protocol.rs`:
  - Factor bounded tag + 4-byte length validation and payload slicing into `pub async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R, scratch: &mut BytesMut, max_frame_bytes: usize) -> Result<(u8, BytesMut), crate::errors::PostgresError>`.
  - Keep `read_message` signature and `postgres_protocol::message::backend::Message::parse` behavior unchanged for all standard sessions.
  - Add `pub enum ReplicationBackendMessage { CopyBothResponse { format: u8, column_formats: Vec<u16> }, Standard(postgres_protocol::message::backend::Message) }` and `pub async fn read_replication_message<R: AsyncRead + Unpin>(reader: &mut R, scratch: &mut BytesMut, max_frame_bytes: usize) -> Result<ReplicationBackendMessage, crate::errors::PostgresError>`:
    - On tag `b'W'`, validate exact payload length `>= 3` (`1` format byte + `2`-byte `u16` column count + `2 * count` bytes), consume exact payload, and require `format == 0` and `column_formats.is_empty()` for logical replication; reject truncated, trailing, nonzero-format, or nonzero-column frames boundedly.
    - Delegate every non-`b'W'` frame directly to `postgres_protocol::message::backend::Message::parse` without duplicating standard decoders.
- `src/rs/vertebra-postgres/src/replication/mod.rs` (wired in `src/rs/vertebra-postgres/src/lib.rs` via `pub mod replication;` and `pub(crate) fn register_pymodule(_m: &Bound<'_, PyModule>) -> PyResult<()>`):
  - `pub struct VerifiedReplicationIdentity { pub system_identifier: String, pub timeline: i32, pub database: String, pub slot: String, pub publication_fingerprint: String, pub confirmed_flush_lsn: u64, pub restart_lsn: u64 }`
  - `pub fn format_pg_lsn(lsn: u64) -> String` (`format!("{:X}/{:X}", (lsn >> 32) as u32, lsn as u32)`) and `pub fn parse_pg_lsn(raw: &str) -> Result<u64, crate::errors::PostgresError>`.
  - `pub struct PostgresReplicationConnection` owning a dedicated non-pooled `PostgresConnection` opened directly from `&crate::config::PostgresConfig` with `replication=database` startup parameter (raw `crate::config::PostgresConfig` carries no `pooler_mode` field; `PoolerMode` lives in `crate::session::statements::PoolerMode` / `SessionHandle` and Python `PostgreSQLConfig.derive_direct_config()`, which reject non-direct/session pooler modes before opening replication):
    - `pub async fn probe_identity(config: &crate::config::PostgresConfig, slot: &str, publication: &str) -> Result<VerifiedReplicationIdentity, crate::errors::PostgresError>` — opens an ordinary short-lived connection + `replication=database` `IDENTIFY_SYSTEM` probe (without `START_REPLICATION`), queries `pg_replication_slots` and `pg_publication` with bound parameters, verifies `database` matches, `slot_type == "logical"`, `plugin == "pgoutput"`, `active == false`, publication exists, computes deterministic `publication_fingerprint`, and closes sockets in all outcomes.
    - `pub async fn connect_logical(config: &crate::config::PostgresConfig, slot: &str, publication: &str, start_lsn: Option<u64>) -> Result<Self, crate::errors::PostgresError>` — runs preflight validation, checks `start_lsn` is not `< restart_lsn` and not `< confirmed_flush_lsn`, validates and quotes slot identifier and publication option literal, issues simple `Query`:
      `START_REPLICATION SLOT "<slot>" LOGICAL <X/Y> (proto_version '1', publication_names '<pub>', binary 'false', streaming 'false', messages 'true')` (using `0/0` when `start_lsn` is `None` so PostgreSQL starts from slot `confirmed_flush_lsn`, with zero `two_phase` option), awaits `CopyBothResponse`, and stores `VerifiedReplicationIdentity`.
    - `pub async fn shutdown(&mut self, deadline: Instant) -> Result<(), crate::errors::PostgresError>` — sends `CopyDone`, drains `CopyDone` / `CommandComplete` / `ReadyForQuery` within `deadline`, or closes and discards the physical socket on timeout/error.

## Edge Cases & Error Semantics

- Never create, alter, or drop replication slots or publications, and never run an initial snapshot export; missing slot or publication preserves server SQLSTATE `42704` (`UndefinedObject`) or explicit `OperationalError`.
- Active slot (`active == true` in preflight or SQLSTATE `55006` `ObjectInUse` on `START_REPLICATION`), wrong `plugin` (e.g. `test_decoding`), wrong database, physical slot (`slot_type != "logical"`), `start_lsn < restart_lsn` (WAL removed), `start_lsn < confirmed_flush_lsn` (stale rewind), or `start_lsn` beyond server current WAL preserves exact SQLSTATE or raises clear `OperationalError`.
- Identifier validation rejects NUL bytes, empty slot/publication names, or unquotable control characters before opening network sockets; SQL quoting escapes embedded `"` in identifiers and `'` / `\` in option literals so option injection is impossible.
- Malformed `'W'` frame (`len < 7`, `format == 1`, `num_columns > 0`, or trailing bytes) fails boundedly as `ProtocolError` and drops the dedicated socket.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs` (scripted raw TCP Protocol 3.0 peer, zero query-pool mocks).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Valid `CopyBoth` startup | `connect_logical` with `slot = "vertebra_slot"`, `publication = "vertebra_pub"`, `start_lsn = Some(0x0000_0001_0000_01A0)` against scripted peer | Startup packet carries `replication = "database"`; preflight binds slot/pub without `START_REPLICATION`; replication socket sends `IDENTIFY_SYSTEM` then exact `START_REPLICATION SLOT "vertebra_slot" LOGICAL 1/1A0 (proto_version '1', publication_names 'vertebra_pub', binary 'false', streaming 'false', messages 'true')`; receives `'W'` (`format=0, cols=0`); `identity` records `system_identifier = "7399001122334455667"`, `timeline = 1`, `database = "appdb"`. |
| Identity-only probe (`probe_identity`) | `probe_identity(opts, "vertebra_slot", "vertebra_pub")` | Runs catalog preflight + `IDENTIFY_SYSTEM`, never emits `START_REPLICATION`, closes socket to EOF, returns `VerifiedReplicationIdentity`. |
| Missing slot / wrong plugin / active slot | Peer returns `plugin = "test_decoding"`, `active = true` (`55006`), or `ErrorResponse(42704)` | Fails closed with exact `OperationalError` / SQLSTATE `55006` or `42704`; socket closed. |
| Resume LSN behind `restart_lsn` or `confirmed_flush_lsn` | Slot `restart_lsn = "1/200"`, `confirmed_flush_lsn = "1/300"`, caller passes `start_lsn = Some(0x1_0000_0150)` | Fails before `START_REPLICATION` with `OperationalError` naming unusable WAL / checkpoint rewind. |
| Malformed `CopyBothResponse` (`'W'`) | Peer sends `'W'` with `format = 1` or `column_count = 2` or truncated 5-byte length | `read_replication_message` returns `ProtocolError` without unbounded allocation; connection closes. |
| Clean `CopyDone` shutdown | Active `PostgresReplicationConnection::shutdown(deadline)` | Emits frontend `CopyDone` (`'c'`), drains backend `CopyDone`, `CommandComplete`, and `ReadyForQuery('I')`, and closes socket cleanly. |

## Verification

- Initial Red baseline: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests`
- Targeted Green + Clippy gate:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests copyboth
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests replication_identity
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
  ```

## Acceptance Criteria

- [ ] `protocol.rs` shares one bounded `read_frame` helper and decodes replication `'W'` (`CopyBothResponse`, `format == 0`, `0` columns) while leaving `read_message` and `postgres-protocol` decoding intact (R22).
- [ ] `PostgresReplicationConnection` uses an isolated `replication=database` session, validates pre-existing `pgoutput` slots/publications and `start_lsn` bounds without creating/dropping slots, emits exact `proto_version '1'` `START_REPLICATION` SQL without `two_phase`, and drains or discards on shutdown (R22).
- [ ] Commit message follows `feat(postgres): establish logical CopyBoth sessions` once verified.

## Notes & Discoveries

- Pre-existing caller-provisioned logical slots and publications are mandatory; `probe_identity` is reused directly by `AsyncPostgresClient.replication_identity` in `ch23:t03` [^npd-contracts].

[^npd-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Native PostgreSQL public contracts for dedicated replication sessions.
[^npd-proto-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL Protocol 3.0 CopyBoth and pgoutput v1 startup constraints.
[^pg-conn-rs]: `src/rs/vertebra-db/src/connection.rs::TransportStream` — Shared transport stream and connection lifecycle traits.
