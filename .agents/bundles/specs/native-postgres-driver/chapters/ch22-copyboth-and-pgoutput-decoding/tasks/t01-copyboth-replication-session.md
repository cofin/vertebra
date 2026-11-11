---
type: Task
id: "native-postgres-driver:ch22:t01"
chapter_id: ch22-copyboth-and-pgoutput-decoding
title: Start dedicated CopyBoth logical replication sessions
description: Factor bounded CopyBothResponse framing in protocol.rs and establish dedicated replication=database PostgresReplicationConnection sessions with catalog preflight and IDENTIFY_SYSTEM validation.
status: stable
state: completed
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
plan_revision: 6
plan_commit: 8e9b324e
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-11T01:52:41Z"
last_verified_commit: 81048251
verification_evidence: "cargo test -p vertebra-postgres --test postgres_replication_wire_tests (9 passed), cargo fmt --check, and cargo clippy --workspace --all-targets --features tls,compression,gcp -- -D warnings exited 0"
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-11T01:53:00Z"
commit: 81048251
generated:
  by: flow-revise
  at: "2026-11-10T14:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-10T14:45:00Z"
    commit: 8e9b324e
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
  - Factor bounded tag + 4-byte length validation, timeout enforcement, and payload slicing into `pub async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R, scratch: &mut BytesMut, max_frame_bytes: usize, deadline: Option<Instant>) -> Result<(u8, BytesMut), crate::errors::PostgresError>`, preserving the exact `deadline: Option<Instant>` semantics of `read_message`.
  - Keep `read_message(..., deadline: Option<Instant>)` signature and `postgres_protocol::message::backend::Message::parse` behavior unchanged for all standard sessions.
  - Add `pub enum ReplicationBackendMessage { CopyBothResponse { format: u8, column_formats: Vec<u16> }, Standard(postgres_protocol::message::backend::Message) }` and `pub async fn read_replication_message<R: AsyncRead + Unpin>(reader: &mut R, scratch: &mut BytesMut, max_frame_bytes: usize, deadline: Option<Instant>) -> Result<ReplicationBackendMessage, crate::errors::PostgresError>`:
    - On tag `b'W'`, validate exact payload length `>= 3` (`1` format byte + `2`-byte `u16` column count + `2 * count` bytes), consume exact payload, and require `format == 0` and `column_formats.is_empty()` for logical replication; reject truncated, trailing, nonzero-format, or nonzero-column frames boundedly.
    - Delegate every non-`b'W'` frame directly to `postgres_protocol::message::backend::Message::parse` without duplicating standard decoders.
- `src/rs/vertebra-postgres/src/replication/mod.rs` (wired in `src/rs/vertebra-postgres/src/lib.rs` via `pub mod replication;` and `pub(crate) fn register_pymodule(_m: &Bound<'_, PyModule>) -> PyResult<()>`):
  - `pub struct VerifiedReplicationIdentity { pub system_identifier: String, pub timeline: i32, pub database: String, pub slot: String, pub publication_fingerprint: String, pub confirmed_flush_lsn: u64, pub restart_lsn: u64 }`
  - `pub fn format_pg_lsn(lsn: u64) -> String` (uppercase hex `"{hi:X}/{lo:X}"` over `(lsn >> 32) as u32` and `lsn as u32`) and `pub fn parse_pg_lsn(raw: &str) -> Result<u64, crate::errors::PostgresError>`.
  - `pub struct PostgresReplicationConnection` owning a dedicated non-pooled `PostgresConnection` dialed via `crate::session::establish_standalone_connection` from `ch21:t01` (preserving Cloud SQL, AlloyDB, direct-IAM, TLS, SCRAM-SHA-256-PLUS, Unix socket, and `connect_timeout` startup):
    - Both `probe_identity` and `connect_logical` inspect `config.pooler_mode` first and reject `PoolerMode::Transaction`, `PoolerMode::Statement`, and `PoolerMode::Auto` (requiring explicit `PoolerMode::Direct | PoolerMode::Session`) with `PostgresError::Interface` before dialing any socket (matching Python `PostgreSQLConfig.derive_direct_config()`).
    - `pub async fn probe_identity(config: &crate::config::PostgresConfig, slot: &str, publication: &str) -> Result<VerifiedReplicationIdentity, crate::errors::PostgresError>` — opens a short-lived ordinary metadata connection via `crate::session::establish_standalone_connection(config, false)` plus a dedicated `replication=database` probe via `crate::session::establish_standalone_connection(config, true)` (`IDENTIFY_SYSTEM` without `START_REPLICATION`), validates `pg_replication_slots` (`database` match, `slot_type == "logical"`, `plugin == "pgoutput"`, `active == false`), computes deterministic `publication_fingerprint` via one parameterized `LEFT JOIN` query (`SELECT p.pubname, p.puballtables, p.pubinsert, p.pubupdate, p.pubdelete, p.pubtruncate, p.pubviaroot, pt.schemaname, pt.tablename` plus `, pt.attnames, pt.rowfilter` when `server_version_num >= 150_000` `FROM pg_catalog.pg_publication p LEFT JOIN pg_catalog.pg_publication_tables pt ON pt.pubname = p.pubname WHERE p.pubname = $1 ORDER BY pt.schemaname ASC NULLS FIRST, pt.tablename ASC NULLS FIRST`, zero correlated scalar subqueries, `0` rows failing closed as missing publication and `>= 1` rows including empty publications hashed via length-prefixed SHA-256 lowercase hex), and closes both sockets in all outcomes.
    - `pub async fn connect_logical(config: &crate::config::PostgresConfig, slot: &str, publication: &str, start_lsn: Option<u64>) -> Result<Self, crate::errors::PostgresError>` — runs preflight validation on `establish_standalone_connection(config, false)` and closes that metadata socket before dialing `establish_standalone_connection(config, true)`, checks `start_lsn` is neither `< restart_lsn` nor `< confirmed_flush_lsn`, validates and quotes slot identifier and publication option literal, issues `IDENTIFY_SYSTEM` then simple `Query`:
      `START_REPLICATION SLOT "<slot>" LOGICAL <X/Y> (proto_version '1', publication_names '<pub>', binary 'false', streaming 'false', messages 'true')` (using `0/0` when `start_lsn` is `None` so PostgreSQL starts from slot `confirmed_flush_lsn`, with zero `two_phase` option), awaits `CopyBothResponse` via `read_replication_message` under `connect_timeout` deadline, and stores `VerifiedReplicationIdentity`.
    - `pub async fn shutdown(&mut self, deadline: Instant) -> Result<(), crate::errors::PostgresError>` — sends `CopyDone`, drains `CopyDone` / `CommandComplete` / `ReadyForQuery` within `deadline`, or closes and discards the physical socket on timeout/error.

## Edge Cases & Error Semantics

- Reject `PoolerMode::Transaction`, `PoolerMode::Statement`, and `PoolerMode::Auto` on `config.pooler_mode` (requiring explicit `PoolerMode::Direct | PoolerMode::Session`) with `PostgresError::Interface` before performing DNS/socket setup or catalog queries.
- Never create, alter, or drop replication slots or publications, and never run an initial snapshot export; missing slot or publication (`0` rows from the `pg_publication LEFT JOIN pg_publication_tables` query) fails closed preserving SQLSTATE `42704` (`UndefinedObject`) or explicit `OperationalError`, whereas an empty existing publication (`1` row with `NULL` table columns) yields a stable SHA-256 `publication_fingerprint`.
- Active slot (`active == true` in preflight or SQLSTATE `55006` `ObjectInUse` on `START_REPLICATION`), wrong `plugin` (e.g. `test_decoding`), wrong database, physical slot (non-`"logical"` `slot_type`), `start_lsn < restart_lsn` (WAL removed), `start_lsn < confirmed_flush_lsn` (stale rewind), or `start_lsn` beyond server current WAL preserves exact SQLSTATE or raises clear `OperationalError`.
- Identifier validation rejects NUL bytes, empty slot/publication names, or unquotable control characters before opening network sockets; SQL quoting escapes embedded `"` in identifiers and `'` / `\` in option literals so option injection is impossible.
- Malformed `'W'` frame (`len < 7`, `format == 1`, `num_columns > 0`, or trailing bytes) or expired `deadline` fails boundedly as `ProtocolError` / timeout `OperationalError` and drops the dedicated socket.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs` (scripted raw TCP Protocol 3.0 peer, zero query-pool mocks).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Non-session `PoolerMode` rejected pre-dial | `probe_identity` and `connect_logical` with `PoolerMode::Transaction`, `PoolerMode::Statement`, and `PoolerMode::Auto` | Returns `PostgresError::Interface` immediately with `0` accepted TCP connections on the listener. |
| Valid `CopyBoth` startup & `LEFT JOIN` fingerprint | `connect_logical` with `slot = "vertebra_slot"`, `publication = "vertebra_pub"`, `start_lsn = Some(0x0000_0001_0000_01A0)` against scripted PG15+ peer (plus empty-publication variant) | Ordinary preflight connection binds `$1 = "vertebra_pub"` on the single `pg_publication LEFT JOIN pg_publication_tables` query (`attnames`/`rowfilter` included on `>= 150_000`), distinguishes empty publication (`1` `NULL`-table row) from missing publication (`0` rows), then replication socket sends `replication = "database"`, `IDENTIFY_SYSTEM`, and exact `START_REPLICATION SLOT "vertebra_slot" LOGICAL 1/1A0 (proto_version '1', publication_names 'vertebra_pub', binary 'false', streaming 'false', messages 'true')`, receiving `'W'` (`format=0, cols=0`). |
| Identity-only probe (`probe_identity`) | `probe_identity(opts, "vertebra_slot", "vertebra_pub")` | Runs catalog preflight + `IDENTIFY_SYSTEM` via `establish_standalone_connection`, never emits `START_REPLICATION`, closes sockets to EOF, returns `VerifiedReplicationIdentity`. |
| Missing slot / wrong plugin / active slot | Peer returns `plugin = "test_decoding"`, `active = true` (`55006`), or `0` publication rows / `ErrorResponse(42704)` | Fails closed with exact `OperationalError` / SQLSTATE `55006` or `42704`; sockets closed. |
| Resume LSN behind `restart_lsn` or `confirmed_flush_lsn` | Slot `restart_lsn = "1/200"`, `confirmed_flush_lsn = "1/300"`, caller passes `start_lsn = Some(0x1_0000_0150)` | Fails before `START_REPLICATION` with `OperationalError` naming unusable WAL / checkpoint rewind. |
| Malformed `'W'` or expired `deadline` | Peer sends `'W'` with `format = 1`, `column_count = 2`, truncated length, or stalls past `deadline` | `read_replication_message` returns `ProtocolError` or timeout `OperationalError` without unbounded allocation; connection closes. |
| Clean `CopyDone` shutdown | Active `PostgresReplicationConnection::shutdown(deadline)` | Emits frontend `CopyDone` (`'c'`), drains backend `CopyDone`, `CommandComplete`, and `ReadyForQuery('I')`, and closes socket cleanly. |

## Verification

- Initial Red baseline: `PYTHONPATH="$(pwd)/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests`
- Targeted Green + Clippy gate:
  ```bash
  PYTHONPATH="$(pwd)/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests copyboth
  PYTHONPATH="$(pwd)/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests replication_identity
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
  ```

## Acceptance Criteria

- [x] `protocol.rs` shares one deadline-aware bounded `read_frame(..., deadline: Option<Instant>)` helper and decodes replication `'W'` (`CopyBothResponse`, `format == 0`, `0` columns) while leaving `read_message` and `postgres-protocol` decoding intact (R22).
- [x] `PostgresReplicationConnection` rejects `PoolerMode::Transaction | Statement | Auto` (requiring explicit `PoolerMode::Direct | Session`) with `PostgresError::Interface` before dialing, reuses `session::establish_standalone_connection(config, false/true)`, hashes `publication_fingerprint` via one parameterized `LEFT JOIN` query without creating/dropping slots, emits exact `proto_version '1'` `START_REPLICATION` SQL without `two_phase`, and drains or discards on shutdown (R22).
- [x] Commit message follows `feat(postgres): establish logical CopyBoth sessions` once verified.

## Notes & Discoveries

- Pre-existing caller-provisioned logical slots and publications are mandatory; `probe_identity` is reused directly by `AsyncPostgresClient.replication_identity` in `ch23:t03` [^npd-contracts].

[^npd-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Native PostgreSQL public contracts for dedicated replication sessions.
[^npd-proto-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL Protocol 3.0 CopyBoth and pgoutput v1 startup constraints.
[^pg-conn-rs]: `src/rs/vertebra-db/src/connection.rs::TransportStream` — Shared transport stream and connection lifecycle traits.
