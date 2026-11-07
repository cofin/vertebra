---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch22-copyboth-and-pgoutput-decoding
title: "Chapter 22: Dedicated CopyBoth Logical Replication Sessions and pgoutput v1 Decoding"
description: Establish dedicated CopyBoth logical replication sessions and decode bounded pgoutput v1 relation, tuple, and transaction change envelopes in vertebra-postgres.
status: stable
state: planned
planning_status: complete
depends_on_chapters:
  - ch21-live-notifications
replan_trigger: null
current_task: "native-postgres-driver:ch22:t01"
plan_revision: 6
plan_commit: 8833b064
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-07T05:00:00Z"
generated:
  by: flow-revise
  at: "2026-11-07T05:00:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-07T05:00:00Z"
    commit: dd432f18
sources:
  - id: npd-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Native PostgreSQL public contracts for CDC, values, and dedicated sessions
  - id: npd-proto-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL Protocol 3.0 CopyBoth and pgoutput v1 wire constraints
  - id: pg-core-rs
    resource: src/rs/vertebra-db/src/postgres/mod.rs::PostgresPool
    title: Shared PostgreSQL wire framing substrate in vertebra-db
  - id: cdc-domain
    resource: .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
    title: Adapter-owned CDC envelopes, unchanged TOAST invariants, and int64 event IDs
tags:
  - postgres
  - replication
  - pgoutput
  - rust
  - cdc
---

# Chapter 22: Dedicated CopyBoth Logical Replication Sessions and pgoutput v1 Decoding

<!-- truth: start -->
- **Dedicated `CopyBoth` Ownership**: `PostgresReplicationConnection` dials an isolated direct/session `PostgresConnection` with `replication=database`, verifies pre-existing caller-owned `pgoutput` slots and publications on a short-lived metadata probe without creating or dropping slots, and enters `CopyBothResponse` (`'W'`) via `START_REPLICATION ... LOGICAL ... (proto_version '1', publication_names '...', binary 'false', streaming 'false', messages 'true')` [^npd-contracts] [^npd-proto-research].
- **Single Bounded Frame Reader**: `protocol.rs` factors frame length checking into one shared `read_frame` helper, adding only tag `'W'` (`CopyBothResponse`, overall format `0`, `0` columns) for replication while delegating all standard tags to `postgres-protocol` [^npd-proto-research] [^pg-core-rs].
- **Lossless `pgoutput` v1 Tuples & Explicit TOAST**: `PgOutputDecoder` maintains an immutable-snapshot `RelationCache` (capped at `4096` relations), distinguishes missing attributes, SQL `NULL` (`'n'`), unchanged TOAST (`'u'` -> `PgUnchangedToast`), text (`'t'`), and binary (`'b'`) values, and emits bounded `ReplicationRecord` envelopes (`<= 16 MiB` per record, `<= 1024` queued records / `8 MiB`) without buffering whole transactions [^npd-contracts] [^cdc-domain].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

PostgreSQL logical replication requires a dedicated `replication=database` Protocol 3.0 session that negotiates `CopyBothResponse` (`'W'`) — a backend tag absent from upstream `postgres-protocol =0.6.12` — and streams binary `pgoutput` v1 logical decoding messages (`Relation`, `Type`, `Begin`, `Origin`, `Insert`, `Update`, `Delete`, `Truncate`, `Message`, `Commit`) [^npd-proto-research]. Chapter 22 establishes `PostgresReplicationConnection` startup and preflight validation, implements incremental `PgOutputDecoder` relation/type/tuple decoding with explicit `PgUnchangedToast` preservation, and orders bounded transaction and DML change envelopes ready for commit-fenced LSN feedback in Chapter 23 [^npd-contracts] [^cdc-domain].

### 1.2 Non-Goals

- `XLogData` (`'w'`), `PrimaryKeepalive` (`'k'`), and `StandbyStatusUpdate` (`'r'`) LSN feedback, reconnect loops, and PyO3 `AsyncPostgresReplication` handles (owned by Chapter 23).
- Python `PostgresChangeSource` and `PostgresCheckpointStore` orchestration (owned by Chapter 24).
- Automatic slot creation, slot drop, DDL replication, initial table snapshot export, or `pgoutput` v2–v4 streaming/two-phase transactions [^npd-contracts] [^npd-proto-research].

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md) and [contracts.md](../../contracts.md). Research: [protocol-and-execution_20261105.md](../../research/protocol-and-execution_20261105.md). Skills: [runtime-ipc](../../../../../skills/runtime-ipc/SKILL.md), [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md), and [python-framework](../../../../../skills/python-framework/SKILL.md). Knowledge: [query-pipeline-cdc-and-tenancy.md](../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md) and [adr-004-optional-driver-wheels-and-shm-ownership.md](../../../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R22.1 (`ch22:t01`)**: Factor `protocol.rs` framing into one bounded `read_frame` with replication-only `'W'` (`CopyBothResponse`) decoding; implement `PostgresReplicationConnection` over a dedicated `replication=database` socket, separate ordinary-session slot/publication preflight (`plugin = 'pgoutput'`, inactive slot, valid `restart_lsn` / `confirmed_flush_lsn` window), `IDENTIFY_SYSTEM`, and `START_REPLICATION SLOT ... LOGICAL ...` startup/shutdown.
- **R22.2 (`ch22:t02`)**: Implement incremental `PgOutputDecoder` (`push`, `next_record`) with atomic `Arc<PgOutputRelationSchema>` replacement in a bounded `4096`-entry `RelationCache`, `Type` (`'Y'`) invalidation, metadata-session publication type preloading, shared scalar/container value decoding, and adapter-owned pure-Python `PgUnchangedToast`, `PostgresReplicationIdentity`, and `PostgresReplicationRecord` structs in `src/py/vertebra/db/adapters/postgres/types.py`.
- **R22.3 (`ch22:t03`)**: Implement transaction state machine and bounded `ReplicationRecord` envelopes across `Begin` (`'B'`), `Origin` (`'O'`), `Insert` (`'I'`), `Update` (`'U'` with `'K'`/`'O'`/`'N'`), `Delete` (`'D'`), `Truncate` (`'T'`), transactional/non-transactional `Message` (`'M'`), and `Commit` (`'C'`), enforcing `16 MiB` max record bytes and `1024`-record / `8 MiB` queue bounds plus stable `(system_identifier, slot, begin_final_lsn, transaction_ordinal)` deduplication coordinates.

### 2.2 Concrete Type & Signature Contracts

- Rust (`src/rs/vertebra-postgres/src/protocol.rs` & `src/rs/vertebra-postgres/src/replication/mod.rs`):
  - `pub enum ReplicationBackendMessage { CopyBothResponse { format: u8, column_formats: Vec<u16> }, Standard(postgres_protocol::message::backend::Message) }`
  - `pub struct VerifiedReplicationIdentity { pub system_identifier: String, pub timeline: i32, pub database: String, pub slot: String, pub publication_fingerprint: String, pub confirmed_flush_lsn: u64, pub restart_lsn: u64 }`
  - `pub fn format_pg_lsn(lsn: u64) -> String` and `pub fn parse_pg_lsn(text: &str) -> Result<u64, crate::errors::PostgresError>`
  - `impl PostgresReplicationConnection { pub async fn probe_identity(config: &crate::config::PostgresConfig, slot: &str, publication: &str) -> Result<VerifiedReplicationIdentity, crate::errors::PostgresError>; pub async fn connect_logical(config: &crate::config::PostgresConfig, slot: &str, publication: &str, start_lsn: Option<u64>) -> Result<Self, crate::errors::PostgresError>; pub async fn shutdown(&mut self, deadline: Instant) -> Result<(), crate::errors::PostgresError>; }` (raw `crate::config::PostgresConfig` carries no `pooler_mode` field and always dials a dedicated non-pooled `replication=database` connection; `PoolerMode` lives in `crate::session::statements::PoolerMode` / `SessionHandle` and Python `PostgreSQLConfig.derive_direct_config()`, which reject non-direct/session pooler modes before opening replication).
- Rust (`src/rs/vertebra-postgres/src/replication/pgoutput.rs`):
  - `pub enum PgOutputTupleValue { Null, UnchangedToast, Value(PgValue) }`
  - `pub struct PgOutputDecoder { ... }` with `pub fn new(max_relations: usize, max_record_bytes: usize) -> Self`, `pub fn push(&mut self, payload: &[u8]) -> Result<(), crate::errors::PostgresError>`, `pub fn next_record(&mut self, identity: &VerifiedReplicationIdentity, catalog: &crate::types::TypeCatalog) -> Result<Option<ReplicationRecord>, crate::errors::PostgresError>`, `pub fn reset_on_disconnect(&mut self)`, and `pub fn finish_clean_eof(&self) -> Result<(), crate::errors::PostgresError>`.
  - `pub enum ReplicationRecord { Begin { ... }, Insert { ... }, Update { ... }, Delete { ... }, Truncate { ... }, Message { ... }, Commit { ... } }`
- Python (`src/py/vertebra/db/adapters/postgres/types.py`):
  - `class PgUnchangedToast(msgspec.Struct, frozen=True, gc=False): pass` (`PG_UNCHANGED_TOAST: Final = PgUnchangedToast()`)
  - `class PostgresReplicationIdentity(msgspec.Struct, frozen=True, gc=False): system_identifier: str; database: str; slot: str; publication_fingerprint: str` with `@property def identity_key(self) -> tuple[str, str, str, str]`
  - `class PostgresReplicationRecord(msgspec.Struct, frozen=True, gc=False): kind: Literal["control", "begin", "insert", "update", "delete", "truncate", "message", "commit"]; payload: dict[str, Any]; commit_end_lsn: int | None = None; generation: int = 0`

### 2.3 Caller -> Seam -> Collaborator Map

- `AsyncPostgresClient` (`ch23:t03`) -> `PostgresReplicationConnection::{probe_identity, connect_logical}` -> ordinary preflight `PostgresConnection` (`pg_replication_slots`, `pg_publication`, `crate::types::TypeCatalog` preload) + dedicated non-pooled `replication=database` `PostgresConnection` (`IDENTIFY_SYSTEM`, `START_REPLICATION`, `protocol::read_replication_message`).
- Replication stream reader (`ch23:t01`) -> `PgOutputDecoder::{push, next_record}` -> `RelationCache` (`Arc<PgOutputRelationSchema>`) + `crate::codecs::decode_value_with_catalog` (`src/rs/vertebra-postgres/src/codecs/mod.rs`) + `crate::types::TypeCatalog` (`src/rs/vertebra-postgres/src/types.rs`) + `types.py::{PgUnchangedToast, PostgresReplicationRecord}`.

### 2.4 Edge-Case & Failure-Mode Matrix

| Condition | Required Behavior |
| --- | --- |
| Missing slot/publication, `plugin != "pgoutput"`, wrong database, or active slot (`55006`) | Reject during preflight or `START_REPLICATION` preserving exact server SQLSTATE or typed `OperationalError`; never create or alter slots. |
| Requested `start_lsn < restart_lsn` or `start_lsn < confirmed_flush_lsn` | Fail closed with `OperationalError` (`wal_unavailable` / invalid resume) before streaming. |
| `CopyBothResponse` (`'W'`) with `format != 0` or `num_columns != 0` | Reject boundedly as `ProtocolError` and close dedicated socket. |
| `TupleData` with `'u'` (unchanged TOAST) vs `'n'` (SQL `NULL`) vs dropped column | Emit `PgOutputTupleValue::UnchangedToast` (`PgUnchangedToast`), `Null` (`None`), and omit dropped attributes respectively; reject `PgUnchangedToast` if passed as a SQL query bind. |
| `Relation` (`'R'`) schema update mid-stream or `> 4096` relations | Atomically swap new `Arc<PgOutputRelationSchema>` without mutating previously emitted records; fail explicitly with resource-limit `OperationalError` above `4096`. |
| DML outside `Begin`..`Commit`, nested `Begin`, or v2+ streaming/two-phase tags | Fail immediately with `ProtocolError` without advancing LSN feedback. |
| Clean `CopyDone` with partial buffered record vs transport EOF mid-transaction | Clean `CopyDone` with trailing bytes raises `ProtocolError`; transport EOF calls `reset_on_disconnect()` discarding partial record/transaction state for reconnect replay. |

### 2.5 Mandatory Alternatives Comparison

| Decision | Chosen Design | Rejected Alternative & Why |
| --- | --- | --- |
| `'W'` (`CopyBothResponse`) framing | Thin wrapper over one shared bounded `read_frame` delegating all non-`'W'` tags to `postgres-protocol` | Forking `postgres-protocol` or duplicating standard `ErrorResponse`/`RowDescription` parsers, creating drift and double maintenance. |
| Transaction staging in Rust | Emit bounded per-record `Begin`..`Commit` envelopes holding only header metadata | Materializing whole multi-GB transactions in Rust memory before emission, violating bounded memory invariants. |
| Unchanged TOAST representation | Dedicated `PgUnchangedToast` record excluded from `PostgresValue` SQL binds | Mapping `'u'` to `None` or empty `bytes`, conflating unchanged TOAST columns with explicit `SET col = NULL` updates. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in ch22]` (user guides and OKF domain chapters consolidate in `ch30:t01` and `ch30:t02`; Google-style docstrings on `PgUnchangedToast`, `PostgresReplicationIdentity`, and `PostgresReplicationRecord` in `src/py/vertebra/db/adapters/postgres/types.py` land in `ch22:t02`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R22 (R22.1) | [t01](tasks/t01-copyboth-replication-session.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs` |
| R22 (R22.2) | [t02](tasks/t02-pgoutput-relation-and-tuple-decoding.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/postgres_pgoutput_tests.rs` |
| R22 (R22.3) | [t03](tasks/t03-pgoutput-transaction-envelopes.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/postgres_pgoutput_tests.rs` |

## 5. Implementation Plan

- [ ] native-postgres-driver:ch22:t01 — Start dedicated CopyBoth logical replication sessions (tasks/t01-copyboth-replication-session.md)
- [ ] native-postgres-driver:ch22:t02 — Decode pgoutput relation metadata and lossless tuples (tasks/t02-pgoutput-relation-and-tuple-decoding.md)
- [ ] native-postgres-driver:ch22:t03 — Order logical transactions and change envelopes (tasks/t03-pgoutput-transaction-envelopes.md)

**Chapter Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests --test postgres_pgoutput_tests && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `HEAD @ dd432f18`; chapter `state: planned`, `planning_status: complete`.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03` (`3/3` pending).
- **Downstream Dependents**: Unblocks `ch23-replication-feedback-resume-and-bridge` and `ch24-change-source-and-checkpoints`.

[^npd-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Native PostgreSQL public contracts for CDC, values, and dedicated sessions.
[^npd-proto-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL Protocol 3.0 CopyBoth and pgoutput v1 wire constraints.
[^pg-core-rs]: `src/rs/vertebra-db/src/postgres/mod.rs::PostgresPool` — Shared PostgreSQL wire framing substrate in vertebra-db.
[^cdc-domain]: `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` — Adapter-owned CDC envelopes, unchanged TOAST invariants, and int64 event IDs.
