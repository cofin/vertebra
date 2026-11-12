---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch22-copyboth-and-pgoutput-decoding
title: "Chapter 22: Dedicated CopyBoth Sessions, pgoutput v1 Decoding and Transaction Envelopes, and the Split-State Limitations Record"
description: Establish dedicated CopyBoth logical replication sessions, decode bounded pgoutput v1 relation, tuple, and transaction change envelopes in vertebra-postgres, and record the split native-wheel/legacy-driver state on main in the PostgreSQL limitations OKF entry.
status: draft
state: completed
planning_status: complete
depends_on_chapters:
  - ch21-live-notifications
replan_trigger: null
current_task: null
plan_revision: 7
plan_commit: c8722c76
state_revision: 3
last_verified_checkpoint: 9c1865b7
last_reviewed_commit: 9c1865b7
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-12T03:49:28Z"
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
    title: Native PostgreSQL public contracts for CDC, values, and dedicated sessions
  - id: npd-proto-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL Protocol 3.0 CopyBoth and pgoutput v1 wire constraints
  - id: pg-core-rs
    resource: src/rs/vertebra-db/src/postgres/mod.rs::encode_query_frame
    title: Shared PostgreSQL wire framing substrate in vertebra-db
  - id: pgoutput-rs
    resource: src/rs/vertebra-postgres/src/replication/pgoutput.rs::PgOutputDecoder
    title: Incremental pgoutput v1 decoder and ordered transaction record state machine
  - id: cdc-domain
    resource: .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
    title: Adapter-owned CDC envelopes, unchanged TOAST invariants, and int64 event IDs
  - id: pg-driver-py
    resource: src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver
    title: Python drivers still executing through psycopg_pool and asyncio.to_thread on main
  - id: storage-limitations
    resource: .agents/bundles/knowledge/domains/storage/object-storage-limitations.md
    title: House format for a domain limitations OKF Gotcha entry
tags:
  - postgres
  - replication
  - pgoutput
  - rust
  - cdc
  - limitations
---

# Chapter 22: Dedicated CopyBoth Sessions, pgoutput v1 Decoding and Transaction Envelopes, and the Split-State Limitations Record

<!-- truth: start -->
- **Dedicated `CopyBoth` Ownership (on main)**: `PostgresReplicationConnection` dials an isolated direct/session connection with `replication=database`, verifies caller-owned `pgoutput` slots and publications on a short-lived metadata probe without creating or dropping slots, and enters `CopyBothResponse` (`'W'`) via `START_REPLICATION ... LOGICAL ... (proto_version '1', publication_names '...', binary 'false', streaming 'false', messages 'true')` [^npd-contracts] [^npd-proto-research].
- **Relation, Type, and Insert Decoding (on main)**: `PgOutputDecoder::decode_next_frame` maintains an `Arc<PgOutputRelationSchema>` relation cache capped at `4096`, reconciles `Type` (`'Y'`) declarations, and decodes `Insert` (`'I'`) tuples distinguishing SQL `NULL`, unchanged TOAST (`PgUnchangedToast`), text, and binary values [^pgoutput-rs] [^cdc-domain].
- **Ordered Transaction Envelopes (`ch22:t03`, completed)**: The decoder adds `Begin`, `Origin`, `Update`, `Delete`, `Truncate`, `Message`, and `Commit` frames and a `next_record` state machine emitting bounded `ReplicationRecord` envelopes (`<= 16 MiB` per record, `<= 1024` queued records / `8 MiB`, with one individually valid record up to `16 MiB` admitted when the queue is empty) with stable `(system_identifier, slot, begin_final_lsn, transaction_ordinal)` coordinates and no whole-transaction buffering [^npd-contracts] [^npd-proto-research].
- **Split-State Limitations Record (`ch22:t04`, completed)**: `.agents/bundles/knowledge/domains/data/postgres-limitations.md` records that `main` (`c8722c76`) carries the native wheel while the Python drivers, queue guard, dialer helpers, and driver tests still use `psycopg`/`asyncpg`, and that the PR #104 description overstates the cutover; both knowledge indexes register the entry [^pg-driver-py] [^storage-limitations].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

PostgreSQL logical replication requires a dedicated `replication=database` Protocol 3.0 session that negotiates `CopyBothResponse` (`'W'`) and streams binary `pgoutput` v1 messages (`Relation`, `Type`, `Begin`, `Origin`, `Insert`, `Update`, `Delete`, `Truncate`, `Message`, `Commit`) [^npd-proto-research]. `ch22:t01` and `ch22:t02` are on `main` in squash commit `c8722c76`: session startup, preflight, relation/type state, and `Insert` tuples decode today, while local commit `2947d015` adds transaction frames, ordered record envelopes, and the bounded queue [^pgoutput-rs]. Chapter 23 supplies commit-fenced LSN feedback.

`main` also carries a split runtime state: the native wheel is installed and tested, but `SyncPostgreSQLDriver` runs on `psycopg_pool`, `AsyncPostgreSQLDriver` offloads it with `asyncio.to_thread`, and `PostgreSQLQueueBackend` probes `psycopg` [^pg-driver-py]. `ch22:t04` records that state in the PostgreSQL limitations OKF entry on the working branch so maintainers see the real boundary until `ch27:t04` removes those caveats.

### 1.2 Non-Goals

- `XLogData` (`'w'`), `PrimaryKeepalive` (`'k'`), and `StandbyStatusUpdate` (`'r'`) feedback, reconnect loops, and PyO3 `AsyncPostgresReplication` handles (Chapter 23).
- Python `PostgresChangeSource` and `PostgresCheckpointStore` orchestration (Chapter 24).
- Automatic slot creation or drop, DDL replication, initial snapshot export, or `pgoutput` v2–v4 streaming/two-phase transactions [^npd-contracts].
- Changing driver, queue, dialer, or test code while recording the split state (Chapter 27), and the final limitations content for runtime caveats (`ch31:t01`).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md) and [contracts.md](../../contracts.md). Research: [protocol-and-execution_20261105.md](../../research/protocol-and-execution_20261105.md). Skills: [runtime-ipc](../../../../../skills/runtime-ipc/SKILL.md) and [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md). Knowledge: [query-pipeline-cdc-and-tenancy.md](../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md), [database-drivers-and-dialers.md](../../../../knowledge/domains/data/database-drivers-and-dialers.md), and [object-storage-limitations.md](../../../../knowledge/domains/storage/object-storage-limitations.md).

| Current symbol and upstream boundary | Verified consequence |
| --- | --- |
| `src/rs/vertebra-postgres/src/replication/pgoutput.rs::PgOutputFrame` | `Relation`, `Type`, `Insert`, `Begin`, `Origin`, `Update`, `Delete`, `Truncate`, `Message`, and `Commit`; transaction frames are local additions in `2947d015`. |
| `src/rs/vertebra-postgres/src/replication/pgoutput.rs::PgOutputDecoder::decode_next_frame` | `(&mut self, catalog: &mut TypeCatalog, codec_ctx: &CodecContext) -> Result<Option<PgOutputFrame>, PostgresError>`; unknown tags fail with `PostgresError::Protocol`. |
| `src/rs/vertebra-postgres/src/replication/pgoutput.rs::PgOutputDecoder::reset_on_disconnect` | Clears buffered bytes, relation cache, and active transaction for replay. |
| `src/rs/vertebra-db/src/postgres/mod.rs::encode_query_frame` [^pg-core-rs] | Shared stateless frame helpers; replication framing stays in `vertebra-postgres`. |
| `src/rs/vertebra-postgres/src/replication/mod.rs::pgoutput_tuple_to_python_dict` | Existing tuple projection reused by `ch23:t03` for record payloads. |
| `src/py/vertebra/db/adapters/postgres/types.py::PostgresReplicationRecord` | Kinds `control`, `begin`, `insert`, `update`, `delete`, `truncate`, `message`, `commit` already validated in Python. |

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R22.1 (`ch22:t01`, completed)**: Shared bounded `read_frame`, replication-only `'W'` decoding, pooler-mode rejection, `PostgresReplicationConnection` preflight and `START_REPLICATION`.
- **R22.2 (`ch22:t02`, completed)**: `PgOutputDecoder` relation cache, type reconciliation, four-state `Insert` tuples, and adapter `PgUnchangedToast`, `PostgresReplicationIdentity`, and `PostgresReplicationRecord` structs.
- **R22.3 (`ch22:t03`, completed)**: Extend `PgOutputFrame` and `decode_next_frame` with exact `proto.c` v1 layouts for `Begin` (`'B'`), `Origin` (`'O'`), `Update` (`'U'` with `'K'`/`'O'`/`'N'`), `Delete` (`'D'`), `Truncate` (`'T'`), transactional/non-transactional `Message` (`'M'`), and `Commit` (`'C'`); add `PgOutputDecoder::next_record` driving an `ActiveLogicalTx` state machine into bounded `ReplicationRecord` envelopes; add `BoundedReplicationQueue` (`1024` records / `8 MiB`); re-export the record types from `replication/mod.rs`.
- **R27.1 (`ch22:t04`, completed)**: Create `postgres-limitations.md` (OKF `Gotcha`) recording the split state on `main` with symbol-anchored provenance, and register it in `.agents/bundles/knowledge/index.md` and `.agents/bundles/knowledge/domains/index.md`.

### 2.2 Concrete Type & Signature Contracts

- Rust (`src/rs/vertebra-postgres/src/replication/pgoutput.rs`), added by `ch22:t03`:
  - `pub const DEFAULT_MAX_QUEUE_RECORDS: usize = 1024;` and `pub const DEFAULT_MAX_QUEUE_BYTES: usize = 8 * 1024 * 1024;`
  - `PgOutputFrame` gains `Begin { final_lsn: u64, commit_time_pg_us: i64, xid: u32 }`, `Origin { origin_lsn: u64, origin_name: String }`, `Update { relation, old_kind: Option<OldTupleKind>, old_tuple: Option<Vec<PgOutputTupleValue>>, new_tuple: Vec<PgOutputTupleValue> }`, `Delete { relation, old_kind: OldTupleKind, old_tuple: Vec<PgOutputTupleValue> }`, `Truncate { relations: Vec<Arc<PgOutputRelationSchema>>, cascade: bool, restart_identity: bool }`, `Message { transactional: bool, message_lsn: u64, prefix: String, content: Bytes }`, and `Commit { flags: u8, commit_lsn: u64, commit_end_lsn: u64, commit_time_pg_us: i64 }`.
  - `pub enum OldTupleKind { Key, Full }`, `pub struct ActiveLogicalTx { pub xid: u32, pub begin_final_lsn: u64, pub commit_time_pg_us: i64, pub origin_name: Option<String>, pub origin_lsn: Option<u64>, pub next_ordinal: u64 }`, `pub struct ReplicationDedupKey { pub system_identifier: String, pub slot: String, pub begin_final_lsn: u64, pub transaction_ordinal: u64 }`.
  - `pub enum ReplicationRecord { Begin { .. }, Insert { .. }, Update { .. }, Delete { .. }, Truncate { .. }, Message { dedup: Option<ReplicationDedupKey>, xid: Option<u32>, .. }, Commit { dedup, xid, flags, commit_lsn, commit_end_lsn, commit_time_pg_us, origin_name: Option<String>, origin_lsn: Option<u64> } }` with `pub fn approx_bytes(&self) -> usize`.
  - `impl PgOutputDecoder { pub fn next_record(&mut self, identity: &VerifiedReplicationIdentity, catalog: &mut TypeCatalog, codec_ctx: &CodecContext) -> Result<Option<ReplicationRecord>, PostgresError>; pub fn active_transaction(&self) -> Option<&ActiveLogicalTx>; }` — `Relation` and `Type` frames update decoder state inside `next_record` and emit no record.
  - `pub struct BoundedReplicationQueue { .. }` with `pub fn new(max_records: usize, max_bytes: usize, max_record_bytes: usize) -> Self`, `pub fn can_accept(&self, approx_bytes: usize) -> bool`, `pub fn push(&mut self, record: ReplicationRecord) -> Result<(), PostgresError>`, `pub fn pop(&mut self) -> Option<ReplicationRecord>`, `pub fn len(&self) -> usize`, `pub fn is_empty(&self) -> bool`.
- OKF (`.agents/bundles/knowledge/domains/data/postgres-limitations.md`), added by `ch22:t04`: frontmatter `type: Gotcha`, `title: PostgreSQL Native Driver Limitations`, `status: stable`, `tags`, `generated`, `verified` (source checkpoint at execution), `sources` with matching `[^id]:` footnotes; a `<!-- truth: start -->` block; and a `## Caller Guidance` section.

### 2.3 Caller -> Seam -> Collaborator Map

- Replication read task (`ch23:t01`) -> `PgOutputDecoder::{push, next_record}` -> `decode_next_frame` + `ActiveLogicalTx` + `RelationCache` + `crate::codecs` + `crate::types::TypeCatalog` -> `BoundedReplicationQueue::push`.
- `AsyncPostgresReplication.next` (`ch23:t03`) -> `ReplicationRecord` -> `replication/mod.rs::pgoutput_tuple_to_python_dict` -> `types.py::PostgresReplicationRecord`.
- Maintainers and agents reading `main` -> `knowledge/index.md` / `domains/index.md` -> `postgres-limitations.md` -> symbol-anchored split-state caveats.

### 2.4 Edge-Case & Failure-Mode Matrix

| Condition | Required Behavior |
| --- | --- |
| DML, `Origin`, `Commit`, or transactional `Message` outside `Begin`..`Commit`, or nested `Begin` | `PostgresError::Protocol` without advancing state. |
| Nonzero `Commit` flags, `commit_lsn != begin_final_lsn`, or `commit_end_lsn < commit_lsn` | `PostgresError::Protocol`. |
| `Truncate` with `nrelids == 0`, unknown option bits (`> 0x03`), or an uncached relation OID | `PostgresError::Protocol`. |
| `Message` flags outside `{0, 1}` or v2–v4 tags (`'S'`, `'E'`, `'A'`, `'c'`, `'b'`, `'P'`, `'K'`, `'r'`) | `PostgresError::Protocol`. |
| Non-transactional `Message` inside or outside a transaction | Emits immediately with `dedup = None`, `xid = active_tx.map(|t| t.xid)`, no commit boundary. |
| Record `> 16 MiB` or full queue | Oversized record fails with `PostgresError::Operational`; full queue returns `can_accept == false` and never drops; an empty queue always admits one record `<= max_record_bytes`. |
| Transport EOF mid-transaction | `reset_on_disconnect()` drops buffered bytes, relation cache, and `ActiveLogicalTx`; the next `Begin` restarts at ordinal `0`. |
| Split-state entry cites a symbol that no longer matches `main` | The task re-verifies each cited symbol with `git grep -n` at execution time and fails rather than recording an unverified claim. |

### 2.5 Mandatory Alternatives Comparison

| Decision | Chosen Design | Rejected Alternative & Why |
| --- | --- | --- |
| Envelope construction | `next_record` wraps the existing `decode_next_frame` parser and owns the transaction state machine | A second parser for transaction frames, duplicating bounds checks and tuple decoding. |
| Transaction staging | Bounded per-record envelopes carrying header metadata | Materializing whole transactions before emission, violating bounded memory. |
| Split-state disclosure | Domain limitations entry registered in both indexes, trimmed at cutover | A note in the spec only, which agents and maintainers reading `knowledge/` never see. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[3/3 files documented in ch22]`; `postgres-limitations.md`, `knowledge/index.md`, and `knowledge/domains/index.md` are synchronized in `48b50600`, contributing interim coverage to `[3/33 files documented]`. Chapters `ch27` and `ch31` still trim and complete these entries after the driver cutover. Rust doc comments cover the new `pgoutput.rs` record and queue types.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R22 (R22.1) | [t01](tasks/t01-copyboth-replication-session.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs` |
| R22 (R22.2) | [t02](tasks/t02-pgoutput-relation-and-tuple-decoding.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/postgres_pgoutput_tests.rs` |
| R22 (R22.3) | [t03](tasks/t03-pgoutput-transaction-envelopes.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/postgres_pgoutput_tests.rs` |
| R27 (R27.1) | [t04](tasks/t04-split-state-limitations-record.md) | `documentation_validation` | `make review-check`, frontmatter/link/symbol checks in the worksheet |

## 5. Implementation Plan

- [x] [c8722c76] native-postgres-driver:ch22:t01 — Start dedicated CopyBoth logical replication sessions (tasks/t01-copyboth-replication-session.md)
- [x] [c8722c76] native-postgres-driver:ch22:t02 — Decode pgoutput relation metadata and lossless tuples (tasks/t02-pgoutput-relation-and-tuple-decoding.md)
- [x] [2947d015] native-postgres-driver:ch22:t03 — Order logical transactions and change envelopes (tasks/t03-pgoutput-transaction-envelopes.md)
- [x] [48b50600] native-postgres-driver:ch22:t04 — Record the split native-wheel and legacy-driver state in the PostgreSQL limitations OKF entry (tasks/t04-split-state-limitations-record.md)

The `t01` and `t02` canonical task SHAs identify squash `c8722c76` on `main`; their worksheets preserve original PR #104 commits in `original_commit`. The `t03` and `t04` SHAs identify local follow-up commits.

**Chapter Recovery Checkpoint:** `PYO3_PYTHON="$(pwd)/.venv/bin/python" PYTHONPATH="$(pwd)/src/py:$(pwd)/.venv/lib/python3.12/site-packages" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests --test postgres_pgoutput_tests && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings && make review-check` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Chapter State**: All four tasks are verified and committed; correctness and fresh quality reviews approve the code and synchronized Markdown at `9c1865b7`. Chapter state is `completed`.
- **Local Implementation**: `t03` (`2947d015`) supplies transaction frames, ordered records, and bounded queue; `t04` (`48b50600`) supplies the three-file limitations record. The upstream `t01` / `t02` worksheets use canonical squash `c8722c76` and preserve `81048251` / `64288c63` as `original_commit` provenance.
- **Verification**: Focused pgoutput and replication wire tests, all-target Clippy, and the full `vertebra-postgres --tests` aggregate pass. Documentation structure, source anchors, review manifests, and privacy checks pass. Live PostgreSQL, wheels, and free-threaded acceptance remain downstream gates.
- **Remaining Tasks in Chapter**: None; the chapter is reviewed and complete. Flow progress is `82/119` completed tasks, `37` remaining across `ch23`–`ch33`; `22/33` chapters hold completed review stamps.
- **Downstream Dependents**: With Chapter 22 reviewed, `ch23-replication-feedback-resume-and-bridge` becomes the next chapter; `ch24` supplies the change source. Chapters `ch27` and `ch31` maintain the interim limitations record.

[^npd-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Native PostgreSQL public contracts for CDC, values, and dedicated sessions.
[^npd-proto-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL Protocol 3.0 CopyBoth and pgoutput v1 wire constraints.
[^pg-core-rs]: `src/rs/vertebra-db/src/postgres/mod.rs::encode_query_frame` — Shared stateless PostgreSQL wire framing helpers in `vertebra-db`.
[^pgoutput-rs]: `src/rs/vertebra-postgres/src/replication/pgoutput.rs::PgOutputDecoder` — Incremental `pgoutput` v1 decoder and ordered transaction record state machine.
[^cdc-domain]: `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` — Adapter-owned CDC envelopes, unchanged TOAST invariants, and int64 event IDs.
[^pg-driver-py]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — Python drivers still executing through `psycopg_pool` and `asyncio.to_thread` on `main`.
[^storage-limitations]: `.agents/bundles/knowledge/domains/storage/object-storage-limitations.md` — House format for a domain limitations OKF `Gotcha` entry.
