---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch13-transactions-portals-and-statement-cache
title: "Chapter 13: Transaction Ownership, Bounded Portals, and Pooler-Safe Statement Caching"
description: Enforce server-verified transaction and savepoint ownership, stream bounded named portals under retained transaction scopes, and cache server prepared statements safely across direct and PgBouncer/PgCat pooler modes.
status: stable
state: planned
planning_status: complete
depends_on_chapters:
  - ch12-pgvector-and-extended-query
replan_trigger: null
current_task: "native-postgres-driver:ch13:t01"
plan_revision: 5
plan_commit: 399e068c
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-07T05:00:00Z"
generated:
  by: flow-setup
  at: "2026-11-07T05:00:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T05:00:00Z"
    commit: dd432f18
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Frozen native PostgreSQL transaction, portal, and pooler public contracts
  - id: pg-protocol-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL protocol 3.0 extended query, portal, and PgBouncer execution constraints
  - id: pg-config
    resource: src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig
    title: PostgreSQL pooler mode and prepared-statement configuration contract
  - id: pg-conn-rs
    resource: src/rs/vertebra-db/src/connection.rs::Connection
    title: Shared Rust database connection lifecycle trait
tags:
  - postgres
  - transactions
  - portals
  - pooling
  - rust
---

# Chapter 13: Transaction Ownership, Bounded Portals, and Pooler-Safe Statement Caching

<!-- truth: start -->
- **Server-Verified Transaction & Savepoint State**: `PostgresConnection` and `SessionHandle` enforce exclusive `TransactionOwner` generation pinning, numeric-only internal savepoint identifiers (`vtb_sp_<u64>`), actual `ReadyForQuery` (`I`/`T`/`E`) transitions, and command-tag verification (`COMMIT` returning `ROLLBACK` in `E` fails explicitly) without holding a Rust mutex across `await`, Python conversion, or GIL reacquisition [^pg-contracts] [^pg-protocol-research].
- **Bounded Portals Under Retained Scopes**: `Portal` pins `Arc<SessionHandle>` and a `TransactionScope` (`BEGIN` when opened at `I`, savepoint when opened at `T`) so intermediate `Execute(max_rows)` + `Sync` exchanges stay in `T`, exposing incrementally resumable `PortalEvent` streaming (`start_fetch` / `next_event`) alongside bounded `QueryResult` `fetch` and nonblocking `Drop` cleanup/retirement [^pg-contracts] [^pg-protocol-research].
- **Pooler-Attested Statement Cache**: `StatementCache` keys real server prepared statements by `(sql, param_oids, schema_generation)` with bounded LRU `Close(b'S', name)` eviction, enabling named protocol preparation in `transaction` mode only when `ParameterStatus` (`pgbouncer.version` / `pgbouncer.pool_mode` / `pgbouncer.max_prepared_statements`) or `protocol_level_pooler_cache` attests support, defaulting `auto` without attestation to `0`, and rejecting scoped transactions/portals/pipelines in `statement` mode [^pg-contracts] [^pg-config].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Chapter 12 (`ch12-pgvector-and-extended-query`) completes single-round-trip cold and warm extended query execution (`PostgresConnection::query`). Multi-statement scopes, row-limited portal streaming, and reusable named server statements require explicit session ownership and protocol state tracking: PostgreSQL destroys non-`HOLD` portals on idle `ReadyForQuery('I')`, returns `CommandComplete("ROLLBACK")` when `COMMIT` executes on an aborted transaction (`'E'`), destroys the unnamed prepared statement whenever a simple `Query` runs, and rotates backend connections on idle `Sync` in transaction poolers unless protocol-level statement multiplexing is supported [^pg-contracts] [^pg-protocol-research]. Chapter 13 implements pinned transaction/savepoint state (`ch13:t01`), bounded `Portal` streaming (`ch13:t02`), and pooler-aware `StatementCache` management (`ch13:t03`).

### 1.2 Non-Goals

- Physical Rust connection pool checkout, `DISCARD ALL` reset, and Python query/pool facades (owned by Chapters 14 and 15).
- Single-`Sync` pipeline submission, cancellation `CancelRequest` sockets, command deadlines, and Python portal PyO3 wrappers (owned by Chapters 15, 16, and 17).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md) and frozen contracts: [contracts.md](../../contracts.md). Engineering skills: [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md) and [runtime-ipc](../../../../../skills/runtime-ipc/SKILL.md). Knowledge chapters: [database-drivers-and-dialers.md](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [query-pipeline-cdc-and-tenancy.md](../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md), and [ffi-and-gil-discipline.md](../../../../knowledge/patterns/rust/ffi-and-gil-discipline.md) [^pg-conn-rs] [^pg-config].

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R13 (`ch13:t01`)**: Implement `transaction.rs::{TransactionOwner, TransactionScope, TransactionDisposition}` and `PostgresConnection::{begin, commit, rollback, savepoint, release_savepoint, rollback_to_savepoint}` with bounded numeric savepoint tokens, server `I`/`T`/`E` + command-tag validation, failed-state `E` recovery gating, and lock-free `SessionHandle` operation/owner generation guards [^pg-contracts].
- **R16 (`ch13:t02`)**: Implement `portal.rs::{Portal, PortalEvent}` and `SessionHandle::open_portal(sql, parameters, parameter_oids, batch_size)` retaining `Arc<SessionHandle>` and `TransactionScope` (`BEGIN` at `I`, savepoint at `T`), supporting row-by-row `start_fetch` / `next_event` suspension/resumption for Arrow consumers plus bounded `fetch(max_rows) -> Result<QueryResult, PostgresError>`, `close()`, and nonblocking `Drop` retirement [^pg-contracts] [^pg-protocol-research].
- **R15 (`ch13:t03`)**: Implement `statements.rs::{StatementCache, StatementKey, PoolerCapabilities, PoolerMode}` with `(sql, param_oids, schema_generation)` keys, safe-boundary LRU `Close`/`CloseComplete`, `direct`/`session`/`transaction`/`statement`/`auto` capability resolution over `ParameterStatus` (`pgbouncer.*`) and `protocol_level_pooler_cache`, split-preflight `TransactionScope` pinning, unnamed re-`Parse` after catalog `Query`, and zero-replay schema invalidation [^pg-contracts] [^pg-config].

### 2.2 Concrete Type & Signature Contracts

- **Caller -> Seam -> Collaborator**:
  - `SessionHandle` callers -> `SessionOperationGuard` + `TransactionScope` (`transaction.rs`, `session.rs`) -> `PostgresConnection` wire frame state machine.
  - Streaming/Arrow consumers -> `SessionHandle::open_portal` -> `Portal::{start_fetch, next_event, fetch, close}` (`portal.rs`) -> named `Bind` + `DescribePortal` + `Execute(max_rows)` + `Sync` inside retained `TransactionScope`.
  - Extended execution -> `PostgresConnection::query` (`extended.rs`) -> `StatementCache` + `PoolerCapabilities` (`statements.rs`).
- **Rust Signatures (`src/rs/vertebra-postgres/src/{transaction,portal,statements,session,extended}.rs`, with `statements.rs` mounted as `crate::session::statements` via `#[path = "statements.rs"] pub mod statements;` in `session.rs` and never duplicated in `lib.rs`)**:
  - `pub struct TransactionOwner { id: u64, generation: u64 }`
  - `pub enum TransactionDisposition { Begun, Committed, RolledBack, SavepointCreated(u64), SavepointReleased(u64), RolledBackToSavepoint(u64) }`
  - `pub(crate) struct SessionOperationGuard<'a>` and `SessionHandle::with_connection<F, Fut, T>(&self, f: F) -> Result<T, PostgresError>` (yielding `(&mut PostgresConnection, PoolerMode)` for sibling modules across `ch13`–`ch23` without re-editing `session.rs`)
  - `PostgresConnection::{begin, commit, rollback}(&mut self) -> Result<TransactionDisposition, PostgresError>`
  - `PostgresConnection::{savepoint, release_savepoint, rollback_to_savepoint}(&mut self, token: u64) -> Result<TransactionDisposition, PostgresError>`
  - `pub enum PortalEvent { Row(Vec<PgValue>), Suspended, Complete { command_tag: String, affected_rows: u64 } }`
  - `SessionHandle::open_portal(self: &Arc<Self>, sql: &str, parameters: &[Parameter], parameter_oids: Option<&[u32]>, batch_size: i32) -> Result<Portal, PostgresError>`
  - `Portal::start_fetch(&mut self, max_rows: i32) -> Result<(), PostgresError>`
  - `Portal::next_event(&mut self) -> Result<PortalEvent, PostgresError>`
  - `Portal::fetch(&mut self, max_rows: i32) -> Result<QueryResult, PostgresError>`
  - `Portal::close(&mut self) -> Result<(), PostgresError>`
  - `pub enum PoolerMode { Direct, Session, Transaction, Statement, Auto }` (`crate::session::statements::PoolerMode`)
  - `StatementCache::resolve_effective_capacity(mode: PoolerMode, configured_capacity: usize, protocol_level_pooler_cache: bool, params: &BTreeMap<String, String>) -> Result<usize, PostgresError>`

### 2.3 Edge-Case Matrix & Alternatives

| Scenario | Input / Wire Condition | Required Behavior |
| --- | --- | --- |
| `COMMIT` inside aborted transaction | Session in `E`; server returns `CommandComplete("ROLLBACK")` + `ReadyForQuery('I')` | Reject silent success; transition state to `I` and return explicit transaction rollback failure (`PostgresError`) [^pg-protocol-research]. |
| Ambient failed transaction | User query fails in `T` -> `E`; caller issues another `SELECT` or `SAVEPOINT` | Refuse non-recovery commands while in `E` and never silently acquire a replacement session; only `rollback` / `rollback_to_savepoint` clears `E` [^pg-contracts]. |
| Portal paused mid-chunk | `start_fetch(2)` reads row 1 via `next_event()`, pauses before `PortalSuspended`, then resumes `next_event()` | Retain active `Execute` frame stream; refuse a second `start_fetch` until `Suspended` or `Complete` arrives; emit zero duplicate `Execute` frames [^pg-protocol-research]. |
| Older or unattested pooler in `auto` / `transaction` | `pooler_mode = Auto` with no `pgbouncer.*` `ParameterStatus` and `protocol_level_pooler_cache = false` | Effective prepared cache capacity is `0` (unnamed `Parse` per group); never assume an unattested proxy is a direct server [^pg-contracts]. |
| `statement` pooler mode with stateful scope | Caller requests `begin()`, `open_portal(...)`, or pipeline scope under `PoolerMode::Statement` | Fail closed with `InterfaceError` / `NotSupportedError` before emitting any wire bytes [^pg-contracts]. |

- **Alternatives Rejected**: Issuing SQL `PREPARE` / `DEALLOCATE` text commands instead of protocol `Parse` / `Close(b'S')` frames fails across modern PgBouncer protocol multiplexing and alters command-tag transcripts; holding a `tokio::sync::Mutex` across Python row conversion risks lock-order deadlocks with GIL acquisition [^pg-protocol-research].

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in ch13]` (public Python `.pyi` stubs and user/OKF documentation update in Chapters 14, 17, and 30; Rust rustdoc invariants land inline on `TransactionScope`, `Portal`, and `StatementCache`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R13 | [t01](tasks/t01-transaction-and-savepoint-state.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/transaction_state.rs` |
| R16 | [t02](tasks/t02-bounded-portal-streaming.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/portal_protocol.rs` |
| R15 | [t03](tasks/t03-prepared-statement-cache-and-poolers.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/statement_cache.rs` |

## 5. Implementation Plan

- [ ] native-postgres-driver:ch13:t01 — Enforce pinned transaction and numeric savepoint command state (tasks/t01-transaction-and-savepoint-state.md)
- [ ] native-postgres-driver:ch13:t02 — Stream bounded results through session-retaining portals (tasks/t02-bounded-portal-streaming.md)
- [ ] native-postgres-driver:ch13:t03 — Cache server prepared statements with pooler capability attestation (tasks/t03-prepared-statement-cache-and-poolers.md)

**Chapter Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test transaction_state --test portal_protocol --test statement_cache && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `HEAD @ dd432f18`; chapter `state: planned`, `planning_status: complete`.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03` (`3/3` pending).
- **Downstream Dependents**: Unblocks `ch14-physical-pools-and-query-facades` (`PostgresPool`, `DISCARD ALL` reset, and Python query/transaction facades), `ch17:t03` (Python portal facades), and `ch20:t03`–`t04` (Arrow portal streaming).

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Frozen public constructor, transaction, pooler, and query contracts.
[^pg-protocol-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL protocol 3.0 extended query, portal transaction lifetime, and PgBouncer multiplexing rules.
[^pg-config]: `src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig` — Pooler modes (`direct`, `session`, `transaction`, `statement`, `auto`) and prepared statement cache settings.
[^pg-conn-rs]: `src/rs/vertebra-db/src/connection.rs::Connection` — Shared Rust database connection trait and transport primitives.
