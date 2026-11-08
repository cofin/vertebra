---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch17-command-deadlines-portals-and-tz-fallback
title: "Chapter 17: Native Command I/O Deadlines, Python Portal Facades, and Ambiguous Timezone Fallback"
description: Bound explicitly timed native queries under one non-renewing absolute I/O deadline, expose SyncPostgresPortal and AsyncPostgresPortal facades, and finalize jiff-backed timezone text resolution with output-only wire_text fallback.
status: stable
state: planned
planning_status: complete
depends_on_chapters:
  - ch16-pipeline-scope-and-cancellation
replan_trigger: null
current_task: "native-postgres-driver:ch17:t01"
plan_revision: 9
plan_commit: b0de4dcb
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
    title: Frozen native PostgreSQL command timeout, portal facade, and PgTemporal wire_text contracts
  - id: pg-protocol-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL timezone_abbreviations asymmetry, portal transaction survival, and cancellation drain rules
  - id: db-errors-rs
    resource: src/rs/vertebra-py/src/errors.rs::DbExceptionTypes
    title: Canonical OperationalError, QueryCancelledError, and DataError classes in vertebra._vertebra
  - id: cargo-workspace
    resource: src/rs/Cargo.toml
    title: Rust workspace Cargo manifest governing crate dependencies
tags:
  - postgres
  - timeouts
  - portals
  - timezones
  - pyo3
---

# Chapter 17: Native Command I/O Deadlines, Python Portal Facades, and Ambiguous Timezone Fallback

<!-- truth: start -->
- **Non-Renewing Command I/O Deadlines**: Optional `timeout: float | None = None` on `query` and `query_multi` computes one absolute `Instant` at native reservation covering parameter catalog discovery, writes/flush, all result frames, and active enrichment; client expiry claims `OperationGeneration`, surfaces `OperationalError` (`sqlstate = None`) with precedence over cleanup `57014`, skips optional post-`ReadyForQuery` enrichment on exhausted budgets, and runs `connect_timeout`-bounded `CancelRequest` + retirement without SQL replay [^pg-contracts] [^db-errors-rs].
- **Session-Pinned Python Portals (`SyncPostgresPortal` / `AsyncPostgresPortal`)**: `open_portal(sql, parameters=(), *, parameter_oids=None, batch_size=65536)` exposes `fetch(max_rows)`, `close()`, `exhausted`, and `closed` while pinning its `SessionHandle` / `PooledConnection` and `TransactionScope` until exhaustion or cleanup [^pg-contracts].
- **Vetted `jiff` Timezone Resolution & Output-Only `wire_text`**: Using `jiff = "=0.2.39"` (`tzdb-bundle-always`), non-ISO `timestamptz` abbreviation text resolves to an instant strictly within `jiff`'s range only when local fields + abbreviation identify a unique instant; DST folds/gaps, LMT, and out-of-range abbreviations preserve `PgTemporal(special="wire_text")`, and every typed bind of `wire_text` raises `DataError` before submission across text, binary, pipeline, and COPY paths [^pg-contracts] [^pg-protocol-research].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

With physical pools, single-`Sync` pipelines, and generation-fenced `CancelRequest` cancellation complete (`ch13`–`ch16`), three Phase 3 capabilities remain before binary `COPY` and Arrow streaming (`ch18`–`ch20`): (1) per-command client I/O budgets (`timeout=`) on `query` and `query_multi` that distinguish client expiry (`OperationalError`, `sqlstate=None`) from server `statement_timeout` (`QueryCancelledError`, `57014`) without renewing per frame or replaying SQL; (2) immutable Python `SyncPostgresPortal` and `AsyncPostgresPortal` facades over `ch13:t02`'s concrete Rust `Portal`; and (3) exact session-zone disambiguation for non-ISO `timestamptz` text (`ch10:t02`) via bundled IANA `tzdb` (`jiff = "=0.2.39"`) paired with unconditional `DataError` refusal on `wire_text` typed rebinds because PostgreSQL's `timezone_abbreviations` input table is independent of `DateStyle` / `TimeZone` output settings [^pg-contracts] [^pg-protocol-research].

### 1.2 Non-Goals

- Global command-timeout defaults or timeout kwargs on `pipeline`, `COPY`, `open_portal`, or change/notification stream APIs [^pg-contracts].
- Preempting CPU-bound Rust decoding or Python object projection once wire I/O completes [^pg-contracts].
- Binary `COPY` and Arrow `RecordBatch` / `VTBARROW` SHM portals (owned by Chapters 18, 19, and 20).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md) and frozen contracts: [contracts.md](../../contracts.md). Engineering skills: [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md) and [runtime-ipc](../../../../../skills/runtime-ipc/SKILL.md). Knowledge chapters: [database-drivers-and-dialers.md](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [query-pipeline-cdc-and-tenancy.md](../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md), and [ffi-and-gil-discipline.md](../../../../knowledge/patterns/rust/ffi-and-gil-discipline.md) [^db-errors-rs] [^cargo-workspace].

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R6, R7, R10, R12, R18 (`ch17:t01`)**: Implement `CommandBudget`, RAII `PostgresConnection::with_command_deadline` (`active_deadline: Option<tokio::time::Instant>`), `SessionHandle::query_with_deadline`, and `SessionHandle::simple_query_with_deadline` (`session.rs`, `query.rs`, `extended.rs`) enforcing one non-renewing `Instant` inside `PostgresConnection::simple_query` and `ExtendedQueryCollector` / `query_with_row_sink` so `TypeCatalog::discover`, `metadata::resolve_parameter_metadata`, and `metadata::normalize_materialized_result` automatically inherit the deadline and preserve `PostgresError::AfterCompletion` (`completed_result`) without modifying `metadata.rs` or `types.rs`, skipping optional post-`ReadyForQuery` enrichment on exhausted budgets and capping `crate::session::cancel` cleanup/retirement by `connect_timeout` with `OperationalError(sqlstate=None)` precedence over cleanup `57014` [^pg-contracts].
- **R6, R7, R10, R12, R18 (`ch17:t02`)**: Add keyword-only `timeout: float | None = None` to `query` and `query_multi` on `SyncPostgresClient` and `AsyncPostgresClient` (`sync_client.rs`, `async_client.rs`, `_postgres.pyi`), validating finite positive floats via `crate::session::CommandBudget` before reservation, delegating to `SessionHandle::{query_with_deadline, simple_query_with_deadline}`, and verifying against live `postgres_service` (`pg_sleep` vs `statement_timeout`) [^pg-contracts] [^db-errors-rs].
- **R6, R7, R16 (`ch17:t03`)**: Expose `SyncPostgresPortal` and `AsyncPostgresPortal` plus `open_portal(sql, parameters=(), *, parameter_oids=None, batch_size=65536)` on `SyncPostgresClient` and `AsyncPostgresClient` (`sync_client.rs`, `async_client.rs`, `_postgres.pyi`) with `fetch(max_rows)`, `close()`, `exhausted`, and `closed`, keeping leases pinned through streaming cleanup [^pg-contracts].
- **R11 (`ch17:t04`)**: Pin `jiff = "=0.2.39"` (`std`, `tzdb-bundle-always`) in `src/rs/vertebra-postgres/Cargo.toml` + `src/rs/Cargo.lock`, implement `resolve_or_capture_temporal` in `src/rs/vertebra-postgres/src/codecs/datetime_text.rs` (`timezone_text`), and reject every `TemporalText` / `PgTemporal(special="wire_text")` typed bind with `DataError` before submission across all paths [^pg-contracts] [^pg-protocol-research] [^cargo-workspace].

### 2.2 Concrete Type & Signature Contracts

- **Caller -> Seam -> Collaborator**:
  - Timed query callers -> `SyncPostgresClient`/`AsyncPostgresClient.{query, query_multi}(..., timeout=...)` -> `CommandBudget` + `SessionHandle::{query_with_deadline, simple_query_with_deadline}` (`session.rs`) -> RAII `PostgresConnection::with_command_deadline` -> `PostgresConnection::simple_query` (`query.rs`) & `ExtendedQueryCollector` / `query_with_row_sink` (`extended.rs`) -> `crate::session::cancel` bounded cleanup.
  - Streaming callers -> `client.open_portal(...)` -> `SyncPostgresPortal` / `AsyncPostgresPortal` -> `Portal::{fetch, close}` (`ch13:t02`).
  - Temporal text decoder -> `datetime_text::decode_timestamp_text` -> `resolve_or_capture_temporal` (`jiff` `0.2.39` `tzdb-bundle-always`) -> `PgValue::Timestamptz(i64)` or `PgValue::TemporalText` -> `PgTemporal(special="wire_text")`.
- **Rust & Python Signatures**:
  - `pub struct CommandBudget(Option<tokio::time::Instant>)` (`crate::session::CommandBudget`)
  - `PostgresConnection::with_command_deadline<F, Fut, T>(&mut self, deadline: Option<tokio::time::Instant>, f: F) -> Result<T, PostgresError>`
  - `SessionHandle::query_with_deadline(&self, sql: &str, parameters: &[Parameter], parameter_oids: Option<&[u32]>, budget: CommandBudget) -> Result<QueryResult, PostgresError>`
  - `SessionHandle::simple_query_with_deadline(&self, sql: &str, budget: CommandBudget) -> Result<Vec<QueryResult>, PostgresError>`
  - `SyncPostgresClient.query(self, sql: str, parameters: Sequence[PostgresValue | PgParameter] = (), *, parameter_oids: Sequence[int] | None = None, timeout: float | None = None) -> PostgresQueryResult`
  - `SyncPostgresClient.query_multi(self, sql: str, *, timeout: float | None = None) -> tuple[PostgresQueryResult, ...]`
  - `SyncPostgresClient.open_portal(self, sql: str, parameters: Sequence[PostgresValue | PgParameter] = (), *, parameter_oids: Sequence[int] | None = None, batch_size: int = 65536) -> SyncPostgresPortal`
  - `SyncPostgresPortal.fetch(self, max_rows: int) -> PostgresQueryResult` / `close(self) -> None` / `exhausted: bool` / `closed: bool`
  - `pub fn resolve_or_capture_temporal(parsed: &ParsedTimestampFields, date_style: &str, time_zone: &str) -> Result<PgValue, PostgresError>`

### 2.3 Edge-Case Matrix & Alternatives

| Scenario | Input / Wire Condition | Required Behavior |
| --- | --- | --- |
| Dribbled multi-frame response under `timeout=0.1` | Peer trickles `DataRow` frames every `40ms` for `200ms` | Single absolute `Instant` expires without per-frame renewal; sends `CancelRequest`, retires socket, raises `OperationalError(sqlstate=None)` [^pg-contracts]. |
| Server `statement_timeout` vs client `timeout` | Server `statement_timeout = '50ms'` fires before client `timeout = 5.0` | Server `ErrorResponse("57014")` arrives first and raises `QueryCancelledError(sqlstate="57014")` rather than `OperationalError` [^pg-contracts]. |
| Post-`ReadyForQuery` budget exhausted before optional enrichment | Query completes `ReadyForQuery` with `0ms` remaining; custom OID column needs `TypeCatalog` lookup | Skip optional enrichment I/O and return completed `PostgresQueryResult` with exact owned text + column OID fallback [^pg-contracts]. |
| DST fold / historical LMT / matching-context rebind | `"Sun Nov 01 01:30:00 2026 EDT"` vs fold `"EST"` or `"CST"` under `America/Havana` / `Asia/Shanghai`, then rebind `PgTemporal(special="wire_text")` under identical `DateStyle`/`TimeZone` | Unambiguous in-range zone+offset resolves to exact UTC `i64`; fold/gap/LMT/ambiguous retains `PgTemporal(special="wire_text")`; typed rebind raises `DataError` before submission in every path [^pg-contracts] [^pg-protocol-research]. |

- **Alternatives Rejected**: Global abbreviation lookup tables misresolve `"CST"` / `"BST"` / `"AST"` across regions and historical LMT transitions; allowing matching-context `wire_text` rebinds fails when `timezone_abbreviations` differs from zone output rules [^pg-protocol-research].

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/1 files documented in ch17]` (`src/py/vertebra/_postgres.pyi` stubs and `PgTemporal` / timeout docstrings update in `ch17:t02`–`t04`; user/OKF documentation consolidates in `ch30`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R6, R7, R10, R12, R18 | [t01](tasks/t01-rust-command-deadlines.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/command_deadlines.rs` |
| R6, R7, R10, R12, R18 | [t02](tasks/t02-python-command-deadline-forwarding.md) | `behavior_tdd` | `src/py/tests/integration/db/test_postgres_command_deadlines.py` |
| R6, R7, R16 | [t03](tasks/t03-python-portal-facades.md) | `behavior_tdd` | `src/py/tests/unit/db/test_postgres_native_portals.py` |
| R11 | [t04](tasks/t04-timezone-text-and-ambiguous-fallback.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/timezone_text.rs`, `src/py/tests/unit/db/test_postgres_native_queries.py` |

## 5. Implementation Plan

- [ ] native-postgres-driver:ch17:t01 — Implement absolute Rust command I/O deadlines and bounded cancel retirement (tasks/t01-rust-command-deadlines.md)
- [ ] native-postgres-driver:ch17:t02 — Forward Python query and query_multi command deadlines and verify live SQLSTATE precedence (tasks/t02-python-command-deadline-forwarding.md)
- [ ] native-postgres-driver:ch17:t03 — Expose session-pinned SyncPostgresPortal and AsyncPostgresPortal facades (tasks/t03-python-portal-facades.md)
- [ ] native-postgres-driver:ch17:t04 — Resolve vetted jiff timezone text and enforce output-only ambiguous temporal fallback (tasks/t04-timezone-text-and-ambiguous-fallback.md)

**Chapter Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test command_deadlines --test timezone_text && cargo check --manifest-path src/rs/Cargo.toml -p vertebra-postgres --locked && make dev && uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_portals.py src/py/tests/unit/db/test_postgres_native_queries.py src/py/tests/integration/db/test_postgres_command_deadlines.py -q && uv run --no-sync python -m mypy.stubtest vertebra._postgres` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `HEAD @ dd432f18`; chapter `state: planned`, `planning_status: complete`.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03`, `t04` (`4/4` pending).
- **Downstream Dependents**: Completes Phase 3 and unblocks `ch18-binary-copy-streaming` (`ch18:t01` incremental binary COPY codec), `ch20:t03`–`t04` (Arrow portal streaming), and `ch25`–`ch26` (permanent driver `timeout=` forwarding and `_publish_notification(..., timeout=10.0)`).

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Frozen command deadline, Python portal, and `PgTemporal` `wire_text` contracts.
[^pg-protocol-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL `timezone_abbreviations` input/output asymmetry and portal/cancellation protocol rules.
[^db-errors-rs]: `src/rs/vertebra-py/src/errors.rs::DbExceptionTypes` — Canonical `OperationalError`, `QueryCancelledError`, and `DataError` classes.
[^cargo-workspace]: `src/rs/Cargo.toml` — Rust workspace Cargo configuration.
