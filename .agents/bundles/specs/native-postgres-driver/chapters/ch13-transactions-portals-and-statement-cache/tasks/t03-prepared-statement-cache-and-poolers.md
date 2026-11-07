---
type: Task
id: "native-postgres-driver:ch13:t03"
chapter_id: ch13-transactions-portals-and-statement-cache
title: "Cache server prepared statements with pooler capability attestation"
description: "Implement per-session LRU prepared statement caching in vertebra-postgres with PgBouncer/PgCat ParameterStatus capability detection, safe eviction, and zero-replay invalidation."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch13:t02"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/chapters/ch13-transactions-portals-and-statement-cache/spec.md
  - .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
  - src/rs/vertebra-postgres/src/extended.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/py/vertebra/db/adapters/postgres/config.py
files:
  - src/rs/vertebra-postgres/src/statements.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/extended.rs
tests:
  - src/rs/vertebra-postgres/tests/statement_cache.rs
estimated_loc: 245
plan_revision: 4
plan_commit: 45ff9563
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
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Frozen native PostgreSQL pooler modes and statement cache contracts
  - id: pg-protocol-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PgBouncer 1.26 ParameterStatus attestation and unnamed statement lifecycle constraints
  - id: pg-config
    resource: src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig
    title: Existing PoolerMode, protocol_level_pooler_cache, and max_prepared_statements semantics
tags:
  - postgres
  - statements
  - pgbouncer
  - rust
---

# Task t03: Cache Server Prepared Statements with Pooler Capability Attestation

## Objective & Scope Boundary

Cache real protocol-level server prepared statements per physical session in `vertebra-postgres` only when connection and pooler capabilities attest safety, enforcing LRU `Close(b'S', name)` eviction, schema-generation invalidation, and zero-replay error handling [^pg-contracts] [^pg-protocol-research]. Physical pool `DISCARD ALL` reset integration (`ch14:t02`) and pool constructor option wiring (`ch14:t01`, `ch15:t01`) build directly on the `statements` module introduced here.

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 13 `StatementCache`, `PoolerMode`, and `PoolerCapabilities` contracts.
- [contracts.md](../../../contracts.md) — Pool modes (`direct`, `session`, `transaction`, `statement`, `auto`), cold/warm extended query ordering, and zero-replay rules [^pg-contracts].
- [protocol-and-execution_20261105.md](../../../research/protocol-and-execution_20261105.md) — PgBouncer 1.26 `ParameterStatus` (`pgbouncer.version`, `pgbouncer.pool_mode`, `pgbouncer.max_prepared_statements`) and simple `Query` unnamed-statement destruction rules [^pg-protocol-research].
- `src/rs/vertebra-postgres/src/extended.rs` — `PreparedDescription`, `encode_bind`, and cold/warm extended execution in `PostgresConnection::query`.
- `src/rs/vertebra-postgres/src/session.rs` — `PostgresConnection` `ParameterStatus` map, `TypeCatalog` generation, `TransactionScope` (`ch13:t01`), and `Portal` (`ch13:t02`).
- `src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig` — Canonical `PoolerMode` spellings and `protocol_level_pooler_cache` flag [^pg-config].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/statements.rs` (mounted as canonical `crate::session::statements` via `#[path = "statements.rs"] pub mod statements;` inside `src/rs/vertebra-postgres/src/session.rs` — `rustc` 2021/2024 resolves `#[path = "statements.rs"]` relative to `session.rs`'s parent `src/` directory, whereas unannotated `pub mod statements;` looks for `src/session/statements.rs` (`E0583`) and `"../statements.rs"` escapes `src/`; never add a duplicate `mod statements;` in `src/lib.rs`, which triggers `clippy::duplicate_mod`, and re-export `pub use crate::session::statements::{...};` at crate root in `ch14:t01`):
  - `pub enum PoolerMode { Direct, Session, Transaction, Statement, Auto }` with `FromStr` parsing exact `"direct"`, `"session"`, `"transaction"`, `"statement"`, `"auto"` (`PostgresError::InvalidConfig` on unknown spelling).
  - `pub struct PoolerCapabilities { pub configured_mode: PoolerMode, pub effective_mode: PoolerMode, pub protocol_level_pooler_cache: bool, pub effective_capacity: usize, pub advertised_version: Option<String>, pub advertised_max_prepared: Option<usize> }`:
    - `PoolerCapabilities::resolve(configured_mode: PoolerMode, configured_capacity: usize, protocol_level_pooler_cache: bool, parameter_status: &BTreeMap<String, String>) -> Self`:
      - Inspects `ParameterStatus` keys `pgbouncer.version`, `pgbouncer.pool_mode`, and `pgbouncer.max_prepared_statements` when present [^pg-protocol-research].
      - `Direct` and `Session`: `effective_capacity = configured_capacity` (capped by `advertised_max_prepared` if advertised; `0` if server advertises `0`).
      - `Transaction`: permits named protocol statements (`min(configured_capacity, advertised_max_prepared.unwrap_or(configured_capacity))`) ONLY when `advertised_max_prepared > Some(0)` or (`advertised_max_prepared.is_none()` and `protocol_level_pooler_cache == true`); server-advertised `0` always forces `0`.
      - `Statement`: `effective_capacity = 0` unconditionally and marks scoped transactions (`begin`), `open_portal`, and pipeline scopes unsupported (`PostgresError::Interface` before wire emission) [^pg-contracts].
      - `Auto`: if `pgbouncer.pool_mode` is advertised, applies the corresponding `Session` / `Transaction` / `Statement` rule above; if no pooler mode/capability is advertised and `protocol_level_pooler_cache == false`, uses conservative `effective_capacity = 0` rather than assuming an older unattested proxy is a direct server.
  - `pub struct StatementKey { pub sql: String, pub param_oids: Vec<u32>, pub schema_generation: u64 }`
  - `pub struct CachedStatement { pub name: String, pub description: PreparedDescription, pub schema_generation: u64 }`
  - `pub struct StatementCache` — bounded LRU cache (`capacity: usize`, monotonic statement counter `vtb_s_<u64>`, `evicted_names: Vec<String>` queued for protocol `Close(b'S', name)` at the next safe idle/command boundary, and `invalidate_all(&mut self)` bumping `schema_generation` without issuing SQL `PREPARE` / `DEALLOCATE`).
- `src/rs/vertebra-postgres/src/session.rs` and `src/rs/vertebra-postgres/src/extended.rs`:
  - Attach `StatementCache` and `PoolerCapabilities` to `PostgresConnection`, recomputing `PoolerCapabilities` when startup/runtime `ParameterStatus` updates arrive and rejecting `begin` / `open_portal` in `PoolerMode::Statement` before sending bytes.
  - Warm cache hit (`effective_capacity > 0` and matching `StatementKey`): skip `Parse` + `DescribeStatement` and emit `Bind(cached.name, ...)` + `DescribePortal` (or cached result formats) + `Execute` + `Sync`, flushing any queued LRU `Close(b'S', evicted)` frames (`CloseComplete`) in the same safe boundary.
  - Cold cache fill (`effective_capacity > 0`): prepare `vtb_s_<u64>` via `Parse` + `DescribeStatement` + `Bind` + `DescribePortal` + `Execute` + `Sync` (all-text cold group with post-`ReadyForQuery` normalization from `ch12:t02`), storing negotiated `PreparedDescription` only after successful `ParseComplete` + descriptions; if a split named-statement preflight (`Parse` + `DescribeStatement` -> catalog `Query` -> `Bind`) is ever required, pin `TransactionScope` (`BEGIN` at `I` or savepoint at `T`) BEFORE `Parse`/`Describe` so every intermediate `ReadyForQuery` stays at `'T'` and no transaction-pooler backend rotation occurs [^pg-contracts].
  - Capacity `0` (`effective_capacity == 0`): always emit unnamed `Parse("")` in every extended group, and if any catalog `Query` executes first, never rely on a prior unnamed statement (`Query` destroys `""`).
  - Schema/statement invalidation (`0A000` cached plan / `26000` invalid sql statement name / `42883` / `42P01` / DDL): evict/bump `schema_generation` so subsequent explicit calls re-prepare cleanly, and NEVER replay an already-submitted query or DML statement.

## Edge Cases & Error Semantics

- **Distinct parameter OIDs on identical SQL**: `SELECT $1` with inferred OID `0` vs explicit `PgParameter(None, 23)` produces distinct `StatementKey` entries (`[0]` vs `[23]`) and never cross-binds incompatible server parameter types.
- **First-use `Parse` failure**: If `Parse("vtb_s_1", ...)` fails with `ErrorResponse("42601")`, `"vtb_s_1"` is never inserted into `StatementCache` and drains cleanly through `ReadyForQuery`.
- **LRU eviction ordering**: Inserting entry `capacity + 1` queues the least-recently-used statement name for `Close(b'S', name)` and processes `CloseComplete` at the safe extended boundary without leaving orphaned server statements or closing an active portal's statement mid-stream.
- **Invalidation without DML replay**: If `ALTER TABLE` invalidates a cached statement (`0A000` feature not supported / cached plan must not change result type, or `26000` behind a rotated backend), `PostgresConnection::query` invalidates the entry/generation and surfaces the canonical `ServerError` once — zero automatic retry can duplicate `INSERT`/`UPDATE`/`DELETE` side effects.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/statement_cache.rs` using a stateful protocol peer that tracks named prepared statements per simulated backend, clears `""` on every simple `Query`, rotates backends on idle `ReadyForQuery('I')` in transaction-pooler mode, and advertises configurable `pgbouncer.*` `ParameterStatus` tuples.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Named cache hit & LRU `Close` | `Direct` mode, `capacity = 2`; execute `Q1` twice, then `Q2`, then `Q3`, then `Q1` | `Q1` run 1 sends `Parse("vtb_s_1")`; `Q1` run 2 sends `Bind("vtb_s_1")` with zero `Parse`; `Q3` evicts LRU `"vtb_s_2"` (`Q2` if `Q1` touched, or `"vtb_s_1"` per access order) via `Close(b'S', ...)` + `CloseComplete`. |
| SQL + OID key isolation | Execute `"SELECT $1"` with `[23]` (`INT4`) then `"SELECT $1"` with `[25]` (`TEXT`) | Prepares two distinct statement names (`vtb_s_1`, `vtb_s_2`) with exact `[23]` and `[25]` OIDs. |
| PgBouncer 1.26 `ParameterStatus` attestation | `Transaction` mode with `pgbouncer.pool_mode = "transaction"`, `pgbouncer.max_prepared_statements = "64"` vs `"0"` | `"64"` enables named caching capped at `min(configured, 64)`; `"0"` forces `effective_capacity == 0` even when `protocol_level_pooler_cache == true`. |
| Unattested `Auto` & older `Transaction` fallback | `Auto` mode with no `pgbouncer.*` status and `protocol_level_pooler_cache = false` across rotating idle backends | `effective_capacity == 0`; every query emits fresh unnamed `Parse("")` and succeeds across backend rotation without `26000` errors. |
| `Statement` mode scope rejection | `PoolerMode::Statement` session calls `conn.begin()` or `session.open_portal(...)` | Fails immediately with `Err(PostgresError::Interface)` and zero wire frames emitted. |
| Schema invalidation without replay | Warm cached `"INSERT INTO t VALUES ($1) RETURNING a"` receives `ErrorResponse("0A000")` after schema change; caller runs again | First post-DDL call returns `Err` (`"0A000"`) with exactly 1 `Execute` (no replay) and invalidates cache; next explicit call sends a fresh `Parse` and succeeds. |

## Verification

- Initial Red baseline:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test statement_cache
  ```
- Final Green, Clippy, and diff gate:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test statement_cache
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test extended_protocol
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
  git diff --check
  ```

## Acceptance Criteria

- [ ] `StatementCache` keys entries by `(sql, param_oids, schema_generation)`, reuses real server prepared names on warm hits, and closes LRU-evicted names via protocol `Close(b'S')` / `CloseComplete` without issuing SQL `PREPARE` (R15).
- [ ] `PoolerCapabilities` resolves `direct`, `session`, `transaction`, `statement`, and `auto` modes from `ParameterStatus` (`pgbouncer.version`, `pgbouncer.pool_mode`, `pgbouncer.max_prepared_statements`) and `protocol_level_pooler_cache`, keeping unattested `auto` at `0` and rejecting stateful scopes in `statement` mode (R15).
- [ ] Cache/schema errors (`0A000`, `26000`) invalidate stale entries without replaying executed statements, and unnamed fallback re-`Parse`s after any catalog `Query` (R15).
- [ ] `statement_cache` and `extended_protocol` tests, Clippy `-D warnings`, and `git diff --check` pass with zero warnings or diff hygiene errors.

## Notes & Discoveries

None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Frozen pooler modes, prepared statement cache defaults, and cold/warm query invariants.
[^pg-protocol-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PgBouncer 1.26 `ParameterStatus` keys and transaction-pooler statement multiplexing constraints.
[^pg-config]: `src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig` — Python `PoolerMode` and `protocol_level_pooler_cache` configuration surface.
