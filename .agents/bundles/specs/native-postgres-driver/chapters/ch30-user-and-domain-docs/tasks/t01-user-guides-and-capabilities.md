---
type: Task
id: "native-postgres-driver:ch30:t01"
chapter_id: ch30-user-and-domain-docs
title: Update user database guides, CDC analytics guide, and capability API references
description: Document vertebra-core[postgres] installation, bind/query/pipeline/value/COPY/Arrow/PgBouncer/DSN contracts, unified $N tenant binds, _open_notification_listener, and vertebra.db.adapters.postgres.change_stream across databases.md, analytics-and-cdc.md, and database-capabilities.md.
status: stable
state: pending
priority: P1
verification_strategy: documentation_validation
depends_on:
  - "native-postgres-driver:ch29:t03"
read_set:
  - docs/usage/databases.md
  - docs/usage/analytics-and-cdc.md
  - docs/reference/database-capabilities.md
  - src/py/vertebra/db/adapters/postgres/config.py
  - src/py/vertebra/db/adapters/postgres/notify.py
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - docs/usage/databases.md
  - docs/usage/analytics-and-cdc.md
  - docs/reference/database-capabilities.md
tests: []
estimated_loc: 225
plan_revision: 9
plan_commit: b0de4dcb
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
updated_at: "2026-11-07T16:45:00Z"
commit: null
sources:
  - id: doc-databases
    resource: docs/usage/databases.md
    title: User-facing Databases and Native Drivers usage guide
  - id: doc-cdc
    resource: docs/usage/analytics-and-cdc.md
    title: User-facing RLS, CDC, and Arrow Analytics usage guide
  - id: doc-capabilities
    resource: docs/reference/database-capabilities.md
    title: API reference for notifications, change streams, and tools
  - id: pg-config
    resource: src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig
    title: PostgreSQL DSN, TLS, pooler_mode, direct_dsn, and cloud configuration
  - id: pg-notify
    resource: src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource
    title: PostgreSQL notification source and publication contract
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Authoritative native PostgreSQL public contracts
tags:
  - postgres
  - documentation
  - zensical
  - cdc
  - reference
---

# Task t01: Update User Database Guides, CDC Analytics Guide, and Capability API References

## Objective & Scope Boundary

Update public user documentation in `docs/usage/databases.md`, `docs/usage/analytics-and-cdc.md`, and `docs/reference/database-capabilities.md` to match the completed `vertebra-postgres` native driver and `vertebra.db.adapters.postgres.change_stream` contracts [^doc-databases] [^doc-cdc] [^doc-capabilities] [^pg-config] [^pg-notify] [^pg-contracts]:
1. `docs/usage/databases.md`:
   - Add `vertebra-core[postgres]` (`vertebra-postgres`) to `Optional Engine Extras` (`uv`, `pip`, `pdm`, `poetry`), remove any PostgreSQL `psycopg`/`asyncpg`/`memory://` fallback wording (preserving `sqlite:///:memory:` and other engine examples), and document clean-break constructor and supplied-pool ownership (`share_pool`, `validate_configuration`).
   - Document native `$N` passthrough vs lexer-aware raw `%s`/`%(name)s` emulation (rejecting ambiguous ordinary backslash-quote strings in favor of explicit `E'...'`), SQLSpec default `ParameterStyle.NUMERIC` (preserving explicit caller `StatementConfig` identity), SQLAlchemy `+vtb` `numeric_dollar` compilation, single-statement extended `execute` vs explicit parameterless `query_multi(sql)` returning ordered `PostgresQueryResult` records (`42601` on multi-SQL `execute`), and optional per-command `timeout`.
   - Document transaction/savepoint-scoped single-`Sync` `pipeline()` (`pipe.select` / `pipe.execute` returning `None`, `pipe.results()` flushing once without early commit, rollback on caller hydration/cardinality error, cancellation/retirement rules), cold text vs warm binary OID negotiation, lossless `PgInterval`, `PgArray`, `PgComposite`, `PgTemporal` (`wire_text` rebind refusal), `PgJson`, `PgBitString`, and `PgVector` (`1..16000`), binary `COPY` & `VTBARROW` `ArrowCursor` resource closure, `PgBouncer` `pooler_mode` vs `protocol_level_pooler_cache`, exact `PGUSER`/`PGDATABASE` DSN precedence, and explicit `sslmode="disable"` only for non-TLS local test fixtures.
2. `docs/usage/analytics-and-cdc.md`:
   - Replace stale `psycopg %s or asyncpg $N` tenant-predicate prose with unified native `$N` (`ParameterStyle.NUMERIC`) across both sync and async PostgreSQL drivers.
   - Replace stale `listen_notifications()` claims with dedicated `_open_notification_listener` / `_publish_notification` hooks on `PostgresNotificationSource` (`queue_capacity`, `reconnect`, `direct_dsn` bypass).
   - Add present-tense PostgreSQL logical replication usage covering `vertebra.db.adapters.postgres.change_stream.PostgresChangeConfig`, `PostgresChangeSource`, `PostgresCheckpoint`, `PostgresCheckpointStore`, and `MemoryPostgresCheckpointStore` (`pgoutput` v1 caller-owned slot/publication tail, explicit CAS checkpoint save before WAL feedback, signed-`int64` replay IDs, and `direct_dsn` when queries traverse transaction-mode `PgBouncer`).
3. `docs/reference/database-capabilities.md`:
   - Add mkdocstrings `:::` blocks for `vertebra.db.adapters.postgres.change_stream.PostgresChangeConfig`, `vertebra.db.adapters.postgres.change_stream.PostgresCheckpoint`, `vertebra.db.adapters.postgres.change_stream.PostgresCheckpointStore`, `vertebra.db.adapters.postgres.change_stream.MemoryPostgresCheckpointStore`, `vertebra.db.adapters.postgres.change_stream.PostgresChangeSource`, and `vertebra.db.adapters.postgres.embeddings.PostgresEmbeddingPushdown` alongside `PostgresNotifyConfig` and `PostgresNotificationSource`.

Out of scope: Modifying Python/Rust source files (docstrings finalized in `ch25`–`ch27`), `README.md` / `workflow.md` / `AGENTS.md` (`ch30:t02`), or OKF `.agents/bundles/` chapters (`ch30:t03`–`t04`).

## Bounded Read Manifest

- [docs/usage/databases.md](../../../../../../../docs/usage/databases.md) — Current optional extras, driver contract, and SQLSpec/PostgreSQL examples [^doc-databases].
- [docs/usage/analytics-and-cdc.md](../../../../../../../docs/usage/analytics-and-cdc.md) — Current RLS tenant isolation, notification, CDC, and Arrow analytics guide [^doc-cdc].
- [docs/reference/database-capabilities.md](../../../../../../../docs/reference/database-capabilities.md) — Existing mkdocstrings `:::` directives [^doc-capabilities].
- [src/py/vertebra/db/adapters/postgres/config.py](../../../../../../../src/py/vertebra/db/adapters/postgres/config.py) — `PostgreSQLConfig` public fields and defaults [^pg-config].
- [src/py/vertebra/db/adapters/postgres/notify.py](../../../../../../../src/py/vertebra/db/adapters/postgres/notify.py) — `PostgresNotifyConfig` and `PostgresNotificationSource` signatures [^pg-notify].
- [.agents/bundles/specs/native-postgres-driver/contracts.md](../../../contracts.md) — Exact public signatures, value records, DSN defaults, and `change_stream` contracts [^pg-contracts].

## Symbol & Signature Contracts

- Every Python code snippet in `docs/usage/databases.md` and `docs/usage/analytics-and-cdc.md` uses PEP 585/604 types, contains zero inline `#` comments, and matches the installed `vertebra._postgres`, `vertebra.db.adapters.postgres`, `vertebra.db.adapters.postgres.change_stream`, `vertebra.extensions.sqlspec`, and `vertebra.extensions.sqlalchemy` signatures.
- Every `::: vertebra.db.adapters.postgres...` directive in `docs/reference/database-capabilities.md` resolves cleanly through Griffe during `make docs`.

## Edge Cases & Error Semantics

- Do not claim automatic replication slot creation/drop, initial snapshot backfill, automatic ack-on-yield, `listen_notifications()`, or default live Google Cloud SQL/AlloyDB CI runs; state the exact caller-owned slot/publication, explicit `acknowledge(commit_end_lsn)`, and local mock/TLS wire verification boundaries.
- Preserve non-PostgreSQL documentation sections (`sqlite:///:memory:` quickstart snippets, `BigQuery`, `Spanner`, `Oracle`, `MySQL`, `DuckDB`) without regressions.

## Public-Seam Test Blueprint

- **Canonical validation gates**: `make docs` and `make docs-check` (`tests: []`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Zensical build + Griffe mkdocstrings resolution | `make docs` | Exits `0`; all `::: vertebra.db.adapters.postgres.change_stream.*`, `notify.*`, and `embeddings.*` targets resolve with zero missing-symbol or broken-link warnings. |
| Live served documentation check | `make docs-check` | Serves built `site/` via `zensical serve` and verifies HTTP `200` across generated pages. |
| Stale claim & code-fence hygiene | Inspect `databases.md`, `analytics-and-cdc.md`, and `database-capabilities.md` + `make review-check` | Zero inline Python `#` comments, zero `psycopg`/`asyncpg` PostgreSQL runtime claims, zero `listen_notifications()` claims, all change-stream imports using `vertebra.db.adapters.postgres.change_stream`, and `make review-check` exits `0`. |

## Verification

- Build & live-serve docs gate: `make docs && make docs-check`
- Review & formatting gate: `make review-check`

## Acceptance Criteria

- [ ] `docs/usage/databases.md`, `docs/usage/analytics-and-cdc.md`, and `docs/reference/database-capabilities.md` accurately document `vertebra-core[postgres]`, unified `$N` binds, `pipeline()`, lossless `Pg*` values, `COPY`/`VTBARROW`, `_open_notification_listener`, and `vertebra.db.adapters.postgres.change_stream.*` (R27).
- [ ] `make docs`, `make docs-check`, and `make review-check` exit `0` with zero broken links, unresolved mkdocstrings targets, or inline Python comments (R27).

## Notes & Discoveries

- [2026-11-07 16:45] `ch27:t01` verified Google-style docstrings across `driver.py` and `embeddings.py` (`tools.docs.scan_public_docstrings`), ensuring `make docs` builds cleanly without touching Python source files here.

[^doc-databases]: `docs/usage/databases.md` — User-facing Databases and Native Drivers usage guide.
[^doc-cdc]: `docs/usage/analytics-and-cdc.md` — User-facing RLS, CDC, and Arrow Analytics usage guide.
[^doc-capabilities]: `docs/reference/database-capabilities.md` — API reference for notifications, change streams, and tools.
[^pg-config]: `src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig` — PostgreSQL DSN, TLS, `pooler_mode`, `direct_dsn`, and cloud configuration.
[^pg-notify]: `src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource` — PostgreSQL notification source and publication contract.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Authoritative native PostgreSQL public contracts.
