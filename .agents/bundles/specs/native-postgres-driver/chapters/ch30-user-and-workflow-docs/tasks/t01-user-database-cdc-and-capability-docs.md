---
type: Task
id: "native-postgres-driver:ch30:t01"
chapter_id: ch30-user-and-workflow-docs
title: Update database usage guide, CDC analytics guide, and capability API references
description: Document vertebra-core[postgres] tabbed installation, native bind/query/pipeline/value/COPY/Arrow/PgBouncer/DSN contracts, unified $n tenant binds, PostgresNotificationSource, and vertebra.db.adapters.postgres.change_stream across databases.md, analytics-and-cdc.md, and database-capabilities.md.
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
  - tools/docs/verify_docs_artifacts.py
  - src/py/vertebra/db/adapters/postgres/config.py
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - docs/usage/databases.md
  - docs/usage/analytics-and-cdc.md
  - docs/reference/database-capabilities.md
tests: []
estimated_loc: 230
plan_revision: 6
plan_commit: ef86844f
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
updated_at: "2026-11-10T14:45:00Z"
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
  - id: docs-verifier
    resource: tools/docs/verify_docs_artifacts.py::verify_site_artifacts
    title: Zensical TOC, HTML byte-budget, and empty-attribute-row verifier
  - id: pg-config
    resource: src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig
    title: PostgreSQL DSN, TLS, pooler_mode, direct_dsn, and cloud configuration
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

# Task t01: Update Database Usage Guide, CDC Analytics Guide, and Capability API References

## Objective & Scope Boundary

Update `docs/usage/databases.md`, `docs/usage/analytics-and-cdc.md`, and `docs/reference/database-capabilities.md` in present tense to document the native `vertebra-postgres` (`vertebra-core[postgres]`) driver, `PostgresEmbeddingPushdown`, and `vertebra.db.adapters.postgres.change_stream` contracts [^doc-databases] [^doc-cdc] [^doc-capabilities] [^docs-verifier] [^pg-config] [^pg-contracts]:
1. `docs/usage/databases.md`:
   - Add `vertebra-core[postgres]` across all four linked `pymdownx.tabbed` blocks (`=== "uv"`, `=== "pip"`, `=== "pdm"`, `=== "poetry"`) under `Optional Engine Extras`.
   - Document `PostgreSQLConfig`, `SyncPostgreSQLDriver`, and `AsyncPostgreSQLDriver` pool ownership (`share_pool`, `validate_configuration`), `$n` numeric passthrough vs SQL-aware raw `%s`/`%(name)s` lowering (requiring explicit `E'...'` for backslash-escaped string literals), SQLSpec `ParameterStyle.NUMERIC` and SQLAlchemy `+vtb` `numeric_dollar`, single-statement extended `execute` vs parameterless `query_multi(sql)`, per-command `timeout`, delayed-commit single-`Sync` `pipeline()`, lossless `PgInterval`/`PgArray`/`PgComposite`/`PgTemporal` (`wire_text` typed-rebind refusal)/`PgJson`/`PgBitString`/`PgVector` (`1..16000`), raw native `vertebra._postgres.{SyncPostgresClient, AsyncPostgresClient}` `copy_to`, `copy_from`, and `copy_from_arrow(table_name, first_batch_ipc, *, columns=None, schema_name=None)` (`write_arrow_ipc`/`finish`/`close`/`aclose`) vs Python `SyncPostgreSQLDriver`/`AsyncPostgreSQLDriver` `load_from_arrow(table_name, data, *, columns=None, schema_name=None)`, `select_to_arrow`, and `stream_arrow_batches` (`VTBARROW` `ArrowCursor`), `PgBouncer` `pooler_mode` (`direct`/`session`/`transaction`/`statement`/`auto`), `direct_dsn`, and `PG*` DSN precedence (`sslmode="prefer"` adapter default vs explicit `sslmode="disable"` only for local non-TLS fixtures).
2. `docs/usage/analytics-and-cdc.md`:
   - Replace pre-cutover tenant placeholder prose and `listen_notifications()` wording with unified `$n` (`ParameterStyle.NUMERIC`) across sync and async PostgreSQL drivers and dedicated `_open_notification_listener` / `PostgresNotificationSource` (`direct_dsn` bypass, bounded `queue_capacity`, `reconnect`).
   - Document `vertebra.db.adapters.postgres.change_stream` (`PostgresChangeConfig`, `PostgresChangeSource`, `PostgresCheckpoint`, `PostgresCheckpointStore`, `MemoryPostgresCheckpointStore`, `PostgresCheckpointConflictError`, `PostgresReplicationGapError`) covering caller-owned `pgoutput` v1 slot/publication tailing, CAS checkpoint persistence before WAL standby feedback, signed-`int64` replay IDs, and explicit `acknowledge(commit_end_lsn)`.
3. `docs/reference/database-capabilities.md`:
   - Render mkdocstrings `:::` directives using `heading_level: 4` (with `members: false` or concise curated `members:` and `summary: false`) across `PostgresChangeConfig`, `PostgresCheckpoint`, `PostgresCheckpointStore`, `MemoryPostgresCheckpointStore`, `PostgresChangeSource`, `PostgresCheckpointConflictError`, `PostgresReplicationGapError`, `PostgresEmbeddingPushdown`, `PostgresNotifyConfig`, and `PostgresNotificationSource` so `H2+H3` secondary TOC stays `<= 14` and `site/reference/database-capabilities/index.html` stays `<= 120,000` bytes [^docs-verifier].

Presentation rules across all three files: keep any Mermaid diagrams high-level and conceptual (`4` to `6` nodes focused on user mental models without internal Rust/helper clutter), declare public examples with plain `class Record(msgspec.Struct):` without `frozen=True, gc=False` boilerplate, pass configuration dicts/kwargs inline without single-use intermediate variables, include zero inline `#` Python comments, and never mention or compare against external SQL, ORM, or Python PostgreSQL client driver libraries.

Out of scope: Modifying Python/Rust source files, `native-stubs.md` / `reference/index.md` / `installation.md` (`ch30:t02`), cloud/packaging/queue guides (`ch30:t03`), or `README.md` / `workflow.md` / `AGENTS.md` (`ch30:t04`).

## Bounded Read Manifest

- [docs/usage/databases.md](../../../../../../../docs/usage/databases.md) — Current optional engine extras, driver usage, and SQLSpec/SQLAlchemy examples [^doc-databases].
- [docs/usage/analytics-and-cdc.md](../../../../../../../docs/usage/analytics-and-cdc.md) — Current RLS tenant isolation, notifications, CDC, and Arrow guide [^doc-cdc].
- [docs/reference/database-capabilities.md](../../../../../../../docs/reference/database-capabilities.md) — Current `heading_level: 4` mkdocstrings layout [^doc-capabilities].
- [tools/docs/verify_docs_artifacts.py](../../../../../../../tools/docs/verify_docs_artifacts.py) — Secondary TOC (`<= 14`), reference HTML (`<= 120,000` bytes), page HTML (`<= 180,000` bytes), and empty-attribute-row checks [^docs-verifier].
- [src/py/vertebra/db/adapters/postgres/config.py](../../../../../../../src/py/vertebra/db/adapters/postgres/config.py) — `PostgreSQLConfig` options and defaults [^pg-config].
- [.agents/bundles/specs/native-postgres-driver/contracts.md](../../../contracts.md) — Public constructor, bind, pipeline, Arrow, value, notification, and `change_stream` contracts [^pg-contracts].

## Symbol & Signature Contracts

- Every `::: vertebra.db.adapters.postgres.change_stream.*`, `notify.*`, and `embeddings.PostgresEmbeddingPushdown` directive in `docs/reference/database-capabilities.md` resolves via Griffe under `heading_level: 4` with zero empty `Attributes` table rows (`_EMPTY_ATTRIBUTE_ROW_PATTERN`) [^docs-verifier].
- Every installation block uses linked `=== "uv"`, `=== "pip"`, `=== "pdm"`, and `=== "poetry"` tabs; every `msgspec.Struct` snippet omits `frozen=True, gc=False` and passes config options inline.

## Edge Cases & Error Semantics

- Do not claim automatic replication slot DDL, initial table snapshot backfill, automatic WAL ack on yield, `listen_notifications()`, or multi-host `target_session_attrs` failover; document explicit caller-owned slots/publications, `MemoryPostgresCheckpointStore` non-durability across restarts, and `direct_dsn` requirements behind `PgBouncer`.
- Keep `databases.md` and `analytics-and-cdc.md` each within `max_toc_items <= 14` (`H2+H3`) and `max_page_bytes <= 180_000`, preserving non-PostgreSQL engine examples (`sqlite:///:memory:`, BigQuery, Spanner, DuckDB) unchanged [^docs-verifier].

## Public-Seam Test Blueprint

- **Canonical validation gates**: `make docs`, `make docs-check`, and `make review-check` (`tests: []`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Griffe resolution & reference page budgets | `make docs` inspecting `site/reference/database-capabilities/index.html` | Exits `0`; all ten PostgreSQL capability directives resolve, secondary TOC `H2+H3 <= 14`, HTML byte length `<= 120,000`, and zero empty `Attributes` table rows. |
| Tabbed install, Mermaid, & `msgspec` example audit | Inspect `docs/usage/databases.md` and `docs/usage/analytics-and-cdc.md` + `make docs-check` | Linked `uv`/`pip`/`pdm`/`poetry` tabs present, any Mermaid block has `4`–`6` conceptual nodes, zero `frozen=True, gc=False` or inline `#` comments in examples, zero external PostgreSQL driver library mentions, and live `zensical serve` responds HTTP `200`. |
| Privacy & bundle hygiene | `make review-check` | Exits `0` with zero forbidden terms or count-prefixed phrase violations across `docs/**/*.md`. |

## Verification

- Build & artifact budget check: `make docs && make docs-check`
- Documentation privacy & structure check: `make review-check`

## Acceptance Criteria

- [ ] `docs/usage/databases.md`, `docs/usage/analytics-and-cdc.md`, and `docs/reference/database-capabilities.md` document `vertebra-core[postgres]`, `$n`/`numeric_dollar` binds, `query_multi`, single-`Sync` `pipeline()`, lossless `Pg*` records, binary `COPY`/`VTBARROW`, `PostgresNotificationSource`, `PostgresEmbeddingPushdown`, and `vertebra.db.adapters.postgres.change_stream.*` using concise inline examples and zero external driver mentions (R27).
- [ ] `make docs`, `make docs-check`, and `make review-check` exit `0` with `database-capabilities/index.html <= 120,000` bytes and every page secondary TOC `<= 14` (R27).

## Notes & Discoveries

- Group PostgreSQL change-stream, notification, and embedding directives under the existing capability `H2` headings in `docs/reference/database-capabilities.md` with `heading_level: 4` so secondary TOC stays at `2` items (`<= 14`).

[^doc-databases]: `docs/usage/databases.md` — User-facing Databases and Native Drivers usage guide.
[^doc-cdc]: `docs/usage/analytics-and-cdc.md` — User-facing RLS, CDC, and Arrow Analytics usage guide.
[^doc-capabilities]: `docs/reference/database-capabilities.md` — API reference for notifications, change streams, and tools.
[^docs-verifier]: `tools/docs/verify_docs_artifacts.py::verify_site_artifacts` — Zensical TOC, HTML byte-budget, and empty-attribute-row verifier.
[^pg-config]: `src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig` — PostgreSQL DSN, TLS, `pooler_mode`, `direct_dsn`, and cloud configuration.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Authoritative native PostgreSQL public contracts.
