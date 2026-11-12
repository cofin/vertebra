---
type: Task
id: "native-postgres-driver:ch31:t01"
chapter_id: ch31-data-and-testing-okf-sync
title: Split database-drivers-and-dialers, create postgres-native-driver, and extend postgres-limitations OKF chapters
description: Refactor database-drivers-and-dialers.md into a <= 200-line cross-engine overview, create postgres-native-driver.md (Concept, <= 200L), and extend the existing postgres-limitations.md (Gotcha, <= 200L) with vertebra-postgres runtime caveats.
status: draft
state: pending
priority: P1
verification_strategy: documentation_validation
depends_on:
  - "native-postgres-driver:ch30:t04"
read_set:
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  - .agents/bundles/knowledge/domains/data/postgres-limitations.md
  - .agents/bundles/knowledge/domains/storage/object-storage-limitations.md
  - src/rs/vertebra-postgres/src/lib.rs
  - src/py/vertebra/db/adapters/postgres/config.py
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  - .agents/bundles/knowledge/domains/data/postgres-native-driver.md
  - .agents/bundles/knowledge/domains/data/postgres-limitations.md
tests: []
estimated_loc: 245
plan_revision: 7
plan_commit: c8722c76
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
updated_at: "2026-11-12T01:30:00Z"
commit: null
generated:
  by: flow-revise
  at: "2026-11-12T01:30:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T01:30:00Z"
    commit: c8722c76
sources:
  - id: okf-drivers
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: Existing cross-engine database drivers and cloud dialers chapter
  - id: okf-pg-limits
    resource: .agents/bundles/knowledge/domains/data/postgres-limitations.md
    title: PostgreSQL limitations Gotcha chapter created by ch22:t04
  - id: okf-storage-limits
    resource: .agents/bundles/knowledge/domains/storage/object-storage-limitations.md
    title: Reference OKF Gotcha limitations chapter structure
  - id: pg-lib-rs
    resource: src/rs/vertebra-postgres/src/lib.rs
    title: Standalone Rust PostgreSQL Protocol 3.0 extension crate
  - id: pg-config
    resource: src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig
    title: PostgreSQL adapter DSN, TLS, pooler_mode, direct_dsn, and cloud config
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Authoritative native PostgreSQL protocol, codec, CDC, and pooler contracts
tags:
  - postgres
  - okf
  - data
  - limitations
  - architecture
---

# Task t01: Split `database-drivers-and-dialers`, Create `postgres-native-driver`, and Extend `postgres-limitations` OKF Chapters

## Objective & Scope Boundary

Refactor `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` below `200` lines, author `.agents/bundles/knowledge/domains/data/postgres-native-driver.md` (`type: Concept`, `<= 200` lines), and update `.agents/bundles/knowledge/domains/data/postgres-limitations.md` (`type: Gotcha`, created and indexed by `native-postgres-driver:ch22:t04`, split-state caveats removed by `ch27:t04`) in OKF v0.2 present tense [^okf-drivers] [^okf-pg-limits] [^okf-storage-limits] [^pg-lib-rs] [^pg-config] [^pg-contracts]:
1. `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` (`257` lines at `c8722c76` -> `<= 200` lines):
   - Retain cross-engine coverage (`DatabaseConfig`, `SyncDriver`/`AsyncDriver`, `LeaseBroker`, `ArrowCursor`, Oracle, SQLite, MySQL, DuckDB, Redis, MSSQL, and standalone `CloudSQLDialer` / `AlloyDBDialer`), replace the sentence pairing the PostgreSQL adapter with `psycopg`/`psycopg_pool` client transports by a crisp summary of `src/rs/vertebra-postgres` (`vertebra._postgres`, `vertebra-core[postgres]`) and `src/py/vertebra/db/adapters/postgres/`, add `postgres` to the native-wheel engine enumeration in the conformance paragraph, and cross-link `[postgres-native-driver.md](postgres-native-driver.md)` and `[postgres-limitations.md](postgres-limitations.md)` [^okf-drivers] [^pg-lib-rs].
2. `.agents/bundles/knowledge/domains/data/postgres-native-driver.md` (new `type: Concept`, `<= 200` lines):
   - Curate `vertebra-postgres` and `vertebra.db.adapters.postgres` internals (detailed implementation Mermaid diagram allowed here): owned primitive-kwarg `SyncPostgresClient`/`AsyncPostgresClient` and physical `SyncPostgresPool`/`AsyncPostgresPool` (`DISCARD ALL` reset, `validate_configuration`, generation-fenced `CancelRequest` socket retirement), per-session LRU `StatementCache` and `PoolerMode` (`direct`/`session`/`transaction`/`statement`/`auto`), `SCRAM-SHA-256` and downgrade-safe `SCRAM-SHA-256-PLUS` `tls-server-end-point` channel binding, single-flight Cloud SQL and AlloyDB Admin API discovery + TLS 1.3 peer identity + scoped IAM (`cloud_auth_options` vs `iam_auth_options`), lossless `Pg*` `msgspec` codecs (`'Y'` catalog retention across `DISCARD ALL`), delayed-commit single-`Sync` `pipeline()`, incremental binary `COPY` + `copy_from_arrow(..., columns=None, schema_name=None)`, `select_to_arrow` / `VTBARROW` `ShmCapsule`, coalesced `read_buf` dedicated `LISTEN`/`NOTIFY` (`PendingDeliveryGuard` async drop restoration), and standalone `CopyBoth` `pgoutput` v1 logical replication + `EventSessionRegistry` [^pg-lib-rs] [^pg-config] [^pg-contracts].
3. `.agents/bundles/knowledge/domains/data/postgres-limitations.md` (existing `type: Gotcha`, `<= 200` lines, structure mirroring `object-storage-limitations.md` [^okf-storage-limits]):
   - Keep every entry already present and add each of the following that it does not yet list, with linked code provenance:
     - Single-host DSNs only (multi-host comma lists, `target_session_attrs`, and opportunistic downgrade `sslmode=allow` fail at validation).
     - Tail-only `pgoutput` v1 logical replication on caller-owned slots/publications without slot DDL or automatic initial table snapshot backfill.
     - Non-durable default `MemoryPostgresCheckpointStore` (requires a caller-supplied durable `PostgresCheckpointStore` for cross-restart continuity; CAS save precedes WAL `confirmed_flush_lsn` feedback).
     - Pyformat `%s`/`%(name)s` SQL lexer rejecting ambiguous ordinary backslash-quote literals (requiring native `$n` binds or explicit `E'...'` strings).
     - Extended-protocol single-statement `execute`/`query` (`42601` on multi-statement strings) versus explicit parameterless simple-query `query_multi(sql)`.
     - Cold unknown-OID pipeline commands falling back to text format (`format=0`) until session `pg_type` catalog metadata warms binary codecs, without hidden extra `Sync` round trips.
     - `PgBouncer` `statement` mode rejecting explicit transactions, savepoints, pipelines, portals, and `COPY` before emission, plus the `direct_dsn` requirement for `LISTEN`/`NOTIFY` and `pgoutput` v1 when queries traverse a pooler.
     - Ed25519 TLS server certificates lacking RFC 5929 `tls-server-end-point` digest derivation (`SCRAM-SHA-256-PLUS` unavailable; `channel_binding="require"` fails closed).
     - Local wire-mock and ephemeral TLS harnesses proving Cloud SQL and AlloyDB Admin/mTLS/IAM paths without live GCP network credentials in default local or CI runs.
     - `PostgreSQLQueueBackend` requires a caller-injected `PostgreSQLQueueExecutor`; the built-in native executor belongs to the `queues-tasks-and-channels` flow.
     - Any runtime, codec, platform, or service caveats recorded in completed `ch20`–`ch29` task worksheets or code.
   - Confirm the file no longer carries split-state caveats (Python drivers on `psycopg`/`asyncpg`); if any remain after `ch27`, remove them only when the code on disk confirms the cutover.

Out of scope: Creating `postgres-limitations.md` (`ch22:t04`), registering chapters in `knowledge/index.md` and `knowledge/domains/index.md` or rewriting `query-pipeline-cdc-and-tenancy.md` (`ch31:t02`).

## Bounded Read Manifest

- [.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md](../../../../../../../.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md) — Current `257`-line data drivers chapter to slim and cross-link [^okf-drivers].
- `.agents/bundles/knowledge/domains/data/postgres-limitations.md` — Existing PostgreSQL `Gotcha` chapter from `ch22:t04` [^okf-pg-limits].
- [.agents/bundles/knowledge/domains/storage/object-storage-limitations.md](../../../../../../../.agents/bundles/knowledge/domains/storage/object-storage-limitations.md) — Canonical `Gotcha` limitations format and provenance style [^okf-storage-limits].
- [src/rs/vertebra-postgres/src/lib.rs](../../../../../../../src/rs/vertebra-postgres/src/lib.rs) — Delivered Rust module hierarchy and re-exports [^pg-lib-rs].
- [src/py/vertebra/db/adapters/postgres/config.py](../../../../../../../src/py/vertebra/db/adapters/postgres/config.py) — Adapter DSN, TLS, `pooler_mode`, `direct_dsn`, and cloud configuration [^pg-config].
- [.agents/bundles/specs/native-postgres-driver/contracts.md](../../../contracts.md) — Complete protocol, codec, pipeline, Arrow, notification, and CDC invariants [^pg-contracts].

## Symbol & Signature Contracts

- All three OKF chapters carry valid multi-line YAML frontmatter (`type: Concept` on `database-drivers-and-dialers.md` and `postgres-native-driver.md`; `type: Gotcha` on `postgres-limitations.md`), `status: stable`, lowercase hyphenated `tags:`, updated `verified[0]` (`commit` set to current ancestor `HEAD`), `<= 40`-line truth blocks, zero standalone `---` body rules, and 1:1 `sources:`-to-footnote parity citing real `src/rs/vertebra-postgres/**` and `src/py/vertebra/db/adapters/postgres/**` symbols on disk.

## Edge Cases & Error Semantics

- Every `sources[].resource` file path and every relative Markdown link across all three chapters exists on disk when `make review-check` runs; never cite deleted modules (`src/py/vertebra/build/packaging/pgo.py` no longer exists) or uncreated paths.
- PGO is not a PostgreSQL limitation: `tools/pgo/rust_pgo.py::discover_extension_manifests` profiles `vertebra-postgres` through its crate-local `pyproject.toml`; do not record a PGO boundary.
- If `postgres-limitations.md` is absent when this task starts, stop and route through `/flow:revise`; never create a second limitations chapter.
- Keep all three files `<= 200` lines and free of historical changelog narratives, exclamation marks, or count-prefixed headings.

## Public-Seam Test Blueprint

- **Canonical validation gates**: `make review-check` and `wc -l` (`tests: []`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Line budgets across split chapters | `wc -l .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md .agents/bundles/knowledge/domains/data/postgres-native-driver.md .agents/bundles/knowledge/domains/data/postgres-limitations.md` | Each of the three files is `<= 200` lines with truth blocks `<= 40` lines. |
| OKF v0.2 frontmatter, source symbols, & link integrity | `make review-check` | Exits `0`; every `sources:` file exists on disk, 1:1 inline/footer footnotes match, relative links resolve between the three chapters, and privacy/pattern rules pass. |
| Stale runtime claim audit | `git grep -n -i -E "psycopg|asyncpg|pgo\.py" -- .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md .agents/bundles/knowledge/domains/data/postgres-native-driver.md .agents/bundles/knowledge/domains/data/postgres-limitations.md` | No output. |
| Caveat inventory audit | Inspect `postgres-limitations.md` against `contracts.md` and `ch20`–`ch29` notes | Covers single-host DSN/`sslmode=allow`, tail-only `pgoutput` v1, non-durable `MemoryPostgresCheckpointStore`, `%s` backslash refusal, `execute` vs `query_multi`, cold pipeline text fallback, `PgBouncer` `statement` + `direct_dsn`, Ed25519 binding, wire-mock cloud CI, caller-injected queue executor, and every `ch20`–`ch29` implementation caveat. |

## Verification

- Line-budget check: `wc -l .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md .agents/bundles/knowledge/domains/data/postgres-native-driver.md .agents/bundles/knowledge/domains/data/postgres-limitations.md`
- Stale claim audit: `git grep -n -i -E "psycopg|asyncpg|pgo\.py" -- .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md .agents/bundles/knowledge/domains/data/postgres-native-driver.md .agents/bundles/knowledge/domains/data/postgres-limitations.md` (expects no output)
- OKF & privacy check: `make review-check`

## Acceptance Criteria

- [ ] `database-drivers-and-dialers.md` (`<= 200L`), `postgres-native-driver.md` (`Concept`, `<= 200L`), and `postgres-limitations.md` (`Gotcha`, `<= 200L`) document the delivered `vertebra-postgres` architecture and complete operational limitations in present tense with zero stale `psycopg`/`asyncpg` runtime claims (R2, R5, R9, R15, R17, R19, R20, R21, R22, R27).
- [ ] `make review-check` exits `0` across all three files (R27).

## Notes & Discoveries

- Before extending `postgres-limitations.md`, scan the `## Notes & Discoveries` sections of completed `ch20`–`ch29` task worksheets so no runtime edge case remains undocumented prior to spec archive.

[^okf-drivers]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Existing cross-engine database drivers and cloud dialers chapter.
[^okf-pg-limits]: `.agents/bundles/knowledge/domains/data/postgres-limitations.md` — PostgreSQL limitations `Gotcha` chapter created by `ch22:t04`.
[^okf-storage-limits]: `.agents/bundles/knowledge/domains/storage/object-storage-limitations.md` — Reference OKF `Gotcha` limitations chapter structure.
[^pg-lib-rs]: `src/rs/vertebra-postgres/src/lib.rs` — Standalone Rust PostgreSQL Protocol 3.0 extension crate.
[^pg-config]: `src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig` — PostgreSQL adapter DSN, TLS, `pooler_mode`, `direct_dsn`, and cloud config.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Authoritative native PostgreSQL protocol, codec, CDC, and pooler contracts.
