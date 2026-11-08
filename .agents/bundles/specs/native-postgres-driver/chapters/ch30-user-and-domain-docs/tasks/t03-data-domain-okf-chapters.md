---
type: Task
id: "native-postgres-driver:ch30:t03"
chapter_id: ch30-user-and-domain-docs
title: Synchronize data domain OKF chapters with native PostgreSQL drivers and change_stream contracts
description: Update database-drivers-and-dialers.md and query-pipeline-cdc-and-tenancy.md (<= 200 lines each) in present tense for vertebra-postgres, unified SQLSpec NUMERIC compilation, single-Sync pipelines, and vertebra.db.adapters.postgres.change_stream checkpoints.
status: stable
state: pending
priority: P1
verification_strategy: documentation_validation
depends_on:
  - "native-postgres-driver:ch30:t02"
read_set:
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  - .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
  - src/py/vertebra/db/adapters/postgres/driver.py
  - src/py/vertebra/extensions/sqlspec/adapters.py
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  - .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
tests: []
estimated_loc: 185
plan_revision: 4
plan_commit: dd432f18
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
updated_at: "2026-11-07T18:00:00Z"
commit: null
sources:
  - id: okf-drivers
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: OKF chapter for database drivers, Arrow SHM, and Cloud SQL / AlloyDB dialers
  - id: okf-cdc
    resource: .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
    title: OKF chapter for native pipelines, CDC change feeds, and tenant predicates
  - id: pg-driver
    resource: src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver
    title: Permanent native SyncPostgreSQLDriver and AsyncPostgreSQLDriver barrel
  - id: sqlspec-adapters
    resource: src/py/vertebra/extensions/sqlspec/adapters.py::BaseSyncDriver
    title: SQLSpec PostgreSQL sync and async adapters using unified NUMERIC binds
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Frozen native PostgreSQL client, pool, value, and change_stream contracts
tags:
  - postgres
  - okf
  - knowledge
  - cdc
---

# Task t03: Synchronize Data Domain OKF Chapters with Native PostgreSQL Drivers and `change_stream` Contracts

## Objective & Scope Boundary

Synchronize `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` and `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` in strict present tense (`<= 200` lines each per `DOC-08`) to reflect the completed `vertebra-postgres` architecture and `vertebra.db.adapters.postgres.change_stream` contracts [^okf-drivers] [^okf-cdc] [^pg-driver] [^sqlspec-adapters] [^pg-contracts]:
1. `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` (`<= 200` lines):
   - Update the bounded truth block (`<= 40` lines) and PostgreSQL adapter row/prose to document `src/rs/vertebra-postgres` (`vertebra._postgres`, `vertebra-core[postgres]`) owning live Protocol 3.0 sockets (`SyncPostgresClient`, `AsyncPostgresClient`), physical connection pools (`SyncPostgresPool`, `AsyncPostgresPool`), `SCRAM-SHA-256`/`SCRAM-SHA-256-PLUS`, automatic Cloud SQL and AlloyDB Admin discovery + scoped IAM/mTLS caches, lossless `Pg*` value records, binary `COPY`, and native `VTBARROW` `ShmCapsule` production, while `vertebra-db/src/postgres/mod.rs` retains only stateless CPU wire and codec helpers (`driver-fail-loud` deletes the SHM-admission `PostgresPool`).
   - Add attested `sources:` entries and matching provenance footnotes for the live `src/rs/vertebra-postgres/` and `src/py/vertebra/db/adapters/postgres/` modules present on the branch at `ch30:t03` execution time, and prepend a fresh `verified:` entry.
2. `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` (`<= 200` lines):
   - Replace stale pre-cutover claims (`PostgreSQL sync uses positional pyformat and async uses numeric binds` and `sync uses psycopg %s, async uses asyncpg $N`) with present-tense unified `ParameterStyle.NUMERIC` (`$N`) compilation across both `PostgreSQLSyncDriver` and `PostgreSQLAsyncDriver` (and lexer-aware `%s`/`%(name)s` lowering on raw driver calls).
   - Document PostgreSQL's single-`Sync` / single-logical-flush `pipeline()` transaction/savepoint retention through caller hydration, `PostgresNotificationSource` delegation to dedicated `_publish_notification` / `_open_notification_listener` hooks, and `PostgresChangeSource` + `PostgresCheckpointStore` (`src/py/vertebra/db/adapters/postgres/change_stream.py`, `vertebra.db.adapters.postgres.change_stream`) `CopyBoth` `pgoutput` v1 logical replication tail/resume, shielded CAS checkpoint persistence before WAL feedback, and signed-`int64` `next_event_id` replay semantics.
   - Synchronize the truth block (`<= 40` lines), `sources:`, provenance footnotes, and `verified:` commit stamp without standalone `---` rules or historical cutover prose.

Out of scope: Modifying extensions/testing/product OKF chapters (`ch30:t04`) or deleting active spec files (`/flow:archive` after `ch31:t01`).

## Bounded Read Manifest

- [.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md](../../../../../knowledge/domains/data/database-drivers-and-dialers.md) — Current adapter matrix, dialer prose, and provenance footnotes [^okf-drivers].
- [.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md](../../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md) — Current pipeline, tenant predicate, and CDC tables [^okf-cdc].
- [src/py/vertebra/db/adapters/postgres/driver.py](../../../../../../../src/py/vertebra/db/adapters/postgres/driver.py) — Final `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` surface [^pg-driver].
- [src/py/vertebra/extensions/sqlspec/adapters.py](../../../../../../../src/py/vertebra/extensions/sqlspec/adapters.py) — `PostgreSQLSyncDriver` and `PostgreSQLAsyncDriver` default statement config and `execute_stack` [^sqlspec-adapters].
- [.agents/bundles/specs/native-postgres-driver/contracts.md](../../../contracts.md) — Authoritative value, pipeline, notification, and `change_stream` contracts [^pg-contracts].

## Symbol & Signature Contracts

- Both OKF files remain strictly `<= 200` lines (`DOC-08`) with `type: Guide`, `status: stable`, multi-line YAML mappings (`generated:`, `verified:`, `sources:`, `tags:` with zero state words), `<!-- truth: start --> ... <!-- truth: end -->` (`<= 40` lines immediately below `# H1`), 1:1 `sources[].id` <-> inline citation <-> bottom footnote definition parity, relative Markdown links resolving on disk, and zero standalone `---` body lines.

## Edge Cases & Error Semantics

- Do not leave residual references to `psycopg` or `asyncpg` as active PostgreSQL runtime drivers anywhere in either OKF chapter; reference `src/py/vertebra/db/adapters/postgres/change_stream.py` (`vertebra.db.adapters.postgres.change_stream`) exclusively for PostgreSQL logical replication change streams.
- Every `sources[].resource` added during `ch30:t03` execution must exist in the repository working tree at that commit so `tools/scripts/review_setup.py` (`make review-check`) validates 100% of `sources:` paths cleanly.

## Public-Seam Test Blueprint

- **Canonical validation gates**: `make review-check`, `make docs`, and `<= 200` line checks (`tests: []`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| OKF v0.2 frontmatter, line-budget, & provenance check | `make review-check` + `wc -l` on both data OKF chapters | Exits `0`; both files are `<= 200` lines and pass YAML block syntax, `sources:` path existence, provenance footnote parity, truth-block `<= 40` line limit, and privacy/cliché checks. |
| Present-tense PostgreSQL SQLSpec & `change_stream` accuracy | Audit `query-pipeline-cdc-and-tenancy.md` against `sqlspec/adapters.py`, `postgres/notify.py`, and `postgres/change_stream.py` | Zero stale `sync uses positional pyformat` / `psycopg` / `asyncpg` claims; `PostgresChangeSource` and `PostgresCheckpointStore` (`change_stream.py`) contracts match live symbols. |
| Relative link & docs build integrity | `make docs` | Exits `0` with all relative links across `.agents/bundles/knowledge/domains/data/` resolving cleanly. |

## Verification

- Line-budget & OKF validator gate: `test "$(wc -l < .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md)" -le 200 && test "$(wc -l < .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md)" -le 200 && make review-check`
- Documentation & link gate: `make docs`

## Acceptance Criteria

- [ ] `database-drivers-and-dialers.md` and `query-pipeline-cdc-and-tenancy.md` accurately capture the present-tense `vertebra-postgres` engine, unified SQLSpec `NUMERIC` compilation, single-`Sync` `pipeline()`, and `vertebra.db.adapters.postgres.change_stream` `pgoutput` v1 architecture within `<= 200` lines each (R27).
- [ ] `make review-check` and `make docs` exit `0` with strict 1:1 `sources:`-to-footnote provenance parity and zero forbidden patterns (R27).

## Notes & Discoveries

- [2026-11-07 16:45] `query-pipeline-cdc-and-tenancy.md` sits at `188` lines at baseline; replace pre-cutover PostgreSQL lines concisely in-place so the updated chapter remains `<= 200` lines.

[^okf-drivers]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — OKF chapter for database drivers, Arrow SHM, and Cloud SQL / AlloyDB dialers.
[^okf-cdc]: `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` — OKF chapter for native pipelines, CDC change feeds, and tenant predicates.
[^pg-driver]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — Permanent native `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` barrel.
[^sqlspec-adapters]: `src/py/vertebra/extensions/sqlspec/adapters.py::BaseSyncDriver` — SQLSpec PostgreSQL sync and async adapters using unified `NUMERIC` binds.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Frozen native PostgreSQL client, pool, value, and `change_stream` contracts.
