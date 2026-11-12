---
type: Task
id: "native-postgres-driver:ch31:t02"
chapter_id: ch31-data-and-testing-okf-sync
title: Synchronize query-pipeline-cdc-and-tenancy and register postgres-native-driver in knowledge indexes
description: Rewrite query-pipeline-cdc-and-tenancy.md (<= 200L) in present tense for unified $n ParameterStyle.NUMERIC, delayed-commit pipeline(), and change_stream CAS checkpointing, and register postgres-native-driver.md in frontmatter-free knowledge/index.md and knowledge/domains/index.md beside the existing postgres-limitations.md entries.
status: draft
state: pending
priority: P1
verification_strategy: documentation_validation
depends_on:
  - "native-postgres-driver:ch31:t01"
read_set:
  - .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
  - .agents/bundles/knowledge/index.md
  - .agents/bundles/knowledge/domains/index.md
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  - src/py/vertebra/db/adapters/postgres/notify.py
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
  - .agents/bundles/knowledge/index.md
  - .agents/bundles/knowledge/domains/index.md
tests: []
estimated_loc: 165
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
created_at: "2026-11-10T14:45:00Z"
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
  - id: okf-cdc
    resource: .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
    title: Query pipeline, CDC change streams, and RLS tenancy OKF chapter
  - id: okf-root-index
    resource: .agents/bundles/knowledge/index.md
    title: Frontmatter-free root knowledge catalog
  - id: okf-domains-index
    resource: .agents/bundles/knowledge/domains/index.md
    title: Frontmatter-free domain knowledge catalog
  - id: pg-notify
    resource: src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource
    title: PostgreSQL dedicated notification source and publish contract
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Unified $n compilation, delayed-commit pipeline, EventSessionRegistry, and change_stream contracts
tags:
  - postgres
  - okf
  - cdc
  - pipeline
  - indexes
---

# Task t02: Synchronize `query-pipeline-cdc-and-tenancy` and Register `postgres-native-driver` in Knowledge Indexes

## Objective & Scope Boundary

Update `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` (`214` lines at `c8722c76` -> `<= 200` lines) in present tense and register `postgres-native-driver.md` (created in `ch31:t01`) in `.agents/bundles/knowledge/index.md` and `.agents/bundles/knowledge/domains/index.md`, keeping the `postgres-limitations.md` entries that `native-postgres-driver:ch22:t04` registered [^okf-cdc] [^okf-root-index] [^okf-domains-index] [^pg-notify] [^pg-contracts]:
1. `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` (`<= 200` lines):
   - Replace the tenant-predicate paragraph that says sync PostgreSQL compiles to psycopg `%s` and async to asyncpg `$N`, and every `listen_notifications()` reference, with unified `$n` `ParameterStyle.NUMERIC` across `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` (including `$n` RLS tenant predicate injection and immutable `StatementConfig` preparation).
   - Document retained transaction/savepoint single-`Sync` `pipeline()` (`pipeline_begin`, `pipeline_results`, `pipeline_finish` deferring commit until clean context exit after caller conversion/cardinality checks; ordered `PostgresPipelineOutcome` with canonical `DatabaseError.outcomes` and `.failing_index`).
   - Document `EventSessionRegistry` dedicated direct-connection pairing (`_open_notification_listener`, `_publish_notification`, `_replication_identity`, `_open_replication_stream`) and `vertebra.db.adapters.postgres.change_stream` (`PostgresChangeConfig`, `PostgresChangeSource`, `PostgresCheckpoint`, `PostgresCheckpointStore`, `MemoryPostgresCheckpointStore`, `PostgresCheckpointConflictError`, `PostgresReplicationGapError`, and `PostgresReplicationRecord(msgspec.Struct, frozen=True)` omitting `gc=False` for container-bearing row values), covering shielded CAS checkpoint persistence before WAL standby feedback and signed-`int64` (`1..2**63-1`) monotone replay IDs, cross-linking `postgres-native-driver.md` and `postgres-limitations.md` [^okf-cdc] [^pg-notify] [^pg-contracts].
2. `.agents/bundles/knowledge/index.md` (frontmatter-free):
   - Register `[Native PostgreSQL Driver](domains/data/postgres-native-driver.md)` under `### Data, Drivers, Cloud Dialers & Analytics` beside the existing `postgres-limitations.md` entry, and refresh the one-line summaries for `database-drivers-and-dialers.md` and `query-pipeline-cdc-and-tenancy.md` [^okf-root-index].
3. `.agents/bundles/knowledge/domains/index.md` (frontmatter-free):
   - Register `[data/postgres-native-driver.md](data/postgres-native-driver.md)` under `## Data` beside the existing `data/postgres-limitations.md` entry with a matching concise description, keeping zero YAML frontmatter and zero standalone `---` rules [^okf-domains-index].

Out of scope: Modifying `postgres-native-driver.md` or `postgres-limitations.md` (`ch31:t01`), adding or removing the `postgres-limitations.md` index entries (`ch22:t04`), SQLSpec/testing chapters (`ch31:t03`), or product/queue chapters (`ch31:t04`).

## Bounded Read Manifest

- [.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md](../../../../../../../.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md) — Current `214`-line pipeline, CDC, and tenancy chapter [^okf-cdc].
- [.agents/bundles/knowledge/index.md](../../../../../../../.agents/bundles/knowledge/index.md) — Root knowledge directory catalog [^okf-root-index].
- [.agents/bundles/knowledge/domains/index.md](../../../../../../../.agents/bundles/knowledge/domains/index.md) — Domain knowledge directory catalog [^okf-domains-index].
- [.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md](../../../../../../../.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md) — Cross-engine overview split in `ch31:t01`.
- [src/py/vertebra/db/adapters/postgres/notify.py](../../../../../../../src/py/vertebra/db/adapters/postgres/notify.py) — `PostgresNotificationSource` implementation [^pg-notify].
- [.agents/bundles/specs/native-postgres-driver/contracts.md](../../../contracts.md) — Pipeline outcome, `EventSessionRegistry`, and `change_stream` CAS contracts [^pg-contracts].

## Symbol & Signature Contracts

- `query-pipeline-cdc-and-tenancy.md` cites the delivered `src/py/vertebra/db/adapters/postgres/change_stream.py` and `events.py` modules in `sources:` with 1:1 inline/footer footnotes, `<= 40`-line truth block, `<= 200` total lines, and updated `verified[0].commit`.
- `.agents/bundles/knowledge/index.md` and `.agents/bundles/knowledge/domains/index.md` contain zero YAML frontmatter (`okf_reserved = ["index.md", "log.md"]`), exactly one entry each for `postgres-native-driver.md` and `postgres-limitations.md`, and zero broken relative `.md` links.

## Edge Cases & Error Semantics

- Do not add YAML frontmatter or standalone `---` horizontal rules to `knowledge/index.md` or `knowledge/domains/index.md`; `review_setup.py` fails on reserved index frontmatter or standalone rules.
- If either index lacks the `postgres-limitations.md` entry when this task starts, add it beside `postgres-native-driver.md` and record the gap under `## Notes & Discoveries` instead of leaving the chapter unindexed.
- Preserve non-PostgreSQL sections in `query-pipeline-cdc-and-tenancy.md` (`SpannerChangeStreamSource`, `MySQLBinlogPollingSource`, `BigQueryTableDiffSource`, dialect-aware RLS predicates across MySQL/Oracle/SQLite/Spanner) while compressing verbose prose to stay `<= 200` lines.

## Public-Seam Test Blueprint

- **Canonical validation gates**: `make review-check` and `wc -l` (`tests: []`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Line budget & reserved index schema | `wc -l .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` + `head -1 .agents/bundles/knowledge/index.md .agents/bundles/knowledge/domains/index.md` | `query-pipeline-cdc-and-tenancy.md <= 200` lines; both indexes start directly with `# ` headings and carry zero frontmatter. |
| Single registration per chapter | `git grep -c "postgres-native-driver.md\|postgres-limitations.md" -- .agents/bundles/knowledge/index.md .agents/bundles/knowledge/domains/index.md` | Each index reports exactly `2` matching lines (one per chapter). |
| Link resolution & OKF validation | `make review-check` | Exits `0`; relative links to `postgres-native-driver.md` and `postgres-limitations.md` resolve from all three files, every `sources:` entry exists on disk, and 1:1 footnote parity holds. |
| Stale placeholder & CDC contract check | `git grep -n -i -E "psycopg|asyncpg|listen_notifications" -- .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` | No output; the chapter documents `$n` `ParameterStyle.NUMERIC`, delayed-commit `pipeline()`, `EventSessionRegistry`, and `vertebra.db.adapters.postgres.change_stream` CAS checkpointing. |

## Verification

- Line-budget check: `wc -l .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md`
- Stale claim audit: `git grep -n -i -E "psycopg|asyncpg|listen_notifications" -- .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` (expects no output)
- Index registration check: `git grep -c "postgres-native-driver.md\|postgres-limitations.md" -- .agents/bundles/knowledge/index.md .agents/bundles/knowledge/domains/index.md`
- OKF bundle & link check: `make review-check`

## Acceptance Criteria

- [ ] `query-pipeline-cdc-and-tenancy.md` (`<= 200L`) documents unified `$n` `ParameterStyle.NUMERIC`, single-`Sync` delayed-commit `pipeline()`, `EventSessionRegistry`, and `vertebra.db.adapters.postgres.change_stream` CAS checkpointing in present tense (R17, R21, R23, R24, R27).
- [ ] Frontmatter-free `knowledge/index.md` and `knowledge/domains/index.md` register `postgres-native-driver.md` beside the existing `postgres-limitations.md` entry, and `make review-check` exits `0` (R27).

## Notes & Discoveries

- Notification source naming: this worksheet executes before `queues-tasks-and-channels` t01 splits `NotificationSource` into `SyncNotificationSource` / `AsyncNotificationSource` and renames `PostgresNotificationSource` to `AsyncPostgresNotificationSource` alongside a new `SyncPostgresNotificationSource` (see [native-data-platform](../../../../native-data-platform/spec.md) Seam 2). It keeps the HEAD names, adds no new unprefixed notification protocol, class, or alias, and leaves the rename of every symbol, doc, and OKF reference it touches to that task.
- Inspect `src/py/vertebra/db/adapters/postgres/change_stream.py` and `events.py` (delivered in `ch24` and `ch26`) when updating `sources:` in `query-pipeline-cdc-and-tenancy.md` so exact symbol anchors match the final modules.

[^okf-cdc]: `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` — Query pipeline, CDC change streams, and RLS tenancy OKF chapter.
[^okf-root-index]: `.agents/bundles/knowledge/index.md` — Frontmatter-free root knowledge catalog.
[^okf-domains-index]: `.agents/bundles/knowledge/domains/index.md` — Frontmatter-free domain knowledge catalog.
[^pg-notify]: `src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource` — PostgreSQL dedicated notification source and publish contract.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Unified `$n` compilation, delayed-commit pipeline, `EventSessionRegistry`, and `change_stream` contracts.
