---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch30-user-and-domain-docs
title: "Chapter 30: User Guides, Capability References, Workflow, and OKF v0.2 Knowledge Synchronization"
description: Synchronize all 11 invalidated user-facing guides, mkdocstrings capability references, repository entry points, and OKF v0.2 data/extensions/testing/product chapters in present tense for vertebra-postgres.
status: stable
state: planned
planning_status: complete
depends_on_chapters:
  - ch29-wheel-ci-and-benchmarks
replan_trigger: null
current_task: "native-postgres-driver:ch30:t01"
plan_revision: 4
plan_commit: dd432f18
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-07T18:00:00Z"
generated:
  by: flow-revise
  at: "2026-11-07T16:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-07T16:45:00Z"
    commit: dd432f18
sources:
  - id: doc-databases
    resource: docs/usage/databases.md
    title: User-facing Databases and Native Drivers usage guide
  - id: doc-cdc
    resource: docs/usage/analytics-and-cdc.md
    title: User-facing RLS, CDC, and Arrow Analytics usage guide
  - id: doc-capabilities
    resource: docs/reference/database-capabilities.md
    title: API reference for database notifications, change streams, and tools
  - id: okf-workflow
    resource: .agents/bundles/knowledge/workflow.md
    title: Canonical Makefile commands, polyglot rebuild cycle, and Flow workflow chapter
  - id: okf-cdc
    resource: .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
    title: OKF domain chapter for native pipelines, CDC, and tenant predicates
  - id: prod-tech-stack
    resource: .agents/bundles/product/tech-stack.md
    title: Product technology stack across Python, Rust crates, and companion wheels
tags:
  - postgres
  - documentation
  - okf
  - zensical
  - workflow
---

# Chapter 30: User Guides, Capability References, Workflow, and OKF v0.2 Knowledge Synchronization

<!-- truth: start -->
- **Complete 11-File Documentation & OKF Coverage (`DOC-04`)**: User guides (`databases.md`, `analytics-and-cdc.md`, `database-capabilities.md`), entry points (`README.md`, `workflow.md`, `AGENTS.md`), and OKF domain/testing/product chapters (`database-drivers-and-dialers.md`, `query-pipeline-cdc-and-tenancy.md`, `sqlspec-alchemy-and-dialects.md`, `benchmarks-and-free-threaded.md`, `tech-stack.md`) synchronize `vertebra-postgres` (`vertebra._postgres`, `vertebra-core[postgres]`) in present tense [^doc-databases] [^doc-cdc] [^doc-capabilities] [^okf-workflow] [^okf-cdc] [^prod-tech-stack].
- **Canonical Module & Wire Contracts**: Documentation names `vertebra.db.adapters.postgres.change_stream` (`PostgresChangeSource`, `PostgresCheckpointStore`, `MemoryPostgresCheckpointStore`), dedicated `_open_notification_listener` / `_open_replication_stream` hooks, and unified `ParameterStyle.NUMERIC` (`$N`) / `numeric_dollar` compilation with zero residual `psycopg`/`asyncpg` runtime claims [^doc-cdc] [^doc-capabilities] [^okf-cdc].
- **Strict Line-Budget & Provenance Discipline (`DOC-08`)**: `AGENTS.md` stays `<= 100` lines and every `.agents/bundles/knowledge/**/*.md` chapter stays `<= 200` lines with `<= 40`-line truth blocks, multi-line YAML frontmatter, and 1:1 `sources:`-to-footnote parity [^okf-workflow].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

After Chapters 01–29 implement, cut over, live-test, package, and benchmark `vertebra-postgres`, `11` user-facing, workflow, and OKF files across the repository contain invalidated pre-cutover claims: `docs/usage/analytics-and-cdc.md` and `sqlspec-alchemy-and-dialects.md` still claim PostgreSQL sync uses `psycopg %s` or `listen_notifications()`, `workflow.md` and `AGENTS.md` still list `5` PyO3 extensions / `8` Rust crates and omit `postgres-*` Make targets, `benchmarks-and-free-threaded.md` and `tech-stack.md` omit `vertebra-postgres` and `[dependency-groups].bench`, and `databases.md` / `database-capabilities.md` / `README.md` / data OKF chapters omit `vertebra.db.adapters.postgres.change_stream` and native pool/pipeline/value contracts [^doc-databases] [^doc-cdc] [^doc-capabilities] [^okf-workflow] [^okf-cdc] [^prod-tech-stack].

This chapter updates all `11` files across four bounded `documentation_validation` tasks (`<= 3` files per task):
1. Update `docs/usage/databases.md`, `docs/usage/analytics-and-cdc.md`, and `docs/reference/database-capabilities.md` (`ch30:t01`) [^doc-databases] [^doc-cdc] [^doc-capabilities].
2. Synchronize `README.md`, `.agents/bundles/knowledge/workflow.md` (`<= 200` lines), and `AGENTS.md` (`<= 100` lines) (`ch30:t02`) [^okf-workflow].
3. Synchronize `database-drivers-and-dialers.md` and `query-pipeline-cdc-and-tenancy.md` (`<= 200` lines each) (`ch30:t03`) [^okf-cdc].
4. Synchronize `sqlspec-alchemy-and-dialects.md` (`<= 200` lines), `benchmarks-and-free-threaded.md` (`<= 200` lines), and `tech-stack.md` (`ch30:t04`) [^prod-tech-stack].

### 1.2 Non-Goals

- Editing Python or Rust production source files (Google-style docstrings and `.pyi` stubs are finalized and verified in `ch08` and `ch25`–`ch27`).
- Running the final branch-wide correctness/quality review and complete-driver sign-off (owned by [ch31-final-review-and-release-gate](../ch31-final-review-and-release-gate/spec.md) `ch31:t01`).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md); frozen contracts: [contracts.md](../../contracts.md); depends on [ch29-wheel-ci-and-benchmarks](../ch29-wheel-ci-and-benchmarks/spec.md). Skills: [review-docs-okf](../../../../../skills/review-docs-okf/SKILL.md) and [review-core](../../../../../skills/review-core/SKILL.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R27.3 (`ch30:t01`)**: Update `docs/usage/databases.md`, `docs/usage/analytics-and-cdc.md` (replacing `%s` psycopg tenant bind & `listen_notifications()` claims with `$N` / `ParameterStyle.NUMERIC`, `_open_notification_listener`, and `vertebra.db.adapters.postgres.change_stream.PostgresChangeSource`), and `docs/reference/database-capabilities.md` (`vertebra.db.adapters.postgres.change_stream.*` + `PostgresEmbeddingPushdown` mkdocstrings targets), passing `make docs` and `make docs-check`.
- **R27.4 (`ch30:t02`)**: Synchronize `9` Rust crates (`vertebra-postgres`), `6` PyO3 extensions (`_postgres`), `[postgres]` extra, `--group bench` isolation, and `postgres-test` / `postgres-wheel-check` / `postgres-bench` / `postgres-bench-check` across `README.md`, `.agents/bundles/knowledge/workflow.md` (`<= 200` lines), and `AGENTS.md` (`<= 100` lines) per `DOC-08`.
- **R27.5 (`ch30:t03`)**: Update `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` and `query-pipeline-cdc-and-tenancy.md` (`<= 200` lines each) in present tense with `vertebra-postgres` (`vertebra._postgres`), `SyncPostgresClient`/`AsyncPostgresClient`, `SyncPostgresPool`/`AsyncPostgresPool`, `vertebra.db.adapters.postgres.change_stream` (`PostgresChangeSource`, `PostgresCheckpointStore`), dedicated event hooks, and unified `ParameterStyle.NUMERIC` (`$N`).
- **R27.6 (`ch30:t04`)**: Update `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` (`<= 200` lines), `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md` (`<= 200` lines), and `.agents/bundles/product/tech-stack.md` for unified `$N` SQLSpec/SQLAlchemy `+vtb` PostgreSQL compilation, `6` extensions + `[dependency-groups].bench` + `postgres-bench*`, and the `9`-crate / `5`-companion-wheel stack.

### 2.2 Documentation & Gate Map

- `docs/usage/databases.md` + `docs/usage/analytics-and-cdc.md` + `docs/reference/database-capabilities.md` -> `make docs` & `make docs-check` (Zensical build, Griffe mkdocstrings resolution, postbuild link verification, live HTTP 200 smoke).
- `README.md` + `AGENTS.md` (`<= 100` lines) + `5` OKF `knowledge/` chapters (`<= 200` lines each) + `tech-stack.md` -> `make review-check` (`tools/scripts/review_setup.py` OKF frontmatter, source-path existence, provenance footnote parity, truth-block bounds, and privacy/cliché checks) + `make docs`.

### 2.3 Edge-Case & Failure-Mode Matrix

| Boundary / Failure Mode | Deterministic Behavior |
| --- | --- |
| `workflow.md` (currently `192` lines) or other OKF `knowledge/` files growing past `200` lines (`DOC-08`) | Edit concisely in-place (consolidating repetitive command descriptions) so every `.agents/bundles/knowledge/**/*.md` stays strictly `<= 200` lines and `AGENTS.md` stays strictly `<= 100` lines. |
| Broken mkdocstrings `:::` target in `docs/reference/database-capabilities.md` | Every change-stream target resolves exclusively from `vertebra.db.adapters.postgres.change_stream.<Symbol>`, verified by `make docs` Griffe resolution. |
| Stale `psycopg`/`asyncpg` runtime claims vs bench-only group | Runtime driver docs/OKF remove all `psycopg`/`asyncpg` PostgreSQL claims (the `driver-fail-loud` baseline documents no PostgreSQL `memory://` mode) while `benchmarks-and-free-threaded.md` and `tech-stack.md` accurately scope `psycopg[binary]==3.2.13` and `asyncpg==0.31.0` to `[dependency-groups].bench`. |

### 2.4 Alternatives Comparison

| Decision | Chosen Seam | Rejected Alternative & Why Rejected |
| --- | --- | --- |
| Documentation & OKF decomposition | `4` tasks (`3 + 3 + 2 + 3 = 11` files) in `ch30` separated from final review gates in `ch31` | Updating only `5` files or cramming `11` files into `2` tasks — either leaves stale `%s` / `5`-extension / `listen_notifications()` contradictions (`DOC-04`) or violates the `<= 3` file task cap. |
| Line-budget enforcement on `workflow.md` & `AGENTS.md` | Explicit `wc -l` verification (`<= 200` on `knowledge/**`, `<= 100` on `AGENTS.md`) inside task gates | Appending multi-paragraph sections blindly — pushes `workflow.md` (`192` lines) over the `200`-line `DOC-08` ceiling. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/11 files documented]`

- `docs/usage/databases.md` (`ch30:t01`) — `vertebra-core[postgres]` installation, `$N` & E-string bind rules, `query_multi`, single-`Sync` `pipeline()`, lossless `Pg*` records, binary `COPY`/`VTBARROW`, `direct_dsn` events, `PgBouncer` modes, DSN defaults, and verification commands.
- `docs/usage/analytics-and-cdc.md` (`ch30:t01`) — Unified `$N` PostgreSQL tenant predicates, `_open_notification_listener` / `_publish_notification` `PostgresNotificationSource`, and `vertebra.db.adapters.postgres.change_stream.PostgresChangeSource` + `PostgresCheckpointStore`.
- `docs/reference/database-capabilities.md` (`ch30:t01`) — mkdocstrings API reference directives for `vertebra.db.adapters.postgres.change_stream.PostgresChangeConfig`, `PostgresCheckpoint`, `PostgresCheckpointStore`, `MemoryPostgresCheckpointStore`, `PostgresChangeSource`, and `PostgresEmbeddingPushdown`.
- `README.md` (`ch30:t02`) — `9` Rust crates (`vertebra-postgres`), `6` PyO3 extensions (`_postgres`), `[postgres]` extra, `src/rs/vertebra-postgres` tree entry, and `postgres-test` / `postgres-wheel-check` / `postgres-bench` / `postgres-bench-check` targets.
- `.agents/bundles/knowledge/workflow.md` (`ch30:t02`) — `6` PyO3 extensions (`_postgres`), `[postgres]` extra, `--group bench` isolation, and `postgres-*` Make targets (`<= 200` lines).
- `AGENTS.md` (`ch30:t02`) — `vertebra-postgres` crate, `6` PyO3 extensions (`_postgres`), and `postgres-*` quick verification commands (`<= 100` lines).
- `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` (`ch30:t03`) — `src/rs/vertebra-postgres` (`vertebra._postgres`), `SyncPostgresClient`/`AsyncPostgresClient`, `SyncPostgresPool`/`AsyncPostgresPool`, Cloud SQL/AlloyDB Admin + mTLS/IAM caches, and lossless `Pg*` / `VTBARROW` contracts (`<= 200` lines).
- `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` (`ch30:t03`) — Unified `ParameterStyle.NUMERIC` (`$N`), single-`Sync` `pipeline()` delayed-commit semantics, dedicated event hooks, and `vertebra.db.adapters.postgres.change_stream` `pgoutput` v1 contracts (`<= 200` lines).
- `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` (`ch30:t04`) — Unified `$N` (`ParameterStyle.NUMERIC`) PostgreSQL sync/async SQLSpec compilation and `+vtb` `numeric_dollar` SQLAlchemy dialects (`<= 200` lines).
- `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md` (`ch30:t04`) — `6` PyO3 extensions (`_postgres`), `[dependency-groups].bench`, `tools/benchmarks/postgres.py`, `postgres-bench*`, and `3.14t`/`3.15t` `vertebra._postgres` disabled-GIL probes (`<= 200` lines).
- `.agents/bundles/product/tech-stack.md` (`ch30:t04`) — `9` Rust workspace crates (`vertebra-postgres`), `5` standalone companion wheels, `6` `mypy.stubtest` targets, `postgres-protocol = "=0.6.12"`, and `[tool.mypy]` benchmark overrides.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Doc / OKF Gate |
| --- | --- | --- | --- |
| R27 (User guides & capability references) | [t01](tasks/t01-user-guides-and-capabilities.md) | `documentation_validation` | `make docs`, `make docs-check` |
| R27 (`README.md`, `workflow.md`, `AGENTS.md`) | [t02](tasks/t02-readme-workflow-and-agents-sync.md) | `documentation_validation` | `make review-check`, `make docs` |
| R27 (Data domain OKF chapters) | [t03](tasks/t03-data-domain-okf-chapters.md) | `documentation_validation` | `make review-check`, `make docs` |
| R27 (Extensions, benchmarks, & tech-stack OKF) | [t04](tasks/t04-extensions-benchmarks-and-tech-stack-okf.md) | `documentation_validation` | `make review-check`, `make docs` |

## 5. Implementation Plan

- [ ] native-postgres-driver:ch30:t01 — Update user database guides, CDC analytics guide, and capability API references (tasks/t01-user-guides-and-capabilities.md)
- [ ] native-postgres-driver:ch30:t02 — Synchronize README, workflow knowledge chapter, and AGENTS instructions for vertebra-postgres (tasks/t02-readme-workflow-and-agents-sync.md)
- [ ] native-postgres-driver:ch30:t03 — Synchronize data domain OKF chapters with native PostgreSQL drivers and change_stream contracts (tasks/t03-data-domain-okf-chapters.md)
- [ ] native-postgres-driver:ch30:t04 — Synchronize extensions, benchmarks, and product tech-stack OKF chapters (tasks/t04-extensions-benchmarks-and-tech-stack-okf.md)

**Chapter Recovery Checkpoint:** `make docs && make docs-check && make review-check` exits `0` with `AGENTS.md <= 100` lines and every `.agents/bundles/knowledge/**/*.md <= 200` lines.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `HEAD @ dd432f18`; `depends_on_chapters: [ch29-wheel-ci-and-benchmarks]`, `state: planned`, `plan_revision: 4`.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03`, `t04` (`4/4` pending); `[0/11 files documented]`.
- **Downstream Dependents**: Unblocks [ch31-final-review-and-release-gate](../ch31-final-review-and-release-gate/spec.md) (`ch31:t01`) for final routed correctness review, fresh quality review, and complete-driver sign-off.

[^doc-databases]: `docs/usage/databases.md` — User-facing Databases and Native Drivers usage guide.
[^doc-cdc]: `docs/usage/analytics-and-cdc.md` — User-facing RLS, CDC, and Arrow Analytics usage guide.
[^doc-capabilities]: `docs/reference/database-capabilities.md` — API reference for database notifications, change streams, and tools.
[^okf-workflow]: `.agents/bundles/knowledge/workflow.md` — Canonical Makefile commands, polyglot rebuild cycle, and Flow workflow chapter.
[^okf-cdc]: `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` — OKF domain chapter for native pipelines, CDC, and tenant predicates.
[^prod-tech-stack]: `.agents/bundles/product/tech-stack.md` — Product technology stack across Python, Rust crates, and companion wheels.
