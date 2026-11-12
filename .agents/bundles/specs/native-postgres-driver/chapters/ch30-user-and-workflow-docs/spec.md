---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch30-user-and-workflow-docs
title: "Chapter 30: User Guides, Reference Stubs, Cloud/Queue Docs, and Workflow Synchronization"
description: Synchronize public user guides, mkdocstrings reference pages, installation tabs, README, workflow OKF chapter, and AGENTS instructions in present tense for vertebra-postgres.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch29-wheel-ci-and-benchmarks
replan_trigger: null
current_task: "native-postgres-driver:ch30:t01"
plan_revision: 8
plan_commit: 5eb75b2a
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-12T04:16:55Z"
generated:
  by: flow-revise
  at: "2026-11-12T01:30:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T01:30:00Z"
    commit: c8722c76
sources:
  - id: doc-databases
    resource: docs/usage/databases.md
    title: User-facing Databases and Native Drivers usage guide
  - id: doc-native-stubs
    resource: docs/reference/native-stubs.md
    title: API reference for compiled PyO3 extension type stubs
  - id: doc-queues
    resource: docs/usage/queues-and-realtime.md
    title: User-facing Distributed Queues and Realtime PubSub guide
  - id: docs-verifier
    resource: tools/docs/verify_docs_artifacts.py::verify_site_artifacts
    title: Zensical built-HTML byte-size, secondary-TOC, and banned-phrase verifier
  - id: okf-workflow
    resource: .agents/bundles/knowledge/workflow.md
    title: Canonical Makefile commands, polyglot rebuild cycle, and Flow workflow chapter
  - id: repo-agents
    resource: AGENTS.md
    title: Root repository agent instructions and quick verification reference
tags:
  - postgres
  - documentation
  - zensical
  - workflow
  - reference
---

# Chapter 30: User Guides, Reference Stubs, Cloud/Queue Docs, and Workflow Synchronization

<!-- truth: start -->
- **Public Documentation & Workflow Coverage (`12` Files)**: Synchronizes database/CDC guides, capability and `.pyi` stub references, installation guides, cloud dialer/packaging/queue guides, `README.md`, `workflow.md`, and `AGENTS.md` to `vertebra-postgres` (`vertebra._postgres`, `vertebra-core[postgres]`) in present tense, satisfying checklist rules `DOC-08` (documented commands match real `Makefile` targets) and `DOC-09` (user-facing docs track public API changes) [^doc-databases] [^doc-native-stubs] [^doc-queues] [^okf-workflow] [^repo-agents].
- **Public Documentation Presentation & Artifact Budgets**: Every installation snippet uses linked `pymdownx.tabbed` blocks (`=== "uv"`, `=== "pip"`, `=== "pdm"`, `=== "poetry"`), Mermaid diagrams stay conceptual (`4` to `6` nodes), `msgspec.Struct` examples omit internal `frozen=True, gc=False` boilerplate and single-use config variables, public `docs/` pages name zero external SQL or PostgreSQL client libraries, and `database-capabilities.md` plus `native-stubs.md` satisfy `verify_docs_artifacts.py` (`H2+H3 <= 14`, reference HTML `<= 120,000` bytes) [^doc-native-stubs] [^docs-verifier].
- **Derived Inventories & Line Budgets**: Module and crate enumerations come from the `Makefile` `lint` stubtest list and `src/rs/*/Cargo.toml` at execution time (sibling driver flows edit the same lists); `.agents/bundles/knowledge/workflow.md` lands `<= 200` lines and `AGENTS.md` `<= 100` lines while registering `_postgres`, `[dependency-groups].postgres-bench`, and the `postgres-*` Make targets [^okf-workflow] [^repo-agents].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

After Chapters `01`–`29` deliver, cut over, live-test, package, and benchmark `vertebra-postgres`, `12` user-facing documentation pages and repository entry points require present-tense synchronization:
- `docs/usage/databases.md`, `docs/usage/analytics-and-cdc.md`, and `docs/reference/database-capabilities.md` lack `vertebra-core[postgres]` tabbed installation, `$n` / `numeric_dollar` bind guidance, delayed-commit single-`Sync` `pipeline()`, lossless `Pg*` value records, raw native `copy_to`/`copy_from`/`copy_from_arrow` vs Python driver `load_from_arrow`/`select_to_arrow`/`stream_arrow_batches`, `vertebra.db.adapters.postgres.change_stream`, and `PostgresEmbeddingPushdown` directives; `analytics-and-cdc.md` still describes PostgreSQL tenant predicates as third-party `%s` / `$N` bind styles [^doc-databases].
- `docs/reference/native-stubs.md` (title and summary table list `_vertebra`, `_bundler`, `_storage`, `_bigquery`, `_oracle`, `_spanner`, `_duckdb` at `c8722c76`), `docs/reference/index.md`, and `docs/getting-started/installation.md` omit `vertebra._postgres` and the `[postgres]` installation extra [^doc-native-stubs].
- `docs/usage/cloud-dialers.md`, `docs/usage/packaging-and-deployment.md`, and `docs/usage/queues-and-realtime.md` omit native `PostgreSQLConfig` Cloud SQL / AlloyDB mTLS+IAM discovery and the `_postgres` companion wheel; `packaging-and-deployment.md` claims release CI applies PGO to published wheels although `.github/workflows/release.yaml` contains no PGO step; `queues-and-realtime.md` lists `psycopg[binary,pool]` in its backend table, prose, and all four install tabs instead of `vertebra-core[postgres]` on `PostgreSQLQueueBackend`'s caller-injected `PostgreSQLQueueExecutor` (`executor=`) contract [^doc-queues].
- `README.md`, `.agents/bundles/knowledge/workflow.md` (`203` lines at `c8722c76`, over the `200`-line ceiling), and `AGENTS.md` (`66` lines) omit `vertebra-postgres` / `_postgres` from their crate and extension enumerations (and carry count phrases such as "all six PyO3 extensions" that no longer match the `Makefile`), and omit `[dependency-groups].postgres-bench` and the `postgres-test`, `postgres-wheel-check`, `postgres-bench`, and `postgres-bench-check` Make targets [^okf-workflow] [^repo-agents].

This chapter updates all `12` files across four `documentation_validation` worksheets (`3` files per task), leaving internal OKF domain/testing/product knowledge chapters to [ch31-data-and-testing-okf-sync](../ch31-data-and-testing-okf-sync/spec.md) and FFI/review-routing knowledge and skills to [ch32-ffi-okf-and-review-routing-sync](../ch32-ffi-okf-and-review-routing-sync/spec.md).

### 1.2 Non-Goals

- Modifying Python or Rust source files or `.pyi` stubs (public docstrings and stub parity are completed and gated in `ch08` and `ch25`–`ch27`).
- Updating `.agents/bundles/knowledge/domains/**`, `testing/**`, `product/**`, `patterns/**`, `gotchas/**`, `architecture/**`, `decisions/**`, or `.agents/skills/**` (owned by `ch31` and `ch32`).
- Documenting a built-in native PostgreSQL queue executor, settings-driven queue backend registry, or `litestar-queues` backend (owned by the `queues-tasks-and-channels` flow).
- Executing final branch-wide correctness and quality review sign-off (owned by [ch33-final-review-and-release-gate](../ch33-final-review-and-release-gate/spec.md)).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md); frozen contracts: [contracts.md](../../contracts.md); depends on [ch29-wheel-ci-and-benchmarks](../ch29-wheel-ci-and-benchmarks/spec.md). `tools/docs/verify_docs_artifacts.py` enforces `max_toc_items = 14` (secondary `H2 + H3` TOC items), `max_reference_bytes = 120_000` on `site/reference/**/index.html`, `max_page_bytes = 180_000` on general pages, zero empty `Attributes` rows, and banned-phrase checks during `make docs` [^docs-verifier]. At `c8722c76`, `docs/reference/native-stubs.md` emits `12` secondary TOC items (`5` `H2` + `7` `heading_level: 3` blocks), so adding `## vertebra._postgres` with `heading_level: 4` class directives holds the page at `13 <= 14`; the executor re-counts at execution time because sibling driver flows may add sections [^doc-native-stubs] [^docs-verifier]. `tools/pgo/rust_pgo.py::discover_extension_manifests` returns `vertebra-py` plus every crate with a crate-local maturin `pyproject.toml`, so `make build-pgo` already profiles `vertebra-postgres`.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R27.1 (`ch30:t01`)**: Update `docs/usage/databases.md`, `docs/usage/analytics-and-cdc.md`, and `docs/reference/database-capabilities.md` with linked `=== "uv"`/`"pip"`/`"pdm"`/`"poetry"` `[postgres]` tabs, conceptual `4`–`6` node Mermaid topologies where diagrams appear, concise `class Model(msgspec.Struct):` examples with inline config dicts, zero external SQL/driver library names (including the `analytics-and-cdc.md` tenant-predicate sentence naming third-party `%s` / `$N` bind styles), and `heading_level: 4` capability directives (`PostgresChangeConfig`, `PostgresCheckpoint`, `PostgresCheckpointStore`, `MemoryPostgresCheckpointStore`, `PostgresChangeSource`, `PostgresCheckpointConflictError`, `PostgresReplicationGapError`, `PostgresEmbeddingPushdown`, `PostgresNotifyConfig`, `PostgresNotificationSource`) passing `verify_docs_artifacts.py` (`H2+H3 <= 14`, HTML `<= 120,000` bytes).
- **R27.2 (`ch30:t02`)**: Add `vertebra._postgres` (`src/rs/vertebra-postgres`, `[postgres]` extra) to the existing module enumerations in `docs/reference/native-stubs.md`, `docs/reference/index.md`, and `docs/getting-started/installation.md`, replacing any stale count word with a plain enumeration matching the `Makefile` `lint` stubtest list, keeping `native-stubs.md` at `H2+H3 <= 14` and `site/reference/native-stubs/index.html <= 120,000` bytes via `heading_level: 4` and `members: false` / concise curated `members:`.
- **R27.3 (`ch30:t03`)**: Update `docs/usage/cloud-dialers.md` (native `PostgreSQLConfig` Cloud SQL / AlloyDB discovery + mTLS 1.3 + IAM `cloud_auth_options` / `iam_auth_options`), `docs/usage/packaging-and-deployment.md` (`_postgres` / `vertebra-core[postgres]` companion wheel; `make build-pgo` contributor tooling profiling every discovered extension including `vertebra-postgres` through its crate-local `pyproject.toml`; published wheels built with workspace `[profile.release]` LTO settings because `release.yaml` runs no PGO step), and `docs/usage/queues-and-realtime.md` (replacing `psycopg[binary,pool]` with `vertebra-core[postgres]` while preserving the caller-injected `PostgreSQLQueueExecutor` `executor=` contract, e.g., wrapping `AsyncPostgreSQLDriver` + `PostgresNotificationSource`, without claiming a built-in default executor).
- **R27.4 (`ch30:t04`)**: Synchronize `README.md`, `.agents/bundles/knowledge/workflow.md` (trimmed `<= 200` lines), and `AGENTS.md` (`<= 100` lines) by adding `vertebra-postgres` / `_postgres` to existing crate and extension enumerations (derived from `src/rs/*/Cargo.toml` and the `Makefile` `lint` stubtest list), `[dependency-groups].postgres-bench`, and `postgres-test`, `postgres-wheel-check`, `postgres-bench`, `postgres-bench-check`, keeping the existing `make review-route` entry (`BASE` defaults to `origin/main`).

### 2.2 Documentation Artifact & Gate Map

- `docs/usage/{databases,analytics-and-cdc,cloud-dialers,packaging-and-deployment,queues-and-realtime}.md` + `docs/getting-started/installation.md` + `docs/reference/{database-capabilities,native-stubs,index}.md` -> `make docs` and `make docs-check` (`scan_public_docstrings`, CLI docs check, Zensical build, `postbuild_docs.py`, `verify_docs_artifacts.py`, and live HTTP `200` probe) [^docs-verifier].
- `README.md` + `.agents/bundles/knowledge/workflow.md` (`<= 200L`) + `AGENTS.md` (`<= 100L`) -> `make review-check` (`bundles`, `privacy`, frontmatter, source existence, link resolution) plus `wc -l` verification [^okf-workflow] [^repo-agents].

### 2.3 Edge-Case & Failure-Mode Matrix

| Boundary / Failure Mode | Deterministic Behavior |
| --- | --- |
| `docs/reference/native-stubs.md` or `database-capabilities.md` exceeding `14` `H2+H3` TOC items or `120,000` rendered HTML bytes | Render `vertebra._postgres` and PostgreSQL capability classes with `heading_level: 4` and `members: false` (or tightly curated key `members:` without repeating `25`-kwarg constructors across pools/clients) so both reference HTML files pass `verify_docs_artifacts.py`. |
| `.agents/bundles/knowledge/workflow.md` (`203L` at `c8722c76`) over the `200`-line OKF ceiling | Consolidate repetitive prose lines while inserting `vertebra-postgres`, `[dependency-groups].postgres-bench`, and `postgres-*` targets so `wc -l` lands `<= 200` (`<= 195` target). |
| Sibling driver flows editing the same module and crate enumerations | Add `_postgres` / `vertebra-postgres` into existing lists and compare the result against the `Makefile` `lint` stubtest list and `src/rs/*/Cargo.toml` at execution time; never write a fixed extension or crate count. |
| Public `docs/` examples leaking internal `msgspec` flags or external driver comparisons | Declare `class Record(msgspec.Struct):` without `frozen=True, gc=False`, pass configuration options inline without single-use variables, and describe Vertebra's native PostgreSQL contract without naming external Python/SQL libraries. |
| Overstating `pgoutput` v1, cloud CI, PGO, or queue scope | Document caller-owned replication slots/publications (tail/resume only, explicit CAS ack before WAL feedback, `MemoryPostgresCheckpointStore` non-durable default), deterministic local cloud wire tests, `make build-pgo` as contributor tooling that discovers `vertebra-postgres`, and a caller-injected queue executor with no built-in default. |

### 2.4 Alternatives Comparison

| Decision | Chosen Seam | Rejected Alternative & Why Rejected |
| --- | --- | --- |
| Chapter partitioning across `33` doc/OKF/skill files | Dedicate `ch30` (`4` tasks, `12` files) to user docs + workflow/entry points, `ch31` (`12` files) to data/testing/product OKF, and `ch32` (`9` files) to FFI/packaging/review skills | Stuffing `33` files into one chapter or omitting `native-stubs.md`, `installation.md`, `cloud-dialers.md`, `packaging-and-deployment.md`, and `queues-and-realtime.md` — either exceeds `<= 3` files per task or leaves user-visible contradictions. |
| `native-stubs.md` `vertebra._postgres` rendering depth | Single `## vertebra._postgres` section with `heading_level: 4` class directives and `members: false` / concise curated `members:` | Default `heading_level: 3` with full expanded member signatures across clients, pools, portals, COPY, Arrow, notify, and replication — breaks both `max_toc_items = 14` (`12` already used at `c8722c76`) and `max_reference_bytes = 120_000`. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/12 files documented]`

- `docs/usage/databases.md` (`ch30:t01`) — Linked `===` `[postgres]` tabs, `$n` passthrough vs `E'...'` pyformat lowering, SQLSpec `NUMERIC` & SQLAlchemy `numeric_dollar`, `query_multi`, single-`Sync` `pipeline()`, lossless `Pg*` values, raw native `copy_to`/`copy_from`/`copy_from_arrow` vs Python driver `load_from_arrow`/`select_to_arrow`/`stream_arrow_batches`/`VTBARROW`, `PgBouncer` modes, `direct_dsn`, and DSN precedence.
- `docs/usage/analytics-and-cdc.md` (`ch30:t01`) — Unified `$n` PostgreSQL tenant predicates (replacing the third-party bind-style sentence), `_open_notification_listener` / `PostgresNotificationSource`, and `vertebra.db.adapters.postgres.change_stream` `pgoutput` v1 CAS checkpointing with conceptual `4`–`6` node Mermaid flow where depicted.
- `docs/reference/database-capabilities.md` (`ch30:t01`) — `heading_level: 4` mkdocstrings directives for `PostgresChangeConfig`, `PostgresCheckpoint`, `PostgresCheckpointStore`, `MemoryPostgresCheckpointStore`, `PostgresChangeSource`, `PostgresCheckpointConflictError`, `PostgresReplicationGapError`, and `PostgresEmbeddingPushdown` alongside `PostgresNotifyConfig` and `PostgresNotificationSource` (`TOC <= 14`, HTML `<= 120,000` bytes).
- `docs/reference/native-stubs.md` (`ch30:t02`) — `vertebra._postgres` (`src/rs/vertebra-postgres`) added to the title, intro, and summary table, rendered with `heading_level: 4` so `TOC <= 14` and HTML `<= 120,000` bytes.
- `docs/reference/index.md` (`ch30:t02`) — `_postgres` added to both native stub enumerations and PostgreSQL CDC/embedding capabilities referenced.
- `docs/getting-started/installation.md` (`ch30:t02`) — `[postgres]` linked `=== "uv"`/`"pip"`/`"pdm"`/`"poetry"` install tabs, `Optional Extras` table row, and `vertebra._postgres` in the Free-Threaded Python `3.14t`/`3.15t` enumeration.
- `docs/usage/cloud-dialers.md` (`ch30:t03`) — Native `PostgreSQLConfig` Cloud SQL and AlloyDB Admin discovery, mTLS 1.3 renewal, and scoped IAM (`cloud_auth_options`, `iam_auth_options`).
- `docs/usage/packaging-and-deployment.md` (`ch30:t03`) — `_postgres` (`vertebra-core[postgres]`) companion wheel, discovery-based `make build-pgo` contributor tooling, and release wheels built with `[profile.release]` LTO settings.
- `docs/usage/queues-and-realtime.md` (`ch30:t03`) — `PostgreSQLQueueBackend` `vertebra-core[postgres]` installation tabs and caller-injected `PostgreSQLQueueExecutor` (`executor=`) contract replacing `psycopg[binary,pool]` references.
- `README.md` (`ch30:t04`) — `vertebra-postgres` (`_postgres`) in the crate layout tree and extension enumerations, `[postgres]` extra, and `postgres-*` Make targets.
- `.agents/bundles/knowledge/workflow.md` (`ch30:t04`) — Trimmed `<= 200`-line workflow reference adding `_postgres` / `vertebra-postgres` to extension and crate enumerations, `[dependency-groups].postgres-bench`, `postgres-test`, `postgres-wheel-check`, `postgres-bench`, and `postgres-bench-check`.
- `AGENTS.md` (`ch30:t04`) — `<= 100`-line root agent instructions adding `_postgres` to the `make dev` enumeration and `postgres-*` quick verification commands.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Doc / OKF Gate |
| --- | --- | --- | --- |
| R27 (Database/CDC usage & capability reference) | [t01](tasks/t01-user-database-cdc-and-capability-docs.md) | `documentation_validation` | `make docs`, `make docs-check`, `make review-check` |
| R1, R26, R27 (Native stubs reference, index & installation) | [t02](tasks/t02-native-stubs-reference-and-install-docs.md) | `documentation_validation` | `make docs`, `make docs-check`, `make review-check` |
| R9, R24, R27 (Cloud dialers, packaging & queue docs) | [t03](tasks/t03-cloud-dialers-packaging-and-queue-docs.md) | `documentation_validation` | `make docs`, `make docs-check`, `make review-check` |
| R1, R27 (`README.md`, `workflow.md`, `AGENTS.md`) | [t04](tasks/t04-readme-workflow-and-agents-sync.md) | `documentation_validation` | `make docs-check`, `make review-check` |

## 5. Implementation Plan

- [ ] native-postgres-driver:ch30:t01 — Update database usage guide, CDC analytics guide, and capability API references (tasks/t01-user-database-cdc-and-capability-docs.md)
- [ ] native-postgres-driver:ch30:t02 — Update native stubs reference, reference index, and installation guide for vertebra._postgres (tasks/t02-native-stubs-reference-and-install-docs.md)
- [ ] native-postgres-driver:ch30:t03 — Synchronize cloud dialers, packaging/deployment, and PostgreSQL queue documentation (tasks/t03-cloud-dialers-packaging-and-queue-docs.md)
- [ ] native-postgres-driver:ch30:t04 — Synchronize README, workflow knowledge chapter, and AGENTS instructions within line budgets (tasks/t04-readme-workflow-and-agents-sync.md)

**Chapter Recovery Checkpoint:** `make docs-check && make review-check` exits `0` with `AGENTS.md <= 100` lines, `.agents/bundles/knowledge/workflow.md <= 200` lines, and `verify_docs_artifacts.py` passing every TOC and HTML byte ceiling.

## 6. Current State & Remaining Plan Snapshot

- **Verified Plan Baseline**: `plan_commit: 5eb75b2a`, `plan_revision: 8`; `depends_on_chapters: [ch29-wheel-ci-and-benchmarks]`, `state: planned`. The flow spans `36` chapters / `129` tasks; `ch01`–`ch21` plus `ch22:t01`–`t02` (`80` tasks) landed on `main` in `c8722c76`, and the remaining chapters ship in a follow-up pull request.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03`, `t04` (`4/4` pending); `[0/12 files documented]`.
- **Downstream Dependents**: Unblocks [ch31-data-and-testing-okf-sync](../ch31-data-and-testing-okf-sync/spec.md) (`ch31:t01`) for OKF domain, testing, and product knowledge synchronization.

[^doc-databases]: `docs/usage/databases.md` — User-facing Databases and Native Drivers usage guide.
[^doc-native-stubs]: `docs/reference/native-stubs.md` — API reference for compiled PyO3 extension type stubs.
[^doc-queues]: `docs/usage/queues-and-realtime.md` — User-facing Distributed Queues and Realtime PubSub guide.
[^docs-verifier]: `tools/docs/verify_docs_artifacts.py::verify_site_artifacts` — Zensical built-HTML byte-size, secondary-TOC, and banned-phrase verifier.
[^okf-workflow]: `.agents/bundles/knowledge/workflow.md` — Canonical Makefile commands, polyglot rebuild cycle, and Flow workflow chapter.
[^repo-agents]: `AGENTS.md` — Root repository agent instructions and quick verification reference.
