---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch31-data-and-testing-okf-sync
title: "Chapter 31: Data Domain Split, Limitations Gotchas, Testing Matrix, and Product OKF Synchronization"
description: Split the PostgreSQL native driver OKF chapter out of the cross-engine drivers chapter, extend the PostgreSQL limitations Gotcha, register knowledge indexes, and synchronize CDC, extensions, benchmarks, polyglot test matrix, product, and queue OKF chapters within <= 200-line budgets.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch30-user-and-workflow-docs
replan_trigger: null
current_task: "native-postgres-driver:ch31:t01"
plan_revision: 8
plan_commit: 5eb75b2a
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-10T14:45:00Z"
updated_at: "2026-11-12T04:16:55Z"
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
    title: Cross-engine database drivers and cloud dialers domain chapter
  - id: okf-storage-limits
    resource: .agents/bundles/knowledge/domains/storage/object-storage-limitations.md
    title: Canonical Gotcha limitations chapter model in OKF knowledge bundle
  - id: okf-cdc
    resource: .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
    title: Query pipeline, CDC change streams, and RLS tenancy domain chapter
  - id: okf-test-matrix
    resource: .agents/bundles/knowledge/testing/polyglot-test-matrix.md
    title: Polyglot Rust, Python, TypeScript, and database conformance test matrix
  - id: prod-tech-stack
    resource: .agents/bundles/product/tech-stack.md
    title: Product technology stack across Python, Rust crates, and companion wheels
  - id: okf-queues
    resource: .agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md
    title: Distributed task queues and PostgreSQL queue backend domain chapter
tags:
  - postgres
  - okf
  - knowledge
  - limitations
  - testing
---

# Chapter 31: Data Domain Split, Limitations Gotchas, Testing Matrix, and Product OKF Synchronization

<!-- truth: start -->
- **Dedicated PostgreSQL Driver Chapter & Limitations Update (`t01`–`t02`)**: Splits `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` (`257L` at `c8722c76` -> `<= 200L`) into a concise cross-engine overview paired with new `postgres-native-driver.md` (`Concept`, `<= 200L`), extends `postgres-limitations.md` (`Gotcha`, created and indexed by `native-postgres-driver:ch22:t04`, split-state caveats removed by `ch27:t04`) with the runtime caveats discovered in `ch20`–`ch29`, rewrites `query-pipeline-cdc-and-tenancy.md` (`214L` -> `<= 200L`), and registers `postgres-native-driver.md` in frontmatter-free `knowledge/index.md` and `knowledge/domains/index.md` [^okf-drivers] [^okf-storage-limits] [^okf-cdc].
- **Extensions, Testing Matrix & Product Alignment (`t03`–`t04`)**: Synchronizes `sqlspec-alchemy-and-dialects.md`, `benchmarks-and-free-threaded.md`, `polyglot-test-matrix.md` (`243L` -> `<= 200L`), `product/tech-stack.md`, `product/product.md`, and `distributed-queues-and-email.md` by adding `vertebra-postgres` / `_postgres` to existing crate, extension, and companion-wheel enumerations (derived from `src/rs/*/Cargo.toml`, the `Makefile` `lint` stubtest list, and `src/rs/*/pyproject.toml` at execution time), `[dependency-groups].postgres-bench`, zero-skip `make postgres-test`, and `PostgreSQLQueueBackend`'s `vertebra-core[postgres]` caller-injected `PostgreSQLQueueExecutor` contract [^okf-test-matrix] [^prod-tech-stack] [^okf-queues].
- **OKF v0.2 Hygiene**: Every non-index bundle file sits `<= 200` lines with `<= 40`-line truth blocks, multi-line YAML frontmatter, zero standalone `---` body rules, zero frontmatter on `index.md` files, 1:1 `sources:`-to-footnote parity, and current-state-only prose (checklist rules `DOC-01`, `DOC-03`, `DOC-05`).
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Across `.agents/bundles/knowledge/` and `.agents/bundles/product/`, existing chapters hold pre-cutover claims or exceed the `200`-line OKF budget at `c8722c76`: `database-drivers-and-dialers.md` is `257` lines and describes PostgreSQL as a Python adapter over `psycopg`/`psycopg_pool`; `query-pipeline-cdc-and-tenancy.md` is `214` lines and says sync PostgreSQL compiles to psycopg `%s` while async compiles to asyncpg `$N`; `polyglot-test-matrix.md` is `243` lines and documents an optional psycopg converter acceptance suite; `distributed-queues-and-email.md` describes the injected `PostgreSQLQueueExecutor` without the native dependency guard; `product/tech-stack.md` (`196` lines) and `product/product.md` (`165` lines) omit `vertebra-postgres` [^okf-drivers] [^okf-cdc] [^okf-test-matrix] [^okf-queues]. `vertebra-postgres` needs a dedicated architectural domain chapter (`postgres-native-driver.md`), and the PostgreSQL limitations Gotcha (`postgres-limitations.md`, modeled on `object-storage-limitations.md`) needs the runtime caveats confirmed during `ch20`–`ch29` [^okf-storage-limits].

This chapter updates or creates `12` OKF files across four `documentation_validation` tasks (`3` files per task):
1. Refactor `database-drivers-and-dialers.md` (`<= 200L`), create `postgres-native-driver.md` (`Concept`, `<= 200L`), and update `postgres-limitations.md` (`Gotcha`, `<= 200L`) with protocol/pooler/CDC boundaries and runtime caveats surfaced during `ch20`–`ch29` (`ch31:t01`) [^okf-drivers] [^okf-storage-limits].
2. Rewrite `query-pipeline-cdc-and-tenancy.md` (`<= 200L`) in present tense and register `postgres-native-driver.md` in frontmatter-free `knowledge/index.md` and `knowledge/domains/index.md`, keeping the existing `postgres-limitations.md` entries (`ch31:t02`) [^okf-cdc].
3. Synchronize `sqlspec-alchemy-and-dialects.md` (`<= 200L`), `benchmarks-and-free-threaded.md` (`<= 200L`), and trimmed `polyglot-test-matrix.md` (`243L -> <= 200L`) (`ch31:t03`) [^okf-test-matrix].
4. Synchronize `product/tech-stack.md` (`<= 200L`), `product/product.md` (`<= 200L`), and `distributed-queues-and-email.md` (`<= 200L`) (`ch31:t04`) [^prod-tech-stack] [^okf-queues].

### 1.2 Non-Goals

- Editing public `docs/`, `README.md`, `workflow.md`, or `AGENTS.md` (completed in [ch30-user-and-workflow-docs](../ch30-user-and-workflow-docs/spec.md)).
- Editing PyO3 FFI patterns/gotchas/worker architecture, `packaging-and-pgo.md`, `ADR-004`, or `.agents/skills/**` (owned by [ch32-ffi-okf-and-review-routing-sync](../ch32-ffi-okf-and-review-routing-sync/spec.md)).
- Creating `postgres-limitations.md` or recording the pre-cutover split state (owned by `native-postgres-driver:ch22:t04`; removed by `ch27:t04`).
- Documenting a built-in native PostgreSQL queue executor, settings-driven backend registry, or `litestar-queues` backend (owned by the `queues-tasks-and-channels` flow).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md); frozen contracts: [contracts.md](../../contracts.md); depends on [ch30-user-and-workflow-docs](../ch30-user-and-workflow-docs/spec.md). `tools/scripts/review_setup.py` rejects YAML frontmatter on `index.md` files, rejects standalone `---` rules in bundle bodies, enforces `<= 40`-line truth blocks, checks every `sources:` resource path and relative `.md` link under `knowledge/` and `product/` on disk, and verifies `verified[].commit` ancestry against `HEAD`. `tools/pgo/rust_pgo.py::discover_extension_manifests` already profiles `vertebra-postgres`, so PGO coverage is not a PostgreSQL limitation.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R27.5 (`ch31:t01`)**: Refactor `database-drivers-and-dialers.md` (`<= 200L`) into a cross-engine overview, author `postgres-native-driver.md` (`type: Concept`, `<= 200L`) covering `vertebra-postgres` Protocol 3.0 sessions, pools, statement caches, SCRAM-SHA-256-PLUS, Cloud SQL/AlloyDB single-flight mTLS+IAM, single-`Sync` `pipeline()`, binary `COPY`, `VTBARROW`, `LISTEN`/`NOTIFY`, and `pgoutput` v1, and extend `postgres-limitations.md` (`type: Gotcha`, `<= 200L`) with every scope boundary, operational edge case, and runtime caveat confirmed across `ch20`–`ch29` that it does not already list.
- **R27.6 (`ch31:t02`)**: Rewrite `query-pipeline-cdc-and-tenancy.md` (`<= 200L`) for unified `$n` `ParameterStyle.NUMERIC`, delayed-commit `pipeline()`, `EventSessionRegistry`, and `vertebra.db.adapters.postgres.change_stream` CAS checkpointing, and register `postgres-native-driver.md` in frontmatter-free `knowledge/index.md` and `knowledge/domains/index.md` beside the existing `postgres-limitations.md` entries.
- **R27.7 (`ch31:t03`)**: Synchronize `sqlspec-alchemy-and-dialects.md` (`ParameterStyle.NUMERIC` + `default_paramstyle = paramstyle = "numeric_dollar"`, `<= 200L`), `benchmarks-and-free-threaded.md` (`_postgres` added to the `gil_used = false` extension enumeration, `[dependency-groups].postgres-bench` `python_version == '3.12'` markers, `postgres-bench*`, `FREE_THREADED is _is_free_threaded_runtime()`, `<= 200L`), and `polyglot-test-matrix.md` (`243L -> <= 200L`, `vertebra-postgres` / `_postgres` added to crate and extension enumerations, `VERTEBRA_CONFORMANCE_DOCKER=1` + `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1` `make postgres-test`).
- **R27.8 (`ch31:t04`)**: Synchronize `product/tech-stack.md` (`vertebra-postgres` added to the Rust workspace inventory and optional companion wheel list, `[dependency-groups].postgres-bench`, `<= 200L`), `product/product.md` (native PostgreSQL capabilities, `PostgreSQLQueueBackend` `vertebra-core[postgres]` caller-injected `PostgreSQLQueueExecutor` baseline, and `_postgres` in the free-threaded extension enumeration, `<= 200L`), and `distributed-queues-and-email.md` (`vertebra-core[postgres]` dependency guard and caller-injected `PostgreSQLQueueExecutor` `executor=` contract, `<= 200L`).

### 2.2 Bundle Validation Map

- `10` OKF `Concept` / `Gotcha` / `Guide` files -> `make review-check` + `wc -l <= 200` + `<= 40`-line truth blocks + 1:1 `sources:` inline and footer footnotes + zero standalone `---`.
- `2` reserved catalog files (`knowledge/index.md`, `knowledge/domains/index.md`) -> zero YAML frontmatter, resolving links to `postgres-native-driver.md` and `postgres-limitations.md`, validated by `make review-check`.

### 2.3 Edge-Case & Failure-Mode Matrix

| Boundary / Failure Mode | Deterministic Behavior |
| --- | --- |
| `database-drivers-and-dialers.md` (`257L`), `query-pipeline-cdc-and-tenancy.md` (`214L`), and `polyglot-test-matrix.md` (`243L`) exceeding `200` lines | Move deep PostgreSQL protocol/codec/replication detail into `postgres-native-driver.md` and `postgres-limitations.md`, and trim obsolete multi-paragraph boilerplate so every chapter lands `<= 200` lines. |
| Adding YAML frontmatter to `knowledge/index.md` or `knowledge/domains/index.md` | Rejected by `review_setup.py` (`okf_reserved = ["index.md", "log.md"]`); both directory indexes stay frontmatter-free. |
| `postgres-limitations.md` missing when `ch31:t01` starts | `ch22:t04` creates it; if absent, stop and route through `/flow:revise` rather than creating a second limitations chapter. |
| Omitting implementation caveats encountered during `ch20`–`ch29` | `ch31:t01` audits `ch20`–`ch29` task notes and code to record every confirmed limitation, platform boundary, and protocol edge case in `postgres-limitations.md`. |
| Sibling driver flows editing the same crate, extension, and wheel enumerations | Add `vertebra-postgres` / `_postgres` into existing lists and compare against `src/rs/*/Cargo.toml`, the `Makefile` `lint` stubtest list, and `src/rs/*/pyproject.toml` at execution time; never write a fixed count. |

### 2.4 Alternatives Comparison

| Decision | Chosen Seam | Rejected Alternative & Why Rejected |
| --- | --- | --- |
| Data-domain PostgreSQL layout | Cross-engine `database-drivers-and-dialers.md` + dedicated `postgres-native-driver.md` (`Concept`) + the existing `postgres-limitations.md` (`Gotcha`) | Appending PostgreSQL architecture and caveats into `database-drivers-and-dialers.md` (`257L`) — keeps the file far past `200` lines and buries operational limitations instead of surfacing an indexed `Gotcha` chapter like `object-storage-limitations.md`. |
| Benchmark dependency group documentation | Document `[dependency-groups].postgres-bench` (`python_version == '3.12'` markers) separately from `[dependency-groups].bench` | Claiming PostgreSQL reference drivers live in `[dependency-groups].bench` — contradicts `pyproject.toml` and breaks `python3.14t`/`3.15` wheel build assumptions on third-party comparator extensions. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/12 files documented]`

- `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` (`ch31:t01`) — Refactored `<= 200L` cross-engine overview linking to `postgres-native-driver.md` and `postgres-limitations.md`.
- `.agents/bundles/knowledge/domains/data/postgres-native-driver.md` (`ch31:t01`) — New `<= 200L` OKF `Concept` covering `vertebra-postgres` Protocol 3.0 sessions, pools, statement caches, SCRAM-SHA-256-PLUS, Cloud SQL/AlloyDB mTLS+IAM, single-`Sync` `pipeline()`, binary `COPY`, `VTBARROW`, `LISTEN`/`NOTIFY`, and `pgoutput` v1.
- `.agents/bundles/knowledge/domains/data/postgres-limitations.md` (`ch31:t01`) — Updated `<= 200L` OKF `Gotcha` adding single-host DSNs, tail-only `pgoutput` v1, non-durable `MemoryPostgresCheckpointStore`, `%s` backslash refusal, single-statement `execute` vs `query_multi`, cold pipeline text fallback, `PgBouncer` `statement` & `direct_dsn` limits, Ed25519 channel binding, local wire-mock cloud CI boundary, and `ch20`–`ch29` runtime caveats where not already listed.
- `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` (`ch31:t02`) — Present-tense `<= 200L` unified `$n` `ParameterStyle.NUMERIC`, delayed-commit `pipeline()`, and `vertebra.db.adapters.postgres.change_stream` CAS checkpointing.
- `.agents/bundles/knowledge/index.md` (`ch31:t02`) — Frontmatter-free root knowledge catalog registering `postgres-native-driver.md` beside the existing `postgres-limitations.md` entry.
- `.agents/bundles/knowledge/domains/index.md` (`ch31:t02`) — Frontmatter-free domain catalog registering `postgres-native-driver.md` beside the existing `postgres-limitations.md` entry.
- `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` (`ch31:t03`) — `<= 200L` unified `ParameterStyle.NUMERIC` (`$n`) and `default_paramstyle = paramstyle = "numeric_dollar"` `+vtb` dialect contracts.
- `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md` (`ch31:t03`) — `<= 200L` `_postgres` in the `gil_used = false` enumeration, `[dependency-groups].postgres-bench` `python_version == '3.12'`, `postgres-bench*`, and `FREE_THREADED is _is_free_threaded_runtime()`.
- `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` (`ch31:t03`) — Trimmed `<= 200L` matrix adding `vertebra-postgres` / `_postgres` and zero-skip `make postgres-test` (`VERTEBRA_CONFORMANCE_DOCKER=1` + `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1`).
- `.agents/bundles/product/tech-stack.md` (`ch31:t04`) — `<= 200L` Rust workspace inventory and optional companion wheel list including `vertebra-postgres`.
- `.agents/bundles/product/product.md` (`ch31:t04`) — `<= 200L` native PostgreSQL driver, CDC, caller-injected `PostgreSQLQueueExecutor` (`vertebra-core[postgres]`), and `_postgres` free-threaded product capabilities.
- `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md` (`ch31:t04`) — `<= 200L` `PostgreSQLQueueBackend` `vertebra-core[postgres]` dependency guard and caller-injected `PostgreSQLQueueExecutor` (`executor=`) contract.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical OKF Gate |
| --- | --- | --- | --- |
| R2, R5, R9, R15, R17, R19, R20, R21, R22, R27 (PostgreSQL driver split & limitations update) | [t01](tasks/t01-split-postgres-driver-and-limitations-okf.md) | `documentation_validation` | `make review-check`, `wc -l <= 200` |
| R17, R21, R23, R24, R27 (CDC, tenancy, & knowledge indexes) | [t02](tasks/t02-cdc-tenancy-and-knowledge-indexes-okf.md) | `documentation_validation` | `make review-check`, `wc -l <= 200` |
| R24, R25, R26, R27 (SQLSpec/SQLAlchemy, benchmarks, & test matrix) | [t03](tasks/t03-extensions-benchmarks-and-test-matrix-okf.md) | `documentation_validation` | `make review-check`, `wc -l <= 200` |
| R1, R24, R27 (Product tech-stack, product vision, & queues) | [t04](tasks/t04-product-tech-stack-and-queues-okf.md) | `documentation_validation` | `make review-check`, `wc -l <= 200` |

## 5. Implementation Plan

- [ ] native-postgres-driver:ch31:t01 — Split database-drivers-and-dialers, create postgres-native-driver, and extend postgres-limitations OKF chapters (tasks/t01-split-postgres-driver-and-limitations-okf.md)
- [ ] native-postgres-driver:ch31:t02 — Synchronize query-pipeline-cdc-and-tenancy and register postgres-native-driver in knowledge indexes (tasks/t02-cdc-tenancy-and-knowledge-indexes-okf.md)
- [ ] native-postgres-driver:ch31:t03 — Synchronize SQLSpec/SQLAlchemy dialects, benchmarks/free-threaded, and polyglot test matrix OKF chapters (tasks/t03-extensions-benchmarks-and-test-matrix-okf.md)
- [ ] native-postgres-driver:ch31:t04 — Synchronize product tech-stack, product definition, and distributed queues OKF chapters (tasks/t04-product-tech-stack-and-queues-okf.md)

**Chapter Recovery Checkpoint:** `make review-check` exits `0` with every non-index OKF chapter updated in `ch31` `<= 200` lines and `knowledge/index.md` plus `knowledge/domains/index.md` free of YAML frontmatter.

## 6. Current State & Remaining Plan Snapshot

- **Verified Plan Baseline**: `plan_commit: 5eb75b2a`, `plan_revision: 8`; `depends_on_chapters: [ch30-user-and-workflow-docs]`, `state: planned`. The flow spans `36` chapters / `129` tasks; `ch01`–`ch21` plus `ch22:t01`–`t02` (`80` tasks) landed on `main` in `c8722c76`, and the remaining chapters ship in a follow-up pull request.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03`, `t04` (`4/4` pending); `[0/12 files documented]`.
- **Downstream Dependents**: Unblocks [ch32-ffi-okf-and-review-routing-sync](../ch32-ffi-okf-and-review-routing-sync/spec.md) (`ch32:t01`), where `areas.toml` binds `[areas.runtime-ipc].knowledge` and `[areas.pyo3-bridge].knowledge` to `postgres-native-driver.md` and `postgres-limitations.md`.

[^okf-drivers]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Cross-engine database drivers and cloud dialers domain chapter.
[^okf-storage-limits]: `.agents/bundles/knowledge/domains/storage/object-storage-limitations.md` — Canonical `Gotcha` limitations chapter model in OKF knowledge bundle.
[^okf-cdc]: `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` — Query pipeline, CDC change streams, and RLS tenancy domain chapter.
[^okf-test-matrix]: `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` — Polyglot Rust, Python, TypeScript, and database conformance test matrix.
[^prod-tech-stack]: `.agents/bundles/product/tech-stack.md` — Product technology stack across Python, Rust crates, and companion wheels.
[^okf-queues]: `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md` — Distributed task queues and PostgreSQL queue backend domain chapter.
