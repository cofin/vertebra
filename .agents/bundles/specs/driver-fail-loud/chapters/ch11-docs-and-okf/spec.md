---
type: Spec
parent_flow: driver-fail-loud
flow_id: driver-fail-loud
chapter_id: ch11-docs-and-okf
title: "Chapter 11: Documentation and OKF Sync"
description: Align README, usage and reference docs, and the OKF data-domain, testing, decision, and product chapters with the fail-loud driver baseline delivered by chapters 01 through 09.
status: stable
state: completed
planning_status: complete
depends_on_chapters:
  - ch01-oracle-mssql-fail-loud
  - ch02-duckdb-sqlalchemy-queue-fail-loud
  - ch03-remove-memory-fakes
  - ch04-redis-valkey-memory-removal
  - ch05-mssql-codec-cleanup
  - ch06-oracle-codec-cleanup
  - ch07-vertebra-db-pools-and-mysql
  - ch08-install-hints-and-extras-gate
  - ch09-conformance-outcomes
replan_trigger: null
current_task: null
plan_revision: 2
plan_commit: c2b09b9b
state_revision: 1
last_verified_checkpoint: 30bc516e
last_reviewed_commit: null
created_at: "2026-11-07T00:00:00Z"
updated_at: "2026-11-09T15:11:12Z"
generated:
  by: flow/0.28.0
  at: "2026-11-07T00:00:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-08T00:00:00Z"
    commit: 0acdf576
sources:
  - id: readme
    resource: README.md
    title: Repository README driver list
  - id: docs-databases
    resource: docs/usage/databases.md
    title: Databases and native drivers usage guide
  - id: docs-extensions
    resource: docs/usage/extensions.md
    title: SQLSpec and SQLAlchemy extension guide
  - id: docs-reference-db
    resource: docs/reference/db.md
    title: vertebra.db API reference page
  - id: docs-analytics
    resource: docs/usage/analytics-and-cdc.md
    title: Tenant predicates, CDC, and Arrow analytics guide
  - id: okf-drivers
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: OKF database drivers and dialers chapter
  - id: okf-test-matrix
    resource: .agents/bundles/knowledge/testing/polyglot-test-matrix.md
    title: OKF polyglot test matrix and driver conformance section
  - id: okf-product
    resource: .agents/bundles/product/product.md
    title: Product overview data access bullet and vertebra-db component row
  - id: missing-dep
    resource: src/py/vertebra/_typing.py::MissingDependencyError
    title: Missing dependency error and install hint
  - id: db-lib
    resource: src/rs/vertebra-db/src/lib.rs
    title: vertebra-db public re-exports
  - id: redis-driver
    resource: src/py/vertebra/db/adapters/redis/driver.py::SyncRedisDriver
    title: Redis and Valkey driver memory:// rejection
  - id: oracle-wallet
    resource: src/py/vertebra/db/adapters/oracle/config.py::OracleWallet
    title: Python Autonomous Database wallet parser
tags:
  - docs
  - okf
  - database
  - drivers
  - fail-loud
---

# Chapter 11: Documentation and OKF Sync

<!-- truth: start -->
- **Engine status in user docs**: README and `docs/**` list Oracle and SQL Server as engines whose native driver packages are not yet published; constructing their drivers raises `MissingDependencyError`. User docs name the error and never quote the `uv add vertebra-core[oracle]` / `vertebra-core[mssql]` hint as an installable command.[^readme][^docs-databases][^missing-dep]
- **DuckDB requirement**: every DuckDB mention that describes execution states that the `duckdb` package is required; the databases guide and the OKF drivers chapter state that DuckDB publishes no free-threaded wheel, so `duckdb` is not installed on `3.14t` / `3.15t` and DuckDB drivers raise `MissingDependencyError` there.[^docs-databases][^okf-drivers]
- **memory:// scope**: user docs state that PostgreSQL, MySQL, Redis, and Valkey drivers raise `ImproperConfigurationError` for `memory://` while their configs still construct, that the Redis/Valkey store factories inherit the error, and that `DatabaseRegistry.parse_url("memory://")` raises `ValueError`; queue, object-storage, and `SQLAlchemyCacheBackend.from_url("memory://")` examples stay unchanged.[^docs-databases][^docs-extensions][^redis-driver]
- **vertebra-db role**: user docs describe `vertebra-db` as shared Rust codecs, dialers, and transport; engine clients ship per engine.[^docs-databases][^db-lib]
- **No SQLite stand-in prose**: no doc or OKF chapter describes Oracle, SQL Server, DuckDB, or SQLAlchemy executing against SQLite.[^docs-analytics][^docs-reference-db]
- **Codec inventory matches `lib.rs`**: OKF and product chapters list only the codecs `src/rs/vertebra-db/src/lib.rs` re-exports after chapters 05 through 07, with no `*Pool` type and no `OracleWalletMaterial` struct.[^db-lib][^okf-drivers][^okf-product]
- **OKF provenance**: every edited OKF file keeps 1:1 `sources` to `[^id]:` footnotes, a `<= 40`-line truth block, zero body `---` rules, and a `verified:` list whose first entry is the prepended `{by: agent:flow-executor, at: <UTC timestamp>, commit: <HEAD short sha>}`; earlier entries stay.[^okf-drivers][^okf-test-matrix]
- **No internal names in user docs**: README and `docs/**` contain no flow, chapter, or spec identifiers.
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Chapters 01 through 09 change what the database layer does: Oracle and MSSQL drivers raise `MissingDependencyError`, DuckDB requires `duckdb`, the SQLAlchemy DB-API raises `InterfaceError` on unparsable URLs, PostgreSQL/MySQL/Redis/Valkey reject `memory://`, `vertebra-db` loses its TDS framing (ch05), TNS framing (ch06), and its `*Pool` types, MySQL module, and `OracleWalletMaterial` struct (ch07), install hints name real packages behind an extras unit gate (ch08; Oracle and MSSQL hints name the planned `vertebra-core[oracle]` / `vertebra-core[mssql]` native extras), and conformance (ch09) skips a missing native module, failing it under Docker conformance only for non-pending engines. The README, five `docs/**` pages, and five OKF chapters still describe Oracle Thin TNS/TTC execution, TDS 7.4 framing, an Oracle SQLite stand-in, `redis`/`memory` registry schemes, strict-xfail conformance, `*Pool` exports, and `vertebra-db` as a set of native shared-memory database drivers.

This chapter rewrites those passages in present tense so user docs and agent knowledge describe the code once chapters 01 through 09 are complete.

**Non-goals**: changelog entries (owned by `/flow:finish`); Rust crate `//!` docs and Python docstrings (owned by ch01, ch05, ch06, and ch07); documenting per-engine extras that do not exist yet; agent instructions, workflow, benchmark, tech-stack, SQLAlchemy-dialect, embeddings, and landing-page docs, owned by [ch13-agent-knowledge-and-embeddings-docs](../ch13-agent-knowledge-and-embeddings-docs/spec.md), including the DuckDB dependency-group and `resp3_server` additions to `polyglot-test-matrix.md`; queue backend docs and knowledge (`docs/usage/queues-and-realtime.md`, `docs/reference/queues.md`, `knowledge/domains/framework/distributed-queues-and-email.md`), owned by [ch12-queue-docs-and-knowledge](../ch12-queue-docs-and-knowledge/spec.md); `docs/usage/testing.md`, `docs/examples/background_queues.py`, and object-storage docs, whose `memory://` usage is the queue and object-storage backend and stays valid.

### 1.2 Upstream Execution Baseline

- Branch `feat/storage-updates`, plan anchored at `0acdf576`; parent roadmap [`spec.md`](../../spec.md).
- Depends on all of `ch01-oracle-mssql-fail-loud`, `ch02-duckdb-sqlalchemy-queue-fail-loud`, `ch03-remove-memory-fakes`, `ch04-redis-valkey-memory-removal`, `ch05-mssql-codec-cleanup`, `ch06-oracle-codec-cleanup`, `ch07-vertebra-db-pools-and-mysql`, `ch08-install-hints-and-extras-gate`, and `ch09-conformance-outcomes` being `completed`; `ch10-queue-backend-fail-loud` is not a prerequisite (its docs belong to ch12). Executors read the post-ch09 source files named in each worksheet; they never document planned behavior.

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined | Doc Claim to Correct |
| --- | --- | --- |
| README | `README.md` "Storage, Databases & TypeScript Client" bullet[^readme] | Lists Oracle 26ai and SQL Server as available drivers |
| Usage guide | `docs/usage/databases.md` intro paragraph, mermaid diagram, "Optional Engine Extras", "Native Database Drivers & Adapters Matrix"[^docs-databases] | Intro and mermaid call `vertebra-db` "native Rust shared-memory database drivers"; Oracle "Pure-Rust Thin TNS/TTC wire framing", SQL Server "TDS 7.4/8.0 framing", DuckDB without `duckdb` or free-threaded caveat, no `memory://` scope statement |
| Extensions guide | `docs/usage/extensions.md` "SQLSpec Integration…" and "SQLAlchemy 2.1…" sections[^docs-extensions] | "across 8 database engines", "across all 8 engines" with no Oracle/SQL Server status or `InterfaceError` behavior |
| Reference | `docs/reference/db.md` overview table and tenant paragraph[^docs-reference-db]; `docs/reference/index.md` "Databases" table | "TDS 7.4 `@P1` rewriting", "helper/stand-in acceptance", adapter summary without status |
| Analytics guide | `docs/usage/analytics-and-cdc.md` "Tenant Predicates and Session Settings", "Zero-Copy Arrow SHM…"[^docs-analytics] | "records these values in its SQLite stand-in"; DuckDB without `duckdb` requirement |
| OKF drivers chapter | `database-drivers-and-dialers.md` truth block, registry, adapter table, conformance section[^okf-drivers] | `memory` scheme, `mysql/mod.rs` link, TNS/TDS framing, wallet parsing attributed to `oracle/tns.rs`, `oracledb`/`pymssql`/`aiomysql` transports |
| OKF test matrix | `polyglot-test-matrix.md` "Database driver conformance"[^okf-test-matrix] | Strict `xfail` for missing native modules; Redis/Valkey default to `memory://` |
| Oracle wallet | `src/py/vertebra/db/adapters/oracle/config.py::OracleWallet`[^oracle-wallet] | Wallet zip/path parsing lives in Python; Rust `oracle/tns.rs` keeps `tnsnames.ora` and connect-descriptor parsing only |
| memory:// guards | `src/py/vertebra/db/adapters/redis/driver.py::SyncRedisDriver`[^redis-driver] | Driver-level rejection that store factories inherit |
| OKF tenancy | `query-pipeline-cdc-and-tenancy.md` "Tenant Predicates" section | "in-process SQLite stand-in" |
| Product / ADR | `product.md` Data Access bullet, tenant paragraph, vertebra-db component row[^okf-product]; `adr-002-…md` "Consequences" | Thin TNS/TTC, TDS, `SqlitePool`, `DuckdbBrokerPool`, `OracleWalletMaterial::from_directory`, `COM_STMT_EXECUTE` |
| Error contract | `src/py/vertebra/_typing.py::MissingDependencyError`[^missing-dep] | Hint text quoted in docs |
| Codec inventory | `src/rs/vertebra-db/src/lib.rs`[^db-lib] | Source of truth for kept codecs |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Database drivers and dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md)
- [Query pipeline, CDC, and tenancy](../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md)
- [Polyglot test matrix](../../../../knowledge/testing/polyglot-test-matrix.md)
- [ADR-002](../../../../knowledge/decisions/adr-002-two-tier-shm-database-and-arrow-data-plane.md)
- [Product overview](../../../../product/product.md)
- [Project workflow](../../../../knowledge/workflow.md)
- [Skills index](../../../../../skills/index.md)

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R13.1 (t01)**: `README.md`, `docs/usage/databases.md`, and `docs/usage/extensions.md` mark Oracle and SQL Server as pending native drivers that raise `MissingDependencyError` without quoting the planned-extra install command, state the `duckdb` package requirement and the free-threaded DuckDB caveat, state that PostgreSQL/MySQL/Redis/Valkey drivers and the Redis/Valkey store factories raise `ImproperConfigurationError` for `memory://`, state that unparsable `+vtb` URLs raise `InterfaceError` and `duckdb+vtb://` requires `duckdb`, describe `vertebra-db` as codecs and transport, and drop TNS/TDS framing claims.
- **R13.2 (t02)**: `docs/reference/db.md`, `docs/reference/index.md`, and `docs/usage/analytics-and-cdc.md` drop SQLite stand-in and TDS claims, describe Oracle tenant context as builder-verified only, and state the DuckDB requirement.
- **R13.3 (t03)**: `database-drivers-and-dialers.md` and the `polyglot-test-matrix.md` "Database driver conformance" section describe adapter status (including the free-threaded DuckDB caveat), the post-ch07 codec inventory with wallet parsing attributed to `OracleWallet`, the install-hint contract and extras gate, Python client transports (`asyncmy` for async MySQL), and conformance skip/fail/expected-failure outcomes with matching provenance.
- **R13.4 (t04)**: `query-pipeline-cdc-and-tenancy.md`, `product.md`, and `adr-002-two-tier-shm-database-and-arrow-data-plane.md` drop the SQLite stand-in, TNS/TDS framing, and deleted `vertebra-db` exports with matching provenance.

### 2.2 Documentation Contracts

Status phrasing used across all files (executors adapt sentence structure, not facts):

| Engine | Required Claim | Forbidden Claim |
| --- | --- | --- |
| Oracle | Config classes import and parse `oracle://` URLs; `SyncOracleDriver` / `AsyncOracleDriver` construction raises `MissingDependencyError` because the native Oracle driver package is not yet published; Python `OracleWallet` wallet parsing, Rust `oracle/tns.rs` `tnsnames.ora` / connect-descriptor parsing, 26ai `VECTOR`, OSON, and session-attribute codecs remain | "Thin TNS/TTC wire framing", "O5LOGON", "SQLite stand-in", "emulation", `uv add vertebra-core[oracle]` in user docs |
| SQL Server | Same status as Oracle for `SyncMSSQLDriver` / `AsyncMSSQLDriver`; `@P1` placeholder rewriting remains | "TDS 7.4/8.0", `PRELOGIN`, `LOGIN7`, `SQL_BATCH`, `pymssql`, `uv add vertebra-core[mssql]` in user docs |
| DuckDB | Requires the `duckdb` package; drivers (and `duckdb+vtb://` connections) raise `MissingDependencyError` without it; DuckDB publishes no free-threaded wheel, so `duckdb` is not installed on `3.14t` / `3.15t` and DuckDB drivers raise `MissingDependencyError` there | Any implication of a fallback engine |
| PostgreSQL / MySQL | Configs construct; drivers raise `ImproperConfigurationError` for a `memory://` DSN | `memory://` as an engine URL |
| Redis / Valkey | Configs construct; drivers raise `ImproperConfigurationError` for a `memory://` URL, and so do `KeyValueStore`, Litestar `MemoryStore`, Starlette `CacheStore`, and `SQLAlchemyCacheBackend` `from_valkey` / `from_redis`; `DatabaseRegistry.parse_url("memory://")` raises `ValueError`; RESP3 codecs remain | `redis`/`rediss`/`memory` scheme triple |
| Queue / object storage / Alchemy L1 cache | `QueueConfig(backend_url="memory://")`, `SyncObjectStore("memory://")`, and `SQLAlchemyCacheBackend.from_url("memory://")` keep in-process backends | Any statement that `memory://` is removed globally |
| `vertebra-db` | Shared Rust wire codecs, Cloud SQL / AlloyDB dialers, transport, and shared-memory Arrow framing; engine clients ship per engine | "native Rust shared-memory database drivers" |

Install-hint contract quoted in OKF only: `MissingDependencyError(package, *, install_package=None, extra=None)` defaults `install_package` to `vertebra-core[<extra>]` when `extra` is set, else `package`; the packaging unit test fails when a `src/py/vertebra` `extra=` literal names an undeclared extra. Oracle and MSSQL guards pass `install_package="vertebra-core[oracle]"` / `"vertebra-core[mssql]"`, so their hint reads `Install it with: uv add vertebra-core[oracle]` / `[mssql]` and names planned extras that are not yet published. OKF quotes that hint and states the extras are planned. User docs name `MissingDependencyError` and state that the native Oracle and SQL Server packages are not yet published; they never quote the hint as an installable command.[^missing-dep]

### 2.3 Caller -> Seam -> Collaborator Map

- Reader of `docs/usage/databases.md` → Oracle matrix row → `SyncOracleDriver.__init__` → `ensure_module("vertebra._oracle", …)` → `MissingDependencyError`.
- Reader of the `memory://` paragraph → `KeyValueStore.from_valkey("memory://")` → `ValkeyConfig(url=...)` → `AsyncRedisDriver.__init__` → `_reject_memory_url` → `ImproperConfigurationError`.
- Agent loading `database-drivers-and-dialers.md` → adapter table / truth block → `src/rs/vertebra-db/src/lib.rs` `pub use` lists → kept codecs.
- `make docs` → `zensical build` + `tools.docs.postbuild` + `tools.docs.verify_docs_artifacts` → broken anchors and leaked text fail the build.
- `make review-check` → `tools.scripts.review_setup validate` → `bundles` and `privacy` rules over `.agents/bundles/**`.

### 2.4 Edge-Case & Failure-Mode Matrix

| Scenario | Trigger | Expected Behavior | Invariant |
| --- | --- | --- | --- |
| Deleted Rust module still linked | OKF adapter table links `src/rs/vertebra-db/src/mysql/mod.rs` after ch07 | Relative-link check fails; link removed | Every relative link resolves |
| Source id dropped from body | Footnote removed with stale prose | Pairing check reports `sources_without_footnote`; delete the `sources` entry too | 1:1 sources/footnotes |
| Upstream behavior differs from plan | Post-ch09 code raises a different error or skips differently | Document the code as found; record a `Notes & Discoveries` line and report to the coordinator | Docs describe code at `HEAD` |
| Queue / object-storage `memory://` | Global search for `memory://` | Matches in queue, testing, storage, and object-store examples stay | Valid in-process backends kept |
| Internal name leak | Worksheet wording copied into docs | Leak grep `native-[a-z-]+-drivers?\|pending_flow_id\|driver work\|driver-fail-loud\|ch[0-9]{2}` returns matches; rewrite | No flow/spec ids in user docs |
| Planned extra quoted as installable | `uv add vertebra-core[oracle]` copied from the hint into user docs | t01 Final grep fails; replace with "not yet published" | User docs never advertise an uninstallable command |
| Changelog mentions | `docs/changelog/index.md` historical entries | Untouched | History belongs in the changelog |

### 2.5 Acceptance Scenarios

- Given the rebuilt docs, when a reader opens the databases guide matrix, then the Oracle and SQL Server rows state `MissingDependencyError` without an install command and the DuckDB row states the `duckdb` requirement and the free-threaded caveat.
- Given `database-drivers-and-dialers.md`, when an agent searches for `DuckdbBrokerPool`, `SqlitePool`, `TDS 7.4`, or `memory` in the scheme list, then it finds no match.
- Given `polyglot-test-matrix.md`, when an agent reads the conformance section, then it states that a missing native module skips the case, that `VERTEBRA_CONFORMANCE_DOCKER=1` turns the skip into a failure only for engines without `pending_flow_id` (`bigquery`, `spanner`), that strict `xfail` applies only when a pending engine's native module imports, and that Redis/Valkey cases need `VERTEBRA_CONFORMANCE_<ENGINE>_DSN` or `VERTEBRA_CONFORMANCE_DOCKER=1` and skip otherwise.

### 2.6 Alternatives Considered

| Alternative | Outcome |
| --- | --- |
| Remove Oracle and SQL Server rows from user docs entirely | Rejected: configs, SQLSpec configs, Django backends, and SQLAlchemy dialects still exist and raise; readers need the status |
| Two tasks (user docs, OKF) as the root plan lists | Rejected: 11 files exceed the 3-file task bound; four tasks keep each executor's read set at 6 |
| Document future `vertebra-core[oracle]` / `[mssql]` extras as installable | Rejected: the extras are not declared in `pyproject.toml`; user docs state only that the native packages are not yet published, and OKF quotes the hint as naming planned extras |
| Leave ADR-002 untouched as a historical record | Rejected: its Consequences section asserts current crate contents (Thin TNS/TTC) that ch06 deletes |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[11/11 files documented]`
- `[x]` `README.md` — t01
- `[x]` `docs/usage/databases.md` — t01
- `[x]` `docs/usage/extensions.md` — t01
- `[x]` `docs/reference/db.md` — t02
- `[x]` `docs/reference/index.md` — t02
- `[x]` `docs/usage/analytics-and-cdc.md` — t02
- `[x]` `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — t03
- `[x]` `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` — t03
- `[x]` `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` — t04
- `[x]` `.agents/bundles/product/product.md` — t04
- `[x]` `.agents/bundles/knowledge/decisions/adr-002-two-tier-shm-database-and-arrow-data-plane.md` — t04

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Doc Gate |
| --- | --- | --- | --- |
| R13.1 | [t01](tasks/t01-user-guides.md) | `documentation_validation` | stale-claim grep, relative-link check, `make docs` |
| R13.2 | [t02](tasks/t02-reference-and-analytics-docs.md) | `documentation_validation` | stale-claim grep, relative-link check, `make docs-check` |
| R13.3 | [t03](tasks/t03-driver-and-conformance-knowledge.md) | `documentation_validation` | OKF pairing check, relative-link check, `make review-check` |
| R13.4 | [t04](tasks/t04-tenancy-product-and-adr-knowledge.md) | `documentation_validation` | OKF pairing check, relative-link check, `make review-check` |

## 5. Implementation Plan

- [x] [30bc516e] driver-fail-loud:ch11:t01 — README, databases guide, and extensions guide state engine status, DuckDB requirement, and memory:// scope (tasks/t01-user-guides.md)
- [x] [30bc516e] driver-fail-loud:ch11:t02 — Reference pages and analytics guide drop stand-in and TDS claims (tasks/t02-reference-and-analytics-docs.md)
- [x] [30bc516e] driver-fail-loud:ch11:t03 — OKF drivers chapter and test matrix describe status, codecs, install hints, and conformance outcomes (tasks/t03-driver-and-conformance-knowledge.md)
- [x] [30bc516e] driver-fail-loud:ch11:t04 — OKF tenancy, product, and ADR-002 chapters drop stand-in and deleted exports (tasks/t04-tenancy-product-and-adr-knowledge.md)
- **Chapter / Phase Recovery Checkpoint**: `make review-check && make docs-check && uv run --no-sync pytest src/py/tests/unit/tools/test_docs_tooling.py -q`

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline (`HEAD`)**: `f2b328dc` — chapter completed and verified (`4/4` tasks completed).
- **Remaining Tasks**: None (`4/4` completed).
- **Documentation & OKF Sync**: `[11/11 files documented]`.
- **Active Invariants**: queue, object-storage, and Alchemy L1 cache `memory://` docs unchanged; changelog untouched; no internal identifiers in user docs; no planned-extra install command in user docs.

[^readme]: `README.md` — Repository README driver list.
[^docs-databases]: `docs/usage/databases.md` — Databases and native drivers usage guide.
[^docs-extensions]: `docs/usage/extensions.md` — SQLSpec and SQLAlchemy extension guide.
[^docs-reference-db]: `docs/reference/db.md` — vertebra.db API reference page.
[^docs-analytics]: `docs/usage/analytics-and-cdc.md` — Tenant predicates, CDC, and Arrow analytics guide.
[^okf-drivers]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — OKF database drivers and dialers chapter.
[^okf-test-matrix]: `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` — OKF polyglot test matrix and driver conformance section.
[^okf-product]: `.agents/bundles/product/product.md` — Product overview data access bullet and vertebra-db component row.
[^missing-dep]: `src/py/vertebra/_typing.py::MissingDependencyError` — Missing dependency error and install hint.
[^db-lib]: `src/rs/vertebra-db/src/lib.rs` — vertebra-db public re-exports.
[^redis-driver]: `src/py/vertebra/db/adapters/redis/driver.py::SyncRedisDriver` — Redis and Valkey driver memory:// rejection.
[^oracle-wallet]: `src/py/vertebra/db/adapters/oracle/config.py::OracleWallet` — Python Autonomous Database wallet parser.
