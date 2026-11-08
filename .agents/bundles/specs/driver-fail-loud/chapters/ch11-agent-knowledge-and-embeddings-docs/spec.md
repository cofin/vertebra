---
type: Spec
parent_flow: driver-fail-loud
flow_id: driver-fail-loud
chapter_id: ch11-agent-knowledge-and-embeddings-docs
title: "Chapter 11: Agent Knowledge, Dependency Groups, and Embeddings Docs"
description: Align the agent instructions, workflow, benchmark, tech-stack, test-matrix, SQLAlchemy-dialect, domain-index, embeddings, and landing-page docs with the DuckDB dependency group, the vertebra-db dependency cleanup, the DB-API InterfaceError, the resp3_server fixture, and the pending native Oracle driver.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch09-docs-and-okf
  - ch10-queue-docs-and-knowledge
replan_trigger: null
current_task: "driver-fail-loud:ch11:t01"
plan_revision: 1
plan_commit: 0acdf576
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-08T00:00:00Z"
updated_at: "2026-11-08T00:00:00Z"
generated:
  by: flow/0.28.0
  at: "2026-11-08T00:00:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-08T00:00:00Z"
    commit: 0acdf576
sources:
  - id: agents-md
    resource: AGENTS.md
    title: Agent instructions Quick Verification Commands block
  - id: okf-workflow
    resource: .agents/bundles/knowledge/workflow.md
    title: OKF project workflow commands and hygiene rules
  - id: okf-benchmarks
    resource: .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
    title: OKF benchmark suite and free-threaded test gate chapter
  - id: okf-tech-stack
    resource: .agents/bundles/product/tech-stack.md
    title: Product tech stack crate dependencies and dependency groups
  - id: okf-test-matrix
    resource: .agents/bundles/knowledge/testing/polyglot-test-matrix.md
    title: OKF polyglot test matrix
  - id: okf-dialects
    resource: .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
    title: OKF SQLSpec, Advanced Alchemy, and SQLAlchemy dialect chapter
  - id: okf-domains-index
    resource: .agents/bundles/knowledge/domains/index.md
    title: OKF domain vocabulary index
  - id: okf-embeddings
    resource: .agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md
    title: OKF vector slabs, embeddings, and SQL pushdown chapter
  - id: docs-embeddings
    resource: docs/usage/embeddings-and-vectors.md
    title: Vector embeddings and SQL pushdown usage guide
  - id: docs-usage-index
    resource: docs/usage/index.md
    title: Usage guide landing page
  - id: docs-index
    resource: docs/index.md
    title: Documentation home page capability table
  - id: makefile
    resource: Makefile
    title: install, py-test, coverage, and upgrade targets
  - id: pyproject
    resource: pyproject.toml
    title: Dependency groups
  - id: db-cargo
    resource: src/rs/vertebra-db/Cargo.toml
    title: vertebra-db crate dependencies
  - id: sqla-dialects
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIConnection
    title: SQLAlchemy DB-API connection URL parsing and InterfaceError
  - id: oracle-driver
    resource: src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver
    title: Oracle driver native-module guard
  - id: oracle-embeddings
    resource: src/py/vertebra/db/adapters/oracle/embeddings.py::OracleEmbeddingPushdown
    title: Oracle SQL embedding pushdown over an Oracle driver
  - id: duckdb-group-task
    resource: .agents/bundles/specs/driver-fail-loud/chapters/ch02-duckdb-sqlalchemy-queue-fail-loud/tasks/t01-duckdb-dependency-group.md
    title: DuckDB dependency group and Makefile wiring contract
  - id: resp3-fixture-task
    resource: .agents/bundles/specs/driver-fail-loud/chapters/ch03-remove-memory-fakes/tasks/t02-resp3-loopback-fixture-and-unit-retarget.md
    title: resp3_server loopback fixture contract
tags:
  - docs
  - okf
  - agents
  - duckdb
  - embeddings
  - fail-loud
---

# Chapter 11: Agent Knowledge, Dependency Groups, and Embeddings Docs

<!-- truth: start -->
- **DuckDB group in agent docs**: `AGENTS.md`, `workflow.md`, `benchmarks-and-free-threaded.md`, `tech-stack.md`, and `polyglot-test-matrix.md` state that `duckdb>=1.0` lives in a dedicated GIL-only `duckdb` dependency group that `bench` includes and `dev` excludes; `make install` syncs `--group duckdb`, and `make py-test`, `make coverage`, and `make upgrade` add it only on GIL interpreters.[^duckdb-group-task][^makefile][^pyproject]
- **DuckDB test guard**: `polyglot-test-matrix.md` states that DuckDB-executing tests call `pytest.importorskip("duckdb", reason=_DUCKDB_SKIP_REASON)` and that parametrized tests guard only their DuckDB branch.[^okf-test-matrix][^duckdb-group-task]
- **resp3_server fixture**: `polyglot-test-matrix.md` names the `src/py/tests/unit/conftest.py` `resp3_server` loopback fixture as the Redis/Valkey unit-test seam.[^resp3-fixture-task]
- **vertebra-db dependencies**: `tech-stack.md` lists no `sha2` or `rmp-serde` for `vertebra-db`.[^db-cargo][^okf-tech-stack]
- **DB-API errors**: `sqlspec-alchemy-and-dialects.md` states that an unparsable `+vtb` URL raises the DB-API `InterfaceError` and that Oracle and MSSQL sessions raise `MissingDependencyError`.[^sqla-dialects][^oracle-driver]
- **Oracle embedding pushdown**: user docs and OKF state that `OracleEmbeddingPushdown` needs an Oracle driver, the native Oracle driver package is not yet published, and constructing an Oracle driver raises `MissingDependencyError`.[^oracle-embeddings][^oracle-driver]
- **Landing pages**: `docs/index.md` lists engines without a count and marks the native Oracle and SQL Server drivers as not yet published.[^docs-index]
- **OKF provenance**: every edited OKF file keeps 1:1 `sources` to `[^id]:` footnotes, a `<= 40`-line truth block, zero body `---` rules, and a prepended first `verified:` entry `{by: agent:flow-executor, at: <UTC timestamp>, commit: <HEAD short sha>}`.[^okf-workflow]
- **No internal names in user docs**: `docs/**` targets contain no flow, chapter, or spec identifiers and no `vertebra-core[oracle]` / `vertebra-core[mssql]` install command.
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Chapters 02, 03, and 06 change facts that eleven files outside the ch09 and ch10 ownership still state the old way:

- `AGENTS.md`, `workflow.md`, `benchmarks-and-free-threaded.md`, and `tech-stack.md` describe `make install` as `uv sync --all-extras --dev --group doc` and keep `duckdb` inside `[dependency-groups].bench`; ch02 t01 moves it into a dedicated `duckdb` group that `bench` includes and that the Makefile installs on GIL interpreters only.[^agents-md][^okf-workflow][^okf-benchmarks][^okf-tech-stack][^duckdb-group-task]
- `tech-stack.md` lists `sha2 0.10` and `rmp-serde 1.3` as `vertebra-db` dependencies; ch06 removes both.[^okf-tech-stack][^db-cargo]
- `polyglot-test-matrix.md` describes neither the GIL-only DuckDB group with its `pytest.importorskip("duckdb")` guard nor the `resp3_server` loopback fixture that ch03 t02 creates.[^okf-test-matrix][^resp3-fixture-task]
- `sqlspec-alchemy-and-dialects.md` says unsupported URLs raise `UnsupportedBackendError` and that the Oracle SQLSpec drivers execute `OracleCore.build_session_context_sql`; after ch01 and ch02 the DB-API raises `InterfaceError` and Oracle sessions raise `MissingDependencyError`.[^okf-dialects][^sqla-dialects][^oracle-driver]
- `docs/usage/embeddings-and-vectors.md`, `docs/usage/index.md`, the OKF embeddings chapter, and the OKF domain index present `OracleEmbeddingPushdown` as usable, but it requires an Oracle driver that raises `MissingDependencyError`.[^docs-embeddings][^docs-usage-index][^okf-embeddings][^okf-domains-index][^oracle-embeddings]
- `docs/index.md` advertises "Nine native database drivers".[^docs-index]

This chapter rewrites those passages in present tense so agent instructions, OKF knowledge, and user docs describe the code at the ch10 closing commit.

**Non-goals**: files owned by ch09 (`README.md`, databases/extensions/reference/analytics docs, OKF drivers, tenancy, product, ADR-002, and the "Database driver conformance" section of `polyglot-test-matrix.md`) and ch10 (queue docs and knowledge); `.github/workflows/ci.yml` (unchanged by ch02 t01); the changelog; Python docstrings and Rust crate docs; `AGENTS.md` lines other than the `make install` entry.

### 1.2 Upstream Execution Baseline

- Branch `feat/storage-updates`, plan anchored at `0acdf576`; parent roadmap [`spec.md`](../../spec.md).
- Depends on `ch09-docs-and-okf` (t02 edits `polyglot-test-matrix.md` after ch09 t03 rewrites its conformance section, and user-doc phrasing for pending engines follows ch09 t01) and `ch10-queue-docs-and-knowledge` (serializes the documentation chapters). Transitively requires ch01, ch02, ch03, and ch06 `completed`.
- Executors read the post-ch10 files named in each worksheet and document the code as found.

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path::Symbol` or heading) | Claim to Correct |
| --- | --- | --- |
| Agent instructions | `AGENTS.md` "Quick Verification Commands" `make install` line[^agents-md] | `uv sync --all-extras --dev --group doc` without `--group duckdb` |
| Workflow | `workflow.md` "Canonical Polyglot Commands (`Makefile`)" block (`make install`, `make upgrade`, `make py-test`, `make coverage` lines) and "Polyglot Rebuild & Hygiene Rules" rule 5[^okf-workflow] | `duckdb` kept in `bench`; install/test lines without the group |
| Benchmarks | `benchmarks-and-free-threaded.md` truth block and "5. Isolating `--group bench` Dependencies (`pyproject.toml`)"[^okf-benchmarks] | `duckdb` listed as a `bench`-only package; "keeping `duckdb` out of `dev`" reasoning without the dedicated group |
| Tech stack | `tech-stack.md` "Rust Workspace Crates" `vertebra-db` bullet, "Python Package Configuration" "Optional Extras" and "Dependency Groups" bullets[^okf-tech-stack] | `sha2 0.10`, `rmp-serde 1.3`; no `duckdb` group; install command |
| Test matrix | `polyglot-test-matrix.md` "2. Test & Lint Targets", "3. CI Matrix", "4. Suite Inventory & Test Quality Standards", "Driver regression boundaries"[^okf-test-matrix] | No DuckDB group, no `importorskip` rule, no `resp3_server` fixture |
| Dialects | `sqlspec-alchemy-and-dialects.md` "1. Plugins, Tier-1 Native Configs…" `SyncAdapterConfig` row and "4. `+vtb` Dialects…" `SyncDBAPIModule` row[^okf-dialects] | `UnsupportedBackendError`; Oracle tenant path described as executing |
| Domain index | `domains/index.md` embeddings entry and "Database Drivers & Cloud Dialers" entry[^okf-domains-index] | Oracle and MSSQL drivers and `OracleEmbeddingPushdown` listed without status |
| Embeddings | `docs/usage/embeddings-and-vectors.md` "Embedder Selection Matrix" and "Google Cloud & In-Database SQL Pushdown"; `docs/usage/index.md` "Vector Embeddings & Slabs" row; OKF embeddings truth block and "4. Embedder Hierarchy" table[^docs-embeddings][^docs-usage-index][^okf-embeddings] | `OracleEmbeddingPushdown` presented as usable |
| Home page | `docs/index.md` "Databases & Analytics" capability row[^docs-index] | "Nine native database drivers" |
| Seams | `Makefile::install`/`py-test`/`coverage`/`upgrade`[^makefile], `pyproject.toml::dependency-groups`[^pyproject], `src/rs/vertebra-db/Cargo.toml`[^db-cargo], `dialects.py::SyncDBAPIConnection`[^sqla-dialects], `oracle/driver.py::SyncOracleDriver`[^oracle-driver], `oracle/embeddings.py::OracleEmbeddingPushdown`[^oracle-embeddings] | Sources of truth |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Project workflow](../../../../knowledge/workflow.md)
- [Benchmarks and free-threaded Python](../../../../knowledge/testing/benchmarks-and-free-threaded.md)
- [Polyglot test matrix](../../../../knowledge/testing/polyglot-test-matrix.md)
- [Tech stack](../../../../product/tech-stack.md)
- [SQLSpec, Advanced Alchemy, and dialects](../../../../knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md)
- [Embeddings and vectors](../../../../knowledge/domains/agent/embeddings-and-vectors.md)
- [Skills index](../../../../../skills/index.md)

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R18.1 (t01)**: `AGENTS.md`, `workflow.md`, and `benchmarks-and-free-threaded.md` state the `make install` command with `--group duckdb`, the GIL-only `--group duckdb` on `py-test`, `coverage`, and `upgrade`, and the dedicated `duckdb` group that `bench` includes and `dev` excludes.
- **R18.2 (t02)**: `tech-stack.md` drops `sha2` and `rmp-serde` from `vertebra-db`, lists the `duckdb` group and `bench`'s inclusion of it, and updates the install command; `polyglot-test-matrix.md` adds the GIL-only DuckDB group, the `pytest.importorskip("duckdb")` rule, and the `resp3_server` fixture outside the "Database driver conformance" section.
- **R18.3 (t03)**: `sqlspec-alchemy-and-dialects.md` states the DB-API `InterfaceError` and Oracle/MSSQL `MissingDependencyError`; `domains/index.md` marks Oracle and MSSQL drivers and `OracleEmbeddingPushdown` as pending; `docs/index.md` drops "Nine native database drivers".
- **R18.4 (t04)**: `docs/usage/embeddings-and-vectors.md`, `docs/usage/index.md`, and the OKF embeddings chapter state that `OracleEmbeddingPushdown` needs the pending native Oracle driver and that constructing an Oracle driver raises `MissingDependencyError`.

### 2.2 Documentation Contracts

Facts used across all files (executors adapt sentence structure, not facts):

| Topic | Required Claim | Forbidden Claim |
| --- | --- | --- |
| `make install` | `uv sync --all-extras --dev --group doc --group duckdb` (the target pins Python 3.12, a GIL interpreter) | `uv sync --all-extras --dev --group doc` as the full command |
| Test targets | `py-test`, `coverage`, and `upgrade` add `$(PY_TEST_GROUPS)`, which is `--group duckdb` when `Py_GIL_DISABLED` is falsy and empty on `3.14t` / `3.15t` | DuckDB installed on free-threaded interpreters |
| Dependency groups | `duckdb = ["duckdb>=1.0"]`; `bench` includes it with `{ include-group = "duckdb" }`; `dev` keeps `test`, `lint`, `build` | `duckdb` as a direct `bench` member; `duckdb` in `dev` |
| Reason | DuckDB publishes only `cp312-abi3` wheels; free-threaded CPython cannot load abi3 wheels | Any claim that `duckdb` builds on `3.14t` |
| DuckDB tests | DuckDB-executing tests call `pytest.importorskip("duckdb", reason=_DUCKDB_SKIP_REASON)` first; parametrized tests guard only the DuckDB branch | Unconditional DuckDB tests |
| `resp3_server` | Function-scoped fixture in `src/py/tests/unit/conftest.py` yielding a threaded loopback `Resp3FakeServer` (`port`, `redis_url`, `valkey_url`, `commands`, `close()`) that Redis, Valkey, and store unit tests use | `memory://` Redis/Valkey configs in tests |
| `vertebra-db` deps | No `sha2` or `rmp-serde`; workspace `sha2` stays for `vertebra-core` and `vertebra-storage` | `rmp-serde` anywhere in the tech stack |
| DB-API | `SyncDBAPIConnection` / `AsyncDBAPIConnection` raise the module's PEP 249 `InterfaceError` chained from `ValueError` when `DatabaseRegistry.parse_url` fails | `UnsupportedBackendError` for unparsable URLs; SQLite substitution |
| Oracle / MSSQL sessions | SQLSpec Oracle and MSSQL session acquisition and `oracle+vtb` / `mssql+vtb` connections raise `MissingDependencyError` because the native driver packages are not yet published | Oracle tenant context executing in a session |
| Oracle pushdown | `OracleEmbeddingPushdown(driver, ...)` takes a `SyncOracleDriver` / `AsyncOracleDriver`; constructing either raises `MissingDependencyError`; the SQL builder facts stay documented | Oracle pushdown presented as runnable today; `uv add vertebra-core[oracle]` in user docs |
| Home page | Engine list without a count; native Oracle and SQL Server drivers not yet published | "Nine native database drivers" |

### 2.3 Caller -> Seam -> Collaborator Map

- Contributor following `AGENTS.md` → `make install` → `uv sync ... --group duckdb` → `.venv` with `duckdb`.
- CI `test-python` on `3.14t` → `make py-test` → `PY_TEST_GROUPS` empty → `pytest.importorskip("duckdb")` skips DuckDB tests.
- Reader of the embeddings guide → `OracleEmbeddingPushdown(driver)` → `SyncOracleDriver.__init__` → `_require_oracle_native` → `MissingDependencyError`.
- Agent loading `sqlspec-alchemy-and-dialects.md` → `SyncDBAPIConnection("nonsense+vtb://...")` → `DatabaseRegistry.parse_url` → `ValueError` → `InterfaceError`.
- `make docs` → Zensical build, `tools.docs.postbuild`, `tools.docs.verify_docs_artifacts` → broken links fail.
- `make review-check` → `tools.scripts.review_setup validate` → bundle and privacy rules over `.agents/bundles/**`.

### 2.4 Edge-Case & Failure-Mode Matrix

| Scenario | Trigger | Expected Behavior | Invariant |
| --- | --- | --- | --- |
| Upstream Makefile differs | Post-ch02 `install` line is not `@uv sync --all-extras --dev --group doc --group duckdb` | Document the Makefile as found; add a Notes line; report | Docs describe code at `HEAD` |
| `resp3_server` absent | `src/py/tests/unit/conftest.py` has no `resp3_server` (ch03 incomplete) | Stop and set `blocked_reason` | Never document planned fixtures |
| `sha2` still in `vertebra-db` | `src/rs/vertebra-db/Cargo.toml` still lists `sha2` or `rmp-serde` | Stop and report a ch06 contradiction | Dependency list matches the manifest |
| Shared file with ch09 | `polyglot-test-matrix.md` "Database driver conformance" | Leave it byte-identical to ch09 t03's result | One owner per section |
| Domain index is reserved | `domains/index.md` frontmatter | Keep it frontmatter-free; edit list text only | Subdirectory indexes carry no frontmatter |
| Truth block growth | New bullets in `benchmarks-and-free-threaded.md` or embeddings chapter | Merge into existing bullets; stay `<= 40` lines | Bounded truth block |
| Internal name leak | Worksheet wording copied into `docs/**` | Leak grep `native-[a-z-]+-drivers?\|pending_flow_id\|driver work\|driver-fail-loud\|ch[0-9]{2}` matches; rewrite | No flow/spec ids in user docs |
| Planned extra quoted | `uv add vertebra-core[oracle]` in user docs | Planned-extra grep fails; say "not yet published" | User docs never advertise an uninstallable command |

### 2.5 Acceptance Scenarios

- Given `AGENTS.md`, when a contributor reads the `make install` line, then it shows `--group duckdb`.
- Given `workflow.md` and `benchmarks-and-free-threaded.md`, when an agent searches for "Keep `duckdb`" in a `bench`-only rule, then it finds the dedicated GIL-only `duckdb` group instead.
- Given `tech-stack.md`, when an agent searches the `vertebra-db` bullet for `sha2` or `rmp-serde`, then it finds no match.
- Given `polyglot-test-matrix.md`, when an agent reads the CI and suite sections, then it finds the GIL-only DuckDB group, the `importorskip` rule, and the `resp3_server` fixture.
- Given the embeddings guide, when a reader looks for Oracle pushdown, then it states that the native Oracle driver is not yet published and that Oracle drivers raise `MissingDependencyError`.
- Given `docs/index.md`, when a reader scans "Databases & Analytics", then no engine count appears and Oracle and SQL Server read as not yet published.

### 2.6 Alternatives Considered

| Alternative | Outcome |
| --- | --- |
| Put the DuckDB-group and `resp3_server` lines into ch09 t03 | Rejected: ch09 t03's read set is at 6 files; the additions need `Makefile`/`pyproject.toml` and the new `conftest.py`, so a ch11 task owns them with an explicit dependency on ch09 |
| Fold these files into ch09 or ch10 | Rejected: ch09 owns 11 files across 4 tasks; adding 11 more breaks the 4-task bound |
| Remove Oracle pushdown from user docs | Rejected: the class, SQL builder, and tests ship; readers need the requirement |
| Rewrite all of `AGENTS.md` to match OKF prose | Rejected: `AGENTS.md` is agent-facing with its own terse style; only the `make install` line changes |
| One task per topic (DuckDB, deps, dialects, embeddings, landing) | Rejected: five tasks exceed the 4-task bound; grouping by file affinity keeps each task at `<= 3` files and `read_set <= 6` |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/11 files documented]`
- `[ ]` `AGENTS.md` — t01
- `[ ]` `.agents/bundles/knowledge/workflow.md` — t01
- `[ ]` `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md` — t01
- `[ ]` `.agents/bundles/product/tech-stack.md` — t02
- `[ ]` `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` — t02 (sections outside "Database driver conformance")
- `[ ]` `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` — t03
- `[ ]` `.agents/bundles/knowledge/domains/index.md` — t03
- `[ ]` `docs/index.md` — t03
- `[ ]` `docs/usage/embeddings-and-vectors.md` — t04
- `[ ]` `docs/usage/index.md` — t04
- `[ ]` `.agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md` — t04

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Doc Gate |
| --- | --- | --- | --- |
| R18.1 | [t01](tasks/t01-duckdb-group-agent-workflow.md) | `documentation_validation` | stale-claim grep, OKF pairing check, relative-link check, `make review-check` |
| R18.2 | [t02](tasks/t02-tech-stack-and-test-matrix.md) | `documentation_validation` | stale-claim grep, OKF pairing check, relative-link check, `make review-check` |
| R18.3 | [t03](tasks/t03-dialect-knowledge-and-landing-pages.md) | `documentation_validation` | stale-claim grep, leak grep, OKF pairing check, relative-link check, `make docs`, `make review-check` |
| R18.4 | [t04](tasks/t04-oracle-embedding-pushdown-docs.md) | `documentation_validation` | stale-claim grep, leak grep, OKF pairing check, relative-link check, `make docs`, `make review-check` |

## 5. Implementation Plan

- [ ] driver-fail-loud:ch11:t01 — AGENTS.md, workflow, and benchmark knowledge state the GIL-only DuckDB group and install command (tasks/t01-duckdb-group-agent-workflow.md)
- [ ] driver-fail-loud:ch11:t02 — Tech stack and test matrix state vertebra-db deps, the DuckDB group, the importorskip rule, and resp3_server (tasks/t02-tech-stack-and-test-matrix.md)
- [ ] driver-fail-loud:ch11:t03 — Dialect knowledge, domain index, and home page state InterfaceError and pending Oracle/SQL Server drivers (tasks/t03-dialect-knowledge-and-landing-pages.md)
- [ ] driver-fail-loud:ch11:t04 — Embeddings guide, usage index, and OKF embeddings chapter state the Oracle pushdown requirement (tasks/t04-oracle-embedding-pushdown-docs.md)
- **Chapter / Phase Recovery Checkpoint**: `make review-check && make docs && uv run --no-sync pytest src/py/tests/unit/tools/test_docs_tooling.py -q`

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline (`HEAD`)**: `0acdf576` — chapter authored; blocked on `ch09` and `ch10`.
- **Remaining Tasks**: `t01`, `t02`, `t03`, `t04` (`0/4` completed).
- **Documentation & OKF Sync**: `[0/11 files documented]`.
- **Active Invariants**: `AGENTS.md` changes only on the `make install` line; the `polyglot-test-matrix.md` conformance section stays as ch09 leaves it; changelog untouched; no internal identifiers or planned-extra commands in user docs.

[^agents-md]: `AGENTS.md` — Agent instructions Quick Verification Commands block.
[^okf-workflow]: `.agents/bundles/knowledge/workflow.md` — OKF project workflow commands and hygiene rules.
[^okf-benchmarks]: `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md` — OKF benchmark suite and free-threaded test gate chapter.
[^okf-tech-stack]: `.agents/bundles/product/tech-stack.md` — Product tech stack crate dependencies and dependency groups.
[^okf-test-matrix]: `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` — OKF polyglot test matrix.
[^okf-dialects]: `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` — OKF SQLSpec, Advanced Alchemy, and SQLAlchemy dialect chapter.
[^okf-domains-index]: `.agents/bundles/knowledge/domains/index.md` — OKF domain vocabulary index.
[^okf-embeddings]: `.agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md` — OKF vector slabs, embeddings, and SQL pushdown chapter.
[^docs-embeddings]: `docs/usage/embeddings-and-vectors.md` — Vector embeddings and SQL pushdown usage guide.
[^docs-usage-index]: `docs/usage/index.md` — Usage guide landing page.
[^docs-index]: `docs/index.md` — Documentation home page capability table.
[^makefile]: `Makefile` — install, py-test, coverage, and upgrade targets.
[^pyproject]: `pyproject.toml` — Dependency groups.
[^db-cargo]: `src/rs/vertebra-db/Cargo.toml` — vertebra-db crate dependencies.
[^sqla-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIConnection` — SQLAlchemy DB-API connection URL parsing and InterfaceError.
[^oracle-driver]: `src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver` — Oracle driver native-module guard.
[^oracle-embeddings]: `src/py/vertebra/db/adapters/oracle/embeddings.py::OracleEmbeddingPushdown` — Oracle SQL embedding pushdown over an Oracle driver.
[^duckdb-group-task]: `.agents/bundles/specs/driver-fail-loud/chapters/ch02-duckdb-sqlalchemy-queue-fail-loud/tasks/t01-duckdb-dependency-group.md` — DuckDB dependency group and Makefile wiring contract.
[^resp3-fixture-task]: `.agents/bundles/specs/driver-fail-loud/chapters/ch03-remove-memory-fakes/tasks/t02-resp3-loopback-fixture-and-unit-retarget.md` — resp3_server loopback fixture contract.
