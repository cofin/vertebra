---
type: Spec
parent_flow: db-pool-broker-perf
chapter_id: ch03-asyncpg-parity-and-verification
title: Local asyncpg Parity, Full SQLite/PostgreSQL Verification, and OKF Sync
description: Align NativePostgresWorldDriver multi-query checkout parity with asyncpg, add granian-rsgi-data-stdlib, execute live SQLite and Docker PostgreSQL 17 benchmarks with --group postgres-bench, and sync OKF knowledge chapters.
status: stable
state: planned
planning_quality: standard
planning_degraded_reason: null
planning_status: complete
depends_on_chapters:
  - ch01-sqlite-sqlalchemy-fastpath
  - ch02-async-shm-broker-contention
replan_trigger: null
plan_revision: 2
plan_commit: "16ccb56e"
current_task: null
last_verified_checkpoint: null
last_reviewed_commit: null
generated:
  by: flow/0.26.0
  at: "2026-11-27T18:30:00Z"
verified:
  - by: user:cofin
    at: "2026-11-27T18:48:00Z"
    commit: "16ccb56e"
sources:
  - id: src-bench-drivers
    resource: tools/benchmarks/apps/_db_drivers.py::NativePostgresWorldDriver
    title: Native and stdlib PostgreSQL WorldDriver implementations
  - id: src-bench-targets
    resource: tools/benchmarks/targets.py::ALL_TARGETS
    title: Benchmark data and baseline target catalog
  - id: src-bench-sql
    resource: tools/benchmarks/adapters/sql.py::capture_postgres_adapter_report
    title: Standalone SQL, SQLSpec, and Advanced Alchemy comparator harness
  - id: src-bench-tests
    resource: src/py/tests/unit/tools/test_e2e_benchmarks.py
    title: Offline benchmark target and driver unit tests
tags:
  - benchmarks
  - postgres
  - sqlite
  - asyncpg
  - documentation
---

# Chapter 03: Local asyncpg Parity, Full SQLite/PostgreSQL Verification, and OKF Sync

<!-- truth: start -->
- `NativePostgresWorldDriver` checks out one pooled connection (and one SHM broker lease when `shm_broker` is active) per `fetch_worlds_async` and `update_worlds_async` call, with a transaction around updates, matching `StdlibPostgresWorldDriver` (`asyncpg`) 1:1.
- `tools/benchmarks/targets.py` registers `granian-rsgi-data-stdlib` (port `8170`, `VTB_BENCH_DB_ADAPTER=stdlib`, `VTB_BENCH_DB_ADAPTER_FLAVOR=upstream`) for direct comparison against the top TechEmpower Python RSGI + `asyncpg` stack.
- Live release verification (`make build` + `--group postgres-bench`) captures apples-to-apples SQLite (Suite B) and Docker PostgreSQL 17 (Suite C HTTP + standalone external comparators) benchmarks and updates the 4 affected OKF knowledge chapters.
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

After `ch01` and `ch02` fix the SQLite SQLAlchemy and Async SHM Broker bottlenecks, two benchmark fairness/coverage gaps remain:
1. `NativePostgresWorldDriver.fetch_worlds_async` and `update_worlds_async` perform 20/10 independent pool+broker checkouts per request, whereas `StdlibPostgresWorldDriver` (`asyncpg`) acquires one pooled connection per request (and wraps updates in one transaction) [^src-bench-drivers].
2. `.venv` has not installed `--group postgres-bench` (`asyncpg`, `psycopg[binary]`) needed by standalone comparators [^src-bench-sql], and `targets.py` lacks a `granian-rsgi-data-stdlib` (`asyncpg`) target covered by offline benchmark tests [^src-bench-targets] [^src-bench-tests].

### 1.2 Upstream Execution Baseline

- Depends on `ch01-sqlite-sqlalchemy-fastpath` and `ch02-async-shm-broker-contention` passing their fast chapter gates.

### 1.3 Code Analysis & Interface Summary

| Area | Files Examined (`path::Symbol`) | Public Interface & Dependency Category | Affected Docs / OKF Chapters |
| :--- | :--- | :--- | :--- |
| Postgres benchmark driver | `tools/benchmarks/apps/_db_drivers.py::NativePostgresWorldDriver` | Benchmark harness driver (`fetch_worlds_async`, `update_worlds_async`) | `testing/benchmarks-and-free-threaded.md` |
| Benchmark targets | `tools/benchmarks/targets.py::ALL_TARGETS` | Benchmark CLI target (`granian-rsgi-data-stdlib`, port `8170`) | `testing/benchmarks-and-free-threaded.md` |
| Standalone SQL comparators | `tools/benchmarks/adapters/sql.py` | Remote-owned Docker `postgres:17-alpine` + `--include-external` | `testing/benchmarks-and-free-threaded.md` |
| OKF knowledge | `.agents/bundles/knowledge/domains/{data/sqlite-limitations,data/broker-and-shm-leases,extensions/sqlspec-alchemy-and-dialects}.md`, `testing/benchmarks-and-free-threaded.md` | OKF v0.2 documentation chapters | Self |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- Knowledge chapters: `domains/data/sqlite-limitations.md`, `domains/data/broker-and-shm-leases.md`, `domains/extensions/sqlspec-alchemy-and-dialects.md`, `testing/benchmarks-and-free-threaded.md`.
- Skills: `review-docs-okf`, `review-performance`.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional & Performance Requirements

- **R1 (Single-checkout multi-query parity)**: Update `NativePostgresWorldDriver.fetch_worlds_async` and `update_worlds_async` to call `await driver.connect()`, acquire one SHM broker lease (when `self.async_broker is not None`, without double-acquiring because `driver.pool` is the raw Rust `AsyncPostgresPool`) and one physical client via `client = await driver.pool.acquire()`, run the 20 `fetch_dicts` or transactional `begin`/`fetch_dicts`/`execute_dml`/`commit` sequence on `client`, and release `client` via `await client.close()` under `CancelScope(shield=True)` plus broker lease release in `finally`. Sort `updates` by `row_id` before executing `UPDATE` statements inside the transaction in both `NativePostgresWorldDriver.update_worlds_async` and `StdlibPostgresWorldDriver.update_worlds_async` (preserving returned row order) to prevent PostgreSQL `40P01` lock-order deadlocks under `c=128`. Keep `fetch_world_async` (1 query) on `driver.select_one(...)`.
- **R2 (`granian-rsgi-data-stdlib` target)**: Register `granian-rsgi-data-stdlib` on port `8170` in `tools/benchmarks/targets.py` using `tools.benchmarks.apps.data_rsgi:rsgi_app`, `interface="rsgi"`, `category="data-baseline"`, `adapter="stdlib"`, `VTB_BENCH_DB_ADAPTER="stdlib"`, `VTB_BENCH_DB_ADAPTER_FLAVOR="upstream"`.
- **R3 (Live Suite B & Suite C release verification)**: Install `--group postgres-bench` alongside existing extras/groups, run `make build`, and execute:
  - Suite B (SQLite 4w HTTP): `vtb-data-shm-native-4w`, `vtb-data-sqlspec-native-4w`, `vtb-data-aa-native-4w`, `granian-asgi-data-{stdlib,sqlspec,aa}`.
  - Suite C (Docker PostgreSQL 17 4w HTTP): `vtb-data-shm-native-4w`, `vtb-data-shm-native-4w-broker`, `vtb-data-sqlspec-native-4w`, `vtb-data-sqlspec-shm-4w-broker`, `vtb-data-aa-native-4w`, `vtb-data-aa-shm-4w-broker`, `granian-rsgi-data-stdlib`, `granian-asgi-data-{stdlib,sqlspec,aa}`, `uvicorn-httptools-data-{stdlib,sqlspec,aa}`.
  - Standalone comparators: `tools.benchmarks.adapters.sql` for `--engine postgres`, `--engine sqlspec`, and `--engine advanced_alchemy --aa-engine postgres` with `--include-external`, plus SQLite `--engine advanced_alchemy --aa-engine sqlite`.
- **R4 (OKF knowledge sync & full verification)**: Update all 4 OKF knowledge chapters in present tense, verify `make lint-all`, `make rs-test`, `make review-check`, and Flow validator exit `0`.

### 2.2 Shared Concrete Contracts

```python
# tools/benchmarks/targets.py
_granian_data(
    "granian-rsgi-data-stdlib",
    "tools.benchmarks.apps.data_rsgi:rsgi_app",
    8170,
    "rsgi",
    "Granian RSGI data baseline (stdlib drivers)",
    factory=False,
    category="data-baseline",
    adapter="stdlib",
    extra_env={"VTB_BENCH_DB_ADAPTER": "stdlib", "VTB_BENCH_DB_ADAPTER_FLAVOR": "upstream"},
)
```

### 2.3 Caller -> Interface -> Collaborator Map

- `tools.benchmarks.apps.data_native` -> `DataBackend` -> `NativePostgresWorldDriver` -> `AsyncPostgreSQLDriver.pool.acquire()` (+ optional `AsyncSHMBrokerTransport.acquire()`) -> `postgres:17-alpine`.
- `tools.benchmarks.apps.data_rsgi` -> `DataBackend` -> `StdlibPostgresWorldDriver` -> `asyncpg.create_pool` -> `postgres:17-alpine`.

### 2.4 Edge-Case & Failure-Mode Matrix

| Edge Case / Failure Mode | Required Behavior |
| :--- | :--- |
| Exception raised inside `NativePostgresWorldDriver.update_worlds_async` | Roll back via `await client.rollback()` (suppressing secondary errors), close `client` via `await client.close()` under `CancelScope(shield=True)` (Rust handles return or dirty-connection discard without a non-existent `pool.evict()`), and release the single broker lease in `finally` with zero leaks. |
| Concurrent multi-row `/updates` under `c=128` | Sort updates by `row_id` before executing `UPDATE` statements inside the transaction in both `NativePostgresWorldDriver.update_worlds_async` and `StdlibPostgresWorldDriver.update_worlds_async` to prevent `40P01` lock-order deadlocks. |
| `NativePostgresWorldDriver` called on `fetch_worlds_async` with `broker_mode="shm_broker"` | Acquires exactly 1 SHM broker lease for all 20 queries, never 20 separate leases. |
| `uv sync` adding `--group postgres-bench` | Must include `--all-extras --dev --group bench --group bench-comparators --group postgres-bench` so existing dev/bench/doc packages stay installed; rebuild native extensions via `make build`. |
| Unstaged repo benchmark results (`tools/benchmarks/results/*`) | Never stage or commit `tools/benchmarks/results/*` or unrelated unstaged repo edits. |

### 2.5 Acceptance Scenarios

1. Offline unit tests in `test_e2e_benchmarks.py` verify `granian-rsgi-data-stdlib` registration, port uniqueness (`8170`), and `NativePostgresWorldDriver` single-checkout / single-lease / sorted-update contract.
2. Live Suite B and Suite C runs complete with `0` errors on all Vertebra targets and produce a 1:1 table comparing Vertebra vs `asyncpg`/`psycopg` (Granian RSGI/ASGI & Uvicorn) and standalone comparators.
3. All 4 OKF knowledge chapters pass Flow OKF validation, `make review-check`, and `make lint-all`.

### 2.6 Alternatives Considered

- **Using `AsyncPostgresPipeline` inside `NativePostgresWorldDriver.fetch_worlds_async`**: Rejected for the primary 1:1 baseline because `StdlibPostgresWorldDriver` (`asyncpg`) executes 20 sequential `await conn.fetchrow(...)` calls; matching 1-checkout + 20 sequential awaits guarantees strict apples-to-apples fairness.

## 3. Documentation & OKF Knowledge Plan

- `[0/4 files documented]`
  - `.agents/bundles/knowledge/domains/data/sqlite-limitations.md` (`t09`)
  - `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` (`t09`)
  - `.agents/bundles/knowledge/domains/data/broker-and-shm-leases.md` (`t09`)
  - `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md` (`t09`)

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task ID | Verification Strategy |
| :--- | :--- | :--- |
| R1, R2 | `t07` | `behavior_tdd`, `regression_tdd` |
| R3 | `t08` | `integration_acceptance` |
| R4 | `t09` | `documentation_validation` |

## 5. Implementation Plan

- [ ] t07 — Align `NativePostgresWorldDriver` single-checkout parity and add `granian-rsgi-data-stdlib` target + unit tests (`tasks/t07-postgres-driver-parity-and-rsgi-target.md`)
- [ ] t08 — Install `--group postgres-bench`, run `make build`, and execute live Suite B (SQLite) and Suite C (Docker PostgreSQL 17 vs `asyncpg`/`psycopg`) benchmarks (`tasks/t08-live-sqlite-and-postgres-asyncpg-verification.md`)
- [ ] t09 — Update OKF knowledge chapters and run final `make lint-all`, `make rs-test`, and `make review-check` gates (`tasks/t09-okf-knowledge-sync-and-final-gate.md`)

### Chapter / Phase Recovery Checkpoint

```bash
uv run --no-sync pytest src/py/tests/unit/tools/test_e2e_benchmarks.py src/py/tests/unit/tools/test_adapter_benchmarks_sql.py && uv run --no-sync ruff check tools/benchmarks/apps/_db_drivers.py tools/benchmarks/targets.py src/py/tests/unit/tools/test_e2e_benchmarks.py
```

## 6. Current State & Remaining Plan Snapshot

- **Chapter State**: `planned` (`0/3` tasks complete).
- **Next Task**: `t07` (blocked until `ch01` and `ch02` complete).

[^src-bench-drivers]: `tools/benchmarks/apps/_db_drivers.py::NativePostgresWorldDriver` — Native and stdlib PostgreSQL WorldDriver implementations.
[^src-bench-targets]: `tools/benchmarks/targets.py::ALL_TARGETS` — Benchmark data and baseline target catalog.
[^src-bench-sql]: `tools/benchmarks/adapters/sql.py::capture_postgres_adapter_report` — Standalone SQL, SQLSpec, and Advanced Alchemy comparator harness.
[^src-bench-tests]: `src/py/tests/unit/tools/test_e2e_benchmarks.py` — Offline benchmark target and driver unit tests.
