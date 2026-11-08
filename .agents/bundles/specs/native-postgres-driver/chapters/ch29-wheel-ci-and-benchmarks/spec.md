---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch29-wheel-ci-and-benchmarks
title: "Chapter 29: Installed Wheel Isolation, Strict CI Matrix, and Reproducible Benchmarks"
description: Verify outside-checkout vertebra-postgres wheel installation, release/CI service and free-threaded lanes, deterministic benchmark capture/comparison gates, and paired PG18 reference vs native measurements.
status: stable
state: planned
planning_status: complete
depends_on_chapters:
  - ch28-live-conformance-and-service-acceptance
replan_trigger: null
current_task: "native-postgres-driver:ch29:t01"
plan_revision: 4
plan_commit: e8901de4
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-08T02:05:22Z"
generated:
  by: flow-revise
  at: "2026-11-07T16:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-07T16:45:00Z"
    commit: dd432f18
sources:
  - id: gh-ci
    resource: .github/workflows/ci.yml
    title: Continuous integration matrix for Python 3.12-3.15t, Rust, and service suites
  - id: gh-release
    resource: .github/workflows/release.yaml
    title: Multi-platform wheel, sdist, and installation verification workflow
  - id: ft-gil-test
    resource: src/py/tests/unit/test_freethreaded_gil.py::test_native_modules_declare_gil_used_false
    title: Free-threaded Python GIL declaration and concurrent extension probes
  - id: bench-pkg
    resource: tools/benchmarks/__init__.py
    title: Vertebra Python benchmark harness package root
  - id: root-makefile
    resource: Makefile
    title: Canonical workspace build, test, and benchmark orchestration targets
tags:
  - postgres
  - wheel
  - ci
  - benchmarks
  - freethreaded
---

# Chapter 29: Installed Wheel Isolation, Strict CI Matrix, and Reproducible Benchmarks

<!-- truth: start -->
- **Outside-Checkout Wheel & Namespace Isolation**: `tools/scripts/verify_postgres_wheel.py` (created in `ch29:t01`) installs `vertebra-core` + `vertebra-postgres` (`--no-deps`, empty `PYTHONPATH`) into an isolated environment outside the repository, exercises live `SELECT` and `transaction()` against a test-owned PostgreSQL service (`sslmode="disable"`), and proves uninstalling `vertebra-postgres` preserves core/sibling wheels while raising `MissingDependencyError(..., install_package="vertebra-core[postgres]")` [^gh-release] [^ft-gil-test].
- **Strict CI & Free-Threaded Matrix**: `.github/workflows/ci.yml` and `.github/workflows/release.yaml` cover Linux `x86_64`/`aarch64`, macOS universal, and Windows wheels across `cp312`–`cp315` plus `cp314t`/`cp315t`, parameterized PG14–18 service lanes, logical/`pgvector`/`PgBouncer` acceptance, and concurrent multi-threaded `3.14t`/`3.15t` pool/query smoke tests with `sys._is_gil_enabled() is False` [^gh-ci] [^gh-release] [^ft-gil-test].
- **Pinned Reference vs Native Benchmark Gate**: `tools/benchmarks/postgres.py` (`schema_version=1`, `psycopg[binary]==3.2.13`, `asyncpg==0.31.0`, strict `[tool.mypy]` files/overrides), `src/rs/vertebra-postgres/benches/postgres_native.rs`, and `src/py/tests/integration/db/test_postgres_benchmarks.py` enforce `median <= 1.25x`, `p95 <= 1.50x`, `statements_s >= 0.80x`, and `CV <= 0.15` across cold connect, warm prepared single-query, and 64-statement single-`Sync` pipeline scenarios via `Makefile` targets [^bench-pkg] [^root-makefile].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Live server conformance (`ch28`) runs inside the editable repository checkout, which cannot catch missing wheel metadata, broken namespace co-location when `vertebra-postgres` is installed or uninstalled outside the source tree, missing CI service containers, or throughput/latency regressions against established Python PostgreSQL clients [^gh-ci] [^gh-release] [^bench-pkg]. Earlier tasks in `ch08:t02`–`t04` already wired the root `[postgres]` optional extra, all four sibling `pyproject.toml` `_postgres.pyi` exclusions, and Phase 1 `release.yaml` `exports: SyncPostgresClient AsyncPostgresClient`.

This chapter closes packaging, CI, free-threaded runtime, and performance evidence across three sequential vertical slices:
1. Create `tools/scripts/verify_postgres_wheel.py`, update `.github/workflows/release.yaml` and `.github/workflows/ci.yml` for final client/pool/result wheel exports, strict PG14–18 + feature lanes, and concurrent `3.14t`/`3.15t` query/pool verification (`ch29:t01`) [^gh-ci] [^gh-release] [^ft-gil-test].
2. Add bench-only `psycopg[binary]==3.2.13` and `asyncpg==0.31.0` dependencies, register `"tools/benchmarks/postgres.py"` in `[tool.mypy].files` plus `"asyncpg*"` / `"psycopg*"` in `[[tool.mypy.overrides]]`, register the `postgres_benchmark` pytest marker (`-m "not postgres_benchmark"` by default), and implement `python -m tools.benchmarks.postgres capture|compare` with frozen `schema_version=1` `msgspec` records (`ch29:t02`) [^bench-pkg].
3. Wire Criterion `src/rs/vertebra-postgres/benches/postgres_native.rs`, the live PG18 paired benchmark orchestrator `src/py/tests/integration/db/test_postgres_benchmarks.py`, and canonical `Makefile` targets `postgres-test`, `postgres-wheel-check`, `postgres-bench`, and `postgres-bench-check` (`ch29:t03`) [^root-makefile].

### 1.2 Non-Goals

- Publishing wheels, pushing commits, dispatching GitHub Actions workflows, or creating/modifying Git tags during local execution.
- Adding `psycopg` or `asyncpg` to runtime `[project.dependencies]` or `[project.optional-dependencies]` (`postgres` extra installs `vertebra-postgres` exclusively).
- Benchmarking uncalibrated Arrow read throughput against a non-matching Python tuple client as a pass/fail gate (the 100k-row `VTBARROW` read is recorded as a schema/checksum diagnostic).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md); frozen contracts: [contracts.md](../../contracts.md); depends on [ch28-live-conformance-and-service-acceptance](../ch28-live-conformance-and-service-acceptance/spec.md). Skills: [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md), [python-framework](../../../../../skills/python-framework/SKILL.md), and [review-performance](../../../../../skills/review-performance/SKILL.md). Knowledge: [database-drivers-and-dialers.md](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [benchmarks-and-free-threaded.md](../../../../knowledge/testing/benchmarks-and-free-threaded.md), and [adr-004-optional-driver-wheels-and-shm-ownership.md](../../../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R26 (`ch29:t01`)**: Create `tools/scripts/verify_postgres_wheel.py` to build `vertebra-core`, `vertebra-postgres`, and sibling wheels (`sdist` + binary wheels), install into a fresh virtual environment outside the checkout with `--no-deps` and unset `PYTHONPATH`, verify `SyncPostgresClient`, `AsyncPostgresClient`, `SyncPostgresPool`, `AsyncPostgresPool`, and `PostgresQueryResult` exports plus live `SELECT` and `transaction()` (`sslmode="disable"`), uninstall `vertebra-postgres` to verify core/sibling imports and `MissingDependencyError` (`vertebra-core[postgres]`), extend `release.yaml` (`cp312`–`cp315`, `cp314t`, `cp315t` across Linux/macOS/Windows) from `ch08`'s Phase 1 client exports to final client/pool/result exports and `ci.yml` (PG14–18 + feature services), and verify concurrent multi-threaded `3.14t`/`3.15t` native pool/query execution with `sys._is_gil_enabled() is False`.
- **R27.1 (`ch29:t02`)**: Add `[dependency-groups] bench` (`psycopg[binary]==3.2.13`, `asyncpg==0.31.0`), add `"tools/benchmarks/postgres.py"` to `[tool.mypy].files` and `"asyncpg"`, `"asyncpg.*"`, `"psycopg"`, `"psycopg.*"` to `[[tool.mypy.overrides]]` (`ignore_missing_imports = true`), and register the `postgres_benchmark` marker (`addopts` excludes `postgres_benchmark` by default) in `pyproject.toml` + `uv.lock`; implement `python -m tools.benchmarks.postgres capture --implementation reference|native --dsn-env VERTEBRA_POSTGRES_BENCH_DSN --output <path>` plus offline `compare --reference <ref.json> --native <nat.json> --output <cmp.json>` using frozen `msgspec` `schema_version=1` records (`median <= 1.25 * reference`, `p95 <= 1.50 * reference`, `statements_s >= 0.80 * reference`, `CV <= 0.15` over 5 per-sample means with at most one fresh whole paired retry).
- **R27.2 (`ch29:t03`)**: Register `[[bench]] name = "postgres_native" harness = false` in `src/rs/vertebra-postgres/Cargo.toml` with Criterion `benches/postgres_native.rs` exercising live cold connect and warm prepared queries, implement `@pytest.mark.postgres_benchmark` orchestrator `src/py/tests/integration/db/test_postgres_benchmarks.py` owning an ephemeral PG18 container (`100_000`-row `bench_values` table + deterministic checksum + paired `reference`/`native` capture and `compare`), and expose `postgres-test`, `postgres-wheel-check`, `postgres-bench`, and `postgres-bench-check` in `Makefile`.

### 2.2 CLI, Schema & Collaborator Map

- `tools/scripts/verify_postgres_wheel.py` -> builds `dist/*.whl` -> spawns isolated outside-checkout venv Python (`PYTHONPATH` unset) -> verifies `vertebra._postgres`, live PG query/tx (`sslmode="disable"`), `pip uninstall vertebra-postgres`, and `test_freethreaded_gil.py` disabled-GIL probes.
- `test_postgres_benchmarks.py` (`postgres_18_service`, `VERTEBRA_POSTGRES_BENCH_DSN`, `VERTEBRA_POSTGRES_BENCH_RUN_ID`, `VERTEBRA_POSTGRES_BENCH_SCHEMA`) -> `python -m tools.benchmarks.postgres capture --implementation reference|native` -> `artifacts/postgres/{reference,native}.json` -> `python -m tools.benchmarks.postgres compare` -> `artifacts/postgres/comparison.json`.
- `Makefile` (`postgres-test`, `postgres-wheel-check`, `postgres-bench`, `postgres-bench-check`) -> canonical entry points consumed by CI and Chapter 31 review gates.

### 2.3 Edge-Case & Failure-Mode Matrix

| Boundary / Failure Mode | Deterministic Behavior |
| --- | --- |
| `vertebra._postgres` imported from checkout instead of installed wheel | `verify_postgres_wheel.py` asserts `Path(mod.__file__).is_relative_to(venv_site_packages)` from `cwd=tmp_outside_repo` with `PYTHONPATH` removed; fails immediately if checkout paths leak. |
| Default `make lint-all` runs without `--group bench` installed | `[tool.mypy].files` includes `"tools/benchmarks/postgres.py"` while `[[tool.mypy.overrides]]` sets `ignore_missing_imports = true` on `"asyncpg"`, `"asyncpg.*"`, `"psycopg"`, and `"psycopg.*"`, so both `mypy tools/benchmarks/postgres.py` (`strict = true`) and `make lint-all` without `--group bench` exit `0`. |
| Secret-bearing DSN or credentials in benchmark capture/exceptions | `tools.benchmarks.postgres` reads the DSN exclusively from the named `--dsn-env` variable, strips user/password/host query secrets from every `schema_version=1` field and error message, and `compare` rejects any report containing credential substrings or mismatched `paired_run_id` / `dataset_fingerprint` / `server_version_num`. |
| Sample noise `CV > 0.15` across the 5 sample-group means | Marks the capture invalid, permits at most one fresh whole paired (`reference` + `native`) retry (`retry_count == 1`), and fails closed if `CV > 0.15` persists; individual slow latencies are never dropped. |
| Benchmark comparison offline guarantee | `python -m tools.benchmarks.postgres compare` and `make postgres-bench-check` read only the JSON files on disk, require no `VERTEBRA_POSTGRES_BENCH_DSN`, and open zero sockets. |

### 2.4 Alternatives Comparison

| Decision | Chosen Seam | Rejected Alternative & Why Rejected |
| --- | --- | --- |
| Async pipeline benchmark reference | Use `psycopg.AsyncConnection.pipeline()` (`psycopg[binary]==3.2.13`) for async 64-statement pipeline comparison and `asyncpg==0.31.0` for async cold connect & single prepared query | Benchmarking native `pipeline()` against sequential `asyncpg` `fetch` loops — `asyncpg` does not expose transactional explicit `Sync` pipeline framing on its public Python cursor surface, producing an apples-to-oranges network RTT comparison. |
| Pooled reset vs warm prepared latency | Separate warm retained-lease prepared query (`5 x 1000`) from high-level transient pool `acquire + DISCARD ALL` diagnostic scenarios | Comparing a permanent native lease against per-call `DISCARD ALL` or vice versa — conflates connection-reset overhead with protocol codec throughput. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in this chapter]` (canonical `make postgres-test`, `make postgres-wheel-check`, `make postgres-bench`, and `make postgres-bench-check` commands, wheel installation guidance, and `--group bench` references are documented in [ch30-user-and-domain-docs](../ch30-user-and-domain-docs/spec.md) tasks `ch30:t01`–`ch30:t04`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test / Gate |
| --- | --- | --- | --- |
| R1, R6, R7, R26 | [t01](tasks/t01-wheel-isolation-and-ci-lanes.md) | `static_validation` | `tools/scripts/verify_postgres_wheel.py`, `src/py/tests/unit/test_freethreaded_gil.py`, `.github/workflows/ci.yml`, `.github/workflows/release.yaml` |
| R27 | [t02](tasks/t02-postgres-benchmark-harness.md) | `behavior_tdd` | `src/py/tests/unit/tools/test_postgres_benchmarks.py`, `tools/benchmarks/postgres.py`, `pyproject.toml` |
| R27 | [t03](tasks/t03-native-postgres-benchmarks.md) | `static_validation` | `src/rs/vertebra-postgres/benches/postgres_native.rs`, `src/py/tests/integration/db/test_postgres_benchmarks.py`, `Makefile` |

## 5. Implementation Plan

- [ ] native-postgres-driver:ch29:t01 — Verify outside-checkout PostgreSQL wheel isolation, release matrix, and strict CI/free-threaded lanes (tasks/t01-wheel-isolation-and-ci-lanes.md)
- [ ] native-postgres-driver:ch29:t02 — Implement reproducible PostgreSQL benchmark capture and offline threshold comparator (tasks/t02-postgres-benchmark-harness.md)
- [ ] native-postgres-driver:ch29:t03 — Wire Criterion native benchmarks, live PG18 paired orchestrator, and Makefile targets (tasks/t03-native-postgres-benchmarks.md)

**Chapter Recovery Checkpoint:** `uv lock --check && make lint-all && make postgres-wheel-check && make postgres-test && uv run --no-sync pytest src/py/tests/unit/tools/test_postgres_benchmarks.py -q && cargo bench --manifest-path src/rs/Cargo.toml -p vertebra-postgres --bench postgres_native --no-run && make postgres-bench && make postgres-bench-check && make review-check` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Plan Baseline**: `e8901de4`; `depends_on_chapters: [ch28-live-conformance-and-service-acceptance]`, `state: planned`, `plan_revision: 4`.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03` (`3/3` pending).
- **Active Invariants**: `psycopg` and `asyncpg` remain confined to `[dependency-groups] bench` with `[tool.mypy]` `ignore_missing_imports` overrides; `VERTEBRA_POSTGRES_BENCH_DSN` is never echoed or persisted in JSON artifacts; no hosted workflow dispatch, push, or tag mutation occurs during execution.

[^gh-ci]: `.github/workflows/ci.yml` — Continuous integration matrix for Python 3.12–3.15t, Rust, and service suites.
[^gh-release]: `.github/workflows/release.yaml` — Multi-platform wheel, sdist, and installation verification workflow.
[^ft-gil-test]: `src/py/tests/unit/test_freethreaded_gil.py::test_native_modules_declare_gil_used_false` — Free-threaded Python GIL declaration and concurrent extension probes.
[^bench-pkg]: `tools/benchmarks/__init__.py` — Vertebra Python benchmark harness package root.
[^root-makefile]: `Makefile` — Canonical workspace build, test, and benchmark orchestration targets.
