---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch29-wheel-ci-and-benchmarks
title: "Chapter 29: Installed Wheel Isolation, Strict CI Matrix, and Reproducible Benchmarks"
description: Verify outside-checkout vertebra-postgres two-phase wheel isolation, release/CI zero-skip service and free-threaded lanes, Python 3.12 postgres-bench comparator gates, and paired PG18 reference vs native measurements.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch28-live-conformance-and-service-acceptance
replan_trigger: null
current_task: "native-postgres-driver:ch29:t01"
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
  - id: gh-ci
    resource: .github/workflows/ci.yml
    title: Continuous integration matrix for Python 3.12-3.15t, Rust, and zero-skip PostgreSQL service suites
  - id: gh-release
    resource: .github/workflows/release.yaml
    title: Multi-platform wheel, sdist, and combined-installation verification workflow
  - id: ft-gil-test
    resource: src/py/tests/unit/test_freethreaded_gil.py::test_native_modules_declare_gil_used_false
    title: Free-threaded Python GIL declaration, immutable pyclass, and concurrent extension probes
  - id: root-pyproject
    resource: pyproject.toml
    title: Dedicated postgres-bench dependency group, bumpversion rules, pytest markers, and Mypy targets
  - id: bench-pkg
    resource: tools/benchmarks/__init__.py
    title: Vertebra Python benchmark harness package root
  - id: root-makefile
    resource: Makefile
    title: Canonical workspace lint, wheel, PostgreSQL live test, and self-contained benchmark targets
tags:
  - postgres
  - wheel
  - ci
  - benchmarks
  - freethreaded
---

# Chapter 29: Installed Wheel Isolation, Strict CI Matrix, and Reproducible Benchmarks

<!-- truth: start -->
- **Two-Probe Outside-Checkout Wheel & Namespace Isolation**: `tools/scripts/verify_postgres_wheel.py` executes outside the checkout (`PYTHONPATH` unset, zero checkout paths on `sys.path`) with (a) a strict `--no-deps --no-index --find-links` empty-venv probe verifying compiled `vertebra._*` imports, `site-packages` `__file__` origins, `.pyi` stubs, and live raw `connect()` + `begin()`/`savepoint()`/`commit()`/`rollback()` + `stream_arrow_batches("SELECT 42")` (`vertebra._vertebra.ArrowCursor`, without importing `msgspec`), followed by cached-wheel `msgspec` verification of `select_to_arrow("SELECT 42")` (`PostgresArrowResult`) alongside `query("SELECT 42")` + `transaction()`, and (b) an offline runtime-deps isolated venv probe testing live `SyncPostgreSQLDriver`/`AsyncPostgreSQLDriver` before `pip uninstall -y vertebra-postgres` and verifying `MissingDependencyError` naming `vertebra-core[postgres]` afterward [^gh-release] [^ft-gil-test].
- **Zero-Skip CI & Free-Threaded Safety**: `.github/workflows/release.yaml` smoke-verifies all final `vertebra._postgres` C-extension exports across `cp312`–`cp315` and `cp314t`/`cp315t` (Linux `x86_64`/`aarch64`, macOS universal, Windows), `.github/workflows/ci.yml` sets `VERTEBRA_CONFORMANCE_DOCKER=1` and `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1` on every live PostgreSQL job, and `test_freethreaded_gil.py` enforces `mod.FREE_THREADED is _is_free_threaded_runtime()`, immutable `#[pyclass]` flags across all final `vertebra._postgres` classes, and `sys._is_gil_enabled() is False` before and after concurrent native pool queries on `3.14t`/`3.15t` [^gh-ci] [^gh-release] [^ft-gil-test].
- **Universal-Lock `postgres-bench` & In-Place `Makefile` Gates**: `[dependency-groups].postgres-bench` isolates `psycopg[binary]==3.2.13; python_version == '3.12'` and `asyncpg==0.31.0; python_version == '3.12'` (preserving universal `uv lock` across `3.12`–`3.15t` and `make bench-ft`), `[[tool.bumpversion.files]]` already tracks `vertebra-postgres`, `tools/benchmarks/postgres.py` (`schema_version=1`, `gc`-safe `msgspec.Struct` models) and Criterion `postgres_native.rs` enforce `median <= 1.25x`, `p95 <= 1.50x`, `statements_s >= 0.80x`, and `CV <= 0.15`, and `Makefile` updates `postgres-test`/`postgres-wheel-check`/`lint`/`lint-all` in place while adding self-contained `postgres-bench` and offline `postgres-bench-check` [^root-pyproject] [^bench-pkg] [^root-makefile].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Live server conformance (`ch28`) runs inside an editable checkout, which cannot detect broken wheel/sdist metadata, checkout-path shadowing on `sys.path`, partial namespace corruption on `vertebra-postgres` uninstall, unguarded free-threaded `Py_TPFLAGS_IMMUTABLETYPE` or GIL regressions, universal lockfile breakage on `3.14t`/`3.15t` from comparator C extensions, or protocol throughput regressions [^gh-ci] [^gh-release] [^root-pyproject] [^bench-pkg]. At `c8722c76`, `[project.optional-dependencies].postgres`, the crate-local `src/rs/vertebra-postgres/pyproject.toml`, the `release.yaml` `postgres` build-matrix entry with its `SyncPostgresClient`/`AsyncPostgresClient` smoke tuple, both `vertebra-postgres` `[[tool.bumpversion.files]]` entries, and the archive-only `Makefile` `postgres-test` / `postgres-wheel-check` recipes are on `main`; `ci.yml` has no PostgreSQL service job.

Chapter 29 closes packaging, CI, free-threaded runtime, and benchmark verification across three sequential tasks:
1. Create `tools/scripts/verify_postgres_wheel.py` with both strict `--no-deps` and cached runtime-deps outside-checkout venv probes, expand `test_freethreaded_gil.py` (`_is_free_threaded_runtime()`, all final immutable `#[pyclass]` types, concurrent `3.14t`/`3.15t` pool queries), and finalize `release.yaml` and zero-skip `ci.yml` PostgreSQL jobs (`ch29:t01`) [^gh-ci] [^gh-release] [^ft-gil-test].
2. Add marker-scoped `[dependency-groups].postgres-bench` (`python_version == '3.12'`), verify the existing `vertebra-postgres` `[[tool.bumpversion.files]]` entries, configure the `postgres_benchmark` pytest marker and `[tool.mypy]` targets/overrides in `pyproject.toml` + `uv.lock`, and implement domain-structured `tools/benchmarks/postgres.py` (`capture` and offline `compare`) with parameterized unit tests (`ch29:t02`) [^root-pyproject] [^bench-pkg].
3. Wire Criterion `src/rs/vertebra-postgres/benches/postgres_native.rs`, implement the `@pytest.mark.postgres_benchmark` PG18 orchestrator `src/py/tests/integration/db/test_postgres_benchmarks.py`, update `Makefile` `postgres-test`, `postgres-wheel-check`, `lint`, and `lint-all` in place, and add self-contained `postgres-bench` plus offline `postgres-bench-check` (`ch29:t03`) [^root-makefile].

### 1.2 Non-Goals

- Publishing wheels, pushing commits, dispatching GitHub Actions workflows, or creating/modifying Git tags during local execution.
- Touching `src/py/vertebra/build/packaging/` or `tools/pgo/` (`tools/pgo/rust_pgo.py::discover_extension_manifests` already profiles `vertebra-postgres` through its crate-local `pyproject.toml`; wheel builds flow through `pyproject.toml`, `src/rs/vertebra-postgres/pyproject.toml`, and `release.yaml`).
- Adding `psycopg` or `asyncpg` to `[project.dependencies]`, `[project.optional-dependencies]`, or `[dependency-groups].bench` (where `make bench-ft` runs on `3.14t`/`3.15t`).
- Benchmarking Arrow read throughput against a tuple-materializing Python driver as a pass/fail gate (the `100_000`-row `VTBARROW` read runs as a typed schema, row-count, checksum, and `/dev/shm` cleanup diagnostic).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md); frozen contracts: [contracts.md](../../contracts.md); depends on [ch28-live-conformance-and-service-acceptance](../ch28-live-conformance-and-service-acceptance/spec.md). Skills: [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md), [python-framework](../../../../../skills/python-framework/SKILL.md), and [review-performance](../../../../../skills/review-performance/SKILL.md). Knowledge: [database-drivers-and-dialers.md](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [benchmarks-and-free-threaded.md](../../../../knowledge/testing/benchmarks-and-free-threaded.md), and [adr-004-optional-driver-wheels-and-shm-ownership.md](../../../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md). The PyO3 extension inventory is read at execution time from the `Makefile` `lint` stubtest list and `make build` `maturin develop` lines; this chapter never hardcodes an extension count.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R1, R6, R7, R26 (`ch29:t01`)**: Create `tools/scripts/verify_postgres_wheel.py` executing sdist/wheel archive validation plus two isolated outside-checkout venv probes (`PYTHONPATH` unset, `python -I`, zero checkout paths on `sys.path`): (a) strict `--no-deps --no-index --find-links` empty-venv probe verifying compiled `vertebra._*` imports, `site-packages` `__file__`, `.pyi` stubs, and zero-third-party live raw `SyncPostgresClient`/`AsyncPostgresClient`/`SyncPostgresPool` `connect()` + `begin()`/`savepoint()`/`commit()`/`rollback()` + `stream_arrow_batches("SELECT 42")` (returning `vertebra._vertebra.ArrowCursor` without importing `msgspec`, `sslmode="disable"`), followed by cached-wheel `msgspec` verification of `select_to_arrow("SELECT 42")` (returning `PostgresArrowResult`, a `msgspec.Struct`) alongside `query("SELECT 42")` + `transaction()`, and (b) cached runtime-deps isolated venv probe testing live `SyncPostgreSQLDriver`/`AsyncPostgreSQLDriver` `SELECT` + `select_to_arrow("SELECT 42")` + `transaction()` before `pip uninstall -y vertebra-postgres` and verifying core/sibling imports plus `MissingDependencyError` naming `vertebra-core[postgres]` afterward; expand `test_freethreaded_gil.py` (`mod.FREE_THREADED is _is_free_threaded_runtime()`, immutable `#[pyclass]` checks on every final `vertebra._postgres` class, disabled-GIL concurrent native pool queries on `3.14t`/`3.15t`); finalize `release.yaml` (`cp312`–`cp315`, `cp314t`, `cp315t` across Linux `x86_64`/`aarch64`, macOS universal, Windows) and add `ci.yml` live PG `14`–`18` and feature jobs with `VERTEBRA_CONFORMANCE_DOCKER=1` and `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1`.
- **R27.1 (`ch29:t02`)**: Add dedicated `[dependency-groups].postgres-bench` (`"psycopg[binary]==3.2.13; python_version == '3.12'"`, `"asyncpg==0.31.0; python_version == '3.12'"`), verify the existing `vertebra-postgres=={current_version}` and `src/rs/vertebra-postgres/pyproject.toml` `[[tool.bumpversion.files]]` entries with a dry-run bump, register the `postgres_benchmark` pytest marker excluded by default `addopts`, and append `"tools/benchmarks/postgres.py"` to `[tool.mypy].files` plus `"asyncpg"`, `"asyncpg.*"`, `"psycopg"`, `"psycopg.*"` to `[[tool.mypy.overrides]]` in `pyproject.toml` + `uv.lock`; implement `python -m tools.benchmarks.postgres capture|compare` using scalar/tuple `gc=False` leaf structs (`ScenarioCapture`, `ScenarioComparison`) and GC-tracked `dict`/container report structs (`PostgresBenchmarkReport`, `PostgresComparisonReport` omitting `gc=False`), top-level `if TYPE_CHECKING:` + `ensure_module`, `vertebra.utils.serializers.{decode_json, encode_json, format_json}`, `sys.stdout.write`/`sys.stderr.write`, and `@pytest.mark.parametrize` unit tests.
- **R25, R26, R27.2 (`ch29:t03`)**: Add `[[bench]] name = "postgres_native" harness = false` in `src/rs/vertebra-postgres/Cargo.toml` and Criterion `benches/postgres_native.rs`, implement `@pytest.mark.postgres_benchmark` live PG18 paired orchestrator `src/py/tests/integration/db/test_postgres_benchmarks.py` (`100_000`-row `bench_values`, deterministic checksum, paired `reference`/`native` capture + `compare`, `VTBARROW` diagnostic + `/dev/shm` check), update existing `Makefile` `postgres-test`, `postgres-wheel-check`, `lint`, and `lint-all` targets in place (zero duplicate `.PHONY`/recipes), and add self-contained `postgres-bench` (`--group postgres-bench` + release build of every PyO3 extension that `make build` builds, with `UV_PYTHON=3.12 UV_NO_SYNC=1`) plus offline `postgres-bench-check`.

### 2.2 CLI, Schema & Collaborator Map

- `Makefile::postgres-wheel-check` -> `tools/scripts/verify_postgres_wheel.py` -> (a) `--no-deps --no-index --find-links` empty venv + (b) cached-runtime-wheels isolated venv (`SyncPostgreSQLDriver`/`AsyncPostgreSQLDriver` + `pip uninstall -y vertebra-postgres`).
- `Makefile::postgres-bench` (`--group postgres-bench`, release `UV_NO_SYNC=1` build of every `make build` extension) -> `test_postgres_benchmarks.py` (`postgres_18_service`, `VERTEBRA_POSTGRES_BENCH_DSN`, `VERTEBRA_POSTGRES_BENCH_RUN_ID`, `VERTEBRA_POSTGRES_BENCH_SCHEMA`) -> `python -m tools.benchmarks.postgres capture --implementation reference|native` -> `Makefile::postgres-bench-check` (`compare` offline).
- `Makefile::postgres-test` (updated in place) -> `make dev` + `cargo test -p vertebra-postgres --locked` + `mypy.stubtest vertebra._postgres` + live zero-skip pytest across all five `ch28` PostgreSQL suites.

### 2.3 Edge-Case & Failure-Mode Matrix

| Boundary / Failure Mode | Deterministic Behavior |
| --- | --- |
| `FREE_THREADED` inspected on standard GIL Python `3.12`–`3.15` vs `3.14t`/`3.15t` | `vertebra._postgres.FREE_THREADED` equals `bool(sysconfig.get_config_var("Py_GIL_DISABLED"))` (`False` on GIL builds, `True` on free-threaded builds), while `GIL_USED` is `False` on every build and `sys._is_gil_enabled()` stays `False` before and after concurrent `3.14t`/`3.15t` pool queries. |
| Checkout path leak or `types.py` `msgspec` dependency during wheel verification | Both subprocess venv probes run from an outside-repo temp `cwd` with `PYTHONPATH` stripped under `python -I` and assert `Path(mod.__file__).is_relative_to(site_packages)`; probe (a) verifies pure `--no-deps` C-extension imports/stubs plus raw `connect()` + `begin()`/`savepoint()`/`commit()`/`rollback()` + `stream_arrow_batches("SELECT 42")` (`vertebra._vertebra.ArrowCursor`, zero `msgspec` import), then stages cached `msgspec` to verify raw `select_to_arrow("SELECT 42")` (`PostgresArrowResult`) alongside `query("SELECT 42")` + `transaction()`, and probe (b) installs cached runtime wheels to test `SyncPostgreSQLDriver`/`AsyncPostgreSQLDriver` (`SELECT`, `select_to_arrow`, `transaction`) and `vertebra-postgres` uninstall isolation without network access. |
| Universal `uv lock` across `3.12`–`3.15t`, `make lint-all`, and `make bench-ft` | Comparator packages live exclusively in `[dependency-groups].postgres-bench` gated by `; python_version == '3.12'`; `make bench-ft`, `uv lock --check`, and `make lint-all` (including `tools/benchmarks/postgres.py` in `ruff`/`mypy`) exit `0` without `postgres-bench` installed. |
| `dict`-bearing `PostgresBenchmarkReport` GC safety & noisy samples (`CV > 0.15`) | `PostgresBenchmarkReport` and `PostgresComparisonReport` omit `gc=False` so GC tracks container fields; `CV > 0.15` across the 5 per-sample mean latencies permits at most one fresh whole paired (`reference` + `native`) retry (`retry_count <= 1`) before failing closed without dropping slow samples or leaking DSN secrets. |

### 2.4 Alternatives Comparison

| Decision | Chosen Seam | Rejected Alternative & Why Rejected |
| --- | --- | --- |
| Comparator dependency group | Dedicated `[dependency-groups].postgres-bench` with `; python_version == '3.12'` markers and self-contained `UV_NO_SYNC=1` `postgres-bench` build | Adding unguarded `psycopg[binary]==3.2.13` and `asyncpg==0.31.0` to `[dependency-groups].bench` — breaks `make bench-ft` and multi-version `3.14t`/`3.15`/`3.15t` resolution where binary wheels do not exist, and gets pruned if `make build` runs plain `uv run`. |
| Async pipeline benchmark reference | `psycopg.AsyncConnection.pipeline()` (`3.2.13`) for 64-statement single-`Sync` pipeline comparison and `asyncpg` (`0.31.0`) for async cold connect & single prepared query | Comparing native single-`Sync` `pipeline()` against sequential `asyncpg` per-statement round trips — `asyncpg` exposes no transactional single-`Sync` cursor framing on its public Python API. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in ch29]` — hands off to [ch30-user-and-workflow-docs](../ch30-user-and-workflow-docs/spec.md), where `ch30`–`ch32` synchronize `[0/33 files documented]` across user guides, `Makefile`/`workflow.md` (`postgres-test`, `postgres-wheel-check`, `postgres-bench`, `postgres-bench-check`, `--group postgres-bench`), nested OKF chapters, and review manifests ahead of `ch33-final-review-and-release-gate`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test / Gate |
| --- | --- | --- | --- |
| R1, R6, R7, R26 | [t01](tasks/t01-wheel-isolation-and-ci-lanes.md) | `static_validation` | `tools/scripts/verify_postgres_wheel.py`, `src/py/tests/unit/test_freethreaded_gil.py`, `.github/workflows/ci.yml`, `.github/workflows/release.yaml` |
| R27 | [t02](tasks/t02-postgres-benchmark-harness.md) | `behavior_tdd` | `src/py/tests/unit/tools/test_postgres_benchmarks.py`, `tools/benchmarks/postgres.py`, `pyproject.toml`, `uv.lock` |
| R25, R26, R27 | [t03](tasks/t03-native-postgres-benchmarks.md) | `static_validation` | `src/rs/vertebra-postgres/benches/postgres_native.rs`, `src/py/tests/integration/db/test_postgres_benchmarks.py`, `Makefile` |

## 5. Implementation Plan

- [ ] native-postgres-driver:ch29:t01 — Verify outside-checkout PostgreSQL wheel isolation, release matrix, and strict CI/free-threaded lanes (tasks/t01-wheel-isolation-and-ci-lanes.md)
- [ ] native-postgres-driver:ch29:t02 — Implement reproducible PostgreSQL benchmark capture and offline threshold comparator (tasks/t02-postgres-benchmark-harness.md)
- [ ] native-postgres-driver:ch29:t03 — Wire Criterion native benchmarks, live PG18 paired orchestrator, and Makefile targets (tasks/t03-native-postgres-benchmarks.md)

**Chapter Recovery Checkpoint:** `uv lock --check && make lint-all && make postgres-wheel-check && make postgres-test && uv run --no-sync pytest src/py/tests/unit/test_freethreaded_gil.py src/py/tests/unit/tools/test_postgres_benchmarks.py -q && cargo bench --manifest-path src/rs/Cargo.toml -p vertebra-postgres --bench postgres_native --no-run && make postgres-bench && make postgres-bench-check && make review-check` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Plan Baseline**: `plan_commit: 5eb75b2a`, `plan_revision: 8`; chapter `state: planned`, `planning_status: complete`. The flow spans `36` chapters / `129` tasks; `ch01`–`ch21` plus `ch22:t01`–`t02` (`80` tasks) landed on `main` in `c8722c76` ([#104](https://github.com/cofin/vertebra/pull/104)), and the remaining chapters ship in a follow-up pull request.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03` (`3/3` pending).
- **Downstream Handoff & Invariants**: Unblocks `ch30-user-and-workflow-docs` (`[0/33 files documented]` across `ch30`–`ch32`, followed by `ch33-final-review-and-release-gate`); `psycopg` and `asyncpg` stay confined to `[dependency-groups].postgres-bench` (`python_version == '3.12'`); `src/py/vertebra/build/packaging/` and `tools/pgo/` stay untouched; `VERTEBRA_POSTGRES_BENCH_DSN` is never persisted in JSON artifacts; zero workflow dispatch, push, or tag mutation occurs locally.

[^gh-ci]: `.github/workflows/ci.yml` — Continuous integration matrix for Python 3.12–3.15t, Rust, and zero-skip PostgreSQL service suites.
[^gh-release]: `.github/workflows/release.yaml` — Multi-platform wheel, sdist, and combined-installation verification workflow.
[^ft-gil-test]: `src/py/tests/unit/test_freethreaded_gil.py::test_native_modules_declare_gil_used_false` — Free-threaded Python GIL declaration, immutable pyclass, and concurrent extension probes.
[^root-pyproject]: `pyproject.toml` — Dedicated postgres-bench dependency group, bumpversion rules, pytest markers, and Mypy targets.
[^bench-pkg]: `tools/benchmarks/__init__.py` — Vertebra Python benchmark harness package root.
[^root-makefile]: `Makefile` — Canonical workspace lint, wheel, PostgreSQL live test, and self-contained benchmark targets.
