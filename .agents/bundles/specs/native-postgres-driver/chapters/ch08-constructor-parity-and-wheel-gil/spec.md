---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch08-constructor-parity-and-wheel-gil
title: "Chapter 08: Complete Constructor Parity, Wheel Packaging, and Free-Threaded GIL Verification"
description: Lock complete 26-parameter constructor parity across SyncPostgresClient and AsyncPostgresClient, wire root vertebra-core[postgres] packaging and review areas, exclude _postgres.pyi from all four companion wheels, and verify release.yaml schema plus isolated Python 3.14t/3.15t disabled-GIL imports.
status: stable
state: planned
planning_status: complete
depends_on_chapters:
  - ch07-async-bridge-and-client-sessions
replan_trigger: null
current_task: "native-postgres-driver:ch08:t01"
plan_revision: 6
plan_commit: 8833b064
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-07T16:55:00Z"
generated:
  by: flow-revise
  at: "2026-11-07T16:55:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-07T16:55:00Z"
    commit: dd432f18
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Frozen 26-parameter SyncPostgresClient and AsyncPostgresClient constructor specification
  - id: pg-driver-tests
    resource: src/py/tests/unit/db/test_postgres_driver.py
    title: Canonical native PostgreSQL constructor and session unit test suite
  - id: root-pyproject
    resource: pyproject.toml
    title: Root vertebra-core optional extras, uv.sources, and bumpversion rules
  - id: review-areas
    resource: .agents/skills/review-core/references/areas.toml
    title: Review area routing manifest for runtime-ipc and pyo3-bridge
  - id: spanner-wheel
    resource: src/rs/vertebra-spanner/pyproject.toml
    title: Companion Maturin wheel stub exclusion conventions
  - id: ft-gil-tests
    resource: src/py/tests/unit/test_freethreaded_gil.py
    title: Free-threaded Python Py_GIL_DISABLED extension verification suite
  - id: release-yaml
    resource: .github/workflows/release.yaml
    title: Multi-wheel release build, Windows MSVC toolchain, and installed-wheel smoke workflow
tags:
  - postgres
  - wheel
  - freethreaded
  - pyo3
  - ci
---

# Chapter 08: Complete Constructor Parity, Wheel Packaging, and Free-Threaded GIL Verification

<!-- truth: start -->
- **Complete 26-Parameter Constructor Parity (`t01`)**: `@final` `SyncPostgresClient` and `AsyncPostgresClient` expose identical keyword-only `inspect.signature` names, defaults, and `def __new__(cls, *, ...) -> SyncPostgresClient | AsyncPostgresClient` `.pyi` annotations (preserving `FREE_THREADED: bool`, `GIL_USED: bool`, and `__version__: str`), copy `Mapping[str, str]` snapshots purely without I/O, and drive `channel_binding="require"` and Unix `sslmode="disable"` from Python [^pg-contracts] [^pg-driver-tests].
- **Root `[postgres]` Extra, Lockfile & Review Areas (`t02`)**: Root `pyproject.toml` wires `postgres` and `all` extras, `[tool.uv.sources].vertebra-postgres`, and bumpversion rules with a clean `uv.lock`, while `.agents/skills/review-core/references/areas.toml` routes `src/rs/vertebra-postgres/**` and `src/py/vertebra/_postgres.pyi` to `runtime-ipc` and `pyo3-bridge` [^root-pyproject] [^review-areas].
- **Companion Wheel Stub Isolation (`t03` & `t04`)**: All four sibling wheels (`vertebra-bigquery`, `vertebra-bundler`, `vertebra-spanner`, `vertebra-storage`) exclude `"**/py/vertebra/_postgres.pyi"` in `[tool.maturin].exclude` so zero companion wheel archives leak PostgreSQL stubs [^spanner-wheel].
- **Isolated `3.14t`/`3.15t` Disabled-GIL & Release Parity (`t04`)**: `test_postgres_import_preserves_disabled_gil_in_child` proves `sys._is_gil_enabled() is False` before and after `import vertebra._postgres` in an isolated `-I` subprocess, and `.github/workflows/release.yaml` registers exact `name`/`path`/`module`/`exports` matrix keys plus Windows MSVC `cargo check` [^ft-gil-tests] [^release-yaml].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Chapters 01–07 establish the complete Phase 1 transport, authentication, cloud mTLS/IAM, Unix, keepalive, and sync/async PyO3 session stack. Before Phase 2–4 build query codecs, pools, pipelines, `COPY`, Arrow, notifications, and CDC on top of `vertebra._postgres`, Chapter 08 locks four Phase 1 invariants across bounded `<= 3`-file worksheets:
1. Exact runtime `inspect.signature`, `@final` `def __new__` `mypy.stubtest` parity (preserving `GIL_USED: bool` and `FREE_THREADED: bool`), pure zero-I/O construction, mapping copy isolation, `TypeError` vs `ValueError` separation, secret redaction, and Python-level option reachability across all 26 constructor parameters on both `SyncPostgresClient` and `AsyncPostgresClient` [^pg-contracts] [^pg-driver-tests].
2. Root `pyproject.toml` `[postgres]` optional extra + `all` + `[tool.uv.sources]` + `[[tool.bumpversion.files]]` synchronized with `uv.lock`, alongside `runtime-ipc` and `pyo3-bridge` path registration in `.agents/skills/review-core/references/areas.toml` [^root-pyproject] [^review-areas].
3. Explicit `"**/py/vertebra/_postgres.pyi"` exclusions across all four companion Maturin wheels (`vertebra-bigquery`, `vertebra-bundler`, `vertebra-spanner`, and `vertebra-storage`) so `python-source = "../../py"` never bundles `_postgres.pyi` into sibling wheels [^spanner-wheel].
4. Isolated-subprocess free-threaded `3.14t`/`3.15t` disabled-GIL import verification (`test_postgres_import_preserves_disabled_gil_in_child`), standalone `vertebra-core[postgres]` wheel installation + `stubtest` outside the checkout, and `.github/workflows/release.yaml` `name`/`path`/`module`/`exports` + Windows MSVC `cargo check` registration [^ft-gil-tests] [^release-yaml].

### 1.2 Non-Goals

- Final cutover wheel exports (`SyncPostgresPool`, `AsyncPostgresPool`, `PostgresQueryResult`, notification/replication handles) and `tools/scripts/verify_postgres_wheel.py` core-only uninstall acceptance (owned by `ch29:t01` after full driver cutover in Chapter 27).
- Query execution and lossless PostgreSQL value codecs (owned by Chapters 09–12).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md) and [contracts.md](../../contracts.md). Skills: [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md) and [python-framework](../../../../../skills/python-framework/SKILL.md). Knowledge: [database-drivers-and-dialers.md](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [adr-004-optional-driver-wheels-and-shm-ownership.md](../../../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md), [ffi-and-gil-discipline.md](../../../../knowledge/patterns/rust/ffi-and-gil-discipline.md), and [pyo3-and-python-runtime.md](../../../../knowledge/gotchas/pyo3-and-python-runtime.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R2, R6, R7 (`ch08:t01`)**: Align runtime `__text_signature__` defaults, `Mapping[str, str]` snapshot extraction, and `_postgres.pyi` `@final` `def __new__(cls, *, ...) -> SyncPostgresClient | AsyncPostgresClient` Google-style docstrings (preserving `FREE_THREADED: bool`, `GIL_USED: bool`, and `__version__: str`) across `src/rs/vertebra-postgres/src/sync_client.rs`, `src/rs/vertebra-postgres/src/async_client.rs`, and `src/py/vertebra/_postgres.pyi`, and characterize all 26 constructor parameters in `src/py/tests/unit/db/test_postgres_driver.py` (`native_constructor`).
- **R1 (`ch08:t02`)**: Wire `postgres` optional extra, `all` inclusion, `[tool.uv.sources].vertebra-postgres`, and bumpversion rules in root `pyproject.toml`, update `uv.lock`, and register `src/rs/vertebra-postgres/**` and `src/py/vertebra/_postgres.pyi` in `.agents/skills/review-core/references/areas.toml`, verifying `uv lock --check` and `make review-check`.
- **R1 (`ch08:t03`)**: Add `"**/py/vertebra/_postgres.pyi"` to `[tool.maturin].exclude` in `src/rs/vertebra-bigquery/pyproject.toml`, `src/rs/vertebra-bundler/pyproject.toml`, and `src/rs/vertebra-spanner/pyproject.toml`, verifying zero `_postgres.pyi` leakage in built wheel archives.
- **R1, R6, R7, R26 (`ch08:t04`)**: Add `"**/py/vertebra/_postgres.pyi"` to `src/rs/vertebra-storage/pyproject.toml`, register `postgres` (`name: postgres`, `path: src/rs/vertebra-postgres`, `module: vertebra._postgres`, `exports: SyncPostgresClient AsyncPostgresClient`) plus Windows MSVC `cargo check -p vertebra-db -p vertebra-postgres --all-targets`, `build_sdist`, and `test_wheels` in `.github/workflows/release.yaml`, preserve `GIL_USED: bool` + `FREE_THREADED: bool` stubtest parity, and add `test_postgres_import_preserves_disabled_gil_in_child` in `src/py/tests/unit/test_freethreaded_gil.py` verified across `3.14t`, `3.15t`, `3.12`, and a standalone `/tmp` wheel venv.

### 2.2 Concrete Type & Signature Contracts

- **Caller -> Seam -> Collaborator Map**:
  - `inspect.signature(SyncPostgresClient)` / `inspect.signature(AsyncPostgresClient)` -> PyO3 `#[pyo3(signature = (*, ...))]` `__text_signature__` & `src/py/vertebra/_postgres.pyi` (`@final` `def __new__(cls, *, ...)`) -> verified identically by `mypy.stubtest vertebra._postgres`.
  - `vertebra-core[postgres]` (`pyproject.toml` + `uv.lock`) -> `src/rs/vertebra-postgres` (`vertebra._postgres`) with disjoint `_*.pyi` exclusions across all five optional wheel `pyproject.toml` manifests.
  - `subprocess.run([sys.executable, "-I", "-c", ...])` -> `import vertebra._postgres` -> `#[pymodule(gil_used = false)]` keeps `sys._is_gil_enabled() is False` on `3.14t` and `3.15t`.
- **Exact 26-Parameter Keyword-Only Signature (`SyncPostgresClient` & `AsyncPostgresClient` in `_postgres.pyi`)**:
  - `def __new__(cls, *, user: str, database: str, host: str = "localhost", port: int = 5432, password: str | None = None, unix_socket: str | None = None, sslmode: str = "verify-full", ca_pem: bytes | None = None, client_cert_pem: bytes | None = None, client_key_pem: bytes | None = None, server_name: str | None = None, channel_binding: str = "prefer", connect_timeout: float = 10.0, application_name: str = "vertebra", session_gucs: Mapping[str, str] | None = None, keepalives: bool = True, keepalives_idle: int | None = None, keepalives_interval: int | None = None, keepalives_count: int | None = None, cloud_sql_instance: str | None = None, cloud_sql_ip_type: str = "PRIVATE", alloydb_instance_uri: str | None = None, alloydb_ip_type: str = "PRIVATE", enable_iam_auth: bool = False, cloud_auth_options: Mapping[str, str] | None = None, iam_auth_options: Mapping[str, str] | None = None) -> SyncPostgresClient` (and `-> AsyncPostgresClient`), alongside module-level `__version__: str`, `FREE_THREADED: bool`, and `GIL_USED: bool`.
- **CI Release Matrix (`.github/workflows/release.yaml`)**:
  - `jobs.build_wheels.strategy.matrix.package` entry uses exact live keys `name: postgres`, `path: src/rs/vertebra-postgres`, `module: vertebra._postgres`, `exports: SyncPostgresClient AsyncPostgresClient`, and executes `cargo check --manifest-path src/rs/Cargo.toml -p vertebra-db -p vertebra-postgres --all-targets` on `runner.os == 'Windows' && matrix.package.name == 'postgres'` prior to `cibuildwheel`.

### 2.3 Edge-Case & Failure-Mode Matrix

| Condition | Behavior & Error Mapping |
| --- | --- |
| Positional constructor argument `SyncPostgresClient("u", "d")` or unknown keyword `foo=1` | Raises `TypeError` immediately at PyO3 signature binding. |
| Non-`str` key or value in `session_gucs`, `cloud_auth_options`, or `iam_auth_options` (`{"k": 1}`, `{1: "v"}`), or `bool` passed for integer `port` / `keepalives_*` | Raises `TypeError` before semantic validation (Python `bool` is an `int` subclass and is explicitly rejected on integer fields). |
| Mutating caller `gucs` / `iam_auth_options` dict after `SyncPostgresClient(..., session_gucs=gucs)` construction | Subsequent `connect()` sends the original snapshot captured at construction time; construction performs `0` DNS, socket, or OAuth I/O. |
| Building any companion wheel (`bigquery`, `bundler`, `spanner`, `storage`) with `python-source = "../../py"` | `[tool.maturin].exclude` omits `**/py/vertebra/_postgres.pyi`, preventing duplicate `.pyi` file collisions when multiple optional wheels are installed together. |
| Importing `vertebra._postgres` inside `python -I` on `3.14t` / `3.15t` if any PyO3 type omits `gil_used = false` / `immutable_type` | Re-enables the GIL (`sys._is_gil_enabled() == True`) and fails `test_postgres_import_preserves_disabled_gil_in_child`. |

### 2.4 Alternatives Comparison

| Option | Decision & Rationale |
| --- | --- |
| Bundle `pyproject.toml`, `uv.lock`, `areas.toml`, and 4 sibling `pyproject.toml` files into one task vs splitting across `t02`–`t04` | **Chosen: Split across `t02`–`t04` (`<= 3` files each)**. Keeps `uv.lock` paired atomically with root `pyproject.toml` in `t02` and keeps every task strictly within the `<= 3`-file worksheet bound. |
| Gate free-threaded child test on `sys._is_gil_enabled()` in parent vs `sysconfig.get_config_var("Py_GIL_DISABLED")` | **Chosen: `sysconfig.get_config_var("Py_GIL_DISABLED")`**. Prevents another extension in the parent pytest process from masking a GIL re-enablement regression by causing a silent skip [^ft-gil-tests]. |
| Cross-compile Windows `cargo check --target x86_64-pc-windows-msvc` on Linux vs native Windows runner in `release.yaml` | **Chosen: Native Windows runner in `release.yaml`**. Uses the existing MSVC + Rust toolchain without local Windows SDK cross-compilation assumptions [^release-yaml]. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[1/1 stub file finalized for Phase 1 in ch08]` (`src/py/vertebra/_postgres.pyi` complete Google-style docstrings and `@final` `def __new__` signatures verified by `mypy.stubtest` both in-tree and from an installed `/tmp` wheel outside the repository checkout; user guides and OKF chapters update in Chapter 30).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test / Gate |
| --- | --- | --- | --- |
| R2, R6, R7 | [t01](tasks/t01-constructor-parity.md) | `characterization` | `src/py/tests/unit/db/test_postgres_driver.py` |
| R1 | [t02](tasks/t02-root-extra-and-review-areas.md) | `static_validation` | `uv lock --check`, `make review-check` |
| R1 | [t03](tasks/t03-sibling-wheel-stub-exclusions.md) | `static_validation` | `uv build` + `zipfile` stub-leakage gate (`bigquery`, `bundler`, `spanner`) |
| R1, R6, R7, R26 | [t04](tasks/t04-ffi-wheel-and-freethreaded-gil.md) | `regression_tdd` | `src/py/tests/unit/test_freethreaded_gil.py`, `.github/workflows/release.yaml` |

## 5. Implementation Plan

- [ ] native-postgres-driver:ch08:t01 — Characterize and lock complete sync and async constructor parity (tasks/t01-constructor-parity.md)
- [ ] native-postgres-driver:ch08:t02 — Register root postgres extra, uv.lock, and review area paths (tasks/t02-root-extra-and-review-areas.md)
- [ ] native-postgres-driver:ch08:t03 — Exclude _postgres.pyi from bigquery, bundler, and spanner wheels (tasks/t03-sibling-wheel-stub-exclusions.md)
- [ ] native-postgres-driver:ch08:t04 — Exclude _postgres.pyi from storage wheel, wire release.yaml, and verify isolated free-threaded imports (tasks/t04-ffi-wheel-and-freethreaded-gil.md)

**Chapter Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres && make dev && uv lock --check && make review-check && uv run --no-sync python -m mypy.stubtest vertebra._postgres && uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_driver.py -k native_ -q && uv run --no-sync python -m pytest src/py/tests/unit/test_freethreaded_gil.py -q` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `HEAD @ dd432f18`; chapter `state: planned`, `planning_status: complete`.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03`, `t04` (`4/4` pending).
- **Downstream Dependents**: Completes Phase 1 transport, cloud, auth, packaging, and session foundations and unblocks `ch09-owned-results-and-numeric-codecs` (`ch09:t01` depends on `ch08:t04`).

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Frozen 26-parameter constructor and FFI ownership specification.
[^pg-driver-tests]: `src/py/tests/unit/db/test_postgres_driver.py` — Canonical native PostgreSQL client unit and characterization tests.
[^root-pyproject]: `pyproject.toml` — Root optional extras, `uv.sources`, and `bumpversion` configuration.
[^review-areas]: `.agents/skills/review-core/references/areas.toml` — Review area routing manifest.
[^spanner-wheel]: `src/rs/vertebra-spanner/pyproject.toml` — Companion Maturin wheel `.pyi` exclusion rules.
[^ft-gil-tests]: `src/py/tests/unit/test_freethreaded_gil.py` — Free-threaded Python `Py_GIL_DISABLED` verification suite.
[^release-yaml]: `.github/workflows/release.yaml` — Wheel build, Windows MSVC toolchain, and multi-wheel smoke workflow.
