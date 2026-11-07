---
type: Task
id: "native-postgres-driver:ch29:t01"
chapter_id: ch29-wheel-ci-and-benchmarks
title: Verify outside-checkout PostgreSQL wheel isolation, release matrix, and strict CI/free-threaded lanes
description: Create tools/scripts/verify_postgres_wheel.py for final client/pool/live-service/uninstall verification, extend release.yaml and ci.yml across all platforms and PG14-18/feature lanes, and add concurrent free-threaded 3.14t/3.15t query/pool smoke tests.
status: stable
state: pending
priority: P1
verification_strategy: static_validation
depends_on:
  - "native-postgres-driver:ch28:t04"
read_set:
  - .github/workflows/ci.yml
  - .github/workflows/release.yaml
  - src/py/tests/unit/test_freethreaded_gil.py
  - src/py/vertebra/db/adapters/postgres/config.py
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - .github/workflows/ci.yml
  - .github/workflows/release.yaml
  - tools/scripts/verify_postgres_wheel.py
tests:
  - src/py/tests/unit/test_freethreaded_gil.py
estimated_loc: 230
plan_revision: 6
plan_commit: 8833b064
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-07T16:45:00Z"
commit: null
sources:
  - id: gh-ci
    resource: .github/workflows/ci.yml
    title: Continuous integration matrix for Python 3.12-3.15t and database services
  - id: gh-release
    resource: .github/workflows/release.yaml
    title: Multi-platform wheel and sdist build/smoke matrix
  - id: ft-gil-test
    resource: src/py/tests/unit/test_freethreaded_gil.py::test_native_modules_declare_gil_used_false
    title: Free-threaded GIL state and concurrent native extension tests
  - id: pg-config
    resource: src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig
    title: PostgreSQL adapter configuration and MissingDependencyError seam
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Public client, pool, result, and wheel isolation contracts
tags:
  - postgres
  - wheel
  - ci
  - release
  - freethreaded
---

# Task t01: Verify Outside-Checkout PostgreSQL Wheel Isolation, Release Matrix, and Strict CI/Free-Threaded Lanes

## Objective & Scope Boundary

Create `tools/scripts/verify_postgres_wheel.py` and finalize release wheel coverage, strict CI PostgreSQL service lanes, and free-threaded concurrency checks across `.github/workflows/ci.yml`, `.github/workflows/release.yaml`, and `src/py/tests/unit/test_freethreaded_gil.py` (`ch08:t02`–`t04` already wired the root `[postgres]` extra, all four sibling `pyproject.toml` `_postgres.pyi` exclusions, and `release.yaml` `exports: SyncPostgresClient AsyncPostgresClient`) [^gh-ci] [^gh-release] [^ft-gil-test] [^pg-config] [^pg-contracts]:
1. Create `tools/scripts/verify_postgres_wheel.py` to build `vertebra-core`, `vertebra-postgres`, and sibling wheels + sdist (with sdist verified without local checkout assets), install with `--no-deps` and `--no-index --find-links` (`vertebra-core[postgres]`) into a fresh virtual environment outside the checkout (`PYTHONPATH` unset), assert imported `__file__` paths reside in `site-packages`, verify `SyncPostgresClient`, `AsyncPostgresClient`, `SyncPostgresPool`, `AsyncPostgresPool`, `PostgresQueryResult`, query/bulk/pipeline/cancel exports and `.pyi` parity, execute a real `SELECT` and `transaction()` against a test-owned PostgreSQL service (`sslmode="disable"`), and uninstall `vertebra-postgres` to prove `vertebra._vertebra` + sibling extensions remain intact while PostgreSQL adapter instantiation raises `MissingDependencyError` naming `vertebra-core[postgres]`.
2. Update `.github/workflows/release.yaml` (extending `ch08`'s `SyncPostgresClient AsyncPostgresClient` entry) so Linux `x86_64`/`aarch64`, macOS universal, and Windows wheel/sdist/combined-install jobs across `cp312`–`cp315` and `cp314t`/`cp315t` build and smoke-verify `vertebra-postgres` final exports (`SyncPostgresClient`, `AsyncPostgresClient`, `SyncPostgresPool`, `AsyncPostgresPool`, `PostgresQueryResult`, `FREE_THREADED`).
3. Add strict native PostgreSQL service jobs in `.github/workflows/ci.yml`: a parameterized PostgreSQL `14`–`18` conformance/cancellation matrix (`VERTEBRA_CONFORMANCE_DOCKER=1`, `[postgres]` extra installed) plus a live feature acceptance lane running logical replication, digest-pinned `pgvector` 0.8.1, and `PgBouncer` 1.26.0 suites; ensure `test-python` (`3.12`, `3.13`, `3.14`, `3.14t`, `3.15`, `3.15t`) builds `vertebra._postgres` before `mypy.stubtest` and runs concurrent multi-threaded `SyncPostgresPool`/`SyncPostgresClient` smoke probes in `test_freethreaded_gil.py` verifying `sys._is_gil_enabled() is False` before and after importing `vertebra._postgres`.

Out of scope: Editing `Makefile` (owned by `ch29:t03`), benchmark harnesses (`ch29:t02`–`t03`), or dispatching/pushing workflows/tags.

## Bounded Read Manifest

- [.github/workflows/ci.yml](../../../../../../../.github/workflows/ci.yml) — Existing Python `3.12`–`3.15t`, Rust, and stubtest CI jobs [^gh-ci].
- [.github/workflows/release.yaml](../../../../../../../.github/workflows/release.yaml) — Multi-OS wheel, sdist, and combined-install verification jobs [^gh-release].
- [src/py/tests/unit/test_freethreaded_gil.py](../../../../../../../src/py/tests/unit/test_freethreaded_gil.py) — `GIL_USED` / `FREE_THREADED` and `ThreadPoolExecutor` concurrency probes [^ft-gil-test].
- [src/py/vertebra/db/adapters/postgres/config.py](../../../../../../../src/py/vertebra/db/adapters/postgres/config.py) — `PostgreSQLConfig` (`sslmode="disable"` for local non-TLS test services) [^pg-config].
- [.agents/bundles/specs/native-postgres-driver/contracts.md](../../../contracts.md) — Final client, pool, and `PostgresQueryResult` export contracts [^pg-contracts].

## Symbol & Signature Contracts

- `tools/scripts/verify_postgres_wheel.py` (created by this task):
  - `def main(argv: Sequence[str] | None = None) -> int`: CLI supporting `--help`, `--wheel-dir <dir>`, and optional `--dsn <dsn>` (or test-owned local PostgreSQL service with `sslmode="disable"`); returns `0` on full wheel/sdist/live-query/uninstall/GIL verification and raises/returns non-zero on any leak or mismatch.
- `src/py/tests/unit/test_freethreaded_gil.py`:
  - Include `"vertebra._postgres"` in `test_native_modules_declare_gil_used_false()` (`FREE_THREADED is True`, `sys._is_gil_enabled()` remains `False` in a fresh child subprocess before and after `import vertebra._postgres`) and add `test_postgres_native_concurrent_threads_safety()` exercising simultaneous worker threads against `SyncPostgresClient` / `SyncPostgresPool` (loopback wire responder or local service) without shared `PyObject` reference hazards.

## Edge Cases & Error Semantics

- Local test-owned PostgreSQL containers in `verify_postgres_wheel.py` and `ci.yml` explicitly configure `sslmode="disable"` without altering production `PostgreSQLConfig` (`prefer`) or raw client (`verify-full`) defaults.
- Subprocess probes in `verify_postgres_wheel.py` execute with `cwd` set to a temporary directory outside the repository and `env` stripped of `PYTHONPATH`; if `vertebra` or `vertebra._postgres` resolves inside the checkout tree, verification fails immediately.
- After `pip uninstall -y vertebra-postgres` in the isolated venv, `import vertebra._vertebra` and `DatabaseError` identity remain intact while `SyncPostgreSQLDriver(PostgreSQLConfig(...))` raises `MissingDependencyError` citing `vertebra-core[postgres]`.

## Public-Seam Test Blueprint

- **Canonical test / validation targets**: `src/py/tests/unit/test_freethreaded_gil.py` and `tools/scripts/verify_postgres_wheel.py`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Outside-checkout wheel + live query + uninstall isolation | Run `verify_postgres_wheel.py` against freshly built `vertebra-core` + `vertebra-postgres` wheels and sdist | Installed `site-packages` origin confirmed; `SyncPostgresClient`/`AsyncPostgresClient`/`SyncPostgresPool`/`AsyncPostgresPool`/`PostgresQueryResult` + live `SELECT`/`transaction()` succeed; post-uninstall `MissingDependencyError` names `vertebra-core[postgres]` while `_vertebra` still imports. |
| Free-threaded `3.14t`/`3.15t` child import & concurrent pool smoke | Subprocess `python -X gil=0` imports `vertebra._postgres` and runs 8 concurrent `ThreadPoolExecutor` threads over `SyncPostgresPool` | `FREE_THREADED is True`, `sys._is_gil_enabled() is False` before and after import, and all threads receive isolated results with zero races. |
| CI & release workflow static validation | Parse `.github/workflows/ci.yml` and `.github/workflows/release.yaml` YAML matrices | `release.yaml` covers Linux `x86_64`/`aarch64`, macOS universal, Windows (`cp312`–`cp315`, `cp314t`, `cp315t`) with final client/pool/result exports; `ci.yml` defines strict `VERTEBRA_CONFORMANCE_DOCKER=1` PG `14`–`18` and feature lanes. |

## Verification

- Static & unit checks: `make ts-install && make ts-build && uv lock --check && make dev && uv run --no-sync python -m mypy.stubtest vertebra._postgres && uv run --no-sync pytest src/py/tests/unit/test_freethreaded_gil.py -q`
- Wheel isolation & workflow checks: `uv run --no-sync python tools/scripts/verify_postgres_wheel.py --help && uv run --no-sync python tools/scripts/verify_postgres_wheel.py && uv run --no-sync ruff check tools/scripts/verify_postgres_wheel.py src/py/tests/unit/test_freethreaded_gil.py && uv run --no-sync mypy tools/scripts/verify_postgres_wheel.py && make review-check`

## Acceptance Criteria

- [ ] `tools/scripts/verify_postgres_wheel.py` is created and validates outside-checkout wheel + sdist installation, final client/pool/result exports, live `SELECT` + `transaction()`, and clean `vertebra-postgres` uninstall diagnostics (R1, R26).
- [ ] `.github/workflows/release.yaml`, `.github/workflows/ci.yml`, and `src/py/tests/unit/test_freethreaded_gil.py` enforce full platform/interpreter wheel builds, strict PG14–18 + feature acceptance lanes, and disabled-GIL concurrent safety (R6, R7, R26).

## Notes & Discoveries

- [2026-11-07 16:45] Canonical `Makefile` targets `postgres-wheel-check` and `postgres-test` wrapping `verify_postgres_wheel.py` and the `ch28` integration suites are added in `ch29:t03` alongside `postgres-bench` and `postgres-bench-check` so `Makefile` is touched once in Chapter 29.

[^gh-ci]: `.github/workflows/ci.yml` — Continuous integration matrix for Python 3.12–3.15t and database services.
[^gh-release]: `.github/workflows/release.yaml` — Multi-platform wheel and sdist build/smoke matrix.
[^ft-gil-test]: `src/py/tests/unit/test_freethreaded_gil.py::test_native_modules_declare_gil_used_false` — Free-threaded GIL state and concurrent native extension tests.
[^pg-config]: `src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig` — PostgreSQL adapter configuration and `MissingDependencyError` seam.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Public client, pool, result, and wheel isolation contracts.
