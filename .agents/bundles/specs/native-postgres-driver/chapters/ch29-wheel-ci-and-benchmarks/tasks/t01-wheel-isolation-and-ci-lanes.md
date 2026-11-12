---
type: Task
id: "native-postgres-driver:ch29:t01"
chapter_id: ch29-wheel-ci-and-benchmarks
title: Verify outside-checkout PostgreSQL wheel isolation, release matrix, and strict CI/free-threaded lanes
description: Create two-probe tools/scripts/verify_postgres_wheel.py for strict --no-deps and cached-runtime-deps outside-checkout verification, extend release.yaml and zero-skip ci.yml PostgreSQL lanes, and harden free-threaded GIL/immutable-type/concurrent pool tests.
status: draft
state: pending
priority: P1
verification_strategy: static_validation
depends_on:
  - "native-postgres-driver:ch28:t04"
read_set:
  - .github/workflows/ci.yml
  - .github/workflows/release.yaml
  - src/py/tests/unit/test_freethreaded_gil.py
  - src/py/vertebra/_postgres.pyi
  - src/py/vertebra/db/adapters/postgres/config.py
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - .github/workflows/ci.yml
  - .github/workflows/release.yaml
  - tools/scripts/verify_postgres_wheel.py
tests:
  - src/py/tests/unit/test_freethreaded_gil.py
estimated_loc: 245
plan_revision: 8
plan_commit: 5eb75b2a
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
updated_at: "2026-11-12T01:30:00Z"
commit: null
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
    title: Continuous integration matrix for Python 3.12-3.15t and zero-skip PostgreSQL service jobs
  - id: gh-release
    resource: .github/workflows/release.yaml
    title: Multi-platform wheel, sdist, and combined-install smoke matrix
  - id: ft-gil-test
    resource: src/py/tests/unit/test_freethreaded_gil.py::test_native_modules_declare_gil_used_false
    title: Free-threaded runtime detection, Py_TPFLAGS_IMMUTABLETYPE checks, and concurrent pool probes
  - id: pg-stub
    resource: src/py/vertebra/_postgres.pyi
    title: Final vertebra._postgres C-extension pyclass and flag exports
  - id: pg-config
    resource: src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig
    title: PostgreSQL adapter configuration and MissingDependencyError seam naming vertebra-core[postgres]
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Public client, pool, result, handle, and wheel isolation contracts
tags:
  - postgres
  - wheel
  - ci
  - release
  - freethreaded
---

# Task t01: Verify Outside-Checkout PostgreSQL Wheel Isolation, Release Matrix, and Strict CI/Free-Threaded Lanes

## Objective & Scope Boundary

Create `tools/scripts/verify_postgres_wheel.py` with two isolated outside-checkout virtual-environment probes and finalize release wheel verification, zero-skip CI PostgreSQL service lanes, and free-threaded `3.14t`/`3.15t` checks across `.github/workflows/ci.yml`, `.github/workflows/release.yaml`, and `src/py/tests/unit/test_freethreaded_gil.py` [^gh-ci] [^gh-release] [^ft-gil-test] [^pg-stub] [^pg-config] [^pg-contracts]:
1. Update `src/py/tests/unit/test_freethreaded_gil.py`:
   - Define module-level `_is_free_threaded_runtime() -> bool` (`return bool(sysconfig.get_config_var("Py_GIL_DISABLED"))`) and assert `postgres_mod.FREE_THREADED is _is_free_threaded_runtime()` (`True` on `3.14t`/`3.15t`, `False` on standard GIL `3.12`–`3.15`) alongside `postgres_mod.GIL_USED is False`.
   - Expand `test_native_pyclasses_enforce_immutable_type()` across all `14` final `#[pyclass]` exports of `vertebra._postgres` (`SyncPostgresClient`, `AsyncPostgresClient`, `SyncPostgresPool`, `AsyncPostgresPool`, `SyncPostgresPortal`, `AsyncPostgresPortal`, `SyncPostgresCopyOut`, `AsyncPostgresCopyOut`, `SyncPostgresCopyIn`, `AsyncPostgresCopyIn`, `SyncPostgresArrowStream`, `AsyncPostgresArrowStream`, `AsyncPostgresListener`, and `AsyncPostgresReplication`), plus module constants `FREE_THREADED`, `GIL_USED`, and `__version__` verified as module attributes.
   - Add `test_postgres_native_concurrent_threads_safety()` driving 8 concurrent `ThreadPoolExecutor` workers over `SyncPostgresPool` / `SyncPostgresClient` queries (against a deterministic in-process loopback Protocol 3.0 responder so offline `make py-test` executes without Docker, plus extending `test_postgres_import_preserves_disabled_gil_in_child()` on free-threaded runtimes) and asserting `sys._is_gil_enabled() is False` both **before** and **after** importing `vertebra._postgres` and completing concurrent native pool queries on `3.14t`/`3.15t`.
2. Create `tools/scripts/verify_postgres_wheel.py` (clean module layout, structured `PostgresWheelVerifier` class, `sys.stdout.write` / `sys.stderr.write`, zero nested imports, zero inline `#` comments) that builds `vertebra-core`, `vertebra-postgres`, and a fixed sibling sample (`vertebra-bigquery`, `vertebra-bundler`, `vertebra-spanner`, `vertebra-storage`; the sample proves sibling isolation and is not the full companion inventory) wheels plus `vertebra-postgres` sdist, inspects archive contents (`vertebra/_postgres.pyi` + single compiled `_postgres` artifact, zero `.py` or sibling `.pyi` leaks, standalone sdist-to-wheel build outside the checkout), provisions a test-owned PostgreSQL service when `--dsn` is omitted (`sslmode="disable"`), and runs **two** isolated temporary venv probes from `cwd=tmp_outside_repo` with `PYTHONPATH` stripped under `python -I` (asserting zero checkout paths on `sys.path`):
   - **(a) Strict `--no-deps --no-index --find-links` empty-venv probe**: installs only the locally built `vertebra-*` wheels with `--no-deps --no-index --find-links <wheel_dir>` into a fresh empty venv, verifies every installed compiled module (`vertebra._vertebra`, `vertebra._postgres`, `vertebra._bigquery`, `vertebra._bundler`, `vertebra._spanner`, `vertebra._storage`) imports, every `Path(mod.__file__).is_relative_to(site_packages)`, `.pyi` stubs co-locate in `site-packages/vertebra/`, all `14` final `vertebra._postgres` `#[pyclass]` exports plus `FREE_THREADED`, `GIL_USED`, and `__version__` exist, and live `SyncPostgresClient` / `AsyncPostgresClient` / `SyncPostgresPool` `connect()` + `begin()`/`savepoint()`/`commit()`/`rollback()` + `stream_arrow_batches("SELECT 42")` (returning `vertebra._vertebra.ArrowCursor` without importing `msgspec`) succeed with zero third-party distributions, then adds strictly the cached offline `msgspec` wheel (`--no-deps --no-index --find-links <cached_wheels>`, required by `PostgresQueryResult` and `PostgresArrowResult` in `vertebra.db.adapters.postgres.types`) to verify live raw `select_to_arrow("SELECT 42")` (returning `PostgresArrowResult`, a `msgspec.Struct`) alongside `SyncPostgresClient.query("SELECT 42")` / `AsyncPostgresClient.query("SELECT 42")` / `SyncPostgresPool` `SELECT` + `transaction()` (`begin()`/`commit()`/`rollback()`) with zero other third-party packages and zero checkout paths.
   - **(b) Runtime-deps isolated venv + uninstall probe**: installs `vertebra-core[bigquery,bundler,postgres,spanner,storage]` + cached runtime third-party wheels (`msgspec`, `anyio`, `rich`, `rich-click`, `uvloop`/`winloop`) from the local `.venv`/uv/pip cache via `--no-index --find-links` (`--only-binary :all:`, zero checkout paths on `sys.path`), asserts `Path(vertebra.__file__).is_relative_to(site_packages)` and `Path(vertebra._postgres.__file__).is_relative_to(site_packages)`, executes live `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` `SELECT` + `select_to_arrow("SELECT 42")` + `transaction()` (`commit` and rollback-on-error, `sslmode="disable"`), runs `pip uninstall -y vertebra-postgres` (or `uv pip uninstall --python <venv_py> vertebra-postgres`), and verifies in a fresh `python -I` child that `vertebra._vertebra` + sibling native extensions and `DatabaseError` stay importable while `SyncPostgreSQLDriver(cfg)` and `AsyncPostgreSQLDriver(cfg)` raise `MissingDependencyError` with `package == "vertebra-core[postgres]"` and `"vertebra-core[postgres]"` in `str(exc)`.
3. Update `.github/workflows/release.yaml` (`cp312`–`cp315`, `cp314t`, `cp315t` across Linux `x86_64`/`aarch64`, macOS universal, Windows; the `postgres` build-matrix entry, the `"postgres": ("SyncPostgresClient", "AsyncPostgresClient")` smoke tuple, and the combined `vertebra-core[...,postgres,...]` `test_wheels` install already exist at `c8722c76`) by extending the existing `postgres` smoke tuple to check all `14` final `vertebra._postgres` `#[pyclass]` exports (`SyncPostgresClient`, `AsyncPostgresClient`, `SyncPostgresPool`, `AsyncPostgresPool`, `SyncPostgresPortal`, `AsyncPostgresPortal`, `SyncPostgresCopyOut`, `AsyncPostgresCopyOut`, `SyncPostgresCopyIn`, `AsyncPostgresCopyIn`, `SyncPostgresArrowStream`, `AsyncPostgresArrowStream`, `AsyncPostgresListener`, and `AsyncPostgresReplication`) plus `FREE_THREADED`, `GIL_USED`, and `__version__` in `build_wheels`, and `PostgresQueryResult` + `PostgresArrowResult` + `SyncPostgreSQLDriver`/`AsyncPostgreSQLDriver` imports in `test_wheels`; add to `.github/workflows/ci.yml` (which has no PostgreSQL job at `c8722c76`) parameterized PostgreSQL `14`–`18` conformance and live PostgreSQL services/features jobs (logical replication, `pgvector` `0.8.1`, `PgBouncer` `1.26.0`) setting **both** `VERTEBRA_CONFORMANCE_DOCKER: "1"` and `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS: "1"` on every live PostgreSQL job while keeping `test-python` (`3.12`–`3.15t`) offline and green.

Out of scope: Modifying `Makefile` (updated in place in `ch29:t03`), `src/py/vertebra/build/packaging/` and `tools/pgo/` (explicitly untouched; `tools/pgo/rust_pgo.py::discover_extension_manifests` already includes `vertebra-postgres`), benchmark modules (`ch29:t02`–`t03`), or pushing/dispatching workflows/tags.

## Bounded Read Manifest

- [.github/workflows/ci.yml](../../../../../../../.github/workflows/ci.yml) — Existing Python `3.12`–`3.15t`, Rust, and stubtest CI jobs [^gh-ci].
- [.github/workflows/release.yaml](../../../../../../../.github/workflows/release.yaml) — Multi-platform `cibuildwheel`, `build_sdist`, and two-step `test_wheels` (`--no-deps` + extras) smoke jobs [^gh-release].
- [src/py/tests/unit/test_freethreaded_gil.py](../../../../../../../src/py/tests/unit/test_freethreaded_gil.py) — `GIL_USED`/`FREE_THREADED`, `test_native_pyclasses_enforce_immutable_type`, and child `-c` GIL checks [^ft-gil-test].
- [src/py/vertebra/_postgres.pyi](../../../../../../../src/py/vertebra/_postgres.pyi) — Complete final `vertebra._postgres` class and module-constant export list [^pg-stub].
- [src/py/vertebra/db/adapters/postgres/config.py](../../../../../../../src/py/vertebra/db/adapters/postgres/config.py) — `PostgreSQLConfig` (`sslmode="disable"` on local non-TLS test services) [^pg-config].
- [.agents/bundles/specs/native-postgres-driver/contracts.md](../../../contracts.md) — Public client, pool, handle, `PostgresQueryResult`, and `MissingDependencyError` contracts [^pg-contracts].

## Symbol & Signature Contracts

- `tools/scripts/verify_postgres_wheel.py`:
  - `class PostgresWheelVerifier:` + `def main(argv: Sequence[str] | None = None) -> int` supporting `--help`, `--wheel-dir <dir>`, and optional `--dsn <dsn>` (launching and tearing down an ephemeral Docker PostgreSQL 18 container with `sslmode="disable"` when `--dsn` is unset and Docker is available, failing non-zero if neither `--dsn` nor Docker can satisfy live checks unless `--archive-only` is not passed — full default run requires live query/tx verification).
  - Stages cached offline runtime third-party wheels from the active `.venv`/uv/pip wheel cache (`pip download`/`uv cache` local wheel export without network when offline, or cached `.whl` lookup) so both isolated venv probes run strictly with `--no-index --find-links`.
- `src/py/tests/unit/test_freethreaded_gil.py`:
  - `def _is_free_threaded_runtime() -> bool` (`bool(sysconfig.get_config_var("Py_GIL_DISABLED"))`).
  - `test_native_modules_declare_gil_used_false()`: asserts `ext_mod.FREE_THREADED is _is_free_threaded_runtime()` and `ext_mod.GIL_USED is False` for `vertebra._postgres`.
  - `test_native_pyclasses_enforce_immutable_type()`: asserts `setattr(cls, "_mutated_type_attr", 1)` raises `TypeError` across every final `vertebra._postgres` class export.
  - `test_postgres_native_concurrent_threads_safety()`: verifies 8 concurrent threads executing queries through `SyncPostgresPool` / `SyncPostgresClient` return isolated per-thread values and preserve `sys._is_gil_enabled() is False` before and after on `3.14t`/`3.15t`.

## Edge Cases & Error Semantics

- Local test-owned PostgreSQL containers in `verify_postgres_wheel.py` and `ci.yml` pass `sslmode="disable"` explicitly without altering production `PostgreSQLConfig` (`prefer`) or raw client (`verify-full`) defaults, and redact DSN passwords from any diagnostic error messages.
- Every subprocess probe in `verify_postgres_wheel.py` runs with `cwd` inside an outside-checkout `tempfile.TemporaryDirectory`, `PYTHONPATH` and `PYTHONHOME` removed, and `-I` isolation; any `sys.path` entry or `mod.__file__` resolving under the checkout root fails immediately.
- Every live PostgreSQL job in `.github/workflows/ci.yml` sets both `VERTEBRA_CONFORMANCE_DOCKER: "1"` and `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS: "1"`, guaranteeing `ch28`'s `pytest_terminal_summary` hook fails CI if any PostgreSQL test silently skips or xfails.

## Public-Seam Test Blueprint

- **Canonical test / validation targets**: `src/py/tests/unit/test_freethreaded_gil.py` and `tools/scripts/verify_postgres_wheel.py`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Strict `--no-deps` empty-venv probe (a) | `verify_postgres_wheel.py` installs built `vertebra-*` wheels with `--no-deps --no-index --find-links` outside checkout (`python -I`, unset `PYTHONPATH`), then adds cached `msgspec` wheel | Zero-third-party phase verifies `vertebra._vertebra`, `vertebra._postgres`, and the four sampled sibling modules + `.pyi` stubs, zero `.py` leaks in `vertebra_postgres-*.whl`, and live raw `connect()` + `begin()`/`savepoint()`/`commit()`/`rollback()` + `stream_arrow_batches("SELECT 42")` (`ArrowCursor`, no `msgspec`); `msgspec` phase verifies `select_to_arrow("SELECT 42")` (`PostgresArrowResult`) + `query("SELECT 42")` + `transaction()`. |
| Runtime-deps isolated venv + uninstall probe (b) | Install `vertebra-core[postgres]` + cached runtime wheels via `--no-index --find-links`, run live `SyncPostgreSQLDriver`/`AsyncPostgreSQLDriver` `SELECT` + `transaction()`, then `pip uninstall -y vertebra-postgres` | `vertebra.__file__` and `vertebra._postgres.__file__` sit inside temp `site-packages`; live driver commit/rollback pass; post-uninstall `_vertebra` + sibling native modules still import while both drivers raise `MissingDependencyError` naming `vertebra-core[postgres]`. |
| GIL vs free-threaded `FREE_THREADED`, immutable `#[pyclass]`, & concurrent pool safety | Run `pytest src/py/tests/unit/test_freethreaded_gil.py` on GIL `3.12`–`3.15` and free-threaded `3.14t`/`3.15t` | `FREE_THREADED` is `False` on GIL builds and `True` on `3.14t`/`3.15t`; every `vertebra._postgres` class rejects `setattr` with `TypeError`; 8-thread concurrent `SyncPostgresPool` queries isolate results and keep `sys._is_gil_enabled() is False` before and after on `3.14t`/`3.15t`. |
| CI zero-skip & release workflow validation | Parse `.github/workflows/ci.yml` and `.github/workflows/release.yaml` YAML definitions | `release.yaml` covers `cp312`–`cp315` and `cp314t`/`cp315t` on Linux `x86_64`/`aarch64`, macOS universal, and Windows with final exports; every live PG job in `ci.yml` exports `VERTEBRA_CONFORMANCE_DOCKER=1` and `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1`. |

## Verification

- Static & unit checks:
  ```bash
  make ts-install && make ts-build && uv lock --check && make dev
  uv run --no-sync python -m mypy.stubtest vertebra._postgres
  uv run --no-sync pytest src/py/tests/unit/test_freethreaded_gil.py -q
  ```
- Wheel isolation, lint, typecheck, & review validation:
  ```bash
  uv run --no-sync python tools/scripts/verify_postgres_wheel.py --help
  uv run --no-sync python tools/scripts/verify_postgres_wheel.py
  uv run --no-sync ruff check tools/scripts/verify_postgres_wheel.py src/py/tests/unit/test_freethreaded_gil.py
  uv run --no-sync mypy tools/scripts/verify_postgres_wheel.py
  make review-check
  ```

## Acceptance Criteria

- [ ] `src/py/tests/unit/test_freethreaded_gil.py` asserts `postgres_mod.FREE_THREADED is _is_free_threaded_runtime()`, checks `Py_TPFLAGS_IMMUTABLETYPE` across all final `vertebra._postgres` `#[pyclass]` exports, and verifies `sys._is_gil_enabled() is False` before and after concurrent multi-threaded native pool queries on `3.14t`/`3.15t` (R6, R26).
- [ ] `tools/scripts/verify_postgres_wheel.py` validates wheel/sdist archives and executes both (a) the strict `--no-deps --no-index --find-links` empty-venv live raw client/pool probe and (b) the cached runtime-deps isolated venv live `SyncPostgreSQLDriver`/`AsyncPostgreSQLDriver` + `pip uninstall -y vertebra-postgres` `MissingDependencyError("vertebra-core[postgres]")` probe with zero checkout paths on `sys.path` (R1, R26).
- [ ] `.github/workflows/release.yaml` verifies all final `vertebra._postgres` exports across `cp312`–`cp315` and `cp314t`/`cp315t` on Linux/macOS/Windows, and `.github/workflows/ci.yml` sets `VERTEBRA_CONFORMANCE_DOCKER=1` and `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1` across all live PostgreSQL `14`–`18` and service jobs (R6, R7, R26).

## Notes & Discoveries

- Pre-existing `Makefile` targets `postgres-wheel-check` and `postgres-test` are updated in place in `ch29:t03` alongside `lint`, `lint-all`, `postgres-bench`, and `postgres-bench-check` so `Makefile` is edited in a single chapter task [^gh-ci] [^gh-release].

[^gh-ci]: `.github/workflows/ci.yml` — Continuous integration matrix for Python 3.12–3.15t and zero-skip PostgreSQL service jobs.
[^gh-release]: `.github/workflows/release.yaml` — Multi-platform wheel, sdist, and combined-install smoke matrix.
[^ft-gil-test]: `src/py/tests/unit/test_freethreaded_gil.py::test_native_modules_declare_gil_used_false` — Free-threaded runtime detection, Py_TPFLAGS_IMMUTABLETYPE checks, and concurrent pool probes.
[^pg-stub]: `src/py/vertebra/_postgres.pyi` — Final `vertebra._postgres` C-extension pyclass and flag exports.
[^pg-config]: `src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig` — PostgreSQL adapter configuration and `MissingDependencyError` seam naming `vertebra-core[postgres]`.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Public client, pool, result, handle, and wheel isolation contracts.
