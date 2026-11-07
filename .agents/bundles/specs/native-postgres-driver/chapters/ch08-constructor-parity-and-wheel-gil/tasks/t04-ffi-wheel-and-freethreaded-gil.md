---
type: Task
id: "native-postgres-driver:ch08:t04"
chapter_id: ch08-constructor-parity-and-wheel-gil
title: Exclude _postgres.pyi from storage wheel, wire release.yaml, and verify isolated free-threaded imports
description: Exclude _postgres.pyi from vertebra-storage, register postgres with exact release.yaml matrix keys and Windows MSVC cargo check, preserve GIL_USED and FREE_THREADED stubtest checks, and verify Python 3.14t/3.15t disabled-GIL subprocess imports plus standalone /tmp wheel installation.
status: stable
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on:
  - "native-postgres-driver:ch08:t03"
read_set:
  - src/rs/vertebra-storage/pyproject.toml
  - .github/workflows/release.yaml
  - src/py/tests/unit/test_freethreaded_gil.py
  - src/rs/vertebra-postgres/src/lib.rs
  - src/py/vertebra/_postgres.pyi
  - .agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md
files:
  - src/rs/vertebra-storage/pyproject.toml
  - .github/workflows/release.yaml
tests:
  - src/py/tests/unit/test_freethreaded_gil.py
estimated_loc: 195
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
updated_at: "2026-11-07T16:55:00Z"
commit: null
sources:
  - id: storage-pyproject
    resource: src/rs/vertebra-storage/pyproject.toml
    title: Storage optional Maturin wheel manifest and sibling stub exclusions
  - id: release-yaml
    resource: .github/workflows/release.yaml
    title: Multi-wheel release build matrix, Windows MSVC toolchain, sdist, and wheel smoke checks
  - id: ft-gil-tests
    resource: src/py/tests/unit/test_freethreaded_gil.py
    title: Free-threaded Python Py_GIL_DISABLED extension verification tests
  - id: spanner-py
    resource: src/rs/vertebra-spanner/src/lib.rs::PySyncSpannerClient
    title: Reference optional-wheel gil_used=false, GIL_USED, and FREE_THREADED exports
tags:
  - postgres
  - wheel
  - freethreaded
  - ci
---

# Task t04: Exclude `_postgres.pyi` from `storage` Wheel, Wire `release.yaml`, and Verify Isolated Free-Threaded Imports

## Objective & Scope Boundary

Complete companion wheel stub isolation by adding `"**/py/vertebra/_postgres.pyi"` to `src/rs/vertebra-storage/pyproject.toml`, register `postgres` across `.github/workflows/release.yaml` using the exact live `build_wheels` matrix schema (`name`, `path`, `module`, `exports`) plus Windows MSVC `cargo check -p vertebra-db -p vertebra-postgres --all-targets`, `build_sdist`, and `test_wheels`, preserve `GIL_USED: bool` and `FREE_THREADED: bool` stubtest verification, and prove isolated-subprocess free-threaded disabled-GIL imports (`test_postgres_import_preserves_disabled_gil_in_child`) across Python `3.14t` and `3.15t` plus standalone `/tmp` wheel installation outside the checkout [^storage-pyproject] [^release-yaml] [^ft-gil-tests] [^spanner-py].

## Bounded Read Manifest

- `src/rs/vertebra-storage/pyproject.toml` — `[tool.maturin].exclude` list for `vertebra._storage` [^storage-pyproject].
- `.github/workflows/release.yaml` — `jobs.build_wheels.strategy.matrix.package`, Windows Rust/MSVC setup, `jobs.build_sdist`, and `jobs.test_wheels` [^release-yaml].
- `src/py/tests/unit/test_freethreaded_gil.py` — Existing `GIL_USED` and `immutable_type` tests across `_vertebra`, `_bundler`, `_storage`, `_bigquery`, and `_spanner` [^ft-gil-tests].
- `src/rs/vertebra-postgres/src/lib.rs` — `#[pymodule(gil_used = false)]` exports (`__version__`, `FREE_THREADED`, `GIL_USED`, `SyncPostgresClient`, `AsyncPostgresClient`).
- `src/py/vertebra/_postgres.pyi` — Public stub constants (`__version__: str`, `FREE_THREADED: bool`, `GIL_USED: bool`) and `@final` `def __new__` client stubs from `ch08:t01`.
- `.agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md` — `Py_GIL_DISABLED` subprocess isolation and `immutable_type` invariants.

## Symbol & Signature Contracts

- `src/rs/vertebra-storage/pyproject.toml`:
  - Append `"**/py/vertebra/_postgres.pyi"` to `[tool.maturin].exclude` (completing 4/4 companion wheel exclusions alongside `ch08:t03`).
- `.github/workflows/release.yaml`:
  - In `jobs.build_wheels.strategy.matrix.package`, add the `postgres` entry using the exact four keys used by sibling packages:
    ```yaml
    - name: postgres
      path: src/rs/vertebra-postgres
      module: vertebra._postgres
      exports: SyncPostgresClient AsyncPostgresClient
    ```
  - Add a step in `jobs.build_wheels` before `Build wheels` gated strictly by `if: runner.os == 'Windows' && matrix.package.name == 'postgres'` executing `cargo check --manifest-path src/rs/Cargo.toml -p vertebra-db -p vertebra-postgres --all-targets` to verify `#[cfg(unix)]` / `#[cfg(windows)]` TCP keepalive and Unix-socket `cfg` gates on native Windows MSVC.
  - In `jobs.build_sdist`, extend the package loop to `for package in bigquery bundler postgres spanner storage; do uv build "src/rs/vertebra-$package" --sdist --out-dir dist; done`.
  - In `jobs.test_wheels`:
    - Append `vertebra-postgres` to the `--no-deps` `uv pip install` line and add `postgres` to `"vertebra-core[bigquery,bundler,postgres,spanner,storage]==$version"`.
    - Add `"postgres": ("SyncPostgresClient", "AsyncPostgresClient")` to the namespace/version check dict (`importlib.metadata.version(f"vertebra-{name}") == vertebra.__version__`), assert `not hasattr(_vertebra, "SyncPostgresClient")`, and verify pure zero-I/O `SyncPostgresClient(user="u", database="d").closed is True` and `AsyncPostgresClient(user="u", database="d").parameters == {}`.
- `src/py/tests/unit/test_freethreaded_gil.py`:
  - Add `"vertebra._postgres"` to `test_native_modules_declare_gil_used_false()` (`assert getattr(ext_mod, "GIL_USED", None) is False` and `isinstance(ext_mod.FREE_THREADED, bool)`) and add `postgres_mod.SyncPostgresClient` and `postgres_mod.AsyncPostgresClient` to `test_native_pyclasses_enforce_immutable_type()`.
  - Add `def test_postgres_import_preserves_disabled_gil_in_child() -> None`:
    - Gate **only** on `bool(sysconfig.get_config_var("Py_GIL_DISABLED"))` (`pytest.skip(...)` on standard GIL-enabled builds; never gate on parent `sys._is_gil_enabled()`).
    - Strip `PYTHON_GIL` from a copied `os.environ` (`env.pop("PYTHON_GIL", None)`) and invoke `subprocess.run([sys.executable, "-I", "-c", script], env=env, check=True, capture_output=True, text=True, timeout=30)` with zero `-X gil` overrides.
    - Child `script` asserts `hasattr(sys, "_is_gil_enabled")`, asserts `sys._is_gil_enabled() is False` *before* `import vertebra._postgres`, imports `vertebra._postgres as pg`, asserts `sys._is_gil_enabled() is False` *after* import, and verifies `pg.FREE_THREADED is True`, `pg.GIL_USED is False`, and `pg.SyncPostgresClient(user="u", database="d").closed is True`.

## Edge Cases & Error Semantics

- Checking `FREE_THREADED` or `GIL_USED` inside an already-running pytest process is insufficient if an earlier C extension forced the GIL back on; the `-I` child process without `PYTHON_GIL` fails immediately if `vertebra._postgres` or any registered PyO3 class omits `gil_used = false` / `immutable_type`.
- Standalone wheel verification must `cd` into a temporary directory outside the repository root with `python -I` (`PYTHONPATH` unset) so `import vertebra` and `import vertebra._postgres` resolve strictly from `sys.prefix` (`site-packages`), never from `src/py/vertebra`, while `mypy.stubtest vertebra._postgres` verifies `GIL_USED: bool`, `FREE_THREADED: bool`, `__version__: str`, `SyncPostgresClient`, and `AsyncPostgresClient`.
- Restore `.venv` to Python `3.12` (`uv sync --python 3.12 --all-extras --dev --group doc && UV_PYTHON=3.12 UV_NO_SYNC=1 make dev`) after `3.14t` and `3.15t` runs.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/test_freethreaded_gil.py` (`test_postgres_import_preserves_disabled_gil_in_child`, `test_native_modules_declare_gil_used_false`, `test_native_pyclasses_enforce_immutable_type`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `vertebra-storage` stub exclusion & `release.yaml` schema | Inspect `src/rs/vertebra-storage/pyproject.toml` + built `vertebra_storage-*.whl` (`zipfile`) and parse `.github/workflows/release.yaml` (`yaml.safe_load`) | `"vertebra/_postgres.pyi"` absent from `vertebra_storage-*.whl`; `release.yaml` `build_wheels` matrix has `{"name": "postgres", "path": "src/rs/vertebra-postgres", "module": "vertebra._postgres", "exports": "SyncPostgresClient AsyncPostgresClient"}` plus Windows `cargo check`, `build_sdist`, and `test_wheels` entries. |
| Isolated `3.14t` & `3.15t` child import (`test_postgres_import_preserves_disabled_gil_in_child`) | `UV_PYTHON=3.14t` and `UV_PYTHON=3.15t` (`Py_GIL_DISABLED == 1`), `subprocess.run([sys.executable, "-I", "-c", ...])` with `PYTHON_GIL` removed | Exit `0`; `sys._is_gil_enabled() is False` before and after `import vertebra._postgres`; `pg.FREE_THREADED is True` and `pg.GIL_USED is False`. |
| Free-threaded async cancellation under `3.14t` & `3.15t` | `test_postgres_driver.py -k native_async_cancel` under `3.14t` and `3.15t` | Exit `0` with zero GIL deadlocks, data races, or leaked `PendingSession` handles. |
| Standalone wheel install, namespace co-location & stubtest | Build `vertebra_core-*.whl` + `vertebra_postgres-*.whl` into `/tmp/vertebra-postgres-wheel-XXXXXX/dist`, install `'vertebra-core[postgres]'` with `--no-index --find-links` into fresh venv, run `python -I` from `/tmp` | `Path(vertebra.__file__).is_relative_to(sys.prefix)` and `Path(p.__file__).is_relative_to(sys.prefix)` are both `True`; `p.GIL_USED is False`; versions match; `mypy.stubtest vertebra._postgres` exits `0`. |

## Verification

- Storage wheel exclusion + `release.yaml` schema + `3.12` stubtest baseline:
  - `STORAGE_DIST="$(mktemp -d /tmp/vertebra-storage-wheel-XXXXXX)" && uv build src/rs/vertebra-storage --wheel --out-dir "$STORAGE_DIST" && uv run --no-sync python -c 'import glob,sys,zipfile; whls=glob.glob(f"{sys.argv[1]}/vertebra_storage-*.whl"); assert len(whls)==1; zf=zipfile.ZipFile(whls[0]); assert "vertebra/_postgres.pyi" not in zf.namelist()' "$STORAGE_DIST" && rm -rf "$STORAGE_DIST"`
  - `make dev && uv run --no-sync python -m mypy.stubtest vertebra._postgres`
- Free-threaded `3.14t` / `3.15t` + `3.12` restore gate:
  - `uv sync --python 3.14t --all-extras --dev --group doc && UV_PYTHON=3.14t UV_NO_SYNC=1 make dev`
  - `UV_PYTHON=3.14t uv run --no-sync python -m pytest src/py/tests/unit/test_freethreaded_gil.py -q`
  - `UV_PYTHON=3.14t uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_driver.py -k native_async_cancel -q`
  - `uv sync --python 3.15t --all-extras --dev --group doc && UV_PYTHON=3.15t UV_NO_SYNC=1 make dev`
  - `UV_PYTHON=3.15t uv run --no-sync python -m pytest src/py/tests/unit/test_freethreaded_gil.py -q`
  - `UV_PYTHON=3.15t uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_driver.py -k native_async_cancel -q`
  - `uv sync --python 3.12 --all-extras --dev --group doc && UV_PYTHON=3.12 UV_NO_SYNC=1 make dev`
  - `uv run --no-sync python -m pytest src/py/tests/unit/test_freethreaded_gil.py -q`
- Fresh installed-wheel lane outside checkout (`PYTHONPATH` unset):
  - `PG_WHEEL_DIR="$(mktemp -d /tmp/vertebra-postgres-wheel-XXXXXX)" && make ts-install && make ts-build && uv build --wheel --out-dir "$PG_WHEEL_DIR/dist" && uv build src/rs/vertebra-postgres --wheel --out-dir "$PG_WHEEL_DIR/dist" && uv export --only-group test --only-group lint --no-emit-project --format requirements-txt --output-file "$PG_WHEEL_DIR/test-requirements.txt" && uv venv --python 3.12 "$PG_WHEEL_DIR/venv" && uv pip install --python "$PG_WHEEL_DIR/venv/bin/python" "$PG_WHEEL_DIR"/dist/vertebra_core-*.whl "$PG_WHEEL_DIR"/dist/vertebra_postgres-*.whl && uv pip install --python "$PG_WHEEL_DIR/venv/bin/python" --no-index --find-links "$PG_WHEEL_DIR/dist" 'vertebra-core[postgres]' && uv pip install --python "$PG_WHEEL_DIR/venv/bin/python" -r "$PG_WHEEL_DIR/test-requirements.txt" && (cd "$PG_WHEEL_DIR" && "$PG_WHEEL_DIR/venv/bin/python" -I -c 'import pathlib,sys,importlib.metadata,vertebra,vertebra._postgres as p; assert pathlib.Path(p.__file__).is_relative_to(sys.prefix); assert pathlib.Path(vertebra.__file__).is_relative_to(sys.prefix); assert importlib.metadata.version("vertebra-postgres")==importlib.metadata.version("vertebra-core"); assert p.GIL_USED is False and isinstance(p.FREE_THREADED, bool); assert p.SyncPostgresClient(user="u",database="d").closed; assert p.AsyncPostgresClient(user="u",database="d").parameters=={}' && "$PG_WHEEL_DIR/venv/bin/python" -I -m mypy.stubtest vertebra._postgres) && rm -rf "$PG_WHEEL_DIR"`

## Acceptance Criteria

- [ ] `src/rs/vertebra-storage/pyproject.toml` excludes `"**/py/vertebra/_postgres.pyi"` so all four companion wheels omit `_postgres.pyi` (R1).
- [ ] `.github/workflows/release.yaml` registers `postgres` with exact `name`/`path`/`module`/`exports` matrix keys, Windows MSVC `cargo check -p vertebra-db -p vertebra-postgres --all-targets`, `build_sdist`, and `test_wheels` namespace/version/pure-constructor checks (R1, R26).
- [ ] `test_postgres_import_preserves_disabled_gil_in_child` (gated solely on `sysconfig.get_config_var("Py_GIL_DISABLED")`) and `GIL_USED`/`FREE_THREADED`/`immutable_type` checks pass on `3.14t`, `3.15t`, and `3.12`, standalone `/tmp` wheel installation + `mypy.stubtest` pass outside the checkout, and commit `test(postgres): verify wheel stubs and free-threaded import isolation` completes Phase 1 (R1, R6, R7, R26).

## Notes & Discoveries

- None.

[^storage-pyproject]: `src/rs/vertebra-storage/pyproject.toml` — Maturin wheel manifest and `.pyi` exclusions for `vertebra-storage`.
[^release-yaml]: `.github/workflows/release.yaml` — Multi-wheel build matrix, Windows MSVC check, and installed-wheel smoke workflow.
[^ft-gil-tests]: `src/py/tests/unit/test_freethreaded_gil.py` — Free-threaded Python `Py_GIL_DISABLED` extension verification suite.
[^spanner-py]: `src/rs/vertebra-spanner/src/lib.rs::PySyncSpannerClient` — Reference optional-wheel `#[pymodule(gil_used = false)]` module layout.
