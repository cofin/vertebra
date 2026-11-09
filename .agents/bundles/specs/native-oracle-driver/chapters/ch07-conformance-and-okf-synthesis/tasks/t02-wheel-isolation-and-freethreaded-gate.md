---
type: Task
id: "native-oracle-driver:ch07:t02"
flow_id: native-oracle-driver
chapter_id: ch07-conformance-and-okf-synthesis
title: "Task 02: Implement Standalone Wheel Isolation and Free-Threaded Gate"
description: Implement tools/scripts/verify_oracle_wheel.py (make oracle-wheel-check), wire vertebra-oracle across build_wheels, build_sdist, and test_wheels in .github/workflows/release.yaml, and add vertebra._oracle (GIL_USED == False) to src/py/tests/unit/test_freethreaded_gil.py.
status: stable
state: pending
priority: P1
owner: null
depends_on:
  - "native-oracle-driver:ch07:t01"
files:
  - tools/scripts/verify_oracle_wheel.py
  - .github/workflows/release.yaml
  - src/py/tests/unit/test_freethreaded_gil.py
tests:
  - src/py/tests/unit/test_freethreaded_gil.py
read_set:
  - .agents/bundles/specs/native-oracle-driver/contracts.md
  - .agents/bundles/specs/native-oracle-driver/chapters/ch07-conformance-and-okf-synthesis/spec.md
  - .github/workflows/release.yaml
  - src/py/tests/unit/test_freethreaded_gil.py
  - pyproject.toml
verification_strategy: regression_tdd
estimated_loc: 185
plan_revision: 3
plan_commit: 769302bc
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-09T00:48:00Z"
updated_at: "2026-11-09T01:28:30Z"
completed_at: null
commit_sha: null
commit: null
generated:
  by: flow-plan
  at: "2026-11-09T01:15:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-09T01:28:30Z"
    commit: 769302bc
sources:
  - id: ora-contracts
    resource: .agents/bundles/specs/native-oracle-driver/contracts.md
    title: Native vertebra._oracle module exports and wheel isolation contracts
  - id: freethreaded-py
    resource: src/py/tests/unit/test_freethreaded_gil.py::test_native_modules_declare_gil_used_false
    title: Free-threaded Python 3.14t/3.15t GIL_USED == False verification suite
  - id: release-yaml
    resource: .github/workflows/release.yaml
    title: Multi-wheel cibuildwheel and sdist release workflow
tags:
  - oracle
  - wheel
  - packaging
  - freethreaded
  - ci
---

# Task 02: Implement Standalone Wheel Isolation and Free-Threaded Gate

## Objective & Scope Boundary

Implement `tools/scripts/verify_oracle_wheel.py` (`make oracle-wheel-check`), register `vertebra-oracle` (`src/rs/vertebra-oracle`) across `build_wheels`, `build_sdist`, and `test_wheels` in `.github/workflows/release.yaml`, and add `vertebra._oracle` and its PyO3 exports to `src/py/tests/unit/test_freethreaded_gil.py` [^ora-contracts] [^freethreaded-py] [^release-yaml].

- **In Scope**:
  - Create `tools/scripts/verify_oracle_wheel.py` verifying:
    1. `vertebra_oracle-*.whl` and sdist contain `vertebra/_oracle*.(so|pyd|dylib)` and `vertebra/_oracle.pyi`.
    2. Sibling wheels (`vertebra_core-*.whl`) exclude `vertebra/_oracle.*` and `vertebra/_oracle.pyi`.
    3. `vertebra/_oracle*.so` has zero dynamic linkage to `libclntsh`, `libnnz`, or `libodpic`.
    4. Outside-checkout installation of `vertebra-core` alone raises `MissingDependencyError` naming `vertebra-core[oracle]`, and installing `vertebra-oracle` resolves `vertebra._oracle` with `GIL_USED is False`.
  - Update `.github/workflows/release.yaml` across all three sections:
    1. `jobs.build_wheels.strategy.matrix.package`: add `{ name: oracle, path: src/rs/vertebra-oracle, module: vertebra._oracle, exports: SyncOracleClient AsyncOracleClient SyncOraclePool AsyncOraclePool }`.
    2. `jobs.build_sdist`: update the sdist loop to `for package in bigquery bundler oracle spanner storage; do`.
    3. `jobs.test_wheels`: install `vertebra-oracle` and `"vertebra-core[bigquery,bundler,oracle,spanner,storage]==$version"`, add `"oracle": ("SyncOracleClient", "AsyncOracleClient", "SyncOraclePool", "AsyncOraclePool")` to the export loop, and assert `not hasattr(_vertebra, "SyncOracleClient")`.
  - Add `vertebra._oracle` (`GIL_USED is False`, `FREE_THREADED`, `__version__`, `ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED`, and all exported PyO3 classes) to `src/py/tests/unit/test_freethreaded_gil.py`.
- **Out of Scope**:
  - User documentation and OKF knowledge chapters (owned by `ch07:t03` and `ch07:t04`).

## Bounded Read Manifest

1. `.agents/bundles/specs/native-oracle-driver/contracts.md` — Section 1 `vertebra._oracle` exports.
2. `.agents/bundles/specs/native-oracle-driver/chapters/ch07-conformance-and-okf-synthesis/spec.md` — Section 2.2 wheel isolation and free-threaded verification rules.
3. `.github/workflows/release.yaml` — `build_wheels`, `build_sdist`, and `test_wheels` jobs.
4. `src/py/tests/unit/test_freethreaded_gil.py` — Existing `NATIVE_MODULES` and PyO3 type checks.
5. `pyproject.toml` — Root wheel exclusions and `[project.optional-dependencies].oracle`.

## Symbol & Signature Contracts

### `tools/scripts/verify_oracle_wheel.py`

```python
def verify_core_wheel_excludes_oracle_binary_and_stub(wheel_path: Path) -> None: ...
def verify_oracle_wheel_contents_and_pure_rust_linkage(wheel_path: Path) -> None: ...
def verify_isolated_venv_install_and_fail_loud_contract(
    core_wheel: Path,
    oracle_wheel: Path,
) -> None: ...
def main() -> int: ...
```

### `.github/workflows/release.yaml`

```yaml
# Under jobs.build_wheels.strategy.matrix.package:
          - name: oracle
            path: src/rs/vertebra-oracle
            module: vertebra._oracle
            exports: SyncOracleClient AsyncOracleClient SyncOraclePool AsyncOraclePool

# Under jobs.build_sdist ("Build source distributions"):
          for package in bigquery bundler oracle spanner storage; do
            uv build "src/rs/vertebra-$package" --sdist --out-dir dist
          done

# Under jobs.test_wheels:
# Install vertebra-oracle and "vertebra-core[bigquery,bundler,oracle,spanner,storage]==$version"
# Verify "oracle": ("SyncOracleClient", "AsyncOracleClient", "SyncOraclePool", "AsyncOraclePool")
# Assert not hasattr(_vertebra, "SyncOracleClient")
```

### `src/py/tests/unit/test_freethreaded_gil.py`

Add `"vertebra._oracle"` to `NATIVE_MODULES` and verify:
- `vertebra._oracle.GIL_USED is False`
- `isinstance(vertebra._oracle.FREE_THREADED, bool)`
- `vertebra._oracle.ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED is False`
- `SyncOracleClient`, `AsyncOracleClient`, `SyncOraclePool`, `AsyncOraclePool`, `OracleColumn`, `OracleQueryResult`, `OraclePipelineOutcome`, `OracleNotification`, and `AsyncOracleListener` are importable native types.

## Edge Cases & Error Semantics

- **Outside-Checkout Working Directory**: `verify_isolated_venv_install_and_fail_loud_contract` must run `python -c ...` with `cwd=temp_dir` (outside the repository root) and `PYTHONNOUSERSITE=1` so local `src/py/vertebra` checkout files cannot mask missing wheel files.
- **No `print()` or Missing `__init__.py` Lint Regressions**: In `tools/scripts/verify_oracle_wheel.py`, write output via `sys.stdout.write` / `sys.stderr.write`, omit any executable file mode bit unless required, and use zero inline `#` comments so `ruff check` passes cleanly without rule ignores.
- **Pure-Rust Shared Library Check**: Inspect dynamic library headers (`readelf -d` / `ldd` on Linux, `otool -L` on macOS) to verify `libclntsh`, `libnnz`, and `libodpic` never appear in `NEEDED` entries.

## Public-Seam Test Blueprint

- `uv run pytest src/py/tests/unit/test_freethreaded_gil.py`: verifies `vertebra._oracle` declares `GIL_USED is False`, matches `sys._is_gil_enabled()`, and constructs `SyncOracleClient` / `AsyncOracleClient` concurrently across threads without re-enabling the GIL.
- `uv run python tools/scripts/verify_oracle_wheel.py` (`make oracle-wheel-check`): builds `vertebra-core` and `vertebra-oracle` wheels and verifies archive contents, pure-Rust linkage, fail-loud `MissingDependencyError` when uninstalled, and clean import when installed.

## Verification

```bash
uv run pytest src/py/tests/unit/test_freethreaded_gil.py
make oracle-wheel-check
make lint-all
```

## Acceptance Criteria

- [ ] `tools/scripts/verify_oracle_wheel.py` (`make oracle-wheel-check`) passes and verifies wheel contents, sibling `_oracle.pyi` exclusion, zero Instant Client linkage, and outside-checkout `MissingDependencyError` / installed import behavior [^ora-contracts].
- [ ] `.github/workflows/release.yaml` includes `oracle` (`src/rs/vertebra-oracle`) in `build_wheels`, `build_sdist`, and `test_wheels` [^release-yaml].
- [ ] `src/py/tests/unit/test_freethreaded_gil.py` verifies `vertebra._oracle` (`GIL_USED == False`) and all exported PyO3 classes [^freethreaded-py].

## Notes & Discoveries

None yet.

[^ora-contracts]: `.agents/bundles/specs/native-oracle-driver/contracts.md` — Native `vertebra._oracle` module exports and wheel isolation contracts.
[^freethreaded-py]: `src/py/tests/unit/test_freethreaded_gil.py::test_native_modules_declare_gil_used_false` — Free-threaded Python `3.14t`/`3.15t` `GIL_USED == False` verification suite.
[^release-yaml]: `.github/workflows/release.yaml` — Multi-wheel `cibuildwheel` and `sdist` release workflow.
