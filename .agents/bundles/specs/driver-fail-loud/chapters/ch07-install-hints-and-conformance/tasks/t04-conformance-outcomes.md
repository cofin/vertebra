---
type: Task
id: "driver-fail-loud:ch07:t04"
chapter_id: ch07-install-hints-and-conformance
title: Skip or fail conformance explicitly when a native module is missing
description: Resolve each conformance spec's native module before credential skips, skip the case when it is missing (failing it under VERTEBRA_CONFORMANCE_DOCKER=1 only for non-pending engines), apply strict xfail to pending engines only when their native module imports, drop from __future__ from the integration conftest, and delete its unused vtb_oracle_driver and vtb_mssql_driver fixtures.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - src/py/tests/integration/db/test_driver_conformance.py
  - src/py/tests/integration/conftest.py
  - src/py/vertebra/utils/module_loader.py
  - .agents/bundles/knowledge/testing/polyglot-test-matrix.md
files:
  - src/py/tests/integration/db/test_driver_conformance.py
  - src/py/tests/integration/conftest.py
tests:
  - src/py/tests/integration/db/test_driver_conformance.py
estimated_loc: 150
plan_revision: 1
plan_commit: 0acdf576
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-07T00:00:00Z"
updated_at: "2026-11-07T00:00:00Z"
commit: null
sources:
  - id: conformance
    resource: src/py/tests/integration/db/test_driver_conformance.py::_resolve_live_config
    title: Conformance live config resolution
  - id: conformance-params
    resource: src/py/tests/integration/db/test_driver_conformance.py::_build_live_engine_params
    title: Pending-engine strict xfail parameter builder
  - id: integration-conftest
    resource: src/py/tests/integration/conftest.py
    title: pytest-databases Docker fixtures
  - id: module-loader
    resource: src/py/vertebra/utils/module_loader.py::import_optional
    title: Optional module import returning None when unavailable
tags:
  - conformance
  - testing
  - pytest
  - python
---

# Task t04: Explicit Conformance Outcomes for Missing Native Modules

## Objective & Scope Boundary

Replace the bare `ensure_module` call at the top of `_resolve_live_config` with `_require_native_module(spec)`, which skips the case when the spec's native module does not import. Under `VERTEBRA_CONFORMANCE_DOCKER == "1"` it fails the case instead, but only for a non-pending spec (`pending_flow_id is None`: `bigquery`, `spanner`, and any engine whose native flow has shipped and cleared `pending_flow_id`). A pending spec whose module is missing skips in both modes. Reason: pending native modules (`vertebra._postgres`, `vertebra._oracle`, `vertebra._mysql`, `vertebra._mssql`, `vertebra._duckdb`) do not exist yet, so a Docker run that failed on them would fail on every host until each native flow ships. Make `_build_live_engine_params` attach strict `xfail` to a pending engine only when its native module imports, so a missing-module skip or fail is never converted to `xfailed`. Remove `from __future__ import annotations` from `src/py/tests/integration/conftest.py`, and delete its `vtb_oracle_driver` and `vtb_mssql_driver` fixtures with the `SyncOracleDriver` and `SyncMSSQLDriver` imports. Reason: no test under `src/py/tests` requests either fixture, and each constructs a pure-Python driver that ch01 and ch02 make fail loud.[^conformance][^conformance-params][^integration-conftest]

The helper unit tests live in `test_driver_conformance.py` beside the private helpers. Reason: the helpers are module-private, `make py-test` already collects `src/py/tests/integration`, and a separate unit module would import private names from an integration test module.

Out of scope: `test_engine_driver_resolution_and_provide_driver` (owned by ch01 / ch02); `CACHE_SPECS` and cache fixtures; removing `from __future__ import annotations` from `test_driver_conformance.py` (its `EngineConformanceSpec.bind_scalar_case` annotation references `ScalarTypeCase` before definition); engine extras.

## Bounded Read Manifest

- `src/py/tests/integration/db/test_driver_conformance.py::EngineConformanceSpec`, `ENGINE_SPECS`, `_build_live_engine_params`, `_resolve_live_config`, `sync_sql_driver`, `async_sql_driver`.[^conformance][^conformance-params]
- `src/py/tests/integration/conftest.py` — seven fixtures annotated with `TYPE_CHECKING`-only service types; `vtb_oracle_driver` and `vtb_mssql_driver`, which no test requests.[^integration-conftest]
- `src/py/vertebra/utils/module_loader.py::import_optional`, `ensure_module`.[^module-loader]
- `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` — integration suite placement and `make py-test` scope.

## Symbol & Signature Contracts

In `src/py/tests/integration/db/test_driver_conformance.py` (add `Sequence` to the `collections.abc` import, `import_optional` to the `vertebra.utils.module_loader` import, and `from msgspec import structs` for the tests; place helpers between `ENGINE_SPECS` and the fixtures, in call order):

```python
_CONFORMANCE_DOCKER_ENV = "VERTEBRA_CONFORMANCE_DOCKER"


def _build_live_engine_params(specs: Sequence[EngineConformanceSpec] = ENGINE_SPECS) -> list[Any]:
    """Build fixture parameters, marking pending engines strict ``xfail`` only when their native module imports.

    Args:
        specs: Engine specs to parametrize.

    Returns:
        One ``pytest.param`` per spec with ``id=spec.engine``.
    """
    params: list[Any] = []
    for spec in specs:
        if spec.pending_flow_id is not None and not _native_module_missing(spec):
            params.append(
                pytest.param(
                    spec,
                    id=spec.engine,
                    marks=pytest.mark.xfail(strict=True, reason=f"Owned by {spec.pending_flow_id}"),
                )
            )
        else:
            params.append(pytest.param(spec, id=spec.engine))
    return params


def _native_module_missing(spec: EngineConformanceSpec) -> bool:
    """Return True when ``spec`` names a native module that does not import."""
    return spec.native_extension_module is not None and import_optional(spec.native_extension_module) is None


def _require_native_module(spec: EngineConformanceSpec) -> None:
    """Skip the case, or fail a non-pending case under Docker conformance, when the native module is missing.

    Args:
        spec: Engine spec whose ``native_extension_module`` is checked.

    Raises:
        pytest.skip.Exception: When the module is missing and either the spec is pending or
            ``VERTEBRA_CONFORMANCE_DOCKER`` is not ``"1"``.
        pytest.fail.Exception: When the module is missing, the spec is not pending, and
            ``VERTEBRA_CONFORMANCE_DOCKER`` is ``"1"``.
    """
    if spec.native_extension_module is None:
        return
    try:
        ensure_module(spec.native_extension_module)
    except MissingDependencyError:
        reason = f"{spec.engine} conformance requires the {spec.native_extension_module} native module"
        if spec.pending_flow_id is None and os.environ.get(_CONFORMANCE_DOCKER_ENV) == "1":
            pytest.fail(reason, pytrace=False)
        pytest.skip(reason)
```

- `_resolve_live_config(spec, tmp_path, request) -> BaseDatabaseConfig` — first statement becomes `_require_native_module(spec)`; the `ensure_module(..., package=f"vertebra-core[{spec.engine}]", extra=spec.engine)` block is deleted; BigQuery / Spanner credential skips, env DSN lookup, Docker fixture branch (switch its literal to `_CONFORMANCE_DOCKER_ENV`), and `sample_url` fallback stay in order. Update the docstring to say native modules resolve before credential checks.
- `sync_sql_driver` / `async_sql_driver` keep `@pytest.fixture(params=_build_live_engine_params())`.
- `_build_live_engine_params` stays defined after `ENGINE_SPECS` so the default argument binds the module tuple.

In `src/py/tests/integration/conftest.py`:

- delete `from __future__ import annotations` and quote each `TYPE_CHECKING`-only parameter annotation: `"PostgresService"`, `"MySQLService"`, `"MariaDBService"`, `"OracleService"`, `"MSSQLService"`, `"RedisService"`, `"ValkeyService"`. Return annotations name runtime imports and stay unquoted;
- delete the `vtb_oracle_driver` and `vtb_mssql_driver` fixtures; the imports become `from vertebra.db.adapters.mssql import MSSQLConfig` and `from vertebra.db.adapters.oracle import OracleConfig`. `vtb_oracle_config`, `vtb_mssql_config`, and `vtb_sqlspec_oracle_config` stay.

## Edge Cases & Error Semantics

- Local run, module missing: SKIPPED with `<engine> conformance requires the <module> native module`; no setup ERROR.
- Docker run (`VERTEBRA_CONFORMANCE_DOCKER=1`), non-pending engine (`bigquery`, `spanner`), module missing: setup ERROR from `pytest.fail` with the same reason and no traceback; pytest exits non-zero.
- Docker run, pending engine (`postgres`, `oracle`, `mysql`, `mssql`, `duckdb`), module missing: SKIPPED with the same reason. Pending engines get no `xfail` mark in this state, so the skip is not reported as `xfailed`.
- `VERTEBRA_CONFORMANCE_DOCKER` set to `"0"`, `"true"`, or `""`: skip (exact match on `"1"`, like the Docker fixture branch).
- Pending engine whose module imports: strict `xfail` (`Owned by <pending_flow_id>`); an unexpected pass fails as XPASS(strict).
- Module present but raising `ImportError` on import: `import_optional` returns `None` at collection (no `xfail`) and `ensure_module` raises at runtime (skip or fail); both checks agree.
- Spec with `native_extension_module=None` (SQLite): no check, no mark.
- BigQuery without `vertebra._bigquery` and without `VERTEBRA_CONFORMANCE_BIGQUERY_DSN`: the native-module skip fires first.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/integration/db/test_driver_conformance.py` (in-process; specs built with `structs.replace(<ENGINE_SPECS entry>, ...)`; absent module name `vertebra._vtb_conformance_absent`; present module name `json`; env controlled with `monkeypatch.setenv` / `monkeypatch.delenv(..., raising=False)`).
- New tests: `test_require_native_module_outcome_by_docker_mode` (parametrized over the first seven rows), `test_live_engine_params_mark_strict_xfail_only_when_native_module_imports`, `test_resolve_live_config_checks_native_module_before_credentials`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Missing, local | sqlite spec with `native_extension_module="vertebra._vtb_conformance_absent"` (`pending_flow_id=None`); `VERTEBRA_CONFORMANCE_DOCKER` unset | raises `pytest.skip.Exception`; `str(exc) == "sqlite conformance requires the vertebra._vtb_conformance_absent native module"` |
| Missing, Docker | same; `VERTEBRA_CONFORMANCE_DOCKER="1"` | raises `pytest.fail.Exception` with the same message |
| Missing, Docker env `"0"` | same; `VERTEBRA_CONFORMANCE_DOCKER="0"` | raises `pytest.skip.Exception` |
| Missing pending, local | same plus `pending_flow_id="native-probe-driver"`; `VERTEBRA_CONFORMANCE_DOCKER` unset | raises `pytest.skip.Exception` with the same message |
| Missing pending, Docker | same pending spec; `VERTEBRA_CONFORMANCE_DOCKER="1"` | raises `pytest.skip.Exception` with the same message, never `pytest.fail.Exception` |
| Present | sqlite spec with `native_extension_module="json"`; `VERTEBRA_CONFORMANCE_DOCKER="1"` | returns `None` |
| No module | unmodified sqlite spec (`native_extension_module=None`); `VERTEBRA_CONFORMANCE_DOCKER="1"` | returns `None` |
| Params | `_build_live_engine_params((absent_pending, present_pending, absent_plain))` where `absent_pending` has `engine="absent"`, module `vertebra._vtb_conformance_absent`, `pending_flow_id="native-probe-driver"`; `present_pending` has `engine="present"`, module `json`, same flow; `absent_plain` has `engine="plain"`, absent module, `pending_flow_id=None` | ids `["absent", "present", "plain"]`; marks `[[], ["xfail"], []]`; `present` mark kwargs `{"strict": True, "reason": "Owned by native-probe-driver"}` |
| Resolution order | bigquery `ENGINE_SPECS` entry with `native_extension_module="vertebra._vtb_conformance_absent"`; `VERTEBRA_CONFORMANCE_BIGQUERY_DSN` and `VERTEBRA_CONFORMANCE_DOCKER` unset; `_resolve_live_config(spec, tmp_path, request)` | raises `pytest.skip.Exception` with `bigquery conformance requires the vertebra._vtb_conformance_absent native module`, not `BigQuery conformance requires VERTEBRA_CONFORMANCE_BIGQUERY_DSN` |
| Suite outcome | `uv run --no-sync pytest src/py/tests/integration/db/test_driver_conformance.py -q -rs` on a host without `vertebra._postgres` | `postgres` cases listed as SKIPPED with the native-module reason; no ERROR rows for missing modules |
| Unused driver fixtures removed | `grep -n -e vtb_oracle_driver -e vtb_mssql_driver -e SyncOracleDriver -e SyncMSSQLDriver src/py/tests/integration/conftest.py` after the change; `uv run --no-sync pytest src/py/tests/integration --collect-only -q` | grep prints nothing (exit `1`); collection exits `0` |
| Docker suite outcome, pending | `VERTEBRA_CONFORMANCE_DOCKER=1` and the same host | `postgres` and `duckdb` cases still SKIPPED with the native-module reason; no ERROR and no `xfailed` rows for pending engines |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/integration/db/test_driver_conformance.py -q -k "require_native_module or live_engine_params or before_credentials"` fails (helpers undefined; `_build_live_engine_params` takes no argument).
- Final Green + lint: `uv run --no-sync ruff check src/py/tests/integration/db/test_driver_conformance.py src/py/tests/integration/conftest.py && uv run --no-sync ruff format --check src/py/tests/integration/db/test_driver_conformance.py src/py/tests/integration/conftest.py && uv run --no-sync pytest src/py/tests/integration/db/test_driver_conformance.py -q -rs`
- Docker-mode pending check (expected exit `0` with skips): `VERTEBRA_CONFORMANCE_DOCKER=1 uv run --no-sync pytest "src/py/tests/integration/db/test_driver_conformance.py::test_sync_execution_contract" -q -rs -k postgres` reports SKIPPED with `postgres conformance requires the vertebra._postgres native module` and no `xfailed`.
- Docker-mode non-pending check (expected non-zero exit; `vertebra._bigquery` is masked through `sys.modules`, so `_vertebra` and the rest of the debug build stay importable): `VERTEBRA_CONFORMANCE_DOCKER=1 uv run --no-sync python -c "import sys; sys.modules['vertebra._bigquery'] = None; import pytest; raise SystemExit(pytest.main(['src/py/tests/integration/db/test_driver_conformance.py::test_sync_execution_contract', '-q', '-k', 'bigquery']))"` reports an ERROR for `test_sync_execution_contract[bigquery]` containing `bigquery conformance requires the vertebra._bigquery native module`. Collection succeeds with the mask because `vertebra.db.adapters.bigquery` imports `vertebra._bigquery` only under `TYPE_CHECKING` or through `ensure_module`.
- Conftest import check: `uv run --no-sync pytest src/py/tests/integration --collect-only -q` exits `0`.

## Acceptance Criteria

- [ ] `_resolve_live_config` calls `_require_native_module` before any credential skip (R12).
- [ ] A missing native module skips locally; under `VERTEBRA_CONFORMANCE_DOCKER=1` it fails non-pending engines and still skips pending engines (R12).
- [ ] Strict `xfail` applies to pending engines only when their native module imports (R12).
- [ ] `src/py/tests/integration/conftest.py` has no `from __future__ import annotations`, no `vtb_oracle_driver` / `vtb_mssql_driver` fixtures or `SyncOracleDriver` / `SyncMSSQLDriver` imports, and collects cleanly.
- [ ] Zero inline `#` comments in touched files.

## Notes & Discoveries

- [2026-11-07 00:00] pytest reports any setup exception under an `xfail` mark, including `pytest.fail`, as `xfailed`; `pytest.skip` is reported as skipped. Gating the `xfail` mark on module presence is what makes Docker-mode failures visible.
- [2026-11-07 00:00] `VERTEBRA_CONFORMANCE_DOCKER=1` runs skip pending engines (`postgres`, `oracle`, `mysql`, `mssql`, `duckdb`) whose native module is absent; each native flow clears `pending_flow_id` when its module ships, which turns a missing module into a Docker-mode failure. The DuckDB case stays pending on `vertebra._duckdb` even though the `duckdb` package is installed.

[^conformance]: `src/py/tests/integration/db/test_driver_conformance.py::_resolve_live_config` — Conformance live config resolution.
[^conformance-params]: `src/py/tests/integration/db/test_driver_conformance.py::_build_live_engine_params` — Pending-engine strict xfail parameter builder.
[^integration-conftest]: `src/py/tests/integration/conftest.py` — pytest-databases Docker fixtures.
[^module-loader]: `src/py/vertebra/utils/module_loader.py::import_optional` — Optional module import returning None when unavailable.
