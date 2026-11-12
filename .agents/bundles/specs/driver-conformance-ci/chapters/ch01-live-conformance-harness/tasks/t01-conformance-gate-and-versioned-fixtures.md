---
type: Task
id: "driver-conformance-ci:ch01:t01"
chapter_id: ch01-live-conformance-harness
title: Add the conformance gate, engine tagging, versioned service fixtures, and conftest hooks
description: Create ConformanceGate and ServiceVersionMatrix in an importable integration helper module, wire marker registration, deselection, report recording, and session-failure hooks into the integration conftest, and route MySQL, MariaDB, SQL Server, and Oracle fixtures through versioned pytest-databases services.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - src/py/tests/integration/conftest.py
  - src/py/tests/integration/db/test_driver_conformance.py
  - .venv/lib/python3.12/site-packages/pytest_databases/docker/mysql.py
  - .venv/lib/python3.12/site-packages/pytest_databases/docker/mariadb.py
  - .venv/lib/python3.12/site-packages/pytest_databases/docker/mssql.py
  - .agents/bundles/specs/driver-conformance-ci/chapters/ch01-live-conformance-harness/spec.md
files:
  - src/py/tests/integration/conformance_gate.py
  - src/py/tests/integration/conftest.py
tests:
  - src/py/tests/integration/test_conformance_gate.py
estimated_loc: 240
plan_revision: 1
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T00:30:00Z"
updated_at: "2026-11-12T00:30:00Z"
commit: null
sources:
  - id: int-conftest
    resource: src/py/tests/integration/conftest.py::vtb_mysql_config
    title: Integration pytest-databases service fixtures
  - id: conformance-specs
    resource: src/py/tests/integration/db/test_driver_conformance.py::EngineConformanceSpec
    title: Conformance spec structs carrying an engine attribute
  - id: pd-mysql
    resource: .venv/lib/python3.12/site-packages/pytest_databases/docker/mysql.py::mysql_97_service
    title: Versioned MySQL service fixtures
tags:
  - conformance
  - pytest
  - harness
---

# Task t01: Add the Conformance Gate, Engine Tagging, Versioned Service Fixtures, and Conftest Hooks

## Objective & Scope Boundary

Create `src/py/tests/integration/conformance_gate.py` holding `ConformanceGate` and `ServiceVersionMatrix`, add its canonical test file `src/py/tests/integration/test_conformance_gate.py`, and wire `src/py/tests/integration/conftest.py`:

1. `pytest_configure(config)` registers the `driver_engine(name): tag a live integration test with the database engine it exercises` marker and stores `ConformanceGate.from_environ(os.environ)` under a module-level `pytest.StashKey[ConformanceGate]`.
2. `pytest_collection_modifyitems(session, config, items)` deselects items for which `gate.selects(item, Path(__file__).parent)` is `False`, reporting them through `config.hook.pytest_deselected(items=deselected)` and mutating `items[:]` in place.
3. A `@pytest.hookimpl(wrapper=True)` `pytest_runtest_makereport(item, call)` records each report through `gate.record(report.nodeid, ConformanceGate.item_engines(item), report)` and returns the report.
4. `pytest_sessionfinish(session, exitstatus)` writes each `gate.failures()` line through the terminal reporter (`config.pluginmanager.get_plugin("terminalreporter")`) under a `"driver conformance gate"` section and sets `session.exitstatus = pytest.ExitCode.TESTS_FAILED` when failures exist.
5. `vtb_mysql_config`, `vtb_mariadb_config`, `vtb_mssql_config`, and `vtb_oracle_config` take `request: pytest.FixtureRequest` and resolve their service with `request.getfixturevalue(ServiceVersionMatrix.fixture_name("<family>", os.environ))`, keeping today's config construction.

Out of scope: PostgreSQL, Redis, and Valkey fixtures (unchanged), conformance matrix edits (`t02`), suite tagging (ch02/ch03), Make and CI (ch04).

## Bounded Read Manifest

- `src/py/tests/integration/conftest.py` — `vtb_mysql_config`, `vtb_mariadb_config`, `vtb_mssql_config`, `vtb_oracle_config`, `TYPE_CHECKING` service imports [^int-conftest].
- `src/py/tests/integration/db/test_driver_conformance.py` — `EngineConformanceSpec.engine`, `CacheConformanceSpec.engine`, `sync_sql_driver` parametrization [^conformance-specs].
- `pytest_databases/docker/mysql.py` — `mysql_8_service`, `mysql_84_service`, `mysql_96_service`, `mysql_97_service` [^pd-mysql].
- `pytest_databases/docker/mariadb.py` — `mariadb_113_service` … `mariadb_130_service`.
- `pytest_databases/docker/mssql.py` — `mssql_2022_service`, `mssql_2025_service`.
- `.agents/bundles/specs/driver-conformance-ci/chapters/ch01-live-conformance-harness/spec.md` — edge-case matrix.

## Symbol & Signature Contracts

```python
"""Live database conformance lane selection, zero-skip enforcement, and service version resolution."""

from collections.abc import Mapping
from pathlib import Path
from typing import ClassVar

import pytest

__all__ = ("ConformanceGate", "ServiceVersionMatrix")


class ConformanceGate:
    """Selects integration items by database engine and enforces zero-skip live evidence."""

    ENGINES_ENV: ClassVar[str] = "VERTEBRA_CONFORMANCE_ENGINES"
    ENGINE_MARKER: ClassVar[str] = "driver_engine"
    SUPPORTED_ENGINES: ClassVar[frozenset[str]] = frozenset({"duckdb", "mariadb", "mssql", "mysql", "oracle"})

    def __init__(self, engines: frozenset[str]) -> None: ...
    @classmethod
    def from_environ(cls, environ: Mapping[str, str]) -> "ConformanceGate": ...
    @property
    def engines(self) -> frozenset[str]: ...
    @property
    def active(self) -> bool: ...
    @staticmethod
    def item_engines(item: pytest.Item) -> frozenset[str]: ...
    def selects(self, item: pytest.Item, integration_root: Path) -> bool: ...
    def record(self, nodeid: str, engines: frozenset[str], report: pytest.TestReport) -> None: ...
    def failures(self) -> list[str]: ...


class ServiceVersionMatrix:
    """Maps engine families and version strings onto pytest-databases service fixture names."""

    FIXTURES: ClassVar[dict[str, dict[str, str]]] = {
        "mysql": {"8.0": "mysql_8_service", "8.4": "mysql_84_service", "9.6": "mysql_96_service", "9.7": "mysql_97_service"},
        "mariadb": {"11.3": "mariadb_113_service", "11.4": "mariadb_114_service", "11.8": "mariadb_118_service", "12.2": "mariadb_122_service", "12.3": "mariadb_123_service", "13.0": "mariadb_130_service"},
        "mssql": {"2022": "mssql_2022_service", "2025": "mssql_2025_service"},
        "oracle": {"23ai": "oracle_23ai_service"},
    }
    DEFAULTS: ClassVar[dict[str, str]] = {"mysql": "8.4", "mariadb": "11.4", "mssql": "2022", "oracle": "23ai"}

    @classmethod
    def fixture_name(cls, family: str, environ: Mapping[str, str]) -> str: ...
```

- `from_environ` splits on `,`, strips, lowercases, drops empties; `"postgres"` raises `pytest.UsageError("VERTEBRA_CONFORMANCE_ENGINES does not cover postgres; PostgreSQL live conformance is owned by native-postgres-driver (VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS)")`; other unknown names raise `pytest.UsageError("VERTEBRA_CONFORMANCE_ENGINES contains unsupported engine(s): <sorted, comma-joined>; supported: duckdb, mariadb, mssql, mysql, oracle")`.
- `item_engines` unions every `item.iter_markers(ConformanceGate.ENGINE_MARKER)` first positional arg with every `getattr(value, "engine", None)` that is a `str` among `item.callspec.params.values()` when `item` has `callspec`.
- `selects` returns `True` when the gate is inactive or `item.path` is not under `integration_root`; otherwise `bool(item_engines(item) & engines)`.
- `record` ignores reports whose engines do not intersect `engines`; adds `"<nodeid>: skipped"` when `report.skipped` and no `wasxfail`; `"<nodeid>: xfailed"` when `report.skipped` with `wasxfail`; `"<nodeid>: xpassed"` when `report.passed` with `wasxfail`; marks each intersecting engine executed when `report.when == "call"` and the report passed or failed without `wasxfail`.
- `failures` returns sorted violations followed by `"<engine>: 0 tests executed"` for each listed engine never executed; empty when inactive.
- `fixture_name` reads `VERTEBRA_CONFORMANCE_<FAMILY>_VERSION` (stripped; empty uses `DEFAULTS[family]`); unknown family raises `KeyError`-free `pytest.UsageError("Unknown service family: <family>")`; unknown version raises `pytest.UsageError("VERTEBRA_CONFORMANCE_<FAMILY>_VERSION=<v> is not supported; choose one of: <keys in insertion order>")`.
- Classes follow attributes → `__init__` → properties → public → private ordering, Google docstrings, no inline comments, PEP 604 types, module-level imports only.

## Edge Cases & Error Semantics

- Duplicate names (`"mysql,mysql"`) collapse to one engine.
- A conformance item parametrized with `EngineConformanceSpec(engine="sqlite")` is deselected when the gate lists `mysql`.
- A teardown-phase skip on a tagged item is recorded as a violation; a passing setup report does not mark the engine executed.
- An `xfail(strict=True)` failure surfaces as `report.failed` without `wasxfail` and marks the engine executed (pytest already fails the run).
- `pytest_sessionfinish` never lowers a non-zero `exitstatus`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/integration/test_conformance_gate.py` (new canonical module test file for `conformance_gate.py`). Dependency category: in-process; items and reports are built from minimal `msgspec.Struct`-free stand-ins exposing `path`, `callspec.params`, `iter_markers`, `nodeid`, `when`, `skipped`, `passed`, `failed`, and optional `wasxfail` (use `types.SimpleNamespace` and `pytest.mark.driver_engine("mysql").mark`).
- All cases use `@pytest.mark.parametrize`; no loops over cases.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Inactive gate | `ConformanceGate.from_environ({})` | `active is False`, `engines == frozenset()`, `failures() == []` |
| Parse with spaces/case | `{"VERTEBRA_CONFORMANCE_ENGINES": " MySQL , mssql ,"}` | `engines == frozenset({"mysql", "mssql"})` |
| Unsupported engine | `{"VERTEBRA_CONFORMANCE_ENGINES": "mysql,sybase"}` | `pytest.UsageError` matching `unsupported engine\(s\): sybase` |
| PostgreSQL boundary | `{"VERTEBRA_CONFORMANCE_ENGINES": "postgres"}` | `pytest.UsageError` matching `native-postgres-driver` |
| Marker tagging | item with `driver_engine("oracle")` marker, no callspec | `item_engines == frozenset({"oracle"})` |
| Callspec tagging | item with `callspec.params == {"spec": SimpleNamespace(engine="mariadb"), "type_case": SimpleNamespace(name="null")}` | `frozenset({"mariadb"})` |
| Deselect untagged integration item | gate `{"mysql"}`, item path `<root>/db/test_x.py`, no engines | `selects(...) is False` |
| Keep unit item | gate `{"mysql"}`, item path `/repo/src/py/tests/unit/test_y.py` | `selects(...) is True` |
| Skip violation | gate `{"mssql"}`, record setup report `skipped=True` for `t.py::a[mssql]` | `failures() == ["t.py::a[mssql]: skipped", "mssql: 0 tests executed"]` |
| Xfail violation | report `when="call"`, `skipped=True`, `wasxfail="Owned by x"` | `"t.py::b[mysql]: xfailed"` in failures |
| Executed engine | report `when="call"`, `passed=True` for engines `{"duckdb"}` | `failures() == []` |
| Version default | `ServiceVersionMatrix.fixture_name("mssql", {})` | `"mssql_2022_service"` |
| Version override | `fixture_name("mysql", {"VERTEBRA_CONFORMANCE_MYSQL_VERSION": "9.7"})` | `"mysql_97_service"` |
| Unknown version | `fixture_name("mariadb", {"VERTEBRA_CONFORMANCE_MARIADB_VERSION": "10.6"})` | `pytest.UsageError` matching `choose one of: 11.3, 11.4, 11.8, 12.2, 12.3, 13.0` |

## Verification

- Red: `uv run --no-sync pytest src/py/tests/integration/test_conformance_gate.py -q` fails (module missing).
- Green + offline safety: `make dev && uv run --no-sync pytest src/py/tests/integration/test_conformance_gate.py src/py/tests/integration/db/test_driver_conformance.py -q -rs` exits `0` with no Docker calls.
- Gate smoke (no Docker): `VERTEBRA_CONFORMANCE_ENGINES=mssql uv run --no-sync pytest src/py/tests/integration/db/test_driver_conformance.py -q -rs` exits `1` and prints the `driver conformance gate` section listing skipped `[mssql]` items.
- Lint: `uv run --no-sync ruff check src/py/tests/integration && uv run --no-sync ruff format --check src/py/tests/integration`.

## Acceptance Criteria

- [ ] `ConformanceGate` and `ServiceVersionMatrix` match the contracts and every Worked Test Matrix row passes (R1, R2, R3).
- [ ] Integration conftest registers `driver_engine`, deselects, records, and fails the session per the gate; the gate smoke command exits `1` (R1).
- [ ] MySQL, MariaDB, SQL Server, and Oracle fixtures resolve through `ServiceVersionMatrix`; PostgreSQL, Redis, and Valkey fixtures are byte-identical (R3).

## Notes & Discoveries

- [2026-11-12 00:30] `pytest-databases` 0.20.0b1 exposes `oracle_23ai_service` (not `oracle_26ai_service`); keep the `23ai` key aligned with the installed fixture name.

[^int-conftest]: `src/py/tests/integration/conftest.py::vtb_mysql_config` — Integration `pytest-databases` service fixtures.
[^conformance-specs]: `src/py/tests/integration/db/test_driver_conformance.py::EngineConformanceSpec` — Conformance spec structs carrying `engine`.
[^pd-mysql]: `pytest_databases/docker/mysql.py::mysql_97_service` — Versioned MySQL service fixtures.
