"""Unit tests for ``ConformanceGate`` lane selection, zero-skip enforcement, and ``ServiceVersionMatrix``."""

from __future__ import annotations

import ast
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from tests.integration.conformance_gate import ConformanceGate, ServiceVersionMatrix

_REPO_ROOT = Path(__file__).resolve().parents[5]
_INTEGRATION_ROOT = _REPO_ROOT / "src" / "py" / "tests" / "integration"


class _FakeMarker:
    """Minimal ``pytest.Mark`` double carrying positional arguments."""

    def __init__(self, name: str, *args: Any) -> None:
        self.name = name
        self.args = args


class _FakeItem:
    """Minimal ``pytest.Item`` double for testing ``ConformanceGate`` item selection."""

    def __init__(
        self,
        path: Path,
        *,
        markers: tuple[_FakeMarker, ...] = (),
        params: dict[str, Any] | None = None,
    ) -> None:
        self.path = path
        self._markers = markers
        self.callspec = SimpleNamespace(params=params) if params is not None else None

    def iter_markers(self, name: str | None = None) -> list[_FakeMarker]:
        """Return markers matching ``name``."""
        if name is None:
            return list(self._markers)
        return [marker for marker in self._markers if marker.name == name]


def _make_report(
    *,
    when: str = "call",
    passed: bool = False,
    failed: bool = False,
    skipped: bool = False,
    longrepr: Any = "",
    wasxfail: str | None = None,
) -> Any:
    """Construct a lightweight ``pytest.TestReport`` stand-in."""
    ns = SimpleNamespace(
        when=when,
        passed=passed,
        failed=failed,
        skipped=skipped,
        longrepr=longrepr,
    )
    if wasxfail is not None:
        ns.wasxfail = wasxfail
    return ns


@pytest.mark.parametrize(
    ("environ", "expected_active", "expected_engines"),
    [
        ({}, False, frozenset()),
        ({"VERTEBRA_CONFORMANCE_ENGINES": "   ,  "}, False, frozenset()),
        ({"VERTEBRA_CONFORMANCE_ENGINES": "duckdb"}, True, frozenset({"duckdb"})),
        (
            {"VERTEBRA_CONFORMANCE_ENGINES": " MySQL , MariaDB , mssql , ORACLE , sqlite "},
            True,
            frozenset({"mysql", "mariadb", "mssql", "oracle", "sqlite"}),
        ),
        (
            {"VERTEBRA_CONFORMANCE_ENGINES": "redis"},
            True,
            frozenset({"redis"}),
        ),
        (
            {"VERTEBRA_CONFORMANCE_ENGINES": "valkey"},
            True,
            frozenset({"valkey"}),
        ),
        (
            {
                "VERTEBRA_CONFORMANCE_ENGINES": "postgres",
                "VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS": "1",
            },
            False,
            frozenset(),
        ),
        (
            {
                "VERTEBRA_CONFORMANCE_ENGINES": "postgres, mysql",
                "VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS": "1",
            },
            True,
            frozenset({"mysql"}),
        ),
    ],
)
def test_conformance_gate_from_environ_valid_inputs(
    environ: dict[str, str],
    expected_active: bool,
    expected_engines: frozenset[str],
) -> None:
    """Verify ``ConformanceGate.from_environ`` parses engine lists and delegates PostgreSQL when zero-skip is active."""
    gate = ConformanceGate.from_environ(environ)
    assert gate.active is expected_active
    assert gate.engines == expected_engines


@pytest.mark.parametrize(
    ("environ", "expected_match"),
    [
        (
            {"VERTEBRA_CONFORMANCE_ENGINES": "postgres"},
            r"VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1",
        ),
        (
            {"VERTEBRA_CONFORMANCE_ENGINES": "cockroachdb"},
            r"Unsupported VERTEBRA_CONFORMANCE_ENGINES value\(s\): cockroachdb",
        ),
        (
            {"VERTEBRA_CONFORMANCE_ENGINES": "mysql, informix"},
            r"Allowed engines: duckdb, mariadb, mssql, mysql, oracle, redis, sqlite, valkey",
        ),
    ],
)
def test_conformance_gate_from_environ_rejects_invalid_engines(
    environ: dict[str, str],
    expected_match: str,
) -> None:
    """Verify ``ConformanceGate.from_environ`` raises ``pytest.UsageError`` on standalone ``postgres`` or unknown engines."""
    with pytest.raises(pytest.UsageError, match=expected_match):
        ConformanceGate.from_environ(environ)


def test_conformance_gate_item_engines_and_selects() -> None:
    """Verify ``item_engines`` unions ``driver_engine`` markers and ``.engine`` parameter specs without deselecting unit tests."""
    gate = ConformanceGate.from_environ({"VERTEBRA_CONFORMANCE_ENGINES": "mysql,duckdb"})
    int_file = _INTEGRATION_ROOT / "db" / "test_mysql_native.py"
    unit_file = _REPO_ROOT / "src" / "py" / "tests" / "unit" / "db" / "test_conformance_gate.py"

    marked_item = _FakeItem(
        int_file,
        markers=(_FakeMarker("driver_engine", "MySQL"),),
    )
    param_item = _FakeItem(
        int_file,
        params={"spec": SimpleNamespace(engine="DuckDB")},
    )
    other_int_item = _FakeItem(
        int_file,
        markers=(_FakeMarker("driver_engine", "oracle"),),
    )
    untagged_int_item = _FakeItem(int_file)
    unit_item = _FakeItem(unit_file)

    assert ConformanceGate.item_engines(cast("Any", marked_item)) == frozenset({"mysql"})
    assert ConformanceGate.item_engines(cast("Any", param_item)) == frozenset({"duckdb"})
    assert gate.selects(cast("Any", marked_item), _INTEGRATION_ROOT) is True
    assert gate.selects(cast("Any", param_item), _INTEGRATION_ROOT) is True
    assert gate.selects(cast("Any", other_int_item), _INTEGRATION_ROOT) is False
    assert gate.selects(cast("Any", untagged_int_item), _INTEGRATION_ROOT) is False
    assert gate.selects(cast("Any", unit_item), _INTEGRATION_ROOT) is True


def test_conformance_gate_record_and_failures_detect_skips_xfails_and_zero_runs() -> None:
    """Verify ``ConformanceGate`` flags setup/call skips, xfails, and engines with 0 executed call-phase tests."""
    gate = ConformanceGate.from_environ({"VERTEBRA_CONFORMANCE_ENGINES": "mysql,mariadb,oracle"})
    gate.record(
        "test_mysql.py::test_ok[mysql]",
        frozenset({"mysql"}),
        _make_report(when="call", passed=True),
    )
    gate.record(
        "test_mysql.py::test_skip[mariadb]",
        frozenset({"mariadb"}),
        _make_report(when="setup", skipped=True, longrepr=("file.py", 10, "Skipped: no container")),
    )
    gate.record(
        "test_oracle.py::test_xfail",
        frozenset({"oracle"}),
        _make_report(when="call", skipped=True, wasxfail="pending feature"),
    )

    failures = gate.failures()
    assert len(failures) == 4
    assert any("rejected 1 skipped test(s)" in msg and "no container" in msg for msg in failures)
    assert any("rejected 1 xfail/xpass test(s)" in msg and "pending feature" in msg for msg in failures)
    assert any("engine 'mariadb' executed 0 conformance tests" in msg for msg in failures)
    assert any("engine 'oracle' executed 0 conformance tests" in msg for msg in failures)


@pytest.mark.parametrize(
    ("family", "environ", "expected_fixture"),
    [
        ("mysql", {}, "mysql_84_service"),
        ("mysql", {"VERTEBRA_CONFORMANCE_MYSQL_VERSION": "8.0"}, "mysql_8_service"),
        ("mysql", {"VERTEBRA_CONFORMANCE_MYSQL_VERSION": "8.4"}, "mysql_84_service"),
        ("mysql", {"VERTEBRA_CONFORMANCE_MYSQL_VERSION": "9.6"}, "mysql_96_service"),
        ("mysql", {"VERTEBRA_CONFORMANCE_MYSQL_VERSION": "9.7"}, "mysql_97_service"),
        ("mariadb", {}, "mariadb_114_service"),
        ("mariadb", {"VERTEBRA_CONFORMANCE_MARIADB_VERSION": "11.3"}, "mariadb_113_service"),
        ("mariadb", {"VERTEBRA_CONFORMANCE_MARIADB_VERSION": "11.4"}, "mariadb_114_service"),
        ("mariadb", {"VERTEBRA_CONFORMANCE_MARIADB_VERSION": "11.8"}, "mariadb_118_service"),
        ("mariadb", {"VERTEBRA_CONFORMANCE_MARIADB_VERSION": "12.2"}, "mariadb_122_service"),
        ("mariadb", {"VERTEBRA_CONFORMANCE_MARIADB_VERSION": "12.3"}, "mariadb_123_service"),
        ("mariadb", {"VERTEBRA_CONFORMANCE_MARIADB_VERSION": "13.0"}, "mariadb_130_service"),
        ("mssql", {}, "mssql_2022_service"),
        ("mssql", {"VERTEBRA_CONFORMANCE_MSSQL_VERSION": "2022"}, "mssql_2022_service"),
        ("mssql", {"VERTEBRA_CONFORMANCE_MSSQL_VERSION": "2025"}, "mssql_2025_service"),
        ("oracle", {}, "oracle_26ai_service"),
        ("oracle", {"VERTEBRA_CONFORMANCE_ORACLE_VERSION": "23ai"}, "oracle_26ai_service"),
        ("oracle", {"VERTEBRA_CONFORMANCE_ORACLE_VERSION": "26ai"}, "oracle_26ai_service"),
    ],
)
def test_service_version_matrix_resolves_fixture_names(
    family: str,
    environ: dict[str, str],
    expected_fixture: str,
) -> None:
    """Verify ``ServiceVersionMatrix.fixture_name`` maps defaults and explicit versions onto ``pytest-databases`` fixtures."""
    assert ServiceVersionMatrix.fixture_name(family, environ) == expected_fixture


@pytest.mark.parametrize(
    ("family", "environ", "expected_match"),
    [
        ("spanner", {}, r"Unsupported service family 'spanner'"),
        (
            "mysql",
            {"VERTEBRA_CONFORMANCE_MYSQL_VERSION": "5.5"},
            r"Unsupported VERTEBRA_CONFORMANCE_MYSQL_VERSION='5.5' for mysql\. Allowed versions: 8\.0, 8\.4, 9\.6, 9\.7",
        ),
        (
            "mariadb",
            {"VERTEBRA_CONFORMANCE_MARIADB_VERSION": "10.5"},
            r"Unsupported VERTEBRA_CONFORMANCE_MARIADB_VERSION='10.5' for mariadb",
        ),
        (
            "mssql",
            {"VERTEBRA_CONFORMANCE_MSSQL_VERSION": "2019"},
            r"Allowed versions: 2022, 2025",
        ),
        (
            "oracle",
            {"VERTEBRA_CONFORMANCE_ORACLE_VERSION": "19c"},
            r"Allowed versions: 23ai, 26ai",
        ),
    ],
)
def test_service_version_matrix_rejects_unknown_family_or_version(
    family: str,
    environ: dict[str, str],
    expected_match: str,
) -> None:
    """Verify ``ServiceVersionMatrix.fixture_name`` raises ``pytest.UsageError`` on unknown families or versions."""
    with pytest.raises(pytest.UsageError, match=expected_match):
        ServiceVersionMatrix.fixture_name(family, environ)


@pytest.mark.parametrize(
    ("rel_path", "expected_engine_tokens"),
    [
        ("db/test_duckdb_native.py", ("duckdb",)),
        ("db/test_mysql_native.py", ("mysql", "mariadb")),
        ("db/test_mssql_native.py", ("mssql",)),
        ("db/test_oracle_native.py", ("oracle",)),
    ],
)
def test_native_integration_suites_declare_driver_engine_markers_and_no_for_loops_in_tests(
    rel_path: str,
    expected_engine_tokens: tuple[str, ...],
) -> None:
    """Verify native integration test modules declare ``driver_engine`` markers and avoid top-level ``for`` loops in tests."""
    suite_path = _INTEGRATION_ROOT / rel_path
    source = suite_path.read_text(encoding="utf-8")
    for token in expected_engine_tokens:
        assert f'driver_engine("{token}")' in source

    if rel_path in {"db/test_duckdb_native.py", "db/test_mysql_native.py", "db/test_mssql_native.py"}:
        tree = ast.parse(source, filename=str(suite_path))
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("test_"):
                for_nodes = [child for child in ast.walk(node) if isinstance(child, (ast.For, ast.AsyncFor))]
                assert not for_nodes, f"{suite_path.name}::{node.name} contains a forbidden for-loop"
