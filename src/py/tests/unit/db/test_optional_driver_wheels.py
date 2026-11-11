"""Unit tests for optional database driver wheel isolation and `MissingDependencyError` contracts."""

from __future__ import annotations

import sys
import tomllib
from pathlib import Path
from typing import Any

import pytest

from vertebra.db.adapters.duckdb import AsyncDuckDBDriver, DuckDBConfig, SyncDuckDBDriver
from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import reset_dependency_cache

_REPO_ROOT = Path(__file__).resolve().parents[5]

SIBLING_WHEEL_MANIFESTS: tuple[tuple[str, str, str], ...] = (
    ("vertebra-duckdb", "src/rs/vertebra-duckdb/pyproject.toml", "_duckdb.pyi"),
    ("vertebra-mysql", "src/rs/vertebra-mysql/pyproject.toml", "_mysql.pyi"),
    ("vertebra-bigquery", "src/rs/vertebra-bigquery/pyproject.toml", "_bigquery.pyi"),
    ("vertebra-spanner", "src/rs/vertebra-spanner/pyproject.toml", "_spanner.pyi"),
    ("vertebra-storage", "src/rs/vertebra-storage/pyproject.toml", "_storage.pyi"),
    ("vertebra-bundler", "src/rs/vertebra-bundler/pyproject.toml", "_bundler.pyi"),
)


def test_root_pyproject_omits_pypi_duckdb_from_core_and_non_duckdb_groups() -> None:
    """Root `pyproject.toml` declares `vertebra-core[duckdb]` matching project version and isolates PyPI `duckdb`."""
    data = tomllib.loads((_REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    version = data["project"]["version"]
    pinned = f"vertebra-duckdb=={version}"
    opt_deps = data["project"]["optional-dependencies"]
    assert opt_deps["duckdb"] == [pinned]
    assert pinned in opt_deps["all"]

    uv_sources = data["tool"]["uv"]["sources"]
    assert uv_sources["vertebra-duckdb"] == {"path": "src/rs/vertebra-duckdb", "editable": True}

    core_deps = data["project"].get("dependencies", [])
    assert not any(str(dep).startswith("duckdb") and not str(dep).startswith("vertebra-duckdb") for dep in core_deps)

    for group_name, group_deps in data.get("dependency-groups", {}).items():
        if group_name == "duckdb":
            continue
        assert not any(
            isinstance(dep, str) and dep.startswith("duckdb") and not dep.startswith("vertebra-duckdb")
            for dep in group_deps
        ), f"dependency-groups.{group_name} unexpectedly contains PyPI duckdb"


def test_vertebra_duckdb_wheel_pyproject_includes_only_duckdb_stub() -> None:
    """`src/rs/vertebra-duckdb/pyproject.toml` ships only `vertebra._duckdb` and `_duckdb.pyi`."""
    data = tomllib.loads((_REPO_ROOT / "src/rs/vertebra-duckdb/pyproject.toml").read_text(encoding="utf-8"))
    assert data["project"]["name"] == "vertebra-duckdb"
    maturin_cfg = data["tool"]["maturin"]
    assert maturin_cfg["module-name"] == "vertebra._duckdb"
    assert maturin_cfg["python-source"] == "../../py"

    excludes = maturin_cfg["exclude"]
    assert "**/py/vertebra/**/*.py" in excludes
    assert "**/py/vertebra/_duckdb.pyi" not in excludes
    assert "**/py/vertebra/_vertebra.pyi" in excludes
    assert "**/py/vertebra/_bigquery.pyi" in excludes
    assert "**/py/vertebra/_bundler.pyi" in excludes
    assert "**/py/vertebra/_mysql.pyi" in excludes
    assert "**/py/vertebra/_spanner.pyi" in excludes
    assert "**/py/vertebra/_storage.pyi" in excludes


@pytest.mark.parametrize(
    "sibling_crate",
    [
        "vertebra-bigquery",
        "vertebra-bundler",
        "vertebra-mysql",
        "vertebra-spanner",
        "vertebra-storage",
    ],
)
def test_sibling_extension_wheels_exclude_duckdb_stub(sibling_crate: str) -> None:
    """All sibling optional wheels exclude `**/py/vertebra/_duckdb.pyi` from their wheel payload."""
    pyproject_path = _REPO_ROOT / "src/rs" / sibling_crate / "pyproject.toml"
    data = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    excludes = data["tool"]["maturin"]["exclude"]
    assert "**/py/vertebra/_duckdb.pyi" in excludes


@pytest.mark.parametrize(
    "driver_cls",
    [SyncDuckDBDriver, AsyncDuckDBDriver],
    ids=["sync_driver", "async_driver"],
)
def test_duckdb_drivers_raise_missing_dependency_error_when_wheel_absent(
    driver_cls: type[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`SyncDuckDBDriver` and `AsyncDuckDBDriver` raise `MissingDependencyError` naming `vertebra-core[duckdb]`."""
    monkeypatch.setitem(sys.modules, "vertebra._duckdb", None)
    reset_dependency_cache("vertebra._duckdb")
    try:
        with pytest.raises(MissingDependencyError, match=r"vertebra-core\[duckdb\]"):
            driver_cls(DuckDBConfig())
    finally:
        reset_dependency_cache("vertebra._duckdb")


@pytest.mark.parametrize(
    ("pkg_name", "rel_path", "own_stub"),
    SIBLING_WHEEL_MANIFESTS,
    ids=[item[0] for item in SIBLING_WHEEL_MANIFESTS],
)
def test_optional_wheel_pyproject_isolation_and_exclusions(
    pkg_name: str,
    rel_path: str,
    own_stub: str,
) -> None:
    """Each native extension wheel manifest excludes `vertebra/__init__.py` and sibling `.pyi` stubs."""
    manifest_path = _REPO_ROOT / rel_path
    data = tomllib.loads(manifest_path.read_text(encoding="utf-8"))
    assert data["project"]["name"] == pkg_name
    maturin_cfg = data["tool"]["maturin"]
    assert maturin_cfg["python-source"] == "../../py"
    excludes = set(maturin_cfg.get("exclude", []))
    assert "**/py/vertebra/**/*.py" in excludes
    assert "**/py/vertebra/**/*.so" in excludes
    assert "**/py/vertebra/_vertebra.pyi" in excludes
    sibling_stubs = {
        "_duckdb.pyi",
        "_mysql.pyi",
        "_bigquery.pyi",
        "_spanner.pyi",
        "_storage.pyi",
        "_bundler.pyi",
    } - {own_stub}
    assert all(f"**/py/vertebra/{stub}" in excludes for stub in sibling_stubs)


def test_root_pyproject_declares_vertebra_mysql_optional_extra() -> None:
    """Root `pyproject.toml` maps the `mysql` optional extra to `vertebra-mysql`."""
    root_toml = tomllib.loads((_REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    optional_deps = root_toml["project"]["optional-dependencies"]
    assert any(dep.startswith("vertebra-mysql") for dep in optional_deps["mysql"])
    assert any(dep.startswith("vertebra-mysql") for dep in optional_deps["all"])
    uv_sources = root_toml["tool"]["uv"]["sources"]
    assert uv_sources["vertebra-mysql"] == {"path": "src/rs/vertebra-mysql", "editable": True}


def test_vertebra_mysql_runtime_module_surface() -> None:
    """`vertebra._mysql` exposes `SyncMySQLClient`, `AsyncMySQLClient`, `MySQLWireTestServer`, and `GIL_USED=False`."""
    mysql_mod = pytest.importorskip("vertebra._mysql")
    assert getattr(mysql_mod, "GIL_USED", None) is False
    assert isinstance(getattr(mysql_mod, "__version__", None), str)
    assert hasattr(mysql_mod, "SyncMySQLClient")
    assert hasattr(mysql_mod, "AsyncMySQLClient")
    assert hasattr(mysql_mod, "MySQLWireTestServer")
