"""Unit tests for optional database driver wheel isolation and `MissingDependencyError` contracts."""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any

import pytest

import vertebra.db.adapters.duckdb.driver as duckdb_driver_mod
from vertebra.db.adapters.duckdb import AsyncDuckDBDriver, DuckDBConfig, SyncDuckDBDriver
from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import ensure_module

_REPO_ROOT = Path(__file__).resolve().parents[5]


def test_root_pyproject_declares_vertebra_duckdb_extra_and_no_pypi_duckdb() -> None:
    """Root `pyproject.toml` declares `vertebra-core[duckdb]` and omits PyPI `duckdb` from dependency groups."""
    data = tomllib.loads((_REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    opt_deps = data["project"]["optional-dependencies"]
    assert opt_deps["duckdb"] == ["vertebra-duckdb==0.3.0"]
    assert "vertebra-duckdb==0.3.0" in opt_deps["all"]

    uv_sources = data["tool"]["uv"]["sources"]
    assert uv_sources["vertebra-duckdb"] == {"path": "src/rs/vertebra-duckdb", "editable": True}

    bench_group = data["dependency-groups"]["bench"]
    assert not any(str(dep).startswith("duckdb") for dep in bench_group)


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
    assert "**/py/vertebra/_spanner.pyi" in excludes
    assert "**/py/vertebra/_storage.pyi" in excludes


@pytest.mark.parametrize(
    "sibling_crate",
    [
        "vertebra-bigquery",
        "vertebra-bundler",
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

    def _raise_missing(module_name: str, *, package: str | None = None, extra: str | None = None) -> Any:
        if module_name == "vertebra._duckdb":
            raise MissingDependencyError(package or "vertebra-core[duckdb]", extra=extra)
        return ensure_module(module_name, package=package, extra=extra)

    monkeypatch.setattr(duckdb_driver_mod, "ensure_module", _raise_missing)
    with pytest.raises(MissingDependencyError, match=r"vertebra-core\[duckdb\]"):
        driver_cls(DuckDBConfig())
