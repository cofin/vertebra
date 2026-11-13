"""Unit tests for optional database driver wheel isolation and `MissingDependencyError` contracts."""

from __future__ import annotations

import asyncio
import subprocess
import sys
import tarfile
import tomllib
import zipfile
from io import BytesIO
from pathlib import Path
from typing import Any

import pytest
from tools.scripts.verify_postgres_wheel import (
    verify_core_wheel_excludes_postgres_binary_and_stub,
    verify_postgres_sdist_contents,
    verify_postgres_wheel_contents,
)
from tools.scripts.verify_storage_wheel import (
    COMPANION_SPECS,
    CompanionPackage,
    verify_companion_sdist_contents,
    verify_companion_wheel_contents,
    verify_core_wheel_excludes_companion,
)
from tools.scripts.verify_storage_wheel import (
    main as verify_storage_wheel_main,
)

from vertebra import _vertebra
from vertebra.db.adapters.duckdb import AsyncDuckDBDriver, DuckDBConfig, SyncDuckDBDriver
from vertebra.db.adapters.postgres import AsyncPostgreSQLDriver, SyncPostgreSQLDriver
from vertebra.framework.queues.postgres import PostgreSQLQueueBackend
from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import reset_dependency_cache

_REPO_ROOT = Path(__file__).resolve().parents[5]

SIBLING_WHEEL_MANIFESTS: tuple[tuple[str, str, str], ...] = (
    ("vertebra-duckdb", "src/rs/vertebra-duckdb/pyproject.toml", "_duckdb.pyi"),
    ("vertebra-mssql", "src/rs/vertebra-mssql/pyproject.toml", "_mssql.pyi"),
    ("vertebra-mysql", "src/rs/vertebra-mysql/pyproject.toml", "_mysql.pyi"),
    ("vertebra-postgres", "src/rs/vertebra-postgres/pyproject.toml", "_postgres.pyi"),
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
    assert "**/py/vertebra/_mssql.pyi" in excludes
    assert "**/py/vertebra/_mysql.pyi" in excludes
    assert "**/py/vertebra/_spanner.pyi" in excludes
    assert "**/py/vertebra/_storage.pyi" in excludes


@pytest.mark.parametrize(
    "sibling_crate",
    [
        "vertebra-bigquery",
        "vertebra-bundler",
        "vertebra-mssql",
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
    assert f"**/py/vertebra/{own_stub}" not in excludes
    assert "**/py/vertebra/**/*.py" in excludes
    assert "**/py/vertebra/**/*.so" in excludes
    assert "**/py/vertebra/_vertebra.pyi" in excludes
    sibling_stubs = {
        "_duckdb.pyi",
        "_mssql.pyi",
        "_mysql.pyi",
        "_oracle.pyi",
        "_postgres.pyi",
        "_bigquery.pyi",
        "_spanner.pyi",
        "_storage.pyi",
        "_bundler.pyi",
    } - {own_stub}
    assert all(f"**/py/vertebra/{stub}" in excludes for stub in sibling_stubs)
    stub_file = _REPO_ROOT / "src" / "py" / "vertebra" / own_stub
    assert stub_file.is_file()


@pytest.mark.parametrize("extra_name", ["mysql", "mssql"])
def test_root_pyproject_declares_optional_driver_extra(extra_name: str) -> None:
    """Root `pyproject.toml` maps each optional SQL driver extra to its matching workspace crate."""
    root_toml = tomllib.loads((_REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    pkg_name = f"vertebra-{extra_name}"
    optional_deps = root_toml["project"]["optional-dependencies"]
    assert any(dep.startswith(pkg_name) for dep in optional_deps[extra_name])
    assert any(dep.startswith(pkg_name) for dep in optional_deps["all"])
    uv_sources = root_toml["tool"]["uv"]["sources"]
    assert uv_sources[pkg_name] == {"path": f"src/rs/{pkg_name}", "editable": True}


def test_vertebra_mysql_runtime_module_surface() -> None:
    """`vertebra._mysql` exposes `SyncMySQLClient`, `AsyncMySQLClient`, `MySQLWireTestServer`, and `GIL_USED=False`."""
    mysql_mod = pytest.importorskip("vertebra._mysql")
    assert getattr(mysql_mod, "GIL_USED", None) is False
    assert isinstance(getattr(mysql_mod, "__version__", None), str)
    assert hasattr(mysql_mod, "SyncMySQLClient")
    assert hasattr(mysql_mod, "AsyncMySQLClient")
    assert hasattr(mysql_mod, "MySQLWireTestServer")


def _write_wheel_archive(path: Path, entries: dict[str, bytes]) -> Path:
    with zipfile.ZipFile(path, "w") as zf:
        for name, data in entries.items():
            zf.writestr(name, data)
    return path


def _write_sdist_archive(path: Path, prefix: str, entries: dict[str, bytes]) -> Path:
    with tarfile.open(path, "w:gz") as tf:
        for rel, data in entries.items():
            info = tarfile.TarInfo(name=f"{prefix}/{rel}")
            info.size = len(data)
            tf.addfile(info, BytesIO(data))
    return path


@pytest.mark.parametrize("package", ["storage", "bigquery", "spanner", "bundler"])
def test_companion_wheel_and_sdist_isolation_verifier(tmp_path: Path, package: CompanionPackage) -> None:
    """verify_storage_wheel accepts clean companion/core archives and rejects leaks or missing stubs."""
    spec = COMPANION_SPECS[package]
    binary_entry = f"{spec.binary_prefix}.cpython-312-x86_64-linux-gnu.so"

    core_ok = _write_wheel_archive(
        tmp_path / f"core-{package}-ok.whl",
        {spec.core_sentinel: b"", "vertebra/_vertebra.cpython-312-x86_64-linux-gnu.so": b""},
    )
    verify_core_wheel_excludes_companion(core_ok, package=package)

    core_leaked = _write_wheel_archive(
        tmp_path / f"core-{package}-leak.whl",
        {spec.core_sentinel: b"", spec.stub_entry: b""},
    )
    with pytest.raises(RuntimeError, match="leaked optional extension artifacts"):
        verify_core_wheel_excludes_companion(core_leaked, package=package)

    companion_ok = _write_wheel_archive(
        tmp_path / f"{package}-ok.whl",
        {spec.stub_entry: b"", binary_entry: b""},
    )
    verify_companion_wheel_contents(companion_ok, package=package)

    leaked_py = _write_wheel_archive(
        tmp_path / f"{package}-leaked-py.whl",
        {spec.stub_entry: b"", binary_entry: b"", "vertebra/__init__.py": b""},
    )
    with pytest.raises(RuntimeError, match="unexpected vertebra/ entries"):
        verify_companion_wheel_contents(leaked_py, package=package)

    sibling_stub = "vertebra/_mysql.pyi"
    leaked_stub = _write_wheel_archive(
        tmp_path / f"{package}-leaked-stub.whl",
        {spec.stub_entry: b"", binary_entry: b"", sibling_stub: b""},
    )
    with pytest.raises(RuntimeError, match="unexpected vertebra/ entries"):
        verify_companion_wheel_contents(leaked_stub, package=package)

    missing_stub = _write_wheel_archive(
        tmp_path / f"{package}-missing-stub.whl",
        {binary_entry: b""},
    )
    with pytest.raises(RuntimeError, match="missing"):
        verify_companion_wheel_contents(missing_stub, package=package)

    duplicate_so = _write_wheel_archive(
        tmp_path / f"{package}-dup-so.whl",
        {
            spec.stub_entry: b"",
            binary_entry: b"",
            f"{spec.binary_prefix}.abi3.so": b"",
        },
    )
    with pytest.raises(RuntimeError, match="Expected exactly 1"):
        verify_companion_wheel_contents(duplicate_so, package=package)

    sdist_ok = _write_sdist_archive(
        tmp_path / f"vertebra_{package}-0.3.0.tar.gz",
        f"vertebra_{package}-0.3.0",
        {"Cargo.toml": b"", "build.rs": b"", "src/lib.rs": b"", f"src/py/{spec.stub_entry}": b""},
    )
    verify_companion_sdist_contents(sdist_ok, package=package)

    sdist_missing = _write_sdist_archive(
        tmp_path / f"vertebra_{package}-missing.tar.gz",
        f"vertebra_{package}-0.3.0",
        {"Cargo.toml": b"", "build.rs": b"", "src/lib.rs": b""},
    )
    with pytest.raises(RuntimeError, match="missing required entries"):
        verify_companion_sdist_contents(sdist_missing, package=package)


@pytest.mark.parametrize("package", ["storage", "bigquery", "spanner", "bundler"])
def test_companion_isolation_runtime_and_makefile_targets(tmp_path: Path, package: CompanionPackage) -> None:
    """Core _vertebra excludes companion symbols, CLI rejects --all archive flags, and make -n resolves targets."""
    spec = COMPANION_SPECS[package]
    assert not hasattr(_vertebra, spec.leaked_core_attr)
    proc = subprocess.run(
        ["make", "-n", f"{package}-wheel-check"],
        cwd=_REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert f"verify_storage_wheel.py --package {package}" in proc.stdout
    with pytest.raises(ValueError, match="cannot be combined with --all"):
        verify_storage_wheel_main(["--all", "--companion-wheel", str(tmp_path / "dummy.whl")])


def _has_pg_comparator_dep(dep: object) -> bool:
    if isinstance(dep, str):
        return "psycopg" in dep or "asyncpg" in dep
    if isinstance(dep, dict):
        return dep.get("include-group") == "postgres-bench"
    return False


def test_postgres_wheel_and_sdist_verifier_checks_isolation(tmp_path: Path) -> None:
    """verify_postgres_wheel validates clean core, binary+stub wheel, sdist layout, and Makefile target."""
    pg_binary = "vertebra/_postgres.cpython-312-x86_64-linux-gnu.so"
    core_ok = _write_wheel_archive(
        tmp_path / "core-pg-ok.whl",
        {
            "vertebra/db/adapters/postgres/driver.py": b"",
            "vertebra/db/adapters/postgres/_sync.py": b"",
            "vertebra/db/adapters/postgres/_async.py": b"",
            "vertebra/_vertebra.cpython-312-x86_64-linux-gnu.so": b"",
        },
    )
    verify_core_wheel_excludes_postgres_binary_and_stub(core_ok)

    pg_ok = _write_wheel_archive(
        tmp_path / "pg-ok.whl",
        {"vertebra/_postgres.pyi": b"", pg_binary: b"\x7fELF"},
    )
    verify_postgres_wheel_contents(pg_ok)

    sdist_ok = _write_sdist_archive(
        tmp_path / "vertebra_postgres-0.3.0.tar.gz",
        "vertebra_postgres-0.3.0",
        {"Cargo.toml": b"", "build.rs": b"", "src/lib.rs": b"", "vertebra/_postgres.pyi": b""},
    )
    verify_postgres_sdist_contents(sdist_ok)

    proc = subprocess.run(
        ["make", "-n", "postgres-wheel-check"],
        cwd=_REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert "verify_postgres_wheel.py" in proc.stdout


@pytest.mark.parametrize(
    ("extra_entries", "expected_error"),
    [
        ({"vertebra/_postgres.pyi": b""}, "leaked optional extension artifacts"),
        ({"vertebra/_postgres.cpython-312-x86_64-linux-gnu.so": b""}, "leaked optional extension artifacts"),
        ({"vertebra/_oracle.cpython-312-x86_64-linux-gnu.so": b""}, "leaked optional extension artifacts"),
    ],
)
def test_postgres_core_wheel_verifier_rejects_native_leaks(
    tmp_path: Path,
    extra_entries: dict[str, bytes],
    expected_error: str,
) -> None:
    """verify_core_wheel_excludes_postgres_binary_and_stub fails when core ships `_postgres` or sibling binaries."""
    entries: dict[str, bytes] = {
        "vertebra/db/adapters/postgres/driver.py": b"",
        "vertebra/db/adapters/postgres/_sync.py": b"",
        "vertebra/db/adapters/postgres/_async.py": b"",
        **extra_entries,
    }
    bad_core = _write_wheel_archive(tmp_path / "core-bad.whl", entries)
    with pytest.raises(RuntimeError, match=expected_error):
        verify_core_wheel_excludes_postgres_binary_and_stub(bad_core)


@pytest.mark.parametrize(
    ("wheel_entries", "expected_error"),
    [
        ({"vertebra/_postgres.cpython-312-x86_64-linux-gnu.so": b"\x7fELF"}, "missing vertebra/_postgres.pyi"),
        (
            {
                "vertebra/_postgres.pyi": b"",
                "vertebra/_postgres.cpython-312-x86_64-linux-gnu.so": b"\x7fELF",
                "vertebra/_postgres.abi3.so": b"\x7fELF",
            },
            "Expected exactly 1 vertebra/_postgres",
        ),
        (
            {
                "vertebra/_postgres.pyi": b"",
                "vertebra/_postgres.cpython-312-x86_64-linux-gnu.so": b"\x7fELF",
                "vertebra/_oracle.pyi": b"",
            },
            "unexpected vertebra/ entries",
        ),
        (
            {
                "vertebra/_postgres.pyi": b"",
                "vertebra/_postgres.cpython-312-x86_64-linux-gnu.so": b"\x7fELF",
                "vertebra/__init__.py": b"",
            },
            "unexpected vertebra/ entries",
        ),
        (
            {
                "vertebra/_postgres.pyi": b"",
                "vertebra/_postgres.cpython-312-x86_64-linux-gnu.so": b"\x7fELF\x00libpq.so.5\x00",
            },
            "forbidden PostgreSQL C libraries",
        ),
    ],
)
def test_postgres_wheel_verifier_rejects_malformed_archives_and_libpq(
    tmp_path: Path,
    wheel_entries: dict[str, bytes],
    expected_error: str,
) -> None:
    """verify_postgres_wheel_contents rejects missing stubs, extra Python/sibling files, and libpq linkage."""
    bad_wheel = _write_wheel_archive(tmp_path / "pg-bad.whl", wheel_entries)
    with pytest.raises(RuntimeError, match=expected_error):
        verify_postgres_wheel_contents(bad_wheel)


@pytest.mark.parametrize(
    "sdist_entries",
    [
        {"build.rs": b"", "src/lib.rs": b"", "vertebra/_postgres.pyi": b""},
        {"Cargo.toml": b"", "build.rs": b"", "src/lib.rs": b""},
    ],
)
def test_postgres_sdist_verifier_rejects_missing_files(tmp_path: Path, sdist_entries: dict[str, bytes]) -> None:
    """verify_postgres_sdist_contents rejects sdists missing Cargo.toml or `_postgres.pyi`."""
    bad_sdist = _write_sdist_archive(
        tmp_path / "vertebra_postgres-bad.tar.gz", "vertebra_postgres-0.3.0", sdist_entries
    )
    with pytest.raises(RuntimeError, match="missing required entries"):
        verify_postgres_sdist_contents(bad_sdist)


def test_postgres_missing_dependency_fail_loud_probes_without_native_extension(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sync connect, async connect, and PostgreSQLQueueBackend raise MissingDependencyError without `_postgres`."""
    reset_dependency_cache()
    monkeypatch.setitem(sys.modules, "vertebra._postgres", None)
    try:
        with pytest.raises(MissingDependencyError) as sync_exc:
            SyncPostgreSQLDriver().connect()
        assert sync_exc.value.package == "vertebra-core[postgres]"
        assert sync_exc.value.install_package == "vertebra-core[postgres]"

        with pytest.raises(MissingDependencyError) as async_exc:
            asyncio.run(AsyncPostgreSQLDriver().connect())
        assert async_exc.value.package == "vertebra-core[postgres]"

        with pytest.raises(MissingDependencyError) as queue_exc:
            PostgreSQLQueueBackend()
        assert queue_exc.value.package == "vertebra-core[postgres]"
    finally:
        reset_dependency_cache()


def test_root_pyproject_isolates_postgres_bench_comparators() -> None:
    """Third-party psycopg/asyncpg comparators live exclusively in dependency-groups.postgres-bench."""
    data = tomllib.loads((_REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    groups = data.get("dependency-groups", {})
    pg_bench = groups.get("postgres-bench", [])
    assert any(isinstance(dep, str) and dep.startswith("psycopg") for dep in pg_bench)
    assert any(isinstance(dep, str) and dep.startswith("asyncpg") for dep in pg_bench)
    assert all(isinstance(dep, str) and "python_version < '3.14'" in dep for dep in pg_bench)

    core_deps = data["project"].get("dependencies", [])
    assert not any(_has_pg_comparator_dep(dep) for dep in core_deps)

    offending_extras = [
        extra_name
        for extra_name, extra_deps in data["project"].get("optional-dependencies", {}).items()
        if any(_has_pg_comparator_dep(dep) for dep in extra_deps)
    ]
    assert offending_extras == []

    offending_groups = [
        group_name
        for group_name, group_deps in groups.items()
        if group_name != "postgres-bench" and any(_has_pg_comparator_dep(dep) for dep in group_deps)
    ]
    assert offending_groups == []
    assert _has_pg_comparator_dep({"include-group": "postgres-bench"}) is True
