"""Build and verify standalone vertebra-postgres wheel isolation and pure-Rust linkage."""

from __future__ import annotations

import argparse
import os
import platform
import shutil
import subprocess
import sys
import sysconfig
import tarfile
import tempfile
import venv
import zipfile
from pathlib import Path

__all__ = (
    "POSTGRES_EXPORTED_CLASSES",
    "main",
    "verify_core_wheel_excludes_postgres_binary_and_stub",
    "verify_isolated_venv_install_and_fail_loud_contract",
    "verify_postgres_sdist_contents",
    "verify_postgres_wheel_contents",
)

FORBIDDEN_NATIVE_LIBS: tuple[str, ...] = (
    "libpq",
    "libecpg",
)
SIBLING_STUBS: frozenset[str] = frozenset(
    {
        "vertebra/_vertebra.pyi",
        "vertebra/_bigquery.pyi",
        "vertebra/_bundler.pyi",
        "vertebra/_duckdb.pyi",
        "vertebra/_mssql.pyi",
        "vertebra/_mysql.pyi",
        "vertebra/_oracle.pyi",
        "vertebra/_spanner.pyi",
        "vertebra/_storage.pyi",
    }
)
NATIVE_SUFFIXES: tuple[str, ...] = (".so", ".pyd", ".dylib")
POSTGRES_EXPORTED_CLASSES: tuple[str, ...] = (
    "SyncPostgresClient",
    "AsyncPostgresClient",
    "SyncPostgresPool",
    "AsyncPostgresPool",
    "SyncPostgresPortal",
    "AsyncPostgresPortal",
    "SyncPostgresArrowStream",
    "AsyncPostgresArrowStream",
    "SyncPostgresCopyOut",
    "SyncPostgresCopyIn",
    "AsyncPostgresCopyOut",
    "AsyncPostgresCopyIn",
    "AsyncPostgresListener",
    "AsyncPostgresReplication",
)


def verify_core_wheel_excludes_postgres_binary_and_stub(wheel_path: Path) -> None:
    """Assert `vertebra_core-*.whl` packs the Python PostgreSQL adapter without `_postgres` binary or stub."""
    with zipfile.ZipFile(wheel_path) as zf:
        names = set(zf.namelist())

    required_py = (
        "vertebra/db/adapters/postgres/driver.py",
        "vertebra/db/adapters/postgres/_sync.py",
        "vertebra/db/adapters/postgres/_async.py",
    )
    missing = [entry for entry in required_py if entry not in names]
    if missing:
        msg = f"Core wheel {wheel_path.name} is missing PostgreSQL adapter modules: {missing}"
        raise RuntimeError(msg)

    leaked = sorted(
        name
        for name in names
        if name == "vertebra/_postgres.pyi"
        or name.startswith("vertebra/_postgres.")
        or (
            name.startswith("vertebra/_")
            and name.endswith(NATIVE_SUFFIXES)
            and not name.startswith("vertebra/_vertebra.")
        )
    )
    if leaked:
        msg = f"Core wheel {wheel_path.name} leaked optional extension artifacts: {leaked}"
        raise RuntimeError(msg)


def verify_postgres_wheel_contents(wheel_path: Path) -> None:
    """Assert `vertebra_postgres-*.whl` ships only `_postgres` binary + stub with zero `libpq` linkage."""
    with zipfile.ZipFile(wheel_path) as zf:
        names = zf.namelist()
        vtb_entries = sorted(name for name in names if name.startswith("vertebra/"))
        if "vertebra/_postgres.pyi" not in vtb_entries:
            msg = f"PostgreSQL wheel {wheel_path.name} is missing vertebra/_postgres.pyi: {vtb_entries}"
            raise RuntimeError(msg)

        binaries = [
            name for name in vtb_entries if name.startswith("vertebra/_postgres") and name.endswith(NATIVE_SUFFIXES)
        ]
        if len(binaries) != 1:
            msg = f"Expected exactly 1 vertebra/_postgres shared library in {wheel_path.name}, found: {binaries}"
            raise RuntimeError(msg)

        unexpected = [
            name
            for name in vtb_entries
            if name not in {"vertebra/_postgres.pyi", binaries[0]} or name in SIBLING_STUBS or name.endswith(".py")
        ]
        if unexpected:
            msg = f"PostgreSQL wheel {wheel_path.name} contains unexpected vertebra/ entries: {unexpected}"
            raise RuntimeError(msg)

        with tempfile.TemporaryDirectory(prefix="vtb-pg-link-") as tmp:
            extracted_lib = Path(zf.extract(binaries[0], path=tmp))
            _assert_zero_libpq_linkage(extracted_lib)


def verify_postgres_sdist_contents(sdist_path: Path) -> None:
    """Assert `vertebra_postgres-*.tar.gz` contains crate manifest, build script, source, and stub."""
    with tarfile.open(sdist_path, "r:gz") as tf:
        names = tf.getnames()

    required_suffixes = ("Cargo.toml", "build.rs", "src/lib.rs", "vertebra/_postgres.pyi")
    missing = [suffix for suffix in required_suffixes if not any(name.endswith(suffix) for name in names)]
    if missing:
        msg = f"PostgreSQL sdist {sdist_path.name} is missing required entries {missing}: {names}"
        raise RuntimeError(msg)


def verify_isolated_venv_install_and_fail_loud_contract(
    core_wheel: Path,
    postgres_wheel: Path,
) -> None:
    """Verify outside-checkout `vertebra-core` fails loud until `vertebra-postgres` installs."""
    with tempfile.TemporaryDirectory(prefix="vtb-pg-venv-") as tmp:
        root = Path(tmp).resolve()
        venv_dir = root / "isolated-venv"
        run_dir = root / "outside-checkout"
        run_dir.mkdir(parents=True, exist_ok=True)

        venv.EnvBuilder(with_pip=False, clear=True, symlinks=True).create(venv_dir)
        venv_python = _venv_python_path(venv_dir)
        venv_purelib = _query_venv_purelib(venv_python, run_dir)
        _seed_third_party_runtime_deps(venv_purelib)

        _install_wheel_no_deps(venv_python, venv_purelib, core_wheel, run_dir)
        _run_isolated_python(
            venv_python,
            run_dir,
            (
                "import importlib.util, pathlib, sys\n"
                "import vertebra\n"
                "import vertebra.db.adapters.postgres as pg_pkg\n"
                "from vertebra.db.adapters.postgres import AsyncPostgreSQLDriver, SyncPostgreSQLDriver\n"
                "from vertebra.framework.queues.postgres import PostgreSQLQueueBackend\n"
                "from vertebra.typing import MissingDependencyError\n"
                "venv_root = pathlib.Path(sys.prefix).resolve()\n"
                "assert pathlib.Path(vertebra.__file__).resolve().is_relative_to(venv_root), vertebra.__file__\n"
                "assert pathlib.Path(pg_pkg.__file__).resolve().is_relative_to(venv_root), pg_pkg.__file__\n"
                "assert importlib.util.find_spec('vertebra._postgres') is None\n"
                "for factory in (SyncPostgreSQLDriver, AsyncPostgreSQLDriver, PostgreSQLQueueBackend):\n"
                "    try:\n"
                "        factory()\n"
                "    except MissingDependencyError as exc:\n"
                "        assert exc.package == 'vertebra-core[postgres]', exc.package\n"
                "        assert exc.install_package == 'vertebra-core[postgres]', exc.install_package\n"
                "        assert 'vertebra-core[postgres]' in exc.hint, exc.hint\n"
                "        assert 'vertebra-core[postgres]' in str(exc), str(exc)\n"
                "    else:\n"
                "        raise AssertionError(f'{factory.__name__} did not raise MissingDependencyError')\n"
            ),
        )

        classes_literal = ", ".join(repr(name) for name in POSTGRES_EXPORTED_CLASSES)
        _install_wheel_no_deps(venv_python, venv_purelib, postgres_wheel, run_dir)
        _run_isolated_python(
            venv_python,
            run_dir,
            (
                "import pathlib, socket, sys\n"
                "import vertebra._postgres as mod\n"
                "from vertebra import _vertebra\n"
                "from vertebra.db.driver import ConnectionFailure\n"
                f"expected_classes = ({classes_literal})\n"
                "venv_root = pathlib.Path(sys.prefix).resolve()\n"
                "assert pathlib.Path(mod.__file__).resolve().is_relative_to(venv_root), mod.__file__\n"
                "assert mod.GIL_USED is False\n"
                "assert isinstance(mod.FREE_THREADED, bool)\n"
                "assert isinstance(mod.__version__, str) and mod.__version__\n"
                "for cls_name in expected_classes:\n"
                "    cls = getattr(mod, cls_name, None)\n"
                "    assert isinstance(cls, type), f'Missing _postgres class {cls_name}'\n"
                "    assert not hasattr(_vertebra, cls_name), f'_vertebra leaked {cls_name}'\n"
                "sync_client = mod.SyncPostgresClient(user='postgres', database='postgres', sslmode='disable')\n"
                "async_client = mod.AsyncPostgresClient(user='postgres', database='postgres', sslmode='disable')\n"
                "sync_pool = mod.SyncPostgresPool(user='postgres', database='postgres', sslmode='disable', min_size=0, max_size=2)\n"
                "async_pool = mod.AsyncPostgresPool(user='postgres', database='postgres', sslmode='disable', min_size=0, max_size=2)\n"
                "sync_pool.validate_configuration(user='postgres', database='postgres', sslmode='disable', min_size=0, max_size=4)\n"
                "async_pool.validate_configuration(user='postgres', database='postgres', sslmode='disable', min_size=0, max_size=4)\n"
                "assert sync_pool.closed is False and async_pool.closed is False\n"
                "try:\n"
                "    mod.SyncPostgresClient(user='postgres', database='postgres', unix_socket='/tmp/.s.PGSQL.5432', sslmode='require')\n"
                "except ValueError:\n"
                "    pass\n"
                "else:\n"
                "    raise AssertionError('SyncPostgresClient accepted unix_socket with sslmode=require')\n"
                "probe_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)\n"
                "probe_sock.bind(('127.0.0.1', 0))\n"
                "closed_port = int(probe_sock.getsockname()[1])\n"
                "probe_sock.close()\n"
                "dead_client = mod.SyncPostgresClient(\n"
                "    user='postgres',\n"
                "    database='postgres',\n"
                "    host='127.0.0.1',\n"
                "    port=closed_port,\n"
                "    sslmode='disable',\n"
                "    connect_timeout=0.5,\n"
                ")\n"
                "try:\n"
                "    dead_client.connect()\n"
                "except ConnectionFailure as exc:\n"
                "    assert exc.is_transient is True\n"
                "else:\n"
                "    raise AssertionError('SyncPostgresClient.connect() did not raise ConnectionFailure on dead port')\n"
                "finally:\n"
                "    dead_client.close()\n"
                "sync_client.close()\n"
                "sync_pool.close()\n"
                "assert sync_client.closed is True\n"
                "assert sync_pool.closed is True\n"
                "assert async_client.closed is True\n"
                "assert async_pool.closed is False\n"
            ),
        )


def main(argv: list[str] | None = None) -> int:
    """Build or accept `vertebra-core` and `vertebra-postgres` distributions and verify wheel isolation."""
    parser = argparse.ArgumentParser(description="Verify standalone vertebra-postgres wheel isolation.")
    parser.add_argument("--core-wheel", type=Path, default=None, help="Prebuilt vertebra_core-*.whl to verify.")
    parser.add_argument("--postgres-wheel", type=Path, default=None, help="Prebuilt vertebra_postgres-*.whl to verify.")
    parser.add_argument(
        "--postgres-sdist", type=Path, default=None, help="Prebuilt vertebra_postgres-*.tar.gz to verify."
    )
    args = parser.parse_args(argv)

    repo_root = Path(__file__).resolve().parents[2]
    with tempfile.TemporaryDirectory(prefix="vtb-postgres-wheel-check-") as tmp:
        out_dir = Path(tmp)
        core_wheel = args.core_wheel or _build_dist(repo_root, repo_root, out_dir / "core", kind="wheel")
        postgres_wheel = args.postgres_wheel or _build_dist(
            repo_root,
            repo_root / "src" / "rs" / "vertebra-postgres",
            out_dir / "postgres-whl",
            kind="wheel",
        )
        postgres_sdist = args.postgres_sdist or _build_dist(
            repo_root,
            repo_root / "src" / "rs" / "vertebra-postgres",
            out_dir / "postgres-sdist",
            kind="sdist",
        )

        verify_core_wheel_excludes_postgres_binary_and_stub(core_wheel)
        verify_postgres_wheel_contents(postgres_wheel)
        verify_postgres_sdist_contents(postgres_sdist)
        verify_isolated_venv_install_and_fail_loud_contract(core_wheel, postgres_wheel)

    sys.stdout.write(
        "Verified vertebra-core and vertebra-postgres wheel isolation, pure-Rust linkage, and 14-class contract.\n"
    )
    return 0


def _build_dist(repo_root: Path, package_dir: Path, out_dir: Path, *, kind: str) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    uv_bin = shutil.which("uv")
    if uv_bin is None:
        raise RuntimeError("`uv` executable not found on PATH")
    flag = "--wheel" if kind == "wheel" else "--sdist"
    cmd = [
        uv_bin,
        "build",
        str(package_dir),
        flag,
        "--no-build-isolation",
        "--python",
        sys.executable,
        "--out-dir",
        str(out_dir),
    ]
    env = os.environ.copy()
    env.setdefault("PYO3_PYTHON", sys.executable)
    proc = subprocess.run(cmd, cwd=repo_root, env=env, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        sys.stderr.write(proc.stdout)
        sys.stderr.write(proc.stderr)
        msg = f"Failed to build {kind} for {package_dir}: exit code {proc.returncode}"
        raise RuntimeError(msg)

    pattern = "*.whl" if kind == "wheel" else "*.tar.gz"
    matches = sorted(out_dir.glob(pattern))
    if len(matches) != 1:
        msg = f"Expected exactly 1 built {pattern} in {out_dir}, found: {matches}"
        raise RuntimeError(msg)
    return matches[0]


def _assert_zero_libpq_linkage(shared_lib: Path) -> None:
    inspected_output = ""
    sys_name = platform.system()
    if sys_name == "Linux":
        readelf_bin = shutil.which("readelf")
        ldd_bin = shutil.which("ldd")
        tool_cmd = (
            [readelf_bin, "-d", str(shared_lib)] if readelf_bin else ([ldd_bin, str(shared_lib)] if ldd_bin else None)
        )
        if tool_cmd is not None:
            proc = subprocess.run(tool_cmd, capture_output=True, text=True, check=False)
            if proc.returncode == 0:
                inspected_output = proc.stdout.lower()
    elif sys_name == "Darwin":
        otool_bin = shutil.which("otool")
        if otool_bin is not None:
            proc = subprocess.run([otool_bin, "-L", str(shared_lib)], capture_output=True, text=True, check=False)
            if proc.returncode == 0:
                inspected_output = proc.stdout.lower()

    if inspected_output:
        found = [lib for lib in FORBIDDEN_NATIVE_LIBS if lib in inspected_output]
        if found:
            msg = f"{shared_lib.name} dynamically links forbidden PostgreSQL C libraries {found}:\n{inspected_output}"
            raise RuntimeError(msg)
        return

    raw_bytes = shared_lib.read_bytes().lower()
    found_raw = [lib for lib in FORBIDDEN_NATIVE_LIBS if f"{lib}.".encode("ascii") in raw_bytes]
    if found_raw:
        msg = f"{shared_lib.name} references forbidden PostgreSQL C libraries: {found_raw}"
        raise RuntimeError(msg)


def _venv_python_path(venv_dir: Path) -> Path:
    if platform.system() == "Windows":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def _clean_isolated_env() -> dict[str, str]:
    preserved_keys = ("PATH", "HOME", "TMPDIR", "TEMP", "TMP", "SYSTEMROOT", "LANG", "LC_ALL")
    env = {key: os.environ[key] for key in preserved_keys if key in os.environ}
    env["PYTHONNOUSERSITE"] = "1"
    return env


def _query_venv_purelib(venv_python: Path, cwd: Path) -> Path:
    proc = subprocess.run(
        [str(venv_python), "-I", "-c", "import sysconfig; print(sysconfig.get_path('purelib'))"],
        cwd=cwd,
        env=_clean_isolated_env(),
        capture_output=True,
        text=True,
        check=True,
    )
    return Path(proc.stdout.strip()).resolve()


def _seed_third_party_runtime_deps(dest_site_packages: Path) -> None:
    host_dirs = {
        Path(sysconfig.get_path("purelib")).resolve(),
        Path(sysconfig.get_path("platlib")).resolve(),
    }
    skip_prefixes = ("vertebra", "_vertebra", "__editable__", "_virtualenv")
    for host_dir in host_dirs:
        if not host_dir.is_dir() or host_dir == dest_site_packages:
            continue
        for item in host_dir.iterdir():
            lower_name = item.name.lower()
            if lower_name.startswith(skip_prefixes) or lower_name.endswith(".pth"):
                continue
            target = dest_site_packages / item.name
            if not target.exists():
                target.symlink_to(item, target_is_directory=item.is_dir())


def _install_wheel_no_deps(venv_python: Path, venv_purelib: Path, wheel_path: Path, cwd: Path) -> None:
    uv_bin = shutil.which("uv")
    if uv_bin is not None:
        proc = subprocess.run(
            [
                uv_bin,
                "pip",
                "install",
                "--python",
                str(venv_python),
                "--no-index",
                "--no-deps",
                "--reinstall",
                str(wheel_path),
            ],
            cwd=cwd,
            env=_clean_isolated_env(),
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode == 0:
            return
    with zipfile.ZipFile(wheel_path) as zf:
        zf.extractall(venv_purelib)


def _run_isolated_python(venv_python: Path, cwd: Path, code: str) -> None:
    proc = subprocess.run(
        [str(venv_python), "-I", "-c", code],
        cwd=cwd,
        env=_clean_isolated_env(),
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        sys.stderr.write(proc.stdout)
        sys.stderr.write(proc.stderr)
        msg = f"Isolated venv check failed with exit code {proc.returncode}"
        raise RuntimeError(msg)


if __name__ == "__main__":
    raise SystemExit(main())
