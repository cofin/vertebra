"""Build and verify standalone vertebra-oracle wheel isolation and pure-Rust linkage."""

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
    "main",
    "verify_core_wheel_excludes_oracle_binary_and_stub",
    "verify_isolated_venv_install_and_fail_loud_contract",
    "verify_oracle_sdist_contents",
    "verify_oracle_wheel_contents_and_pure_rust_linkage",
)

FORBIDDEN_NATIVE_LIBS: tuple[str, ...] = (
    "libclntsh",
    "libnnz",
    "libodpic",
    "libociicus",
)
SIBLING_STUBS: frozenset[str] = frozenset(
    {
        "vertebra/_vertebra.pyi",
        "vertebra/_bigquery.pyi",
        "vertebra/_bundler.pyi",
        "vertebra/_spanner.pyi",
        "vertebra/_storage.pyi",
    }
)
NATIVE_SUFFIXES: tuple[str, ...] = (".so", ".pyd", ".dylib")


def verify_core_wheel_excludes_oracle_binary_and_stub(wheel_path: Path) -> None:
    """Assert `vertebra_core-*.whl` packs the Python Oracle adapter without `_oracle` binary or stub."""
    with zipfile.ZipFile(wheel_path) as zf:
        names = set(zf.namelist())

    if "vertebra/db/adapters/oracle/driver.py" not in names:
        msg = f"Core wheel {wheel_path.name} is missing vertebra/db/adapters/oracle/driver.py"
        raise RuntimeError(msg)

    leaked = sorted(
        name
        for name in names
        if name == "vertebra/_oracle.pyi"
        or name.startswith("vertebra/_oracle.")
        or (
            name.startswith("vertebra/_")
            and name.endswith(NATIVE_SUFFIXES)
            and not name.startswith("vertebra/_vertebra.")
        )
    )
    if leaked:
        msg = f"Core wheel {wheel_path.name} leaked optional extension artifacts: {leaked}"
        raise RuntimeError(msg)


def verify_oracle_wheel_contents_and_pure_rust_linkage(wheel_path: Path) -> None:
    """Assert `vertebra_oracle-*.whl` ships only `_oracle` binary + stub with zero Instant Client linkage."""
    with zipfile.ZipFile(wheel_path) as zf:
        names = zf.namelist()
        vtb_entries = sorted(name for name in names if name.startswith("vertebra/"))
        if "vertebra/_oracle.pyi" not in vtb_entries:
            msg = f"Oracle wheel {wheel_path.name} is missing vertebra/_oracle.pyi: {vtb_entries}"
            raise RuntimeError(msg)

        binaries = [
            name for name in vtb_entries if name.startswith("vertebra/_oracle") and name.endswith(NATIVE_SUFFIXES)
        ]
        if len(binaries) != 1:
            msg = f"Expected exactly 1 vertebra/_oracle shared library in {wheel_path.name}, found: {binaries}"
            raise RuntimeError(msg)

        unexpected = [
            name
            for name in vtb_entries
            if name not in {"vertebra/_oracle.pyi", binaries[0]} or name in SIBLING_STUBS or name.endswith(".py")
        ]
        if unexpected:
            msg = f"Oracle wheel {wheel_path.name} contains unexpected vertebra/ entries: {unexpected}"
            raise RuntimeError(msg)

        with tempfile.TemporaryDirectory(prefix="vtb-ora-link-") as tmp:
            extracted_lib = Path(zf.extract(binaries[0], path=tmp))
            _assert_zero_instant_client_linkage(extracted_lib)


def verify_oracle_sdist_contents(sdist_path: Path) -> None:
    """Assert `vertebra_oracle-*.tar.gz` contains the Rust crate manifest, build script, source, and stub."""
    with tarfile.open(sdist_path, "r:gz") as tf:
        names = tf.getnames()

    required_suffixes = ("Cargo.toml", "build.rs", "src/lib.rs", "vertebra/_oracle.pyi")
    missing = [suffix for suffix in required_suffixes if not any(name.endswith(suffix) for name in names)]
    if missing:
        msg = f"Oracle sdist {sdist_path.name} is missing required entries {missing}: {names}"
        raise RuntimeError(msg)


def verify_isolated_venv_install_and_fail_loud_contract(
    core_wheel: Path,
    oracle_wheel: Path,
) -> None:
    """Verify outside-checkout `vertebra-core` fails loud with `MissingDependencyError` until `vertebra-oracle` installs."""
    with tempfile.TemporaryDirectory(prefix="vtb-ora-venv-") as tmp:
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
                "import vertebra.db.adapters.oracle as ora_pkg\n"
                "from vertebra.db.adapters.oracle.driver import AsyncOracleDriver, SyncOracleDriver\n"
                "from vertebra.typing import MissingDependencyError\n"
                "venv_root = pathlib.Path(sys.prefix).resolve()\n"
                "assert pathlib.Path(vertebra.__file__).resolve().is_relative_to(venv_root), vertebra.__file__\n"
                "assert pathlib.Path(ora_pkg.__file__).resolve().is_relative_to(venv_root), ora_pkg.__file__\n"
                "assert importlib.util.find_spec('vertebra._oracle') is None\n"
                "for cls in (SyncOracleDriver, AsyncOracleDriver):\n"
                "    try:\n"
                "        cls()\n"
                "    except MissingDependencyError as exc:\n"
                "        assert exc.package == 'vertebra-core[oracle]', exc.package\n"
                "        assert exc.install_package == 'vertebra-core[oracle]', exc.install_package\n"
                "        assert 'vertebra-core[oracle]' in exc.hint, exc.hint\n"
                "        assert 'vertebra-core[oracle]' in str(exc), str(exc)\n"
                "    else:\n"
                "        raise AssertionError(f'{cls.__name__} did not raise MissingDependencyError')\n"
            ),
        )

        _install_wheel_no_deps(venv_python, venv_purelib, oracle_wheel, run_dir)
        _run_isolated_python(
            venv_python,
            run_dir,
            (
                "import pathlib, socket, sys\n"
                "import vertebra._oracle as mod\n"
                "from vertebra import _vertebra\n"
                "from vertebra.db.driver import ConnectionFailure, InterfaceError\n"
                "venv_root = pathlib.Path(sys.prefix).resolve()\n"
                "assert pathlib.Path(mod.__file__).resolve().is_relative_to(venv_root), mod.__file__\n"
                "assert mod.GIL_USED is False\n"
                "assert mod.ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED is False\n"
                "assert not hasattr(_vertebra, 'SyncOracleClient')\n"
                "sync_client = mod.SyncOracleClient(host='localhost', service_name='FREEPDB1')\n"
                "async_client = mod.AsyncOracleClient(host='localhost', service_name='FREEPDB1')\n"
                "sync_pool = mod.SyncOraclePool(host='localhost', service_name='FREEPDB1', min_size=0, max_size=2)\n"
                "async_pool = mod.AsyncOraclePool(host='localhost', service_name='FREEPDB1', min_size=0, max_size=2)\n"
                "sync_client.set_session_attrs(client_identifier='wheel-check', module='vertebra')\n"
                "assert sync_pool.stats()['max_size'] == 2\n"
                "assert async_pool.stats()['max_size'] == 2\n"
                "try:\n"
                "    mod.SyncOracleClient(host='memory', service_name='FREEPDB1')\n"
                "except InterfaceError:\n"
                "    pass\n"
                "else:\n"
                "    raise AssertionError('SyncOracleClient accepted synthetic memory host')\n"
                "probe_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)\n"
                "probe_sock.bind(('127.0.0.1', 0))\n"
                "closed_port = int(probe_sock.getsockname()[1])\n"
                "probe_sock.close()\n"
                "dead_client = mod.SyncOracleClient(\n"
                "    host='127.0.0.1',\n"
                "    port=closed_port,\n"
                "    service_name='FREEPDB1',\n"
                "    connect_timeout=0.5,\n"
                ")\n"
                "try:\n"
                "    dead_client.connect()\n"
                "except ConnectionFailure as exc:\n"
                "    assert exc.sqlstate == '08006', exc.sqlstate\n"
                "    assert exc.is_transient is True\n"
                "else:\n"
                "    raise AssertionError('SyncOracleClient.connect() did not raise ConnectionFailure on dead port')\n"
                "finally:\n"
                "    dead_client.close()\n"
                "sync_client.close()\n"
                "sync_pool.close()\n"
                "assert sync_client.closed is True\n"
                "assert sync_pool.closed is True\n"
                "assert async_client.closed is False\n"
                "assert async_pool.closed is False\n"
            ),
        )


def main(argv: list[str] | None = None) -> int:
    """Build or accept `vertebra-core` and `vertebra-oracle` distributions and verify full wheel isolation."""
    parser = argparse.ArgumentParser(description="Verify standalone vertebra-oracle wheel isolation.")
    parser.add_argument("--core-wheel", type=Path, default=None, help="Prebuilt vertebra_core-*.whl to verify.")
    parser.add_argument("--oracle-wheel", type=Path, default=None, help="Prebuilt vertebra_oracle-*.whl to verify.")
    parser.add_argument("--oracle-sdist", type=Path, default=None, help="Prebuilt vertebra_oracle-*.tar.gz to verify.")
    args = parser.parse_args(argv)

    repo_root = Path(__file__).resolve().parents[2]
    with tempfile.TemporaryDirectory(prefix="vtb-oracle-wheel-check-") as tmp:
        out_dir = Path(tmp)
        core_wheel = args.core_wheel or _build_dist(repo_root, repo_root, out_dir / "core", kind="wheel")
        oracle_wheel = args.oracle_wheel or _build_dist(
            repo_root,
            repo_root / "src" / "rs" / "vertebra-oracle",
            out_dir / "oracle-whl",
            kind="wheel",
        )
        oracle_sdist = args.oracle_sdist or _build_dist(
            repo_root,
            repo_root / "src" / "rs" / "vertebra-oracle",
            out_dir / "oracle-sdist",
            kind="sdist",
        )

        verify_core_wheel_excludes_oracle_binary_and_stub(core_wheel)
        verify_oracle_wheel_contents_and_pure_rust_linkage(oracle_wheel)
        verify_oracle_sdist_contents(oracle_sdist)
        verify_isolated_venv_install_and_fail_loud_contract(core_wheel, oracle_wheel)

    sys.stdout.write(
        "Verified vertebra-core and vertebra-oracle wheel isolation, pure-Rust linkage, and fail-loud contract.\n"
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


def _assert_zero_instant_client_linkage(shared_lib: Path) -> None:
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
            msg = f"{shared_lib.name} dynamically links forbidden Oracle C libraries {found}:\n{inspected_output}"
            raise RuntimeError(msg)
        return

    raw_bytes = shared_lib.read_bytes().lower()
    found_raw = [lib for lib in FORBIDDEN_NATIVE_LIBS if f"{lib}.".encode("ascii") in raw_bytes]
    if found_raw:
        msg = f"{shared_lib.name} references forbidden Oracle C libraries: {found_raw}"
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
