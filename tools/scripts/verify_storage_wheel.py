"""Build and verify standalone companion wheel isolation across storage, cloud, and bundler crates."""

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
from collections.abc import Sequence
from pathlib import Path
from typing import Literal, get_args

import msgspec

CompanionPackage = Literal["storage", "bigquery", "spanner", "bundler"]

__all__ = (
    "COMPANION_SPECS",
    "CompanionPackage",
    "CompanionWheelSpec",
    "main",
    "verify_companion_sdist_contents",
    "verify_companion_wheel_contents",
    "verify_core_wheel_excludes_companion",
    "verify_isolated_venv_contract",
)

NATIVE_SUFFIXES: tuple[str, ...] = (".so", ".pyd", ".dylib")

ALL_KNOWN_STUBS: frozenset[str] = frozenset(
    {
        "vertebra/_vertebra.pyi",
        "vertebra/_bigquery.pyi",
        "vertebra/_bundler.pyi",
        "vertebra/_duckdb.pyi",
        "vertebra/_mssql.pyi",
        "vertebra/_mysql.pyi",
        "vertebra/_oracle.pyi",
        "vertebra/_postgres.pyi",
        "vertebra/_spanner.pyi",
        "vertebra/_storage.pyi",
    }
)


class CompanionWheelSpec(msgspec.Struct, frozen=True):
    """Packaging and isolated-venv verification specification for a companion extension."""

    package: CompanionPackage
    crate_dir: str
    module_name: str
    stub_entry: str
    binary_prefix: str
    core_sentinel: str
    install_target: str
    exports: tuple[str, ...]
    leaked_core_attr: str
    expect_gil_used: bool
    preinstall_probe: str
    postinstall_probe: str


COMPANION_SPECS: dict[CompanionPackage, CompanionWheelSpec] = {
    "storage": CompanionWheelSpec(
        package="storage",
        crate_dir="src/rs/vertebra-storage",
        module_name="vertebra._storage",
        stub_entry="vertebra/_storage.pyi",
        binary_prefix="vertebra/_storage",
        core_sentinel="vertebra/storage/_store.py",
        install_target="vertebra-core[storage]",
        exports=("SyncObjectStore", "AsyncObjectStore"),
        leaked_core_attr="SyncObjectStore",
        expect_gil_used=False,
        preinstall_probe=(
            "from vertebra.storage import SyncObjectStore\n"
            "try:\n"
            "    SyncObjectStore('memory://')\n"
            "except MissingDependencyError as exc:\n"
            "    assert exc.install_package == 'vertebra-core[storage]', exc.install_package\n"
            "    assert 'vertebra-core[storage]' in str(exc), str(exc)\n"
            "else:\n"
            "    raise AssertionError('SyncObjectStore did not raise MissingDependencyError')\n"
        ),
        postinstall_probe="assert mod.SyncObjectStore('memory://').scheme == 'memory'\n",
    ),
    "bigquery": CompanionWheelSpec(
        package="bigquery",
        crate_dir="src/rs/vertebra-bigquery",
        module_name="vertebra._bigquery",
        stub_entry="vertebra/_bigquery.pyi",
        binary_prefix="vertebra/_bigquery",
        core_sentinel="vertebra/db/adapters/bigquery/driver.py",
        install_target="vertebra-core[bigquery]",
        exports=("SyncBigQueryClient", "AsyncBigQueryClient", "GIL_USED"),
        leaked_core_attr="SyncBigQueryClient",
        expect_gil_used=True,
        preinstall_probe=(
            "from vertebra.db.adapters.bigquery import BigQueryConfig, SyncBigQueryDriver\n"
            "driver = SyncBigQueryDriver(BigQueryConfig(project_id='test-project'))\n"
            "try:\n"
            "    driver.execute('SELECT 1')\n"
            "except MissingDependencyError as exc:\n"
            "    assert exc.install_package == 'vertebra-core[bigquery]', exc.install_package\n"
            "    assert 'vertebra-core[bigquery]' in str(exc), str(exc)\n"
            "else:\n"
            "    raise AssertionError('SyncBigQueryDriver did not raise MissingDependencyError')\n"
        ),
        postinstall_probe="assert hasattr(mod, 'SyncBigQueryClient') and hasattr(mod, 'AsyncBigQueryClient')\n",
    ),
    "spanner": CompanionWheelSpec(
        package="spanner",
        crate_dir="src/rs/vertebra-spanner",
        module_name="vertebra._spanner",
        stub_entry="vertebra/_spanner.pyi",
        binary_prefix="vertebra/_spanner",
        core_sentinel="vertebra/db/adapters/spanner/driver.py",
        install_target="vertebra-core[spanner]",
        exports=("SyncSpannerClient", "AsyncSpannerClient", "GIL_USED"),
        leaked_core_attr="SyncSpannerClient",
        expect_gil_used=True,
        preinstall_probe=(
            "from vertebra.db.adapters.spanner import SpannerConfig, SyncSpannerDriver\n"
            "driver = SyncSpannerDriver(SpannerConfig(database='projects/p/instances/i/databases/d'))\n"
            "try:\n"
            "    driver.execute('SELECT 1')\n"
            "except MissingDependencyError as exc:\n"
            "    assert exc.install_package == 'vertebra-core[spanner]', exc.install_package\n"
            "    assert 'vertebra-core[spanner]' in str(exc), str(exc)\n"
            "else:\n"
            "    raise AssertionError('SyncSpannerDriver did not raise MissingDependencyError')\n"
        ),
        postinstall_probe="assert hasattr(mod, 'SyncSpannerClient') and hasattr(mod, 'AsyncSpannerClient')\n",
    ),
    "bundler": CompanionWheelSpec(
        package="bundler",
        crate_dir="src/rs/vertebra-bundler",
        module_name="vertebra._bundler",
        stub_entry="vertebra/_bundler.pyi",
        binary_prefix="vertebra/_bundler",
        core_sentinel="vertebra/build/_orchestrator.py",
        install_target="vertebra-core[bundler]",
        exports=("Bundler", "BundleConfig", "GIL_USED", "minify_css"),
        leaked_core_attr="Bundler",
        expect_gil_used=True,
        preinstall_probe=(
            "from vertebra.utils.module_loader import ensure_module\n"
            "try:\n"
            "    ensure_module('vertebra._bundler', package='vertebra-core[bundler]', extra='bundler')\n"
            "except MissingDependencyError as exc:\n"
            "    assert exc.install_package == 'vertebra-core[bundler]', exc.install_package\n"
            "    assert 'vertebra-core[bundler]' in str(exc), str(exc)\n"
            "else:\n"
            "    raise AssertionError('ensure_module did not raise MissingDependencyError for bundler')\n"
        ),
        postinstall_probe="assert 'body{color:red}' in mod.minify_css('body { color: red; }')\n",
    ),
}


def verify_core_wheel_excludes_companion(wheel_path: Path, *, package: CompanionPackage) -> None:
    """Assert `vertebra_core-*.whl` ships the Python wrapper without companion binaries or stubs."""
    spec = COMPANION_SPECS[package]
    with zipfile.ZipFile(wheel_path) as zf:
        names = set(zf.namelist())

    if spec.core_sentinel not in names:
        msg = f"Core wheel {wheel_path.name} is missing {spec.core_sentinel}"
        raise RuntimeError(msg)

    leaked = sorted(
        name
        for name in names
        if name == spec.stub_entry
        or name.startswith(f"{spec.binary_prefix}.")
        or (
            name.startswith("vertebra/_")
            and name.endswith(NATIVE_SUFFIXES)
            and not name.startswith("vertebra/_vertebra.")
        )
    )
    if leaked:
        msg = f"Core wheel {wheel_path.name} leaked optional extension artifacts for {package}: {leaked}"
        raise RuntimeError(msg)


def verify_companion_wheel_contents(wheel_path: Path, *, package: CompanionPackage) -> None:
    """Assert `vertebra_<pkg>-*.whl` ships solely its native binary and matching `.pyi` stub."""
    spec = COMPANION_SPECS[package]
    with zipfile.ZipFile(wheel_path) as zf:
        vtb_entries = sorted(name for name in zf.namelist() if name.startswith("vertebra/"))

    if spec.stub_entry not in vtb_entries:
        msg = f"{package} wheel {wheel_path.name} is missing {spec.stub_entry}: {vtb_entries}"
        raise RuntimeError(msg)

    binaries = [
        name
        for name in vtb_entries
        if (name == f"{spec.binary_prefix}" or name.startswith(f"{spec.binary_prefix}."))
        and name.endswith(NATIVE_SUFFIXES)
    ]
    if len(binaries) != 1:
        msg = f"Expected exactly 1 {spec.binary_prefix} native binary in {wheel_path.name}, found: {binaries}"
        raise RuntimeError(msg)

    sibling_stubs = ALL_KNOWN_STUBS - {spec.stub_entry}
    unexpected = [
        name
        for name in vtb_entries
        if name not in {spec.stub_entry, binaries[0]} or name in sibling_stubs or name.endswith(".py")
    ]
    if unexpected:
        msg = f"{package} wheel {wheel_path.name} contains unexpected vertebra/ entries: {unexpected}"
        raise RuntimeError(msg)


def verify_companion_sdist_contents(sdist_path: Path, *, package: CompanionPackage) -> None:
    """Assert `vertebra_<pkg>-*.tar.gz` contains the crate manifest, build script, source, and stub."""
    spec = COMPANION_SPECS[package]
    with tarfile.open(sdist_path, "r:gz") as tf:
        names = tf.getnames()

    required_suffixes = ("Cargo.toml", "build.rs", "src/lib.rs", spec.stub_entry)
    missing = [suffix for suffix in required_suffixes if not any(name.endswith(suffix) for name in names)]
    if missing:
        msg = f"{package} sdist {sdist_path.name} is missing required entries {missing}: {names}"
        raise RuntimeError(msg)


def verify_isolated_venv_contract(
    core_wheel: Path,
    companion_wheel: Path,
    *,
    package: CompanionPackage,
) -> None:
    """Verify outside-checkout `vertebra-core` fails loud pre-install and works cleanly post-install."""
    spec = COMPANION_SPECS[package]
    with tempfile.TemporaryDirectory(prefix=f"vtb-{package}-venv-") as tmp:
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
                "from vertebra.typing import MissingDependencyError\n"
                "venv_root = pathlib.Path(sys.prefix).resolve()\n"
                "assert pathlib.Path(vertebra.__file__).resolve().is_relative_to(venv_root), vertebra.__file__\n"
                f"assert importlib.util.find_spec({spec.module_name!r}) is None\n"
                f"{spec.preinstall_probe}"
            ),
        )

        _install_wheel_no_deps(venv_python, venv_purelib, companion_wheel, run_dir)
        gil_check = "assert mod.GIL_USED is False\n" if spec.expect_gil_used else ""
        _run_isolated_python(
            venv_python,
            run_dir,
            (
                "import importlib, pathlib, sys\n"
                f"mod = importlib.import_module({spec.module_name!r})\n"
                "from vertebra import _vertebra\n"
                "venv_root = pathlib.Path(sys.prefix).resolve()\n"
                "assert pathlib.Path(mod.__file__).resolve().is_relative_to(venv_root), mod.__file__\n"
                f"{gil_check}"
                f"for export_name in {spec.exports!r}:\n"
                "    assert hasattr(mod, export_name), export_name\n"
                f"assert not hasattr(_vertebra, {spec.leaked_core_attr!r})\n"
                f"{spec.postinstall_probe}"
            ),
        )


def main(argv: Sequence[str] | None = None) -> int:
    """Build or accept core and companion distributions and verify wheel isolation."""
    valid_packages = get_args(CompanionPackage)
    parser = argparse.ArgumentParser(description="Verify standalone companion wheel isolation.")
    parser.add_argument(
        "--package",
        choices=valid_packages,
        default="storage",
        help="Companion package to verify (default: storage).",
    )
    parser.add_argument("--all", action="store_true", help="Verify all companion packages in sequence.")
    parser.add_argument("--core-wheel", type=Path, default=None, help="Prebuilt vertebra_core-*.whl to verify.")
    parser.add_argument(
        "--companion-wheel",
        type=Path,
        default=None,
        help="Prebuilt companion wheel (*.whl) to verify (single-package mode only).",
    )
    parser.add_argument(
        "--companion-sdist",
        type=Path,
        default=None,
        help="Prebuilt companion sdist (*.tar.gz) to verify (single-package mode only).",
    )
    args = parser.parse_args(argv)

    packages: tuple[CompanionPackage, ...] = valid_packages if args.all else (args.package,)
    if args.all and (args.companion_wheel is not None or args.companion_sdist is not None):
        msg = "--companion-wheel and --companion-sdist cannot be combined with --all"
        raise ValueError(msg)

    repo_root = Path(__file__).resolve().parents[2]
    with tempfile.TemporaryDirectory(prefix="vtb-companion-wheel-check-") as tmp:
        out_dir = Path(tmp)
        core_wheel = args.core_wheel or _build_dist(repo_root, repo_root, out_dir / "core", kind="wheel")
        for pkg in packages:
            spec = COMPANION_SPECS[pkg]
            companion_wheel = args.companion_wheel or _build_dist(
                repo_root,
                repo_root / spec.crate_dir,
                out_dir / f"{pkg}-whl",
                kind="wheel",
            )
            companion_sdist = args.companion_sdist or _build_dist(
                repo_root,
                repo_root / spec.crate_dir,
                out_dir / f"{pkg}-sdist",
                kind="sdist",
            )
            verify_core_wheel_excludes_companion(core_wheel, package=pkg)
            verify_companion_wheel_contents(companion_wheel, package=pkg)
            verify_companion_sdist_contents(companion_sdist, package=pkg)
            verify_isolated_venv_contract(core_wheel, companion_wheel, package=pkg)
            sys.stdout.write(f"Verified vertebra-core and vertebra-{pkg} wheel isolation and fail-loud contract.\n")

    return 0


def _build_dist(repo_root: Path, package_dir: Path, out_dir: Path, *, kind: str) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    uv_bin = shutil.which("uv")
    if uv_bin is None:
        msg = "`uv` executable not found on PATH"
        raise RuntimeError(msg)
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
