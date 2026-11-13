"""Shared archive, linkage, and isolated virtualenv helpers for wheel isolation verifiers."""

from __future__ import annotations

import os
import platform
import shutil
import subprocess
import sys
import sysconfig
import tempfile
import venv
import zipfile
from pathlib import Path

__all__ = (
    "NATIVE_SUFFIXES",
    "assert_zero_native_linkage",
    "build_dist",
    "clean_isolated_env",
    "install_wheel_no_deps",
    "query_venv_purelib",
    "run_isolated_python",
    "seed_third_party_runtime_deps",
    "venv_python_path",
    "verify_isolated_wheel_pair",
)

NATIVE_SUFFIXES: tuple[str, ...] = (".so", ".pyd", ".dylib")


def build_dist(repo_root: Path, package_dir: Path, out_dir: Path, *, kind: str) -> Path:
    """Build a single wheel or sdist archive without build isolation and return its path."""
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


def assert_zero_native_linkage(
    shared_lib: Path,
    *,
    forbidden_libs: tuple[str, ...],
    tech_label: str,
) -> None:
    """Verify a compiled native extension references zero forbidden external C libraries."""
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
        found = [lib for lib in forbidden_libs if lib in inspected_output]
        if found:
            msg = f"{shared_lib.name} dynamically links forbidden {tech_label} C libraries {found}:\n{inspected_output}"
            raise RuntimeError(msg)
        return

    raw_bytes = shared_lib.read_bytes().lower()
    found_raw = [lib for lib in forbidden_libs if f"{lib}.".encode("ascii") in raw_bytes]
    if found_raw:
        msg = f"{shared_lib.name} references forbidden {tech_label} C libraries: {found_raw}"
        raise RuntimeError(msg)


def venv_python_path(venv_dir: Path) -> Path:
    """Return the Python interpreter path inside an isolated virtual environment."""
    if platform.system() == "Windows":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def clean_isolated_env() -> dict[str, str]:
    """Return a clean environment dictionary with `PYTHONNOUSERSITE=1`."""
    preserved_keys = ("PATH", "HOME", "TMPDIR", "TEMP", "TMP", "SYSTEMROOT", "LANG", "LC_ALL")
    env = {key: os.environ[key] for key in preserved_keys if key in os.environ}
    env["PYTHONNOUSERSITE"] = "1"
    return env


def query_venv_purelib(venv_python: Path, cwd: Path) -> Path:
    """Resolve the resolved `purelib` directory inside an isolated virtual environment."""
    proc = subprocess.run(
        [str(venv_python), "-I", "-c", "import sysconfig; print(sysconfig.get_path('purelib'))"],
        cwd=cwd,
        env=clean_isolated_env(),
        capture_output=True,
        text=True,
        check=True,
    )
    return Path(proc.stdout.strip()).resolve()


def seed_third_party_runtime_deps(dest_site_packages: Path) -> None:
    """Symlink non-Vertebra runtime dependencies from the host environment into an offline test venv."""
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


def install_wheel_no_deps(venv_python: Path, venv_purelib: Path, wheel_path: Path, cwd: Path) -> None:
    """Install a single built wheel offline without pulling index dependencies."""
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
            env=clean_isolated_env(),
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode == 0:
            return
    with zipfile.ZipFile(wheel_path) as zf:
        zf.extractall(venv_purelib)


def run_isolated_python(venv_python: Path, cwd: Path, code: str) -> None:
    """Execute Python assertions in `-I` isolated mode outside the repository checkout."""
    proc = subprocess.run(
        [str(venv_python), "-I", "-c", code],
        cwd=cwd,
        env=clean_isolated_env(),
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        sys.stderr.write(proc.stdout)
        sys.stderr.write(proc.stderr)
        msg = f"Isolated venv check failed with exit code {proc.returncode}"
        raise RuntimeError(msg)


def verify_isolated_wheel_pair(
    *,
    temp_prefix: str,
    core_wheel: Path,
    ext_wheel: Path,
    preinstall_code: str,
    postinstall_code: str,
) -> None:
    """Create an isolated venv, verify core-only failure contracts, install extension, and run smoke checks."""
    with tempfile.TemporaryDirectory(prefix=temp_prefix) as tmp:
        root = Path(tmp).resolve()
        venv_dir = root / "isolated-venv"
        run_dir = root / "outside-checkout"
        run_dir.mkdir(parents=True, exist_ok=True)

        venv.EnvBuilder(with_pip=False, clear=True, symlinks=True).create(venv_dir)
        venv_python = venv_python_path(venv_dir)
        venv_purelib = query_venv_purelib(venv_python, run_dir)
        seed_third_party_runtime_deps(venv_purelib)

        install_wheel_no_deps(venv_python, venv_purelib, core_wheel, run_dir)
        run_isolated_python(venv_python, run_dir, preinstall_code)

        install_wheel_no_deps(venv_python, venv_purelib, ext_wheel, run_dir)
        run_isolated_python(venv_python, run_dir, postinstall_code)
