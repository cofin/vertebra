"""Shared archive, linkage, and isolated virtualenv helpers for wheel isolation verifiers."""

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
    "NATIVE_SUFFIXES",
    "assert_zero_native_linkage",
    "build_dist",
    "verify_core_wheel_excludes",
    "verify_extension_wheel_contents",
    "verify_isolated_wheel_pair",
    "verify_sdist_contents",
    "verify_standalone_extension_wheel",
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


def verify_core_wheel_excludes(
    wheel_path: Path,
    *,
    required_entries: tuple[str, ...],
    stub_entry: str,
    binary_prefix: str,
    package_label: str | None = None,
) -> None:
    """Assert a core wheel ships required Python wrappers without optional extension artifacts."""
    with zipfile.ZipFile(wheel_path) as zf:
        names = set(zf.namelist())

    missing = [entry for entry in required_entries if entry not in names]
    if missing:
        msg = f"Core wheel {wheel_path.name} is missing {', '.join(missing)}"
        raise RuntimeError(msg)

    leaked = sorted(
        name
        for name in names
        if name == stub_entry
        or name.startswith(f"{binary_prefix}.")
        or (
            name.startswith("vertebra/_")
            and name.endswith(NATIVE_SUFFIXES)
            and not name.startswith("vertebra/_vertebra.")
        )
    )
    if leaked:
        suffix = f" for {package_label}" if package_label else ""
        msg = f"Core wheel {wheel_path.name} leaked optional extension artifacts{suffix}: {leaked}"
        raise RuntimeError(msg)


def verify_extension_wheel_contents(
    wheel_path: Path,
    *,
    label: str,
    stub_entry: str,
    binary_prefix: str,
    forbidden_libs: tuple[str, ...] = (),
    link_temp_prefix: str = "vtb-link-",
) -> None:
    """Assert an optional extension wheel ships only its stub, native binary, and clean linkage."""
    with zipfile.ZipFile(wheel_path) as zf:
        vtb_entries = sorted(name for name in zf.namelist() if name.startswith("vertebra/"))
        if stub_entry not in vtb_entries:
            msg = f"{label} wheel {wheel_path.name} is missing {stub_entry}: {vtb_entries}"
            raise RuntimeError(msg)

        binaries = [
            name for name in vtb_entries if name.startswith(f"{binary_prefix}.") and name.endswith(NATIVE_SUFFIXES)
        ]
        if len(binaries) != 1:
            msg = f"Expected exactly 1 {binary_prefix} native binary in {wheel_path.name}, found: {binaries}"
            raise RuntimeError(msg)

        unexpected = [name for name in vtb_entries if name not in {stub_entry, binaries[0]}]
        if unexpected:
            msg = f"{label} wheel {wheel_path.name} contains unexpected vertebra/ entries: {unexpected}"
            raise RuntimeError(msg)

        if forbidden_libs:
            with tempfile.TemporaryDirectory(prefix=link_temp_prefix) as tmp:
                extracted_lib = Path(zf.extract(binaries[0], path=tmp))
                assert_zero_native_linkage(extracted_lib, forbidden_libs=forbidden_libs, tech_label=label)


def verify_sdist_contents(
    sdist_path: Path,
    *,
    label: str,
    required_suffixes: tuple[str, ...],
) -> None:
    """Assert a companion sdist contains Cargo metadata, build script, Rust sources, and `.pyi` stub."""
    with tarfile.open(sdist_path, "r:gz") as tf:
        names = tf.getnames()

    missing = [suffix for suffix in required_suffixes if not any(name.endswith(suffix) for name in names)]
    if missing:
        msg = f"{label} sdist {sdist_path.name} is missing required entries {missing}: {names}"
        raise RuntimeError(msg)


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
        venv_python = _venv_python_path(venv_dir)
        venv_purelib = _query_venv_purelib(venv_python, run_dir)
        _seed_third_party_runtime_deps(venv_purelib)

        _install_wheel_no_deps(venv_python, venv_purelib, core_wheel, run_dir)
        _run_isolated_python(venv_python, run_dir, preinstall_code)

        _install_wheel_no_deps(venv_python, venv_purelib, ext_wheel, run_dir)
        _run_isolated_python(venv_python, run_dir, postinstall_code)


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


def verify_standalone_extension_wheel(
    wheel_dir: Path,
    *,
    wheel_glob: str,
    extension_prefix: str,
    forbid_python_files: bool = False,
) -> Path:
    """Verify a standalone extension wheel ships its binary without unrelated namespace files."""
    wheels = sorted(wheel_dir.glob(wheel_glob))
    if not wheels:
        msg = f"No wheel matching {wheel_glob!r} produced in {wheel_dir}"
        raise RuntimeError(msg)
    wheel_path = wheels[-1]
    with zipfile.ZipFile(wheel_path) as zf:
        names = zf.namelist()
    if not any(name.startswith(extension_prefix) and name.endswith(NATIVE_SUFFIXES) for name in names):
        msg = f"No {extension_prefix} native binary found in {wheel_path.name}: {names}"
        raise RuntimeError(msg)
    unexpected = [
        name
        for name in names
        if name.startswith("vertebra/")
        and not name.startswith(extension_prefix)
        or (forbid_python_files and name.endswith(".py"))
    ]
    if unexpected:
        msg = f"Unexpected payload files in {wheel_path.name}: {unexpected}"
        raise RuntimeError(msg)
    return wheel_path


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for standalone wheel payload verification."""
    parser = argparse.ArgumentParser(description="Verify standalone Vertebra extension wheel isolation.")
    parser.add_argument("--wheel-dir", type=Path, required=True)
    parser.add_argument("--wheel-glob", required=True)
    parser.add_argument("--extension-prefix", required=True)
    parser.add_argument("--forbid-python-files", action="store_true")
    args = parser.parse_args(argv)
    verify_standalone_extension_wheel(
        args.wheel_dir,
        wheel_glob=args.wheel_glob,
        extension_prefix=args.extension_prefix,
        forbid_python_files=args.forbid_python_files,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
