"""Build, stage, and verify Vertebra wheels and sdists for build-once CI workflows."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING

import msgspec

from tools.scripts import verify_oracle_wheel, verify_postgres_wheel, verify_storage_wheel
from tools.scripts._wheel_verify import (
    NATIVE_SUFFIXES,
    build_dist,
    verify_standalone_extension_wheel,
)
from tools.scripts.build_server_binary import ServerBinaryBuilder, ServerBinaryTarget

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = (
    "ALL_PACKAGES",
    "COMPANION_PACKAGES",
    "SDIST_VERIFIED_PACKAGES",
    "STANDALONE_CHECKS",
    "StandaloneWheelCheck",
    "build_ci_distributions",
    "install_prebuilt_wheels",
    "main",
    "parse_packages",
    "stage_wheel_binaries",
    "verify_prebuilt_wheels",
)


class StandaloneWheelCheck(msgspec.Struct, frozen=True, gc=False):
    """Archive isolation parameters for standalone optional driver wheel checks."""

    package: str
    forbid_python_files: bool = False


COMPANION_PACKAGES: tuple[str, ...] = (
    "bundler",
    "storage",
    "mysql",
    "bigquery",
    "mssql",
    "spanner",
    "duckdb",
    "oracle",
    "postgres",
    "sqlite",
    "redis",
    "valkey",
)
ALL_PACKAGES: tuple[str, ...] = ("core", *COMPANION_PACKAGES)
SDIST_VERIFIED_PACKAGES: tuple[str, ...] = (
    "bigquery",
    "bundler",
    "oracle",
    "postgres",
    "spanner",
    "storage",
)
STANDALONE_CHECKS: tuple[StandaloneWheelCheck, ...] = (
    StandaloneWheelCheck(package="sqlite", forbid_python_files=False),
    StandaloneWheelCheck(package="duckdb", forbid_python_files=False),
    StandaloneWheelCheck(package="mysql", forbid_python_files=True),
    StandaloneWheelCheck(package="mssql", forbid_python_files=True),
    StandaloneWheelCheck(package="redis", forbid_python_files=True),
    StandaloneWheelCheck(package="valkey", forbid_python_files=True),
)


def parse_packages(raw: str | None) -> tuple[str, ...]:
    """Parse a comma-separated package filter into canonical package names.

    Raises:
        ValueError: When an unknown package name is supplied.
    """
    if raw is None or not raw.strip() or raw.strip().lower() == "all":
        return ALL_PACKAGES
    selected: list[str] = []
    known = set(ALL_PACKAGES)
    for token in raw.split(","):
        name = token.strip().lower()
        if not name:
            continue
        if name not in known:
            msg = f"Unknown package {name!r}; expected one of: {', '.join(ALL_PACKAGES)}"
            raise ValueError(msg)
        if name not in selected:
            selected.append(name)
    if not selected:
        return ALL_PACKAGES
    return tuple(selected)


def build_ci_distributions(
    repo_root: Path,
    *,
    wheel_dir: Path,
    packages: Sequence[str] = ALL_PACKAGES,
    profile: str = "dev",
    strip: bool = True,
    skip_auditwheel: bool = True,
    with_server: bool = False,
    server_profile: str | None = None,
    sdist_dir: Path | None = None,
    interpreter: Path | None = None,
) -> list[Path]:
    """Compile requested Vertebra wheels and optional companion sdists once per interpreter."""
    resolved_root = repo_root.resolve()
    wheel_dir.mkdir(parents=True, exist_ok=True)
    raw_py = interpreter or Path(sys.executable)
    maturin_bin = _resolve_maturin_executable(raw_py)
    py_bin = raw_py.resolve()

    env = dict(os.environ)
    env["PYO3_PYTHON"] = str(py_bin)
    env["PYTHONNOUSERSITE"] = "1"
    if profile == "dev":
        env.setdefault("CARGO_PROFILE_DEV_DEBUG", "0")
    env.setdefault("CARGO_INCREMENTAL", "0")

    if with_server and "core" in packages:
        target = ServerBinaryTarget.from_host()
        builder = ServerBinaryBuilder(repo_root=resolved_root, target=target)
        builder.build(
            build_python=py_bin,
            target_triple="",
            extra_env=env,
            profile=server_profile or profile,
        )

    built_wheels: list[Path] = []
    for pkg in packages:
        pattern = f"vertebra_{pkg}-*.whl"
        for stale in wheel_dir.glob(pattern):
            stale.unlink()
        cmd = _maturin_build_command(
            maturin_bin=maturin_bin,
            repo_root=resolved_root,
            package=pkg,
            wheel_dir=wheel_dir,
            interpreter=py_bin,
            profile=profile,
            strip=strip,
            skip_auditwheel=skip_auditwheel,
        )
        subprocess.run(cmd, cwd=resolved_root, env=env, check=True)
        matches = sorted(wheel_dir.glob(pattern))
        if not matches:
            msg = f"Expected wheel matching {pattern!r} inside {wheel_dir}"
            raise RuntimeError(msg)
        built_wheels.append(matches[-1])

    if sdist_dir is not None:
        sdist_dir.mkdir(parents=True, exist_ok=True)
        for pkg in packages:
            if pkg not in SDIST_VERIFIED_PACKAGES:
                continue
            crate_dir = resolved_root / "src" / "rs" / f"vertebra-{pkg}"
            pkg_sdist_out = sdist_dir / pkg
            sdist_path = build_dist(resolved_root, crate_dir, pkg_sdist_out, kind="sdist")
            dest = sdist_dir / sdist_path.name
            shutil.copy2(sdist_path, dest)
            shutil.rmtree(pkg_sdist_out, ignore_errors=True)

    return built_wheels


def stage_wheel_binaries(
    wheel_paths: Sequence[Path],
    package_dir: Path,
) -> list[Path]:
    """Extract compiled native extensions and bundled server binary into ``src/py/vertebra``."""
    package_dir.mkdir(parents=True, exist_ok=True)
    staged: list[Path] = []
    for wheel_path in wheel_paths:
        with zipfile.ZipFile(wheel_path) as archive:
            for member in archive.namelist():
                if not member.startswith("vertebra/"):
                    continue
                relative = member.removeprefix("vertebra/")
                if not relative or relative.endswith("/"):
                    continue
                is_native = "/" not in relative and relative.endswith(NATIVE_SUFFIXES)
                is_server_bin = relative in {"bin/vertebra-server", "bin/vertebra-server.exe"}
                if not (is_native or is_server_bin):
                    continue
                destination = package_dir / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                with archive.open(member) as src, destination.open("wb") as dst:
                    shutil.copyfileobj(src, dst)
                if os.name != "nt":
                    destination.chmod(0o755)
                staged.append(destination)
    return staged


def install_prebuilt_wheels(
    repo_root: Path,
    *,
    wheel_dir: Path,
    packages: Sequence[str] = ALL_PACKAGES,
    python_bin: Path | None = None,
    stage_editable: bool = True,
) -> list[Path]:
    """Install selected prebuilt wheels into the active venv and stage native binaries into ``src/py/vertebra``."""
    resolved_root = repo_root.resolve()
    target_python = Path(os.path.abspath(python_bin if python_bin is not None else sys.executable))
    wheel_paths = [_find_single_archive(wheel_dir, f"vertebra_{pkg}-*.whl") for pkg in packages]

    uv_bin = shutil.which("uv")
    if uv_bin is not None:
        cmd = [
            uv_bin,
            "pip",
            "install",
            "--python",
            str(target_python),
            "--no-index",
            "--no-deps",
            "--reinstall",
            *(str(path) for path in wheel_paths),
        ]
        subprocess.run(cmd, cwd=resolved_root, check=True)
    else:
        subprocess.run(
            [
                str(target_python),
                "-m",
                "pip",
                "install",
                "--no-deps",
                "--force-reinstall",
                *(str(p) for p in wheel_paths),
            ],
            cwd=resolved_root,
            check=True,
        )

    if stage_editable:
        stage_wheel_binaries(wheel_paths, resolved_root / "src" / "py" / "vertebra")

    return wheel_paths


def verify_prebuilt_wheels(
    repo_root: Path,
    *,
    wheel_dir: Path,
    sdist_dir: Path,
    smoke_vtb: bool = True,
) -> None:
    """Run archive, linkage, isolation, and ``vtb run`` smoke verification against prebuilt distributions."""
    resolved_root = repo_root.resolve()
    core_wheel = _find_single_archive(wheel_dir, "vertebra_core-*.whl")
    postgres_wheel = _find_single_archive(wheel_dir, "vertebra_postgres-*.whl")
    postgres_sdist = _find_single_archive(sdist_dir, "vertebra_postgres-*.tar.gz")
    oracle_wheel = _find_single_archive(wheel_dir, "vertebra_oracle-*.whl")
    oracle_sdist = _find_single_archive(sdist_dir, "vertebra_oracle-*.tar.gz")

    verify_postgres_wheel.main(
        [
            "--core-wheel",
            str(core_wheel),
            "--postgres-wheel",
            str(postgres_wheel),
            "--postgres-sdist",
            str(postgres_sdist),
        ]
    )
    verify_oracle_wheel.main(
        [
            "--core-wheel",
            str(core_wheel),
            "--oracle-wheel",
            str(oracle_wheel),
            "--oracle-sdist",
            str(oracle_sdist),
        ]
    )
    verify_storage_wheel.main(
        [
            "--all",
            "--core-wheel",
            str(core_wheel),
            "--wheel-dir",
            str(wheel_dir),
            "--sdist-dir",
            str(sdist_dir),
        ]
    )
    for check in STANDALONE_CHECKS:
        verify_standalone_extension_wheel(
            wheel_dir,
            wheel_glob=f"vertebra_{check.package}-*.whl",
            extension_prefix=f"vertebra/_{check.package}",
            forbid_python_files=check.forbid_python_files,
        )
        sys.stdout.write(f"Verified standalone vertebra-{check.package} wheel layout.\n")

    if smoke_vtb:
        _run_vtb_smoke(resolved_root, core_wheel, wheel_dir)


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint supporting ``build``, ``install``, and ``verify`` subcommands."""
    args = _parse_args(argv)
    repo_root = args.repo_root.resolve()

    if args.command == "build":
        packages = parse_packages(args.packages)
        sdist_out: Path | None = args.sdist_dir
        built = build_ci_distributions(
            repo_root,
            wheel_dir=args.wheel_dir,
            packages=packages,
            profile=args.profile,
            strip=not args.no_strip,
            skip_auditwheel=not args.auditwheel,
            with_server=args.with_server,
            server_profile=args.server_profile,
            sdist_dir=sdist_out,
            interpreter=args.interpreter,
        )
        sys.stdout.write(f"Built {len(built)} wheel(s) in {args.wheel_dir}.\n")
        return 0

    if args.command == "install":
        packages = parse_packages(args.packages)
        installed = install_prebuilt_wheels(
            repo_root,
            wheel_dir=args.wheel_dir,
            packages=packages,
            python_bin=args.python,
            stage_editable=not args.no_stage_editable,
        )
        sys.stdout.write(f"Installed {len(installed)} prebuilt wheel(s) from {args.wheel_dir}.\n")
        return 0

    if args.command == "verify":
        verify_prebuilt_wheels(
            repo_root,
            wheel_dir=args.wheel_dir,
            sdist_dir=args.sdist_dir,
            smoke_vtb=not args.no_smoke_vtb,
        )
        return 0

    return 2


def _resolve_maturin_executable(interpreter: Path) -> str:
    sibling = interpreter.parent / ("maturin.exe" if os.name == "nt" else "maturin")
    if sibling.is_file():
        return str(sibling)
    found = shutil.which("maturin")
    if found is not None:
        return found
    msg = "`maturin` executable not found in active Python environment or PATH"
    raise RuntimeError(msg)


def _maturin_build_command(
    *,
    maturin_bin: str,
    repo_root: Path,
    package: str,
    wheel_dir: Path,
    interpreter: Path,
    profile: str,
    strip: bool,
    skip_auditwheel: bool,
) -> list[str]:
    cmd = [
        maturin_bin,
        "build",
        "--profile",
        profile,
        "--interpreter",
        str(interpreter),
        "--out",
        str(wheel_dir),
    ]
    if strip:
        cmd.append("--strip")
    if skip_auditwheel and sys.platform.startswith("linux"):
        cmd.extend(["--auditwheel", "skip"])
    if package != "core":
        manifest = repo_root / "src" / "rs" / f"vertebra-{package}" / "Cargo.toml"
        cmd.extend(["--manifest-path", str(manifest)])
    return cmd


def _find_single_archive(directory: Path, pattern: str) -> Path:
    matches = sorted(directory.glob(pattern))
    if not matches:
        msg = f"No archive matching {pattern!r} found in {directory}"
        raise FileNotFoundError(msg)
    return matches[-1]


def _run_vtb_smoke(repo_root: Path, core_wheel: Path, wheel_dir: Path) -> None:
    uv_bin = shutil.which("uv")
    if uv_bin is None:
        msg = "`uv` executable not found on PATH for vtb smoke verification"
        raise RuntimeError(msg)
    with tempfile.TemporaryDirectory(prefix="vtb-core-smoke-") as tmp:
        tmp_root = Path(tmp)
        venv_dir = tmp_root / "venv"
        workdir = tmp_root / "app"
        subprocess.run([uv_bin, "venv", "--python", sys.executable, str(venv_dir)], check=True)
        py_bin = venv_dir / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        vtb_bin = venv_dir / ("Scripts/vtb.exe" if os.name == "nt" else "bin/vtb")
        subprocess.run(
            [uv_bin, "pip", "install", "--python", str(py_bin), str(core_wheel)],
            cwd=repo_root,
            check=True,
        )
        subprocess.run(
            [
                sys.executable,
                str(repo_root / "tools" / "scripts" / "smoke_vtb_run.py"),
                "--vtb",
                str(vtb_bin),
                "--wheel-dir",
                str(wheel_dir),
                "--workdir",
                str(workdir),
            ],
            cwd=repo_root,
            check=True,
        )


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build, install, and verify Vertebra CI wheel distributions.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    build_parser = subparsers.add_parser("build", help="Compile requested wheels and optional companion sdists.")
    build_parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    build_parser.add_argument("--wheel-dir", type=Path, required=True)
    build_parser.add_argument("--sdist-dir", type=Path, default=None)
    build_parser.add_argument("--packages", default="all", help="Comma-separated packages or 'all'.")
    build_parser.add_argument("--profile", default="dev", help="Cargo profile for maturin build (default: dev).")
    build_parser.add_argument("--server-profile", default=None, help="Override Cargo profile for vertebra-server.")
    build_parser.add_argument("--with-server", action="store_true", help="Compile and bundle vertebra-server.")
    build_parser.add_argument("--no-strip", action="store_true", help="Skip stripping compiled binaries.")
    build_parser.add_argument("--auditwheel", action="store_true", help="Enable auditwheel repair/check on Linux.")
    build_parser.add_argument("--interpreter", type=Path, default=None)

    install_parser = subparsers.add_parser("install", help="Install prebuilt wheels and stage native binaries.")
    install_parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    install_parser.add_argument("--wheel-dir", type=Path, required=True)
    install_parser.add_argument("--packages", default="all", help="Comma-separated packages or 'all'.")
    install_parser.add_argument("--python", type=Path, default=None)
    install_parser.add_argument("--no-stage-editable", action="store_true")

    verify_parser = subparsers.add_parser("verify", help="Verify all 13 prebuilt wheels and isolation contracts.")
    verify_parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    verify_parser.add_argument("--wheel-dir", type=Path, required=True)
    verify_parser.add_argument("--sdist-dir", type=Path, required=True)
    verify_parser.add_argument("--no-smoke-vtb", action="store_true")

    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
