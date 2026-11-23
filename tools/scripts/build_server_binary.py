"""Relocatable vertebra-server binary builder and wheel repair helper."""

from __future__ import annotations

import argparse
import json
import os
import platform
import shlex
import shutil
import subprocess
import sys
import sysconfig
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

__all__ = (
    "ServerBinaryBuilder",
    "ServerBinaryTarget",
    "main",
)

_SERVER_FEATURES = "vertebra-http/python,vertebra-http/tls,vertebra-http/compression,vertebra-http/http3"


def _normalize_platform(raw_platform: str) -> str:
    """Normalize a platform identifier into ``linux``, ``darwin``, or ``windows``."""
    lower = raw_platform.strip().lower()
    if lower.startswith("linux"):
        return "linux"
    if lower in {"darwin", "macos"} or lower.startswith("darwin"):
        return "darwin"
    if lower in {"win32", "windows"} or lower.startswith("win"):
        return "windows"
    msg = f"Unsupported target platform: {raw_platform}"
    raise ValueError(msg)


def _normalize_python_version(raw_version: str) -> str:
    """Return canonical ``MAJOR.MINOR`` version text with optional ``t`` stripped."""
    cleaned = raw_version.strip().rstrip("t")
    parts = cleaned.split(".")
    if len(parts) < 2 or not parts[0].isdigit() or not parts[1].isdigit():
        msg = f"Invalid Python version string: {raw_version!r}"
        raise ValueError(msg)
    return f"{parts[0]}.{parts[1]}"


def _macos_arches_from_archflags(archflags: str | None) -> tuple[str, ...]:
    """Extract ordered macOS architecture tokens from an ``ARCHFLAGS`` string."""
    if not archflags:
        return ()
    tokens = shlex.split(archflags)
    arches: list[str] = []
    idx = 0
    while idx < len(tokens):
        if tokens[idx] == "-arch" and idx + 1 < len(tokens):
            arch = tokens[idx + 1]
            if arch not in arches:
                arches.append(arch)
            idx += 2
            continue
        idx += 1
    return tuple(arches)


def _default_host_arch() -> str:
    """Return normalized host CPU architecture (``x86_64`` or ``arm64``/``aarch64``)."""
    raw = platform.machine().strip().lower()
    if raw in {"amd64", "x86_64"}:
        return "x86_64"
    if raw in {"arm64", "aarch64"}:
        return "arm64"
    return raw


def _default_rust_target(target_platform: str, archflags: str | None, host_arch: str | None = None) -> str:
    """Resolve the Rust target triple for a platform and optional macOS ``ARCHFLAGS``."""
    resolved_arch = host_arch or _default_host_arch()
    if target_platform == "darwin":
        macos_arches = _macos_arches_from_archflags(archflags)
        primary = macos_arches[0] if macos_arches else resolved_arch
        if primary in {"arm64", "aarch64"}:
            return "aarch64-apple-darwin"
        if primary == "x86_64":
            return "x86_64-apple-darwin"
        msg = f"Unsupported macOS architecture: {primary}"
        raise ValueError(msg)
    if target_platform == "linux":
        rust_arch = "aarch64" if resolved_arch in {"arm64", "aarch64"} else "x86_64"
        return f"{rust_arch}-unknown-linux-gnu"
    if target_platform == "windows":
        rust_arch = "aarch64" if resolved_arch in {"arm64", "aarch64"} else "x86_64"
        return f"{rust_arch}-pc-windows-msvc"
    msg = f"Unsupported platform for Rust target: {target_platform}"
    raise ValueError(msg)


def _probe_python_shared_info(python_executable: str | Path) -> tuple[bool, str | None, str | None]:
    """Inspect a Python executable to determine shared-library availability via ``sysconfig``."""
    probe_code = (
        "import json, sysconfig; "
        "print(json.dumps({"
        "'enable_shared': sysconfig.get_config_var('ENABLE_SHARED'), "
        "'libdir': sysconfig.get_config_var('LIBDIR'), "
        "'ldlibrary': sysconfig.get_config_var('LDLIBRARY')"
        "}))"
    )
    proc = subprocess.run(
        [str(python_executable), "-c", probe_code],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        return False, None, None
    try:
        payload = json.loads(proc.stdout.strip())
    except json.JSONDecodeError:
        return False, None, None
    enable_shared = payload.get("enable_shared")
    libdir = payload.get("libdir")
    ldlibrary = payload.get("ldlibrary")
    if isinstance(ldlibrary, str) and ldlibrary.endswith(".a"):
        return False, libdir if isinstance(libdir, str) else None, ldlibrary
    if os.name == "nt":
        return True, libdir if isinstance(libdir, str) else None, ldlibrary if isinstance(ldlibrary, str) else None
    is_explicit_shared = (
        enable_shared is True or enable_shared == 1 or str(enable_shared).strip().lower() in {"1", "true"}
    )
    is_explicit_static = (
        enable_shared is False or enable_shared == 0 or str(enable_shared).strip().lower() in {"0", "false"}
    )
    has_shared_path = isinstance(libdir, str) and bool(libdir) and isinstance(ldlibrary, str) and bool(ldlibrary)
    if is_explicit_static or not has_shared_path:
        is_shared = False
    elif is_explicit_shared:
        is_shared = True
    else:
        assert isinstance(libdir, str) and isinstance(ldlibrary, str)
        is_shared = (Path(libdir) / ldlibrary).exists()
    return is_shared, libdir if isinstance(libdir, str) else None, ldlibrary if isinstance(ldlibrary, str) else None


@dataclass(frozen=True, slots=True)
class ServerBinaryTarget:
    """Platform, Python ABI, and Rust target metadata for ``vertebra-server`` builds."""

    python_version: str
    free_threaded: bool = False
    platform: str = "linux"
    rust_target: str = "x86_64-unknown-linux-gnu"
    archflags: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "python_version", _normalize_python_version(self.python_version))
        object.__setattr__(self, "platform", _normalize_platform(self.platform))

    @classmethod
    def from_host(
        cls,
        *,
        python_version: str | None = None,
        free_threaded: bool | None = None,
        target_platform: str | None = None,
        archflags: str | None = None,
    ) -> ServerBinaryTarget:
        """Build a target descriptor from the active host interpreter and environment."""
        if python_version is None:
            resolved_ver = f"{sys.version_info.major}.{sys.version_info.minor}"
        else:
            resolved_ver = _normalize_python_version(python_version)
        if free_threaded is None:
            resolved_ft = bool(sysconfig.get_config_var("Py_GIL_DISABLED")) or (
                python_version is not None and python_version.strip().endswith("t")
            )
        else:
            resolved_ft = free_threaded
        resolved_platform = _normalize_platform(target_platform or sys.platform)
        resolved_archflags = archflags if archflags is not None else os.environ.get("ARCHFLAGS")
        rust_target = _default_rust_target(resolved_platform, resolved_archflags)
        return cls(
            python_version=resolved_ver,
            free_threaded=resolved_ft,
            platform=resolved_platform,
            rust_target=rust_target,
            archflags=resolved_archflags,
        )

    @property
    def abi_suffix(self) -> str:
        """Return ``'t'`` for free-threaded Python builds or empty string otherwise."""
        return "t" if self.free_threaded else ""

    @property
    def python_request(self) -> str:
        """Return the ``uv python find`` selector for this target interpreter."""
        return f"cpython-{self.python_version}{self.abi_suffix}"

    @property
    def binary_name(self) -> str:
        """Return the staged executable filename for this target platform."""
        return "vertebra-server.exe" if self.platform == "windows" else "vertebra-server"

    @property
    def libpython_soname(self) -> str:
        """Return the shared libpython filename/SONAME excluded during wheel repair."""
        ver = self.python_version
        suffix = self.abi_suffix
        if self.platform == "darwin":
            return f"libpython{ver}{suffix}.dylib"
        if self.platform == "windows":
            compact = ver.replace(".", "")
            return f"python{compact}{suffix}.dll"
        return f"libpython{ver}{suffix}.so.1.0"

    @property
    def libpython_dylib(self) -> str:
        """Return the macOS dynamic library basename rewritten via ``@rpath``."""
        return f"libpython{self.python_version}{self.abi_suffix}.dylib"

    @property
    def delocate_archs(self) -> str:
        """Return the comma-separated macOS architecture specification for ``delocate-wheel``."""
        arches = _macos_arches_from_archflags(self.archflags)
        if arches:
            return ",".join(arches)
        if self.rust_target.startswith("aarch64"):
            return "arm64"
        if self.rust_target.startswith("x86_64"):
            return "x86_64"
        return _default_host_arch()


class ServerBinaryBuilder:
    """Orchestrate relocatable ``vertebra-server`` builds, install-name rewrites, and wheel repair."""

    def __init__(self, repo_root: Path, target: ServerBinaryTarget) -> None:
        self.repo_root = repo_root.resolve()
        self.target = target

    @property
    def cargo_manifest_path(self) -> Path:
        """Return the workspace Cargo.toml path."""
        return self.repo_root / "src" / "rs" / "Cargo.toml"

    @property
    def staged_binary_path(self) -> Path:
        """Return the destination path under ``src/py/vertebra/bin/``."""
        return self.repo_root / "src" / "py" / "vertebra" / "bin" / self.target.binary_name

    def select_build_python(self, candidate: str | Path | None = None) -> Path:
        """Select a shared-library CPython build, falling back to ``uv python find``.

        Raises:
            SystemExit: When neither the candidate interpreter nor a ``uv``-managed shared
                CPython interpreter provides shared ``libpython``.
        """
        initial = Path(candidate) if candidate is not None else Path(sys.executable)
        is_shared, _libdir, _ldlib = _probe_python_shared_info(initial)
        if is_shared:
            return initial

        uv_bin = shutil.which("uv")
        if not uv_bin:
            msg = (
                f"Python interpreter {initial} is statically linked and 'uv' was not found on PATH "
                f"to locate {self.target.python_request} with shared libpython."
            )
            raise SystemExit(msg)

        proc = subprocess.run(
            [uv_bin, "python", "find", self.target.python_request],
            capture_output=True,
            text=True,
            check=False,
        )
        found = proc.stdout.strip().splitlines()[0].strip() if proc.returncode == 0 and proc.stdout.strip() else ""
        if not found:
            stderr_note = proc.stderr.strip() or f"no interpreter matching {self.target.python_request}"
            msg = (
                f"Python interpreter {initial} is static and 'uv python find {self.target.python_request}' "
                f"failed ({stderr_note}). Install a shared CPython build via 'uv python install {self.target.python_version}'."
            )
            raise SystemExit(msg)

        found_path = Path(found)
        uv_is_shared, _uv_libdir, _uv_ldlib = _probe_python_shared_info(found_path)
        if not uv_is_shared:
            msg = (
                f"Located interpreter {found_path} via 'uv python find {self.target.python_request}' "
                "does not provide a shared libpython library (ENABLE_SHARED/LDLIBRARY missing)."
            )
            raise SystemExit(msg)
        return found_path

    def cargo_command(
        self,
        *,
        cargo_bin: str = "cargo",
        target_triple: str | None = None,
        profile: str = "release",
    ) -> list[str]:
        """Construct the ``cargo build`` command for ``vertebra-server``."""
        cmd = [
            cargo_bin,
            "build",
            "--manifest-path",
            str(self.cargo_manifest_path),
            "--bin",
            "vertebra-server",
        ]
        if profile == "release":
            cmd.append("--release")
        else:
            cmd.extend(["--profile", profile])
        cmd.extend(["--features", _SERVER_FEATURES])
        resolved_target = target_triple if target_triple is not None else self.target.rust_target
        if resolved_target:
            cmd.extend(["--target", resolved_target])
        return cmd

    def macos_install_name_changes(
        self,
        binary_path: Path,
        otool_output: str | None = None,
    ) -> list[list[str]]:
        """Return ``install_name_tool`` and ``codesign`` commands to make macOS ``libpython`` relocatable."""
        if self.target.platform != "darwin":
            return []
        inspect_text = otool_output
        if inspect_text is None:
            proc = subprocess.run(
                ["otool", "-L", str(binary_path)],
                capture_output=True,
                text=True,
                check=True,
            )
            inspect_text = proc.stdout

        commands: list[list[str]] = []
        target_ref = f"@rpath/{self.target.libpython_dylib}"
        for raw_line in inspect_text.splitlines()[1:]:
            dep = raw_line.strip().split(" (", 1)[0].strip()
            if not dep or dep == target_ref:
                continue
            dep_name = Path(dep).name
            if dep_name.startswith("libpython") or f"libpython{self.target.python_version}" in dep:
                commands.append(
                    [
                        "install_name_tool",
                        "-change",
                        dep,
                        target_ref,
                        str(binary_path),
                    ]
                )

        if commands:
            commands.append(["codesign", "--force", "--sign", "-", str(binary_path)])
        return commands

    def repair_command(self, wheel: Path, dest_dir: Path) -> list[str]:
        """Return the platform wheel-repair command keeping ``libpython`` external."""
        if self.target.platform == "linux":
            return [
                "auditwheel",
                "repair",
                "-w",
                str(dest_dir),
                "--exclude",
                self.target.libpython_soname,
                str(wheel),
            ]
        if self.target.platform == "darwin":
            return [
                "delocate-wheel",
                "--require-archs",
                self.target.delocate_archs,
                "-w",
                str(dest_dir),
                "-v",
                "--exclude",
                "libpython",
                str(wheel),
            ]
        return []

    def _resolve_cargo_target_dir(self, env: Mapping[str, str]) -> Path:
        """Resolve the Cargo target directory honoring ``CARGO_TARGET_DIR``."""
        if raw := env.get("CARGO_TARGET_DIR"):
            return Path(raw).resolve()
        return (self.repo_root / "src" / "rs" / "target").resolve()

    def build(
        self,
        *,
        build_python: Path | None = None,
        cargo_bin: str = "cargo",
        target_triple: str | None = None,
        extra_env: Mapping[str, str] | None = None,
        profile: str = "release",
    ) -> Path:
        """Compile ``vertebra-server`` against a shared CPython build and stage it under ``src/py/vertebra/bin/``."""
        selected_python = self.select_build_python(build_python).resolve()
        env = dict(os.environ)
        if extra_env:
            env.update(extra_env)
        env["PYO3_PYTHON"] = str(selected_python)
        env["PYTHONNOUSERSITE"] = "1"

        resolved_target = target_triple if target_triple is not None else self.target.rust_target
        cmd = self.cargo_command(cargo_bin=cargo_bin, target_triple=resolved_target, profile=profile)
        subprocess.run(cmd, cwd=self.repo_root, env=env, check=True)

        profile_dir = "debug" if profile == "dev" else profile
        target_root = self._resolve_cargo_target_dir(env)
        built_candidates: list[Path] = []
        if resolved_target:
            built_candidates.append(target_root / resolved_target / profile_dir / self.target.binary_name)
        built_candidates.append(target_root / profile_dir / self.target.binary_name)
        built_binary = next((path for path in built_candidates if path.is_file()), None)
        if built_binary is None:
            checked = ", ".join(str(p) for p in built_candidates)
            msg = f"Built vertebra-server binary not found at any expected path: {checked}"
            raise FileNotFoundError(msg)

        if self.target.platform == "darwin":
            for post_cmd in self.macos_install_name_changes(built_binary):
                subprocess.run(post_cmd, check=True)

        dest = self.staged_binary_path
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(built_binary, dest)
        if self.target.platform != "windows":
            dest.chmod(0o755)
        return dest


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for building and repairing relocatable ``vertebra-server`` binaries."""
    parser = argparse.ArgumentParser(description="Build and package relocatable vertebra-server binaries.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    build_parser = subparsers.add_parser("build", help="Compile vertebra-server and stage it into vertebra/bin/")
    build_parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    build_parser.add_argument("--python-version", default=None)
    build_parser.add_argument("--free-threaded", action="store_true", default=None)
    build_parser.add_argument("--platform", default=None)
    build_parser.add_argument("--rust-target", default=None)
    build_parser.add_argument("--build-python", type=Path, default=None)
    build_parser.add_argument("--profile", default="release", help="Cargo profile to build (default: release).")
    build_parser.add_argument(
        "--no-target-triple",
        action="store_true",
        help="Omit --target from cargo build so host target cache directories are shared.",
    )

    repair_parser = subparsers.add_parser("repair", help="Repair a built core wheel excluding libpython")
    repair_parser.add_argument("wheel", type=Path)
    repair_parser.add_argument("-w", "--wheel-dir", type=Path, required=True)
    repair_parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    repair_parser.add_argument("--python-version", default=None)
    repair_parser.add_argument("--free-threaded", action="store_true", default=None)
    repair_parser.add_argument("--platform", default=None)
    repair_parser.add_argument("--rust-target", default=None)

    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint for building and repairing ``vertebra-server`` artifacts."""
    args = _parse_args(argv)
    target = ServerBinaryTarget.from_host(
        python_version=args.python_version,
        free_threaded=args.free_threaded,
        target_platform=args.platform,
    )
    if args.rust_target is not None:
        target = ServerBinaryTarget(
            python_version=target.python_version,
            free_threaded=target.free_threaded,
            platform=target.platform,
            rust_target=args.rust_target,
            archflags=target.archflags,
        )
    builder = ServerBinaryBuilder(repo_root=args.repo_root, target=target)

    if args.command == "build":
        resolved_triple = "" if args.no_target_triple else target.rust_target
        staged = builder.build(
            build_python=args.build_python,
            target_triple=resolved_triple,
            profile=args.profile,
        )
        sys.stdout.write(f"Staged vertebra-server binary at {staged}\n")
        return 0

    if args.command == "repair":
        args.wheel_dir.mkdir(parents=True, exist_ok=True)
        cmd = builder.repair_command(args.wheel, args.wheel_dir)
        if cmd:
            subprocess.run(cmd, check=True)
        else:
            shutil.copy2(args.wheel, args.wheel_dir / args.wheel.name)
        return 0

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
