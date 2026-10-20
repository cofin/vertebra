"""Platform target triples and python-build-standalone release resolution."""

from __future__ import annotations

import platform
import sys
from dataclasses import dataclass

__all__ = (
    "KNOWN_TARGETS",
    "PBS_BASE_URL",
    "PBS_RELEASE_TAG",
    "BuildTargetTuple",
    "resolve_build_target",
)

PBS_RELEASE_TAG: str = "20260301"
PBS_BASE_URL: str = "https://github.com/astral-sh/python-build-standalone/releases/download"

_PATCH_VERSIONS: dict[str, str] = {
    "3.12": "3.12.9",
    "3.13": "3.13.3",
    "3.14": "3.14.0",
    "3.14t": "3.14.0",
}


@dataclass(frozen=True, slots=True)
class BuildTargetTuple:
    """Resolved platform triple and python-build-standalone release coordinates."""

    os_name: str
    arch: str
    libc: str
    python_version: str
    rust_triple: str

    @property
    def is_freethreaded(self) -> bool:
        """Return True when targeting a free-threaded Python build (e.g. 3.14t)."""
        return self.python_version.endswith("t")

    @property
    def pbs_archive_name(self) -> str:
        """Return the python-build-standalone .tar.zst archive filename."""
        full_ver = _PATCH_VERSIONS.get(self.python_version, "3.12.9")
        variant = "freethreaded+pgo+lto" if self.is_freethreaded else "install_only_stripped"
        return f"cpython-{full_ver}+{PBS_RELEASE_TAG}-{self.rust_triple}-{variant}.tar.zst"

    @property
    def pbs_url(self) -> str:
        """Return the full download URL for the python-build-standalone archive."""
        return f"{PBS_BASE_URL}/{PBS_RELEASE_TAG}/{self.pbs_archive_name}"

    @property
    def python_executable(self) -> str:
        """Return the relative Python interpreter binary name inside the PBS distribution."""
        return "python.exe" if self.os_name == "windows" else "python3"

    @classmethod
    def detect_host(cls, *, python_version: str = "3.12") -> BuildTargetTuple:
        """Detect the running host OS and CPU architecture."""
        if sys.platform.startswith("linux"):
            os_name = "linux"
            libc = "gnu"
        elif sys.platform == "darwin":
            os_name = "darwin"
            libc = "apple"
        elif sys.platform in {"win32", "cygwin"}:
            os_name = "windows"
            libc = "msvc"
        else:
            msg = f"Unsupported host platform: {sys.platform}"
            raise RuntimeError(msg)

        machine = platform.machine().lower()
        if machine in {"x86_64", "amd64"}:
            arch = "x86_64"
        elif machine in {"aarch64", "arm64"}:
            arch = "aarch64"
        else:
            msg = f"Unsupported host architecture: {machine}"
            raise RuntimeError(msg)

        if os_name == "linux":
            triple = f"{arch}-unknown-linux-{libc}"
        elif os_name == "darwin":
            triple = f"{arch}-apple-darwin"
        else:
            triple = f"{arch}-pc-windows-msvc"

        return cls(
            os_name=os_name,
            arch=arch,
            libc=libc,
            python_version=python_version,
            rust_triple=triple,
        )


KNOWN_TARGETS: dict[str, tuple[str, str, str, str]] = {
    "linux-x86_64": ("linux", "x86_64", "gnu", "x86_64-unknown-linux-gnu"),
    "linux-x64": ("linux", "x86_64", "gnu", "x86_64-unknown-linux-gnu"),
    "x86_64-unknown-linux-gnu": ("linux", "x86_64", "gnu", "x86_64-unknown-linux-gnu"),
    "linux-aarch64": ("linux", "aarch64", "gnu", "aarch64-unknown-linux-gnu"),
    "linux-arm64": ("linux", "aarch64", "gnu", "aarch64-unknown-linux-gnu"),
    "aarch64-unknown-linux-gnu": ("linux", "aarch64", "gnu", "aarch64-unknown-linux-gnu"),
    "darwin-x86_64": ("darwin", "x86_64", "apple", "x86_64-apple-darwin"),
    "darwin-aarch64": ("darwin", "aarch64", "apple", "aarch64-apple-darwin"),
    "darwin-arm64": ("darwin", "aarch64", "apple", "aarch64-apple-darwin"),
    "macos-aarch64": ("darwin", "aarch64", "apple", "aarch64-apple-darwin"),
    "aarch64-apple-darwin": ("darwin", "aarch64", "apple", "aarch64-apple-darwin"),
    "windows-x86_64": ("windows", "x86_64", "msvc", "x86_64-pc-windows-msvc"),
}


def resolve_build_target(
    target_spec: str | None = None,
    *,
    python_version: str = "3.12",
) -> BuildTargetTuple:
    """Resolve a target alias or Rust triple into a BuildTargetTuple."""
    if not target_spec or target_spec in {"host", "native"}:
        return BuildTargetTuple.detect_host(python_version=python_version)
    entry = KNOWN_TARGETS.get(target_spec)
    if entry is None:
        msg = f"Unsupported build target specification: {target_spec}"
        raise ValueError(msg)
    os_name, arch, libc, triple = entry
    return BuildTargetTuple(
        os_name=os_name,
        arch=arch,
        libc=libc,
        python_version=python_version,
        rust_triple=triple,
    )
