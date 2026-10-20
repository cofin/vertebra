"""CPython PGO, LTO, and BOLT optimization detection and platform support matrix."""

import json
import platform
import subprocess
import sys
import sysconfig
from dataclasses import dataclass

__all__ = (
    "CPYTHON_PGO_FLAGS",
    "PLATFORM_MATRIX",
    "PlatformSupport",
    "PythonBuildInfo",
    "check_pgo_availability",
    "detect_python_build",
    "get_platform_support",
)


@dataclass(frozen=True, slots=True)
class PythonBuildInfo:
    """Immutable snapshot of a CPython interpreter's optimization configuration."""

    version: str
    is_pgo: bool
    is_lto: bool
    is_debug: bool
    architecture: str


@dataclass(frozen=True, slots=True)
class PlatformSupport:
    """CPython optimization availability record for an OS and CPU architecture pair."""

    os_name: str
    architecture: str
    cpython_pgo: bool
    cpython_lto: bool
    cpython_bolt: bool
    notes: str


CPYTHON_PGO_FLAGS: dict[str, str] = {
    "--enable-optimizations": "Enables CPython Profile-Guided Optimization (PGO) for 10-20% interpreter speedup.",
    "--with-lto": "Enables Link-Time Optimization (LTO) across translation units.",
    "--enable-bolt": "Enables LLVM BOLT post-link binary layout optimization.",
}

PLATFORM_MATRIX: list[PlatformSupport] = [
    PlatformSupport(
        os_name="linux",
        architecture="x86_64",
        cpython_pgo=True,
        cpython_lto=True,
        cpython_bolt=True,
        notes="PGO, LTO, and BOLT all supported on Linux x86_64.",
    ),
    PlatformSupport(
        os_name="linux",
        architecture="aarch64",
        cpython_pgo=True,
        cpython_lto=True,
        cpython_bolt=False,
        notes="PGO and LTO stable on Linux ARM64.",
    ),
    PlatformSupport(
        os_name="darwin",
        architecture="x86_64",
        cpython_pgo=True,
        cpython_lto=True,
        cpython_bolt=False,
        notes="PGO and LTO supported via Apple Clang.",
    ),
    PlatformSupport(
        os_name="darwin",
        architecture="arm64",
        cpython_pgo=True,
        cpython_lto=True,
        cpython_bolt=False,
        notes="PGO and LTO supported on Apple Silicon.",
    ),
    PlatformSupport(
        os_name="windows",
        architecture="x64",
        cpython_pgo=True,
        cpython_lto=False,
        cpython_bolt=False,
        notes="PGO supported via MSVC /LTCG:PGO.",
    ),
]


def detect_python_build(executable: str = "python") -> PythonBuildInfo:
    """Inspect the specified Python interpreter and return its PythonBuildInfo."""
    config_vars = _query_sysconfig(executable)
    version = _query_version(executable)
    return PythonBuildInfo(
        version=version,
        is_pgo=_detect_pgo(config_vars),
        is_lto=_detect_lto(config_vars),
        is_debug=_detect_debug(config_vars),
        architecture=platform.machine(),
    )


def check_pgo_availability(executable: str = "python") -> dict[str, bool]:
    """Return a dictionary of active optimization flags (pgo, lto, bolt, debug)."""
    config_vars = _query_sysconfig(executable)
    return {
        "pgo": _detect_pgo(config_vars),
        "lto": _detect_lto(config_vars),
        "bolt": _detect_bolt(config_vars),
        "debug": _detect_debug(config_vars),
    }


def get_platform_support() -> PlatformSupport:
    """Return the PlatformSupport record matching the current host OS and architecture."""
    os_name = platform.system().lower()
    arch = platform.machine().lower()
    arch_map = {
        "amd64": "x64" if os_name == "windows" else "x86_64",
        "x86_64": "x86_64",
        "aarch64": "aarch64",
        "arm64": "arm64" if os_name == "darwin" else "aarch64",
    }
    normalized = arch_map.get(arch, arch)
    for entry in PLATFORM_MATRIX:
        if entry.os_name == os_name and entry.architecture == normalized:
            return entry
    msg = f"Unsupported platform combination: os={os_name!r}, arch={normalized!r}"
    raise RuntimeError(msg)


def _query_sysconfig(executable: str) -> dict[str, str]:
    if executable in {"python", sys.executable}:
        return {k: str(v) for k, v in sysconfig.get_config_vars().items() if v is not None}
    script = (
        "import json, sysconfig; "
        "print(json.dumps({k: str(v) for k, v in sysconfig.get_config_vars().items() if v is not None}))"
    )
    try:
        result = subprocess.run(
            [executable, "-c", script],
            capture_output=True,
            text=True,
            check=True,
            timeout=15,
        )
        parsed = json.loads(result.stdout)
        return {str(k): str(v) for k, v in parsed.items()} if isinstance(parsed, dict) else {}
    except (subprocess.SubprocessError, FileNotFoundError, json.JSONDecodeError, ValueError):
        return {}


def _query_version(executable: str) -> str:
    if executable in {"python", sys.executable}:
        return platform.python_version()
    try:
        result = subprocess.run(
            [executable, "-c", "import platform; print(platform.python_version())"],
            capture_output=True,
            text=True,
            check=True,
            timeout=15,
        )
        return result.stdout.strip()
    except (subprocess.SubprocessError, FileNotFoundError):
        return "unknown"


def _detect_pgo(config_vars: dict[str, str]) -> bool:
    config_args = config_vars.get("CONFIG_ARGS", "")
    if "--enable-optimizations" in config_args:
        return True
    if config_vars.get("PGO_PROF_USE_FLAG", "").strip():
        return True
    cflags = config_vars.get("CFLAGS_NODIST", "")
    return "-fprofile-use" in cflags or "-fprofile-instr-use" in cflags


def _detect_lto(config_vars: dict[str, str]) -> bool:
    if "--with-lto" in config_vars.get("CONFIG_ARGS", ""):
        return True
    if config_vars.get("LTOFLAGS", "").strip():
        return True
    return any("-flto" in config_vars.get(k, "") for k in ("LDFLAGS_NODIST", "CFLAGS_NODIST"))


def _detect_bolt(config_vars: dict[str, str]) -> bool:
    if "--enable-bolt" in config_vars.get("CONFIG_ARGS", ""):
        return True
    return bool(config_vars.get("BOLT_APPLY_FLAGS", "").strip())


def _detect_debug(config_vars: dict[str, str]) -> bool:
    if "--with-pydebug" in config_vars.get("CONFIG_ARGS", ""):
        return True
    return config_vars.get("Py_DEBUG", "0") not in {"0", ""}
