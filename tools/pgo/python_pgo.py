"""CPython PGO, LTO, BOLT, tail-call interpreter, and JIT optimization detection."""

import os
import platform
import subprocess
import sys
import sysconfig

import msgspec

from vertebra.utils.serializers import decode_json

__all__ = (
    "CPYTHON_PGO_FLAGS",
    "PLATFORM_MATRIX",
    "PlatformSupport",
    "PythonBuildInfo",
    "check_pgo_availability",
    "detect_python_build",
    "get_platform_support",
)


class PythonBuildInfo(msgspec.Struct, frozen=True, gc=False, cache_hash=True):
    """Immutable snapshot of a CPython interpreter's optimization configuration."""

    version: str
    """CPython semantic version string."""
    is_pgo: bool
    """Whether CPython was compiled with Profile-Guided Optimization."""
    is_lto: bool
    """Whether CPython was compiled with Link-Time Optimization."""
    is_debug: bool
    """Whether CPython was compiled with debug assertions enabled."""
    architecture: str
    """Host CPU architecture reported by platform.machine()."""
    is_tail_call_interp: bool = False
    """Whether the tail-calling bytecode interpreter is enabled."""
    is_jit_available: bool = False
    """Whether the copy-and-patch JIT compiler was built into CPython."""
    is_jit_enabled: bool = False
    """Whether the copy-and-patch JIT compiler is active at runtime."""
    is_free_threaded: bool = False
    """Whether the GIL is disabled (`Py_GIL_DISABLED=1`)."""
    has_frame_pointers: bool = False
    """Whether frame pointers were preserved during compilation."""


class PlatformSupport(msgspec.Struct, frozen=True, gc=False, cache_hash=True):
    """CPython optimization availability record for an OS and CPU architecture pair."""

    os_name: str
    """Normalized operating system identifier (`linux`, `darwin`, `windows`)."""
    architecture: str
    """Normalized CPU architecture identifier (`x86_64`, `aarch64`, `arm64`, `x64`)."""
    cpython_pgo: bool
    """Whether CPython PGO builds are supported on this platform."""
    cpython_lto: bool
    """Whether CPython LTO builds are supported on this platform."""
    cpython_bolt: bool
    """Whether LLVM BOLT post-link optimization is supported on this platform."""
    notes: str
    """Human-readable summary of toolchain optimization support."""


CPYTHON_PGO_FLAGS: dict[str, str] = {
    "--enable-optimizations": "Enables CPython Profile-Guided Optimization (PGO) for 10-20% interpreter speedup.",
    "--with-lto": "Enables Link-Time Optimization (LTO) across translation units.",
    "--enable-bolt": "Enables LLVM BOLT post-link binary layout optimization.",
    "--with-tail-call-interp": "Enables CPython 3.14/3.15 tail-calling bytecode interpreter dispatch.",
    "--enable-experimental-jit": "Enables CPython 3.14/3.15 copy-and-patch JIT compiler support.",
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
    jit_available, jit_enabled = _detect_jit(executable, config_vars)
    return PythonBuildInfo(
        version=version,
        is_pgo=_detect_pgo(config_vars),
        is_lto=_detect_lto(config_vars),
        is_debug=_detect_debug(config_vars),
        architecture=platform.machine(),
        is_tail_call_interp=_detect_tail_call_interp(config_vars),
        is_jit_available=jit_available,
        is_jit_enabled=jit_enabled,
        is_free_threaded=_detect_free_threaded(config_vars),
        has_frame_pointers=_detect_frame_pointers(config_vars),
    )


def check_pgo_availability(executable: str = "python") -> dict[str, bool]:
    """Return a dictionary of active optimization flags across PGO, LTO, BOLT, JIT, and 3.15 capabilities."""
    config_vars = _query_sysconfig(executable)
    jit_available, jit_enabled = _detect_jit(executable, config_vars)
    return {
        "pgo": _detect_pgo(config_vars),
        "lto": _detect_lto(config_vars),
        "bolt": _detect_bolt(config_vars),
        "debug": _detect_debug(config_vars),
        "tail_call_interp": _detect_tail_call_interp(config_vars),
        "jit_available": jit_available,
        "jit_enabled": jit_enabled,
        "free_threaded": _detect_free_threaded(config_vars),
        "frame_pointers": _detect_frame_pointers(config_vars),
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
        parsed = decode_json(result.stdout.encode("utf-8"))
        return {str(k): str(v) for k, v in parsed.items()} if isinstance(parsed, dict) else {}
    except (subprocess.SubprocessError, FileNotFoundError, msgspec.DecodeError, ValueError):
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


def _detect_tail_call_interp(config_vars: dict[str, str]) -> bool:
    if "--with-tail-call-interp" in config_vars.get("CONFIG_ARGS", ""):
        return True
    return config_vars.get("Py_TAIL_CALL_INTERP", "0") not in {"0", ""}


def _detect_free_threaded(config_vars: dict[str, str]) -> bool:
    if "--disable-gil" in config_vars.get("CONFIG_ARGS", ""):
        return True
    return config_vars.get("Py_GIL_DISABLED", "0") not in {"0", ""}


def _detect_frame_pointers(config_vars: dict[str, str]) -> bool:
    flag_keys = ("CFLAGS", "CFLAGS_NODIST", "PY_CFLAGS", "PY_CFLAGS_NODIST")
    combined = " ".join(config_vars.get(k, "") for k in flag_keys)
    return "-fno-omit-frame-pointer" in combined or "-mno-omit-leaf-frame-pointer" in combined


def _detect_jit(executable: str, config_vars: dict[str, str]) -> tuple[bool, bool]:
    if executable in {"python", sys.executable}:
        jit_mod = getattr(sys, "_jit", None)
        if jit_mod is not None:
            avail_fn = getattr(jit_mod, "is_available", None)
            enabled_fn = getattr(jit_mod, "is_enabled", None)
            avail = bool(avail_fn()) if callable(avail_fn) else False
            enabled = bool(enabled_fn()) if callable(enabled_fn) else False
            return avail, enabled
        configured = "--enable-experimental-jit" in config_vars.get("CONFIG_ARGS", "")
        return configured, configured and os.environ.get("PYTHON_JIT", "") in {"1", "true", "yes"}

    script = (
        "import sys; j = getattr(sys, '_jit', None); "
        "a = bool(j.is_available()) if j and hasattr(j, 'is_available') else False; "
        "e = bool(j.is_enabled()) if j and hasattr(j, 'is_enabled') else False; "
        "print(f'{int(a)},{int(e)}')"
    )
    try:
        result = subprocess.run(
            [executable, "-c", script],
            capture_output=True,
            text=True,
            check=True,
            timeout=10,
        )
        parts = result.stdout.strip().split(",")
        if len(parts) == 2:
            return parts[0] == "1", parts[1] == "1"
    except (subprocess.SubprocessError, FileNotFoundError, ValueError):
        pass
    configured = "--enable-experimental-jit" in config_vars.get("CONFIG_ARGS", "")
    return configured, False
