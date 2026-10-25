"""Profile-Guided Optimization (PGO) pipeline for Python and Rust artifacts."""

import logging
import os
import platform
import shutil
import subprocess
import sys
import sysconfig
import tempfile
from pathlib import Path
from typing import Literal

import msgspec

from vertebra.utils.serializers import decode_json

__all__ = (
    "CODEGEN_FLAGS",
    "CPYTHON_PGO_FLAGS",
    "PLATFORM_MATRIX",
    "PgoMode",
    "PlatformSupport",
    "PythonBuildInfo",
    "RustPgoConfig",
    "build_cargo_pgo_env",
    "build_flags",
    "build_instrumented",
    "build_instrumented_extension",
    "build_optimized",
    "build_optimized_extension",
    "check_pgo_availability",
    "check_profdata_size",
    "detect_python_build",
    "detect_python_pgo_lto",
    "find_llvm_profdata",
    "get_platform_support",
    "merge_profdata_command",
    "merge_profiles",
    "resolve_llvm_profdata",
    "validate_pgo_support",
    "validate_profdata",
    "weighted_merge_profiles",
)

logger = logging.getLogger("vertebra.pgo")

_REPO_ROOT = Path(__file__).resolve().parents[4]
_RS_WORKSPACE = _REPO_ROOT / "src" / "rs"
_DEFAULT_MANIFEST = _RS_WORKSPACE / "vertebra-http" / "Cargo.toml"
_DEFAULT_PROFILE_DIR = _REPO_ROOT / ".profiles" / "raw"

type PgoMode = Literal["generate", "use", "baseline"]

CODEGEN_FLAGS: dict[str, str] = {
    "target-cpu": "-Ctarget-cpu={value}",
    "pgo-warn-missing-function": "-Cllvm-args=-pgo-warn-missing-function",
}

CPYTHON_PGO_FLAGS: dict[str, str] = {
    "--enable-optimizations": "Enables CPython Profile-Guided Optimization (PGO) for 10-20% interpreter speedup.",
    "--with-lto": "Enables Link-Time Optimization (LTO) across translation units.",
    "--enable-bolt": "Enables LLVM BOLT post-link binary layout optimization.",
    "--with-tail-call-interp": "Enables CPython 3.14/3.15 tail-calling bytecode interpreter dispatch.",
    "--enable-experimental-jit": "Enables CPython 3.14/3.15 copy-and-patch JIT compiler support.",
}


class PythonBuildInfo(msgspec.Struct, frozen=True, gc=False, cache_hash=True):
    """Immutable snapshot of a CPython interpreter's optimization configuration."""

    version: str
    is_pgo: bool
    is_lto: bool
    is_debug: bool
    architecture: str
    is_tail_call_interp: bool = False
    is_jit_available: bool = False
    is_jit_enabled: bool = False
    is_free_threaded: bool = False
    has_frame_pointers: bool = False


class PlatformSupport(msgspec.Struct, frozen=True, gc=False, cache_hash=True):
    """CPython optimization availability record for an OS and CPU architecture pair."""

    os_name: str
    architecture: str
    cpython_pgo: bool
    cpython_lto: bool
    cpython_bolt: bool
    notes: str


class RustPgoConfig(msgspec.Struct, frozen=True, gc=False, cache_hash=True):
    """Immutable configuration for Rust PGO build stages."""

    crate_name: str = "vertebra-http"
    manifest_path: Path = msgspec.field(default_factory=lambda: _DEFAULT_MANIFEST)
    profile_dir: Path = msgspec.field(default_factory=lambda: _DEFAULT_PROFILE_DIR)
    llvm_profdata_bin: str = ""
    maturin_features: str = "pyo3/extension-module"

    def resolved_profdata_bin(self) -> str:
        """Return the llvm-profdata executable path, auto-detecting when unset."""
        if self.llvm_profdata_bin:
            return self.llvm_profdata_bin
        return find_llvm_profdata()

    def instrument_rustflags(self) -> str:
        """Return the RUSTFLAGS string for profile generation."""
        return " ".join(build_flags(self, pgo_mode="generate"))


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


def detect_python_pgo_lto(executable: str = "python") -> tuple[bool, bool]:
    """Detect if PGO and LTO are enabled in Python build.

    Returns:
        A tuple of (is_pgo, is_lto).
    """
    info = detect_python_build(executable)
    return info.is_pgo, info.is_lto


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


def validate_pgo_support() -> bool:
    """Return True if current platform supports CPython PGO."""
    try:
        return get_platform_support().cpython_pgo
    except RuntimeError:
        return False


def find_llvm_profdata() -> str:
    """Locate llvm-profdata on PATH or inside the active rustup llvm-tools component."""
    direct = shutil.which("llvm-profdata")
    if direct:
        return direct

    for version in range(21, 13, -1):
        candidate = shutil.which(f"llvm-profdata-{version}")
        if candidate:
            return candidate

    rustup_home = Path(os.environ.get("RUSTUP_HOME", str(Path.home() / ".rustup")))
    toolchains_dir = rustup_home / "toolchains"
    if toolchains_dir.exists():
        triple = _detect_target_triple()
        for entry in sorted(toolchains_dir.iterdir()):
            bin_path = entry / "lib" / "rustlib" / triple / "bin" / "llvm-profdata"
            if bin_path.exists():
                return str(bin_path)

    msg = "Could not find llvm-profdata; install via 'rustup component add llvm-tools'"
    raise FileNotFoundError(msg)


resolve_llvm_profdata = find_llvm_profdata


def build_flags(
    config: RustPgoConfig,
    *,
    pgo_mode: PgoMode,
    profdata_path: Path | None = None,
    target_cpu: str = "native",
) -> list[str]:
    """Assemble RUSTFLAGS list for generate, use, or baseline PGO modes."""
    flags: list[str] = []
    if pgo_mode == "generate":
        flags.append(f"-Cprofile-generate={config.profile_dir}")
    elif pgo_mode == "use":
        if profdata_path is None:
            msg = "profdata_path is required when pgo_mode='use'"
            raise ValueError(msg)
        flags.append(f"-Cprofile-use={profdata_path}")
        flags.append("-Cllvm-args=-pgo-warn-missing-function")

    if target_cpu:
        flags.append(f"-Ctarget-cpu={target_cpu}")
    return flags


def build_cargo_pgo_env(flags: list[str]) -> dict[str, str]:
    """Assemble environment variables with injected RUSTFLAGS."""
    env = os.environ.copy()
    existing = env.get("RUSTFLAGS", "")
    joined = " ".join(flags)
    env["RUSTFLAGS"] = f"{joined} {existing}".strip()
    return env


def merge_profdata_command(
    profile_dir: Path,
    output_path: Path,
    *,
    llvm_profdata: str | None = None,
) -> list[str]:
    """Assemble the command list to merge .profraw profiles."""
    profraw_files = sorted(profile_dir.glob("**/*.profraw")) if profile_dir.exists() else []
    if not profraw_files:
        msg = f"No .profraw files found in {profile_dir}"
        raise FileNotFoundError(msg)

    binary = llvm_profdata or find_llvm_profdata()
    return [binary, "merge", "-o", str(output_path), *[str(f) for f in profraw_files]]


def merge_profiles(
    profile_dir: Path,
    output_path: Path,
    *,
    llvm_profdata: str | None = None,
) -> Path:
    """Merge all .profraw files in profile_dir into a single .profdata file."""
    cmd = merge_profdata_command(profile_dir, output_path, llvm_profdata=llvm_profdata)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(cmd, check=True)
    return output_path


def validate_profdata(
    profdata_path: Path,
    *,
    llvm_profdata: str | None = None,
) -> bool:
    """Validate a merged .profdata file via llvm-profdata show."""
    if not profdata_path.exists():
        return False
    binary = llvm_profdata or find_llvm_profdata()
    try:
        subprocess.run([binary, "show", str(profdata_path)], check=True, capture_output=True, text=True)
        return True
    except subprocess.CalledProcessError:
        return False


def weighted_merge_profiles(
    old_profdata: Path,
    new_profraw_dir: Path,
    output: Path,
    *,
    old_weight: int = 1,
    new_weight: int = 2,
    llvm_profdata: str | None = None,
) -> Path:
    """Blend historical .profdata with fresh .profraw runs using weighted inputs."""
    binary = llvm_profdata or find_llvm_profdata()
    if not old_profdata.exists():
        return merge_profiles(new_profraw_dir, output, llvm_profdata=binary)

    with tempfile.TemporaryDirectory(prefix="vtb-pgo-") as tmpdir:
        new_profdata = Path(tmpdir) / "new.profdata"
        merge_profiles(new_profraw_dir, new_profdata, llvm_profdata=binary)
        output.parent.mkdir(parents=True, exist_ok=True)
        cmd = [
            binary,
            "merge",
            f"-weighted-input={old_weight},{old_profdata}",
            f"-weighted-input={new_weight},{new_profdata}",
            "-o",
            str(output),
        ]
        subprocess.run(cmd, check=True)
    return output


def check_profdata_size(profdata_path: Path) -> int:
    """Return the byte size of profdata_path, or 0 if it does not exist."""
    if not profdata_path.exists():
        return 0
    return profdata_path.stat().st_size


def build_instrumented(config: RustPgoConfig) -> Path:
    """Compile the Rust crate with -Cprofile-generate instrumentation enabled."""
    config.profile_dir.mkdir(parents=True, exist_ok=True)
    env = build_cargo_pgo_env(build_flags(config, pgo_mode="generate"))
    cmd = ["cargo", "build", "--release", "--manifest-path", str(config.manifest_path)]
    subprocess.run(cmd, check=True, env=env)
    return config.manifest_path.parent.parent / "target" / "release"


def build_instrumented_extension(config: RustPgoConfig) -> Path:
    """Compile the PyO3 extension with -Cprofile-generate via maturin develop."""
    config.profile_dir.mkdir(parents=True, exist_ok=True)
    env = build_cargo_pgo_env(build_flags(config, pgo_mode="generate"))
    cmd = ["maturin", "develop", "--uv", "--release", "--features", config.maturin_features]
    subprocess.run(cmd, check=True, env=env)
    return config.profile_dir


def build_optimized(config: RustPgoConfig, profdata_path: Path) -> Path:
    """Compile the Rust crate with -Cprofile-use optimization, falling back to standard release if invalid."""
    target_dir = config.manifest_path.parent.parent / "target" / "release"
    if not profdata_path.exists():
        subprocess.run(
            ["cargo", "build", "--release", "--manifest-path", str(config.manifest_path)],
            check=True,
        )
        return target_dir

    env = build_cargo_pgo_env(build_flags(config, pgo_mode="use", profdata_path=profdata_path))
    subprocess.run(
        ["cargo", "build", "--release", "--manifest-path", str(config.manifest_path)],
        check=True,
        env=env,
    )
    return target_dir


def build_optimized_extension(config: RustPgoConfig, profdata_path: Path) -> Path:
    """Compile the PyO3 extension with -Cprofile-use via maturin develop."""
    target_dir = config.manifest_path.parent.parent / "target" / "release"
    cmd = ["maturin", "develop", "--uv", "--release", "--features", config.maturin_features]
    if not profdata_path.exists():
        subprocess.run(cmd, check=True)
        return target_dir
    env = build_cargo_pgo_env(build_flags(config, pgo_mode="use", profdata_path=profdata_path))
    subprocess.run(cmd, check=True, env=env)
    return target_dir


def _detect_target_triple() -> str:
    try:
        result = subprocess.run(
            ["rustc", "-vV"],
            capture_output=True,
            text=True,
            check=True,
            timeout=10,
        )
        for line in result.stdout.splitlines():
            if line.startswith("host:"):
                return line.split(":", 1)[1].strip()
    except (subprocess.SubprocessError, FileNotFoundError):
        pass
    return "x86_64-unknown-linux-gnu"


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
