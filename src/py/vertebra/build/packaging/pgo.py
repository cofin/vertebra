"""Profile-Guided Optimization (PGO) pipeline for Python and Rust artifacts."""

import http.client
import logging
import os
import platform
import shutil
import socket
import subprocess
import sys
import sysconfig
import tempfile
import time
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Literal

import msgspec

from vertebra.utils.serializers import decode_json

__all__ = (
    "CODEGEN_FLAGS",
    "CPYTHON_PGO_FLAGS",
    "EXTENSION_MANIFESTS",
    "PLATFORM_MATRIX",
    "PGOMode",
    "PGOPipelineResult",
    "PlatformSupport",
    "PythonBuildInfo",
    "RustPGOConfig",
    "build_cargo_pgo_env",
    "build_flags",
    "build_instrumented",
    "build_instrumented_extension",
    "build_instrumented_extensions",
    "build_optimized",
    "build_optimized_extension",
    "build_optimized_extensions",
    "check_pgo_availability",
    "check_profdata_size",
    "detect_python_build",
    "detect_python_pgo_lto",
    "find_llvm_profdata",
    "get_platform_support",
    "merge_profdata_command",
    "merge_profiles",
    "resolve_cargo_release_dir",
    "run_default_pgo_workload",
    "run_pgo_pipeline",
    "validate_pgo_support",
    "validate_profdata",
    "weighted_merge_profiles",
)

logger = logging.getLogger("vertebra.pgo")

_REPO_ROOT = Path(__file__).resolve().parents[5]
_RS_WORKSPACE = _REPO_ROOT / "src" / "rs"
_DEFAULT_MANIFEST = _RS_WORKSPACE / "vertebra-http" / "Cargo.toml"
_PY_EXT_MANIFEST = _RS_WORKSPACE / "vertebra-py" / "Cargo.toml"
_BUNDLER_EXT_MANIFEST = _RS_WORKSPACE / "vertebra-bundler" / "Cargo.toml"
_STORAGE_EXT_MANIFEST = _RS_WORKSPACE / "vertebra-storage" / "Cargo.toml"
EXTENSION_MANIFESTS: tuple[Path, ...] = (
    _PY_EXT_MANIFEST,
    _BUNDLER_EXT_MANIFEST,
    _STORAGE_EXT_MANIFEST,
)
_DEFAULT_PROFILE_DIR = _REPO_ROOT / ".profiles" / "raw"
_DEFAULT_PROFDATA_PATH = _REPO_ROOT / ".profiles" / "merged.profdata"

type PGOMode = Literal["generate", "use", "baseline"]

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


class RustPGOConfig(msgspec.Struct, frozen=True, gc=False, cache_hash=True):
    """Immutable configuration for Rust PGO build stages."""

    crate_name: str = "vertebra-http"
    manifest_path: Path = msgspec.field(default_factory=lambda: _DEFAULT_MANIFEST)
    profile_dir: Path = msgspec.field(default_factory=lambda: _DEFAULT_PROFILE_DIR)
    llvm_profdata_bin: str = ""
    maturin_features: str = "extension-module"
    extension_manifests: tuple[Path, ...] = EXTENSION_MANIFESTS

    def resolved_profdata_bin(self) -> str:
        """Return the llvm-profdata executable path, auto-detecting when unset."""
        if self.llvm_profdata_bin:
            return self.llvm_profdata_bin
        return find_llvm_profdata()

    def instrument_rustflags(self) -> str:
        """Return the RUSTFLAGS string for profile generation."""
        return " ".join(build_flags(self, pgo_mode="generate"))

    def use_rustflags(self, profdata_path: Path) -> str:
        """Return the RUSTFLAGS string for profile-guided optimization use."""
        return " ".join(build_flags(self, pgo_mode="use", profdata_path=profdata_path))


class PGOPipelineResult(msgspec.Struct, frozen=True, gc=False, cache_hash=True):
    """Summary of an executed Rust PGO build pipeline."""

    release_dir: Path
    profdata_path: Path
    profdata_valid: bool
    profdata_bytes: int
    used_pgo: bool
    extension_manifests: tuple[Path, ...] = EXTENSION_MANIFESTS


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


def resolve_cargo_release_dir(
    config: RustPGOConfig,
    *,
    env: Mapping[str, str] | None = None,
) -> Path:
    """Resolve the Cargo release output directory honoring CARGO_TARGET_DIR."""
    active_env = os.environ if env is None else env
    raw_target = active_env.get("CARGO_TARGET_DIR", "").strip()
    if raw_target:
        target_path = Path(raw_target)
        if not target_path.is_absolute():
            target_path = _REPO_ROOT / target_path
        return target_path / "release"
    return config.manifest_path.parent.parent / "target" / "release"


def build_flags(
    config: RustPGOConfig,
    *,
    pgo_mode: PGOMode,
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
    if not profdata_path.is_file() or profdata_path.stat().st_size == 0:
        return False
    try:
        binary = llvm_profdata or find_llvm_profdata()
    except FileNotFoundError:
        return False
    try:
        subprocess.run([binary, "show", str(profdata_path)], check=True, capture_output=True, text=True)
        return True
    except (subprocess.SubprocessError, OSError):
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


def build_instrumented(config: RustPGOConfig) -> Path:
    """Compile the Rust server crate with -Cprofile-generate instrumentation enabled."""
    config.profile_dir.mkdir(parents=True, exist_ok=True)
    env = build_cargo_pgo_env(build_flags(config, pgo_mode="generate"))
    cmd = ["cargo", "build", "--release", "--manifest-path", str(config.manifest_path)]
    subprocess.run(cmd, check=True, env=env)
    return resolve_cargo_release_dir(config, env=env)


def build_instrumented_extension(
    config: RustPGOConfig,
    manifest_path: Path | None = None,
) -> Path:
    """Compile a PyO3 extension crate with -Cprofile-generate via maturin develop."""
    config.profile_dir.mkdir(parents=True, exist_ok=True)
    env = build_cargo_pgo_env(build_flags(config, pgo_mode="generate"))
    cmd = _maturin_develop_command(config, manifest_path)
    subprocess.run(cmd, check=True, env=env, cwd=_REPO_ROOT)
    return config.profile_dir


def build_instrumented_extensions(config: RustPGOConfig) -> Path:
    """Compile all configured PyO3 extension crates with -Cprofile-generate."""
    for manifest in config.extension_manifests:
        build_instrumented_extension(config, manifest_path=manifest)
    return config.profile_dir


def build_optimized(config: RustPGOConfig, profdata_path: Path) -> Path:
    """Compile the Rust server crate with -Cprofile-use, falling back cleanly if profdata is invalid."""
    valid = validate_profdata(profdata_path, llvm_profdata=config.llvm_profdata_bin or None)
    return _build_optimized_cargo(config, profdata_path, valid=valid)


def build_optimized_extension(
    config: RustPGOConfig,
    profdata_path: Path,
    manifest_path: Path | None = None,
) -> Path:
    """Compile a PyO3 extension crate with -Cprofile-use, falling back cleanly if profdata is invalid."""
    valid = validate_profdata(profdata_path, llvm_profdata=config.llvm_profdata_bin or None)
    return _build_optimized_maturin(config, profdata_path, manifest_path=manifest_path, valid=valid)


def build_optimized_extensions(config: RustPGOConfig, profdata_path: Path) -> Path:
    """Compile all configured PyO3 extension crates with -Cprofile-use, validating profdata once."""
    valid = validate_profdata(profdata_path, llvm_profdata=config.llvm_profdata_bin or None)
    for manifest in config.extension_manifests:
        _build_optimized_maturin(config, profdata_path, manifest_path=manifest, valid=valid)
    return resolve_cargo_release_dir(config)


def run_default_pgo_workload(
    config: RustPGOConfig,
    release_dir: Path,
    *,
    requests_per_route: int = 250,
) -> None:
    """Execute a representative loopback HTTP and PyO3 workload to flush LLVM .profraw counters."""
    config.profile_dir.mkdir(parents=True, exist_ok=True)
    ext_env = os.environ.copy()
    ext_env["LLVM_PROFILE_FILE"] = str(config.profile_dir / "vtb-ext-%p-%m.profraw")
    ext_script = (
        "import vertebra._vertebra as v, vertebra._bundler, vertebra._storage; "
        "hdrs = v.Headers.from_pairs([('content-type', 'application/json'), ('x-pgo', '1')]); "
        "req = v.Request.from_parts('POST', '/echo', b'pgo', headers=[('content-type', 'application/json')]); "
        "resp = v.Response(200, b'ok'); "
        "_ = hdrs.get('content-type'); _ = list(hdrs); _ = bytes(req.body_view); _ = resp.to_bytes()"
    )
    subprocess.run([sys.executable, "-c", ext_script], check=True, env=ext_env, cwd=_REPO_ROOT)

    binary_name = "vertebra-server.exe" if os.name == "nt" else "vertebra-server"
    server_bin = release_dir / binary_name
    if not server_bin.is_file():
        return

    with tempfile.TemporaryDirectory(prefix="vtb-pgo-train-") as tmpdir:
        app_path = Path(tmpdir) / "pgo_train_app.py"
        app_path.write_text(
            '"""Temporary PGO training application."""\n'
            "from vertebra import App\n"
            "from vertebra._vertebra import Request, Response\n\n"
            "app = App()\n\n"
            '@app.get("/")\n'
            "def index(request: Request) -> Response:\n"
            '    return Response(status=200, body=b"ok")\n\n'
            '@app.get("/json")\n'
            "def json_route(request: Request) -> dict[str, str]:\n"
            '    return {"status": "ok", "runtime": "vertebra"}\n\n'
            '@app.post("/echo")\n'
            "def echo_route(request: Request) -> Response:\n"
            "    return Response(status=200, body=bytes(request.body_view))\n",
            encoding="utf-8",
        )
        port = _reserve_loopback_port()
        srv_env = os.environ.copy()
        srv_env["LLVM_PROFILE_FILE"] = str(config.profile_dir / "vtb-server-%p-%m.profraw")
        srv_env["VERTEBRA_APP"] = "pgo_train_app:app"
        srv_env["VERTEBRA_HOST"] = "127.0.0.1"
        srv_env["VERTEBRA_PORT"] = str(port)
        srv_env["VERTEBRA_WORKERS"] = "1"
        existing_py_path = srv_env.get("PYTHONPATH", "")
        py_parts = [tmpdir, str(_REPO_ROOT / "src" / "py")]
        if existing_py_path:
            py_parts.append(existing_py_path)
        srv_env["PYTHONPATH"] = os.pathsep.join(py_parts)

        stderr_log = Path(tmpdir) / "vertebra-server.stderr.log"
        with stderr_log.open("wb") as stderr_handle:
            proc = subprocess.Popen(
                [str(server_bin)],
                env=srv_env,
                cwd=_REPO_ROOT,
                stdout=subprocess.DEVNULL,
                stderr=stderr_handle,
            )
            try:
                _wait_for_http_ready("127.0.0.1", port, timeout_seconds=15.0, proc=proc, stderr_log=stderr_log)
                _drive_http_training_requests("127.0.0.1", port, requests_per_route=requests_per_route)
            finally:
                if proc.poll() is None:
                    proc.terminate()
                    try:
                        proc.wait(timeout=10.0)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait(timeout=5.0)


def run_pgo_pipeline(
    config: RustPGOConfig | None = None,
    *,
    profdata_output: Path | None = None,
    baseline_profdata: Path | None = None,
    workload_cmd: Sequence[str] | None = None,
    workload_runner: Callable[[RustPGOConfig, Path], None] | None = None,
    build_extensions: bool = True,
    clean_raw_before: bool = True,
    use_existing_profdata: bool = False,
) -> PGOPipelineResult:
    """Execute the two-phase Rust PGO pipeline across vertebra-server and PyO3 extensions."""
    cfg = config or RustPGOConfig()
    out_profdata = profdata_output or _DEFAULT_PROFDATA_PATH
    llvm_bin = cfg.llvm_profdata_bin or None
    manifests = cfg.extension_manifests if build_extensions else ()

    if use_existing_profdata:
        existing_valid = validate_profdata(out_profdata, llvm_profdata=llvm_bin)
        release_dir = _build_optimized_cargo(cfg, out_profdata, valid=existing_valid)
        if build_extensions:
            for manifest in cfg.extension_manifests:
                _build_optimized_maturin(cfg, out_profdata, manifest_path=manifest, valid=existing_valid)
        return PGOPipelineResult(
            release_dir=release_dir,
            profdata_path=out_profdata,
            profdata_valid=existing_valid,
            profdata_bytes=check_profdata_size(out_profdata) if existing_valid else 0,
            used_pgo=existing_valid,
            extension_manifests=manifests,
        )

    if clean_raw_before and cfg.profile_dir.exists():
        for stale in cfg.profile_dir.glob("**/*.profraw"):
            stale.unlink(missing_ok=True)
    cfg.profile_dir.mkdir(parents=True, exist_ok=True)

    instrumented_dir = build_instrumented(cfg)
    if build_extensions:
        build_instrumented_extensions(cfg)

    profile_pattern = str(cfg.profile_dir / "vtb-%p-%m.profraw")
    if workload_runner is not None:
        previous_profile_env = os.environ.get("LLVM_PROFILE_FILE")
        os.environ["LLVM_PROFILE_FILE"] = profile_pattern
        try:
            workload_runner(cfg, instrumented_dir)
        finally:
            if previous_profile_env is None:
                os.environ.pop("LLVM_PROFILE_FILE", None)
            else:
                os.environ["LLVM_PROFILE_FILE"] = previous_profile_env
    elif workload_cmd is not None:
        workload_env = os.environ.copy()
        workload_env["LLVM_PROFILE_FILE"] = profile_pattern
        subprocess.run(list(workload_cmd), check=True, env=workload_env, cwd=_REPO_ROOT)
    else:
        run_default_pgo_workload(cfg, instrumented_dir)

    if baseline_profdata is not None and baseline_profdata.exists():
        weighted_merge_profiles(baseline_profdata, cfg.profile_dir, out_profdata, llvm_profdata=llvm_bin)
    else:
        merge_profiles(cfg.profile_dir, out_profdata, llvm_profdata=llvm_bin)

    valid = validate_profdata(out_profdata, llvm_profdata=llvm_bin)
    release_dir = _build_optimized_cargo(cfg, out_profdata, valid=valid)
    if build_extensions:
        for manifest in cfg.extension_manifests:
            _build_optimized_maturin(cfg, out_profdata, manifest_path=manifest, valid=valid)

    return PGOPipelineResult(
        release_dir=release_dir,
        profdata_path=out_profdata,
        profdata_valid=valid,
        profdata_bytes=check_profdata_size(out_profdata),
        used_pgo=valid,
        extension_manifests=manifests,
    )


def _build_optimized_cargo(config: RustPGOConfig, profdata_path: Path, *, valid: bool) -> Path:
    """Compile the Rust server crate using pre-validated profdata status."""
    cmd = ["cargo", "build", "--release", "--manifest-path", str(config.manifest_path)]
    if not valid:
        logger.warning("PGO profdata %s is missing or invalid; falling back to standard release build.", profdata_path)
        subprocess.run(cmd, check=True)
        return resolve_cargo_release_dir(config)
    env = build_cargo_pgo_env(build_flags(config, pgo_mode="use", profdata_path=profdata_path))
    subprocess.run(cmd, check=True, env=env)
    return resolve_cargo_release_dir(config, env=env)


def _build_optimized_maturin(
    config: RustPGOConfig,
    profdata_path: Path,
    *,
    manifest_path: Path | None,
    valid: bool,
) -> Path:
    """Compile a PyO3 extension crate using pre-validated profdata status."""
    cmd = _maturin_develop_command(config, manifest_path)
    if not valid:
        logger.warning(
            "PGO profdata %s is missing or invalid; building extension without -Cprofile-use.",
            profdata_path,
        )
        subprocess.run(cmd, check=True, cwd=_REPO_ROOT)
        return resolve_cargo_release_dir(config)
    env = build_cargo_pgo_env(build_flags(config, pgo_mode="use", profdata_path=profdata_path))
    subprocess.run(cmd, check=True, env=env, cwd=_REPO_ROOT)
    return resolve_cargo_release_dir(config, env=env)


def _maturin_develop_command(config: RustPGOConfig, manifest_path: Path | None) -> list[str]:
    """Build the maturin develop argv, omitting -m for root-configured vertebra-py."""
    cmd = ["maturin", "develop", "--uv", "--release"]
    if config.maturin_features:
        cmd.extend(["--features", config.maturin_features])
    if manifest_path is not None and manifest_path.resolve() != _PY_EXT_MANIFEST.resolve():
        cmd.extend(["-m", str(manifest_path)])
    return cmd


def _reserve_loopback_port() -> int:
    """Bind an ephemeral TCP port on 127.0.0.1 and return its port number."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        address = sock.getsockname()
        return int(address[1])


def _read_stderr_tail(stderr_log: Path | None) -> str:
    """Read captured server stderr output for diagnostic failure reporting."""
    if stderr_log is None or not stderr_log.is_file():
        return ""
    return stderr_log.read_text(encoding="utf-8", errors="replace").strip()


def _wait_for_http_ready(
    host: str,
    port: int,
    *,
    timeout_seconds: float,
    proc: subprocess.Popen[bytes] | None = None,
    stderr_log: Path | None = None,
) -> None:
    """Poll a loopback HTTP server until it responds, proc exits, or timeout_seconds elapses."""
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if proc is not None and (exit_code := proc.poll()) is not None:
            stderr_text = _read_stderr_tail(stderr_log)
            detail = f": {stderr_text}" if stderr_text else ""
            msg = f"Instrumented vertebra-server exited prematurely with code {exit_code}{detail}"
            raise RuntimeError(msg)
        conn = http.client.HTTPConnection(host, port, timeout=1.0)
        try:
            conn.request("GET", "/")
            resp = conn.getresponse()
            resp.read()
            if resp.status < 500:
                return
        except OSError:
            time.sleep(0.05)
        finally:
            conn.close()
    stderr_text = _read_stderr_tail(stderr_log)
    detail = f": {stderr_text}" if stderr_text else ""
    msg = f"Timed out waiting for instrumented vertebra-server on {host}:{port}{detail}"
    raise TimeoutError(msg)


def _drive_http_training_requests(host: str, port: int, *, requests_per_route: int) -> None:
    """Issue keep-alive GET and POST requests against the instrumented server."""
    payload = b'{"message":"vertebra-pgo-training"}' * 16
    conn = http.client.HTTPConnection(host, port, timeout=5.0)
    try:
        for _ in range(requests_per_route):
            conn.request("GET", "/")
            conn.getresponse().read()
            conn.request("GET", "/json")
            conn.getresponse().read()
            conn.request("POST", "/echo", body=payload, headers={"Content-Type": "application/json"})
            conn.getresponse().read()
    finally:
        conn.close()


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
