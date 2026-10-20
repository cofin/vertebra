"""Rust Profile-Guided Optimization (PGO) build pipeline for Vertebra crates."""

import logging
import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

__all__ = (
    "CODEGEN_FLAGS",
    "PgoMode",
    "RustPgoConfig",
    "build_flags",
    "build_instrumented",
    "build_instrumented_extension",
    "build_optimized",
    "build_optimized_extension",
    "check_profdata_size",
    "find_llvm_profdata",
    "merge_profiles",
    "validate_profdata",
    "weighted_merge_profiles",
)

logger = logging.getLogger("vertebra.pgo.rust")

_REPO_ROOT = Path(__file__).resolve().parents[2]
_RS_WORKSPACE = _REPO_ROOT / "src" / "rs"
_DEFAULT_MANIFEST = _RS_WORKSPACE / "vertebra-http" / "Cargo.toml"
_DEFAULT_PROFILE_DIR = _REPO_ROOT / "tools" / "pgo" / ".profiles" / "raw"

type PgoMode = Literal["generate", "use", "baseline"]

CODEGEN_FLAGS: dict[str, str] = {
    "target-cpu": "-Ctarget-cpu={value}",
    "pgo-warn-missing-function": "-Cllvm-args=-pgo-warn-missing-function",
}


@dataclass(frozen=True, slots=True)
class RustPgoConfig:
    """Immutable configuration for Rust PGO build stages."""

    crate_name: str = "vertebra-http"
    manifest_path: Path = field(default_factory=lambda: _DEFAULT_MANIFEST)
    profile_dir: Path = field(default_factory=lambda: _DEFAULT_PROFILE_DIR)
    llvm_profdata_bin: str = ""
    maturin_features: str = "pyo3/extension-module"

    def resolved_profdata_bin(self) -> str:
        """Return the llvm-profdata executable path, auto-detecting when unset."""
        if self.llvm_profdata_bin:
            return self.llvm_profdata_bin
        return find_llvm_profdata()


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


def build_instrumented(config: RustPgoConfig) -> Path:
    """Compile the Rust crate with -Cprofile-generate instrumentation enabled."""
    config.profile_dir.mkdir(parents=True, exist_ok=True)
    env = _cargo_env(build_flags(config, pgo_mode="generate"))
    cmd = ["cargo", "build", "--release", "--manifest-path", str(config.manifest_path)]
    subprocess.run(cmd, check=True, env=env)
    return config.manifest_path.parent.parent / "target" / "release"


def build_instrumented_extension(config: RustPgoConfig) -> Path:
    """Compile the PyO3 extension with -Cprofile-generate via maturin develop."""
    config.profile_dir.mkdir(parents=True, exist_ok=True)
    env = _cargo_env(build_flags(config, pgo_mode="generate"))
    cmd = ["maturin", "develop", "--uv", "--release", "--features", config.maturin_features]
    subprocess.run(cmd, check=True, env=env)
    return config.profile_dir


def merge_profiles(
    profile_dir: Path,
    output_path: Path,
    *,
    llvm_profdata: str | None = None,
) -> Path:
    """Merge all .profraw files in profile_dir into a single .profdata file."""
    profraw_files = sorted(profile_dir.glob("**/*.profraw")) if profile_dir.exists() else []
    if not profraw_files:
        msg = f"No .profraw files found in {profile_dir}"
        raise FileNotFoundError(msg)

    binary = llvm_profdata or find_llvm_profdata()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [binary, "merge", "-o", str(output_path), *[str(f) for f in profraw_files]]
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


def build_optimized(config: RustPgoConfig, profdata_path: Path) -> Path:
    """Compile the Rust crate with -Cprofile-use optimization, falling back to standard release if invalid."""
    target_dir = config.manifest_path.parent.parent / "target" / "release"
    if not profdata_path.exists():
        subprocess.run(
            ["cargo", "build", "--release", "--manifest-path", str(config.manifest_path)],
            check=True,
        )
        return target_dir

    env = _cargo_env(build_flags(config, pgo_mode="use", profdata_path=profdata_path))
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
    env = _cargo_env(build_flags(config, pgo_mode="use", profdata_path=profdata_path))
    subprocess.run(cmd, check=True, env=env)
    return target_dir


def _cargo_env(flags: list[str]) -> dict[str, str]:
    env = os.environ.copy()
    existing = env.get("RUSTFLAGS", "")
    joined = " ".join(flags)
    env["RUSTFLAGS"] = f"{joined} {existing}".strip()
    return env


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
