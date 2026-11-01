"""CLI entrypoint and canonical re-exports for the two-phase Rust PGO build pipeline."""

import argparse
import shlex
import sys
from collections.abc import Sequence
from pathlib import Path

from vertebra.build.packaging.pgo import (
    CODEGEN_FLAGS,
    EXTENSION_MANIFESTS,
    PGOMode,
    PGOPipelineResult,
    RustPGOConfig,
    build_cargo_pgo_env,
    build_flags,
    build_instrumented,
    build_instrumented_extension,
    build_instrumented_extensions,
    build_optimized,
    build_optimized_extension,
    build_optimized_extensions,
    check_profdata_size,
    find_llvm_profdata,
    merge_profdata_command,
    merge_profiles,
    resolve_cargo_release_dir,
    run_default_pgo_workload,
    run_pgo_pipeline,
    validate_profdata,
    weighted_merge_profiles,
)

__all__ = (
    "CODEGEN_FLAGS",
    "EXTENSION_MANIFESTS",
    "PGOMode",
    "PGOPipelineResult",
    "RustPGOConfig",
    "build_cargo_pgo_env",
    "build_flags",
    "build_instrumented",
    "build_instrumented_extension",
    "build_instrumented_extensions",
    "build_optimized",
    "build_optimized_extension",
    "build_optimized_extensions",
    "check_profdata_size",
    "find_llvm_profdata",
    "main",
    "merge_profdata_command",
    "merge_profiles",
    "resolve_cargo_release_dir",
    "run_default_pgo_workload",
    "run_pgo_pipeline",
    "validate_profdata",
    "weighted_merge_profiles",
)


def main(argv: Sequence[str] | None = None) -> int:
    """Parse CLI arguments and run the Rust PGO build pipeline."""
    parser = argparse.ArgumentParser(
        prog="python -m tools.pgo.rust_pgo",
        description="Run the two-phase Rust PGO pipeline across vertebra-server and PyO3 extensions.",
    )
    parser.add_argument(
        "--profile-dir",
        type=Path,
        default=None,
        help="Directory for raw .profraw counters (defaults to .profiles/raw).",
    )
    parser.add_argument(
        "--profdata",
        type=Path,
        default=None,
        help="Path for merged .profdata output or existing profile input (defaults to .profiles/merged.profdata).",
    )
    parser.add_argument(
        "--baseline-profdata",
        type=Path,
        default=None,
        help="Optional historical .profdata file for weighted profile merging.",
    )
    parser.add_argument(
        "--workload-cmd",
        type=str,
        default=None,
        help="Optional shell command to generate training traffic against the instrumented build.",
    )
    parser.add_argument(
        "--no-extensions",
        action="store_true",
        help="Skip building the PyO3 extension crates (_vertebra, _bundler, _storage).",
    )
    parser.add_argument(
        "--use-existing",
        action="store_true",
        help="Skip instrumentation/training and compile directly with --profdata (falls back if invalid).",
    )
    parser.add_argument(
        "--llvm-profdata",
        type=str,
        default="",
        help="Explicit path or binary name for llvm-profdata.",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    default_cfg = RustPGOConfig()
    cfg = RustPGOConfig(
        profile_dir=args.profile_dir if args.profile_dir is not None else default_cfg.profile_dir,
        llvm_profdata_bin=args.llvm_profdata,
    )
    workload_argv = shlex.split(args.workload_cmd) if args.workload_cmd else None
    result = run_pgo_pipeline(
        cfg,
        profdata_output=args.profdata,
        baseline_profdata=args.baseline_profdata,
        workload_cmd=workload_argv,
        build_extensions=not args.no_extensions,
        use_existing_profdata=args.use_existing,
    )
    mode_label = "pgo-use" if result.used_pgo else "release-fallback"
    sys.stdout.write(
        f"Rust PGO complete ({mode_label}): release_dir={result.release_dir} "
        f"profdata={result.profdata_path} ({result.profdata_bytes} bytes)\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
