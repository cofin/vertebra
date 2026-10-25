"""Standalone, onefile, and PGO packaging subpackage for Vertebra."""

from vertebra.build.packaging.onefile import (
    ONEFILE_MAGIC,
    ONEFILE_TRAILER_SIZE,
    OnefileBuilder,
)
from vertebra.build.packaging.pgo import (
    EXTENSION_MANIFESTS,
    PgoPipelineResult,
    PlatformSupport,
    PythonBuildInfo,
    RustPgoConfig,
    build_cargo_pgo_env,
    build_instrumented_extensions,
    build_optimized_extensions,
    detect_python_pgo_lto,
    find_llvm_profdata,
    merge_profdata_command,
    resolve_cargo_release_dir,
    run_default_pgo_workload,
    run_pgo_pipeline,
    validate_pgo_support,
)
from vertebra.build.packaging.standalone import StandaloneBuilder
from vertebra.build.packaging.targets import (
    KNOWN_TARGETS,
    PBS_BASE_URL,
    PBS_RELEASE_TAG,
    BuildTarget,
)

__all__ = (
    "EXTENSION_MANIFESTS",
    "KNOWN_TARGETS",
    "ONEFILE_MAGIC",
    "ONEFILE_TRAILER_SIZE",
    "PBS_BASE_URL",
    "PBS_RELEASE_TAG",
    "BuildTarget",
    "OnefileBuilder",
    "PgoPipelineResult",
    "PlatformSupport",
    "PythonBuildInfo",
    "RustPgoConfig",
    "StandaloneBuilder",
    "build_cargo_pgo_env",
    "build_instrumented_extensions",
    "build_optimized_extensions",
    "detect_python_pgo_lto",
    "find_llvm_profdata",
    "merge_profdata_command",
    "resolve_cargo_release_dir",
    "run_default_pgo_workload",
    "run_pgo_pipeline",
    "validate_pgo_support",
)
