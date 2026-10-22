"""Standalone, onefile, and PGO packaging subpackage for Vertebra."""

from vertebra.build.packaging.onefile import (
    ONEFILE_MAGIC,
    ONEFILE_TRAILER_SIZE,
    OnefileBuilder,
)
from vertebra.build.packaging.pgo import (
    PlatformSupport,
    PythonBuildInfo,
    RustPgoConfig,
    build_cargo_pgo_env,
    detect_python_pgo_lto,
    merge_profdata_command,
    resolve_llvm_profdata,
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
    "KNOWN_TARGETS",
    "ONEFILE_MAGIC",
    "ONEFILE_TRAILER_SIZE",
    "PBS_BASE_URL",
    "PBS_RELEASE_TAG",
    "BuildTarget",
    "OnefileBuilder",
    "PlatformSupport",
    "PythonBuildInfo",
    "RustPgoConfig",
    "StandaloneBuilder",
    "build_cargo_pgo_env",
    "detect_python_pgo_lto",
    "merge_profdata_command",
    "resolve_llvm_profdata",
    "validate_pgo_support",
)
