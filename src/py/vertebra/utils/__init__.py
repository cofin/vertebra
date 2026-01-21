"""Vertebra utility modules.

This package provides shared utilities mirroring the sqlspec/utils/ pattern:
- module_loader: Lazy dependency detection and ensure functions
- dependencies: Application-specific dependency flags
- encoders: Shared encoding utilities for serializers
"""

from vertebra.utils.dependencies import FSSPEC_INSTALLED, LITESTAR_INSTALLED, SQLSPEC_INSTALLED, WATCHDOG_INSTALLED
from vertebra.utils.encoders import default_encoder
from vertebra.utils.module_loader import (
    OptionalDependencyFlag,
    dependency_flag,
    ensure_dependency,
    ensure_numpy,
    ensure_pandas,
    ensure_polars,
    ensure_pyarrow,
    module_available,
    reset_dependency_cache,
)

__all__ = (
    # Dependency flags
    "FSSPEC_INSTALLED",
    "LITESTAR_INSTALLED",
    "SQLSPEC_INSTALLED",
    "WATCHDOG_INSTALLED",
    # Module loader
    "OptionalDependencyFlag",
    # Encoders
    "default_encoder",
    "dependency_flag",
    "ensure_dependency",
    "ensure_numpy",
    "ensure_pandas",
    "ensure_polars",
    "ensure_pyarrow",
    "module_available",
    "reset_dependency_cache",
)
