"""Vertebra utility modules.

Dependency Detection:
    module_available - Check if a module is available
    dependency_flag - Create lazy-evaluated availability flag
    ensure_dependency - Ensure dependency is installed or raise error
    ensure_pyarrow, ensure_numpy, etc. - Specific dependency checks

Dependency Flags:
    OptionalDependencyFlag - Lazy-evaluated flag class
    FSSPEC_INSTALLED, LITESTAR_INSTALLED, etc. - Pre-defined flags

For serialization utilities, see vertebra.serializers.
"""

from vertebra.utils.dependencies import FSSPEC_INSTALLED, LITESTAR_INSTALLED, SQLSPEC_INSTALLED, WATCHDOG_INSTALLED
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
    "dependency_flag",
    "ensure_dependency",
    "ensure_numpy",
    "ensure_pandas",
    "ensure_polars",
    "ensure_pyarrow",
    "module_available",
    "reset_dependency_cache",
)
