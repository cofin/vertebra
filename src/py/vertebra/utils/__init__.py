"""Backwards-compatible re-export from vertebra.common.utils.

This module is deprecated. Import from vertebra.common.utils instead.
"""

from vertebra.common.utils import (
    FSSPEC_INSTALLED,
    LITESTAR_INSTALLED,
    SQLSPEC_INSTALLED,
    WATCHDOG_INSTALLED,
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
    "FSSPEC_INSTALLED",
    "LITESTAR_INSTALLED",
    "SQLSPEC_INSTALLED",
    "WATCHDOG_INSTALLED",
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
