"""Helpers for detecting optional dependencies.

Follows SQLSpec's typing pattern for lazy evaluation and caching.
"""

from importlib.util import find_spec
from typing import TYPE_CHECKING

from vertebra.common.exceptions import MissingDependencyError

if TYPE_CHECKING:
    from types import ModuleType

# Module-level cache for dependency checks
_dependency_cache: dict[str, bool] = {}


def module_available(module_name: str) -> bool:
    """Check if a module is available for import (cached).

    Args:
        module_name: The dotted module path to check (e.g., "pyarrow").

    Returns:
        True if the module can be imported, False otherwise.
    """
    cached = _dependency_cache.get(module_name)
    if cached is not None:
        return cached
    try:
        is_available = find_spec(module_name) is not None
    except (ModuleNotFoundError, ValueError):
        is_available = False
    _dependency_cache[module_name] = is_available
    return is_available


def reset_dependency_cache() -> None:
    """Reset the dependency cache (primarily for testing)."""
    _dependency_cache.clear()


class OptionalDependencyFlag:
    """Lazy-evaluated flag for optional dependency availability.

    This class defers the actual import check until the flag is used
    in a boolean context. The result is cached per session.

    Example:
        PYARROW_INSTALLED = dependency_flag("pyarrow")

        if PYARROW_INSTALLED:
            import pyarrow as pa
    """

    __slots__ = ("module_name",)

    def __init__(self, module_name: str) -> None:
        self.module_name = module_name

    def __bool__(self) -> bool:
        return module_available(self.module_name)

    def __repr__(self) -> str:
        return f"OptionalDependencyFlag({self.module_name!r})"


def dependency_flag(module_name: str) -> OptionalDependencyFlag:
    """Create a lazy-evaluated availability flag for an optional module.

    Args:
        module_name: The dotted module path (e.g., "pyarrow", "numpy").

    Returns:
        An OptionalDependencyFlag that evaluates to True/False when used
        in boolean context.
    """
    return OptionalDependencyFlag(module_name)


def ensure_dependency(module_name: str, install_package: str | None = None, feature: str | None = None) -> "ModuleType":
    """Ensure an optional dependency is installed, raising a clear error if not.

    Args:
        module_name: The module to import (e.g., "pyarrow").
        install_package: The pip package name if different from module name.
        feature: A human-readable description of the feature requiring this dep.

    Returns:
        The imported module if available.

    Raises:
        MissingDependencyError: If the module is not installed.
    """
    if not module_available(module_name):
        install = install_package or module_name
        raise MissingDependencyError(
            package=module_name, install_package=f"vertebra[{install}]" if install != module_name else install
        )
    import importlib

    return importlib.import_module(module_name)


def ensure_pyarrow() -> "ModuleType":
    """Ensure pyarrow is installed, raising MissingDependencyError if not.

    Returns:
        The pyarrow module.

    Raises:
        MissingDependencyError: With instructions to install vertebra[arrow].
    """
    return ensure_dependency("pyarrow", install_package="arrow")


def ensure_numpy() -> "ModuleType":
    """Ensure numpy is installed, raising MissingDependencyError if not."""
    return ensure_dependency("numpy")


def ensure_pandas() -> "ModuleType":
    """Ensure pandas is installed, raising MissingDependencyError if not."""
    return ensure_dependency("pandas")


def ensure_polars() -> "ModuleType":
    """Ensure polars is installed, raising MissingDependencyError if not."""
    return ensure_dependency("polars")


__all__ = (
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
