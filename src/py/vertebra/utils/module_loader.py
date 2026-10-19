"""Module loading utilities for Vertebra.

Provides functions for dynamic module imports, path resolution, and dependency
availability checking. Aligned with vtb.
"""

import importlib
from importlib.util import find_spec
from pathlib import Path
from typing import TYPE_CHECKING, Any

from vertebra._typing import (
    MissingDependencyError,
    dependency_flag,
    module_available,
    reset_dependency_cache,
)

if TYPE_CHECKING:
    from types import ModuleType

__all__ = (
    "MissingDependencyError",
    "dependency_flag",
    "ensure_advanced_alchemy",
    "ensure_msgspec",
    "ensure_pyarrow",
    "ensure_sqlalchemy",
    "ensure_sqlspec",
    "ensure_uvloop",
    "import_app",
    "import_optional",
    "import_string",
    "module_available",
    "module_to_os_path",
    "reset_dependency_cache",
)


def import_app(app_path: str) -> Any:
    """Import an application object from a ``module:attr`` target string."""
    if ":" not in app_path:
        msg = f"VERTEBRA_APP must be in 'module:attr' format, got: {app_path!r}"
        raise ValueError(msg)
    module_path, attr = app_path.rsplit(":", 1)
    module = importlib.import_module(module_path)
    return getattr(module, attr)


def import_optional(module_name: str) -> Any | None:
    """Import and return a module when available, or None if not installed."""
    if not module_available(module_name):
        return None
    try:
        return importlib.import_module(module_name)
    except Exception:
        return None


def module_to_os_path(dotted_path: str = "app") -> Path:
    """Convert a module dotted path to a filesystem path."""
    try:
        if (src := find_spec(dotted_path)) is None:
            msg = f"Couldn't find the path for {dotted_path}"
            raise TypeError(msg)
    except (ModuleNotFoundError, ValueError) as e:
        msg = f"Couldn't find the path for {dotted_path}"
        raise TypeError(msg) from e

    path = Path(str(src.origin))
    return path.parent if path.is_file() else path


def import_string(dotted_path: str) -> Any:
    """Import a module or attribute from a dotted path string."""
    obj: Any = None
    try:
        parts = dotted_path.split(".")
        module = None
        i = len(parts)

        for i in range(len(parts), 0, -1):
            module_path = ".".join(parts[:i])
            try:
                module = importlib.import_module(module_path)
                break
            except ModuleNotFoundError:
                continue
        else:
            _raise_import_error(f"{dotted_path} doesn't look like a module path")

        if module is None:
            _raise_import_error(f"Failed to import any module from {dotted_path}")

        obj = module
        attrs = parts[i:]
        if not attrs and i == len(parts) and len(parts) > 1:
            parent_module_path = ".".join(parts[:-1])
            attr = parts[-1]
            try:
                parent_module = importlib.import_module(parent_module_path)
            except Exception:
                return obj
            if attr not in parent_module.__dict__:
                _raise_import_error(f"Module '{parent_module_path}' has no attribute '{attr}' in '{dotted_path}'")

        for attr in attrs:
            obj = _resolve_import_attr(obj, attr, module, dotted_path)
    except Exception as e:
        _raise_import_error(f"Could not import '{dotted_path}': {e}", e)
    return obj


def ensure_sqlspec() -> None:
    """Ensure sqlspec is available."""
    _require_dependency("sqlspec")


def ensure_sqlalchemy() -> None:
    """Ensure sqlalchemy is available."""
    _require_dependency("sqlalchemy")


def ensure_advanced_alchemy() -> None:
    """Ensure advanced_alchemy is available."""
    _require_dependency("advanced_alchemy", package_name="advanced-alchemy")


def ensure_uvloop() -> None:
    """Ensure uvloop is available."""
    _require_dependency("uvloop")


def ensure_pyarrow() -> None:
    """Ensure pyarrow is available."""
    _require_dependency("pyarrow")


def ensure_msgspec() -> None:
    """Ensure msgspec is available."""
    _require_dependency("msgspec")


def _require_dependency(
    module_name: str, *, package_name: str | None = None, install_package: str | None = None
) -> None:
    """Raise MissingDependencyError when an optional dependency is absent."""
    if module_available(module_name):
        return

    package = package_name or module_name
    install = install_package or package
    raise MissingDependencyError(package=package, install_package=install)


def _raise_import_error(msg: str, exc: Exception | None = None) -> None:
    """Raise an ImportError with optional exception chaining."""
    if exc is not None:
        raise ImportError(msg) from exc
    raise ImportError(msg)


def _resolve_import_attr(obj: Any, attr: str, module: "ModuleType | None", dotted_path: str) -> Any:
    """Resolve a dotted attribute path segment on a module or object."""
    try:
        return obj.__getattribute__(attr)
    except AttributeError as exc:
        module_name = module.__name__ if module is not None else "unknown"
        _raise_import_error(f"Module '{module_name}' has no attribute '{attr}' in '{dotted_path}'", exc)
        raise
