"""Module loading, optional attribute resolution, and lazy export utilities for Vertebra."""

from __future__ import annotations

import importlib
import importlib.util
import sys
from pathlib import Path
from typing import Any

from vertebra.typing import (
    MissingDependencyError,
    _module_cache,
    dependency_flag,
    module_available,
    reset_dependency_cache,
)

__all__ = (
    "MissingDependencyError",
    "dependency_flag",
    "ensure_module",
    "ensure_numpy",
    "ensure_pandas",
    "ensure_pyarrow",
    "import_app",
    "import_optional",
    "import_optional_attr",
    "import_string",
    "lazy_exports",
    "module_available",
    "module_to_os_path",
    "reset_dependency_cache",
    "resolve_optional_attr",
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
    """Import and return a cached module when available, or None if not installed."""
    if module_name in sys.modules:
        loaded = sys.modules[module_name]
        if loaded is not None:
            _module_cache[module_name] = loaded
        return loaded

    cached = _module_cache.get(module_name)
    if cached is not None:
        return cached

    if not module_available(module_name):
        return None
    try:
        mod = importlib.import_module(module_name)
    except ImportError:
        return None
    _module_cache[module_name] = mod
    return mod


def import_optional_attr(module_name: str, attr_name: str, default: Any = None) -> Any:
    """Import ``module_name`` when available and return ``getattr(mod, attr_name, default)``."""
    mod = import_optional(module_name)
    if mod is None:
        return default
    return getattr(mod, attr_name, default)


def resolve_optional_attr(
    module_name: str,
    attr_name: str,
    *,
    fallback: Any = None,
) -> tuple[Any, bool]:
    """Resolve ``(attr, True)`` from an optional module when installed, or ``(fallback, False)``."""
    mod = import_optional(module_name)
    if mod is None:
        return fallback, False
    sentinel = object()
    resolved = getattr(mod, attr_name, sentinel)
    if resolved is sentinel:
        return fallback, False
    return resolved, True


def module_to_os_path(dotted_path: str = "vertebra") -> Path:
    """Resolve the filesystem directory or file path for an importable module path."""
    spec = importlib.util.find_spec(dotted_path)
    if spec is None:
        msg = f"Cannot locate module specification for {dotted_path!r}"
        raise ModuleNotFoundError(msg)
    if spec.submodule_search_locations:
        return Path(next(iter(spec.submodule_search_locations))).resolve()
    if spec.origin and spec.origin not in {"built-in", "frozen"}:
        return Path(spec.origin).resolve().parent
    msg = f"Module {dotted_path!r} does not have a filesystem path"
    raise TypeError(msg)


def ensure_module(
    module_name: str,
    *,
    package: str | None = None,
    extra: str | None = None,
    install_package: str | None = None,
) -> Any:
    """Import and return a cached module, or raise MissingDependencyError if unavailable."""
    mod = import_optional(module_name)
    if mod is None:
        raise MissingDependencyError(
            package or module_name,
            install_package=install_package,
            extra=extra,
        )
    return mod


def ensure_pyarrow() -> Any:
    """Ensure ``pyarrow`` is installed and return the imported module."""
    return ensure_module("pyarrow", package="pyarrow", install_package="pyarrow", extra="arrow")


def ensure_numpy() -> Any:
    """Ensure ``numpy`` is installed and return the imported module."""
    return ensure_module("numpy", package="numpy", install_package="numpy", extra="numpy")


def ensure_pandas() -> Any:
    """Ensure ``pandas`` is installed and return the imported module."""
    return ensure_module("pandas", package="pandas", install_package="pandas", extra="pandas")


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
            except ImportError:
                return obj
            if attr not in parent_module.__dict__:
                _raise_import_error(f"Module '{parent_module_path}' has no attribute '{attr}' in '{dotted_path}'")

        for attr in attrs:
            obj = _resolve_import_attr(obj, attr, module, dotted_path)
    except (ImportError, AttributeError) as e:
        _raise_import_error(f"Could not import '{dotted_path}': {e}", e)
    return obj


def lazy_exports(
    module_name: str,
    exports_or_map: tuple[str, ...] | dict[str, Any],
    mapping: dict[str, tuple[str, str]] | None = None,
) -> tuple[Any, Any]:
    """Create ``__getattr__`` and ``__dir__`` hooks for lazy module attribute exports.

    Args:
        module_name: Fully-qualified name of the module (e.g. ``__name__``).
        exports_or_map: Either a tuple/list of public export names, or a dict
            mapping attribute names to their module paths or ``(module, attr)`` tuples.
        mapping: Optional dictionary mapping attribute names to ``(submodule_path, attr_name)``
            tuples, used when ``exports_or_map`` is a tuple of names.

    Returns:
        A tuple of ``(getattr_fn, dir_fn)`` suitable for assigning to ``__getattr__`` and ``__dir__``.
    """
    if mapping is not None:
        exports = tuple(exports_or_map)
        export_map: dict[str, Any] = mapping
    elif isinstance(exports_or_map, dict):
        export_map = exports_or_map
        exports = tuple(export_map.keys())
    else:
        exports = tuple(exports_or_map)
        export_map = {}

    def getattr_fn(name: str) -> Any:
        if name in export_map:
            target = export_map[name]
            if isinstance(target, tuple):
                submodule_path, attr_name = target
                module = importlib.import_module(submodule_path)
                val = getattr(module, attr_name) if attr_name else module
            elif isinstance(target, str):
                module = importlib.import_module(target)
                val = getattr(module, name) if hasattr(module, name) else module
            else:
                val = target

            target_module = sys.modules.get(module_name)
            if target_module is not None:
                target_module.__dict__[name] = val
            return val

        msg = f"module {module_name!r} has no attribute {name!r}"
        raise AttributeError(msg)

    def dir_fn() -> list[str]:
        target_module = sys.modules.get(module_name)
        mod_keys = set(target_module.__dict__.keys()) if target_module is not None else set()
        return sorted(set(exports) | set(export_map.keys()) | mod_keys)

    return getattr_fn, dir_fn


def _raise_import_error(msg: str, exc: Exception | None = None) -> None:
    """Raise an ImportError with optional exception chaining."""
    if exc is not None:
        raise ImportError(msg) from exc
    raise ImportError(msg)


def _resolve_import_attr(obj: Any, attr: str, module: Any | None, dotted_path: str) -> Any:
    """Resolve a dotted attribute path segment on a module or object."""
    try:
        return obj.__getattribute__(attr)
    except AttributeError as exc:
        module_name = module.__name__ if module is not None else "unknown"
        _raise_import_error(f"Module '{module_name}' has no attribute '{attr}' in '{dotted_path}'", exc)
        raise
