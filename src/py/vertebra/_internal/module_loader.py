"""Helpers for detecting optional dependencies."""

from functools import lru_cache
from importlib.util import find_spec


@lru_cache
def module_available(module_path: str) -> bool:
    """Return True when the module can be imported."""
    return find_spec(module_path) is not None


def dependency_flag(module_path: str) -> bool:
    """Return a cached availability flag for an optional module."""
    return module_available(module_path)


__all__ = ("dependency_flag", "module_available")
