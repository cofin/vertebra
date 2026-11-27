"""Public Python facade over the optional ``vertebra._bundler`` Rust extension."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from vertebra._bundler import (
        BundleConfig,
        BundleDevConfig,
        BundleOutput,
        Bundler,
        BundleResult,
        IsolatedDeclarationsGenerator,
        SourceWatcher,
        minify_css,
    )

__all__ = (
    "BundleConfig",
    "BundleDevConfig",
    "BundleOutput",
    "BundleResult",
    "Bundler",
    "IsolatedDeclarationsGenerator",
    "SourceWatcher",
    "minify_css",
)

_EXPORTS = frozenset(__all__)


def __getattr__(name: str) -> Any:
    """Lazy-load public bundler symbols from ``vertebra._bundler`` on first access."""
    if name in _EXPORTS:
        mod = ensure_module("vertebra._bundler", package="vertebra-core[bundler]", extra="bundler")
        value = getattr(mod, name)
        globals()[name] = value
        return value
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)


def __dir__() -> list[str]:
    """Return public bundler exports for ``dir()``."""
    return sorted(set(__all__) | set(globals().keys()))
