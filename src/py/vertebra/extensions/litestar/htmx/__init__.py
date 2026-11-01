"""Vertebra HTMX extension for litestar-htmx."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
    from vertebra.extensions.litestar.htmx.plugin import create_htmx_plugin

__all__ = ("create_htmx_plugin",)

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "create_htmx_plugin": ("vertebra.extensions.litestar.htmx.plugin", "create_htmx_plugin"),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
