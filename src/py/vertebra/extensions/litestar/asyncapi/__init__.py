"""Vertebra AsyncAPI extension for litestar-asyncapi."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
    from vertebra.extensions.litestar.asyncapi.plugin import create_asyncapi_plugin

__all__ = ("create_asyncapi_plugin",)

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "create_asyncapi_plugin": ("vertebra.extensions.litestar.asyncapi.plugin", "create_asyncapi_plugin"),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
