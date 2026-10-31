"""Vertebra Vite extension for litestar-vite."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
    from vertebra.extensions.litestar.vite.plugin import (
        create_vite_plugin,
        get_vertebra_client_path,
        get_vertebra_client_vite_aliases,
    )

__all__ = (
    "create_vite_plugin",
    "get_vertebra_client_path",
    "get_vertebra_client_vite_aliases",
)

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "create_vite_plugin": ("vertebra.extensions.litestar.vite.plugin", "create_vite_plugin"),
    "get_vertebra_client_path": ("vertebra.extensions.litestar.vite.plugin", "get_vertebra_client_path"),
    "get_vertebra_client_vite_aliases": (
        "vertebra.extensions.litestar.vite.plugin",
        "get_vertebra_client_vite_aliases",
    ),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
