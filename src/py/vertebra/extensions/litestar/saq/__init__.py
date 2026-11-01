"""Vertebra SAQ extension for litestar-saq."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
    from vertebra.extensions.litestar.saq.plugin import create_saq_plugin

__all__ = ("create_saq_plugin",)

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "create_saq_plugin": ("vertebra.extensions.litestar.saq.plugin", "create_saq_plugin"),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
