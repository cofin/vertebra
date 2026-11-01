"""Vertebra autowire extension for litestar-autowire."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
    from vertebra.extensions.litestar.autowire.integration import (
        AutowireBridgeIntegration,
        create_autowire_plugin,
        ensure_autowire_before_route_compiler,
    )

__all__ = (
    "AutowireBridgeIntegration",
    "create_autowire_plugin",
    "ensure_autowire_before_route_compiler",
)

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "AutowireBridgeIntegration": ("vertebra.extensions.litestar.autowire.integration", "AutowireBridgeIntegration"),
    "create_autowire_plugin": ("vertebra.extensions.litestar.autowire.integration", "create_autowire_plugin"),
    "ensure_autowire_before_route_compiler": (
        "vertebra.extensions.litestar.autowire.integration",
        "ensure_autowire_before_route_compiler",
    ),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
