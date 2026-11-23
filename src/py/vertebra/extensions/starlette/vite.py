"""Vite, AssetManifest, EarlyHintsSpec, and Inertia v3 SSR integration for Starlette and FastAPI."""

from __future__ import annotations

from typing import Any

from vertebra.extensions._web.asgi_starlette import BaseViteExtension
from vertebra.framework.inertia import InertiaConfig, InertiaHttpResponse
from vertebra.framework.ssr import render_page_shell
from vertebra.framework.vite import (
    AssetManifest,
    EarlyHintsSpec,
    ViteBridge,
    ViteConfig,
    clear_vite_bridges,
    get_active_vite_bridge,
    get_vertebra_client_path,
    get_vertebra_client_vite_aliases,
    register_vite_bridge,
    vite_asset,
    vite_hmr_client,
)

__all__ = (
    "AssetManifest",
    "EarlyHintsSpec",
    "InertiaConfig",
    "InertiaHttpResponse",
    "ViteBridge",
    "ViteConfig",
    "ViteExtension",
    "clear_vite_bridges",
    "get_active_vite_bridge",
    "get_vertebra_client_path",
    "get_vertebra_client_vite_aliases",
    "register_vite_bridge",
    "render_page_shell",
    "vite_asset",
    "vite_hmr_client",
)


class ViteExtension(BaseViteExtension):
    """Attaches ViteBridge, AssetManifest, EarlyHintsSpec, and InertiaConfig to a Starlette or FastAPI app."""

    __slots__ = ()

    def init_app(self, app: Any, *, bridge_name: str = "default") -> Any:
        """Register the bridge under ``bridge_name`` and attach Vite state to ``app.state``."""
        register_vite_bridge(self._bridge, name=bridge_name)
        return self._attach_state(app)
