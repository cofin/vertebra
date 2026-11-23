"""Vite, AssetManifest, EarlyHintsSpec, and Inertia v3 SSR integration for FastAPI."""

from __future__ import annotations

from typing import Any

from vertebra.build._assets import AssetManifest, EarlyHintsSpec
from vertebra.extensions._web.asgi_starlette import BaseViteExtension
from vertebra.framework.inertia import InertiaConfig, InertiaHttpResponse
from vertebra.framework.ssr import render_page_shell
from vertebra.framework.vite import (
    ViteBridge,
    ViteConfig,
    get_vertebra_client_path,
    get_vertebra_client_vite_aliases,
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
    "get_vertebra_client_path",
    "get_vertebra_client_vite_aliases",
    "render_page_shell",
    "vite_asset",
    "vite_hmr_client",
)


class ViteExtension(BaseViteExtension):
    """Attaches ``AssetManifest``, ``EarlyHintsSpec``, and ``InertiaConfig`` to a FastAPI application."""

    __slots__ = ()

    def init_app(self, app: Any) -> Any:
        """Attach Vite manifest, bridge, and ``EarlyHintsSpec`` to ``app.state``."""
        return self._attach_state(app)
