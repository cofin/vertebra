"""Vite, AssetManifest, EarlyHintsSpec, and Inertia v3 SSR integration for FastAPI."""

from __future__ import annotations

from vertebra.extensions.starlette.vite import (
    AssetManifest,
    EarlyHintsSpec,
    InertiaConfig,
    InertiaHttpResponse,
    ViteExtension,
    get_vertebra_client_path,
    get_vertebra_client_vite_aliases,
    render_page_shell,
)

__all__ = (
    "AssetManifest",
    "EarlyHintsSpec",
    "InertiaConfig",
    "InertiaHttpResponse",
    "ViteExtension",
    "get_vertebra_client_path",
    "get_vertebra_client_vite_aliases",
    "render_page_shell",
)
