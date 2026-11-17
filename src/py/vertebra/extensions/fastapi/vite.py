"""Vite, AssetManifest, EarlyHintsSpec, and Inertia v3 SSR integration for FastAPI."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from vertebra.build._assets import AssetManifest, EarlyHintsSpec
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

if TYPE_CHECKING:
    from pathlib import Path

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


class ViteExtension:
    """Attaches ``AssetManifest``, ``EarlyHintsSpec``, and ``InertiaConfig`` to a FastAPI application."""

    __slots__ = ("_bridge", "_early_hints", "_inertia_config", "_manifest")

    def __init__(
        self,
        *,
        manifest_path: str | Path | None = None,
        manifest: AssetManifest | None = None,
        early_hints: EarlyHintsSpec | None = None,
        inertia_config: InertiaConfig | None = None,
        config: ViteConfig | None = None,
    ) -> None:
        resolved_config = config or ViteConfig(manifest_path=manifest_path)
        self._bridge = ViteBridge(resolved_config, manifest=manifest)
        self._manifest = self._bridge.manifest
        self._early_hints = early_hints or self._bridge.early_hints
        self._inertia_config = inertia_config

    @property
    def bridge(self) -> ViteBridge:
        """Return the underlying neutral ``ViteBridge``."""
        return self._bridge

    @property
    def manifest(self) -> AssetManifest | None:
        """Return the loaded ``AssetManifest`` when available."""
        return self._manifest

    @property
    def early_hints(self) -> EarlyHintsSpec:
        """Return the active ``EarlyHintsSpec``."""
        return self._early_hints

    def asset_tags(
        self,
        entry: str,
        *,
        csp_nonce: str | None = None,
        preload_dynamic: bool | None = None,
    ) -> str:
        """Render ``<script>`` and ``<link>`` HTML tags for ``entry``."""
        tags = self._bridge.asset_tags(entry, csp_nonce=csp_nonce, preload_dynamic=preload_dynamic)
        self._early_hints = self._bridge.early_hints
        return tags

    def hmr_client_tag(self, *, csp_nonce: str | None = None) -> str:
        """Render the Vite HMR client script tag when ``dev_mode`` is enabled."""
        return self._bridge.hmr_client_tag(csp_nonce=csp_nonce)

    def init_app(self, app: Any) -> Any:
        """Attach Vite manifest, bridge, and ``EarlyHintsSpec`` to ``app.state``."""
        state = getattr(app, "state", None)
        if state is not None:
            if isinstance(state, dict):
                state["vite"] = self
                state["vite_bridge"] = self._bridge
                state["asset_manifest"] = self._manifest
                state["early_hints"] = self._early_hints
            else:
                state.vite = self
                state.vite_bridge = self._bridge
                state.asset_manifest = self._manifest
                state.early_hints = self._early_hints
        return app
