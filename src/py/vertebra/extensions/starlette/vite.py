"""Vite, AssetManifest, EarlyHintsSpec, and Inertia v3 SSR integration for Starlette and FastAPI."""

from __future__ import annotations

from pathlib import Path
from typing import Any

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


class ViteExtension:
    """Attaches ViteBridge, AssetManifest, EarlyHintsSpec, and InertiaConfig to a Starlette or FastAPI app."""

    __slots__ = ("_bridge", "_inertia_config")

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
        if early_hints is not None:
            self._bridge._early_hints = early_hints
        self._inertia_config = inertia_config

    @property
    def bridge(self) -> ViteBridge:
        """Return the underlying neutral ViteBridge."""
        return self._bridge

    @property
    def manifest(self) -> AssetManifest | None:
        """Return the loaded AssetManifest when available."""
        return self._bridge.manifest

    @property
    def early_hints(self) -> EarlyHintsSpec:
        """Return the active EarlyHintsSpec."""
        return self._bridge.early_hints

    def hmr_client_tag(self, *, csp_nonce: str | None = None) -> str:
        """Return the Vite HMR client script tag via the underlying ViteBridge."""
        return self._bridge.hmr_client_tag(csp_nonce=csp_nonce)

    def asset_tags(
        self,
        entry: str,
        *,
        csp_nonce: str | None = None,
        preload_dynamic: bool | None = None,
    ) -> str:
        """Return script and link HTML tags for ``entry`` via the underlying ViteBridge."""
        return self._bridge.asset_tags(
            entry,
            csp_nonce=csp_nonce,
            preload_dynamic=preload_dynamic,
        )

    def init_app(self, app: Any, *, bridge_name: str = "default") -> Any:
        """Attach Vite manifest, bridge, and EarlyHintsSpec to ``app.state``."""
        register_vite_bridge(self._bridge, name=bridge_name)
        state = getattr(app, "state", None)
        if state is not None:
            if isinstance(state, dict):
                state["vite"] = self
                state["vite_bridge"] = self._bridge
                state["asset_manifest"] = self._bridge.manifest
                state["early_hints"] = self._bridge.early_hints
            else:
                state.vite = self
                state.vite_bridge = self._bridge
                state.asset_manifest = self._bridge.manifest
                state.early_hints = self._bridge.early_hints
        return app
