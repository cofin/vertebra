"""Vite asset manifest and Jinja2 template context processor for Flask applications."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

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
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from flask import Flask

__all__ = (
    "AssetManifest",
    "EarlyHintsSpec",
    "ViteAssetBridge",
    "ViteBridge",
    "ViteConfig",
    "attach_vite",
    "clear_vite_bridges",
    "get_active_vite_bridge",
    "get_vertebra_client_path",
    "get_vertebra_client_vite_aliases",
    "register_vite_bridge",
    "vite_asset",
    "vite_hmr_client",
)


class ViteAssetBridge:
    """Resolves Vite manifest assets and HMR tags for Flask Jinja2 templates."""

    __slots__ = ("_bridge",)

    def __init__(
        self,
        config: ViteConfig | None = None,
        *,
        manifest: AssetManifest | None = None,
    ) -> None:
        self._bridge = ViteBridge(config or ViteConfig(), manifest=manifest)

    @property
    def bridge(self) -> ViteBridge:
        """Return the underlying neutral ``ViteBridge``."""
        return self._bridge

    @property
    def config(self) -> ViteConfig:
        """Return the ``ViteConfig``."""
        return self._bridge.config

    @property
    def manifest(self) -> AssetManifest | None:
        """Return the loaded ``AssetManifest`` when available."""
        return self._bridge.manifest

    @property
    def early_hints(self) -> EarlyHintsSpec:
        """Return the active ``EarlyHintsSpec``."""
        return self._bridge.early_hints

    def hmr_client_tag(self, *, csp_nonce: str | None = None) -> str:
        """Return the ``@vite/client`` script tag in dev mode or empty string in production."""
        return self._bridge.hmr_client_tag(csp_nonce=csp_nonce)

    def asset_tags(
        self,
        entry: str,
        *,
        csp_nonce: str | None = None,
        preload_dynamic: bool | None = None,
    ) -> str:
        """Return ``<script>`` and ``<link>`` tags for ``entry``."""
        return self._bridge.asset_tags(
            entry,
            csp_nonce=csp_nonce,
            preload_dynamic=preload_dynamic,
        )


def attach_vite(
    app: Flask | Any,
    config: ViteConfig | None = None,
    *,
    attribute: str = "vite",
    bridge_name: str = "default",
) -> ViteAssetBridge:
    """Attach a ``ViteAssetBridge`` to a Flask app and register Jinja2 template helpers."""
    ensure_module("flask", extra="flask")
    bridge = ViteAssetBridge(config or ViteConfig())
    register_vite_bridge(bridge.bridge, name=bridge_name)
    setattr(app, attribute, bridge)
    extensions = getattr(app, "extensions", None)
    if isinstance(extensions, dict):
        extensions["vertebra_vite"] = bridge

    if hasattr(app, "context_processor") and callable(app.context_processor):

        def _inject_vite_helpers() -> dict[str, Any]:
            return {
                "vite_hmr_client": bridge.hmr_client_tag,
                "vite_asset": bridge.asset_tags,
            }

        app.context_processor(_inject_vite_helpers)

    return bridge
