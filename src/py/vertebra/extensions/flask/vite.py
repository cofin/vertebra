"""Vite asset manifest and Jinja2 template context processor for Flask applications."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import msgspec

from vertebra.build._assets import AssetManifest, EarlyHintsSpec
from vertebra.extensions.litestar.vite import (
    get_vertebra_client_path,
    get_vertebra_client_vite_aliases,
)

__all__ = (
    "AssetManifest",
    "EarlyHintsSpec",
    "ViteAssetBridge",
    "ViteConfig",
    "attach_vite",
    "get_vertebra_client_path",
    "get_vertebra_client_vite_aliases",
)


class ViteConfig(msgspec.Struct, frozen=True, gc=False):
    """Configuration for Flask Vite asset and HMR integration."""

    manifest_path: str | None = None
    asset_url: str = "/static/build/"
    dev_mode: bool = False
    dev_server_url: str = "http://localhost:5173"


class ViteAssetBridge:
    """Resolves Vite manifest assets and HMR tags for Flask Jinja2 templates."""

    __slots__ = ("_config", "_early_hints", "_manifest")

    def __init__(
        self,
        config: ViteConfig | None = None,
        *,
        manifest: AssetManifest | None = None,
    ) -> None:
        self._config = config or ViteConfig()
        self._manifest: AssetManifest | None
        if manifest is not None:
            self._manifest = manifest
        elif self._config.manifest_path is not None and Path(self._config.manifest_path).exists():
            self._manifest = AssetManifest.from_file(Path(self._config.manifest_path))
        else:
            self._manifest = None
        self._early_hints = EarlyHintsSpec()

    @property
    def config(self) -> ViteConfig:
        """Return the ``ViteConfig``."""
        return self._config

    @property
    def manifest(self) -> AssetManifest | None:
        """Return the loaded ``AssetManifest`` when available."""
        return self._manifest

    def hmr_client_tag(self) -> str:
        """Return the ``@vite/client`` script tag in dev mode or empty string in production."""
        if not self._config.dev_mode:
            return ""
        base = self._config.dev_server_url.rstrip("/")
        return f'<script type="module" src="{base}/@vite/client"></script>'

    def asset_tags(self, entry: str) -> str:
        """Return ``<script>`` and ``<link>`` tags for ``entry``."""
        if self._config.dev_mode:
            base = self._config.dev_server_url.rstrip("/")
            clean = entry.lstrip("/")
            return f'<script type="module" src="{base}/{clean}"></script>'
        prefix = self._config.asset_url.rstrip("/")
        clean = entry.lstrip("/")
        return f'<script type="module" src="{prefix}/{clean}"></script>'


def attach_vite(
    app: Any,
    config: ViteConfig | None = None,
    *,
    attribute: str = "vite",
) -> ViteAssetBridge:
    """Attach a ``ViteAssetBridge`` to a Flask app and register Jinja2 template helpers."""
    bridge = ViteAssetBridge(config or ViteConfig())
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
