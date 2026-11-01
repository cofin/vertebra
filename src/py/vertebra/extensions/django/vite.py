"""Vite asset manifest and Django template tag bridge."""

from __future__ import annotations

import contextlib
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
    "ViteBridge",
    "ViteConfig",
    "attach_vite",
    "get_vertebra_client_path",
    "get_vertebra_client_vite_aliases",
)


class ViteConfig(msgspec.Struct, frozen=True, gc=False):
    """Configuration for Django Vite asset and HMR integration."""

    manifest_path: str | None = None
    asset_url: str = "/static/build/"
    dev_mode: bool = False
    dev_server_url: str = "http://localhost:5173"


class ViteBridge:
    """Resolves Vite manifest assets and HMR tags for Django templates."""

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


_ACTIVE_VITE_BRIDGE: ViteBridge = ViteBridge()


def get_active_vite_bridge() -> ViteBridge:
    """Return the active ``ViteBridge`` instance."""
    return _ACTIVE_VITE_BRIDGE


def attach_vite(
    target: Any = None,
    config: ViteConfig | None = None,
    *,
    attribute: str = "vite",
) -> ViteBridge:
    """Create and attach a ``ViteBridge`` to a Django settings/plugin object."""
    global _ACTIVE_VITE_BRIDGE
    bridge = ViteBridge(config or ViteConfig())
    _ACTIVE_VITE_BRIDGE = bridge
    if target is not None:
        with contextlib.suppress(AttributeError, TypeError):
            setattr(target, attribute, bridge)
    return bridge
