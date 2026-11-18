"""Vite asset manifest and Django template tag bridge."""

from __future__ import annotations

import contextlib
from typing import Any

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


def attach_vite(
    target: Any = None,
    config: ViteConfig | None = None,
    *,
    attribute: str = "vite",
    bridge_name: str = "default",
) -> ViteBridge:
    """Create, register, and attach a ``ViteBridge`` to a Django settings/plugin object."""
    bridge = ViteBridge(config or ViteConfig())
    register_vite_bridge(bridge, name=bridge_name)
    if target is not None:
        with contextlib.suppress(AttributeError, TypeError):
            setattr(target, attribute, bridge)
    return bridge
