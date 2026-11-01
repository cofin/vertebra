"""Vertebra Vite extension for litestar-vite."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
    from vertebra.extensions.litestar.vite.plugin import (
        PROTECTED_METADATA_FILENAMES,
        AssetManifest,
        EarlyHintsSpec,
        ManifestEntry,
        ResolvedAssetTags,
        _StaticAfterRequestHook,
        _StaticBeforeRequestHook,
        create_vite_plugin,
        escape_html_json,
        get_vertebra_client_path,
        get_vertebra_client_vite_aliases,
        is_fingerprinted_asset,
        is_protected_build_metadata_path,
    )

__all__ = (
    "PROTECTED_METADATA_FILENAMES",
    "AssetManifest",
    "EarlyHintsSpec",
    "ManifestEntry",
    "ResolvedAssetTags",
    "_StaticAfterRequestHook",
    "_StaticBeforeRequestHook",
    "create_vite_plugin",
    "escape_html_json",
    "get_vertebra_client_path",
    "get_vertebra_client_vite_aliases",
    "is_fingerprinted_asset",
    "is_protected_build_metadata_path",
)

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "PROTECTED_METADATA_FILENAMES": ("vertebra.extensions.litestar.vite.plugin", "PROTECTED_METADATA_FILENAMES"),
    "AssetManifest": ("vertebra.extensions.litestar.vite.plugin", "AssetManifest"),
    "EarlyHintsSpec": ("vertebra.extensions.litestar.vite.plugin", "EarlyHintsSpec"),
    "ManifestEntry": ("vertebra.extensions.litestar.vite.plugin", "ManifestEntry"),
    "ResolvedAssetTags": ("vertebra.extensions.litestar.vite.plugin", "ResolvedAssetTags"),
    "_StaticAfterRequestHook": ("vertebra.extensions.litestar.vite.plugin", "_StaticAfterRequestHook"),
    "_StaticBeforeRequestHook": ("vertebra.extensions.litestar.vite.plugin", "_StaticBeforeRequestHook"),
    "create_vite_plugin": ("vertebra.extensions.litestar.vite.plugin", "create_vite_plugin"),
    "escape_html_json": ("vertebra.extensions.litestar.vite.plugin", "escape_html_json"),
    "get_vertebra_client_path": ("vertebra.extensions.litestar.vite.plugin", "get_vertebra_client_path"),
    "get_vertebra_client_vite_aliases": (
        "vertebra.extensions.litestar.vite.plugin",
        "get_vertebra_client_vite_aliases",
    ),
    "is_fingerprinted_asset": ("vertebra.extensions.litestar.vite.plugin", "is_fingerprinted_asset"),
    "is_protected_build_metadata_path": (
        "vertebra.extensions.litestar.vite.plugin",
        "is_protected_build_metadata_path",
    ),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
