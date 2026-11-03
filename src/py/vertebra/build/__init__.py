"""Unified build orchestrator, asset manifest, multi-target codegen, and packaging for Vertebra."""

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
    from vertebra.build import codegen, packaging
    from vertebra.build._assets import (
        PROTECTED_METADATA_FILENAMES,
        AssetManifest,
        EarlyHintsSpec,
        ManifestEntry,
        ResolvedAssetTags,
        escape_html_json,
        is_fingerprinted_asset,
        is_protected_build_metadata_path,
    )
    from vertebra.build._config import BuildConfig, JsConfig
    from vertebra.build._orchestrator import BuildOrchestrator

__all__ = (
    "PROTECTED_METADATA_FILENAMES",
    "AssetManifest",
    "BuildConfig",
    "BuildOrchestrator",
    "EarlyHintsSpec",
    "JsConfig",
    "ManifestEntry",
    "ResolvedAssetTags",
    "codegen",
    "escape_html_json",
    "is_fingerprinted_asset",
    "is_protected_build_metadata_path",
    "packaging",
)

_EXPORTS: dict[str, str] = {
    "PROTECTED_METADATA_FILENAMES": "vertebra.build._assets",
    "AssetManifest": "vertebra.build._assets",
    "BuildConfig": "vertebra.build._config",
    "BuildOrchestrator": "vertebra.build._orchestrator",
    "EarlyHintsSpec": "vertebra.build._assets",
    "JsConfig": "vertebra.build._config",
    "ManifestEntry": "vertebra.build._assets",
    "ResolvedAssetTags": "vertebra.build._assets",
    "codegen": "vertebra.build.codegen",
    "escape_html_json": "vertebra.build._assets",
    "is_fingerprinted_asset": "vertebra.build._assets",
    "is_protected_build_metadata_path": "vertebra.build._assets",
    "packaging": "vertebra.build.packaging",
}

__getattr__, __dir__ = lazy_exports(__name__, _EXPORTS)
