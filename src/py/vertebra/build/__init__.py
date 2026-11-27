"""Unified build orchestrator, native bundler facade, asset manifest, codegen, and packaging."""

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
    from vertebra.build._bundler import (
        BundleConfig,
        BundleDevConfig,
        BundleOutput,
        Bundler,
        BundleResult,
        IsolatedDeclarationsGenerator,
        SourceWatcher,
        minify_css,
    )
    from vertebra.build._config import BuildConfig, JsConfig
    from vertebra.build._orchestrator import BuildOrchestrator

__all__ = (
    "PROTECTED_METADATA_FILENAMES",
    "AssetManifest",
    "BuildConfig",
    "BuildOrchestrator",
    "BundleConfig",
    "BundleDevConfig",
    "BundleOutput",
    "BundleResult",
    "Bundler",
    "EarlyHintsSpec",
    "IsolatedDeclarationsGenerator",
    "JsConfig",
    "ManifestEntry",
    "ResolvedAssetTags",
    "SourceWatcher",
    "codegen",
    "escape_html_json",
    "is_fingerprinted_asset",
    "is_protected_build_metadata_path",
    "minify_css",
    "packaging",
)

_EXPORTS: dict[str, str] = {
    "PROTECTED_METADATA_FILENAMES": "vertebra.build._assets",
    "AssetManifest": "vertebra.build._assets",
    "BuildConfig": "vertebra.build._config",
    "BuildOrchestrator": "vertebra.build._orchestrator",
    "BundleConfig": "vertebra.build._bundler",
    "BundleDevConfig": "vertebra.build._bundler",
    "BundleOutput": "vertebra.build._bundler",
    "BundleResult": "vertebra.build._bundler",
    "Bundler": "vertebra.build._bundler",
    "EarlyHintsSpec": "vertebra.build._assets",
    "IsolatedDeclarationsGenerator": "vertebra.build._bundler",
    "JsConfig": "vertebra.build._config",
    "ManifestEntry": "vertebra.build._assets",
    "ResolvedAssetTags": "vertebra.build._assets",
    "SourceWatcher": "vertebra.build._bundler",
    "codegen": "vertebra.build.codegen",
    "escape_html_json": "vertebra.build._assets",
    "is_fingerprinted_asset": "vertebra.build._assets",
    "is_protected_build_metadata_path": "vertebra.build._assets",
    "minify_css": "vertebra.build._bundler",
    "packaging": "vertebra.build.packaging",
}

__getattr__, __dir__ = lazy_exports(__name__, _EXPORTS)
