"""First-class integration for litestar-vite with wheel-native @vertebra/client asset resolution and static hooks."""

from __future__ import annotations

from pathlib import Path
from typing import Any

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
from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import ensure_module

try:
    from litestar.response import Response as LitestarResponse
    from litestar_vite import ViteConfig, VitePlugin
    from litestar_vite.config._paths import PathConfig
except ImportError as exc:
    raise MissingDependencyError(
        "litestar-vite",
        install_package="litestar-vite",
        extra="vite",
    ) from exc

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


class _StaticBeforeRequestHook:
    """Picklable slotted Router before-request hook blocking protected build metadata files with 404."""

    __slots__ = ("asset_url",)

    def __init__(self, asset_url: str = "/static/") -> None:
        self.asset_url = asset_url.rstrip("/")

    def __call__(self, request: Any) -> Any:
        url_obj = getattr(request, "url", None)
        path = str(getattr(url_obj, "path", getattr(request, "path", "")))
        rel_path = (
            path[len(self.asset_url) :].lstrip("/")
            if self.asset_url and path.startswith(self.asset_url)
            else path.lstrip("/")
        )
        if is_protected_build_metadata_path(rel_path) or is_protected_build_metadata_path(path):
            return LitestarResponse(content=b"", status_code=404, media_type="text/plain")
        return None


class _StaticAfterRequestHook:
    """Picklable slotted Router after-request hook setting immutable Cache-Control on fingerprinted assets."""

    __slots__ = ("max_age",)

    def __init__(self, max_age: int = 31536000) -> None:
        self.max_age = max_age

    def __call__(self, response: Any) -> Any:
        headers = getattr(response, "headers", None)
        if headers is None:
            return response
        file_path = str(getattr(response, "file_path", "") or "")
        if file_path and is_fingerprinted_asset(file_path):
            headers["Cache-Control"] = f"public, max-age={self.max_age}, immutable"
        return response


def get_vertebra_client_path() -> Path:
    """Return the filesystem path to the @vertebra/client TypeScript SDK package."""
    ensure_module("litestar_vite", extra="vite")
    cwd = Path.cwd()
    repo_ts_path = cwd / "src" / "ts" / "vertebra-client"
    if repo_ts_path.is_dir():
        return repo_ts_path

    module_dir = Path(__file__).resolve().parent.parent.parent
    bundled_path = module_dir / "client"
    if bundled_path.is_dir():
        return bundled_path

    return repo_ts_path


def get_vertebra_client_vite_aliases() -> dict[str, str]:
    """Return Vite resolve.alias mappings for @vertebra/client."""
    client_path = get_vertebra_client_path()
    dist_path = client_path / "dist"
    target = dist_path if dist_path.is_dir() else (client_path / "src")
    return {
        "@vertebra/client": str(target),
    }


def create_vite_plugin(
    config: ViteConfig | None = None,
    *,
    bundle_dir: str | Path = "public",
    resource_dir: str | Path = "resources",
    include_client_alias: bool = True,
    **kwargs: Any,
) -> VitePlugin:
    """Create a VitePlugin configured with @vertebra/client asset paths."""
    _ = include_client_alias
    ensure_module("litestar_vite", extra="vite")
    if config is not None:
        return VitePlugin(config)

    resolved_config = ViteConfig(
        paths=PathConfig(
            bundle_dir=Path(bundle_dir),
            resource_dir=Path(resource_dir),
        ),
        **kwargs,
    )
    return VitePlugin(resolved_config)
