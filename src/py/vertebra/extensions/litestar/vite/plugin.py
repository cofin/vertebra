"""First-class integration for litestar-vite with wheel-native @vertebra/client asset resolution and static hooks."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from vertebra.framework.vite import (
    PROTECTED_METADATA_FILENAMES,
    AssetManifest,
    EarlyHintsSpec,
    ManifestEntry,
    ResolvedAssetTags,
    ViteBridge,
    ViteConfig,
    escape_html_json,
    get_vertebra_client_path,
    get_vertebra_client_vite_aliases,
    is_fingerprinted_asset,
    is_protected_build_metadata_path,
)
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from litestar_vite import ViteConfig as LitestarViteConfig
    from litestar_vite import VitePlugin

    class BundlerVitePlugin(VitePlugin):
        """Slotted VitePlugin subclass exposing resolved @vertebra/client aliases."""

        __slots__ = ("client_aliases",)

        client_aliases: dict[str, str]

        def __init__(
            self,
            config: LitestarViteConfig | None = None,
            *,
            client_aliases: dict[str, str] | None = None,
        ) -> None: ...


__all__ = (
    "PROTECTED_METADATA_FILENAMES",
    "AssetManifest",
    "BundlerVitePlugin",
    "EarlyHintsSpec",
    "ManifestEntry",
    "ResolvedAssetTags",
    "ViteBridge",
    "ViteConfig",
    "_StaticAfterRequestHook",
    "_StaticBeforeRequestHook",
    "create_vite_plugin",
    "escape_html_json",
    "get_vertebra_client_path",
    "get_vertebra_client_vite_aliases",
    "is_fingerprinted_asset",
    "is_protected_build_metadata_path",
)

_CACHED_VITE_PLUGIN_CLS: type[Any] | None = None


def _get_bundler_vite_plugin_cls() -> type[BundlerVitePlugin]:
    global _CACHED_VITE_PLUGIN_CLS
    if _CACHED_VITE_PLUGIN_CLS is None:
        vite_mod = ensure_module("litestar_vite", extra="vite")
        base_cls: Any = vite_mod.VitePlugin

        def __init__(
            self: Any,
            config: LitestarViteConfig | None = None,
            *,
            client_aliases: dict[str, str] | None = None,
        ) -> None:
            base_cls.__init__(self, config)
            self.client_aliases = dict(client_aliases) if client_aliases is not None else {}

        _CACHED_VITE_PLUGIN_CLS = type(
            "BundlerVitePlugin",
            (base_cls,),
            {
                "__module__": __name__,
                "__doc__": "Slotted VitePlugin subclass exposing resolved @vertebra/client aliases.",
                "__slots__": ("client_aliases",),
                "__init__": __init__,
            },
        )
    return cast("type[BundlerVitePlugin]", _CACHED_VITE_PLUGIN_CLS)


def __getattr__(name: str) -> Any:
    """Lazily bind ``BundlerVitePlugin`` to ``litestar_vite.VitePlugin``."""
    if name == "BundlerVitePlugin":
        return _get_bundler_vite_plugin_cls()
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)


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
            response_mod = ensure_module("litestar.response", extra="litestar")
            return response_mod.Response(content=b"", status_code=404, media_type="text/plain")
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


def create_vite_plugin(
    config: LitestarViteConfig | None = None,
    *,
    bundle_dir: str | Path = "public",
    resource_dir: str | Path = "resources",
    include_client_alias: bool = True,
    **kwargs: Any,
) -> BundlerVitePlugin:
    """Create a slotted BundlerVitePlugin configured with @vertebra/client asset paths."""
    plugin_cls = _get_bundler_vite_plugin_cls()
    aliases = get_vertebra_client_vite_aliases() if include_client_alias else {}
    if config is not None:
        return plugin_cls(config, client_aliases=aliases)

    vite_mod = ensure_module("litestar_vite", extra="vite")
    paths_mod = ensure_module("litestar_vite.config._paths", extra="vite")
    resolved_config = vite_mod.ViteConfig(
        paths=paths_mod.PathConfig(
            bundle_dir=Path(bundle_dir),
            resource_dir=Path(resource_dir),
        ),
        **kwargs,
    )
    return plugin_cls(resolved_config, client_aliases=aliases)
