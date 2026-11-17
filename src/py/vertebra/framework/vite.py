"""Framework-neutral Vite asset manifest resolution, HMR tags, and client path helpers."""

from __future__ import annotations

import html
from pathlib import Path

import msgspec

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
from vertebra.build._import_map import load_client_subpaths

__all__ = (
    "PROTECTED_METADATA_FILENAMES",
    "AssetManifest",
    "EarlyHintsSpec",
    "ManifestEntry",
    "ResolvedAssetTags",
    "ViteBridge",
    "ViteConfig",
    "clear_vite_bridges",
    "escape_html_json",
    "get_active_vite_bridge",
    "get_vertebra_client_path",
    "get_vertebra_client_vite_aliases",
    "is_fingerprinted_asset",
    "is_protected_build_metadata_path",
    "register_vite_bridge",
    "vite_asset",
    "vite_hmr_client",
)


class ViteConfig(msgspec.Struct, frozen=True, gc=False):
    """Configuration for Vite asset manifest resolution and HMR integration."""

    manifest_path: str | Path | None = None
    asset_url: str = "/static/build/"
    dev_mode: bool = False
    dev_server_url: str = "http://localhost:5173"
    csp_nonce: str | None = None
    preload_dynamic: bool = False


class ViteBridge:
    """Resolve Vite manifest assets and HMR tags independently of the web framework."""

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
        elif self._config.manifest_path is not None:
            manifest_target = Path(self._config.manifest_path)
            self._manifest = AssetManifest.from_file(manifest_target) if manifest_target.exists() else None
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

    @property
    def early_hints(self) -> EarlyHintsSpec:
        """Return the most recently resolved ``EarlyHintsSpec``."""
        return self._early_hints

    def hmr_client_tag(self, *, csp_nonce: str | None = None) -> str:
        """Return the ``@vite/client`` script tag in dev mode or empty string in production."""
        if not self._config.dev_mode:
            return ""
        base = self._config.dev_server_url.rstrip("/")
        src = html.escape(f"{base}/@vite/client", quote=True)
        nonce = csp_nonce if csp_nonce is not None else self._config.csp_nonce
        nonce_attr = f' nonce="{html.escape(nonce, quote=True)}"' if nonce else ""
        return f'<script type="module" src="{src}"{nonce_attr}></script>'

    def asset_tags(
        self,
        entry: str,
        *,
        csp_nonce: str | None = None,
        preload_dynamic: bool | None = None,
    ) -> str:
        """Return ``<script>`` and ``<link>`` HTML tags for ``entry``."""
        stripped = entry.strip()
        clean = stripped.lstrip("/")
        if not clean:
            msg = "Vite asset entrypoint must not be empty"
            raise ValueError(msg)

        nonce = csp_nonce if csp_nonce is not None else self._config.csp_nonce
        nonce_attr = f' nonce="{html.escape(nonce, quote=True)}"' if nonce else ""

        if self._config.dev_mode:
            base = self._config.dev_server_url.rstrip("/")
            src = html.escape(f"{base}/{clean}", quote=True)
            return f'<script type="module" src="{src}"{nonce_attr}></script>'

        use_preload_dynamic = self._config.preload_dynamic if preload_dynamic is None else preload_dynamic
        if self._manifest is not None and self._manifest.entries:
            lookup_key = stripped if stripped in self._manifest.entries else clean
            resolved = self._manifest.resolve_entry(
                lookup_key,
                asset_url=self._config.asset_url,
                csp_nonce=nonce,
                preload_dynamic=use_preload_dynamic,
            )
            self._early_hints = resolved.early_hints
            return resolved.to_html()

        prefix = self._config.asset_url.rstrip("/")
        src = html.escape(f"{prefix}/{clean}", quote=True)
        return f'<script type="module" src="{src}"{nonce_attr}></script>'


_VITE_BRIDGES: dict[str, ViteBridge] = {}
_DEFAULT_VITE_BRIDGE = ViteBridge()


def register_vite_bridge(bridge: ViteBridge, name: str = "default") -> ViteBridge:
    """Register a named ``ViteBridge`` instance in the process-wide bridge registry."""
    _VITE_BRIDGES[name] = bridge
    return bridge


def get_active_vite_bridge(name: str = "default") -> ViteBridge:
    """Return the registered ``ViteBridge`` for ``name`` or the default bridge."""
    bridge = _VITE_BRIDGES.get(name)
    if bridge is not None:
        return bridge
    if name == "default":
        return _DEFAULT_VITE_BRIDGE
    available = ", ".join(sorted(_VITE_BRIDGES)) or "<none>"
    msg = f"Unknown Vite bridge {name!r}; registered bridges: {available}"
    raise KeyError(msg)


def clear_vite_bridges() -> None:
    """Remove all registered ``ViteBridge`` instances from the registry."""
    _VITE_BRIDGES.clear()


def vite_hmr_client(*, name: str = "default", csp_nonce: str | None = None) -> str:
    """Render the Vite HMR client script tag from the active ``ViteBridge``."""
    return get_active_vite_bridge(name).hmr_client_tag(csp_nonce=csp_nonce)


def vite_asset(
    entry: str,
    *,
    name: str = "default",
    csp_nonce: str | None = None,
    preload_dynamic: bool | None = None,
) -> str:
    """Render Vite module script and CSS link tags from the active ``ViteBridge``."""
    return get_active_vite_bridge(name).asset_tags(
        entry,
        csp_nonce=csp_nonce,
        preload_dynamic=preload_dynamic,
    )


def get_vertebra_client_path() -> Path:
    """Return the filesystem path to the ``@vertebra/client`` TypeScript SDK package."""
    cwd = Path.cwd()
    repo_ts_path = cwd / "src" / "ts" / "vertebra-client"
    if repo_ts_path.is_dir():
        return repo_ts_path

    module_dir = Path(__file__).resolve().parent.parent
    bundled_path = module_dir / "client"
    if bundled_path.is_dir():
        return bundled_path

    return repo_ts_path


def get_vertebra_client_vite_aliases() -> dict[str, str]:
    """Return Vite ``resolve.alias`` mappings for ``@vertebra/client`` and all subpaths."""
    client_path = get_vertebra_client_path()
    dist_path = client_path / "dist"
    src_path = client_path / "src"
    use_dist = dist_path.is_dir()
    package_json = client_path / "package.json"
    subpaths = load_client_subpaths(package_json if package_json.is_file() else None)
    aliases: dict[str, str] = {}
    for subpath, js_rel, _types_rel in subpaths:
        alias_key = "@vertebra/client" if subpath == "." else f"@vertebra/client/{subpath.removeprefix('./')}"
        rel_clean = js_rel.removeprefix("./")
        if use_dist:
            target_file = dist_path / rel_clean
        else:
            stem = rel_clean.removesuffix(".js")
            candidates = (
                src_path / f"{stem}.ts",
                src_path / f"{stem}.tsx",
                src_path / stem / "index.ts",
                src_path / stem / "index.tsx",
            )
            target_file = next((candidate for candidate in candidates if candidate.is_file()), candidates[0])
        aliases[alias_key] = str(target_file)
    return aliases
