"""First-class integration for litestar-vite with wheel-native @vertebra/client asset resolution."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import ensure_module

try:
    from litestar_vite import ViteConfig, VitePlugin
    from litestar_vite.config._paths import PathConfig
except ImportError as exc:
    raise MissingDependencyError(
        "litestar-vite",
        install_package="litestar-vite",
        extra="vite",
    ) from exc


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
