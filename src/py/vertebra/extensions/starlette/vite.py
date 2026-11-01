"""Vite, AssetManifest, EarlyHintsSpec, and Inertia v3 SSR integration for Starlette and FastAPI."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from vertebra.build._assets import AssetManifest, EarlyHintsSpec
from vertebra.extensions.litestar.vite import (
    get_vertebra_client_path,
    get_vertebra_client_vite_aliases,
)
from vertebra.framework.inertia import InertiaConfig, InertiaHttpResponse
from vertebra.framework.ssr import render_page_shell

__all__ = (
    "AssetManifest",
    "EarlyHintsSpec",
    "InertiaConfig",
    "InertiaHttpResponse",
    "ViteExtension",
    "get_vertebra_client_path",
    "get_vertebra_client_vite_aliases",
    "render_page_shell",
)


class ViteExtension:
    """Attaches AssetManifest, EarlyHintsSpec, and InertiaConfig to a Starlette or FastAPI application."""

    __slots__ = ("_early_hints", "_inertia_config", "_manifest")

    def __init__(
        self,
        *,
        manifest_path: str | Path | None = None,
        manifest: AssetManifest | None = None,
        early_hints: EarlyHintsSpec | None = None,
        inertia_config: InertiaConfig | None = None,
    ) -> None:
        if manifest is not None:
            self._manifest: AssetManifest | None = manifest
        elif manifest_path is not None:
            path_obj = Path(manifest_path)
            self._manifest = AssetManifest.from_file(path_obj) if path_obj.exists() else None
        else:
            self._manifest = None
        self._early_hints = early_hints or EarlyHintsSpec()
        self._inertia_config = inertia_config

    @property
    def manifest(self) -> AssetManifest | None:
        """Return the loaded AssetManifest when available."""
        return self._manifest

    @property
    def early_hints(self) -> EarlyHintsSpec:
        """Return the active EarlyHintsSpec."""
        return self._early_hints

    def init_app(self, app: Any) -> Any:
        """Attach Vite manifest and EarlyHintsSpec to ``app.state``."""
        state = getattr(app, "state", None)
        if state is not None:
            if isinstance(state, dict):
                state["vite"] = self
                state["asset_manifest"] = self._manifest
                state["early_hints"] = self._early_hints
            else:
                state.vite = self
                state.asset_manifest = self._manifest
                state.early_hints = self._early_hints
        return app
