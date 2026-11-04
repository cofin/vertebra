"""Configuration models for the Flask Vertebra extension."""

from __future__ import annotations

from typing import TYPE_CHECKING

import msgspec

if TYPE_CHECKING:
    from vertebra.extensions.flask.mcp import MCPConfig
    from vertebra.extensions.flask.openapi import OpenAPIConfig
    from vertebra.extensions.flask.vite import ViteConfig

__all__ = ("VertebraConfig",)


class VertebraConfig(msgspec.Struct, frozen=True, gc=False):
    """Configuration for the Vertebra Flask extension."""

    compile_routes: bool = True
    enable_fastpath: bool = True
    offload_middlewares: bool = True
    pre_intern_headers: bool = True
    static_url_path: str | None = None
    static_folder: str | None = None
    openapi: OpenAPIConfig | None = None
    mcp: MCPConfig | None = None
    vite: ViteConfig | None = None
    app_attribute: str = "vertebra"
