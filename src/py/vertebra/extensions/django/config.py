"""Configuration models for the Django Vertebra extension."""

from __future__ import annotations

from typing import TYPE_CHECKING

import msgspec

if TYPE_CHECKING:
    from vertebra.extensions.django.mcp import MCPConfig
    from vertebra.extensions.django.openapi import OpenAPIConfig
    from vertebra.extensions.django.vite import ViteConfig

__all__ = ("VertebraConfig",)


class VertebraConfig(msgspec.Struct, frozen=True, gc=False):
    """Configuration for the Vertebra Django extension."""

    root_urlconf: str | None = None
    compile_routes: bool = True
    enable_fastpath: bool = True
    offload_middlewares: bool = True
    accelerate_ninja: bool = True
    accelerate_drf: bool = True
    openapi: OpenAPIConfig | None = None
    mcp: MCPConfig | None = None
    vite: ViteConfig | None = None
