"""Configurable OpenAPI, Scalar, Swagger UI, Redoc, Elements, and AsyncAPI portal for Litestar."""

from __future__ import annotations

from typing import Any

from vertebra.framework.openapi import (
    AsyncAPIDocsConfig,
    DocsBackendType,
    ElementsDocsConfig,
    OpenAPIConfig,
    RapiDocDocsConfig,
    RedocDocsConfig,
    ScalarDocsConfig,
    ScalarSpecSource,
    SwaggerDocsConfig,
)
from vertebra.utils.module_loader import ensure_module

__all__ = (
    "AsyncAPIDocsConfig",
    "DocsBackendType",
    "ElementsDocsConfig",
    "OpenAPIConfig",
    "RapiDocDocsConfig",
    "RedocDocsConfig",
    "ScalarDocsConfig",
    "ScalarSpecSource",
    "SwaggerDocsConfig",
    "build_docs_route_handlers",
)


def build_docs_route_handlers(config: OpenAPIConfig | None = None) -> list[Any]:
    """Create Litestar GET route handlers for all enabled documentation backends."""
    litestar_mod = ensure_module("litestar", extra="litestar")
    media_type_html = litestar_mod.MediaType.HTML
    get_decorator = litestar_mod.get
    cfg = config or OpenAPIConfig()
    handlers: list[Any] = []
    for path, html_doc in cfg.render_docs_routes().items():

        def _make_handler(route_path: str, payload: str) -> Any:
            def _docs_portal() -> str:
                return payload

            handler = get_decorator(
                route_path,
                media_type=media_type_html,
                include_in_schema=False,
                sync_to_thread=False,
                opt={"exclude_from_auth": True},
            )(_docs_portal)
            handler.fn.__name__ = f"docs_{route_path.strip('/').replace('/', '_') or 'root'}"
            return handler

        handlers.append(_make_handler(path, html_doc))
    return handlers
