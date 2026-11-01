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
from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import ensure_module

try:
    from litestar import MediaType, get
except ImportError as exc:
    raise MissingDependencyError(
        "litestar",
        install_package="litestar",
        extra="litestar",
    ) from exc

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
    ensure_module("litestar", extra="litestar")
    cfg = config or OpenAPIConfig()
    handlers: list[Any] = []
    for path, html_doc in cfg.render_docs_routes().items():

        def _make_handler(route_path: str, payload: str) -> Any:
            @get(
                route_path,
                media_type=MediaType.HTML,
                include_in_schema=False,
                sync_to_thread=False,
                opt={"exclude_from_auth": True},
            )
            def _docs_portal() -> str:
                return payload

            _docs_portal.fn.__name__ = f"docs_{route_path.strip('/').replace('/', '_') or 'root'}"
            return _docs_portal

        handlers.append(_make_handler(path, html_doc))
    return handlers
