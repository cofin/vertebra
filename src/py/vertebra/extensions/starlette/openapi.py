"""Configurable OpenAPI 3.1/3.2 and AsyncAPI 3.0/3.1 portal routes for Starlette and FastAPI."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

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

if TYPE_CHECKING:
    import starlette.responses as _starlette_responses
    import starlette.routing as _starlette_routing

    _ = (_starlette_responses, _starlette_routing)

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
    "mount_docs_routes",
)


def mount_docs_routes(app: Any, config: OpenAPIConfig | None = None) -> dict[str, str]:
    """Mount configured Scalar, Swagger UI, AsyncAPI, Redoc, Elements, and RapiDoc routes onto ``app``."""
    resolved = config or OpenAPIConfig()
    rendered = resolved.render_docs_routes()
    routes = getattr(app, "routes", None)
    if not isinstance(routes, list):
        return rendered

    routing_mod = ensure_module("starlette.routing", package="starlette", extra="starlette")
    responses_mod = ensure_module("starlette.responses", package="starlette", extra="starlette")
    existing_paths = {getattr(r, "path", None) for r in routes}

    for path, html_body in rendered.items():
        if path in existing_paths:
            continue

        async def _serve_portal(_request: Any, _html: str = html_body) -> Any:
            return responses_mod.HTMLResponse(_html)

        routes.append(
            routing_mod.Route(
                path,
                endpoint=_serve_portal,
                methods=["GET"],
                include_in_schema=False,
            )
        )
    return rendered
