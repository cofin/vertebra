"""Configurable OpenAPI 3.1/3.2 and AsyncAPI 3.0/3.1 portal routes for FastAPI."""

from __future__ import annotations

from types import SimpleNamespace
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
from vertebra.utils.module_loader import import_optional

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

    routing_mod = import_optional("starlette.routing")
    responses_mod = import_optional("starlette.responses")
    existing_paths = {getattr(r, "path", None) for r in routes}

    for path, html_body in rendered.items():
        if path in existing_paths:
            continue

        async def _serve_portal(_request: Any, _html: str = html_body) -> Any:
            if responses_mod is not None and hasattr(responses_mod, "HTMLResponse"):
                return responses_mod.HTMLResponse(_html)
            return _html

        if routing_mod is not None and hasattr(routing_mod, "Route"):
            routes.append(
                routing_mod.Route(
                    path,
                    endpoint=_serve_portal,
                    methods=["GET"],
                    include_in_schema=False,
                )
            )
        else:
            routes.append(
                SimpleNamespace(
                    path=path,
                    endpoint=_serve_portal,
                    methods=["GET"],
                    name=f"_vertebra_docs_{path.strip('/')}",
                )
            )
    return rendered
