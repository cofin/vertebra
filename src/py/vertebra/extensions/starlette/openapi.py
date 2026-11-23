"""Configurable OpenAPI 3.1/3.2 and AsyncAPI 3.0/3.1 portal routes for Starlette and FastAPI."""

from __future__ import annotations

from typing import Any

from vertebra.extensions._web.asgi_starlette import mount_starlette_docs_routes
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
    return mount_starlette_docs_routes(app, config, require_starlette=True)
