"""OpenAPI 3.1.0 schema extraction and configurable Scalar/Swagger/AsyncAPI docs for Django."""

from __future__ import annotations

from typing import Any

from vertebra._vertebra import Response
from vertebra.extensions._web.wsgi_shared import build_openapi_operation
from vertebra.extensions.django.router import RouteCompiler
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
from vertebra.utils.serializers import encode_json_bytes

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
    "attach_openapi_docs",
    "extract_openapi_schema",
)


def extract_openapi_schema(
    urlpatterns_or_compiler: Any,
    *,
    title: str = "Django API",
    version: str = "0.1.0",
) -> dict[str, Any]:
    """Extract an OpenAPI 3.1.0 schema from Django URL patterns or a ``RouteCompiler``."""
    if isinstance(urlpatterns_or_compiler, RouteCompiler):
        compiler = urlpatterns_or_compiler
    else:
        compiler = RouteCompiler()
        if urlpatterns_or_compiler is not None:
            compiler.compile_urlpatterns(urlpatterns_or_compiler)

    paths: dict[str, dict[str, Any]] = {}
    for entry in compiler.entries:
        if entry.name in {"vertebra_openapi_json"} or (entry.name and entry.name.startswith("vertebra_docs_")):
            continue
        openapi_path = entry.matchit_path.replace("{*", "{")
        op_prefix = entry.name or getattr(entry.callback, "__name__", "view")
        path_item = paths.setdefault(openapi_path, {})
        path_item.update(
            build_openapi_operation(
                entry.callback,
                entry.param_converters,
                entry.methods,
                operation_prefix=op_prefix,
                ignored_params=frozenset({"request", "self", "cls"}),
            )
        )
    return {
        "openapi": "3.1.0",
        "info": {"title": title, "version": version},
        "paths": paths,
    }


def attach_openapi_docs(
    urlpatterns: list[Any] | None = None,
    config: OpenAPIConfig | None = None,
    *,
    compiler: RouteCompiler | None = None,
    openapi_schema: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Response]]:
    """Build OpenAPI 3.1.0 schema and pre-rendered ``Response`` objects for Scalar, Swagger, and AsyncAPI."""
    resolved = config or OpenAPIConfig()
    source = compiler if compiler is not None else urlpatterns
    schema = openapi_schema or extract_openapi_schema(source, title=resolved.title, version=resolved.version)
    schema_bytes = encode_json_bytes(schema)
    rendered_pages = resolved.render_docs_routes()

    static_responses: dict[str, Response] = {
        resolved.schema_path: Response(
            status=200,
            headers=[("content-type", "application/json")],
            body=schema_bytes,
        )
    }
    for route_path, html_body in rendered_pages.items():
        static_responses[route_path] = Response(
            status=200,
            headers=[("content-type", "text/html; charset=utf-8")],
            body=html_body.encode("utf-8"),
        )

    return schema, static_responses
