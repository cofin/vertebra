"""OpenAPI 3.1.0 schema extraction and configurable Scalar/Swagger/AsyncAPI docs for Django."""

from __future__ import annotations

import inspect
from collections.abc import Mapping
from typing import Any, get_type_hints

import msgspec

from vertebra._vertebra import Response
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
from vertebra.utils.serializers import encode_json_bytes, schema_components

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


def _schema_for_type(tp: Any) -> dict[str, Any]:
    """Return a JSON Schema snippet for a Python type annotation."""
    if tp is int:
        return {"type": "integer"}
    if tp is float:
        return {"type": "number"}
    if tp is bool:
        return {"type": "boolean"}
    if tp is str:
        return {"type": "string"}
    if isinstance(tp, type) and issubclass(tp, msgspec.Struct):
        schemas, defs = schema_components((tp,))
        out = dict(schemas[0])
        if defs:
            out["$defs"] = defs
        return out
    model_json_schema = getattr(tp, "model_json_schema", None)
    if isinstance(tp, type) and callable(model_json_schema):
        schema = model_json_schema()
        if isinstance(schema, Mapping):
            return dict(schema)
    return {"type": "object"}


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
        view_fn = entry.callback
        try:
            hints = get_type_hints(view_fn)
        except Exception:
            hints = {}
        try:
            sig = inspect.signature(view_fn)
        except (TypeError, ValueError):
            sig = None

        path_param_names = {n for n, _ in entry.param_converters}
        parameters: list[dict[str, Any]] = []
        for p_name, conv in entry.param_converters:
            p_type = "integer" if conv == "int" else ("number" if conv == "float" else "string")
            parameters.append(
                {
                    "name": p_name,
                    "in": "path",
                    "required": True,
                    "schema": {"type": p_type},
                }
            )

        request_body: dict[str, Any] | None = None
        return_ann: Any = inspect.Parameter.empty
        if sig is not None:
            for param_name, param in sig.parameters.items():
                if param_name in path_param_names or param_name in {"request", "self", "cls"}:
                    continue
                ann = hints.get(param_name, param.annotation)
                if ann is not inspect.Parameter.empty:
                    request_body = {
                        "required": True,
                        "content": {"application/json": {"schema": _schema_for_type(ann)}},
                    }
            return_ann = hints.get("return", sig.return_annotation)

        response_schema = (
            _schema_for_type(return_ann) if return_ann is not inspect.Parameter.empty else {"type": "object"}
        )
        path_item = paths.setdefault(openapi_path, {})
        for method in entry.methods:
            m = method.lower()
            if m in {"head", "options"}:
                continue
            op_id = f"{entry.name or getattr(view_fn, '__name__', 'view')}_{m}"
            op: dict[str, Any] = {
                "operationId": op_id,
                "summary": (inspect.getdoc(view_fn) or op_id).splitlines()[0],
                "responses": {
                    "200": {
                        "description": "Successful Response",
                        "content": {"application/json": {"schema": response_schema}},
                    }
                },
            }
            if parameters:
                op["parameters"] = parameters
            if request_body is not None and m in {"post", "put", "patch"}:
                op["requestBody"] = request_body
            path_item[m] = op

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
