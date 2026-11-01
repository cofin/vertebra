"""OpenAPI 3.1.0 schema extraction and configurable Scalar/Swagger/AsyncAPI docs for Flask."""

from __future__ import annotations

import inspect
from typing import Any, get_type_hints

import msgspec

from vertebra.extensions.flask.router import convert_werkzeug_rule
from vertebra.extensions.litestar.openapi import (
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
        schemas, defs = msgspec.json.schema_components((tp,))
        out = dict(schemas[0])
        if defs:
            out["$defs"] = defs
        return out
    if isinstance(tp, type) and hasattr(tp, "model_json_schema"):
        return dict(tp.model_json_schema())
    return {"type": "object"}


def extract_openapi_schema(
    app: Any,
    *,
    title: str = "Flask API",
    version: str = "0.1.0",
) -> dict[str, Any]:
    """Inspect Flask ``url_map`` and ``view_functions`` to build an OpenAPI 3.1.0 schema."""
    paths: dict[str, dict[str, Any]] = {}
    url_map = getattr(app, "url_map", None)
    view_functions = getattr(app, "view_functions", {}) or {}
    if url_map is not None and hasattr(url_map, "iter_rules"):
        for rule_obj in url_map.iter_rules():
            endpoint = str(getattr(rule_obj, "endpoint", ""))
            if endpoint in {"static", "vertebra_openapi_json"} or endpoint.startswith("vertebra_docs_"):
                continue
            view_func = view_functions.get(endpoint)
            if view_func is None:
                continue
            rule_str = str(getattr(rule_obj, "rule", "/"))
            openapi_path, converters = convert_werkzeug_rule(rule_str)
            openapi_path = openapi_path.replace("{*", "{")
            raw_methods = getattr(rule_obj, "methods", None) or ("GET",)
            methods = [str(m).lower() for m in raw_methods if str(m).upper() not in {"HEAD", "OPTIONS"}]
            try:
                hints = get_type_hints(view_func)
            except Exception:
                hints = {}
            sig = inspect.signature(view_func)
            path_param_names = {n for n, _ in converters}
            parameters: list[dict[str, Any]] = []
            for p_name, conv in converters:
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
            for param_name, param in sig.parameters.items():
                if param_name in path_param_names or param_name == "request":
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
            for m in methods:
                op: dict[str, Any] = {
                    "operationId": f"{endpoint}_{m}",
                    "summary": (inspect.getdoc(view_func) or endpoint).splitlines()[0],
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
    app: Any,
    config: OpenAPIConfig | None = None,
    *,
    openapi_schema: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Attach `/openapi.json` and configurable Scalar, Swagger, and AsyncAPI routes to a Flask app."""
    resolved = config or OpenAPIConfig()
    schema = openapi_schema or extract_openapi_schema(app, title=resolved.title, version=resolved.version)
    schema_bytes = encode_json_bytes(schema)
    rendered_pages = resolved.render_docs_routes()

    if hasattr(app, "add_url_rule") and callable(app.add_url_rule):

        def _serve_openapi_json() -> tuple[bytes, int, dict[str, str]]:
            return schema_bytes, 200, {"Content-Type": "application/json"}

        existing_endpoints = set((getattr(app, "view_functions", {}) or {}).keys())
        if "vertebra_openapi_json" not in existing_endpoints:
            app.add_url_rule(
                resolved.schema_path,
                endpoint="vertebra_openapi_json",
                view_func=_serve_openapi_json,
                methods=["GET"],
            )

        for route_path, html_body in rendered_pages.items():
            ep_name = f"vertebra_docs_{route_path.strip('/').replace('/', '_') or 'root'}"
            if ep_name in existing_endpoints:
                continue

            def _make_doc_view(html_str: str) -> Any:
                def _serve_doc() -> tuple[str, int, dict[str, str]]:
                    return html_str, 200, {"Content-Type": "text/html; charset=utf-8"}

                return _serve_doc

            app.add_url_rule(
                route_path,
                endpoint=ep_name,
                view_func=_make_doc_view(html_body),
                methods=["GET"],
            )

    return schema
