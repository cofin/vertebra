"""OpenAPI 3.1.0 schema extraction and configurable Scalar/Swagger/AsyncAPI docs for Flask."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from vertebra.extensions._web.wsgi_shared import build_openapi_operation
from vertebra.extensions.flask.router import convert_werkzeug_rule
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
from vertebra.utils.serializers import encode_json_bytes

if TYPE_CHECKING:
    from flask import Flask

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
            openapi_path, converters = convert_werkzeug_rule(str(getattr(rule_obj, "rule", "/")))
            openapi_path = openapi_path.replace("{*", "{")
            raw_methods = getattr(rule_obj, "methods", None) or ("GET",)
            path_item = paths.setdefault(openapi_path, {})
            path_item.update(
                build_openapi_operation(
                    view_func,
                    converters,
                    raw_methods,
                    operation_prefix=endpoint,
                    summary_fallback=endpoint,
                )
            )
    return {
        "openapi": "3.1.0",
        "info": {"title": title, "version": version},
        "paths": paths,
    }


def attach_openapi_docs(
    app: Flask | Any,
    config: OpenAPIConfig | None = None,
    *,
    openapi_schema: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Attach `/openapi.json` and configurable Scalar, Swagger, and AsyncAPI routes to a Flask app."""
    ensure_module("flask", extra="flask")
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
