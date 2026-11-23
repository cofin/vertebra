"""Shared framework-neutral OpenAPI schema and parameter helpers for Flask and Django."""

from __future__ import annotations

import inspect
from collections.abc import Mapping, Sequence
from typing import Any, get_type_hints

import msgspec

from vertebra.utils.serializers import schema_components

__all__ = (
    "build_openapi_operation",
    "schema_for_type",
)


def schema_for_type(tp: Any) -> dict[str, Any]:
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


def build_openapi_operation(
    view_func: Any,
    converters: Sequence[tuple[str, str]],
    methods: Sequence[str],
    *,
    operation_prefix: str,
    summary_fallback: str | None = None,
    ignored_params: frozenset[str] = frozenset({"request"}),
) -> dict[str, dict[str, Any]]:
    """Build OpenAPI 3.1.0 path-item operations map from a Python view callable."""
    try:
        hints = get_type_hints(view_func)
    except Exception:
        hints = {}
    try:
        sig = inspect.signature(view_func)
    except (TypeError, ValueError):
        sig = None

    path_param_names = {n for n, _ in converters}
    parameters: list[dict[str, Any]] = [
        {
            "name": p_name,
            "in": "path",
            "required": True,
            "schema": {"type": "integer" if conv == "int" else ("number" if conv == "float" else "string")},
        }
        for p_name, conv in converters
    ]

    request_body: dict[str, Any] | None = None
    return_ann: Any = inspect.Parameter.empty
    if sig is not None:
        for param_name, param in sig.parameters.items():
            if param_name in path_param_names or param_name in ignored_params:
                continue
            ann = hints.get(param_name, param.annotation)
            if ann is not inspect.Parameter.empty:
                request_body = {
                    "required": True,
                    "content": {"application/json": {"schema": schema_for_type(ann)}},
                }
        return_ann = hints.get("return", sig.return_annotation)

    response_schema = schema_for_type(return_ann) if return_ann is not inspect.Parameter.empty else {"type": "object"}
    doc = inspect.getdoc(view_func)
    result: dict[str, dict[str, Any]] = {}
    for raw_method in methods:
        method = str(raw_method).lower()
        if method in {"head", "options"}:
            continue
        op_id = f"{operation_prefix}_{method}"
        summary = (doc or summary_fallback or op_id).splitlines()[0]
        op: dict[str, Any] = {
            "operationId": op_id,
            "summary": summary,
            "responses": {
                "200": {
                    "description": "Successful Response",
                    "content": {"application/json": {"schema": response_schema}},
                }
            },
        }
        if parameters:
            op["parameters"] = parameters
        if request_body is not None and method in {"post", "put", "patch"}:
            op["requestBody"] = request_body
        result[method] = op
    return result
