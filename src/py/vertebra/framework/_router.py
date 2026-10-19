"""Routing primitives for the native app framework."""

from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

import msgspec

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

__all__ = (
    "ParamSpec",
    "RouteDef",
    "Router",
    "delete",
    "extract_path_params",
    "get",
    "handler_id",
    "patch",
    "post",
    "put",
)

_SUPPORTED_PARAM_TYPES = {"str", "string", "int", "i64", "u64", "float", "f64", "bool", "uuid", "path"}


class ParamSpec(msgspec.Struct, frozen=True, array_like=True):
    name: str
    param_type: str


class RouteDef(msgspec.Struct, frozen=True, array_like=True):
    method: str
    path: str
    handler: "Callable[..., Any]"
    segments: "tuple[str | ParamSpec, ...]" = ()

    def with_prefix(self, prefix: str) -> "RouteDef":
        normalized_prefix = _normalize_prefix(prefix)
        normalized_path = _normalize_path(self.path)
        if not normalized_prefix:
            return self
        new_path = f"{normalized_prefix}{normalized_path}"
        return RouteDef(
            method=self.method, path=new_path, handler=self.handler, segments=_compile_path_template(new_path)
        )


class Router:
    """Group route handlers under a shared prefix."""

    __slots__ = ("prefix", "routes")

    def __init__(self, prefix: str = "") -> None:
        self.prefix = _normalize_prefix(prefix)
        self.routes: list[RouteDef] = []

    def register(self, handler: "Callable[..., Any]") -> None:
        routes: Sequence[RouteDef] = getattr(handler, "__vertebra_routes__", ())
        for route in routes:
            self.routes.append(route.with_prefix(self.prefix))


def get(path: str) -> "Callable[[Callable[..., Any]], Callable[..., Any]]":
    return _route("GET", path)


def post(path: str) -> "Callable[[Callable[..., Any]], Callable[..., Any]]":
    return _route("POST", path)


def put(path: str) -> "Callable[[Callable[..., Any]], Callable[..., Any]]":
    return _route("PUT", path)


def delete(path: str) -> "Callable[[Callable[..., Any]], Callable[..., Any]]":
    return _route("DELETE", path)


def patch(path: str) -> "Callable[[Callable[..., Any]], Callable[..., Any]]":
    return _route("PATCH", path)


def handler_id(method: str, path: str) -> int:
    """Compute the stable uint64 handler ID for a method+path pair.

    Uses FNV-1a matching the Rust implementation in route_table.rs.

    Args:
        method: HTTP method, e.g. "GET". Case-insensitive.
        path: URL path. Will be normalized to start with '/'.

    Returns:
        Stable uint64 integer.
    """
    from vertebra.framework._app import _make_handler_id

    return _make_handler_id(method, _normalize_path(path))


def extract_path_params(template: str, path: str) -> "dict[str, Any]":
    """Extract and coerce params from a path based on a typed template."""
    segments = _compile_path_template(_normalize_path(template))
    return _extract_params_from_segments(segments, _normalize_path(path))


def _route(method: str, path: str) -> "Callable[[Callable[..., Any]], Callable[..., Any]]":
    normalized_path = _normalize_path(path)
    segments = _compile_path_template(normalized_path)

    def decorator(handler: "Callable[..., Any]") -> "Callable[..., Any]":
        routes: list[RouteDef] = getattr(handler, "__vertebra_routes__", [])
        route = RouteDef(method=method, path=normalized_path, handler=handler, segments=segments)
        routes.append(route)
        cast("Any", handler).__vertebra_routes__ = routes
        return handler

    return decorator


def _normalize_prefix(prefix: str) -> str:
    if not prefix:
        return ""
    prefix = prefix.strip()
    if prefix == "/":
        return ""
    if not prefix.startswith("/"):
        prefix = f"/{prefix}"
    return prefix.rstrip("/")


def _normalize_path(path: str) -> str:
    if not path:
        return "/"
    if not path.startswith("/"):
        return f"/{path}"
    return path


def _split_path(path: str) -> "list[str]":
    if not path or path == "/":
        return []
    return [segment for segment in path.strip("/").split("/") if segment]


def _compile_path_template(path: str) -> "tuple[str | ParamSpec, ...]":
    template_segments = _split_path(path)
    compiled: list[str | ParamSpec] = []

    for index, segment in enumerate(template_segments):
        param = _parse_param_segment(segment)
        if param is None:
            compiled.append(segment)
            continue

        name, param_type = param
        param_type = param_type.lower()
        if param_type not in _SUPPORTED_PARAM_TYPES:
            msg = f"unsupported param type: {param_type}"
            raise ValueError(msg)
        if param_type == "path" and index != len(template_segments) - 1:
            msg = "path parameters must be the final segment"
            raise ValueError(msg)
        compiled.append(ParamSpec(name=name, param_type=param_type))

    return tuple(compiled)


def _parse_param_segment(segment: str) -> "tuple[str, str] | None":
    if not (segment.startswith("{") and segment.endswith("}")):
        return None

    inner = segment[1:-1].strip()
    if not inner:
        msg = "empty parameter segment"
        raise ValueError(msg)

    if ":" in inner:
        name, param_type = inner.split(":", 1)
        name = name.strip()
        param_type = param_type.strip()
    else:
        name, param_type = inner, "str"

    if not name:
        msg = "empty parameter name"
        raise ValueError(msg)

    return name, (param_type or "str")


def _extract_params_from_segments(segments: "tuple[str | ParamSpec, ...]", path: str) -> "dict[str, Any]":
    path_segments = _split_path(path)
    params: dict[str, Any] = {}
    seg_index = 0

    for segment in segments:
        if isinstance(segment, str):
            if seg_index >= len(path_segments) or path_segments[seg_index] != segment:
                msg = "path does not match template"
                raise ValueError(msg)
            seg_index += 1
            continue

        if segment.param_type == "path":
            remainder = "/".join(path_segments[seg_index:])
            if remainder == "" and seg_index < len(path_segments):
                msg = "path does not match template"
                raise ValueError(msg)
            params[segment.name] = remainder
            return params

        if seg_index >= len(path_segments):
            msg = "path does not match template"
            raise ValueError(msg)

        params[segment.name] = _coerce_param(segment.param_type, path_segments[seg_index])
        seg_index += 1

    if seg_index != len(path_segments):
        msg = "path does not match template"
        raise ValueError(msg)

    return params


def _coerce_param(param_type: str, value: str) -> Any:
    if param_type in {"str", "string", "path"}:
        return value
    if param_type in {"int", "i64", "u64"}:
        return int(value)
    if param_type in {"float", "f64"}:
        return float(value)
    if param_type == "bool":
        return _parse_bool(value)
    if param_type == "uuid":
        return UUID(value)
    msg = f"unsupported param type: {param_type}"
    raise ValueError(msg)


def _parse_bool(value: str) -> bool:
    value = value.strip().lower()
    if value in {"true", "1", "yes", "y", "t"}:
        return True
    if value in {"false", "0", "no", "n", "f"}:
        return False
    msg = "invalid boolean value"
    raise ValueError(msg)
