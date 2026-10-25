"""Routing primitives for the native app framework."""

import re
from collections.abc import Callable, Sequence
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, cast
from uuid import UUID

import msgspec

try:
    import vertebra._vertebra as _vtb

    _native_handler_id: Callable[[str, str], int] | None = getattr(_vtb, "handler_id", None)
except ImportError:
    _native_handler_id = None

__all__ = (
    "PARAM_CONVERTERS",
    "ParamSpec",
    "RouteDef",
    "Router",
    "delete",
    "extract_path_params",
    "get",
    "handler_id",
    "normalize_path",
    "normalize_prefix",
    "patch",
    "post",
    "put",
)

_SUPPORTED_PARAM_TYPES = {"str", "string", "int", "i64", "u64", "float", "f64", "bool", "uuid", "path"}
_FNV_OFFSET = 0xCBF29CE484222325
_FNV_PRIME = 0x100000001B3
_MASK_64 = 0xFFFFFFFFFFFFFFFF
_ISO_DURATION_RE = re.compile(
    r"^P(?:(?P<days>\d+)D)?(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+(?:\.\d+)?)S)?)?$"
)


def _parse_timedelta(raw: str) -> timedelta:
    match = _ISO_DURATION_RE.match(raw)
    if match and any(match.groupdict().values()):
        days = int(match.group("days") or 0)
        hours = int(match.group("hours") or 0)
        minutes = int(match.group("minutes") or 0)
        seconds = float(match.group("seconds") or 0.0)
        return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
    return timedelta(seconds=float(raw))


def _parse_path_param(raw: str) -> str:
    return raw.lstrip("/")


PARAM_CONVERTERS: dict[str, Callable[[str], Any]] = {
    "str": str,
    "string": str,
    "int": int,
    "float": float,
    "uuid": UUID,
    "decimal": Decimal,
    "date": date.fromisoformat,
    "datetime": datetime.fromisoformat,
    "time": time.fromisoformat,
    "timedelta": _parse_timedelta,
    "path": _parse_path_param,
}
_PARAM_CONVERTERS = PARAM_CONVERTERS


class ParamSpec(msgspec.Struct, frozen=True, gc=False, cache_hash=True):
    name: str
    param_type: str


class RouteDef(msgspec.Struct, frozen=True, gc=False):
    method: str
    path: str
    handler: Callable[..., Any]
    segments: tuple[str | ParamSpec, ...] = ()
    sync_to_thread: bool = False

    def with_prefix(self, prefix: str) -> "RouteDef":
        normalized_prefix = _normalize_prefix(prefix)
        normalized_path = _normalize_path(self.path)
        if not normalized_prefix:
            return self
        new_path = f"{normalized_prefix}{normalized_path}"
        return RouteDef(
            method=self.method,
            path=new_path,
            handler=self.handler,
            segments=_compile_path_template(new_path),
            sync_to_thread=self.sync_to_thread,
        )


class Router:
    """Group route handlers under a shared prefix."""

    __slots__ = ("prefix", "routes")

    def __init__(self, prefix: str = "") -> None:
        self.prefix = _normalize_prefix(prefix)
        self.routes: list[RouteDef] = []

    def register(self, handler: Callable[..., Any]) -> None:
        routes: Sequence[RouteDef] = getattr(handler, "__vertebra_routes__", ())
        for route in routes:
            self.routes.append(route.with_prefix(self.prefix))


def get(path: str, *, sync_to_thread: bool = False) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    return _route("GET", path, sync_to_thread=sync_to_thread)


def post(path: str, *, sync_to_thread: bool = False) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    return _route("POST", path, sync_to_thread=sync_to_thread)


def put(path: str, *, sync_to_thread: bool = False) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    return _route("PUT", path, sync_to_thread=sync_to_thread)


def delete(path: str, *, sync_to_thread: bool = False) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    return _route("DELETE", path, sync_to_thread=sync_to_thread)


def patch(path: str, *, sync_to_thread: bool = False) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    return _route("PATCH", path, sync_to_thread=sync_to_thread)


def handler_id(method: str, path: str) -> int:
    """Compute the stable uint64 handler ID for a method+path pair.

    Uses FNV-1a matching the Rust implementation in route_table.rs.

    Args:
        method: HTTP method, e.g. "GET". Case-insensitive.
        path: URL path. Will be normalized to start with '/'.

    Returns:
        Stable uint64 integer.
    """
    return _make_handler_id(method, _normalize_path(path))


def _fnv1a_write(h: int, data: bytes) -> int:
    """Update FNV-1a hash with each byte of ``data``."""
    for b in data:
        h ^= b
        h = (h * _FNV_PRIME) & _MASK_64
    return h


def _make_handler_id(method: str, path: str) -> int:
    """Derive a stable uint64 handler ID from method and path using FNV-1a."""
    if _native_handler_id is not None:
        return int(_native_handler_id(method, path))
    h = _FNV_OFFSET
    h = _fnv1a_write(h, method.upper().encode())
    h = _fnv1a_write(h, b"\xff")
    h = _fnv1a_write(h, b" ")
    h = _fnv1a_write(h, path.encode())
    h = _fnv1a_write(h, b"\xff")
    return h


def extract_path_params(template: str, path: str) -> dict[str, Any]:
    """Extract and coerce params from a path based on a typed template."""
    segments = _compile_path_template(_normalize_path(template))
    return _extract_params_from_segments(segments, _normalize_path(path))


def _route(
    method: str,
    path: str,
    *,
    sync_to_thread: bool = False,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    normalized_path = _normalize_path(path)
    segments = _compile_path_template(normalized_path)

    def decorator(handler: Callable[..., Any]) -> Callable[..., Any]:
        routes: list[RouteDef] = getattr(handler, "__vertebra_routes__", [])
        route = RouteDef(
            method=method,
            path=normalized_path,
            handler=handler,
            segments=segments,
            sync_to_thread=sync_to_thread,
        )
        routes.append(route)
        target = cast("Any", handler)
        target.__vertebra_routes__ = routes
        if sync_to_thread:
            target.__vertebra_sync_to_thread__ = True
        return handler

    return decorator


def normalize_prefix(prefix: str) -> str:
    """Normalize a router prefix to start with '/' and omit trailing '/'."""
    if not prefix:
        return ""
    prefix = prefix.strip()
    if prefix == "/":
        return ""
    if not prefix.startswith("/"):
        prefix = f"/{prefix}"
    return prefix.rstrip("/")


_normalize_prefix = normalize_prefix


def normalize_path(path: str) -> str:
    """Normalize a route path to start with '/'."""
    if not path:
        return "/"
    if not path.startswith("/"):
        return f"/{path}"
    return path


_normalize_path = normalize_path


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
