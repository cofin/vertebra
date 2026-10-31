"""Routing primitives, Controller class, and Router composition for the native app framework."""

import functools
import inspect
import re
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, ClassVar, cast
from uuid import UUID

import msgspec

from vertebra.framework.di import Inject, Provide, inspect_callable_bindings
from vertebra.framework.procedures import Procedure

try:
    import vertebra._vertebra as _vtb

    _native_handler_id: Callable[[str, str], int] | None = getattr(_vtb, "handler_id", None)
except ImportError:
    _native_handler_id = None

__all__ = (
    "Controller",
    "PARAM_CONVERTERS",
    "ParamSpec",
    "Route",
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
    """Parse an ISO-8601 duration or float seconds string into a timedelta."""
    match = _ISO_DURATION_RE.match(raw)
    if match and any(match.groupdict().values()):
        days = int(match.group("days") or 0)
        hours = int(match.group("hours") or 0)
        minutes = int(match.group("minutes") or 0)
        seconds = float(match.group("seconds") or 0.0)
        return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
    return timedelta(seconds=float(raw))


def _parse_path_param(raw: str) -> str:
    """Normalize a greedy path parameter by stripping any leading slash."""
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
    """Compiled path template parameter descriptor."""

    name: str
    param_type: str


class Route(msgspec.Struct, frozen=True, gc=False):
    """HTTP route definition binding method, path, handler, dependencies, guards, and metadata."""

    method: str
    path: str
    handler: Callable[..., Any]
    segments: tuple[str | ParamSpec, ...] = ()
    sync_to_thread: bool = False
    dependencies: dict[str, Any] = msgspec.field(default_factory=dict)
    guards: tuple[Callable[..., Any], ...] = ()
    tags: tuple[str, ...] = ()
    status_code: int = 200

    def __post_init__(self) -> None:
        """Normalize method and path and compile path segments if not pre-supplied."""
        norm_method = self.method.upper()
        if norm_method != self.method:
            msgspec.structs.force_setattr(self, "method", norm_method)
        norm_path = _normalize_path(self.path)
        if norm_path != self.path:
            msgspec.structs.force_setattr(self, "path", norm_path)
        if not self.segments and norm_path != "/":
            msgspec.structs.force_setattr(self, "segments", _compile_path_template(norm_path))

    def with_prefix(self, prefix: str) -> "Route":
        """Return a copy of this Route with a prepended URL prefix."""
        return self.with_context(prefix=prefix)

    def with_context(
        self,
        *,
        prefix: str = "",
        handler: Callable[..., Any] | None = None,
        dependencies: Mapping[str, Any] | None = None,
        guards: Sequence[Callable[..., Any]] | None = None,
        tags: Sequence[str] = (),
        sync_to_thread: bool | None = None,
    ) -> "Route":
        """Return a copy of this Route merging parent prefix, dependencies, guards, and tags."""
        normalized_prefix = _normalize_prefix(prefix)
        normalized_path = _normalize_path(self.path)
        if normalized_prefix:
            new_path = normalized_prefix if normalized_path == "/" else f"{normalized_prefix}{normalized_path}"
        else:
            new_path = normalized_path

        merged_deps: dict[str, Any] = dict(dependencies) if dependencies else {}
        merged_deps.update(self.dependencies)
        merged_guards = (*tuple(guards or ()), *self.guards)
        merged_tags = tuple(dict.fromkeys((*tags, *self.tags)))
        resolved_sync = self.sync_to_thread or bool(sync_to_thread)

        return Route(
            method=self.method,
            path=new_path,
            handler=handler if handler is not None else self.handler,
            segments=_compile_path_template(new_path),
            sync_to_thread=resolved_sync,
            dependencies=merged_deps,
            guards=merged_guards,
            tags=merged_tags,
            status_code=self.status_code,
        )


class Controller:
    """Declarative class grouping route and procedure methods under shared path, dependencies, and guards."""

    path: ClassVar[str] = ""
    dependencies: ClassVar[Mapping[str, Any]] = {}
    guards: ClassVar[Sequence[Callable[..., Any]]] = ()
    tags: ClassVar[Sequence[str]] = ()
    sync_to_thread: ClassVar[bool] = False

    @classmethod
    def get_routes(cls, instance: "Controller | None" = None) -> tuple[Route, ...]:
        """Collect and bind all HTTP Route definitions declared on this Controller."""
        resolved_instance, ctrl_deps = cls._resolve_binding_mode(instance)
        collected: list[Route] = []
        for base in reversed(cls.__mro__):
            if base is object or base is Controller:
                continue
            for attr_name, raw_val in vars(base).items():
                if attr_name.startswith("_"):
                    continue
                route_defs: Sequence[Route] = getattr(raw_val, "__vertebra_routes__", ())
                if not route_defs:
                    continue
                if resolved_instance is not None:
                    bound_fn: Callable[..., Any] = getattr(resolved_instance, attr_name)
                else:
                    ctrl_key = f"__controller_{cls.__qualname__}__"
                    bound_fn = _bind_controller_method(raw_val, cls, ctrl_key)
                for route in route_defs:
                    collected.append(
                        route.with_context(
                            prefix=cls.path,
                            handler=bound_fn,
                            dependencies=ctrl_deps,
                            guards=cls.guards,
                            tags=cls.tags,
                            sync_to_thread=cls.sync_to_thread,
                        )
                    )
        return tuple(collected)

    @classmethod
    def get_procedures(cls, instance: "Controller | None" = None) -> tuple[Procedure, ...]:
        """Collect and bind all RPC Procedure definitions declared on this Controller."""
        resolved_instance, ctrl_deps = cls._resolve_binding_mode(instance)
        collected: list[Procedure] = []
        for base in reversed(cls.__mro__):
            if base is object or base is Controller:
                continue
            for attr_name, raw_val in vars(base).items():
                if attr_name.startswith("_") or not isinstance(raw_val, Procedure):
                    continue
                if resolved_instance is not None:
                    bound_fn = raw_val.fn.__get__(resolved_instance, cls)
                else:
                    ctrl_key = f"__controller_{cls.__qualname__}__"
                    bound_fn = _bind_controller_method(raw_val.fn, cls, ctrl_key)
                collected.append(
                    raw_val.with_context(
                        fn=bound_fn,
                        dependencies=ctrl_deps,
                        guards=cls.guards,
                    )
                )
        return tuple(collected)

    @classmethod
    def _resolve_binding_mode(cls, instance: "Controller | None") -> tuple["Controller | None", dict[str, Any]]:
        """Determine whether to bind to a singleton Controller instance or inject the Controller per request."""
        merged_deps = dict(cls.dependencies) if cls.dependencies else {}
        if instance is not None:
            return instance, merged_deps
        init_bindings = inspect_callable_bindings(cls)
        if all(b.has_default for b in init_bindings):
            return cls(), merged_deps
        ctrl_key = f"__controller_{cls.__qualname__}__"
        merged_deps[ctrl_key] = Provide(cls, scope="request")
        return None, merged_deps


class Router:
    """Group route handlers, Controllers, and sub-routers under a shared prefix, dependencies, and guards."""

    __slots__ = ("dependencies", "guards", "prefix", "procedures", "routes", "tags")

    def __init__(
        self,
        prefix: str = "",
        *,
        route_handlers: Sequence[Any] = (),
        dependencies: Mapping[str, Any] | None = None,
        guards: Sequence[Callable[..., Any]] | None = None,
        tags: Sequence[str] = (),
    ) -> None:
        """Initialize a Router with optional prefix, route_handlers, dependencies, guards, and tags."""
        self.prefix = _normalize_prefix(prefix)
        self.dependencies: dict[str, Any] = dict(dependencies) if dependencies else {}
        self.guards: tuple[Callable[..., Any], ...] = tuple(guards) if guards else ()
        self.tags: tuple[str, ...] = tuple(tags)
        self.routes: list[Route] = []
        self.procedures: list[Procedure] = []
        for item in route_handlers:
            self.register(item)

    def register(self, target: Any) -> Any:
        """Register a Route, decorated handler, Controller (class or instance), Procedure, or nested Router."""
        if isinstance(target, Route):
            self.routes.append(
                target.with_context(
                    prefix=self.prefix,
                    dependencies=self.dependencies,
                    guards=self.guards,
                    tags=self.tags,
                )
            )
            return target

        if isinstance(target, Router):
            for route in target.routes:
                self.routes.append(
                    route.with_context(
                        prefix=self.prefix,
                        dependencies=self.dependencies,
                        guards=self.guards,
                        tags=self.tags,
                    )
                )
            for proc in target.procedures:
                self.procedures.append(proc.with_context(dependencies=self.dependencies, guards=self.guards))
            return target

        if isinstance(target, Procedure):
            self.procedures.append(target.with_context(dependencies=self.dependencies, guards=self.guards))
            return target

        if isinstance(target, Controller) or (isinstance(target, type) and issubclass(target, Controller)):
            ctrl_cls = target if isinstance(target, type) else type(target)
            ctrl_inst = None if isinstance(target, type) else target
            for route in ctrl_cls.get_routes(ctrl_inst):
                self.routes.append(
                    route.with_context(
                        prefix=self.prefix,
                        dependencies=self.dependencies,
                        guards=self.guards,
                        tags=self.tags,
                    )
                )
            for proc in ctrl_cls.get_procedures(ctrl_inst):
                self.procedures.append(proc.with_context(dependencies=self.dependencies, guards=self.guards))
            return target

        routes: Sequence[Route] = getattr(target, "__vertebra_routes__", ())
        for route in routes:
            self.routes.append(
                route.with_context(
                    prefix=self.prefix,
                    dependencies=self.dependencies,
                    guards=self.guards,
                    tags=self.tags,
                )
            )
        return target


def get(
    path: str = "/",
    *,
    sync_to_thread: bool = False,
    dependencies: Mapping[str, Any] | None = None,
    guards: Sequence[Callable[..., Any]] | None = None,
    tags: Sequence[str] = (),
    status_code: int = 200,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorate a function or Controller method as an HTTP GET route."""
    return _route(
        "GET",
        path,
        sync_to_thread=sync_to_thread,
        dependencies=dependencies,
        guards=guards,
        tags=tags,
        status_code=status_code,
    )


def post(
    path: str = "/",
    *,
    sync_to_thread: bool = False,
    dependencies: Mapping[str, Any] | None = None,
    guards: Sequence[Callable[..., Any]] | None = None,
    tags: Sequence[str] = (),
    status_code: int = 200,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorate a function or Controller method as an HTTP POST route."""
    return _route(
        "POST",
        path,
        sync_to_thread=sync_to_thread,
        dependencies=dependencies,
        guards=guards,
        tags=tags,
        status_code=status_code,
    )


def put(
    path: str = "/",
    *,
    sync_to_thread: bool = False,
    dependencies: Mapping[str, Any] | None = None,
    guards: Sequence[Callable[..., Any]] | None = None,
    tags: Sequence[str] = (),
    status_code: int = 200,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorate a function or Controller method as an HTTP PUT route."""
    return _route(
        "PUT",
        path,
        sync_to_thread=sync_to_thread,
        dependencies=dependencies,
        guards=guards,
        tags=tags,
        status_code=status_code,
    )


def delete(
    path: str = "/",
    *,
    sync_to_thread: bool = False,
    dependencies: Mapping[str, Any] | None = None,
    guards: Sequence[Callable[..., Any]] | None = None,
    tags: Sequence[str] = (),
    status_code: int = 200,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorate a function or Controller method as an HTTP DELETE route."""
    return _route(
        "DELETE",
        path,
        sync_to_thread=sync_to_thread,
        dependencies=dependencies,
        guards=guards,
        tags=tags,
        status_code=status_code,
    )


def patch(
    path: str = "/",
    *,
    sync_to_thread: bool = False,
    dependencies: Mapping[str, Any] | None = None,
    guards: Sequence[Callable[..., Any]] | None = None,
    tags: Sequence[str] = (),
    status_code: int = 200,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorate a function or Controller method as an HTTP PATCH route."""
    return _route(
        "PATCH",
        path,
        sync_to_thread=sync_to_thread,
        dependencies=dependencies,
        guards=guards,
        tags=tags,
        status_code=status_code,
    )


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
    dependencies: Mapping[str, Any] | None = None,
    guards: Sequence[Callable[..., Any]] | None = None,
    tags: Sequence[str] = (),
    status_code: int = 200,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Construct a route decorator attaching Route metadata to the target callable."""
    normalized_path = _normalize_path(path)
    segments = _compile_path_template(normalized_path)

    def decorator(handler: Callable[..., Any]) -> Callable[..., Any]:
        routes: list[Route] = list(getattr(handler, "__vertebra_routes__", ()))
        route = Route(
            method=method,
            path=normalized_path,
            handler=handler,
            segments=segments,
            sync_to_thread=sync_to_thread,
            dependencies=dict(dependencies) if dependencies else {},
            guards=tuple(guards) if guards else (),
            tags=tuple(tags),
            status_code=status_code,
        )
        routes.append(route)
        target = cast("Any", handler)
        target.__vertebra_routes__ = routes
        if sync_to_thread:
            target.__vertebra_sync_to_thread__ = True
        return handler

    return decorator


def _bind_controller_method(
    raw_fn: Callable[..., Any],
    ctrl_cls: type[Controller],
    ctrl_dep_key: str,
) -> Callable[..., Any]:
    """Wrap an unbound Controller method so self is resolved via DI per request."""
    sig = inspect.signature(raw_fn)
    params = list(sig.parameters.values())
    ctrl_ann: Any = Inject.__class_getitem__((ctrl_cls, ctrl_dep_key))
    if params and params[0].name == "self":
        new_self_param = params[0].replace(
            name="_controller_self",
            annotation=ctrl_ann,
        )
        new_params = [new_self_param, *params[1:]]
    else:
        new_params = params

    new_sig = sig.replace(parameters=new_params)
    raw_annotations = dict(getattr(raw_fn, "__annotations__", {}) or {})
    raw_annotations.pop("self", None)
    raw_annotations["_controller_self"] = ctrl_ann

    if inspect.iscoroutinefunction(raw_fn):

        @functools.wraps(raw_fn)
        async def _async_bound(_controller_self: Any, *args: Any, **kwargs: Any) -> Any:
            return await raw_fn(_controller_self, *args, **kwargs)

        cast("Any", _async_bound).__signature__ = new_sig
        _async_bound.__annotations__ = raw_annotations
        return _async_bound

    @functools.wraps(raw_fn)
    def _sync_bound(_controller_self: Any, *args: Any, **kwargs: Any) -> Any:
        return raw_fn(_controller_self, *args, **kwargs)

    cast("Any", _sync_bound).__signature__ = new_sig
    _sync_bound.__annotations__ = raw_annotations
    return _sync_bound


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
    if len(path) > 1 and path.endswith("/"):
        return path.rstrip("/")
    return path


_normalize_path = normalize_path


def _split_path(path: str) -> "list[str]":
    """Split a URL path into non-empty path segments."""
    if not path or path == "/":
        return []
    return [segment for segment in path.strip("/").split("/") if segment]


def _compile_path_template(path: str) -> "tuple[str | ParamSpec, ...]":
    """Compile a URL path template into static segments and ParamSpec items."""
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
    """Parse a '{name:type}' segment into (name, param_type) or None if static."""
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
    """Extract and coerce typed path parameters from compiled path segments."""
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
    """Coerce a raw string path segment into the declared param_type."""
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
    """Parse a boolean path or query parameter value."""
    value = value.strip().lower()
    if value in {"true", "1", "yes", "y", "t"}:
        return True
    if value in {"false", "0", "no", "n", "f"}:
        return False
    msg = "invalid boolean value"
    raise ValueError(msg)
