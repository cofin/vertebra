"""Zero-copy C-API fastpath, @vertebra_view decorator, and MsgspecView for Django."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any, ClassVar, Self, cast, get_type_hints

from vertebra._vertebra import Request, Response, WsgiEnviron, WsgiInput
from vertebra.extensions.django.router import RouteCompiler
from vertebra.server._fastpath import (
    extract_scope_body_fast,
    resolve_relative_path,
    serialize_fastpath_special_result,
    wrap_asgi_fastpath,
)
from vertebra.server._fastpath_core import (
    CompiledRouteBase,
    coerce_path_param,
    resolve_wsgi_relative_path,
    step_coro_sync,
)
from vertebra.utils.serializers import decode_json, encode_json_bytes
from vertebra.utils.type_guards import (
    is_coroutine_callable,
    is_msgspec_struct_type,
    is_pydantic_model_type,
)

__all__ = (
    "CompiledRoute",
    "FastPath",
    "MsgspecView",
    "compile_fastpath_routes",
    "dispatch_django_direct",
    "vertebra_view",
)


class MsgspecView:
    """Base class for high-throughput Django class-based views using zero-copy msgspec/Pydantic DTOs."""

    http_method_names: ClassVar[tuple[str, ...]] = ("get", "post", "put", "patch", "delete")

    @classmethod
    def as_view(cls, **initkwargs: Any) -> Callable[..., Any]:
        """Return a fastpath-enabled view callable dispatching by HTTP method."""
        cached_instance = cls() if not initkwargs else None

        def _view(request: Request, *args: Any, **kwargs: Any) -> Any:
            self = cached_instance if cached_instance is not None else cls(**initkwargs)
            handler = getattr(self, request.method.lower(), None)
            if handler is None:
                return Response.error(405, int(request.request_id))
            return handler(request, *args, **kwargs)

        _view.__name__ = cls.__name__
        _view.__qualname__ = cls.__qualname__
        dyn_view = cast("Any", _view)
        dyn_view.__vertebra_fastpath__ = True
        dyn_view.__vertebra_view_class__ = cls
        methods = tuple(m.upper() for m in cls.http_method_names if hasattr(cls, m))
        dyn_view.__vertebra_methods__ = methods or ("GET",)
        dyn_view.__vertebra_is_async__ = any(
            is_coroutine_callable(getattr(cls, m, None)) for m in cls.http_method_names if hasattr(cls, m)
        )
        return _view


class CompiledRoute(CompiledRouteBase, frozen=True, gc=False, kw_only=True):
    """Pre-compiled Django fastpath execution plan for a single view."""

    name: str
    """Django URL pattern name."""

    pattern: str
    """Django URL pattern string."""

    callback: Callable[..., Any]
    """Underlying Django view callable."""

    body_param_name: str | None = None
    """Optional request body parameter name."""

    body_type: Any = None
    """Target request body annotation or ``None``."""

    body_is_msgspec: bool = False
    """Whether ``body_type`` is a ``msgspec.Struct`` subclass."""

    body_is_pydantic: bool = False
    """Whether ``body_type`` is a Pydantic ``BaseModel`` subclass."""

    accepts_request: bool = True
    """Whether the view expects ``Request`` as its first positional argument."""


class FastPath:
    """Fastpath route table and dispatcher for Django applications."""

    __slots__ = ("_compiler", "_mount_prefix", "_routes")

    def __init__(self, compiler: RouteCompiler, *, mount_prefix: str = "") -> None:
        self._compiler = compiler
        self._mount_prefix = mount_prefix
        self._routes = compile_fastpath_routes(compiler)

    @property
    def routes(self) -> dict[int, CompiledRoute]:
        """Return mapping from ``handler_id`` to ``CompiledRoute``."""
        return self._routes

    @classmethod
    def from_app(cls, app: Any, *, mount_prefix: str = "") -> Self:
        """Construct a bound ``FastPath`` directly from a Django handler or URL configuration."""
        urlconf = getattr(app, "urlconf", None)
        urlpatterns = getattr(app, "urlpatterns", None)
        compiler = RouteCompiler(urlpatterns=urlpatterns, urlconf=urlconf)
        return cls(compiler, mount_prefix=mount_prefix)

    def match(self, method: str, path: str) -> tuple[CompiledRoute, dict[str, str]] | None:
        """Match ``(method, path)`` in the router and return ``(CompiledRoute, params)``."""
        matched = self._compiler.router.match_route(method, path)
        if matched is None:
            return None
        handler_id, params = matched
        route = self._routes.get(handler_id)
        if route is None:
            return None
        return route, params

    def dispatch(self, request: Request) -> Response | None:
        """Dispatch ``request`` directly if it matches a compiled fastpath route."""
        matched = self.match(request.method, request.path)
        if matched is None:
            return None
        compiled, params = matched
        if not getattr(compiled.callback, "__vertebra_fastpath__", True):
            return None
        return dispatch_django_direct(compiled, request, params)

    async def dispatch_async(
        self,
        scope: Any,
        receive: Any,
        send: Any,
        *,
        body: bytes | None = None,
    ) -> tuple[int, list[tuple[bytes, bytes]], bytes] | None:
        """Dispatch an ASGI request directly through a compiled sync or async Django view."""
        _ = send
        scope_type = scope.get("type", "http") if hasattr(scope, "get") else scope["type"]
        if scope_type != "http":
            return None
        method = str(scope.get("method", "GET") if hasattr(scope, "get") else scope["method"]).upper()
        path = resolve_relative_path(scope, self._mount_prefix)
        matched = self.match(method, path)
        if matched is None:
            return None
        compiled, params = matched
        if not getattr(compiled.callback, "__vertebra_fastpath__", True):
            return None
        raw_body = await extract_scope_body_fast(scope, receive, body, method=method)
        req_obj = Request.from_parts(method=method, path=path, body=raw_body)
        resp = await _dispatch_django_async(compiled, req_obj, params)
        encoded_headers = [
            (
                k if isinstance(k, bytes) else str(k).encode("latin-1"),
                v if isinstance(v, bytes) else str(v).encode("latin-1"),
            )
            for k, v in resp.headers
        ]
        return int(resp.status), encoded_headers, bytes(resp.body)

    def dispatch_wsgi(
        self,
        environ: WsgiEnviron | dict[str, Any],
        start_response: Callable[..., Any],
    ) -> list[bytes] | None:
        """Attempt zero-copy fastpath dispatch for a PEP 3333 WSGI call."""
        method = str(environ.get("REQUEST_METHOD", "GET"))
        raw_path = resolve_wsgi_relative_path(environ, self._mount_prefix)
        matched = self.match(method, raw_path)
        if matched is None:
            return None
        compiled, params = matched
        if not getattr(compiled.callback, "__vertebra_fastpath__", True):
            return None
        resp = dispatch_django_direct(compiled, environ, params)
        if resp is None:
            return None
        raw_send = getattr(start_response, "send_raw_response", None)
        if callable(raw_send):
            return cast("list[bytes]", raw_send(int(resp.status), resp.headers, bytes(resp.body)))
        status_line = f"{resp.status} OK" if resp.status < 400 else f"{resp.status} ERROR"
        hdrs = [
            (
                k.decode("latin-1") if isinstance(k, bytes) else str(k),
                v.decode("latin-1") if isinstance(v, bytes) else str(v),
            )
            for k, v in resp.headers
        ]
        start_response(status_line, hdrs)
        return [bytes(resp.body)]

    def wrap_wsgi(self, wsgi_handler: Callable[..., Any] | None = None) -> Callable[..., Any]:
        """Wrap a Django WSGI callable so eligible routes execute via the zero-copy fastpath."""

        def _wrapped_wsgi(environ: WsgiEnviron | dict[str, Any], start_response: Callable[..., Any]) -> Any:
            chunks = self.dispatch_wsgi(environ, start_response)
            if chunks is not None:
                return chunks
            if callable(wsgi_handler):
                return wsgi_handler(environ, start_response)
            start_response("404 Not Found", [("content-type", "text/plain")])
            return [b"Not Found"]

        dyn_wsgi = cast("Any", _wrapped_wsgi)
        dyn_wsgi.__vertebra_fastpath__ = self
        dyn_wsgi.__vertebra_fastpath_attached__ = True
        return _wrapped_wsgi

    def wrap_asgi(self, fallback_app: Callable[..., Any] | None = None) -> Callable[..., Any]:
        """Wrap an ASGI callable so eligible sync and async Django views execute via the fastpath."""

        async def _default_fallback(_scope: Any, _receive: Any, send: Any) -> None:
            await send({"type": "http.response.start", "status": 404, "headers": [(b"content-type", b"text/plain")]})
            await send({"type": "http.response.body", "body": b"Not Found", "more_body": False})

        return wrap_asgi_fastpath(self, fallback_app or _default_fallback)


def vertebra_view(
    fn: Callable[..., Any] | None = None,
    *,
    methods: tuple[str, ...] | list[str] = ("GET", "POST", "PUT", "PATCH", "DELETE"),
    status_code: int = 200,
) -> Any:
    """Mark a Django view function as eligible for the zero-copy Vertebra fastpath."""

    def _decorate(target: Callable[..., Any]) -> Callable[..., Any]:
        dyn = cast("Any", target)
        dyn.__vertebra_fastpath__ = True
        dyn.__vertebra_methods__ = tuple(m.upper() for m in methods)
        dyn.__vertebra_status_code__ = status_code
        return target

    if fn is not None:
        return _decorate(fn)
    return _decorate


def compile_fastpath_routes(compiler: RouteCompiler) -> dict[int, CompiledRoute]:
    """Compile all Django routes in ``compiler`` into ``CompiledRoute`` plans."""
    compiled: dict[int, CompiledRoute] = {}
    for handler_id, entry in compiler.entries_by_id.items():
        compiled[handler_id] = _compile_single_callback(
            handler_id=handler_id,
            name=entry.name,
            pattern=entry.pattern,
            callback=entry.callback,
            param_converters=entry.param_converters,
            http_method=entry.methods[0] if entry.methods else "GET",
        )
    return compiled


def dispatch_django_direct(
    compiled: CompiledRoute,
    request_or_environ: Request | WsgiEnviron | dict[str, Any],
    path_params: dict[str, str],
) -> Response | None:
    """Execute a synchronous or non-suspending async Django view directly using zero-copy body decoding."""
    request_id, req_obj, kwargs = _prepare_django_kwargs(compiled, request_or_environ, path_params)
    if compiled.accepts_request and req_obj is not None:
        result = compiled.callback(req_obj, **kwargs)
    else:
        result = compiled.callback(**kwargs)
    if inspect.iscoroutine(result):
        completed, stepped_val = step_coro_sync(result)
        if not completed:
            return None
        result = stepped_val
    return _serialize_django_result(result, compiled.status_code, request_id)


def _compile_single_callback(
    handler_id: int,
    name: str,
    pattern: str,
    callback: Callable[..., Any],
    param_converters: tuple[tuple[str, str], ...],
    *,
    http_method: str = "GET",
) -> CompiledRoute:
    """Inspect a Django view callable and type annotations to build a ``CompiledRoute``."""
    view_cls = getattr(callback, "__vertebra_view_class__", None)
    target_fn: Callable[..., Any] = (
        getattr(view_cls, http_method.lower(), callback) if view_cls is not None else callback
    )
    is_async = (
        is_coroutine_callable(target_fn)
        if view_cls is not None
        else (is_coroutine_callable(target_fn) or bool(getattr(callback, "__vertebra_is_async__", False)))
    )
    status_code = int(
        getattr(target_fn, "__vertebra_status_code__", getattr(callback, "__vertebra_status_code__", 200))
    )
    sig = inspect.signature(target_fn)
    try:
        hints = get_type_hints(target_fn)
    except Exception:
        hints = {}

    path_param_names = {p_name for p_name, _ in param_converters}
    body_param_name: str | None = None
    body_type: Any = None
    body_is_msgspec = False
    body_is_pydantic = False
    accepts_request = view_cls is not None

    params = list(sig.parameters.values())
    for idx, param in enumerate(params):
        p_name = param.name
        if p_name == "self":
            continue
        ann = hints.get(p_name, param.annotation)
        if (
            p_name == "request"
            or ann is Request
            or (idx == 0 and p_name not in path_param_names and ann is inspect.Parameter.empty)
        ):
            accepts_request = True
            continue
        if p_name in path_param_names or param.kind in {
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        }:
            continue
        if is_msgspec_struct_type(ann):
            body_param_name = p_name
            body_type = ann
            body_is_msgspec = True
        elif is_pydantic_model_type(ann):
            body_param_name = p_name
            body_type = ann
            body_is_pydantic = True
        elif p_name in {"body", "data", "payload"} and body_param_name is None:
            body_param_name = p_name
            body_type = None if ann is inspect.Parameter.empty else ann
            body_is_msgspec = True

    return CompiledRoute(
        handler_id=handler_id,
        name=name,
        pattern=pattern,
        callback=callback,
        is_async=is_async,
        status_code=status_code,
        param_converters=param_converters,
        body_param_name=body_param_name,
        body_type=body_type,
        body_is_msgspec=body_is_msgspec,
        body_is_pydantic=body_is_pydantic,
        accepts_request=accepts_request,
    )


def _serialize_django_result(result: Any, default_status: int, request_id: int) -> Response:
    """Convert a Django view return value into a Rust ``Response``."""
    if isinstance(result, Response):
        return result

    special = serialize_fastpath_special_result(result, default_status=default_status)
    if special is not None:
        sp_status, sp_headers, sp_body = special
        str_headers = [(k.decode("latin-1"), v.decode("latin-1")) for k, v in sp_headers]
        return Response.from_buffer(sp_status, sp_body, str_headers, request_id)

    if hasattr(result, "status_code") and hasattr(result, "content"):
        status = int(result.status_code)
        raw_content = result.content
        body_bytes = raw_content if isinstance(raw_content, bytes) else str(raw_content).encode("utf-8")
        headers: list[tuple[str, str]] = []
        if hasattr(result, "headers") and hasattr(result.headers, "items"):
            headers = [(str(k).lower(), str(v)) for k, v in result.headers.items()]
        return Response(status=status, body=body_bytes, headers=headers, request_id=request_id)

    if isinstance(result, bytes):
        return Response(
            status=default_status,
            body=result,
            headers=[("content-type", "application/octet-stream")],
            request_id=request_id,
        )

    if isinstance(result, str):
        return Response(
            status=default_status,
            body=result.encode("utf-8"),
            headers=[("content-type", "text/html; charset=utf-8")],
            request_id=request_id,
        )

    if is_pydantic_model_type(type(result)):
        serializer = getattr(type(result), "__pydantic_serializer__", None)
        if serializer is not None:
            return Response(
                status=default_status,
                body=serializer.to_json(result),
                headers=[("content-type", "application/json")],
                request_id=request_id,
            )

    return Response(
        status=default_status,
        body=encode_json_bytes(result),
        headers=[("content-type", "application/json")],
        request_id=request_id,
    )


def _prepare_django_kwargs(
    compiled: CompiledRoute,
    request_or_environ: Request | WsgiEnviron | dict[str, Any],
    path_params: dict[str, str],
) -> tuple[int, Request | None, dict[str, Any]]:
    """Build keyword arguments and Request handle for a compiled Django view."""
    request_id = int(getattr(request_or_environ, "request_id", 0))
    kwargs: dict[str, Any] = {}
    converter_map = dict(compiled.param_converters)
    for k, v in path_params.items():
        kwargs[k] = coerce_path_param(v, converter_map.get(k, "str"))

    req_obj: Request | None = None
    if isinstance(request_or_environ, Request):
        req_obj = request_or_environ
    elif isinstance(request_or_environ, WsgiEnviron):
        req_obj = request_or_environ.request()

    if compiled.body_param_name is not None:
        if req_obj is not None:
            if compiled.body_is_pydantic and compiled.body_type is not None:
                kwargs[compiled.body_param_name] = req_obj.decode_pydantic_json(compiled.body_type)
            else:
                kwargs[compiled.body_param_name] = req_obj.decode_msgspec(compiled.body_type)
        elif isinstance(request_or_environ, dict):
            wsgi_inp = request_or_environ.get("wsgi.input")
            if isinstance(wsgi_inp, WsgiInput):
                if compiled.body_is_pydantic and compiled.body_type is not None:
                    kwargs[compiled.body_param_name] = wsgi_inp.decode_pydantic_json(compiled.body_type)
                else:
                    kwargs[compiled.body_param_name] = wsgi_inp.decode_msgspec(compiled.body_type)
            elif wsgi_inp is not None and hasattr(wsgi_inp, "read"):
                raw = wsgi_inp.read()
                if compiled.body_type is not None:
                    kwargs[compiled.body_param_name] = decode_json(raw, type=compiled.body_type)
                else:
                    kwargs[compiled.body_param_name] = decode_json(raw)
    return request_id, req_obj, kwargs


async def _dispatch_django_async(
    compiled: CompiledRoute,
    request_obj: Request,
    path_params: dict[str, str],
) -> Response:
    """Execute a synchronous or async Django view within an ASGI event loop."""
    request_id, req_obj, kwargs = _prepare_django_kwargs(compiled, request_obj, path_params)
    if compiled.accepts_request and req_obj is not None:
        result = compiled.callback(req_obj, **kwargs)
    else:
        result = compiled.callback(**kwargs)
    if inspect.isawaitable(result):
        result = await result
    return _serialize_django_result(result, compiled.status_code, request_id)
