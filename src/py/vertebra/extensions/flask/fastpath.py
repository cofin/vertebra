"""Zero-copy C-API fastpath dispatcher for Flask view functions."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any, Self, cast, get_type_hints

from vertebra._vertebra import Request, Response, WsgiEnviron, WsgiInput
from vertebra.extensions.flask.router import RouteCompiler
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
    "compile_fastpath_routes",
    "dispatch_wsgi_direct",
    "vertebra_route",
)


class CompiledRoute(CompiledRouteBase, frozen=True, gc=False, kw_only=True):
    """Pre-compiled Flask fastpath execution plan for a single view function."""

    endpoint: str
    """Flask endpoint name."""

    rule: str
    """Werkzeug URL rule string."""

    view_func: Callable[..., Any]
    """Underlying Flask view callable."""

    body_param_name: str | None = None
    """Optional request body parameter name."""

    body_type: Any = None
    """Target request body annotation or ``None``."""

    body_is_msgspec: bool = False
    """Whether ``body_type`` is a ``msgspec.Struct`` subclass."""

    body_is_pydantic: bool = False
    """Whether ``body_type`` is a Pydantic ``BaseModel`` subclass."""

    accepts_request: bool = False
    """Whether the view accepts a native ``Request`` handle."""


class FastPath:
    """Fastpath route table and dispatcher for Flask applications."""

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
        """Construct a bound ``FastPath`` directly from a Flask application."""
        return cls(RouteCompiler(app), mount_prefix=mount_prefix)

    def match(self, method: str, path: str) -> tuple[CompiledRoute, dict[str, str]] | None:
        """Match ``(method, path)`` in the Rust router and return ``(CompiledRoute, params)``."""
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
        if not getattr(compiled.view_func, "__vertebra_fastpath__", True):
            return None
        return dispatch_wsgi_direct(compiled, request, params)

    async def dispatch_async(
        self,
        scope: Any,
        receive: Any,
        send: Any,
        *,
        body: bytes | None = None,
    ) -> tuple[int, list[tuple[bytes, bytes]], bytes] | None:
        """Dispatch an ASGI request directly through a compiled sync or async Flask view."""
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
        if not getattr(compiled.view_func, "__vertebra_fastpath__", True):
            return None
        raw_body = await extract_scope_body_fast(scope, receive, body, method=method)
        req_obj = Request.from_parts(method=method, path=path, body=raw_body)
        resp = await _dispatch_flask_async(compiled, req_obj, params)
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
        if not getattr(compiled.view_func, "__vertebra_fastpath__", True):
            return None
        resp = dispatch_wsgi_direct(compiled, environ, params)
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

    def wrap_wsgi(self, wsgi_app: Callable[..., Any] | None = None) -> Callable[..., Any]:
        """Wrap a WSGI callable so eligible routes execute via the zero-copy fastpath."""
        target = wsgi_app or getattr(getattr(self._compiler, "_app", None), "wsgi_app", None)

        def _wrapped_wsgi(environ: WsgiEnviron | dict[str, Any], start_response: Callable[..., Any]) -> Any:
            chunks = self.dispatch_wsgi(environ, start_response)
            if chunks is not None:
                return chunks
            if callable(target):
                return target(environ, start_response)
            start_response("404 Not Found", [("content-type", "text/plain")])
            return [b"Not Found"]

        dyn_wsgi = cast("Any", _wrapped_wsgi)
        dyn_wsgi.__vertebra_fastpath__ = self
        dyn_wsgi.__vertebra_fastpath_attached__ = True
        return _wrapped_wsgi

    def wrap_asgi(self, fallback_app: Callable[..., Any] | None = None) -> Callable[..., Any]:
        """Wrap an ASGI callable so eligible sync and async Flask views execute via the fastpath."""

        async def _default_fallback(_scope: Any, _receive: Any, send: Any) -> None:
            await send({"type": "http.response.start", "status": 404, "headers": [(b"content-type", b"text/plain")]})
            await send({"type": "http.response.body", "body": b"Not Found", "more_body": False})

        return wrap_asgi_fastpath(self, fallback_app or _default_fallback)


def vertebra_route(
    fn: Callable[..., Any] | None = None,
    *,
    status_code: int = 200,
) -> Any:
    """Mark a Flask view function as eligible for the zero-copy Vertebra fastpath.

    Args:
        fn: Flask view function.
        status_code: Default HTTP status code for successful responses.

    Returns:
        Decorated callable with ``__vertebra_fastpath__ = True``.
    """

    def _decorate(target: Callable[..., Any]) -> Callable[..., Any]:
        dyn = cast("Any", target)
        dyn.__vertebra_fastpath__ = True
        dyn.__vertebra_status_code__ = status_code
        return target

    if fn is not None:
        return _decorate(fn)
    return _decorate


def compile_fastpath_routes(compiler: RouteCompiler) -> dict[int, CompiledRoute]:
    """Compile all Flask routes registered in ``compiler`` into ``CompiledRoute`` descriptors."""
    compiled: dict[int, CompiledRoute] = {}
    for handler_id, entry in compiler.entries.items():
        compiled[handler_id] = _compile_single_view(
            handler_id=handler_id,
            endpoint=entry.endpoint,
            rule=entry.rule,
            view_func=entry.view_func,
            param_converters=entry.param_converters,
        )
    return compiled


def dispatch_wsgi_direct(
    compiled: CompiledRoute,
    request_or_environ: Request | WsgiEnviron | dict[str, Any],
    path_params: dict[str, str],
) -> Response | None:
    """Execute a synchronous or non-suspending async Flask view directly using zero-copy body decoding."""
    request_id, kwargs = _prepare_flask_kwargs(compiled, request_or_environ, path_params)
    result = compiled.view_func(**kwargs)
    if inspect.iscoroutine(result):
        completed, stepped_val = step_coro_sync(result)
        if not completed:
            return None
        result = stepped_val
    return _serialize_flask_result(result, compiled.status_code, request_id)


def _compile_single_view(
    handler_id: int,
    endpoint: str,
    rule: str,
    view_func: Callable[..., Any],
    param_converters: tuple[tuple[str, str], ...],
) -> CompiledRoute:
    """Inspect a Flask view function signature and type hints to build a ``CompiledRoute``."""
    is_async = is_coroutine_callable(view_func)
    status_code = int(getattr(view_func, "__vertebra_status_code__", 200))
    sig = inspect.signature(view_func)
    try:
        hints = get_type_hints(view_func)
    except Exception:
        hints = {}

    path_param_names = {name for name, _ in param_converters}
    body_param_name: str | None = None
    body_type: Any = None
    body_is_msgspec = False
    body_is_pydantic = False
    accepts_request = False

    for param_name, param in sig.parameters.items():
        ann = hints.get(param_name, param.annotation)
        if param_name == "request" or ann is Request:
            accepts_request = True
            continue
        if param_name in path_param_names:
            continue
        if is_msgspec_struct_type(ann):
            body_param_name = param_name
            body_type = ann
            body_is_msgspec = True
        elif is_pydantic_model_type(ann):
            body_param_name = param_name
            body_type = ann
            body_is_pydantic = True
        elif param_name in {"body", "data", "payload"} and body_param_name is None:
            body_param_name = param_name
            body_type = None if ann is inspect.Parameter.empty else ann
            body_is_msgspec = True

    return CompiledRoute(
        handler_id=handler_id,
        endpoint=endpoint,
        rule=rule,
        view_func=view_func,
        is_async=is_async,
        status_code=status_code,
        param_converters=param_converters,
        body_param_name=body_param_name,
        body_type=body_type,
        body_is_msgspec=body_is_msgspec,
        body_is_pydantic=body_is_pydantic,
        accepts_request=accepts_request,
    )


def _serialize_flask_result(
    result: Any,
    default_status: int,
    request_id: int,
) -> Response:
    """Convert a Flask view return value directly into a Rust ``Response``."""
    if isinstance(result, Response):
        return result

    special = serialize_fastpath_special_result(result, default_status=default_status)
    if special is not None:
        sp_status, sp_headers, sp_body = special
        str_headers = [(k.decode("latin-1"), v.decode("latin-1")) for k, v in sp_headers]
        return Response.from_buffer(sp_status, sp_body, str_headers, request_id)

    status = default_status
    headers: list[tuple[str, str]] = []
    payload = result

    if isinstance(result, tuple):
        if len(result) == 2:
            payload, second = result
            if isinstance(second, int):
                status = second
            elif isinstance(second, dict):
                headers = [(str(k).lower(), str(v)) for k, v in second.items()]
            elif isinstance(second, list):
                headers = [(str(k).lower(), str(v)) for k, v in second]
        elif len(result) == 3:
            payload, status_raw, headers_raw = result
            status = int(status_raw)
            if isinstance(headers_raw, dict):
                headers = [(str(k).lower(), str(v)) for k, v in headers_raw.items()]
            elif isinstance(headers_raw, list):
                headers = [(str(k).lower(), str(v)) for k, v in headers_raw]

    has_ct = any(k == "content-type" for k, _ in headers)
    if isinstance(payload, bytes):
        if not has_ct:
            headers.append(("content-type", "application/octet-stream"))
        return Response(status=status, body=payload, headers=headers, request_id=request_id)

    if isinstance(payload, str):
        if not has_ct:
            headers.append(("content-type", "text/html; charset=utf-8"))
        return Response(status=status, body=payload.encode("utf-8"), headers=headers, request_id=request_id)

    if is_pydantic_model_type(type(payload)):
        serializer = getattr(type(payload), "__pydantic_serializer__", None)
        if serializer is not None:
            body_bytes = serializer.to_json(payload)
            if not has_ct:
                headers.append(("content-type", "application/json"))
            return Response(status=status, body=body_bytes, headers=headers, request_id=request_id)

    body_bytes = encode_json_bytes(payload)
    if not has_ct:
        headers.append(("content-type", "application/json"))
    return Response(status=status, body=body_bytes, headers=headers, request_id=request_id)


def _prepare_flask_kwargs(
    compiled: CompiledRoute,
    request_or_environ: Request | WsgiEnviron | dict[str, Any],
    path_params: dict[str, str],
) -> tuple[int, dict[str, Any]]:
    """Build keyword arguments for a compiled Flask view function."""
    request_id = int(getattr(request_or_environ, "request_id", 0))
    kwargs: dict[str, Any] = {}
    converter_map = dict(compiled.param_converters)
    for k, v in path_params.items():
        kwargs[k] = coerce_path_param(v, converter_map.get(k, "string"))

    if compiled.accepts_request:
        if isinstance(request_or_environ, Request):
            kwargs["request"] = request_or_environ
        elif isinstance(request_or_environ, WsgiEnviron):
            kwargs["request"] = request_or_environ.request()

    if compiled.body_param_name is not None:
        if isinstance(request_or_environ, Request):
            if compiled.body_is_pydantic and compiled.body_type is not None:
                kwargs[compiled.body_param_name] = request_or_environ.decode_pydantic_json(compiled.body_type)
            else:
                kwargs[compiled.body_param_name] = request_or_environ.decode_msgspec(compiled.body_type)
        elif isinstance(request_or_environ, WsgiEnviron):
            wsgi_inp = request_or_environ["wsgi.input"]
            if isinstance(wsgi_inp, WsgiInput):
                if compiled.body_is_pydantic and compiled.body_type is not None:
                    kwargs[compiled.body_param_name] = wsgi_inp.decode_pydantic_json(compiled.body_type)
                else:
                    kwargs[compiled.body_param_name] = wsgi_inp.decode_msgspec(compiled.body_type)
            else:
                req = request_or_environ.request()
                if compiled.body_is_pydantic and compiled.body_type is not None:
                    kwargs[compiled.body_param_name] = req.decode_pydantic_json(compiled.body_type)
                else:
                    kwargs[compiled.body_param_name] = req.decode_msgspec(compiled.body_type)
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
    return request_id, kwargs


async def _dispatch_flask_async(
    compiled: CompiledRoute,
    request_obj: Request,
    path_params: dict[str, str],
) -> Response:
    """Execute a synchronous or async Flask view within an ASGI event loop."""
    request_id, kwargs = _prepare_flask_kwargs(compiled, request_obj, path_params)
    result = compiled.view_func(**kwargs)
    if inspect.isawaitable(result):
        result = await result
    return _serialize_flask_result(result, compiled.status_code, request_id)
