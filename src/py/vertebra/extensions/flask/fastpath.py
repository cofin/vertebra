"""Zero-copy C-API fastpath dispatcher for Flask view functions."""

from __future__ import annotations

import inspect
import uuid
from typing import TYPE_CHECKING, Any, cast, get_type_hints

import msgspec

from vertebra._vertebra import Request, Response, WsgiEnviron, WsgiInput
from vertebra.utils.serializers import encode_json_bytes

if TYPE_CHECKING:
    from collections.abc import Callable

    from vertebra.extensions.flask.router import RouteCompiler

__all__ = (
    "CompiledRoute",
    "FastPath",
    "compile_fastpath_routes",
    "dispatch_wsgi_direct",
    "vertebra_route",
)


def _is_pydantic_model_type(tp: Any) -> bool:
    """Return True when ``tp`` is a Pydantic v2 BaseModel subclass."""
    return isinstance(tp, type) and hasattr(tp, "__pydantic_validator__") and hasattr(tp, "__pydantic_serializer__")


def _is_msgspec_struct_type(tp: Any) -> bool:
    """Return True when ``tp`` is a ``msgspec.Struct`` subclass."""
    try:
        return isinstance(tp, type) and issubclass(tp, msgspec.Struct)
    except TypeError:
        return False


def _coerce_param(raw: str, converter: str) -> Any:
    """Coerce a URL path parameter according to its Werkzeug converter name."""
    if converter == "int":
        return int(raw)
    if converter == "float":
        return float(raw)
    if converter == "uuid":
        return uuid.UUID(raw)
    return raw


def vertebra_route[F: Callable[..., Any]](
    fn: F | None = None,
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

    def _decorate(target: F) -> F:
        dyn = cast("Any", target)
        dyn.__vertebra_fastpath__ = True
        dyn.__vertebra_status_code__ = status_code
        return target

    if fn is not None:
        return _decorate(fn)
    return _decorate


class CompiledRoute(msgspec.Struct, frozen=True, gc=False):
    """Pre-compiled Flask fastpath execution plan for a single view function."""

    handler_id: int
    endpoint: str
    rule: str
    view_func: Callable[..., Any]
    is_async: bool
    status_code: int
    param_converters: tuple[tuple[str, str], ...] = ()
    body_param_name: str | None = None
    body_type: Any = None
    body_is_msgspec: bool = False
    body_is_pydantic: bool = False
    accepts_request: bool = False


def _compile_single_view(
    handler_id: int,
    endpoint: str,
    rule: str,
    view_func: Callable[..., Any],
    param_converters: tuple[tuple[str, str], ...],
) -> CompiledRoute:
    """Inspect a Flask view function signature and type hints to build a ``CompiledRoute``."""
    is_async = inspect.iscoroutinefunction(view_func)
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
        if _is_msgspec_struct_type(ann):
            body_param_name = param_name
            body_type = ann
            body_is_msgspec = True
        elif _is_pydantic_model_type(ann):
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


def _serialize_flask_result(
    result: Any,
    default_status: int,
    request_id: int,
) -> Response:
    """Convert a Flask view return value directly into a Rust ``Response``."""
    if isinstance(result, Response):
        return result

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

    if _is_pydantic_model_type(type(payload)):
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


def dispatch_wsgi_direct(
    compiled: CompiledRoute,
    request_or_environ: Request | WsgiEnviron | dict[str, Any],
    path_params: dict[str, str],
) -> Response:
    """Execute a synchronous Flask view directly using zero-copy body decoding."""
    request_id = int(getattr(request_or_environ, "request_id", 0))
    kwargs: dict[str, Any] = {}
    converter_map = dict(compiled.param_converters)
    for k, v in path_params.items():
        kwargs[k] = _coerce_param(v, converter_map.get(k, "string"))

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
                    kwargs[compiled.body_param_name] = msgspec.json.decode(raw, type=compiled.body_type)
                else:
                    kwargs[compiled.body_param_name] = msgspec.json.decode(raw)

    result = compiled.view_func(**kwargs)
    return _serialize_flask_result(result, compiled.status_code, request_id)


class FastPath:
    """Fastpath route table and dispatcher for Flask applications."""

    __slots__ = ("_compiler", "_routes")

    def __init__(self, compiler: RouteCompiler) -> None:
        self._compiler = compiler
        self._routes = compile_fastpath_routes(compiler)

    @property
    def routes(self) -> dict[int, CompiledRoute]:
        """Return mapping from ``handler_id`` to ``CompiledRoute``."""
        return self._routes

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
        if compiled.is_async:
            return None
        return dispatch_wsgi_direct(compiled, request, params)

    def dispatch_wsgi(
        self,
        environ: WsgiEnviron | dict[str, Any],
        start_response: Callable[..., Any],
    ) -> list[bytes] | None:
        """Attempt zero-copy fastpath dispatch for a PEP 3333 WSGI call."""
        method = str(environ.get("REQUEST_METHOD", "GET"))
        path = str(environ.get("PATH_INFO", "/"))
        matched = self.match(method, path)
        if matched is None:
            return None
        compiled, params = matched
        if compiled.is_async or not getattr(compiled.view_func, "__vertebra_fastpath__", True):
            return None
        resp = dispatch_wsgi_direct(compiled, environ, params)
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
