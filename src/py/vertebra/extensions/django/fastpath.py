"""Zero-copy C-API fastpath, @vertebra_view decorator, and MsgspecView for Django."""

from __future__ import annotations

import inspect
import uuid
from typing import TYPE_CHECKING, Any, ClassVar, cast, get_type_hints

import msgspec

from vertebra._vertebra import Request, Response, WsgiEnviron, WsgiInput
from vertebra.utils.serializers import encode_json_bytes

if TYPE_CHECKING:
    from collections.abc import Callable

    from vertebra.extensions.django.router import RouteCompiler

__all__ = (
    "CompiledRoute",
    "FastPath",
    "MsgspecView",
    "compile_fastpath_routes",
    "dispatch_django_direct",
    "vertebra_view",
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
    """Coerce a URL path parameter according to its Django converter name."""
    if converter == "int":
        return int(raw)
    if converter == "float":
        return float(raw)
    if converter == "uuid":
        return uuid.UUID(raw)
    return raw


def vertebra_view[F: Callable[..., Any]](
    fn: F | None = None,
    *,
    methods: tuple[str, ...] | list[str] = ("GET", "POST", "PUT", "PATCH", "DELETE"),
    status_code: int = 200,
) -> Any:
    """Mark a Django view function as eligible for the zero-copy Vertebra fastpath."""

    def _decorate(target: F) -> F:
        dyn = cast("Any", target)
        dyn.__vertebra_fastpath__ = True
        dyn.__vertebra_methods__ = tuple(m.upper() for m in methods)
        dyn.__vertebra_status_code__ = status_code
        return target

    if fn is not None:
        return _decorate(fn)
    return _decorate


class MsgspecView:
    """Base class for high-throughput Django class-based views using zero-copy msgspec/Pydantic DTOs."""

    http_method_names: ClassVar[tuple[str, ...]] = ("get", "post", "put", "patch", "delete")

    @classmethod
    def as_view(cls, **initkwargs: Any) -> Callable[..., Any]:
        """Return a fastpath-enabled view callable dispatching by HTTP method."""

        def _view(request: Request, *args: Any, **kwargs: Any) -> Any:
            self = cls(**initkwargs)
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
        return _view


class CompiledRoute(msgspec.Struct, frozen=True, gc=False):
    """Pre-compiled Django fastpath execution plan for a single view."""

    handler_id: int
    name: str
    pattern: str
    callback: Callable[..., Any]
    is_async: bool
    status_code: int
    param_converters: tuple[tuple[str, str], ...] = ()
    body_param_name: str | None = None
    body_type: Any = None
    body_is_msgspec: bool = False
    body_is_pydantic: bool = False
    accepts_request: bool = True


def _compile_single_callback(
    handler_id: int,
    name: str,
    pattern: str,
    callback: Callable[..., Any],
    param_converters: tuple[tuple[str, str], ...],
) -> CompiledRoute:
    """Inspect a Django view callable and type annotations to build a ``CompiledRoute``."""
    is_async = inspect.iscoroutinefunction(callback)
    status_code = int(getattr(callback, "__vertebra_status_code__", 200))
    sig = inspect.signature(callback)
    try:
        hints = get_type_hints(callback)
    except Exception:
        hints = {}

    path_param_names = {p_name for p_name, _ in param_converters}
    body_param_name: str | None = None
    body_type: Any = None
    body_is_msgspec = False
    body_is_pydantic = False
    accepts_request = False

    params = list(sig.parameters.values())
    for idx, param in enumerate(params):
        p_name = param.name
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
        if _is_msgspec_struct_type(ann):
            body_param_name = p_name
            body_type = ann
            body_is_msgspec = True
        elif _is_pydantic_model_type(ann):
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
        )
    return compiled


def _serialize_django_result(result: Any, default_status: int, request_id: int) -> Response:
    """Convert a Django view return value into a Rust ``Response``."""
    if isinstance(result, Response):
        return result

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

    if _is_pydantic_model_type(type(result)):
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


def dispatch_django_direct(
    compiled: CompiledRoute,
    request_or_environ: Request | WsgiEnviron | dict[str, Any],
    path_params: dict[str, str],
) -> Response:
    """Execute a synchronous Django view directly using zero-copy body decoding."""
    request_id = int(getattr(request_or_environ, "request_id", 0))
    kwargs: dict[str, Any] = {}
    converter_map = dict(compiled.param_converters)
    for k, v in path_params.items():
        kwargs[k] = _coerce_param(v, converter_map.get(k, "str"))

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

    if compiled.accepts_request and req_obj is not None:
        result = compiled.callback(req_obj, **kwargs)
    else:
        result = compiled.callback(**kwargs)
    return _serialize_django_result(result, compiled.status_code, request_id)


class FastPath:
    """Fastpath route table and dispatcher for Django applications."""

    __slots__ = ("_compiler", "_routes")

    def __init__(self, compiler: RouteCompiler) -> None:
        self._compiler = compiler
        self._routes = compile_fastpath_routes(compiler)

    @property
    def routes(self) -> dict[int, CompiledRoute]:
        """Return mapping from ``handler_id`` to ``CompiledRoute``."""
        return self._routes

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
        if compiled.is_async:
            return None
        return dispatch_django_direct(compiled, request, params)

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
        if compiled.is_async or not getattr(compiled.callback, "__vertebra_fastpath__", True):
            return None
        resp = dispatch_django_direct(compiled, environ, params)
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
