"""Fused direct Litestar v2/v3 HTTPRouteHandler execution and ScopeState.body pre-seeding."""

import enum
import inspect
from collections.abc import AsyncGenerator, AsyncIterable, Callable, Generator, Iterable
from typing import Annotated, Any, Self, get_origin, get_type_hints
from urllib.parse import parse_qsl

import msgspec

from vertebra.extensions.litestar.offload import (
    _get_handler_after_response,
    _get_handler_exception_handlers,
    _get_handler_guards,
    _get_handler_middleware,
    _get_handler_request_class,
    _is_default_local_filesystem,
    _is_empty_sentinel,
)
from vertebra.extensions.litestar.router import (
    ExtractedLitestarRoute,
    LitestarRouterTable,
    extract_litestar_routes,
)
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import decode_json, encode_json_bytes

__all__ = (
    "CompiledLitestarRoute",
    "LitestarFastPathBypass",
    "compile_litestar_fastpath_routes",
    "dispatch_litestar_handler_direct",
    "preseed_litestar_scope_body",
)


class CompiledLitestarRoute(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Pre-compiled per-route dispatch metadata for zero-overhead Litestar v2/v3 handler invocation."""

    handler_id: int
    method: str
    path_template: str
    route: Any
    route_handler: Any
    kwargs_model: Any
    request_class: Any
    has_guards: bool
    has_middleware: bool
    has_before_send: bool
    after_response: Callable[..., Any] | None
    handler_owns_get_response: bool
    has_exception_handlers: bool = False
    has_response_cache: bool = False

    @property
    def is_bypass_eligible(self) -> bool:
        """Return True when outer Litestar ASGI middleware and guard layers can be bypassed safely."""
        return (
            not self.has_guards
            and not self.has_middleware
            and not self.has_before_send
            and not self.has_exception_handlers
            and not self.has_response_cache
        )

    @property
    def can_dispatch_direct(self) -> bool:
        """Return True when Litestar route or handler _get_response_for_request is available."""
        return self.request_class is not None and (
            self.handler_owns_get_response or callable(getattr(self.route, "_get_response_for_request", None))
        )


class LitestarFastPathBypass:
    """Fast-path bypass router dispatching directly to compiled Litestar routes."""

    __slots__ = ("_app", "_compiled_routes", "_direct_plans", "_router_table")

    def __init__(
        self,
        router_table: Any = None,
        *,
        app: Any = None,
        compiled_routes: dict[int, CompiledLitestarRoute] | None = None,
    ) -> None:
        resolved_app = app
        resolved_table = router_table
        if (
            resolved_app is None
            and resolved_table is not None
            and not isinstance(resolved_table, LitestarRouterTable)
            and not hasattr(resolved_table, "match")
            and hasattr(resolved_table, "routes")
        ):
            resolved_app = resolved_table
            resolved_table = extract_litestar_routes(resolved_app)

        self._app = resolved_app
        self._router_table: LitestarRouterTable | None = None
        self._compiled_routes: dict[int, CompiledLitestarRoute] = {}
        self._direct_plans: dict[int, _DirectSignaturePlan] = {}
        self.bind_app(resolved_app, router_table=resolved_table, compiled_routes=compiled_routes)

    @property
    def router_table(self) -> Any:
        """Return the wrapped router table."""
        return self._router_table

    @classmethod
    def from_app(cls, app: Any) -> Self:
        """Construct a bound LitestarFastPathBypass directly from an initialized Litestar application."""
        table = extract_litestar_routes(app)
        compiled = compile_litestar_fastpath_routes(app, router_table=table)
        return cls(table, app=app, compiled_routes=compiled)

    def bind_app(
        self,
        app: Any,
        *,
        router_table: Any = None,
        compiled_routes: dict[int, CompiledLitestarRoute] | None = None,
    ) -> None:
        """Bind or refresh the target Litestar application, router table, and compiled route plans."""
        if app is not None:
            self._app = app
        if router_table is not None:
            self._router_table = router_table
        elif self._router_table is None and self._app is not None:
            self._router_table = extract_litestar_routes(self._app)

        if compiled_routes is not None:
            self._compiled_routes = dict(compiled_routes)
        elif self._router_table is not None:
            self._compiled_routes = _compile_routes_from_table(self._router_table, self._app)

        plans: dict[int, _DirectSignaturePlan] = {}
        if self._router_table is not None:
            for handler_id, entry in self._router_table.routes.items():
                compiled = self._compiled_routes.get(handler_id)
                if compiled is None or not compiled.is_bypass_eligible:
                    continue
                plan = _compile_direct_signature_plan(entry, compiled)
                if plan is not None:
                    plans[handler_id] = plan
        self._direct_plans = plans

    def wrap_asgi(self, fallback_app: Callable[..., Any]) -> Callable[..., Any]:
        """Wrap an ASGI 3.0 callable so eligible HTTP requests execute via direct fastpath dispatch."""
        bypass = self

        async def _fastpath_asgi_app(scope: Any, receive: Any, send: Any) -> None:
            sent_via_callback = False

            async def _tracked_send(message: Any) -> None:
                nonlocal sent_via_callback
                sent_via_callback = True
                await send(message)

            direct = await bypass.dispatch(scope, receive, _tracked_send)
            if direct is not None:
                status_code, encoded_headers, raw_body = direct
                await send(
                    {
                        "type": "http.response.start",
                        "status": status_code,
                        "headers": encoded_headers,
                    }
                )
                await send(
                    {
                        "type": "http.response.body",
                        "body": raw_body,
                        "more_body": False,
                    }
                )
                return
            if sent_via_callback:
                return
            await fallback_app(scope, receive, send)

        return _fastpath_asgi_app

    async def dispatch(
        self,
        scope: Any,
        receive: Any,
        send: Any,
        *,
        body: bytes | None = None,
    ) -> tuple[int, list[tuple[bytes, bytes]], bytes] | None:
        """Attempt to dispatch the request directly via the compiled fastpath routes."""
        table = self._router_table
        if table is None or not table.routes:
            return None

        scope_type = scope.get("type", "http") if hasattr(scope, "get") else scope["type"]
        if scope_type != "http":
            return None

        method = str(scope.get("method", "GET") if hasattr(scope, "get") else scope["method"]).upper()
        path = str(scope.get("path", "/") if hasattr(scope, "get") else scope["path"])

        try:
            matched = (
                table.match_with_params(method, path)
                if hasattr(table, "match_with_params")
                else _fallback_match_with_params(table, method, path)
            )
        except (ValueError, TypeError):
            return None
        if matched is None:
            return None

        entry, path_params = matched
        compiled = self._compiled_routes.get(entry.handler_id)
        if compiled is None or not compiled.is_bypass_eligible:
            return None

        direct_plan = self._direct_plans.get(entry.handler_id)
        if direct_plan is not None:
            return await _execute_direct_signature_plan(
                direct_plan,
                scope=scope,
                path_params=path_params,
                receive=receive,
                body=body,
            )

        if not compiled.can_dispatch_direct:
            return None

        if self._app is not None:
            scope["app"] = self._app
            scope["litestar_app"] = self._app
        scope["path_params"] = path_params
        scope["route_handler"] = compiled.route_handler
        scope["path_template"] = compiled.path_template

        resolved_body = await _resolve_request_body(scope, receive, body, method=method)
        return await dispatch_litestar_handler_direct(
            compiled,
            scope=scope,
            body=resolved_body,
            receive=receive,
            send=send,
        )


def preseed_litestar_scope_body(scope: Any, body: bytes) -> Any:
    """Pre-populate Litestar ScopeState.body on scope so await request.body() never calls AsgiReceive."""
    connection_state = _get_scope_state_cls().from_scope(scope)
    connection_state.body = body
    return connection_state


def compile_litestar_fastpath_routes(
    app: Any,
    *,
    router_table: LitestarRouterTable | None = None,
) -> dict[int, CompiledLitestarRoute]:
    """Compile all Litestar routes into CompiledLitestarRoute descriptors keyed by handler_id."""
    table = router_table if router_table is not None else extract_litestar_routes(app)
    return _compile_routes_from_table(table, app)


async def dispatch_litestar_handler_direct(
    compiled: CompiledLitestarRoute,
    *,
    scope: Any,
    body: bytes,
    receive: Any,
    send: Any,
) -> tuple[int, list[tuple[bytes, bytes]], bytes] | None:
    """Execute a middleware-free Litestar v2 or v3 HTTPRouteHandler and return (status, headers, body)."""
    preseed_litestar_scope_body(scope, body)
    request = compiled.request_class(scope=scope, receive=receive, send=send)
    scope["connection"] = request

    if compiled.has_guards:
        await compiled.route_handler.authorize_connection(request)

    if compiled.handler_owns_get_response:
        asgi_response = await compiled.route_handler._get_response_for_request(request=request)
    else:
        asgi_response = await compiled.route._get_response_for_request(
            scope=scope,
            request=request,
            route_handler=compiled.route_handler,
            parameter_model=compiled.kwargs_model,
        )

    result: tuple[int, list[tuple[bytes, bytes]], bytes] | None = None
    resp_cls_name = type(asgi_response).__name__

    if type(asgi_response) is ASGIResponse or resp_cls_name == "ASGIResponse":
        encoded_headers = asgi_response.encode_headers()
        raw_body = asgi_response.body if isinstance(asgi_response.body, bytes) else bytes(asgi_response.body)
        result = (int(asgi_response.status_code), encoded_headers, raw_body)
    elif resp_cls_name == "ASGIFileResponse" and _is_default_local_filesystem(
        getattr(asgi_response, "file_system", None)
    ):
        file_path = getattr(asgi_response, "file_path", None)
        if file_path is not None:
            await send(
                {
                    "type": "http.response.pathsend",
                    "path": str(file_path),
                    "status": int(getattr(asgi_response, "status_code", 200)),
                    "headers": asgi_response.encode_headers(),
                }
            )
        else:
            await asgi_response(scope, receive, send)
    elif resp_cls_name == "ASGIStreamingSSEResponse":
        await _offload_sse_response(asgi_response, send)
    else:
        await asgi_response(scope, receive, send)

    if compiled.after_response is not None:
        after_res = compiled.after_response(request)
        if inspect.isawaitable(after_res):
            await after_res

    return result


_RESERVED_CONNECTION_KWARGS: frozenset[str] = frozenset(
    {"request", "socket", "scope", "state", "headers", "cookies", "query", "body"}
)
_CUSTOM_RESPONSE_RETURN_NAMES: frozenset[str] = frozenset(
    {"Response", "Redirect", "File", "Stream", "Template", "ServerSentEvent", "ASGIResponse"}
)
_BODYLESS_HTTP_METHODS: frozenset[str] = frozenset({"GET", "HEAD", "DELETE", "OPTIONS"})
_UNSET_DEFAULT: object = object()


class _QueryParamSpec(msgspec.Struct, frozen=True, gc=False):
    """Compiled scalar query parameter extraction spec for _DirectSignaturePlan."""

    name: str
    converter: Callable[[str], Any]
    default: Any


class _DirectSignaturePlan(msgspec.Struct, frozen=True, gc=False):
    """Zero-allocation compiled signature execution plan for pure Litestar route handlers."""

    fn: Callable[..., Any]
    is_async: bool
    status_code: int
    media_type: str
    path_param_names: tuple[str, ...]
    query_params: tuple[_QueryParamSpec, ...]
    body_param: str | None
    body_type: Any
    json_headers: list[tuple[bytes, bytes]]
    text_headers: list[tuple[bytes, bytes]]
    raw_bytes_headers: list[tuple[bytes, bytes]]


class _FallbackScopeState:
    """Fallback ScopeState representation when the optional litestar extra is not installed."""

    __slots__ = ("body",)

    def __init__(self) -> None:
        self.body: bytes | None = None

    @classmethod
    def from_scope(cls, scope: Any) -> Self:
        state = scope.setdefault("state", {})
        existing = state.get("_ls_connection_state")
        if isinstance(existing, cls):
            return existing
        created = cls()
        state["_ls_connection_state"] = created
        return created


class _FallbackASGIResponse:
    """Fallback ASGIResponse representation when the optional litestar extra is not installed."""

    __slots__ = ("body", "headers", "status_code")

    def __init__(
        self,
        *,
        body: bytes = b"",
        status_code: int = 200,
        headers: list[tuple[bytes, bytes]] | None = None,
    ) -> None:
        self.body = body
        self.status_code = status_code
        self.headers = headers or []

    def encode_headers(self) -> list[tuple[bytes, bytes]]:
        return list(self.headers)


def _load_litestar_fastpath_types() -> tuple[Any, Any]:
    resp_mod = import_optional("litestar.response.base")
    state_mod = import_optional("litestar.utils.scope.state")
    if resp_mod is not None and state_mod is not None:
        return resp_mod.ASGIResponse, state_mod.ScopeState
    return _FallbackASGIResponse, _FallbackScopeState


ASGIResponse, ScopeState = _load_litestar_fastpath_types()


def _get_scope_state_cls() -> Any:
    return ScopeState


def _has_lifecycle_hook(handler: Any, app: Any, attr_name: str, resolve_name: str) -> bool:
    resolve_fn = getattr(handler, resolve_name, None)
    if callable(resolve_fn):
        return not _is_empty_sentinel(resolve_fn())
    raw = getattr(handler, attr_name, None)
    if not _is_empty_sentinel(raw):
        return True
    if app is not None:
        return not _is_empty_sentinel(getattr(app, attr_name, None))
    return False


def _compile_routes_from_table(
    router_table: LitestarRouterTable,
    app: Any,
) -> dict[int, CompiledLitestarRoute]:
    has_before_send = bool(getattr(app, "before_send", None)) if app is not None else False
    app_has_middleware = bool(getattr(app, "middleware", None)) if app is not None else False
    compiled: dict[int, CompiledLitestarRoute] = {}

    for handler_id, entry in router_table.routes.items():
        handler = entry.route_handler
        guards = _get_handler_guards(handler)
        middleware = _get_handler_middleware(handler)
        has_hooks = _has_lifecycle_hook(
            handler, app, "before_request", "resolve_before_request"
        ) or _has_lifecycle_hook(handler, app, "after_request", "resolve_after_request")
        request_class = _get_handler_request_class(handler, app)
        after_response = _get_handler_after_response(handler)
        exception_handlers = _get_handler_exception_handlers(handler)
        has_cache = bool(getattr(handler, "cache", False))
        handler_owns_get_response = callable(getattr(handler, "_get_response_for_request", None))
        compiled[handler_id] = CompiledLitestarRoute(
            handler_id=handler_id,
            method=entry.method,
            path_template=entry.path_template,
            route=entry.route,
            route_handler=handler,
            kwargs_model=entry.kwargs_model,
            request_class=request_class,
            has_guards=bool(guards),
            has_middleware=app_has_middleware or bool(middleware) or has_hooks,
            has_before_send=has_before_send,
            after_response=after_response,
            handler_owns_get_response=handler_owns_get_response,
            has_exception_handlers=bool(exception_handlers),
            has_response_cache=has_cache,
        )
    return compiled


def _parse_bool_query(raw: str) -> bool:
    lowered = raw.strip().lower()
    if lowered in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if lowered in {"0", "false", "f", "no", "n", "off"}:
        return False
    msg = f"invalid boolean query parameter: {raw!r}"
    raise ValueError(msg)


def _resolve_scalar_query_converter(annotation: Any) -> Callable[[str], Any] | None:
    if annotation is inspect.Parameter.empty or annotation is Any or annotation is str:
        return str
    if annotation is int:
        return int
    if annotation is float:
        return float
    if annotation is bool:
        return _parse_bool_query
    return None


def _handler_has_dependencies_or_dto(handler: Any) -> bool:
    resolve_deps = getattr(handler, "resolve_dependencies", None)
    if callable(resolve_deps):
        if resolve_deps():
            return True
    else:
        raw_deps = getattr(handler, "dependencies", None)
        if not _is_empty_sentinel(raw_deps) and bool(raw_deps):
            return True

    for dto_attr, resolve_attr in (("dto", "resolve_data_dto"), ("return_dto", "resolve_return_dto")):
        resolve_dto = getattr(handler, resolve_attr, None)
        if callable(resolve_dto):
            if not _is_empty_sentinel(resolve_dto()):
                return True
        elif not _is_empty_sentinel(getattr(handler, dto_attr, None)):
            return True
    return False


def _compile_direct_signature_plan(
    entry: ExtractedLitestarRoute,
    compiled: CompiledLitestarRoute,
) -> _DirectSignaturePlan | None:
    handler = compiled.route_handler
    if compiled.after_response is not None or _handler_has_dependencies_or_dto(handler):
        return None

    raw_fn = getattr(handler, "fn", None)
    fn = getattr(raw_fn, "value", raw_fn)
    if not callable(fn):
        return None

    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return None

    try:
        hints = get_type_hints(fn, include_extras=True)
    except Exception:
        hints = {}

    ret_hint = hints.get("return", sig.return_annotation)
    ret_name = getattr(ret_hint, "__name__", "")
    if ret_name in _CUSTOM_RESPONSE_RETURN_NAMES:
        return None

    path_param_names: list[str] = []
    query_specs: list[_QueryParamSpec] = []
    body_param: str | None = None
    body_type: Any = Any

    for param in sig.parameters.values():
        if param.kind in {inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD}:
            return None
        if param.name in _RESERVED_CONNECTION_KWARGS:
            return None
        ann = hints.get(param.name, param.annotation)
        if get_origin(ann) is Annotated:
            return None

        if param.name in entry.param_types:
            path_param_names.append(param.name)
        elif param.name == "data":
            body_param = "data"
            body_type = Any if ann is inspect.Parameter.empty else ann
        else:
            converter = _resolve_scalar_query_converter(ann)
            if converter is None:
                return None
            default_val = _UNSET_DEFAULT if param.default is inspect.Parameter.empty else param.default
            query_specs.append(_QueryParamSpec(name=param.name, converter=converter, default=default_val))

    raw_status = getattr(handler, "status_code", None)
    if isinstance(raw_status, enum.IntEnum):
        status_code = int(raw_status.value)
    elif isinstance(raw_status, int):
        status_code = raw_status
    elif entry.method == "POST":
        status_code = 201
    elif entry.method == "DELETE":
        status_code = 204
    else:
        status_code = 200

    raw_media = getattr(handler, "media_type", None)
    if isinstance(raw_media, enum.Enum):
        media_type = str(raw_media.value)
    elif isinstance(raw_media, str) and raw_media:
        media_type = raw_media
    else:
        media_type = "application/json"

    media_bytes = media_type.encode("latin-1")
    return _DirectSignaturePlan(
        fn=fn,
        is_async=inspect.iscoroutinefunction(fn),
        status_code=status_code,
        media_type=media_type,
        path_param_names=tuple(path_param_names),
        query_params=tuple(query_specs),
        body_param=body_param,
        body_type=body_type,
        json_headers=[(b"content-type", b"application/json")],
        text_headers=[
            (b"content-type", media_bytes if media_type.startswith("text/") else b"text/plain; charset=utf-8")
        ],
        raw_bytes_headers=[
            (b"content-type", media_bytes if media_type != "application/json" else b"application/octet-stream")
        ],
    )


def _fallback_match_with_params(
    table: Any,
    method: str,
    path: str,
) -> tuple[ExtractedLitestarRoute, dict[str, Any]] | None:
    entry = table.match(method, path)
    if entry is None:
        return None
    matched = entry.regex_pattern.match(path)
    raw_groups = matched.groupdict() if matched is not None else {}
    params = table.convert_params(entry, raw_groups) if raw_groups else {}
    return entry, params


async def _resolve_request_body(
    scope: Any,
    receive: Any,
    body: bytes | None,
    *,
    method: str = "POST",
) -> bytes:
    if body is not None:
        return body
    state_obj = scope.get("state") if hasattr(scope, "get") else None
    if isinstance(state_obj, dict):
        conn_state = state_obj.get("_ls_connection_state")
        preseeded = getattr(conn_state, "body", None)
        if preseeded is not None:
            return preseeded if isinstance(preseeded, bytes) else bytes(preseeded)
    if method in _BODYLESS_HTTP_METHODS or not callable(receive):
        return b""

    chunks: list[bytes] = []
    while True:
        message = await receive()
        chunk = message.get("body", b"")
        if chunk:
            chunks.append(chunk if isinstance(chunk, bytes) else bytes(chunk))
        if not message.get("more_body", False):
            break
    if not chunks:
        return b""
    return chunks[0] if len(chunks) == 1 else b"".join(chunks)


async def _execute_direct_signature_plan(
    plan: _DirectSignaturePlan,
    *,
    scope: Any,
    path_params: dict[str, Any],
    receive: Any,
    body: bytes | None,
) -> tuple[int, list[tuple[bytes, bytes]], bytes] | None:
    kwargs: dict[str, Any] = {}
    for name in plan.path_param_names:
        if name not in path_params:
            return None
        kwargs[name] = path_params[name]

    if plan.query_params:
        raw_qs = scope.get("query_string", b"") if hasattr(scope, "get") else scope["query_string"]
        if isinstance(raw_qs, bytes | bytearray):
            qs_str = raw_qs.decode("latin-1")
        elif isinstance(raw_qs, memoryview):
            qs_str = bytes(raw_qs).decode("latin-1")
        else:
            qs_str = str(raw_qs or "")
        parsed_qs = dict(parse_qsl(qs_str, keep_blank_values=True)) if qs_str else {}
        for qspec in plan.query_params:
            if qspec.name in parsed_qs:
                try:
                    kwargs[qspec.name] = qspec.converter(parsed_qs[qspec.name])
                except (ValueError, TypeError):
                    return None
            elif qspec.default is not _UNSET_DEFAULT:
                kwargs[qspec.name] = qspec.default
            else:
                return None

    if plan.body_param is not None:
        raw_body = await _resolve_request_body(scope, receive, body)
        try:
            decoded = decode_json(raw_body)
            kwargs[plan.body_param] = (
                decoded if plan.body_type is Any else msgspec.convert(decoded, type=plan.body_type)
            )
        except (msgspec.ValidationError, msgspec.DecodeError, ValueError, TypeError):
            return None

    out = plan.fn(**kwargs)
    if plan.is_async or inspect.isawaitable(out):
        out = await out

    if out is None or plan.status_code == 204:
        return (plan.status_code, [], b"")
    if isinstance(out, bytes):
        return (plan.status_code, list(plan.raw_bytes_headers), out)
    if isinstance(out, bytearray | memoryview):
        return (plan.status_code, list(plan.raw_bytes_headers), bytes(out))
    if isinstance(out, str) and plan.media_type.startswith("text/"):
        return (plan.status_code, list(plan.text_headers), out.encode("utf-8"))
    return (plan.status_code, list(plan.json_headers), encode_json_bytes(out))


async def _offload_sse_response(asgi_response: Any, send: Any) -> None:
    ping_interval = getattr(asgi_response, "_ping_interval", None)
    headers = asgi_response.encode_headers()
    status_code = int(getattr(asgi_response, "status_code", 200))
    await send(
        {
            "type": "http.response.sse",
            "status": status_code,
            "headers": headers,
            "ping_interval_s": ping_interval,
        }
    )
    iterator = getattr(asgi_response, "iterator", None)
    if isinstance(iterator, (AsyncIterable, AsyncGenerator)):
        async for chunk in iterator:
            raw = chunk if isinstance(chunk, bytes) else str(chunk).encode("utf-8")
            await send({"type": "http.response.body", "body": raw, "more_body": True})
    elif isinstance(iterator, (Iterable, Generator)):
        for chunk in iterator:
            raw = chunk if isinstance(chunk, bytes) else str(chunk).encode("utf-8")
            await send({"type": "http.response.body", "body": raw, "more_body": True})
    await send({"type": "http.response.body", "body": b"", "more_body": False})
