"""Fused direct Litestar v2/v3 HTTPRouteHandler execution and ScopeState.body pre-seeding."""

import importlib
import inspect
from collections.abc import AsyncGenerator, AsyncIterable, Callable, Generator, Iterable
from typing import Any, Self

import msgspec

from vertebra.extensions.litestar.offload import (
    _get_handler_after_response,
    _get_handler_guards,
    _get_handler_middleware,
    _get_handler_request_class,
    _is_default_local_filesystem,
)
from vertebra.extensions.litestar.router import extract_litestar_routes

__all__ = (
    "CompiledLitestarRoute",
    "LitestarFastPathBypass",
    "compile_litestar_fastpath_routes",
    "dispatch_litestar_handler_direct",
    "preseed_litestar_scope_body",
)


class LitestarFastPathBypass:
    """Fast-path bypass router dispatching directly to compiled Litestar routes."""

    __slots__ = ("_router_table",)

    def __init__(self, router_table: Any = None) -> None:
        self._router_table = router_table

    @property
    def router_table(self) -> Any:
        """Return the wrapped router table."""
        return self._router_table

    async def dispatch(
        self,
        scope: Any,
        receive: Any,
        send: Any,
        *,
        body: bytes = b"",
    ) -> tuple[int, list[tuple[bytes, bytes]], bytes] | None:
        """Attempt to dispatch the request directly via the compiled fastpath routes."""
        if not self._router_table:
            return None
        return None


class CompiledLitestarRoute(msgspec.Struct, frozen=True, gc=False, array_like=True):
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


def preseed_litestar_scope_body(scope: Any, body: bytes) -> Any:
    """Pre-populate Litestar ScopeState.body on scope so await request.body() never calls AsgiReceive."""
    connection_state = ScopeState.from_scope(scope)
    connection_state.body = body
    return connection_state


def compile_litestar_fastpath_routes(app: Any) -> dict[int, CompiledLitestarRoute]:
    """Compile all Litestar routes into CompiledLitestarRoute descriptors keyed by handler_id."""
    router_table = extract_litestar_routes(app)
    has_before_send = bool(getattr(app, "before_send", None))
    compiled: dict[int, CompiledLitestarRoute] = {}

    for handler_id, entry in router_table.routes.items():
        handler = entry.route_handler
        guards = _get_handler_guards(handler)
        middleware = _get_handler_middleware(handler)
        request_class = _get_handler_request_class(handler, app)
        after_response = _get_handler_after_response(handler)
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
            has_middleware=bool(middleware),
            has_before_send=has_before_send,
            after_response=after_response,
            handler_owns_get_response=handler_owns_get_response,
        )
    return compiled


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
    try:
        resp_mod = importlib.import_module("litestar.response.base")
        state_mod = importlib.import_module("litestar.utils.scope.state")
        return resp_mod.ASGIResponse, state_mod.ScopeState
    except ImportError:
        return _FallbackASGIResponse, _FallbackScopeState


ASGIResponse, ScopeState = _load_litestar_fastpath_types()


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
