"""Fused direct Starlette endpoint execution, 24-slot AsgiScope population, and Request._body pre-seeding."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any, Self

import anyio.to_thread
import msgspec

from vertebra.extensions.starlette.offload import has_non_offloadable_middleware
from vertebra.extensions.starlette.router import (
    RouterTable,
    extract_routes,
)
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import decode_json, encode_json_bytes

__all__ = (
    "CompiledRoute",
    "FastPath",
    "compile_fastpath_routes",
    "dispatch_handler_direct",
    "preseed_request_body",
)

_BODYLESS_HTTP_METHODS: frozenset[str] = frozenset({"GET", "HEAD", "DELETE", "OPTIONS"})


class CompiledRoute(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Pre-compiled per-route dispatch metadata for Starlette endpoint invocation."""

    handler_id: int
    method: str
    path_template: str
    route: Any
    endpoint: Any
    is_async: bool
    has_middleware: bool
    has_custom_exception_handlers: bool = False

    @property
    def is_bypass_eligible(self) -> bool:
        """Return True when outer Starlette middleware and exception stacks can be bypassed safely."""
        return not self.has_middleware and not self.has_custom_exception_handlers


class FastPath:
    """Fast-path bypass router dispatching directly to compiled Starlette routes."""

    __slots__ = ("_app", "_compiled_routes", "_router_table")

    def __init__(
        self,
        router_table: Any = None,
        *,
        app: Any = None,
        compiled_routes: dict[int, CompiledRoute] | None = None,
    ) -> None:
        resolved_app = app
        resolved_table = router_table
        if (
            resolved_app is None
            and resolved_table is not None
            and not isinstance(resolved_table, RouterTable)
            and not hasattr(resolved_table, "match_with_params")
            and hasattr(resolved_table, "routes")
        ):
            resolved_app = resolved_table
            resolved_table = extract_routes(resolved_app)

        self._app = resolved_app
        self._router_table: RouterTable | None = None
        self._compiled_routes: dict[int, CompiledRoute] = {}
        self.bind_app(resolved_app, router_table=resolved_table, compiled_routes=compiled_routes)

    @property
    def router_table(self) -> RouterTable | None:
        """Return the extracted RouterTable."""
        return self._router_table

    @property
    def compiled_routes(self) -> dict[int, CompiledRoute]:
        """Return the compiled route lookup table."""
        return self._compiled_routes

    @classmethod
    def from_app(cls, app: Any) -> Self:
        """Construct a bound FastPath directly from a Starlette application."""
        table = extract_routes(app)
        compiled = compile_fastpath_routes(app, router_table=table)
        return cls(table, app=app, compiled_routes=compiled)

    def bind_app(
        self,
        app: Any,
        *,
        router_table: Any = None,
        compiled_routes: dict[int, CompiledRoute] | None = None,
    ) -> None:
        """Bind or refresh the target Starlette application, router table, and compiled routes."""
        if app is not None:
            self._app = app
        if router_table is not None:
            self._router_table = router_table
        elif self._router_table is None and self._app is not None:
            self._router_table = extract_routes(self._app)

        if compiled_routes is not None:
            self._compiled_routes = dict(compiled_routes)
        elif self._router_table is not None:
            self._compiled_routes = _compile_routes_from_table(self._router_table, self._app)

    def wrap_asgi(self, fallback_app: Callable[..., Any]) -> Callable[..., Any]:
        """Wrap an ASGI 3.0 callable so eligible HTTP requests execute via direct fastpath dispatch."""
        bypass = self

        async def _fastpath_asgi_app(scope: Any, receive: Any, send: Any) -> None:
            sent_via_callback = False
            buffered_messages: list[Any] = []
            upstream_exhausted = False

            async def _buffering_receive() -> Any:
                nonlocal upstream_exhausted
                msg = await receive()
                buffered_messages.append(msg)
                if isinstance(msg, dict) and msg.get("type") == "http.request" and not msg.get("more_body", False):
                    upstream_exhausted = True
                return msg

            async def _tracked_send(message: Any) -> None:
                nonlocal sent_via_callback
                sent_via_callback = True
                await send(message)

            direct = await bypass.dispatch(scope, _buffering_receive, _tracked_send)
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
            if not buffered_messages:
                await fallback_app(scope, receive, send)
                return

            replay_idx = 0

            async def _replay_receive() -> Any:
                nonlocal replay_idx
                if replay_idx < len(buffered_messages):
                    item = buffered_messages[replay_idx]
                    replay_idx += 1
                    return item
                if upstream_exhausted:
                    return {"type": "http.request", "body": b"", "more_body": False}
                return await receive()

            await fallback_app(scope, _replay_receive, send)

        return _fastpath_asgi_app

    async def dispatch(
        self,
        scope: Any,
        receive: Any,
        send: Any,
        *,
        body: bytes | None = None,
    ) -> tuple[int, list[tuple[bytes, bytes]], bytes] | None:
        """Attempt to dispatch the request directly via the compiled Starlette fastpath."""
        table = self._router_table
        if table is None or not table.routes:
            return None

        scope_type = scope.get("type", "http") if hasattr(scope, "get") else scope["type"]
        if scope_type != "http":
            return None

        method = str(scope.get("method", "GET") if hasattr(scope, "get") else scope["method"]).upper()
        path = str(scope.get("path", "/") if hasattr(scope, "get") else scope["path"])

        try:
            matched = table.match_with_params(method, path)
        except (ValueError, TypeError):
            return None
        if matched is None:
            return None

        entry, path_params = matched
        compiled = self._compiled_routes.get(entry.handler_id)
        if compiled is None or not compiled.is_bypass_eligible:
            return None

        if self._app is not None:
            scope["app"] = self._app
            router_obj = getattr(self._app, "router", None)
            if router_obj is not None:
                scope["router"] = router_obj
        scope["path_params"] = path_params
        scope["endpoint"] = compiled.endpoint
        scope["route"] = compiled.route

        resolved_body = await _resolve_request_body(scope, receive, body, method=method)
        try:
            return await dispatch_handler_direct(
                compiled,
                scope=scope,
                body=resolved_body,
                receive=receive,
                send=send,
            )
        except Exception as exc:
            http_exc_resp = _serialize_starlette_http_exception(exc)
            if http_exc_resp is not None:
                return http_exc_resp
            raise


def preseed_request_body(scope: Any, request: Any, body: bytes | None = None) -> None:
    """Pre-populate Starlette Request._body via C-API PyObject_GenericSetAttr when available."""
    preseed_fn = getattr(scope, "preseed_request_body", None)
    if callable(preseed_fn):
        preseed_fn(request, body)
        return
    resolved = body if body is not None else b""
    request._body = resolved


def compile_fastpath_routes(
    app: Any,
    *,
    router_table: RouterTable | None = None,
) -> dict[int, CompiledRoute]:
    """Compile all Starlette routes into CompiledRoute descriptors keyed by handler_id."""
    table = router_table if router_table is not None else extract_routes(app)
    return _compile_routes_from_table(table, app)


async def dispatch_handler_direct(
    compiled: CompiledRoute,
    *,
    scope: Any,
    body: bytes,
    receive: Any,
    send: Any,
) -> tuple[int, list[tuple[bytes, bytes]], bytes] | None:
    """Execute a middleware-free Starlette endpoint with pre-seeded Request._body."""
    req_cls = _get_starlette_request_cls()
    request = req_cls(scope, receive=receive, send=send)
    preseed_request_body(scope, request, body)
    req_fn = getattr(scope, "request", None)
    if callable(req_fn):
        native_req = req_fn()
        scope["vertebra_request"] = native_req

    if compiled.is_async:
        response = await compiled.endpoint(request)
    else:
        response = await anyio.to_thread.run_sync(compiled.endpoint, request)

    if response is None:
        return 204, [], b""

    if isinstance(response, (dict, list, msgspec.Struct)):
        payload = encode_json_bytes(response)
        return (
            200,
            [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(payload)).encode("ascii")),
            ],
            payload,
        )

    resp_cls_name = type(response).__name__
    raw_headers = getattr(response, "raw_headers", None)
    resp_body = getattr(response, "body", None)
    background = getattr(response, "background", None)

    if (
        isinstance(raw_headers, list)
        and isinstance(resp_body, (bytes, memoryview))
        and background is None
        and resp_cls_name not in {"StreamingResponse", "FileResponse", "_StreamingResponse"}
    ):
        status_code = int(getattr(response, "status_code", 200))
        return status_code, list(raw_headers), bytes(resp_body)

    if callable(response):
        await response(scope, receive, send)
        return None

    payload_bytes = encode_json_bytes(response)
    return (
        200,
        [
            (b"content-type", b"application/json"),
            (b"content-length", str(len(payload_bytes)).encode("ascii")),
        ],
        payload_bytes,
    )


class _FallbackStarletteRequest:
    """Lightweight Starlette Request fallback used when starlette is not installed."""

    __slots__ = ("_body", "_json", "_receive", "_send", "scope")

    def __init__(self, scope: Any, receive: Any = None, send: Any = None) -> None:
        self.scope = scope
        self._receive = receive
        self._send = send
        self._body: bytes = b""
        self._json: Any = None

    @property
    def method(self) -> str:
        return str(self.scope.get("method", "GET"))

    @property
    def path_params(self) -> dict[str, Any]:
        params: dict[str, Any] = self.scope.get("path_params") or {}
        return params

    @property
    def query_params(self) -> dict[str, str]:
        raw = self.scope.get("query_string", b"")
        text = raw.decode("latin-1") if isinstance(raw, bytes) else str(raw)
        if not text:
            return {}
        from urllib.parse import parse_qsl

        return dict(parse_qsl(text, keep_blank_values=True))

    async def body(self) -> bytes:
        return self._body

    async def json(self) -> Any:
        if self._json is None:
            self._json = decode_json(self._body)
        return self._json


def _get_starlette_request_cls() -> Any:
    req_mod = import_optional("starlette.requests")
    if req_mod is not None and hasattr(req_mod, "Request"):
        return req_mod.Request
    return _FallbackStarletteRequest


def _has_custom_exception_handlers(app: Any) -> bool:
    exc_handlers = getattr(app, "exception_handlers", None)
    if not isinstance(exc_handlers, dict) or not exc_handlers:
        return False
    for fn in exc_handlers.values():
        mod_name = str(getattr(fn, "__module__", "") or "")
        if not mod_name.startswith(("starlette.", "fastapi.")):
            return True
    return False


def _is_async_endpoint(endpoint: Any) -> bool:
    if inspect.iscoroutinefunction(endpoint):
        return True
    if callable(endpoint):
        return inspect.iscoroutinefunction(type(endpoint).__call__)
    return False


def _compile_routes_from_table(
    router_table: RouterTable,
    app: Any,
) -> dict[int, CompiledRoute]:
    has_mw = has_non_offloadable_middleware(app) if app is not None else False
    has_exc = _has_custom_exception_handlers(app) if app is not None else False
    compiled: dict[int, CompiledRoute] = {}
    for handler_id, entry in router_table.routes.items():
        compiled[handler_id] = CompiledRoute(
            handler_id=handler_id,
            method=entry.method,
            path_template=entry.path_template,
            route=entry.route,
            endpoint=entry.endpoint,
            is_async=_is_async_endpoint(entry.endpoint),
            has_middleware=has_mw,
            has_custom_exception_handlers=has_exc,
        )
    return compiled


async def _resolve_request_body(
    scope: Any,
    receive: Any,
    body: bytes | None,
    *,
    method: str,
) -> bytes:
    if body is not None:
        return body
    body_bytes_fn = getattr(scope, "body_bytes", None)
    if callable(body_bytes_fn):
        snap_bytes: bytes = body_bytes_fn()
        if snap_bytes:
            return snap_bytes
    if method in _BODYLESS_HTTP_METHODS or receive is None:
        return b""
    chunks: list[bytes] = []
    while True:
        message = await receive()
        if not isinstance(message, dict):
            break
        msg_type = message.get("type")
        if msg_type == "http.disconnect":
            break
        chunk = message.get("body", b"")
        if chunk:
            chunks.append(bytes(chunk))
        if not message.get("more_body", False):
            break
    return b"".join(chunks) if len(chunks) != 1 else chunks[0]


def _serialize_starlette_http_exception(exc: Exception) -> tuple[int, list[tuple[bytes, bytes]], bytes] | None:
    status_code = getattr(exc, "status_code", None)
    detail = getattr(exc, "detail", None)
    if not isinstance(status_code, int):
        return None
    headers: list[tuple[bytes, bytes]] = []
    raw_hdrs = getattr(exc, "headers", None)
    if isinstance(raw_hdrs, dict):
        for k, v in raw_hdrs.items():
            headers.append((str(k).lower().encode("latin-1"), str(v).encode("latin-1")))
    if isinstance(detail, str):
        body = detail.encode("utf-8")
        headers.append((b"content-type", b"text/plain; charset=utf-8"))
    else:
        body = encode_json_bytes({"detail": detail})
        headers.append((b"content-type", b"application/json"))
    headers.append((b"content-length", str(len(body)).encode("ascii")))
    return status_code, headers, body
