"""Fused direct Starlette endpoint execution, 24-slot AsgiScope population, and Request._body pre-seeding."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, Self, cast

import anyio.to_thread
import msgspec

from vertebra.extensions._web.fastpath_base import (
    BUFFERED_RESPONSE_TYPES,
    get_starlette_request_cls,
    preseed_request_body,
    serialize_buffered_response,
)
from vertebra.extensions.starlette.offload import has_non_offloadable_middleware
from vertebra.extensions.starlette.router import (
    RouterTable,
    extract_routes,
)
from vertebra.server._fastpath import (
    extract_scope_body_fast,
    resolve_relative_path,
    serialize_fastpath_special_result,
    wrap_asgi_fastpath,
)
from vertebra.server._fastpath_core import (
    CompiledRouteBase,
    has_custom_exception_handlers,
    serialize_starlette_http_exception,
)
from vertebra.utils.serializers import encode_json_bytes
from vertebra.utils.type_guards import is_coroutine_callable

__all__ = (
    "BUFFERED_RESPONSE_TYPES",
    "CompiledRoute",
    "FastPath",
    "compile_fastpath_routes",
    "dispatch_handler_direct",
    "get_starlette_request_cls",
    "preseed_request_body",
    "serialize_buffered_response",
)


class CompiledRoute(CompiledRouteBase, frozen=True, gc=False, kw_only=True):
    """Pre-compiled per-route dispatch metadata for Starlette endpoint invocation."""

    method: str
    path_template: str
    route: Any
    endpoint: Any
    has_middleware: bool
    has_custom_exception_handlers: bool = False
    sync_to_thread: bool = False

    @property
    def is_bypass_eligible(self) -> bool:
        """Return True when outer Starlette middleware and exception stacks can be bypassed safely."""
        return not self.has_middleware and not self.has_custom_exception_handlers


class FastPath:
    """Fast-path bypass router dispatching directly to compiled Starlette routes."""

    __slots__ = ("_app", "_compiled_routes", "_mount_prefix", "_router_table", "_sync_to_thread")

    def __init__(
        self,
        router_table: Any = None,
        *,
        app: Any = None,
        compiled_routes: dict[int, CompiledRoute] | None = None,
        mount_prefix: str = "",
        sync_to_thread: bool = False,
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
        self._mount_prefix = mount_prefix
        self._sync_to_thread = sync_to_thread
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
    def from_app(cls, app: Any, *, mount_prefix: str = "", sync_to_thread: bool = False) -> Self:
        """Construct a bound FastPath directly from a Starlette application."""
        table = extract_routes(app)
        compiled = compile_fastpath_routes(app, router_table=table)
        return cls(
            table,
            app=app,
            compiled_routes=compiled,
            mount_prefix=mount_prefix,
            sync_to_thread=sync_to_thread,
        )

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
        return wrap_asgi_fastpath(self, fallback_app)

    async def dispatch(
        self,
        scope: Any,
        receive: Any,
        send: Any,
        *,
        body: bytes | None = None,
    ) -> tuple[int, list[tuple[bytes, bytes]], bytes | memoryview] | None:
        """Attempt to dispatch the request directly via the compiled Starlette fastpath."""
        table = self._router_table
        if table is None or not table.routes:
            return None

        scope_type = scope.get("type", "http") if hasattr(scope, "get") else scope["type"]
        if scope_type != "http":
            return None

        method = str(scope.get("method", "GET") if hasattr(scope, "get") else scope["method"]).upper()
        path = resolve_relative_path(scope, self._mount_prefix)

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

        resolved_body = await extract_scope_body_fast(scope, receive, body, method=method)
        try:
            return await dispatch_handler_direct(
                compiled,
                scope=scope,
                body=resolved_body,
                receive=receive,
                send=send,
                sync_to_thread=self._sync_to_thread,
            )
        except Exception as exc:
            http_exc_resp = serialize_starlette_http_exception(exc)
            if http_exc_resp is not None:
                return http_exc_resp
            raise


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
    sync_to_thread: bool = False,
) -> tuple[int, list[tuple[bytes, bytes]], bytes | memoryview] | None:
    """Execute a middleware-free Starlette endpoint with pre-seeded Request._body."""
    req_cls = get_starlette_request_cls()
    request = req_cls(scope, receive=receive, send=send)
    preseed_request_body(scope, request, body)
    req_fn = getattr(scope, "request", None)
    if callable(req_fn):
        native_req = req_fn()
        scope["vertebra_request"] = native_req

    if compiled.is_async:
        response = await compiled.endpoint(request)
    elif compiled.sync_to_thread or sync_to_thread:
        response = await anyio.to_thread.run_sync(compiled.endpoint, request)
    else:
        response = compiled.endpoint(request)

    if response is None:
        return 204, [], b""

    special = serialize_fastpath_special_result(response, default_status=200)
    if special is not None:
        return special

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

    buffered = serialize_buffered_response(response, default_status=200)
    if buffered is not None:
        return buffered

    if callable(response) and not isinstance(response, type):
        await cast("Awaitable[Any]", response(scope, receive, send))
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


def _compile_routes_from_table(
    router_table: RouterTable,
    app: Any,
) -> dict[int, CompiledRoute]:
    has_mw = has_non_offloadable_middleware(app) if app is not None else False
    has_exc = (
        has_custom_exception_handlers(getattr(app, "exception_handlers", None), ("starlette.", "fastapi."))
        if app is not None
        else False
    )
    compiled: dict[int, CompiledRoute] = {}
    for handler_id, entry in router_table.routes.items():
        compiled[handler_id] = CompiledRoute(
            handler_id=handler_id,
            method=entry.method,
            path_template=entry.path_template,
            route=entry.route,
            endpoint=entry.endpoint,
            is_async=is_coroutine_callable(entry.endpoint),
            has_middleware=has_mw,
            has_custom_exception_handlers=has_exc,
            sync_to_thread=bool(getattr(entry.endpoint, "__vertebra_sync_to_thread__", False)),
        )
    return compiled
