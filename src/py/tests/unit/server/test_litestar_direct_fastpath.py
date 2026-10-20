"""Unit tests for pre-seeded ScopeState.body, dual v2/v3 direct HTTPRouteHandler dispatch, and Channels/Store swaps."""

import asyncio
import importlib
import inspect
from collections.abc import Callable, Sequence
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import msgspec
import pytest

from vertebra._vertebra import AsgiScope
from vertebra.extensions.litestar.channels import (
    VertebraChannelsBackend,
    swap_litestar_channels_backend,
)
from vertebra.extensions.litestar.store import (
    VertebraMemoryStore,
    swap_litestar_response_cache_store,
)
from vertebra.server._asgi._litestar_fastpath import (
    ScopeState,
    compile_litestar_fastpath_routes,
    dispatch_litestar_handler_direct,
    preseed_litestar_scope_body,
)


class CreateOrderPayload(msgspec.Struct):
    """Sample msgspec payload for testing DTO and body extraction."""

    sku: str
    quantity: int


def _load_litestar_fastpath_test_primitives() -> dict[str, Any]:
    """Load Litestar fastpath test classes or fallback doubles when optional extra is absent."""
    try:
        litestar_mod = importlib.import_module("litestar")
        ch_mod = importlib.import_module("litestar.channels")
        ch_mem_mod = importlib.import_module("litestar.channels.backends.memory")
        cache_mod = importlib.import_module("litestar.config.response_cache")
        conn_mod = importlib.import_module("litestar.connection")
        di_mod = importlib.import_module("litestar.di")
        dto_mod = importlib.import_module("litestar.dto")
        exc_mod = importlib.import_module("litestar.exceptions")
        resp_mod = importlib.import_module("litestar.response")
        store_mod = importlib.import_module("litestar.stores.memory")
        return {
            "Litestar": litestar_mod.Litestar,
            "Request": litestar_mod.Request,
            "get": litestar_mod.get,
            "post": litestar_mod.post,
            "ChannelsPlugin": ch_mod.ChannelsPlugin,
            "MemoryChannelsBackend": ch_mem_mod.MemoryChannelsBackend,
            "ResponseCacheConfig": cache_mod.ResponseCacheConfig,
            "ASGIConnection": conn_mod.ASGIConnection,
            "Provide": di_mod.Provide,
            "MsgspecDTO": dto_mod.MsgspecDTO,
            "NotAuthorizedException": exc_mod.NotAuthorizedException,
            "File": resp_mod.File,
            "ServerSentEvent": resp_mod.ServerSentEvent,
            "MemoryStore": store_mod.MemoryStore,
        }
    except ImportError:

        class NotAuthorizedExceptionStub(Exception):
            """Stub NotAuthorizedException."""

        class RequestStub:
            """Stub Litestar Request reading from pre-seeded ScopeState.body."""

            def __init__(self, scope: Any, receive: Any, send: Any) -> None:
                self.scope = scope
                self._receive = receive
                self._send = send
                raw_hdrs = scope.get("headers", [])
                self.headers = {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in raw_hdrs}

            async def body(self) -> bytes:
                conn_state = ScopeState.from_scope(self.scope)
                if conn_state.body is not None:
                    return bytes(conn_state.body)
                msg = await self._receive()
                return bytes(msg.get("body", b""))

        class ProvideStub:
            def __init__(self, dependency: Callable[..., Any], **_kwargs: Any) -> None:
                self.dependency = dependency

        class MsgspecDTOStub:
            def __class_getitem__(cls, item: Any) -> Any:
                return item

        class FileStub:
            def __init__(self, path: Path | str, **_kwargs: Any) -> None:
                self.path = Path(path)

        class ServerSentEventStub:
            def __init__(self, content: Any, *, ping_interval: int | None = None) -> None:
                self.content = content
                self.ping_interval = ping_interval

        class ASGIResponse:
            def __init__(
                self,
                *,
                body: bytes,
                status_code: int,
                headers: list[tuple[bytes, bytes]] | None = None,
            ) -> None:
                self.body = body
                self.status_code = status_code
                self._headers = headers or [(b"content-type", b"application/json")]

            def encode_headers(self) -> list[tuple[bytes, bytes]]:
                return list(self._headers)

        class ASGIFileResponse:
            def __init__(self, file_path: Path, status_code: int = 200) -> None:
                self.file_path = file_path
                self.status_code = status_code
                self.file_system = None

            def encode_headers(self) -> list[tuple[bytes, bytes]]:
                return [(b"content-type", b"text/plain")]

        class ASGIStreamingSSEResponse:
            def __init__(self, iterator: Any, ping_interval: int | None = None) -> None:
                self.iterator = iterator
                self._ping_interval = ping_interval
                self.status_code = 200

            def encode_headers(self) -> list[tuple[bytes, bytes]]:
                return [(b"content-type", b"text/event-stream")]

        class _RouteHandlerStub:
            def __init__(
                self,
                *,
                path: str,
                method: str,
                fn: Callable[..., Any],
                dto: Any = None,
                guards: Sequence[Callable[..., Any]] = (),
                dependencies: dict[str, Any] | None = None,
                after_response: Callable[..., Any] | None = None,
            ) -> None:
                self.paths = {path}
                self.http_methods = {method}
                self.fn = fn
                self.dto = dto
                self.guards = tuple(guards)
                self.middleware = ()
                self.opt: dict[str, Any] = {}
                self.dependencies = dict(dependencies or {})
                self.after_response = after_response
                self.request_class = RequestStub
                self.status_code = 201 if method == "POST" else 200

            async def authorize_connection(self, connection: Any) -> None:
                for guard in self.guards:
                    res = guard(connection, self)
                    if inspect.isawaitable(res):
                        await res

            async def _get_response_for_request(self, request: RequestStub) -> Any:
                sig = inspect.signature(self.fn)
                kwargs: dict[str, Any] = {}
                if "request" in sig.parameters:
                    kwargs["request"] = request
                if "data" in sig.parameters:
                    raw = await request.body()
                    kwargs["data"] = (
                        msgspec.json.decode(raw, type=self.dto) if self.dto is not None else msgspec.json.decode(raw)
                    )
                for dep_name, provider in self.dependencies.items():
                    if dep_name in sig.parameters:
                        dep_fn = getattr(provider, "dependency", provider)
                        dep_val = dep_fn()
                        kwargs[dep_name] = await dep_val if inspect.isawaitable(dep_val) else dep_val
                out = self.fn(**kwargs)
                if inspect.isawaitable(out):
                    out = await out
                if isinstance(out, FileStub):
                    return ASGIFileResponse(out.path, status_code=self.status_code)
                if isinstance(out, ServerSentEventStub):
                    return ASGIStreamingSSEResponse(out.content, ping_interval=out.ping_interval)
                if isinstance(out, bytes):
                    return ASGIResponse(body=out, status_code=self.status_code)
                return ASGIResponse(
                    body=msgspec.json.encode(out),
                    status_code=self.status_code,
                )

        def make_route_decorator(method: str) -> Callable[..., Callable[[Callable[..., Any]], Any]]:
            def decorator(
                path: str,
                *,
                dto: Any = None,
                guards: Sequence[Callable[..., Any]] = (),
                dependencies: dict[str, Any] | None = None,
                after_response: Callable[..., Any] | None = None,
                **_kwargs: Any,
            ) -> Callable[[Callable[..., Any]], Any]:
                def wrap(fn: Callable[..., Any]) -> Any:
                    return _RouteHandlerStub(
                        path=path,
                        method=method,
                        fn=fn,
                        dto=dto,
                        guards=guards,
                        dependencies=dependencies,
                        after_response=after_response,
                    )

                return wrap

            return decorator

        class MemoryChannelsBackend:
            def __init__(self, *, history: int = 0) -> None:
                self._history = history

        class ChannelsPlugin:
            def __init__(self, *, backend: Any, channels: Sequence[str] = ()) -> None:
                self._backend = backend
                self.channels = tuple(channels)

        class ResponseCacheConfig:
            def __init__(self, *, store: str = "response_cache") -> None:
                self.store = store

        class MemoryStore:
            """Stub MemoryStore."""

        class LitestarStub:
            def __init__(
                self,
                route_handlers: Sequence[Any] | None = None,
                *,
                plugins: Sequence[Any] | None = None,
                response_cache_config: Any = None,
                stores: dict[str, Any] | None = None,
                **_kwargs: Any,
            ) -> None:
                self.before_send = None
                self.request_class = RequestStub
                self.plugins = SimpleNamespace(plugins=list(plugins or ()))
                self.response_cache_config = response_cache_config
                self.stores = SimpleNamespace(_stores=dict(stores or {}))
                by_path: dict[str, dict[str, Any]] = {}
                for handler in route_handlers or ():
                    for path in getattr(handler, "paths", ("/",)):
                        method_map = by_path.setdefault(path, {})
                        for method in getattr(handler, "http_methods", ("GET",)):
                            method_map[method] = handler
                self.routes = [
                    SimpleNamespace(path=path, route_handler_map=method_map, path_parameters=())
                    for path, method_map in by_path.items()
                ]

        return {
            "Litestar": LitestarStub,
            "Request": RequestStub,
            "get": make_route_decorator("GET"),
            "post": make_route_decorator("POST"),
            "ChannelsPlugin": ChannelsPlugin,
            "MemoryChannelsBackend": MemoryChannelsBackend,
            "ResponseCacheConfig": ResponseCacheConfig,
            "ASGIConnection": RequestStub,
            "Provide": ProvideStub,
            "MsgspecDTO": MsgspecDTOStub,
            "NotAuthorizedException": NotAuthorizedExceptionStub,
            "File": FileStub,
            "ServerSentEvent": ServerSentEventStub,
            "MemoryStore": MemoryStore,
        }


_PRIMS = _load_litestar_fastpath_test_primitives()
Litestar = _PRIMS["Litestar"]
Request = _PRIMS["Request"]
get = _PRIMS["get"]
post = _PRIMS["post"]
ChannelsPlugin = _PRIMS["ChannelsPlugin"]
MemoryChannelsBackend = _PRIMS["MemoryChannelsBackend"]
ResponseCacheConfig = _PRIMS["ResponseCacheConfig"]
ASGIConnection = _PRIMS["ASGIConnection"]
Provide = _PRIMS["Provide"]
MsgspecDTO = _PRIMS["MsgspecDTO"]
NotAuthorizedException = _PRIMS["NotAuthorizedException"]
File = _PRIMS["File"]
ServerSentEvent = _PRIMS["ServerSentEvent"]
MemoryStore = _PRIMS["MemoryStore"]


def _make_scope(
    app: Any,
    *,
    method: str,
    path: str,
    route_handler: Any,
    path_params: dict[str, Any] | None = None,
    headers: list[tuple[bytes, bytes]] | None = None,
) -> AsgiScope:
    """Build a 20-slot AsgiScope populated with Litestar per-request keys."""
    scope = AsgiScope()
    scope["method"] = method
    scope["path"] = path
    scope["raw_path"] = path.encode("latin-1")
    scope["query_string"] = b""
    scope["headers"] = headers or [(b"content-type", b"application/json")]
    scope["app"] = app
    scope["litestar_app"] = app
    scope["path_params"] = path_params or {}
    scope["route_handler"] = route_handler
    scope["path_template"] = path
    return scope


@pytest.mark.anyio
async def test_preseed_scope_state_body_zero_receive_calls() -> None:
    """Verify preseed_litestar_scope_body allows await request.body() without calling receive()."""

    @post("/echo", sync_to_thread=False)
    async def echo_handler(request: Any) -> bytes:
        body_bytes: bytes = await request.body()
        return body_bytes

    app = Litestar(route_handlers=[echo_handler])
    compiled_map = compile_litestar_fastpath_routes(app)
    compiled = next(c for c in compiled_map.values() if c.path_template == "/echo")

    receive_calls = 0

    async def forbidden_receive() -> dict[str, Any]:
        nonlocal receive_calls
        receive_calls += 1
        raise AssertionError("AsgiReceive must not be called when ScopeState.body is pre-seeded")

    async def noop_send(message: dict[str, Any]) -> None:
        _ = message

    payload = b'{"hello":"world"}'
    scope = _make_scope(app, method="POST", path="/echo", route_handler=compiled.route_handler)
    preseed_litestar_scope_body(scope, payload)

    result = await dispatch_litestar_handler_direct(
        compiled,
        scope=scope,
        body=payload,
        receive=forbidden_receive,
        send=noop_send,
    )
    assert receive_calls == 0
    assert result is not None
    status, _headers, raw_body = result
    assert status == 201
    assert raw_body == payload
    assert scope["connection"] is not None


@pytest.mark.anyio
async def test_direct_dispatch_preserves_di_dto_guards_and_after_response() -> None:
    """Verify fused direct dispatch executes Litestar DI, MsgspecDTO, guards, and after_response hooks."""
    after_response_paths: list[str] = []

    async def record_after_response(request: Any) -> None:
        after_response_paths.append(request.scope["path"])

    def require_token_guard(connection: Any, _: Any) -> None:
        if connection.headers.get("x-api-token") != "secret":
            raise NotAuthorizedException("missing secret token")

    def provide_multiplier() -> int:
        return 3

    @post(
        "/orders",
        dto=MsgspecDTO[CreateOrderPayload],
        guards=[require_token_guard],
        dependencies={"multiplier": Provide(provide_multiplier, sync_to_thread=False)},
        after_response=record_after_response,
    )
    async def create_order(data: CreateOrderPayload, multiplier: int) -> dict[str, Any]:
        return {"sku": data.sku, "total": data.quantity * multiplier}

    app = Litestar(route_handlers=[create_order])
    compiled_map = compile_litestar_fastpath_routes(app)
    compiled = next(c for c in compiled_map.values() if c.path_template == "/orders")

    body = msgspec.json.encode({"sku": "A100", "quantity": 4})

    async def noop_receive() -> dict[str, Any]:
        return {"type": "http.request", "body": body, "more_body": False}

    async def noop_send(message: dict[str, Any]) -> None:
        _ = message

    auth_scope = _make_scope(
        app,
        method="POST",
        path="/orders",
        route_handler=compiled.route_handler,
        headers=[
            (b"content-type", b"application/json"),
            (b"x-api-token", b"secret"),
        ],
    )
    result = await dispatch_litestar_handler_direct(
        compiled,
        scope=auth_scope,
        body=body,
        receive=noop_receive,
        send=noop_send,
    )
    assert result is not None
    status, _headers, raw_body = result
    assert status == 201
    assert msgspec.json.decode(raw_body) == {"sku": "A100", "total": 12}
    assert after_response_paths == ["/orders"]

    unauth_scope = _make_scope(
        app,
        method="POST",
        path="/orders",
        route_handler=compiled.route_handler,
        headers=[(b"content-type", b"application/json")],
    )
    with pytest.raises(NotAuthorizedException):
        await dispatch_litestar_handler_direct(
            compiled,
            scope=unauth_scope,
            body=body,
            receive=noop_receive,
            send=noop_send,
        )


@pytest.mark.anyio
async def test_direct_dispatch_file_pathsend_and_sse_ping_offload(tmp_path: Path) -> None:
    """Verify ASGIFileResponse emits http.response.pathsend and ASGIStreamingSSEResponse offloads _ping_interval."""
    sample_file = tmp_path / "report.txt"
    sample_file.write_text("zero-copy file body", encoding="utf-8")

    @get("/download", sync_to_thread=False)
    def download_report() -> Any:
        return File(path=sample_file)

    @get("/events", sync_to_thread=False)
    def stream_events() -> Any:
        return ServerSentEvent(iter(["tick-1", "tick-2"]), ping_interval=15)

    app = Litestar(route_handlers=[download_report, stream_events])
    compiled_map = compile_litestar_fastpath_routes(app)
    dl_compiled = next(c for c in compiled_map.values() if c.path_template == "/download")
    sse_compiled = next(c for c in compiled_map.values() if c.path_template == "/events")

    sent_messages: list[dict[str, Any]] = []

    async def noop_receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def capture_send(message: dict[str, Any]) -> None:
        sent_messages.append(message)

    dl_scope = _make_scope(app, method="GET", path="/download", route_handler=dl_compiled.route_handler)
    dl_res = await dispatch_litestar_handler_direct(
        dl_compiled,
        scope=dl_scope,
        body=b"",
        receive=noop_receive,
        send=capture_send,
    )
    assert dl_res is None
    assert any(m["type"] == "http.response.pathsend" and m["path"] == str(sample_file) for m in sent_messages)

    sent_messages.clear()
    sse_scope = _make_scope(app, method="GET", path="/events", route_handler=sse_compiled.route_handler)
    sse_res = await dispatch_litestar_handler_direct(
        sse_compiled,
        scope=sse_scope,
        body=b"",
        receive=noop_receive,
        send=capture_send,
    )
    assert sse_res is None
    assert any(m["type"] == "http.response.sse" and m.get("ping_interval_s") == 15 for m in sent_messages)


@pytest.mark.anyio
async def test_swap_channels_backend_and_response_cache_store() -> None:
    """Verify MemoryChannelsBackend and MemoryStore are transparently swapped for Vertebra implementations."""

    @get("/cached", cache=True, sync_to_thread=False)
    def get_cached() -> dict[str, str]:
        return {"cached": "yes"}

    channels_plugin = ChannelsPlugin(backend=MemoryChannelsBackend(history=10), channels=["alerts"])
    cache_config = ResponseCacheConfig(store="response_cache")
    app = Litestar(
        route_handlers=[get_cached],
        plugins=[channels_plugin],
        response_cache_config=cache_config,
        stores={"response_cache": MemoryStore()},
    )

    swapped_channels = swap_litestar_channels_backend(app)
    assert isinstance(swapped_channels, VertebraChannelsBackend)
    await swapped_channels.on_startup()
    await swapped_channels.subscribe(["alerts"])
    await swapped_channels.publish(b"alert-1", ["alerts"])
    history = await swapped_channels.get_history("alerts")
    assert history == [b"alert-1"]

    received_events: list[tuple[str, bytes]] = []
    stream_started = asyncio.Event()

    async def _consume_events() -> None:
        stream_started.set()
        async for ch, payload in swapped_channels.stream_events():
            received_events.append((ch, payload))

    consumer_task = asyncio.create_task(_consume_events())
    await stream_started.wait()
    await asyncio.sleep(0)

    await swapped_channels.subscribe(["dynamic_topic"])
    await swapped_channels.publish(b"dyn-1", ["dynamic_topic"])
    await asyncio.sleep(0)
    assert ("dynamic_topic", b"dyn-1") in received_events

    await swapped_channels.unsubscribe(["dynamic_topic"])
    await swapped_channels.publish(b"dyn-2", ["dynamic_topic"])
    await asyncio.sleep(0)
    assert ("dynamic_topic", b"dyn-2") not in received_events

    await swapped_channels.unsubscribe(["alerts"])
    await swapped_channels.on_shutdown()
    await asyncio.wait_for(consumer_task, timeout=1.0)

    swapped_store = swap_litestar_response_cache_store(app)
    assert isinstance(swapped_store, VertebraMemoryStore)
    async with swapped_store as ctx_store:
        assert ctx_store is swapped_store
        await swapped_store.set("k1", b"v1", expires_in=60)
        assert await swapped_store.exists("k1") is True
        assert await swapped_store.get("k1") == b"v1"
        ttl = await swapped_store.expires_in("k1")
        assert ttl is not None and ttl > 0
        await swapped_store.delete("k1")
        assert await swapped_store.get("k1") is None
