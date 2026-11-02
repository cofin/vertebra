"""Unit tests for pre-seeded ScopeState.body, dual v2/v3 direct HTTPRouteHandler dispatch, and Channels/Store swaps."""

import asyncio
import inspect
from collections.abc import Callable, Sequence
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import msgspec
import pytest

from vertebra._vertebra import AsgiProtocol, AsgiScope
from vertebra.extensions.litestar.channels import (
    ChannelsBackend,
    swap_channels_backend,
)
from vertebra.extensions.litestar.config import LitestarConfig
from vertebra.extensions.litestar.fastpath import (
    FastPath,
    ScopeState,
    _is_empty_sentinel,
    compile_fastpath_routes,
    dispatch_handler_direct,
    preseed_scope_body,
)
from vertebra.extensions.litestar.router import extract_routes
from vertebra.extensions.litestar.store import (
    MemoryStore,
    swap_response_cache_store,
)
from vertebra.typing import Empty
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import decode_json, encode_json_bytes


class CreateOrderPayload(msgspec.Struct, frozen=True, gc=False):
    """Sample msgspec payload for testing DTO and body extraction."""

    sku: str
    quantity: int


def _load_litestar_fastpath_test_primitives() -> dict[str, Any]:
    """Load Litestar fastpath test classes or fallback doubles when optional extra is absent."""
    litestar_mod = import_optional("litestar")
    ch_mod = import_optional("litestar.channels")
    ch_mem_mod = import_optional("litestar.channels.backends.memory")
    cache_mod = import_optional("litestar.config.response_cache")
    comp_mod = import_optional("litestar.config.compression")
    cors_mod = import_optional("litestar.config.cors")
    csrf_mod = import_optional("litestar.config.csrf")
    conn_mod = import_optional("litestar.connection")
    di_mod = import_optional("litestar.di")
    dto_mod = import_optional("litestar.dto")
    exc_mod = import_optional("litestar.exceptions")
    params_mod = import_optional("litestar.params")
    resp_mod = import_optional("litestar.response")
    store_mod = import_optional("litestar.stores.memory")
    if (
        litestar_mod is not None
        and ch_mod is not None
        and ch_mem_mod is not None
        and cache_mod is not None
        and comp_mod is not None
        and cors_mod is not None
        and csrf_mod is not None
        and conn_mod is not None
        and di_mod is not None
        and dto_mod is not None
        and exc_mod is not None
        and params_mod is not None
        and resp_mod is not None
        and store_mod is not None
    ):

        class _TestServerSentEvent(resp_mod.ServerSentEvent):
            """Litestar ServerSentEvent wrapper preserving x-vertebra-sse-ping-interval header."""

            def __init__(self, content: Any, *args: Any, ping_interval: int | None = None, **kwargs: Any) -> None:
                hdrs = dict(kwargs.pop("headers", None) or {})
                if ping_interval is not None:
                    hdrs["x-vertebra-sse-ping-interval"] = str(ping_interval)
                super().__init__(content, *args, headers=hdrs, **kwargs)
                self._ping_interval = ping_interval

            def __setattr__(self, name: str, value: Any) -> None:
                super().__setattr__(name, value)
                if name == "_ping_interval" and value is not None and hasattr(self, "headers"):
                    self.headers["x-vertebra-sse-ping-interval"] = str(value)

        return {
            "Litestar": litestar_mod.Litestar,
            "Request": litestar_mod.Request,
            "Router": litestar_mod.Router,
            "get": litestar_mod.get,
            "post": litestar_mod.post,
            "ChannelsPlugin": ch_mod.ChannelsPlugin,
            "MemoryChannelsBackend": ch_mem_mod.MemoryChannelsBackend,
            "ResponseCacheConfig": cache_mod.ResponseCacheConfig,
            "CompressionConfig": comp_mod.CompressionConfig,
            "CORSConfig": cors_mod.CORSConfig,
            "CSRFConfig": csrf_mod.CSRFConfig,
            "ASGIConnection": conn_mod.ASGIConnection,
            "Provide": di_mod.Provide,
            "NamedDependency": di_mod.NamedDependency,
            "FromPath": params_mod.FromPath,
            "FromQuery": params_mod.FromQuery,
            "JSONBody": params_mod.JSONBody,
            "MsgspecDTO": dto_mod.MsgspecDTO,
            "NotAuthorizedException": exc_mod.NotAuthorizedException,
            "File": resp_mod.File,
            "ServerSentEvent": _TestServerSentEvent,
            "MemoryStore": store_mod.MemoryStore,
        }

    class NotAuthorizedExceptionStub(Exception):
        """Stub NotAuthorizedException matching Litestar HTTPException attributes."""

        def __init__(
            self,
            detail: str = "Not Authorized",
            *,
            status_code: int = 401,
            headers: dict[str, str] | None = None,
            extra: dict[str, Any] | list[Any] | None = None,
        ) -> None:
            super().__init__(detail)
            self.detail = detail
            self.status_code = status_code
            self.headers = headers
            self.extra = extra

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
            if conn_state.body is not Empty and conn_state.body is not None:
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
            middleware: Sequence[Any] = (),
            dependencies: dict[str, Any] | None = None,
            after_response: Callable[..., Any] | None = None,
            extra_attrs: dict[str, Any] | None = None,
        ) -> None:
            self.paths = {path}
            self.http_methods = {method}
            self.fn = fn
            self.dto = dto
            self.guards = tuple(guards)
            self.middleware = tuple(middleware)
            self.opt: dict[str, Any] = {}
            self.dependencies = dict(dependencies or {})
            self.after_response = after_response
            self.request_class = RequestStub
            self.status_code = 201 if method == "POST" else 200
            for attr_key, attr_val in (extra_attrs or {}).items():
                setattr(self, attr_key, attr_val)

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
                decoded = decode_json(raw)
                kwargs["data"] = msgspec.convert(decoded, type=self.dto) if self.dto is not None else decoded
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
                body=encode_json_bytes(out),
                status_code=self.status_code,
            )

    def make_route_decorator(method: str) -> Callable[..., Callable[[Callable[..., Any]], Any]]:
        def decorator(
            path: str,
            *,
            dto: Any = None,
            guards: Sequence[Callable[..., Any]] = (),
            middleware: Sequence[Any] = (),
            dependencies: dict[str, Any] | None = None,
            after_response: Callable[..., Any] | None = None,
            **kwargs: Any,
        ) -> Callable[[Callable[..., Any]], Any]:
            extra_attrs = {k: v for k, v in kwargs.items() if k != "sync_to_thread"}

            def wrap(fn: Callable[..., Any]) -> Any:
                return _RouteHandlerStub(
                    path=path,
                    method=method,
                    fn=fn,
                    dto=dto,
                    guards=guards,
                    middleware=middleware,
                    dependencies=dependencies,
                    after_response=after_response,
                    extra_attrs=extra_attrs,
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

    class CompressionConfigStub(msgspec.Struct, frozen=True, gc=False):
        backend: str = "gzip"
        minimum_size: int = 500

    class CORSConfigStub(msgspec.Struct, frozen=True, gc=False):
        allow_origins: Sequence[str] = ("*",)

    class CSRFConfigStub(msgspec.Struct, frozen=True, gc=False):
        secret: str = "test-csrf-secret"

    class LitestarStub:
        def __init__(
            self,
            route_handlers: Sequence[Any] | None = None,
            *,
            plugins: Sequence[Any] | None = None,
            response_cache_config: Any = None,
            stores: dict[str, Any] | None = None,
            **kwargs: Any,
        ) -> None:
            self.before_send = kwargs.get("before_send")
            self.after_exception = kwargs.get("after_exception")
            self.csrf_config = kwargs.get("csrf_config")
            self.allowed_hosts = kwargs.get("allowed_hosts")
            self.compression_config = kwargs.get("compression_config")
            self.cors_config = kwargs.get("cors_config")
            self.request_max_body_size = kwargs.get("request_max_body_size")
            self.state: dict[str, Any] = dict(kwargs.get("state") or {})
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
        "CompressionConfig": CompressionConfigStub,
        "CORSConfig": CORSConfigStub,
        "CSRFConfig": CSRFConfigStub,
        "ASGIConnection": RequestStub,
        "Provide": ProvideStub,
        "NamedDependency": MsgspecDTOStub,
        "FromPath": MsgspecDTOStub,
        "FromQuery": MsgspecDTOStub,
        "JSONBody": MsgspecDTOStub,
        "MsgspecDTO": MsgspecDTOStub,
        "NotAuthorizedException": NotAuthorizedExceptionStub,
        "File": FileStub,
        "ServerSentEvent": ServerSentEventStub,
        "MemoryStore": MemoryStore,
        "Router": None,
    }


_PRIMS = _load_litestar_fastpath_test_primitives()
if TYPE_CHECKING:
    from litestar import Litestar, Request, Router, get, post
    from litestar.channels import ChannelsPlugin
    from litestar.channels.backends.memory import MemoryChannelsBackend
    from litestar.config.compression import CompressionConfig
    from litestar.config.cors import CORSConfig
    from litestar.config.csrf import CSRFConfig
    from litestar.config.response_cache import ResponseCacheConfig
    from litestar.connection import ASGIConnection
    from litestar.di import NamedDependency, Provide
    from litestar.dto import MsgspecDTO
    from litestar.exceptions import NotAuthorizedException
    from litestar.params import FromPath, FromQuery, JSONBody
    from litestar.response import File, ServerSentEvent
    from litestar.stores.memory import MemoryStore as LitestarMemoryStore
else:
    Litestar = _PRIMS["Litestar"]
    Request = _PRIMS["Request"]
    Router = _PRIMS.get("Router")
    get = _PRIMS["get"]
    post = _PRIMS["post"]
    ChannelsPlugin = _PRIMS["ChannelsPlugin"]
    MemoryChannelsBackend = _PRIMS["MemoryChannelsBackend"]
    ResponseCacheConfig = _PRIMS["ResponseCacheConfig"]
    CompressionConfig = _PRIMS["CompressionConfig"]
    CORSConfig = _PRIMS["CORSConfig"]
    CSRFConfig = _PRIMS["CSRFConfig"]
    ASGIConnection = _PRIMS["ASGIConnection"]
    Provide = _PRIMS["Provide"]
    NamedDependency = _PRIMS["NamedDependency"]
    FromPath = _PRIMS["FromPath"]
    FromQuery = _PRIMS["FromQuery"]
    JSONBody = _PRIMS["JSONBody"]
    MsgspecDTO = _PRIMS["MsgspecDTO"]
    NotAuthorizedException = _PRIMS["NotAuthorizedException"]
    File = _PRIMS["File"]
    ServerSentEvent = _PRIMS["ServerSentEvent"]
    LitestarMemoryStore = _PRIMS["MemoryStore"]


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

    @post("/echo")
    async def echo_handler(request: Request[Any, Any, Any]) -> bytes:
        body_bytes: bytes = await request.body()
        return body_bytes

    app = Litestar(route_handlers=[echo_handler])
    compiled_map = compile_fastpath_routes(app)
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
    preseed_scope_body(scope, payload)

    result = await dispatch_handler_direct(
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
    async def create_order(
        data: JSONBody[CreateOrderPayload],
        multiplier: NamedDependency[int],
    ) -> dict[str, Any]:
        return {"sku": data.sku, "total": data.quantity * multiplier}

    app = Litestar(route_handlers=[create_order])
    compiled_map = compile_fastpath_routes(app)
    compiled = next(c for c in compiled_map.values() if c.path_template == "/orders")

    body = encode_json_bytes({"sku": "A100", "quantity": 4})

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
    result = await dispatch_handler_direct(
        compiled,
        scope=auth_scope,
        body=body,
        receive=noop_receive,
        send=noop_send,
    )
    assert result is not None
    status, _headers, raw_body = result
    assert status == 201
    assert decode_json(raw_body) == {"sku": "A100", "total": 12}
    assert after_response_paths == ["/orders"]

    unauth_scope = _make_scope(
        app,
        method="POST",
        path="/orders",
        route_handler=compiled.route_handler,
        headers=[(b"content-type", b"application/json")],
    )
    with pytest.raises(NotAuthorizedException):
        await dispatch_handler_direct(
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
        sse = ServerSentEvent(iter(["tick-1", "tick-2"]))
        sse._ping_interval = 15
        return sse

    app = Litestar(route_handlers=[download_report, stream_events])
    compiled_map = compile_fastpath_routes(app)
    dl_compiled = next(c for c in compiled_map.values() if c.path_template == "/download")
    sse_compiled = next(c for c in compiled_map.values() if c.path_template == "/events")

    sent_messages: list[dict[str, Any]] = []

    async def noop_receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def capture_send(message: dict[str, Any]) -> None:
        sent_messages.append(message)

    dl_scope = _make_scope(app, method="GET", path="/download", route_handler=dl_compiled.route_handler)
    dl_res = await dispatch_handler_direct(
        dl_compiled,
        scope=dl_scope,
        body=b"",
        receive=noop_receive,
        send=capture_send,
    )
    assert dl_res is None
    assert sent_messages[0]["type"] == "http.response.start"
    assert sent_messages[0]["status"] == 200
    assert sent_messages[1]["type"] == "http.response.pathsend"
    assert sent_messages[1]["path"] == str(sample_file)

    sent_messages.clear()
    sse_scope = _make_scope(app, method="GET", path="/events", route_handler=sse_compiled.route_handler)

    async def sse_receive() -> dict[str, Any]:
        await asyncio.sleep(100)
        return {"type": "http.disconnect"}

    sse_res = await dispatch_handler_direct(
        sse_compiled,
        scope=sse_scope,
        body=b"",
        receive=sse_receive,
        send=capture_send,
    )
    assert sse_res is None
    assert sent_messages[0]["type"] == "http.response.start"
    assert sent_messages[0]["status"] == 200
    assert (b"x-vertebra-sse-ping-interval", b"15") in sent_messages[0]["headers"]
    assert any(m["type"] == "http.response.sse" and m.get("ping_interval_s") == 15 for m in sent_messages)
    assert sent_messages[-1] == {"type": "http.response.body", "body": b"", "more_body": False}

    dl_proto = AsgiProtocol(b"")
    dl_proto_scope = _make_scope(app, method="GET", path="/download", route_handler=dl_compiled.route_handler)
    await dispatch_handler_direct(
        dl_compiled,
        scope=dl_proto_scope,
        body=b"",
        receive=dl_proto.receive,
        send=dl_proto.send,
    )
    assert dl_proto.started is True
    assert dl_proto.complete is True
    assert dl_proto.status == 200
    dl_proto_resp = dl_proto.take_response()
    assert dl_proto_resp.body == b"zero-copy file body"

    @get("/events-proto", sync_to_thread=False)
    def stream_events_proto() -> Any:
        return ServerSentEvent(iter(["tick-1", "tick-2"]), ping_interval=15)

    proto_sse_app = Litestar(route_handlers=[stream_events_proto])
    proto_sse_compiled = next(
        c for c in compile_fastpath_routes(proto_sse_app).values() if c.path_template == "/events-proto"
    )
    sse_proto = AsgiProtocol(b"")
    sse_proto_scope = _make_scope(
        proto_sse_app,
        method="GET",
        path="/events-proto",
        route_handler=proto_sse_compiled.route_handler,
    )
    await dispatch_handler_direct(
        proto_sse_compiled,
        scope=sse_proto_scope,
        body=b"",
        receive=sse_proto.receive,
        send=sse_proto.send,
    )
    assert sse_proto.started is True
    assert sse_proto.complete is True
    assert sse_proto.status == 200
    sse_proto_resp = sse_proto.take_response()
    assert (b"x-vertebra-sse-ping-interval", b"15") in [(k.lower(), v) for k, v in sse_proto_resp.headers]
    assert b"tick-1" in sse_proto_resp.body and b"tick-2" in sse_proto_resp.body


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
        stores={"response_cache": LitestarMemoryStore()},
    )

    swapped_channels = swap_channels_backend(app)
    assert isinstance(swapped_channels, ChannelsBackend)
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

    swapped_store = swap_response_cache_store(app)
    assert isinstance(swapped_store, MemoryStore)
    async with swapped_store as ctx_store:
        assert ctx_store is swapped_store
        await swapped_store.set("k1", b"v1", expires_in=60)
        assert await swapped_store.exists("k1") is True
        assert await swapped_store.get("k1") == b"v1"
        ttl = await swapped_store.expires_in("k1")
        assert ttl is not None and ttl > 0
        await swapped_store.delete("k1")
        assert await swapped_store.get("k1") is None


@pytest.mark.anyio
async def test_fastpath_bypass_dispatches_eligible_sync_and_async_routes_and_skips_ineligible() -> None:
    """Verify LitestarFastPathBypass.dispatch runs eligible sync/async routes directly and returns None for ineligible routes."""

    def sample_guard(_conn: Any, _handler: Any) -> None:
        return None

    def noop_middleware(app: Any) -> Any:
        async def middleware(scope: Any, receive: Any, send: Any) -> None:
            await app(scope, receive, send)

        return middleware

    @get("/items/{item_id:int}", sync_to_thread=False)
    def get_item(item_id: FromPath[int], limit: FromQuery[int] = 5) -> dict[str, int]:
        return {"id": item_id, "limit": limit}

    @post("/fast-orders")
    async def create_fast_order(data: JSONBody[CreateOrderPayload]) -> dict[str, Any]:
        return {"sku": data.sku, "qty": data.quantity}

    @get("/guarded", guards=[sample_guard], sync_to_thread=False)
    def guarded_route() -> dict[str, str]:
        return {"ok": "guarded"}

    @get("/wrapped", middleware=[noop_middleware], sync_to_thread=False)
    def middleware_route() -> dict[str, str]:
        return {"ok": "wrapped"}

    app = Litestar(route_handlers=[get_item, create_fast_order, guarded_route, middleware_route])
    router_table = extract_routes(app)
    bypass = FastPath(router_table, app=app)

    async def forbidden_receive() -> dict[str, Any]:
        raise AssertionError("receive() must not be called on GET or pre-supplied POST body")

    async def noop_send(message: dict[str, Any]) -> None:
        _ = message

    get_scope = AsgiScope.from_parts(
        method="GET",
        path="/items/42",
        query_string="limit=15",
        headers=[(b"accept", b"application/json")],
    )
    get_res = await bypass.dispatch(get_scope, forbidden_receive, noop_send)
    assert get_res is not None
    status_get, headers_get, body_get = get_res
    assert status_get == 200
    assert (b"content-type", b"application/json") in [(k.lower(), v) for k, v in headers_get]
    assert decode_json(body_get) == {"id": 42, "limit": 15}

    post_scope = AsgiScope.from_parts(
        method="POST",
        path="/fast-orders",
        headers=[(b"content-type", b"application/json")],
    )
    post_payload = b'{"sku":"X1","quantity":7}'
    post_res = await bypass.dispatch(post_scope, forbidden_receive, noop_send, body=post_payload)
    assert post_res is not None
    status_post, _headers_post, body_post = post_res
    assert status_post == 201
    assert decode_json(body_post) == {"sku": "X1", "qty": 7}

    guarded_scope = AsgiScope.from_parts(method="GET", path="/guarded")
    assert await bypass.dispatch(guarded_scope, forbidden_receive, noop_send) is None

    mw_scope = AsgiScope.from_parts(method="GET", path="/wrapped")
    assert await bypass.dispatch(mw_scope, forbidden_receive, noop_send) is None

    missing_scope = AsgiScope.from_parts(method="GET", path="/does-not-exist")
    assert await bypass.dispatch(missing_scope, forbidden_receive, noop_send) is None

    ws_scope: dict[str, Any] = {"type": "websocket", "method": "GET", "path": "/items/42"}
    assert await bypass.dispatch(ws_scope, forbidden_receive, noop_send) is None


@pytest.mark.anyio
async def test_fastpath_wrap_asgi_replays_body_on_fallback_and_handles_empty_sentinel() -> None:
    """Verify ScopeState.body starts as Empty and wrap_asgi replays drained receive() chunks to fallback_app."""

    @post("/strict-order")
    async def strict_order(data: JSONBody[CreateOrderPayload]) -> dict[str, Any]:
        return {"sku": data.sku, "quantity": data.quantity}

    app = Litestar(route_handlers=[strict_order])
    bypass = FastPath.from_app(app)

    fresh_scope: dict[str, Any] = {
        "type": "http",
        "method": "POST",
        "path": "/strict-order",
        "query_string": b"",
        "headers": [(b"content-type", b"application/json")],
    }
    fresh_state = ScopeState.from_scope(fresh_scope)
    assert _is_empty_sentinel(fresh_state.body)

    invalid_payload = b'{"sku":"BAD","quantity":"not-an-int"}'
    chunks = [
        {"type": "http.request", "body": invalid_payload[:14], "more_body": True},
        {"type": "http.request", "body": invalid_payload[14:], "more_body": False},
    ]
    idx = 0

    async def chunked_receive() -> dict[str, Any]:
        nonlocal idx
        if idx < len(chunks):
            msg = chunks[idx]
            idx += 1
            return msg
        return {"type": "http.request", "body": b"", "more_body": False}

    fallback_bodies: list[bytes] = []
    sent_events: list[dict[str, Any]] = []

    async def fallback_app(scope: Any, receive: Any, send: Any) -> None:
        _ = scope
        parts: list[bytes] = []
        while True:
            msg = await receive()
            parts.append(msg.get("body", b""))
            if not msg.get("more_body", False):
                break
        fallback_bodies.append(b"".join(parts))
        await send({"type": "http.response.start", "status": 400, "headers": [(b"content-type", b"application/json")]})
        await send({"type": "http.response.body", "body": b'{"status_code":400}', "more_body": False})

    async def send_event(event: dict[str, Any]) -> None:
        sent_events.append(event)

    wrapped = bypass.wrap_asgi(fallback_app)
    await wrapped(fresh_scope, chunked_receive, send_event)

    assert fallback_bodies == [invalid_payload]
    assert sent_events[0]["status"] == 400


@pytest.mark.anyio
async def test_fastpath_http_exception_serialization_disqualifiers_query_and_upload_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify HTTPException JSON responses, disqualifiers, default str media_type, first query value, and UploadFile close."""
    monkeypatch.delenv("VERTEBRA_SERVER_ACTIVE", raising=False)
    monkeypatch.delenv("VERTEBRA_RUST_OFFLOAD", raising=False)

    def default_litestar_exc_handler(_req: Any, _exc: Any) -> Any:
        return None

    default_litestar_exc_handler.__module__ = "litestar.exceptions.responses"

    @get("/deny", exception_handlers={Exception: default_litestar_exc_handler}, sync_to_thread=False)
    def deny_route() -> dict[str, str]:
        raise NotAuthorizedException("token expired", status_code=401, headers={"x-auth-error": "expired"})

    @get("/plain-text", sync_to_thread=False)
    def plain_text_route(tag: FromQuery[str] = "none") -> str:
        return f"hello:{tag}"

    @get("/with-custom-headers", response_headers={"x-custom": "1"}, sync_to_thread=False)
    def custom_headers_route() -> dict[str, str]:
        return {"ok": "headers"}

    app = Litestar(
        route_handlers=[deny_route, plain_text_route, custom_headers_route],
        request_max_body_size=16,
    )
    bypass = FastPath.from_app(app)

    async def noop_receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def noop_send(message: dict[str, Any]) -> None:
        _ = message

    deny_scope = AsgiScope.from_parts(method="GET", path="/deny")
    deny_res = await bypass.dispatch(deny_scope, noop_receive, noop_send)
    assert deny_res is not None
    deny_status, deny_headers, deny_body = deny_res
    assert deny_status == 401
    assert (b"x-auth-error", b"expired") in [(k.lower(), v) for k, v in deny_headers]
    assert decode_json(deny_body)["detail"] == "token expired"

    text_scope = AsgiScope.from_parts(method="GET", path="/plain-text", query_string="tag=first&tag=second")
    text_res = await bypass.dispatch(text_scope, noop_receive, noop_send)
    assert text_res is not None
    text_status, text_headers, text_body = text_res
    assert text_status == 200
    assert (b"content-type", b"text/plain; charset=utf-8") in [(k.lower(), v) for k, v in text_headers]
    assert text_body == b"hello:first"

    hdr_route = next(c for c in bypass._compiled_routes.values() if c.path_template == "/with-custom-headers")
    assert hdr_route.handler_id not in bypass._direct_plans

    csrf_app = Litestar(route_handlers=[plain_text_route], csrf_config=CSRFConfig(secret="test-csrf-secret-1234"))
    csrf_bypass = FastPath.from_app(csrf_app)
    assert (
        await csrf_bypass.dispatch(AsgiScope.from_parts(method="GET", path="/plain-text"), noop_receive, noop_send)
        is None
    )

    py_comp_app = Litestar(route_handlers=[plain_text_route], compression_config=CompressionConfig(backend="gzip"))
    py_comp_bypass = FastPath.from_app(py_comp_app)
    assert (
        await py_comp_bypass.dispatch(AsgiScope.from_parts(method="GET", path="/plain-text"), noop_receive, noop_send)
        is None
    )

    closed_files: list[str] = []

    class _UploadFileDouble:
        def __init__(self, name: str) -> None:
            self.name = name

        async def close(self) -> None:
            closed_files.append(self.name)

    @post("/upload")
    async def upload_handler(request: Request[Any, Any, Any]) -> dict[str, str]:
        conn_state = ScopeState.from_scope(request.scope)
        if hasattr(conn_state, "form"):
            conn_state.form = {"file": _UploadFileDouble("doc.pdf")}
        else:
            conn_state._form = {"file": _UploadFileDouble("doc.pdf")}
        return {"uploaded": "yes"}

    upload_app = Litestar(route_handlers=[upload_handler])
    upload_compiled_map = compile_fastpath_routes(upload_app)
    upload_compiled = next(c for c in upload_compiled_map.values() if c.path_template == "/upload")
    upload_scope = _make_scope(upload_app, method="POST", path="/upload", route_handler=upload_compiled.route_handler)
    await dispatch_handler_direct(
        upload_compiled,
        scope=upload_scope,
        body=b"",
        receive=noop_receive,
        send=noop_send,
    )
    assert closed_files == ["doc.pdf"]


@pytest.mark.anyio
async def test_v2_ownership_layers_hooks_metadata_and_unoffloaded_middleware_disqualifiers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify v2 ownership_layers hooks/metadata and un-offloaded Python compression/CORS disqualify fastpath."""

    async def noop_receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def noop_send(message: dict[str, Any]) -> None:
        _ = message

    def controller_after_request(resp: Any) -> Any:
        return resp

    class _CustomResponse:
        pass

    @get("/v2-after-req", sync_to_thread=False)
    def v2_after_req_route() -> dict[str, str]:
        return {"ok": "v2"}

    after_req_router = Router(path="/", after_request=controller_after_request, route_handlers=[v2_after_req_route])

    @get("/v2-layered-decoders", sync_to_thread=False)
    def v2_decoders_route() -> dict[str, str]:
        return {"ok": "decoders"}

    v2_decoders_route.resolve_type_decoders = lambda: [(lambda x: isinstance(x, str), str)]

    @get("/v2-layered-resp-cls", sync_to_thread=False)
    def v2_resp_cls_route() -> dict[str, str]:
        return {"ok": "resp_cls"}

    resp_cls_router = Router(path="/", response_class=_CustomResponse, route_handlers=[v2_resp_cls_route])

    v2_app = Litestar(route_handlers=[after_req_router, v2_decoders_route, resp_cls_router])
    v2_bypass = FastPath.from_app(v2_app)
    after_req_compiled = next(c for c in v2_bypass._compiled_routes.values() if c.path_template == "/v2-after-req")
    assert after_req_compiled.has_middleware is True
    assert after_req_compiled.is_bypass_eligible is False
    assert (
        await v2_bypass.dispatch(AsgiScope.from_parts(method="GET", path="/v2-after-req"), noop_receive, noop_send)
        is None
    )

    decoders_compiled = next(
        c for c in v2_bypass._compiled_routes.values() if c.path_template == "/v2-layered-decoders"
    )
    assert decoders_compiled.handler_id not in v2_bypass._direct_plans

    resp_cls_compiled = next(
        c for c in v2_bypass._compiled_routes.values() if c.path_template == "/v2-layered-resp-cls"
    )
    assert resp_cls_compiled.handler_id not in v2_bypass._direct_plans

    monkeypatch.setenv("VERTEBRA_SERVER_ACTIVE", "1")

    @get("/hello-active", sync_to_thread=False)
    def hello_active() -> dict[str, str]:
        return {"ok": "active"}

    unstripped_comp_app = Litestar(
        route_handlers=[hello_active],
        compression_config=CompressionConfig(backend="gzip"),
    )
    unstripped_comp_bypass = FastPath.from_app(unstripped_comp_app)
    assert (
        await unstripped_comp_bypass.dispatch(
            AsgiScope.from_parts(method="GET", path="/hello-active"),
            noop_receive,
            noop_send,
        )
        is None
    )

    unstripped_comp_app.compression_config = None
    stripped_comp_bypass = FastPath.from_app(unstripped_comp_app)
    stripped_res = await stripped_comp_bypass.dispatch(
        AsgiScope.from_parts(method="GET", path="/hello-active"),
        noop_receive,
        noop_send,
    )
    assert stripped_res is not None
    assert stripped_res[0] == 200

    py_cors_app = Litestar(
        route_handlers=[hello_active],
        cors_config=CORSConfig(allow_origins=["https://example.com"]),
    )
    py_cors_app.state["vertebra_config"] = LitestarConfig(offload_cors=False)
    py_cors_bypass = FastPath.from_app(py_cors_app)
    assert (
        await py_cors_bypass.dispatch(
            AsgiScope.from_parts(method="GET", path="/hello-active"),
            noop_receive,
            noop_send,
        )
        is None
    )

    py_cors_app.state["vertebra_config"] = LitestarConfig(offload_cors=True)
    rust_cors_bypass = FastPath.from_app(py_cors_app)
    rust_cors_res = await rust_cors_bypass.dispatch(
        AsgiScope.from_parts(method="GET", path="/hello-active"),
        noop_receive,
        noop_send,
    )
    assert rust_cors_res is not None
    assert rust_cors_res[0] == 200

    @post("/mv-echo")
    async def mv_echo(data: JSONBody[CreateOrderPayload]) -> dict[str, Any]:
        return {"sku": data.sku, "quantity": data.quantity}

    mv_app = Litestar(route_handlers=[mv_echo])
    mv_bypass = FastPath.from_app(mv_app)
    mv_scope = AsgiScope.from_parts(
        method="POST",
        path="/mv-echo",
        headers=[(b"content-type", b"application/json")],
    )
    conn_state = ScopeState.from_scope(mv_scope)
    conn_state.body = memoryview(b'{"sku":"MV1","quantity":2}')
    mv_res = await mv_bypass.dispatch(mv_scope, noop_receive, noop_send)
    assert mv_res is not None
    assert decode_json(mv_res[2]) == {"sku": "MV1", "quantity": 2}


@pytest.mark.anyio
async def test_litestar_fastpath_wrap_asgi_uses_send_raw_response_and_mount_prefix() -> None:
    """Verify LitestarFastPathBypass.wrap_asgi invokes send.send_raw_response directly and strips root_path."""

    @get("/items/{item_id:int}", sync_to_thread=False)
    def get_item(item_id: FromPath[int]) -> dict[str, int]:
        return {"item_id": item_id}

    app = Litestar(route_handlers=[get_item])
    bypass = FastPath.from_app(app)

    async def fallback_app(_scope: Any, _receive: Any, _send: Any) -> None:
        raise AssertionError("fallback_app must not be called for matched fastpath route")

    wrapped = bypass.wrap_asgi(fallback_app)
    proto = AsgiProtocol(b"")
    scope = AsgiScope.from_parts(method="GET", path="/sub/items/99")
    scope["root_path"] = "/sub"

    raw_calls: list[tuple[int, list[tuple[bytes, bytes]], bytes]] = []

    class _RawSendTracker:
        async def __call__(self, message: dict[str, Any]) -> None:
            await proto.send(message)

        async def send_raw_response(
            self,
            status: int,
            headers: list[tuple[bytes, bytes]],
            body: bytes,
        ) -> None:
            raw_calls.append((status, headers, body))
            await proto.send_raw_response(status, headers, body)

    await wrapped(scope, proto.receive, _RawSendTracker())
    assert len(raw_calls) == 1
    assert raw_calls[0][0] == 200
    assert decode_json(raw_calls[0][2]) == {"item_id": 99}
    assert proto.complete is True
