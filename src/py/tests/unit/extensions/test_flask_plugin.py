"""Unit tests for the standalone Flask Vertebra extension."""

from __future__ import annotations

import asyncio
import base64
import collections
import fnmatch
import hashlib
import hmac
import queue
import subprocess
import sys
import threading
from typing import TYPE_CHECKING, Any, cast

import msgspec
import pydantic
import pytest
from advanced_alchemy.config import EngineConfig, RoutingConfig
from advanced_alchemy.extensions.flask import (
    SQLAlchemyAsyncConfig as FlaskSQLAlchemyAsyncConfig,
)
from advanced_alchemy.extensions.flask import (
    SQLAlchemySyncConfig as FlaskSQLAlchemySyncConfig,
)
from flask import Flask
from flask_caching import Cache as FlaskCache
from flask_caching.backends.base import BaseCache
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

from vertebra._vertebra import AsgiProtocol, AsgiScope, Request, WsgiEnviron, run_wsgi_callable
from vertebra.agent.mcp.models import MCPConfig as CanonicalMCPConfig
from vertebra.agent.mcp.routes import WsgiMcpApp
from vertebra.cache import SyncCacheStore
from vertebra.db.driver import NotificationEvent, SyncNotificationSource
from vertebra.extensions.alchemy import (
    create_async_alchemy_config,
    create_flask_alchemy,
    create_sync_alchemy_config,
)
from vertebra.extensions.flask import (
    AssetManifest,
    AsyncAPIDocsConfig,
    AsyncObjectStore,
    AsyncStorageRegistry,
    AuthenticatedPrincipal,
    CacheBackend,
    ChannelsBackend,
    FastPath,
    FlaskCachingBackend,
    JWTAuthGuard,
    MailBackend,
    MailConfig,
    MailMessage,
    MCPConfig,
    MemoryConfig,
    OpenAPIConfig,
    QueueBackend,
    RateLimitGuard,
    RouteCompiler,
    SwaggerDocsConfig,
    SyncObjectStore,
    SyncStorageRegistry,
    VertebraConfig,
    VertebraPlugin,
    ViteAssetBridge,
    ViteConfig,
    attach_channels,
    attach_storage,
    attach_vite,
    clear_vite_bridges,
    convert_werkzeug_rule,
    get_vertebra_client_path,
    get_vertebra_client_vite_aliases,
    mcp_tool,
    send_storage_file,
    vertebra_route,
    vite_asset,
    vite_hmr_client,
)
from vertebra.extensions.flask.fastpath import CompiledRoute as FlaskCompiledRoute
from vertebra.framework.queues import (
    CronLease,
    DeadLetterEntry,
    QueueService,
    TaskLane,
)
from vertebra.framework.queues import (
    QueueBackend as QueueBackendProtocol,
)
from vertebra.framework.queues import TaskEnvelope as CoreTaskEnvelope
from vertebra.server._channels import ChannelEnvelope
from vertebra.server._fastpath_core import CompiledRouteBase, get_attached_fastpath
from vertebra.utils.serializers import decode_json, encode_json_bytes, encode_msgpack_bytes

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence
    from pathlib import Path


class CreateOrderPayload(msgspec.Struct, frozen=True, gc=False):
    """Order creation payload."""

    sku: str
    qty: int


class PydanticOrderPayload(pydantic.BaseModel):
    """Pydantic order payload."""

    sku: str
    qty: int


class _MockRule:
    """Minimal Werkzeug Rule double."""

    def __init__(self, rule: str, endpoint: str, methods: tuple[str, ...]) -> None:
        self.rule = rule
        self.endpoint = endpoint
        self.methods = methods


class _MockUrlMap:
    """Minimal Werkzeug Map double."""

    def __init__(self, rules: list[_MockRule]) -> None:
        self._rules = rules

    def iter_rules(self) -> list[_MockRule]:
        return list(self._rules)


class _MockFlaskApp:
    """Minimal Flask application double supporting URL rules, context processors, and WSGI."""

    def __init__(self, static_folder: str | None = None) -> None:
        self._rules: list[_MockRule] = []
        self.url_map = _MockUrlMap(self._rules)
        self.view_functions: dict[str, Callable[..., Any]] = {}
        self.extensions: dict[str, Any] = {}
        self.config: dict[str, Any] = {
            "COMPRESS_REGISTER": True,
            "COMPRESS_MIN_SIZE": 256,
            "CORS_ORIGINS": ["https://app.example.com"],
            "CORS_SUPPORTS_CREDENTIALS": True,
        }
        self.static_folder = static_folder
        self.static_url_path = "/static"
        self.context_processors: list[Callable[[], dict[str, Any]]] = []

        def _base_wsgi(environ: Any, start_response: Callable[..., Any]) -> list[bytes]:
            start_response("404 Not Found", [("content-type", "text/plain")])
            return [b"fallback-404"]

        self.wsgi_app: Callable[..., Any] = _base_wsgi

    def add_url_rule(
        self,
        rule: str,
        endpoint: str | None = None,
        view_func: Callable[..., Any] | None = None,
        methods: list[str] | None = None,
    ) -> None:
        ep = endpoint or (view_func.__name__ if view_func is not None else "index")
        self._rules.append(_MockRule(rule, ep, tuple(methods or ["GET"])))
        if view_func is not None:
            self.view_functions[ep] = view_func

    def context_processor(self, fn: Callable[[], dict[str, Any]]) -> Callable[[], dict[str, Any]]:
        self.context_processors.append(fn)
        return fn


def test_convert_werkzeug_rule_and_flask_fastpath(tmp_path: Any) -> None:
    """Verify Werkzeug rule conversion, zero-copy msgspec/pydantic fastpath, OpenAPI, and MCP."""
    matchit_path, converters = convert_werkzeug_rule("/orders/<int:order_id>/<path:rest>")
    assert matchit_path == "/orders/{order_id}/{*rest}"
    assert converters == (("order_id", "int"), ("rest", "path"))

    static_dir = tmp_path / "static"
    static_dir.mkdir()
    app = _MockFlaskApp(static_folder=str(static_dir))

    @mcp_tool(name="create_order", description="Create a new order")
    def create_order(order_id: int, body: CreateOrderPayload) -> dict[str, Any]:
        return {"order_id": order_id, "sku": body.sku, "qty": body.qty}

    vertebra_route(create_order, status_code=201)

    @vertebra_route
    def create_pydantic_order(body: PydanticOrderPayload) -> PydanticOrderPayload:
        return PydanticOrderPayload(sku=body.sku.upper(), qty=body.qty + 1)

    app.add_url_rule("/orders/<int:order_id>", endpoint="create_order", view_func=create_order, methods=["POST"])
    app.add_url_rule(
        "/pydantic-orders",
        endpoint="create_pydantic_order",
        view_func=create_pydantic_order,
        methods=["POST"],
    )

    plugin = VertebraPlugin(
        app,
        config=VertebraConfig(
            openapi=OpenAPIConfig(
                title="Orders Flask API",
                version="1.2.0",
                swagger=SwaggerDocsConfig(path="/swagger"),
                asyncapi=AsyncAPIDocsConfig(path="/asyncapi"),
                enabled_docs=("scalar", "swagger", "asyncapi"),
            ),
            mcp=MCPConfig(server_name="flask-orders-mcp"),
            vite=ViteConfig(asset_url="/static/build/"),
        ),
    )

    req = Request.from_parts(
        method="POST",
        path="/orders/42",
        headers=[(b"content-type", b"application/json")],
        body=b'{"sku":"BOOK-1","qty":3}',
    )
    resp = plugin.handle_request(req)
    assert resp.status == 201
    assert decode_json(bytes(resp.body)) == {"order_id": 42, "sku": "BOOK-1", "qty": 3}

    wsgi_env = WsgiEnviron.from_parts(
        method="POST",
        path="/pydantic-orders",
        headers=[(b"content-type", b"application/json")],
        body=b'{"sku":"pen","qty":9}',
    )
    wsgi_resp = run_wsgi_callable(app.wsgi_app, wsgi_env)
    assert wsgi_resp.status == 200
    assert decode_json(bytes(wsgi_resp.body)) == {"sku": "PEN", "qty": 10}

    assert plugin.openapi_schema is not None
    assert "/orders/{order_id}" in plugin.openapi_schema["paths"]
    assert plugin.offload_bundle is not None
    assert plugin.offload_bundle.compression is not None
    assert plugin.offload_bundle.cors is not None
    assert len(plugin.offload_bundle.static_mounts) == 1
    assert len(app.context_processors) == 1
    ctx_vars = app.context_processors[0]()
    assert "vite_hmr_client" in ctx_vars
    assert "vite_asset" in ctx_vars

    assert plugin.mcp_registry is not None
    mcp_req = Request.from_parts(
        method="POST",
        path="/mcp",
        headers=[(b"content-type", b"application/json")],
        body=b'{"jsonrpc":"2.0","id":1,"method":"tools/list"}',
    )
    mcp_resp = plugin.handle_request(mcp_req)
    assert mcp_resp.status == 200
    mcp_payload = decode_json(bytes(mcp_resp.body))
    assert any(t["name"] == "create_order" for t in mcp_payload["result"]["tools"])


@pytest.mark.anyio
async def test_flask_ecosystem_bridges() -> None:
    """Verify Flask CacheBackend (sync + async), Storage, Mail, Queue, and Security bridges."""
    app = _MockFlaskApp()
    cache = FlaskCachingBackend.factory(app, {"CACHE_DEFAULT_TIMEOUT": 60, "CACHE_KEY_PREFIX": "test:"}, [], {})
    assert cache.set("k1", {"v": 1}) is True
    assert cache.get("k1") == {"v": 1}
    await cache.aset("k2", {"v": 2})
    assert await cache.aget("k2") == {"v": 2}
    assert cache.get_many("k1", "k2") == [{"v": 1}, {"v": 2}]
    assert await cache.aget_many("k1", "k2") == [{"v": 1}, {"v": 2}]

    calls = 0

    @cache.cached(timeout=30)
    def compute(x: int) -> int:
        nonlocal calls
        calls += 1
        return x * 2

    assert compute(5) == 10
    assert compute(5) == 10
    assert calls == 1

    store = SyncObjectStore.from_config(MemoryConfig())
    store.put("reports/q1.txt", b"quarter-1")
    reg = attach_storage(app, store)
    assert isinstance(reg, SyncStorageRegistry)
    file_resp = send_storage_file(reg.get("default"), "reports/q1.txt", as_attachment=True, download_name="q1.txt")
    assert file_resp.status == 200
    assert bytes(file_resp.body) == b"quarter-1"
    assert any(k == b"content-disposition" and b'filename="q1.txt"' in v for k, v in file_resp.headers)

    async_store = AsyncObjectStore.from_config(MemoryConfig())
    await async_store.put("reports/q2.txt", b"quarter-2")
    async_reg = attach_storage(app, async_store, attribute="async_storage")
    assert isinstance(async_reg, AsyncStorageRegistry)
    assert bytes(await async_reg.get("default").get("reports/q2.txt")) == b"quarter-2"

    mail = MailBackend(MailConfig(suppress_send=True))
    mail.init_app(app)
    mail.send(MailMessage(subject="Hello", recipients=("user@example.com",), body="Welcome"))
    await mail.asend(MailMessage(subject="Async Hello", recipients=("user@example.com",), body="Async Welcome"))
    assert len(mail.outbox) == 2

    queue = QueueBackend()

    @queue.task("multiply")
    def multiply(a: int, b: int) -> int:
        return a * b

    await queue.aenqueue("multiply", 6, 7)
    assert queue.run_next() == 42

    limiter = RateLimitGuard(capacity=2, refill_rate=1.0)
    assert limiter.allow("ip-1") is True
    assert limiter.allow("ip-1") is True

    jwt = JWTAuthGuard(b"super-secret-key-at-least-32-bytes-long")
    assert jwt.verify_header(None) is None
    assert jwt.verify_header("Basic abc") is None

    channels = attach_channels(app)
    channels.subscribe(["flask-alerts"])
    channels.publish("flask-alerts", {"alert": "critical"})
    listener = channels.listen(["flask-alerts"])
    msg = next(listener)
    assert msg.channel == "flask-alerts"
    assert b"critical" in msg.data
    channels.close()


@pytest.mark.anyio
async def test_flask_fastpath_async_views_wrap_asgi_and_wsgi_send_raw_response() -> None:
    """Verify Flask FastPath executes async def views, wrap_asgi, and start_response.send_raw_response."""
    app = _MockFlaskApp()

    async def create_async_order(order_id: int, body: CreateOrderPayload) -> dict[str, Any]:
        return {"order_id": order_id, "sku": body.sku, "qty": body.qty, "mode": "immediate"}

    vertebra_route(create_async_order, status_code=201)

    async def suspended_async_order(order_id: int, body: CreateOrderPayload) -> dict[str, Any]:
        await asyncio.sleep(0)
        return {"order_id": order_id, "sku": body.sku, "qty": body.qty, "mode": "suspended"}

    vertebra_route(suspended_async_order, status_code=200)

    app.add_url_rule(
        "/async-orders/<int:order_id>",
        endpoint="create_async_order",
        view_func=create_async_order,
        methods=["POST"],
    )
    app.add_url_rule(
        "/suspended-orders/<int:order_id>",
        endpoint="suspended_async_order",
        view_func=suspended_async_order,
        methods=["POST"],
    )

    compiler = RouteCompiler(app)
    fastpath = FastPath.from_app(app)
    wrapped_wsgi = fastpath.wrap_wsgi()
    assert getattr(wrapped_wsgi, "__vertebra_fastpath__", None) is fastpath
    assert not hasattr(wrapped_wsgi, "_vertebra_fastpath")
    assert len(compiler.entries) == len(fastpath.routes)

    req = Request.from_parts(
        method="POST",
        path="/async-orders/11",
        headers=[(b"content-type", b"application/json")],
        body=b'{"sku":"A-11","qty":2}',
    )
    sync_resp = fastpath.dispatch(req)
    assert sync_resp is not None
    assert sync_resp.status == 201
    assert decode_json(bytes(sync_resp.body)) == {
        "order_id": 11,
        "sku": "A-11",
        "qty": 2,
        "mode": "immediate",
    }

    raw_wsgi_calls: list[tuple[int, Any, bytes]] = []

    class _RawWsgiStartResponse:
        def __call__(self, _status_line: str, _headers: Any) -> None:
            raise AssertionError("Standard start_response must not be called when send_raw_response is present")

        def send_raw_response(self, status: int, headers: Any, body: bytes) -> list[bytes]:
            raw_wsgi_calls.append((status, headers, body))
            return [body]

    wsgi_env = WsgiEnviron.from_parts(
        method="POST",
        path="/async-orders/12",
        headers=[(b"content-type", b"application/json")],
        body=b'{"sku":"A-12","qty":5}',
    )
    wsgi_chunks = fastpath.dispatch_wsgi(wsgi_env, _RawWsgiStartResponse())
    assert wsgi_chunks is not None
    assert len(raw_wsgi_calls) == 1
    assert raw_wsgi_calls[0][0] == 201
    assert decode_json(raw_wsgi_calls[0][2]) == {
        "order_id": 12,
        "sku": "A-12",
        "qty": 5,
        "mode": "immediate",
    }

    asgi_app = fastpath.wrap_asgi()
    proto = AsgiProtocol(b'{"sku":"S-99","qty":7}')
    scope = AsgiScope.from_parts(
        method="POST",
        path="/flask/suspended-orders/99",
        headers=[(b"content-type", b"application/json")],
        body=b'{"sku":"S-99","qty":7}',
    )
    scope["root_path"] = "/flask"
    await asgi_app(scope, proto.receive, proto.send)
    assert proto.complete is True
    assert proto.status == 200
    assert decode_json(proto.take_response().body) == {
        "order_id": 99,
        "sku": "S-99",
        "qty": 7,
        "mode": "suspended",
    }


@pytest.mark.parametrize(
    ("method", "headers", "body", "expected_status", "check_kind", "expected_value"),
    [
        (
            "POST",
            [(b"content-type", b"application/json")],
            b'{"jsonrpc":"2.0","id":1,"method":"server/discover"}',
            200,
            "protocolVersion",
            "2026-07-28",
        ),
        (
            "POST",
            [
                (b"content-type", b"application/json"),
                (b"mcp-protocol-version", b"2026-07-28"),
            ],
            b'{"jsonrpc":"2.0","id":2,"method":"initialize"}',
            None,
            "error_code",
            -32601,
        ),
        ("GET", [], b"", 405, None, None),
        (
            "POST",
            [
                (b"content-type", b"application/json"),
                (b"host", b"api.example.com"),
                (b"origin", b"https://evil.example.com"),
            ],
            b'{"jsonrpc":"2.0","id":3,"method":"tools/list"}',
            403,
            None,
            None,
        ),
        (
            "POST",
            [(b"content-type", b"application/json")],
            b'{"jsonrpc":"2.0","method":"notifications/cancelled"}',
            202,
            "empty_body",
            b"",
        ),
    ],
    ids=["discover", "initialize-error", "get-405", "forbidden-origin", "notification-202"],
)
def test_flask_mcp_protocol_request_cases(
    method: str,
    headers: list[tuple[bytes, bytes]],
    body: bytes,
    expected_status: int | None,
    check_kind: str | None,
    expected_value: Any,
) -> None:
    """Flask MCP endpoint handles discovery, method validation, origin checks, and notifications."""
    plugin = VertebraPlugin(_MockFlaskApp(), config=VertebraConfig(mcp=MCPConfig(server_name="flask-mcp")))
    resp = plugin.handle_request(Request.from_parts(method=method, path="/mcp", headers=headers, body=body))
    if expected_status is not None:
        assert resp.status == expected_status
    if check_kind == "protocolVersion":
        assert decode_json(bytes(resp.body))["result"]["protocolVersion"] == expected_value
    elif check_kind == "error_code":
        assert decode_json(bytes(resp.body))["error"]["code"] == expected_value
    elif check_kind == "empty_body":
        assert bytes(resp.body) == expected_value


def test_flask_mcp_shares_wsgi_mcp_app_and_reuses_persistent_portal() -> None:
    """Flask MCP uses canonical MCPConfig, WsgiMcpApp over MCPCore, and reuses the persistent Portal loop."""
    assert MCPConfig is CanonicalMCPConfig

    observed_loops: list[tuple[int, int]] = []
    app = _MockFlaskApp()

    @mcp_tool(name="probe_loop", description="Probe active event loop and thread")
    async def probe_loop(seq: int) -> dict[str, int]:
        loop = asyncio.get_running_loop()
        observed_loops.append((id(loop), threading.get_ident()))
        return {"seq": seq}

    vertebra_route(probe_loop, status_code=200)

    app.add_url_rule("/probe/<int:seq>", endpoint="probe_loop", view_func=probe_loop, methods=["POST"])
    plugin = VertebraPlugin(app, config=VertebraConfig(mcp=MCPConfig(server_name="flask-mcp")))
    assert isinstance(getattr(plugin, "_mcp_app", None), WsgiMcpApp)

    def _call_tool(seq: int) -> None:
        call_resp = plugin.handle_request(
            Request.from_parts(
                method="POST",
                path="/mcp",
                headers=[(b"content-type", b"application/json")],
                body=f'{{"jsonrpc":"2.0","id":{seq},"method":"tools/call","params":{{"name":"probe_loop","arguments":{{"seq":{seq}}}}}}}'.encode(),
            )
        )
        assert call_resp.status == 200

    _call_tool(1)
    _call_tool(2)

    assert len(observed_loops) == 2
    assert observed_loops[0] == observed_loops[1]


def test_flask_compiled_route_inherits_base_and_attaches_fastpath() -> None:
    """Flask CompiledRoute inherits CompiledRouteBase and VertebraPlugin attaches __vertebra_fastpath__."""
    assert issubclass(FlaskCompiledRoute, CompiledRouteBase)

    class _Rule:
        def __init__(self, rule: str, endpoint: str, methods: set[str]) -> None:
            self.rule = rule
            self.endpoint = endpoint
            self.methods = methods

    class _UrlMap:
        def __init__(self, rules: list[_Rule]) -> None:
            self._rules = rules

        def iter_rules(self) -> list[_Rule]:
            return self._rules

    class _MiniFlaskApp:
        def __init__(self) -> None:
            self.name = "flask_fastpath_attach"
            self.extensions: dict[str, Any] = {}
            self.view_functions: dict[str, Any] = {"ping": lambda: {"ok": True}}
            self.url_map = _UrlMap([_Rule("/ping", "ping", {"GET", "HEAD"})])

        def wsgi_app(self, _environ: Any, start_response: Any) -> list[bytes]:
            start_response("200 OK", [("content-type", "text/plain")])
            return [b"ok"]

    app = _MiniFlaskApp()
    plugin = VertebraPlugin(app, config=VertebraConfig(offload_middlewares=False))
    assert plugin.fastpath is not None
    assert get_attached_fastpath(app) is plugin.fastpath


@pytest.mark.anyio
async def test_flask_channels_sync_notification_source_local_and_remote_fanin() -> None:
    """Flask ChannelsBackend pumps SyncNotificationSource into ChannelBus without dropping local messages."""

    class _EchoSyncNotificationSource(SyncNotificationSource):
        def __init__(self) -> None:
            self.subscribed: set[str] = set()
            self.events: queue.Queue[NotificationEvent | None] = queue.Queue()
            self.closed = False

        def publish(self, channel: str, payload: bytes | str) -> None:
            raw = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
            self.events.put(NotificationEvent(channel=channel, payload=raw, sender_id=None, metadata={}))

        def subscribe(self, channels: Sequence[str]) -> None:
            self.subscribed.update(channels)

        def unsubscribe(self, channels: Sequence[str] = ()) -> None:
            for ch in channels:
                self.subscribed.discard(ch)

        def listen(self) -> Iterator[NotificationEvent]:
            while not self.closed:
                item = self.events.get()
                if item is None:
                    break
                yield item

        def close(self) -> None:
            self.closed = True
            self.events.put(None)

    source = _EchoSyncNotificationSource()
    channels = ChannelsBackend(
        history=8,
        sync_notification_source=source,
        node_id="node-a",
    )
    try:
        stream = channels.listen(["orders"])
        channels.publish("orders", b"local-order-1")
        remote_envelope = b"vtb:env:" + encode_msgpack_bytes(
            ChannelEnvelope(origin_node_id="node-b", channel="orders", payload=b"remote-order-2")
        )
        source.events.put(NotificationEvent(channel="orders", payload=remote_envelope, sender_id="node-b", metadata={}))

        first = next(stream)
        second = next(stream)
        assert (first.channel, first.data) == ("orders", b"local-order-1")
        assert (second.channel, second.data) == ("orders", b"remote-order-2")
        history = await channels.bus.get_history("orders")
        assert history == [b"local-order-1", b"remote-order-2"]
    finally:
        channels.close()
        assert source.closed is True


@pytest.mark.anyio
async def test_flask_cache_backend_remote_client_sync_and_clear_prefix() -> None:
    """Flask CacheBackend routes sync get/set/delete/clear and aclear through remote_client."""

    class _RemoteCacheClient:
        def __init__(self) -> None:
            self.data: dict[str, bytes] = {"other_ns:keep": b'"untouched"'}
            self.ttls: dict[str, int | None] = {}

        def get(self, key: str) -> bytes | None:
            return self.data.get(key)

        def set(self, key: str, value: bytes, *, ex: int | None = None) -> None:
            self.data[key] = bytes(value)
            self.ttls[key] = ex

        def delete(self, key: str) -> int:
            return 1 if self.data.pop(key, None) is not None else 0

        def scan(self, cursor: int = 0, *, match: str | None = None) -> tuple[int, list[str]]:
            _ = cursor
            keys = [k for k in self.data if match is None or fnmatch.fnmatch(k, match)]
            return 0, keys

        def unlink(self, *keys: str) -> int:
            removed = 0
            for key in keys:
                if self.data.pop(key, None) is not None:
                    removed += 1
            return removed

    remote = _RemoteCacheClient()
    writer = CacheBackend(default_timeout=45, key_prefix="flask_test:", remote_client=remote)
    reader = CacheBackend(default_timeout=45, key_prefix="flask_test:", remote_client=remote)

    assert writer.set("alpha", {"v": 10}) is True
    assert remote.ttls["flask_test:alpha"] == 45
    assert reader.get("alpha") == {"v": 10}
    assert reader.delete("alpha") is True
    assert reader.get("alpha") is None

    writer.set("beta", [1, 2, 3])
    writer.set("gamma", "ok")
    assert writer.clear() is True
    assert reader.get("beta") is None
    assert reader.get("gamma") is None
    assert remote.data == {"other_ns:keep": b'"untouched"'}

    await writer.aset("delta", {"async": True})
    assert await reader.aget("delta") == {"async": True}
    assert await writer.aclear() is True
    assert await reader.aget("delta") is None
    assert remote.data == {"other_ns:keep": b'"untouched"'}


@pytest.mark.anyio
async def test_flask_queue_backend_delegates_to_durable_backend_without_lanes_attr() -> None:
    """Flask QueueBackend dequeue, adequeue, and run_next delegate to durable QueueService backends."""

    class _DurableQueueBackend(QueueBackendProtocol):
        def __init__(self) -> None:
            self.ready: collections.deque[CoreTaskEnvelope] = collections.deque()
            self.acked: list[str] = []
            self.promoted: int = 0

        def enqueue_sync(self, envelope: CoreTaskEnvelope) -> CoreTaskEnvelope:
            self.ready.append(envelope)
            return envelope

        async def enqueue(self, envelope: CoreTaskEnvelope) -> CoreTaskEnvelope:
            self.ready.append(envelope)
            return envelope

        async def dequeue(
            self,
            lanes: Sequence[TaskLane],
            *,
            batch_size: int = 1,
            timeout_seconds: float = 0.0,
        ) -> list[CoreTaskEnvelope]:
            _ = timeout_seconds
            claimed: list[CoreTaskEnvelope] = []
            remaining: collections.deque[CoreTaskEnvelope] = collections.deque()
            while self.ready:
                item = self.ready.popleft()
                if item.lane in lanes and len(claimed) < batch_size:
                    claimed.append(item)
                else:
                    remaining.append(item)
            self.ready = remaining
            return claimed

        async def ack(self, envelope: CoreTaskEnvelope) -> None:
            self.acked.append(envelope.task_id)

        async def nack(
            self,
            envelope: CoreTaskEnvelope,
            *,
            error: Exception | str,
            delay_seconds: float | None = None,
            send_to_dlq: bool = False,
        ) -> DeadLetterEntry | None:
            _ = (envelope, error, delay_seconds, send_to_dlq)
            return None

        async def promote_scheduled(self, *, now: float | None = None, batch_size: int = 100) -> int:
            _ = (now, batch_size)
            self.promoted += 1
            return 0

        async def list_dlq(self, *, limit: int = 100) -> list[DeadLetterEntry]:
            _ = limit
            return []

        async def replay_dlq(self, dlq_id: str) -> CoreTaskEnvelope | None:
            _ = dlq_id
            return None

        async def acquire_lease(
            self,
            lease_key: str,
            owner_id: str,
            ttl_seconds: float,
            *,
            slot: str | None = None,
        ) -> CronLease | None:
            _ = (lease_key, owner_id, ttl_seconds, slot)
            return None

        async def renew_lease(
            self,
            lease_key: str,
            owner_id: str,
            ttl_seconds: float,
            *,
            slot: str | None = None,
        ) -> CronLease | None:
            _ = (lease_key, owner_id, ttl_seconds, slot)
            return None

        async def close(self) -> None:
            return None

    durable = _DurableQueueBackend()
    assert not hasattr(durable, "_lanes")
    svc = QueueService(backend=durable)
    flask_queue = QueueBackend(queue_service=svc)

    @flask_queue.task("sum_pair")
    def sum_pair(x: int, y: int) -> int:
        return x + y

    env1 = flask_queue.enqueue("sum_pair", 3, 4)
    popped_sync = flask_queue.dequeue()
    assert popped_sync is not None
    assert popped_sync.id == env1.id
    assert popped_sync.args == (3, 4)

    env2 = await flask_queue.aenqueue("sum_pair", 10, 20)
    popped_async = await flask_queue.adequeue()
    assert popped_async is not None
    assert popped_async.id == env2.id

    env3 = flask_queue.enqueue("sum_pair", 8, 9)
    assert flask_queue.run_next() == 17
    assert durable.acked == [env1.id, env2.id, env3.id]
    assert durable.promoted >= 3


def test_flask_alchemy_factory_and_app_binding() -> None:
    """Verify Flask Alchemy config factories, routing/engine forwarding, and app extension binding."""
    with pytest.raises(ValueError, match="at least one configuration"):
        create_flask_alchemy()

    async_cfg = create_async_alchemy_config("sqlite+vtb_async:///:memory:", framework="flask")
    sync_cfg = create_sync_alchemy_config("sqlite+vtb:///:memory:", framework="flask")
    assert isinstance(async_cfg, FlaskSQLAlchemyAsyncConfig)
    assert isinstance(sync_cfg, FlaskSQLAlchemySyncConfig)

    app = Flask(__name__)
    ext = create_flask_alchemy(sync_cfg, app=app)
    assert ext.config == [sync_cfg]
    assert "advanced_alchemy" in app.extensions

    routing_cfg = RoutingConfig(primary_connection_string="sqlite+vtb:///:memory:")
    routed_cfg = create_sync_alchemy_config(framework="flask", routing_config=routing_cfg)
    assert isinstance(routed_cfg, FlaskSQLAlchemySyncConfig)
    assert routed_cfg.routing_config is routing_cfg

    sync_engine = create_engine("sqlite+vtb:///:memory:")
    explicit_ec = EngineConfig(pool_pre_ping=False, pool_recycle=cast("Any", None), poolclass=NullPool)
    try:
        prebuilt_cfg = create_sync_alchemy_config(
            framework="flask",
            engine_instance=sync_engine,
            engine_config=explicit_ec,
        )
        assert prebuilt_cfg.engine_instance is sync_engine
        assert prebuilt_cfg.engine_config is explicit_ec
    finally:
        sync_engine.dispose()


def _build_hs256_jwt(claims: dict[str, Any], secret: bytes) -> str:
    """Create a deterministic HS256 JWT token for unit assertions."""
    header_b64 = (
        base64.urlsafe_b64encode(encode_json_bytes({"alg": "HS256", "typ": "JWT"})).rstrip(b"=").decode("ascii")
    )
    payload_b64 = base64.urlsafe_b64encode(encode_json_bytes(claims)).rstrip(b"=").decode("ascii")
    signing_input = f"{header_b64}.{payload_b64}".encode("ascii")
    signature = hmac.new(secret, signing_input, hashlib.sha256).digest()
    sig_b64 = base64.urlsafe_b64encode(signature).rstrip(b"=").decode("ascii")
    return f"{header_b64}.{payload_b64}.{sig_b64}"


def test_flask_caching_backend_live_cache_sentinel_none_and_prefix_clear() -> None:
    """FlaskCachingBackend integrates with flask_caching.Cache, preserves None hits, and clears by prefix."""
    app = Flask(__name__)
    ext_cache = FlaskCache(
        app,
        config={
            "CACHE_TYPE": "vertebra.extensions.flask.cache.FlaskCachingBackend",
            "CACHE_DEFAULT_TIMEOUT": 60,
            "CACHE_KEY_PREFIX": "fc:app1:",
        },
    )
    backend = ext_cache.cache
    assert isinstance(backend, BaseCache)

    hits = 0

    @app.route("/items")
    @ext_cache.cached(timeout=30)
    def list_items() -> dict[str, Any]:
        nonlocal hits
        hits += 1
        return {"hits": hits}

    client = app.test_client()
    first_resp = client.get("/items")
    second_resp = client.get("/items")
    assert first_resp.status_code == 200
    assert second_resp.status_code == 200
    assert decode_json(first_resp.data) == {"hits": 1}
    assert decode_json(second_resp.data) == {"hits": 1}
    assert hits == 1

    assert backend.set("nullable", None) is True
    assert backend.has("nullable") is True
    assert backend.get("nullable") is None
    assert backend.has("missing") is False
    assert backend.get("missing") is None
    assert backend.get_many("nullable", "missing") == [None, None]
    assert backend.get_dict("nullable", "missing") == {"nullable": None, "missing": None}
    assert backend.add("nullable", "overwrite-blocked") is False
    assert backend.get("nullable") is None
    assert backend.add("fresh", "created") is True
    assert backend.get("fresh") == "created"

    assert backend.inc("counter", 2) == 2
    assert backend.inc("counter", 3) == 5
    assert backend.dec("counter", 1) == 4
    assert backend.set_many({"m1": "v1", "m2": "v2"}) == ["m1", "m2"]
    assert backend.delete_many("m1", "m2") == ["m1", "m2"]
    assert backend.has("m1") is False

    clock_now = [100.0]

    def _clock() -> float:
        return clock_now[0]

    shared_store = SyncCacheStore("memory://", key_prefix="shared:", _clock=_clock)

    class _NoFlushStore:
        def __init__(self, inner: SyncCacheStore) -> None:
            self._inner = inner

        def __getattr__(self, name: str) -> Any:
            if name == "flushdb":
                raise AssertionError("flushdb() must never be called by FlaskCachingBackend.clear()")
            return getattr(self._inner, name)

    guarded_store = cast("SyncCacheStore", _NoFlushStore(shared_store))
    ns_one = FlaskCachingBackend(default_timeout=10, key_prefix="ns1:", store=guarded_store)
    ns_two = FlaskCachingBackend(default_timeout=10, key_prefix="ns2:", store=guarded_store)

    assert ns_one.set("forever", "kept", timeout=0) is True
    assert ns_one.set("ephemeral", "gone", timeout=5) is True
    assert ns_two.set("untouched", "safe", timeout=0) is True

    clock_now[0] = 106.0
    assert ns_one.has("forever") is True
    assert ns_one.get("forever") == "kept"
    assert ns_one.has("ephemeral") is False

    assert ns_one.clear() is True
    assert ns_one.has("forever") is False
    assert ns_two.has("untouched") is True
    assert ns_two.get("untouched") == "safe"


@pytest.mark.parametrize(
    "blocked_modules",
    [
        ("litestar", "litestar_queues", "vertebra.extensions.litestar"),
        ("flask", "werkzeug"),
        ("flask_caching", "flask_caching.backends", "flask_caching.backends.base"),
    ],
)
def test_flask_subprocess_import_isolation_and_missing_dependencies(blocked_modules: tuple[str, ...]) -> None:
    """Subprocess import isolation verifies zero litestar cross-imports and MissingDependencyError when optional deps are absent."""
    script = (
        "import importlib, sys\n"
        "from types import SimpleNamespace\n"
        "from vertebra.typing import MissingDependencyError\n"
        f"blocked = {list(blocked_modules)!r}\n"
        "for mod in blocked:\n"
        "    sys.modules[mod] = None\n"
        "pkg = importlib.import_module('vertebra.extensions.flask')\n"
        "for sub in (\n"
        "    'cache', 'channels', 'config', 'fastpath', 'mail', 'mcp',\n"
        "    'offload', 'openapi', 'plugin', 'queues', 'router', 'security', 'storage', 'vite'\n"
        "):\n"
        "    importlib.import_module(f'vertebra.extensions.flask.{sub}')\n"
        "cross = [m for m in sys.modules if m.startswith('vertebra.extensions.litestar') and sys.modules[m] is not None]\n"
        "assert not cross, f'Unexpected litestar imports: {cross}'\n"
        "assert hasattr(pkg, 'VertebraPlugin')\n"
        "if 'flask_caching' in blocked:\n"
        "    cache_mod = sys.modules['vertebra.extensions.flask.cache']\n"
        "    try:\n"
        "        cache_mod.FlaskCachingBackend()\n"
        "    except MissingDependencyError as exc:\n"
        "        assert 'flask-caching' in str(exc), str(exc)\n"
        "    else:\n"
        "        raise AssertionError('Expected MissingDependencyError when flask_caching is blocked')\n"
        "if 'flask' in blocked:\n"
        "    fake_app = SimpleNamespace(extensions={}, view_functions={})\n"
        "    for hook in (\n"
        "        lambda: pkg.VertebraPlugin(fake_app),\n"
        "        lambda: pkg.attach_mcp_routes(fake_app),\n"
        "        lambda: pkg.attach_openapi_docs(fake_app),\n"
        "        lambda: pkg.attach_channels(fake_app),\n"
        "        lambda: pkg.attach_vite(fake_app),\n"
        "        lambda: pkg.MailBackend().init_app(fake_app),\n"
        "        lambda: pkg.QueueBackend().init_app(fake_app),\n"
        "    ):\n"
        "        try:\n"
        "            hook()\n"
        "        except MissingDependencyError as exc:\n"
        "            assert 'flask' in str(exc), str(exc)\n"
        "        else:\n"
        "            raise AssertionError('Expected MissingDependencyError when flask is blocked')\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, f"stdout={proc.stdout}\nstderr={proc.stderr}"


def test_flask_feature_adapters_neutral_roundtrips(tmp_path: Path) -> None:
    """Verify Flask Vite, security, storage, and channels adapters delegate cleanly to neutral modules."""
    clear_vite_bridges()
    app = Flask(__name__)

    manifest_file = tmp_path / "manifest.json"
    manifest_file.write_text(
        '{"src/main.ts":{"file":"assets/main-abc123.js","isEntry":true,"css":["assets/main-def456.css"]}}',
        encoding="utf-8",
    )
    manifest = AssetManifest.from_file(manifest_file)
    standalone_bridge = ViteAssetBridge(ViteConfig(asset_url="/static/"), manifest=manifest)
    tags = standalone_bridge.asset_tags("src/main.ts")
    assert "/static/assets/main-abc123.js" in tags
    assert "/static/assets/main-def456.css" in tags
    assert standalone_bridge.hmr_client_tag() == ""

    attached_vite = attach_vite(app, ViteConfig(dev_mode=True, dev_server_url="http://localhost:5173"))
    assert attached_vite is cast("Any", app).vite
    assert "@vite/client" in vite_hmr_client()
    assert "http://localhost:5173/src/main.ts" in vite_asset("src/main.ts")
    assert get_vertebra_client_path().name == "vertebra-client"
    assert "@vertebra/client" in get_vertebra_client_vite_aliases()

    secret = b"test-token-placeholder-secret-32"
    token = _build_hs256_jwt({"sub": "user-flask-1", "scope": "read write", "exp": 4_102_444_800}, secret)
    guard = JWTAuthGuard(secret)
    principal = guard.verify_header(f"Bearer {token}")
    assert isinstance(principal, AuthenticatedPrincipal)
    assert principal.sub == "user-flask-1"
    assert principal.scopes == ("read", "write")
    with pytest.raises(ValueError):
        guard.verify_header("Bearer test-token-placeholder")

    limiter = RateLimitGuard(capacity=1, refill_rate=0.0)
    assert limiter.allow("client-flask-1") is True
    assert limiter.allow("client-flask-1") is False

    sync_reg = SyncStorageRegistry(default_store=MemoryConfig())
    sync_reg.get().put("docs/readme.txt", b"flask-storage-body")
    attach_storage(app, sync_reg)
    resp = send_storage_file(sync_reg.get(), "docs/readme.txt", as_attachment=True, download_name="readme.txt")
    assert resp.status == 200
    assert bytes(resp.body) == b"flask-storage-body"

    channels = attach_channels(app)
    channels.subscribe(["flask-neutral"])
    channels.publish("flask-neutral", {"event": "ready"})
    item = next(channels.listen(["flask-neutral"]))
    assert item.channel == "flask-neutral"
    assert b"ready" in item.data
    channels.close()
