"""Unit tests for the standalone Flask Vertebra extension."""

from __future__ import annotations

import asyncio
import collections
import fnmatch
import queue
import threading
from typing import TYPE_CHECKING, Any

import msgspec
import pydantic
import pytest

from vertebra._vertebra import AsgiProtocol, AsgiScope, Request, WsgiEnviron, run_wsgi_callable
from vertebra.agent.mcp.models import MCPConfig as CanonicalMCPConfig
from vertebra.agent.mcp.routes import WsgiMcpApp
from vertebra.db.driver import NotificationEvent, SyncNotificationSource
from vertebra.extensions.flask import (
    AsyncAPIDocsConfig,
    AsyncObjectStore,
    AsyncStorageRegistry,
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
    ViteConfig,
    attach_channels,
    attach_storage,
    convert_werkzeug_rule,
    mcp_tool,
    send_storage_file,
    vertebra_route,
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
from vertebra.utils.serializers import decode_json, encode_msgpack_bytes

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence


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

    discover_req = Request.from_parts(
        method="POST",
        path="/mcp",
        headers=[(b"content-type", b"application/json")],
        body=b'{"jsonrpc":"2.0","id":1,"method":"server/discover"}',
    )
    discover_resp = plugin.handle_request(discover_req)
    assert discover_resp.status == 200
    discover_payload = decode_json(bytes(discover_resp.body))
    assert discover_payload["result"]["protocolVersion"] == "2026-07-28"

    init_req = Request.from_parts(
        method="POST",
        path="/mcp",
        headers=[
            (b"content-type", b"application/json"),
            (b"mcp-protocol-version", b"2026-07-28"),
        ],
        body=b'{"jsonrpc":"2.0","id":2,"method":"initialize"}',
    )
    init_resp = plugin.handle_request(init_req)
    assert decode_json(bytes(init_resp.body))["error"]["code"] == -32601

    get_resp = plugin.handle_request(Request.from_parts(method="GET", path="/mcp"))
    assert get_resp.status == 405

    origin_resp = plugin.handle_request(
        Request.from_parts(
            method="POST",
            path="/mcp",
            headers=[
                (b"content-type", b"application/json"),
                (b"host", b"api.example.com"),
                (b"origin", b"https://evil.example.com"),
            ],
            body=b'{"jsonrpc":"2.0","id":3,"method":"tools/list"}',
        )
    )
    assert origin_resp.status == 403

    notify_resp = plugin.handle_request(
        Request.from_parts(
            method="POST",
            path="/mcp",
            headers=[(b"content-type", b"application/json")],
            body=b'{"jsonrpc":"2.0","method":"notifications/cancelled"}',
        )
    )
    assert notify_resp.status == 202
    assert bytes(notify_resp.body) == b""

    for seq in (1, 2):
        call_resp = plugin.handle_request(
            Request.from_parts(
                method="POST",
                path="/mcp",
                headers=[(b"content-type", b"application/json")],
                body=f'{{"jsonrpc":"2.0","id":{seq},"method":"tools/call","params":{{"name":"probe_loop","arguments":{{"seq":{seq}}}}}}}'.encode(),
            )
        )
        assert call_resp.status == 200

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
