"""Unit tests for vertebra.extensions.starlette and 24-slot AsgiScope C-API fastpath."""

from __future__ import annotations

import contextlib
import subprocess
import sys
import threading
import time
from collections.abc import AsyncIterator, Awaitable
from datetime import timedelta
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import jwt
import msgspec
import pytest
from advanced_alchemy.config import EngineConfig, RoutingConfig
from advanced_alchemy.extensions.starlette import (
    SQLAlchemyAsyncConfig as StarletteSQLAlchemyAsyncConfig,
)
from advanced_alchemy.extensions.starlette import (
    SQLAlchemySyncConfig as StarletteSQLAlchemySyncConfig,
)
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool
from starlette.applications import Starlette
from starlette.background import BackgroundTask
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse, Response
from starlette.routing import Route
from starlette.testclient import TestClient

from vertebra._vertebra import AsgiProtocol, AsgiScope
from vertebra.agent.mcp.controllers import mcp_tool
from vertebra.agent.mcp.routes import AsgiMcpApp
from vertebra.db.adapters.redis import AsyncRedisDriver, RedisConfig
from vertebra.extensions.alchemy import (
    create_async_alchemy_config,
    create_starlette_alchemy,
    create_sync_alchemy_config,
)
from vertebra.extensions.starlette import (
    AsyncStorageRegistry,
    CacheStore,
    ChannelsBackend,
    FastPath,
    JWTAuthBackend,
    OpenAPIConfig,
    RateLimitGuard,
    RouteCompiler,
    SwaggerDocsConfig,
    SyncStorageRegistry,
    VertebraConfig,
    VertebraPlugin,
    WebMCPConfig,
    attach_cache_store,
    attach_channels,
    attach_storage,
    mount_docs_routes,
    mount_mcp_routes,
    offload_middlewares,
    preseed_request_body,
)
from vertebra.extensions.starlette.fastpath import (
    BUFFERED_RESPONSE_TYPES,
    serialize_buffered_response,
)
from vertebra.extensions.starlette.fastpath import (
    CompiledRoute as StarletteCompiledRoute,
)
from vertebra.extensions.starlette.queues import (
    QueueService as StarletteQueueService,
)
from vertebra.extensions.starlette.queues import (
    attach_queue_service as attach_starlette_queue_service,
)
from vertebra.extensions.starlette.queues import (
    get_queue_service as get_starlette_queue_service,
)
from vertebra.extensions.starlette.queues import (
    queue_lifespan as starlette_queue_lifespan,
)
from vertebra.framework.queues import MemoryQueueBackend, task
from vertebra.server._channels import ChannelBus
from vertebra.server._fastpath import attach_framework_fastpath
from vertebra.server._fastpath_core import CompiledRouteBase, get_attached_fastpath
from vertebra.utils.serializers import decode_json

if TYPE_CHECKING:
    from tests.unit.conftest import Resp3FakeServer


class _ItemPayload(msgspec.Struct, frozen=True, gc=False):
    name: str
    qty: int


def _make_mock_starlette_app() -> Any:
    async def get_item(request: Any) -> dict[str, Any]:
        return {
            "item_id": request.path_params["item_id"],
            "method": request.method,
        }

    @mcp_tool(name="create_item", description="Create a new item")
    async def create_item(request: Any) -> dict[str, Any]:
        raw = await request.body()
        vtb_req = request.scope.get("vertebra_request")
        decoded = vtb_req.decode_msgspec(_ItemPayload) if vtb_req is not None else decode_json(raw, type=_ItemPayload)
        return {"created": decoded.name, "qty": decoded.qty, "body_len": len(raw)}

    def sync_ping(request: Any) -> dict[str, str]:
        return {"pong": request.query_params.get("echo", "ok")}

    routes = [
        SimpleNamespace(
            path="/items/{item_id:int}",
            endpoint=get_item,
            methods=["GET"],
            name="get_item",
        ),
        SimpleNamespace(
            path="/items",
            endpoint=create_item,
            methods=["POST"],
            name="create_item",
        ),
        SimpleNamespace(
            path="/ping",
            endpoint=sync_ping,
            methods=["GET"],
            name="sync_ping",
        ),
    ]
    user_middleware = [
        SimpleNamespace(
            cls=type("GZipMiddleware", (), {}),
            args=(),
            kwargs={"minimum_size": 1024, "compresslevel": 6},
        ),
        SimpleNamespace(
            cls=type("CORSMiddleware", (), {}),
            args=(),
            kwargs={
                "allow_origins": ["https://example.com"],
                "allow_methods": ["GET", "POST"],
                "allow_credentials": True,
            },
        ),
    ]
    return SimpleNamespace(
        routes=routes,
        user_middleware=user_middleware,
        exception_handlers={},
        state=SimpleNamespace(),
        router=SimpleNamespace(routes=routes),
    )


def test_asgi_scope_24_slots_and_preseed_request_body() -> None:
    """Verify 24-slot AsgiScope inline keys, zero-copy request(), and C-API preseed_request_body."""
    scope = AsgiScope.from_parts(
        method="POST",
        path="/items",
        body=b'{"name":"widget","qty":5}',
    )
    assert len(scope.known_keys()) == 24
    assert scope.body_bytes() == b'{"name":"widget","qty":5}'

    vtb_req = scope.request()
    decoded = vtb_req.decode_msgspec(_ItemPayload)
    assert decoded == _ItemPayload(name="widget", qty=5)

    dummy_req = SimpleNamespace()
    preseed_request_body(scope, dummy_req)
    assert dummy_req._body == b'{"name":"widget","qty":5}'


@pytest.mark.anyio
async def test_starlette_plugin_route_compiler_and_fastpath(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify VertebraPlugin compiles routes, offloads middleware, and dispatches fastpath."""
    monkeypatch.setenv("VERTEBRA_SERVER_ACTIVE", "1")
    app = _make_mock_starlette_app()
    plugin = VertebraPlugin(
        app,
        config=VertebraConfig(
            enable_health_endpoint=True,
            openapi_config=OpenAPIConfig(swagger=SwaggerDocsConfig(path="/swagger")),
            webmcp_config=WebMCPConfig(enabled=True),
        ),
    )

    assert plugin.offload_plan is not None
    assert plugin.offload_plan.compression is not None
    assert plugin.offload_plan.compression.minimum_size == 1024
    assert plugin.offload_plan.cors is not None
    assert plugin.offload_plan.cors.allow_origins == ("https://example.com",)

    compiler = RouteCompiler()
    table = compiler.compile(app)
    assert table.match("GET", "/items/42") is not None
    assert table.match("GET", "/healthz") is not None
    assert table.match("GET", "/swagger") is not None
    assert table.match("GET", "/.well-known/webmcp.json") is not None

    fastpath: FastPath = app.__vertebra_fastpath__
    assert not hasattr(app, "_vertebra_fastpath")
    unattached_app = _make_mock_starlette_app()
    wrapped_starlette = attach_framework_fastpath(unattached_app, mount_prefix="/st")
    assert isinstance(unattached_app.__vertebra_fastpath__, FastPath)
    assert not hasattr(unattached_app, "_vertebra_fastpath")
    assert not hasattr(wrapped_starlette, "_vertebra_fastpath")
    scope_get = AsgiScope.from_parts(method="GET", path="/items/42")

    async def noop_receive() -> dict[str, Any]:
        raise AssertionError("receive() should not be called for GET or pre-seeded POST")

    async def noop_send(_msg: dict[str, Any]) -> None:
        return None

    res_get = await fastpath.dispatch(scope_get, noop_receive, noop_send)
    assert res_get is not None
    status, _headers, body = res_get
    assert status == 200
    assert decode_json(bytes(body)) == {"item_id": 42, "method": "GET"}
    assert scope_get["path_params"] == {"item_id": 42}
    assert scope_get["app"] is app

    scope_post = AsgiScope.from_parts(
        method="POST",
        path="/items",
        body=b'{"name":"bolt","qty":9}',
    )
    res_post = await fastpath.dispatch(scope_post, noop_receive, noop_send)
    assert res_post is not None
    status_post, _headers_post, body_post = res_post
    assert status_post == 200
    assert decode_json(bytes(body_post)) == {"created": "bolt", "qty": 9, "body_len": 23}

    scope_sync = AsgiScope.from_parts(method="GET", path="/ping", query_string="echo=fast")
    res_sync = await fastpath.dispatch(scope_sync, noop_receive, noop_send)
    assert res_sync is not None
    assert decode_json(bytes(res_sync[2])) == {"pong": "fast"}


@pytest.mark.anyio
async def test_starlette_ecosystem_bridges() -> None:
    """Verify Starlette CacheStore (sync + async), StorageRegistry, ChannelsBackend, and Security."""
    app = _make_mock_starlette_app()
    cache = attach_cache_store(app)
    assert isinstance(cache, CacheStore)
    cache.set_sync("k1", "v1", expires_in=30)
    assert cache.get_sync("k1") == b"v1"
    await cache.set("k2", b"v2", expires_in=30)
    assert await cache.get("k2") == b"v2"
    cache.delete_sync("k1")
    assert cache.get_sync("k1") is None

    storage = attach_storage(app)
    assert isinstance(storage, SyncStorageRegistry)
    store = storage.get()
    store.put("hello.txt", b"world")
    assert store.get("hello.txt") == b"world"

    async_storage = attach_storage(app, async_mode=True)
    assert isinstance(async_storage, AsyncStorageRegistry)
    async_store = async_storage.get()
    await async_store.put("async_hello.txt", b"async_world")
    assert bytes(await async_store.get("async_hello.txt")) == b"async_world"

    channels = attach_channels(app)
    assert isinstance(channels, ChannelsBackend)

    limiter = RateLimitGuard(capacity=2, refill_rate=0.0)
    assert limiter.allow("ip1", 1) is True
    assert limiter.allow("ip1", 1) is True
    assert limiter.allow("ip1", 1) is False

    auth = JWTAuthBackend("super-secret-key-that-is-32-bytes")
    assert await auth.authenticate(SimpleNamespace(headers={})) is None

    docs = mount_docs_routes(app, OpenAPIConfig())
    assert "/docs" in docs
    mcp_reg = mount_mcp_routes(app, webmcp_config=WebMCPConfig(enabled=True))
    assert mcp_reg.get_tool("create_item") is not None
    plan = offload_middlewares(app)
    assert plan.compression is not None


@pytest.mark.anyio
async def test_starlette_fastpath_inline_sync_and_send_raw_response() -> None:
    """Verify Starlette FastPath runs sync endpoints inline by default and wrap_asgi uses send_raw_response."""
    caller_tid = threading.get_ident()
    observed_tids: list[int] = []

    def sync_ping(request: Any) -> dict[str, str]:
        observed_tids.append(threading.get_ident())
        return {"pong": str(request.query_params.get("echo", "ok"))}

    app = SimpleNamespace(
        routes=[SimpleNamespace(path="/ping", endpoint=sync_ping, methods=["GET"], name="sync_ping")],
        user_middleware=[],
        exception_handlers={},
        state=SimpleNamespace(),
    )
    VertebraPlugin(app)
    fastpath: FastPath = app.__vertebra_fastpath__

    async def fallback_app(_scope: Any, _receive: Any, _send: Any) -> None:
        raise AssertionError("fallback_app must not be called")

    wrapped = fastpath.wrap_asgi(fallback_app)
    proto = AsgiProtocol(b"")
    raw_calls: list[int] = []

    class _RawSender:
        async def __call__(self, message: dict[str, Any]) -> None:
            await proto.send(message)

        async def send_raw_response(
            self,
            status: int,
            headers: list[tuple[bytes, bytes]],
            body: bytes,
        ) -> None:
            raw_calls.append(status)
            await cast("Awaitable[None]", proto.send_raw_response(status, headers, body))

    scope = AsgiScope.from_parts(method="GET", path="/sub/ping", query_string="echo=inline")
    scope["root_path"] = "/sub"
    await wrapped(scope, proto.receive, _RawSender())
    assert raw_calls == [200]
    assert observed_tids == [caller_tid]
    assert decode_json(proto.take_response().body) == {"pong": "inline"}


@pytest.mark.anyio
async def test_cache_store_delete_all_removes_only_its_prefix(resp3_server: Resp3FakeServer) -> None:
    """Starlette CacheStore.delete_all removes its prefixed keys and leaves other keys in place."""
    driver = AsyncRedisDriver(RedisConfig(url=resp3_server.redis_url))
    await driver.set("other:keep", b"x")
    cache = CacheStore(async_driver=driver)
    await cache.set("a", "1", expires_in=timedelta(milliseconds=250))
    assert 0 < await driver.pttl("vertebra:a") <= 250
    await cache.delete_all()
    assert await driver.get("other:keep") == b"x"
    assert await driver.get("vertebra:a") is None
    await driver.close()


@pytest.mark.anyio
@pytest.mark.parametrize("driver_kind", ["valkey", "redis"])
async def test_starlette_cache_store_driver_isolation_and_invalidation(
    resp3_server: Resp3FakeServer,
    driver_kind: str,
) -> None:
    """Verify Starlette CacheStore works across Valkey and Redis with prefix isolation and invalidation."""
    url = resp3_server.valkey_url if driver_kind == "valkey" else resp3_server.redis_url
    if driver_kind == "valkey":
        store1 = CacheStore.from_valkey(url, key_prefix="tenant1")
        store2 = CacheStore.from_valkey(url, key_prefix="tenant2")
    else:
        store1 = CacheStore.from_redis(url, key_prefix="tenant1")
        store2 = CacheStore.from_redis(url, key_prefix="tenant2")

    try:
        await store1.set("session", b"token-123", expires_in=30)
        await store2.set("session", b"token-456", expires_in=30)

        assert await store1.get("session") == b"token-123"
        assert await store2.get("session") == b"token-456"

        assert store1.get_sync("session") == b"token-123"
        assert store2.get_sync("session") == b"token-456"

        await store1.set_many({"k1": b"v1", "k2": b"v2"})
        many_res = await store1.get_many(["k1", "k2", "absent"])
        assert many_res == {"k1": b"v1", "k2": b"v2", "absent": None}

        await store1.delete("k1")
        assert await store1.get("k1") is None
        assert await store1.get("k2") == b"v2"

        await store1.delete_all()
        assert await store1.get("session") is None
        assert await store1.get("k2") is None
        assert await store2.get("session") == b"token-456"

        await store2.set("user:1", b"alice", expires_in=60)
        assert "tenant2:user:1" in store2._entries
        assert store2._async_driver is not None
        store2._async_driver.handle_invalidation_push(["tenant2:user:1"])
        assert "tenant2:user:1" not in store2._entries
    finally:
        await store1.close()
        await store2.close()


def test_channels_backend_accepts_engine_neutral_bus() -> None:
    """Starlette attachments accept both the adapter and the shared channel bus."""
    assert issubclass(ChannelsBackend, ChannelBus)
    app = SimpleNamespace(state=SimpleNamespace())
    bus = ChannelBus(history=2)
    assert attach_channels(app, bus) is bus
    assert app.state.channels is bus


@pytest.mark.anyio
async def test_starlette_mount_mcp_routes_uses_shared_asgi_mcp_app() -> None:
    """Starlette mount_mcp_routes delegates /mcp to shared AsgiMcpApp over MCPCore."""
    app = _make_mock_starlette_app()
    mcp_reg = mount_mcp_routes(app, webmcp_config=WebMCPConfig(enabled=True))
    assert mcp_reg.get_spec("create_item") is not None
    mcp_app = getattr(app.state, "mcp_app", None)
    assert isinstance(mcp_app, AsgiMcpApp)

    proto_discover = AsgiProtocol(b'{"jsonrpc":"2.0","id":1,"method":"server/discover"}')
    scope_discover = AsgiScope.from_parts(
        method="POST",
        path="/mcp",
        headers=[(b"content-type", b"application/json")],
        body=b'{"jsonrpc":"2.0","id":1,"method":"server/discover"}',
    )
    await mcp_app(scope_discover, proto_discover.receive, proto_discover.send)
    resp_discover = proto_discover.take_response()
    assert resp_discover.status == 200
    body_discover = decode_json(resp_discover.body)
    assert body_discover["result"]["protocolVersion"] == "2026-07-28"
    assert body_discover["result"]["_meta"]["resultType"] == "server/discover"

    proto_get = AsgiProtocol(b"")
    scope_get = AsgiScope.from_parts(method="GET", path="/mcp")
    await mcp_app(scope_get, proto_get.receive, proto_get.send)
    assert proto_get.take_response().status == 405

    proto_origin = AsgiProtocol(b'{"jsonrpc":"2.0","id":2,"method":"tools/list"}')
    scope_origin = AsgiScope.from_parts(
        method="POST",
        path="/mcp",
        headers=[
            (b"content-type", b"application/json"),
            (b"host", b"api.example.com"),
            (b"origin", b"https://evil.example.com"),
        ],
        body=b'{"jsonrpc":"2.0","id":2,"method":"tools/list"}',
    )
    await mcp_app(scope_origin, proto_origin.receive, proto_origin.send)
    assert proto_origin.take_response().status == 403

    proto_notify = AsgiProtocol(b'{"jsonrpc":"2.0","method":"notifications/cancelled"}')
    scope_notify = AsgiScope.from_parts(
        method="POST",
        path="/mcp",
        headers=[(b"content-type", b"application/json")],
        body=b'{"jsonrpc":"2.0","method":"notifications/cancelled"}',
    )
    await mcp_app(scope_notify, proto_notify.receive, proto_notify.send)
    resp_notify = proto_notify.take_response()
    assert resp_notify.status == 202
    assert bytes(resp_notify.body) == b""


@pytest.mark.anyio
async def test_starlette_compiled_route_base_attachment_and_buffered_responses() -> None:
    """Starlette plugin attaches __vertebra_fastpath__, inherits CompiledRouteBase, and copies zero buffered bytes."""
    assert issubclass(StarletteCompiledRoute, CompiledRouteBase)

    async def ping(_request: Any) -> JSONResponse:
        return JSONResponse({"ok": True})

    app = Starlette(routes=[Route("/ping", ping)])
    VertebraPlugin(app)
    assert get_attached_fastpath(app) is not None
    assert not hasattr(app.state, "vertebra_fastpath_bypass")

    raw_view = memoryview(b"zero-copy-payload")
    direct_resp = Response(content=raw_view)
    serialized = serialize_buffered_response(direct_resp)
    assert serialized is not None
    assert serialized[0] == 200
    assert isinstance(serialized[2], memoryview)
    assert serialized[2] is direct_resp.body

    bg_called = False

    def _bg() -> None:
        nonlocal bg_called
        bg_called = True

    bg_resp = Response(content=b"with-bg", background=BackgroundTask(_bg))
    assert serialize_buffered_response(bg_resp) is None

    subclass_asgi_called = False

    class _CustomJSONResponse(JSONResponse):
        async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
            nonlocal subclass_asgi_called
            subclass_asgi_called = True
            await super().__call__(scope, receive, send)

    assert _CustomJSONResponse not in BUFFERED_RESPONSE_TYPES
    custom_resp = _CustomJSONResponse({"custom": True})
    assert serialize_buffered_response(custom_resp) is None

    async def custom_endpoint(_request: Any) -> _CustomJSONResponse:
        return custom_resp

    custom_app = Starlette(routes=[Route("/custom", custom_endpoint)])
    VertebraPlugin(custom_app)
    fp = get_attached_fastpath(custom_app)
    assert fp is not None
    wrapped = fp.wrap_asgi(custom_app)
    proto = AsgiProtocol(b"")
    await wrapped(AsgiScope.from_parts(method="GET", path="/custom"), proto.receive, proto.send)
    assert subclass_asgi_called is True
    assert decode_json(proto.take_response().body) == {"custom": True}
    assert bg_called is False


@pytest.mark.anyio
async def test_starlette_queue_service_and_lifespan() -> None:
    """Verify Starlette attach_queue_service, get_queue_service, and queue_lifespan."""
    app = _make_mock_starlette_app()
    qsvc = attach_starlette_queue_service(app, StarletteQueueService(backend=MemoryQueueBackend()))
    assert get_starlette_queue_service(app) is qsvc

    executed: list[int] = []

    def job(val: int) -> int:
        executed.append(val)
        return val * 2

    qsvc.register(task(lane="io")(job))
    await qsvc.enqueue("job", 21)

    async with starlette_queue_lifespan(app, run_worker=False) as svc:
        assert svc is qsvc
        results = await svc.drain_once(("io",))
        assert results == [42]
        assert executed == [21]


@pytest.mark.anyio
async def test_starlette_alchemy_factory_routing_and_engine_instance() -> None:
    """Verify Starlette Alchemy config factories, routing_config, engine_instance, and app binding."""
    with pytest.raises(ValueError, match="at least one configuration"):
        create_starlette_alchemy()

    async_cfg = create_async_alchemy_config("sqlite+vtb_async:///:memory:", framework="starlette")
    sync_cfg = create_sync_alchemy_config("sqlite+vtb:///:memory:", framework="starlette")
    assert isinstance(async_cfg, StarletteSQLAlchemyAsyncConfig)
    assert isinstance(sync_cfg, StarletteSQLAlchemySyncConfig)

    app = Starlette()
    ext = create_starlette_alchemy(async_cfg, app=app)
    assert ext.config == [async_cfg]
    assert async_cfg.app is app

    routing_cfg = RoutingConfig(primary_connection_string="sqlite+vtb:///:memory:")
    routed_sync = create_sync_alchemy_config(framework="starlette", routing_config=routing_cfg)
    assert isinstance(routed_sync, StarletteSQLAlchemySyncConfig)
    assert routed_sync.routing_config is routing_cfg

    sync_engine = create_engine("sqlite+vtb:///:memory:")
    async_engine = create_async_engine("sqlite+vtb_async:///:memory:")
    explicit_ec = EngineConfig(pool_pre_ping=False, pool_recycle=cast("Any", None), poolclass=NullPool)
    try:
        prebuilt_sync = create_sync_alchemy_config(
            framework="starlette",
            engine_instance=sync_engine,
            engine_config=explicit_ec,
        )
        prebuilt_async = create_async_alchemy_config(
            framework="starlette",
            engine_instance=async_engine,
            engine_config=explicit_ec,
        )
        assert prebuilt_sync.engine_instance is sync_engine
        assert prebuilt_sync.engine_config is explicit_ec
        assert prebuilt_async.engine_instance is async_engine
        assert prebuilt_async.engine_config is explicit_ec
    finally:
        sync_engine.dispose()
        await async_engine.dispose()


def test_starlette_plugin_testclient_lifespan_cors_openapi_and_idempotency() -> None:
    """Verify TestClient executes wrapped lifespan, CORS offload, OpenAPI 304 ETag, exception handlers, and idempotency."""
    lifespan_counts = {"startup": 0, "shutdown": 0}
    openapi_hits = {"count": 0}

    class _BoomError(RuntimeError):
        pass

    @contextlib.asynccontextmanager
    async def custom_lifespan(target_app: Starlette) -> AsyncIterator[dict[str, str]]:
        lifespan_counts["startup"] += 1
        target_app.state.custom_started = True
        try:
            yield {"lifespan_marker": "ok"}
        finally:
            lifespan_counts["shutdown"] += 1
            target_app.state.custom_stopped = True

    async def ping(_request: Any) -> JSONResponse:
        return JSONResponse({"ok": True})

    async def openapi_json(_request: Any) -> JSONResponse:
        openapi_hits["count"] += 1
        return JSONResponse({"openapi": "3.1.0", "info": {"title": "Test", "version": "1.0.0"}})

    async def boom(_request: Any) -> JSONResponse:
        raise _BoomError("handled")

    async def handle_boom(_request: Any, exc: Exception) -> JSONResponse:
        return JSONResponse({"error": str(exc)}, status_code=418)

    app = Starlette(
        routes=[
            Route("/ping", ping, methods=["GET"]),
            Route("/openapi.json", openapi_json, methods=["GET"]),
            Route("/boom", boom, methods=["GET"]),
        ],
        middleware=[
            Middleware(
                CORSMiddleware,
                allow_origins=["https://app.example.com"],
                allow_methods=["GET", "POST"],
                allow_headers=["Authorization", "Content-Type"],
                allow_credentials=True,
                max_age=300,
            ),
        ],
        exception_handlers={_BoomError: handle_boom},
        lifespan=custom_lifespan,
    )

    plugin = VertebraPlugin()
    assert plugin.init_app(app) is app
    mw_count_after_first = len(app.user_middleware)
    lifespan_after_first = app.router.lifespan_context

    assert plugin.init_app(app) is app
    assert len(app.user_middleware) == mw_count_after_first
    assert app.router.lifespan_context is lifespan_after_first

    with TestClient(app) as client:
        assert lifespan_counts == {"startup": 1, "shutdown": 0}
        assert app.state.custom_started is True
        assert app.state.vertebra_lifespan_active is True

        preflight = client.options(
            "/ping",
            headers={
                "Origin": "https://app.example.com",
                "Access-Control-Request-Method": "GET",
                "Access-Control-Request-Headers": "Authorization",
            },
        )
        assert preflight.status_code == 204
        assert preflight.headers["access-control-allow-origin"] == "https://app.example.com"
        assert preflight.headers["access-control-allow-credentials"] == "true"
        assert "GET" in preflight.headers["access-control-allow-methods"]

        ping_resp = client.get("/ping", headers={"Origin": "https://app.example.com"})
        assert ping_resp.status_code == 200
        assert ping_resp.json() == {"ok": True}
        assert ping_resp.headers["access-control-allow-origin"] == "https://app.example.com"

        schema_resp = client.get("/openapi.json")
        assert schema_resp.status_code == 200
        assert schema_resp.json()["openapi"] == "3.1.0"
        etag = schema_resp.headers["etag"]
        assert etag
        assert openapi_hits["count"] == 1

        cached_resp = client.get("/openapi.json", headers={"If-None-Match": etag})
        assert cached_resp.status_code == 304
        assert cached_resp.content == b""
        assert openapi_hits["count"] == 1

        exc_resp = client.get("/boom")
        assert exc_resp.status_code == 418
        assert exc_resp.json() == {"error": "handled"}

    assert lifespan_counts == {"startup": 1, "shutdown": 1}
    assert app.state.custom_stopped is True
    assert app.state.vertebra_lifespan_active is False


@pytest.mark.parametrize(
    "blocked_modules",
    [
        ("litestar", "litestar_queues"),
        ("starlette", "starlette.routing", "starlette.responses", "starlette.requests", "starlette.middleware"),
    ],
)
def test_starlette_extension_imports_isolated_in_subprocess(blocked_modules: tuple[str, ...]) -> None:
    """Importing vertebra.extensions.starlette and its submodules succeeds without litestar or starlette."""
    script = f"""
import importlib
import sys

for mod in {list(blocked_modules)!r}:
    sys.modules[mod] = None

pkg = importlib.import_module("vertebra.extensions.starlette")
for sub in (
    "channels",
    "config",
    "fastpath",
    "mcp",
    "offload",
    "openapi",
    "plugin",
    "queues",
    "router",
    "security",
    "storage",
    "store",
    "vite",
):
    importlib.import_module(f"vertebra.extensions.starlette.{{sub}}")

for loaded_name in list(sys.modules):
    if loaded_name.startswith("vertebra.extensions.litestar"):
        raise AssertionError(f"Unexpected litestar import: {{loaded_name}}")

assert hasattr(pkg, "VertebraPlugin")
"""
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, f"stdout={proc.stdout}\nstderr={proc.stderr}"


def test_starlette_missing_dependency_error_only_when_helpers_run() -> None:
    """Blocking starlette allows imports but raises MissingDependencyError when Starlette helpers execute."""
    script = """
import importlib
import sys
from types import SimpleNamespace

for mod in ("starlette", "starlette.routing", "starlette.responses", "starlette.middleware"):
    sys.modules[mod] = None

from vertebra.extensions.starlette import (
    OpenAPIConfig,
    VertebraPlugin,
    ViteExtension,
    mount_docs_routes,
    mount_mcp_routes,
)
from vertebra.extensions.starlette.queues import (
    QueueService,
    attach_queue_service,
    get_queue_service,
)
from vertebra.extensions.starlette.vite import clear_vite_bridges, vite_asset, vite_hmr_client
from vertebra.framework.queues import MemoryQueueBackend
from vertebra.typing import MissingDependencyError

clear_vite_bridges()
vite_ext = ViteExtension()
app = SimpleNamespace(routes=[], state=SimpleNamespace())
vite_ext.init_app(app)
assert "<script" in vite_asset("src/main.ts")
assert vite_hmr_client() == ""

qsvc = attach_queue_service(app, QueueService(backend=MemoryQueueBackend()))
assert get_queue_service(app) is qsvc

for fn in (
    lambda: VertebraPlugin(app),
    lambda: mount_docs_routes(app, OpenAPIConfig()),
    lambda: mount_mcp_routes(app),
):
    try:
        fn()
    except MissingDependencyError:
        pass
    else:
        raise AssertionError("Expected MissingDependencyError when starlette is missing")
"""
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, f"stdout={proc.stdout}\nstderr={proc.stderr}"


@pytest.mark.anyio
async def test_starlette_jwt_auth_backend_and_vite_extension_delegation() -> None:
    """Verify Starlette JWTAuthBackend validates Bearer tokens and ViteExtension delegates to ViteBridge."""
    secret = "test-token-placeholder-secret-32b"
    token = jwt.encode(
        {"sub": "user-42", "scope": "read:items write:items", "exp": int(time.time()) + 300},
        secret,
        algorithm="HS256",
    )
    backend = JWTAuthBackend(secret)
    auth_res = await backend.authenticate(SimpleNamespace(headers={"authorization": f"Bearer {token}"}))
    assert auth_res is not None
    scopes, principal = auth_res
    assert scopes == ("read:items", "write:items")
    assert principal.sub == "user-42"
    assert principal.is_authenticated is True
    assert await backend.authenticate(SimpleNamespace(headers={"authorization": "Bearer invalid-token"})) is None

    from vertebra.extensions.starlette.vite import (
        ViteConfig,
        ViteExtension,
        clear_vite_bridges,
        vite_asset,
        vite_hmr_client,
    )

    clear_vite_bridges()
    vite = ViteExtension(config=ViteConfig(dev_mode=True, dev_server_url="http://localhost:5173"))
    app = SimpleNamespace(state=SimpleNamespace())
    vite.init_app(app)
    assert app.state.vite is vite
    assert app.state.vite_bridge is vite.bridge
    assert "http://localhost:5173/@vite/client" in vite_hmr_client()
    assert "http://localhost:5173/src/app.ts" in vite_asset("src/app.ts")
    clear_vite_bridges()
