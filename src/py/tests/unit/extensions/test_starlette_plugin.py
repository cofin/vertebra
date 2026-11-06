"""Unit tests for vertebra.extensions.starlette and 24-slot AsgiScope C-API fastpath."""

from __future__ import annotations

import threading
from datetime import timedelta
from types import SimpleNamespace
from typing import Any

import msgspec
import pytest

from vertebra._vertebra import AsgiProtocol, AsgiScope
from vertebra.agent.mcp.controllers import mcp_tool
from vertebra.agent.mcp.routes import AsgiMcpApp
from vertebra.db.adapters.redis import AsyncRedisDriver, RedisConfig
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
from vertebra.server._channels import ChannelBus
from vertebra.server._fastpath import attach_framework_fastpath
from vertebra.utils.serializers import decode_json


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
        decoded = (
            vtb_req.decode_msgspec(_ItemPayload) if vtb_req is not None else decode_json(raw, schema_type=_ItemPayload)
        )
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
    assert decode_json(body) == {"item_id": 42, "method": "GET"}
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
    assert decode_json(body_post) == {"created": "bolt", "qty": 9, "body_len": 23}

    scope_sync = AsgiScope.from_parts(method="GET", path="/ping", query_string="echo=fast")
    res_sync = await fastpath.dispatch(scope_sync, noop_receive, noop_send)
    assert res_sync is not None
    assert decode_json(res_sync[2]) == {"pong": "fast"}


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
            await proto.send_raw_response(status, headers, body)

    scope = AsgiScope.from_parts(method="GET", path="/sub/ping", query_string="echo=inline")
    scope["root_path"] = "/sub"
    await wrapped(scope, proto.receive, _RawSender())
    assert raw_calls == [200]
    assert observed_tids == [caller_tid]
    assert decode_json(proto.take_response().body) == {"pong": "inline"}


@pytest.mark.anyio
async def test_cache_store_delete_all_removes_only_its_prefix() -> None:
    """Starlette CacheStore.delete_all removes its prefixed keys and leaves other keys in place."""
    driver = AsyncRedisDriver(RedisConfig(url="memory://"))
    await driver.set("other:keep", b"x")
    cache = CacheStore(async_driver=driver)
    await cache.set("a", "1", expires_in=timedelta(milliseconds=250))
    assert 0 < await driver.pttl("vertebra:a") <= 250
    await cache.delete_all()
    assert await driver.get("other:keep") == b"x"
    assert await driver.get("vertebra:a") is None
    await driver.close()


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
