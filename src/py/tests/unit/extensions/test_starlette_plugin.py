"""Unit tests for vertebra.extensions.starlette and 24-slot AsgiScope C-API fastpath."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import msgspec
import pytest

from vertebra._vertebra import AsgiScope
from vertebra.agent.mcp.controllers import mcp_tool
from vertebra.extensions.starlette import (
    CacheStore,
    ChannelsBackend,
    FastPath,
    JWTAuthBackend,
    OpenAPIConfig,
    RateLimitGuard,
    RouteCompiler,
    StarletteConfig,
    StarlettePlugin,
    StorageRegistry,
    SwaggerDocsConfig,
    WebMCPConfig,
    attach_cache_store,
    attach_channels,
    attach_storage,
    mount_docs_routes,
    mount_mcp_routes,
    offload_middlewares,
    preseed_request_body,
)


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
            vtb_req.decode_msgspec(_ItemPayload) if vtb_req is not None else msgspec.json.decode(raw, type=_ItemPayload)
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
    """Verify StarlettePlugin compiles routes, offloads middleware, and dispatches fastpath."""
    monkeypatch.setenv("VERTEBRA_SERVER_ACTIVE", "1")
    app = _make_mock_starlette_app()
    plugin = StarlettePlugin(
        app,
        config=StarletteConfig(
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
    scope_get = AsgiScope.from_parts(method="GET", path="/items/42")

    async def noop_receive() -> dict[str, Any]:
        raise AssertionError("receive() should not be called for GET or pre-seeded POST")

    async def noop_send(_msg: dict[str, Any]) -> None:
        return None

    res_get = await fastpath.dispatch(scope_get, noop_receive, noop_send)
    assert res_get is not None
    status, _headers, body = res_get
    assert status == 200
    assert msgspec.json.decode(body) == {"item_id": 42, "method": "GET"}
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
    assert msgspec.json.decode(body_post) == {"created": "bolt", "qty": 9, "body_len": 23}

    scope_sync = AsgiScope.from_parts(method="GET", path="/ping", query_string="echo=fast")
    res_sync = await fastpath.dispatch(scope_sync, noop_receive, noop_send)
    assert res_sync is not None
    assert msgspec.json.decode(res_sync[2]) == {"pong": "fast"}


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
    assert isinstance(storage, StorageRegistry)
    store = storage.get()
    store.put("hello.txt", b"world")
    assert store.get("hello.txt") == b"world"

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
