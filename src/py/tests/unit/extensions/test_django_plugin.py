"""Unit tests for the standalone Django Vertebra extension and native DB backends."""

from __future__ import annotations

import asyncio
import io
import subprocess
import sys
import threading
import time
from importlib import import_module
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, ClassVar, get_type_hints
from urllib.parse import parse_qs, urlparse

import msgspec
import pyarrow as pa
import pydantic
import pytest
from django.core.exceptions import SuspiciousFileOperation
from django.core.files.base import ContentFile, File
from django.core.files.storage import Storage
from django.core.files.uploadedfile import UploadedFile
from django.db.migrations.writer import MigrationWriter
from django.db.models import FileField
from django.db.models.fields.files import FieldFile

from vertebra._vertebra import AsgiProtocol, AsgiScope, Request, WsgiEnviron, run_wsgi_callable
from vertebra.agent.mcp.models import MCPConfig as CanonicalMCPConfig
from vertebra.agent.mcp.routes import WsgiMcpApp
from vertebra.db.dbapi import SyncDBAPIConnection
from vertebra.db.registry import DatabaseRegistry
from vertebra.extensions.django import (
    ArrowManager,
    ArrowQuerySet,
    AsyncAPIDocsConfig,
    AsyncStorageRegistry,
    CacheBackend,
    ChannelLayer,
    DjangoStorage,
    EmailBackend,
    FastPath,
    MCPConfig,
    MsgspecView,
    OpenAPIConfig,
    ORMBridge,
    QueuesBackend,
    RouteCompiler,
    SecurityBridge,
    SwaggerDocsConfig,
    SyncStorageRegistry,
    TemplateBridge,
    VertebraAppConfig,
    VertebraConfig,
    VertebraPlugin,
    ViteConfig,
    attach_vite,
    convert_django_pattern,
    mcp_tool,
    vertebra_view,
)
from vertebra.extensions.django.db.backends.mysql import DatabaseWrapper as MySQLDatabaseWrapper
from vertebra.extensions.django.db.backends.oracle import DatabaseWrapper as OracleDatabaseWrapper
from vertebra.extensions.django.db.backends.postgresql import (
    DatabaseWrapper as PostgresDatabaseWrapper,
)
from vertebra.extensions.django.db.backends.sqlite3 import (
    DatabaseWrapper as SQLiteDatabaseWrapper,
)
from vertebra.extensions.django.fastpath import CompiledRoute as DjangoCompiledRoute
from vertebra.extensions.django.templatetags.vertebra_vite import vite_asset, vite_hmr_client
from vertebra.extensions.sqlspec.adapters import PostgresSyncConfig, SQLiteSyncConfig
from vertebra.server._fastpath_core import CompiledRouteBase
from vertebra.storage import (
    AsyncObjectStore,
    LocalConfig,
    MemoryConfig,
    S3Config,
    SigningConfig,
    SyncObjectStore,
)
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.serializers import decode_json

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator


class _ChunkRecordingUpload(UploadedFile):
    """Uploaded file with a tiny chunk size that records every chunk handed to storage."""

    DEFAULT_CHUNK_SIZE = 4

    def __init__(self, payload: bytes) -> None:
        super().__init__(io.BytesIO(payload), name="upload.bin", size=len(payload))
        self.consumed: list[bytes] = []

    def chunks(self, chunk_size: int | None = None) -> Iterator[bytes]:
        """Yield the parent chunks while recording each one."""
        for chunk in super().chunks(chunk_size):
            self.consumed.append(chunk)
            yield chunk


class OrderInput(msgspec.Struct, frozen=True, gc=False):
    """Order creation input."""

    sku: str
    qty: int


class OrderRecord(msgspec.Struct, frozen=True, gc=False):
    """Hydrated order DTO."""

    id: int
    sku: str
    qty: int


class PydanticOrderInput(pydantic.BaseModel):
    """Pydantic order creation input."""

    sku: str
    qty: int


class _MockPatternObj:
    """Minimal Django RoutePattern / RegexPattern double."""

    def __init__(self, route: str, *, regex: str | None = None) -> None:
        self._route = route
        self._regex = regex


class _MockURLPattern:
    """Minimal Django URLPattern double."""

    def __init__(
        self,
        route: str,
        callback: Callable[..., Any],
        *,
        name: str | None = None,
        regex: str | None = None,
    ) -> None:
        self.pattern = _MockPatternObj(route, regex=regex)
        self.callback = callback
        self.name = name


class _MockURLResolver:
    """Minimal Django URLResolver double."""

    def __init__(self, prefix: str, url_patterns: list[Any], *, namespace: str | None = None) -> None:
        self.pattern = _MockPatternObj(prefix)
        self.url_patterns = url_patterns
        self.namespace = namespace


class _MockDjangoSettings:
    """Minimal Django settings object double."""

    def __init__(self, static_root: str) -> None:
        self.MIDDLEWARE = [
            "django.middleware.security.SecurityMiddleware",
            "whitenoise.middleware.WhiteNoiseMiddleware",
            "corsheaders.middleware.CorsMiddleware",
            "django.middleware.gzip.GZipMiddleware",
        ]
        self.CORS_ALLOWED_ORIGINS = ["https://portal.example.com"]
        self.CORS_ALLOW_CREDENTIALS = True
        self.STATIC_URL = "/static/"
        self.STATIC_ROOT = static_root


def test_django_route_compiler_fastpath_openapi_and_mcp(tmp_path: Any) -> None:
    """Verify Django pattern conversion, nested URLResolver compilation, fastpath, OpenAPI, and MCP."""
    matchit_route, converters = convert_django_pattern("orders/<int:order_id>/<path:rest>/")
    assert matchit_route == "/orders/{order_id}/{*rest}/"
    assert converters == (("order_id", "int"), ("rest", "path"))

    re_route, re_converters = convert_django_pattern(
        r"^items/(?P<item_id>[0-9]+)/$",
        is_regex=True,
    )
    assert re_route == "/items/{item_id}/"
    assert re_converters == (("item_id", "int"),)

    @vertebra_view(methods=("POST",), status_code=201)
    @mcp_tool(name="create_django_order", description="Create an order in Django")
    def create_order_view(request: Request, order_id: int, body: OrderInput) -> dict[str, Any]:
        assert request.method == "POST"
        return {"id": order_id, "sku": body.sku, "qty": body.qty}

    @vertebra_view(methods=("POST",))
    def create_pydantic_view(body: PydanticOrderInput) -> PydanticOrderInput:
        return PydanticOrderInput(sku=body.sku.upper(), qty=body.qty + 2)

    class DetailView(MsgspecView):
        def get(self, request: Request, item_id: int) -> OrderRecord:
            return OrderRecord(id=item_id, sku="CBV-SKU", qty=5)

    urlpatterns: list[Any] = [
        _MockURLResolver(
            "api/",
            [
                _MockURLPattern(
                    "orders/<int:order_id>/",
                    create_order_view,
                    name="create_order",
                ),
                _MockURLPattern(
                    "pydantic/",
                    create_pydantic_view,
                    name="create_pydantic",
                ),
                _MockURLPattern(
                    r"^items/(?P<item_id>[0-9]+)/$",
                    DetailView.as_view(),
                    name="item_detail",
                    regex=r"^items/(?P<item_id>[0-9]+)/$",
                ),
            ],
            namespace="api",
        )
    ]

    static_dir = tmp_path / "staticfiles"
    static_dir.mkdir()
    settings = _MockDjangoSettings(str(static_dir))

    plugin = VertebraPlugin(
        urlpatterns=urlpatterns,
        settings=settings,
        config=VertebraConfig(
            openapi=OpenAPIConfig(
                title="Django Orders API",
                version="2.0.0",
                swagger=SwaggerDocsConfig(path="/swagger"),
                asyncapi=AsyncAPIDocsConfig(path="/asyncapi"),
                enabled_docs=("scalar", "swagger", "asyncapi"),
            ),
            mcp=MCPConfig(server_name="django-orders-mcp"),
            vite=ViteConfig(dev_mode=True, dev_server_url="http://localhost:5173"),
        ),
    )

    app_cfg = VertebraAppConfig()
    assert app_cfg.ready() is None

    post_req = Request.from_parts(
        method="POST",
        path="/api/orders/77/",
        headers=[(b"content-type", b"application/json")],
        body=b'{"sku":"WIDGET","qty":4}',
    )
    post_resp = plugin.handle_request(post_req)
    assert post_resp.status == 201
    assert decode_json(bytes(post_resp.body)) == {"id": 77, "sku": "WIDGET", "qty": 4}

    cbv_req = Request.from_parts(method="GET", path="/api/items/99/")
    cbv_resp = plugin.handle_request(cbv_req)
    assert cbv_resp.status == 200
    assert decode_json(bytes(cbv_resp.body)) == {"id": 99, "sku": "CBV-SKU", "qty": 5}

    def _fallback_wsgi(environ: Any, start_response: Callable[..., Any]) -> list[bytes]:
        start_response("404 Not Found", [("content-type", "text/plain")])
        return [b"not-found"]

    wrapped_wsgi = plugin.wrap_wsgi_application(_fallback_wsgi)
    wsgi_env = WsgiEnviron.from_parts(
        method="POST",
        path="/api/pydantic/",
        headers=[(b"content-type", b"application/json")],
        body=b'{"sku":"bolt","qty":8}',
    )
    wsgi_resp = run_wsgi_callable(wrapped_wsgi, wsgi_env)
    assert wsgi_resp.status == 200
    assert decode_json(bytes(wsgi_resp.body)) == {"sku": "BOLT", "qty": 10}

    openapi_resp = plugin.handle_request(Request.from_parts(method="GET", path="/openapi.json"))
    assert openapi_resp.status == 200
    schema_doc = decode_json(bytes(openapi_resp.body))
    assert "/api/orders/{order_id}/" in schema_doc["paths"]

    scalar_resp = plugin.handle_request(Request.from_parts(method="GET", path="/docs"))
    assert scalar_resp.status == 200
    assert b"@scalar/api-reference" in bytes(scalar_resp.body)

    swagger_resp = plugin.handle_request(Request.from_parts(method="GET", path="/swagger"))
    assert swagger_resp.status == 200
    assert b"SwaggerUIBundle" in bytes(swagger_resp.body)

    asyncapi_resp = plugin.handle_request(Request.from_parts(method="GET", path="/asyncapi"))
    assert asyncapi_resp.status == 200

    mcp_req = Request.from_parts(
        method="POST",
        path="/mcp",
        headers=[(b"content-type", b"application/json")],
        body=b'{"jsonrpc":"2.0","id":1,"method":"tools/list"}',
    )
    mcp_resp = plugin.handle_request(mcp_req)
    assert mcp_resp.status == 200
    mcp_data = decode_json(bytes(mcp_resp.body))
    assert any(t["name"] == "create_django_order" for t in mcp_data["result"]["tools"])

    assert plugin.offload_bundle is not None
    assert plugin.offload_bundle.compression is not None
    assert plugin.offload_bundle.cors is not None
    assert len(plugin.offload_bundle.static_mounts) == 1
    assert "django.middleware.gzip.GZipMiddleware" not in settings.MIDDLEWARE


def test_django_arrow_queryset_orm_bridge_and_db_backends() -> None:
    """Verify ArrowQuerySet, ORMBridge, and all 6 native Django database backends."""
    records = [
        {"id": 1, "sku": "A-100", "qty": 10},
        {"id": 2, "sku": "B-200", "qty": 25},
    ]
    manager = ArrowManager(records)
    qs: ArrowQuerySet = manager.get_queryset()

    table = qs.to_arrow()
    assert isinstance(table, pa.Table)
    assert table.num_rows == 2
    assert table.column("sku").to_pylist() == ["A-100", "B-200"]

    dtos = qs.to_schema(OrderRecord)
    assert dtos == [
        OrderRecord(id=1, sku="A-100", qty=10),
        OrderRecord(id=2, sku="B-200", qty=25),
    ]

    reloaded_Manager = ArrowManager()
    loaded_rows = reloaded_Manager.from_arrow(table)
    assert loaded_rows == records

    single_struct = ORMBridge.to_struct(records[0], OrderRecord)
    assert single_struct == OrderRecord(id=1, sku="A-100", qty=10)
    many_structs = ORMBridge.to_structs(records, OrderRecord)
    assert len(many_structs) == 2

    pg_wrapper = PostgresDatabaseWrapper(
        {
            "HOST": "10.0.0.5",
            "PORT": 6432,
            "NAME": "appdb",
            "OPTIONS": {
                "pgbouncer": True,
                "pooler_mode": "transaction",
                "direct_host": "10.0.0.6",
                "direct_port": 5432,
                "alloydb_instance_uri": "projects/p/locations/us-central1/clusters/c/instances/i",
            },
        },
        alias="default",
    )
    assert pg_wrapper.engine == "postgresql"
    assert pg_wrapper.pooler_config.pgbouncer is True
    assert pg_wrapper.pooler_config.pooler_mode == "transaction"
    assert pg_wrapper.build_alloydb_dialer() is not None
    direct_settings = pg_wrapper.derive_direct_settings()
    assert direct_settings["HOST"] == "10.0.0.6"
    assert direct_settings["PORT"] == 5432
    assert direct_settings["OPTIONS"]["pooler_mode"] == "session"

    with pg_wrapper.schema_editor() as editor:
        assert editor["pooler_mode"] == "session"
        assert editor["settings"]["PORT"] == 5432

    lease_id = pg_wrapper.acquire_lease()
    assert lease_id >= 0
    pg_wrapper.release_lease()

    mysql_wrapper = MySQLDatabaseWrapper(
        {
            "NAME": "mydb",
            "OPTIONS": {"cloud_sql_instance": "proj:us-central1:mysql-inst"},
        }
    )
    assert mysql_wrapper.engine == "mysql"
    assert mysql_wrapper.build_cloud_sql_dialer() is not None

    sqlite_wrapper = SQLiteDatabaseWrapper({"NAME": ":memory:"})
    assert sqlite_wrapper.engine == "sqlite3"

    oracle_wrapper = OracleDatabaseWrapper(
        {
            "NAME": "FREEPDB1",
            "OPTIONS": {"wallet_zip": b"PK\x03\x04", "vector_dimensions": 1536},
        }
    )
    assert oracle_wrapper.engine == "oracle"
    assert oracle_wrapper.wallet_zip == b"PK\x03\x04"


@pytest.mark.parametrize("backend", ["bigquery", "duckdb", "mssql"])
def test_unsupported_django_backends_raise_error(backend: str) -> None:
    """Deleted unsupported Django database backends fail on import and raise ImproperConfigurationError in plugin."""
    with pytest.raises(ModuleNotFoundError):
        import_module(f"vertebra.extensions.django.db.backends.{backend}")

    with pytest.raises(ImproperConfigurationError, match="is unsupported by Vertebra"):
        VertebraPlugin(
            settings={"DATABASES": {"default": {"ENGINE": f"vertebra.extensions.django.db.backends.{backend}"}}}
        )


@pytest.mark.anyio
@pytest.mark.parametrize("scheme", ["shm", "memory", "file"])
async def test_django_cache_backend_sync_and_async_matrix(scheme: str, tmp_path: Path) -> None:
    """CacheBackend executes full sync and async operations across multiple backends with L1 SHM."""
    if scheme == "file":
        location = f"file://{tmp_path / 'cache'}"
    elif scheme == "shm":
        location = f"shm://dj_test_{time.time_ns()}"
    else:
        location = "memory://"
    params = {"TIMEOUT": 60, "OPTIONS": {"L1_LOCATION": f"shm://dj_l1_{time.time_ns()}"}}
    cache = CacheBackend(location, params)
    try:
        cache.set("k_sync", "v_sync")
        assert cache.get("k_sync") == "v_sync"
        assert cache.has_key("k_sync") is True
        assert cache.add("k_sync", "v_dup") is False
        assert cache.add("k_sync2", "v_add") is True
        assert cache.get_many(["k_sync", "k_sync2"]) == {"k_sync": "v_sync", "k_sync2": "v_add"}
        cache.set_many({"k_m1": "v1", "k_m2": "v2"})
        assert cache.get("k_m1") == "v1"
        assert cache.touch("k_sync", timeout=120) is True
        cache.delete_many(["k_m1", "k_m2"])
        assert cache.get("k_m1") is None
        assert cache.delete("k_sync2") is True
        assert cache.delete("k_missing") is False

        await cache.aset("k_async", "v_async")
        assert await cache.aget("k_async") == "v_async"
        assert await cache.ahas_key("k_async") is True
        assert await cache.aadd("k_async", "v_dup") is False
        assert await cache.aadd("k_async2", "v_add") is True
        assert await cache.aget_many(["k_async", "k_async2"]) == {"k_async": "v_async", "k_async2": "v_add"}
        await cache.aset_many({"k_am1": "av1", "k_am2": "av2"})
        assert await cache.aget("k_am1") == "av1"
        assert await cache.atouch("k_async", timeout=120) is True
        await cache.adelete_many(["k_am1", "k_am2"])
        assert await cache.aget("k_am1") is None
        assert await cache.adelete("k_async2") is True

        assert cache.incr("num_sync", delta=10) == 10
        assert cache.decr("num_sync", delta=3) == 7
        assert await cache.aincr("num_async", delta=20) == 20
        assert await cache.adecr("num_async", delta=5) == 15

        cache.clear()
        assert cache.get("k_sync") is None
        await cache.aclear()
        assert await cache.aget("k_async") is None
    finally:
        cache.close()
        await cache.aclose()


@pytest.mark.anyio
async def test_django_cache_storage_mail_channels_queues_security_and_templates() -> None:
    """Verify Django CacheBackend, Storage, EmailBackend, ChannelLayer, QueuesBackend, Security, and Templates."""
    cache = CacheBackend("vertebra://default", {"TIMEOUT": 120, "KEY_PREFIX": "dj"})
    cache.set("user:1", {"name": "Alice"})
    assert cache.get("user:1") == {"name": "Alice"}
    await cache.aset("user:2", {"name": "Bob"})
    assert await cache.aget("user:2") == {"name": "Bob"}
    assert await cache.aget_many(["user:1", "user:2"]) == {
        "user:1": {"name": "Alice"},
        "user:2": {"name": "Bob"},
    }
    assert await cache.aincr("counter", delta=5) == 5
    assert await cache.adecr("counter", delta=2) == 3

    storage = DjangoStorage(MemoryConfig(), base_url="/media/")
    assert isinstance(storage.store, SyncObjectStore)
    assert isinstance(storage.async_store, AsyncObjectStore)
    saved_name = storage.save("avatars/a.txt", b"avatar-bytes")
    assert storage.exists(saved_name) is True
    with storage.open(saved_name) as handle:
        assert handle.read() == b"avatar-bytes"
    assert storage.size(saved_name) == 12
    assert storage.url(saved_name) == "/media/avatars/a.txt"
    await storage.asave("avatars/b.txt", b"async-avatar")
    with await storage.aopen("avatars/b.txt") as handle:
        assert handle.read() == b"async-avatar"
    assert await storage.aexists("avatars/b.txt") is True
    assert await storage.asize("avatars/b.txt") == 12
    assert await storage.aurl("avatars/b.txt") == "/media/avatars/b.txt"
    await storage.adelete("avatars/b.txt")
    assert await storage.aexists("avatars/b.txt") is False

    sync_reg = SyncStorageRegistry({"default": storage.store})
    assert sync_reg.get("default") is storage.store
    async_reg = AsyncStorageRegistry({"default": storage.async_store})
    assert async_reg.get("default") is storage.async_store

    email_backend = EmailBackend(suppress_send=True)
    assert email_backend.send_messages([{"subject": "S1"}]) == 1
    assert await email_backend.asend_messages([{"subject": "S2"}]) == 1
    assert len(email_backend.outbox) == 2

    layer = ChannelLayer()
    ch = await layer.new_channel("specific")
    await layer.group_add("updates", ch)
    await layer.group_send("updates", {"type": "order.updated", "id": 10})
    msg = await layer.receive(ch)
    assert msg == {"type": "order.updated", "id": 10}

    queues = QueuesBackend()

    @queues.task("sum_pair")
    def sum_pair(x: int, y: int) -> int:
        return x + y

    await queues.aenqueue("sum_pair", 19, 23)
    assert queues.run_next() == 42

    sec = SecurityBridge(jwt_secret=b"super-secret-key-at-least-32-bytes-long", rate_limit_capacity=5)
    assert sec.rate_limiter is not None
    assert sec.rate_limiter.allow("client-1") is True
    assert sec.jwt_guard is not None
    assert sec.jwt_guard.verify_header(None) is None

    attach_vite(config=ViteConfig(dev_mode=True, dev_server_url="http://localhost:5173"))
    assert "@vite/client" in str(vite_hmr_client())
    assert "http://localhost:5173/src/main.ts" in str(vite_asset("src/main.ts"))

    class _MockJinjaTemplate:
        def render(self, **kwargs: Any) -> str:
            return f"<h1>{kwargs.get('title', '')}</h1>{kwargs['vite_asset']('src/app.ts')}"

    class _MockJinjaEnv:
        def get_template(self, name: str) -> _MockJinjaTemplate:
            assert name == "index.html"
            return _MockJinjaTemplate()

    tpl_bridge = TemplateBridge(jinja_env=_MockJinjaEnv(), default_context={"title": "Dashboard"})
    rendered_resp = tpl_bridge.render_to_response("index.html")
    assert rendered_resp.status == 200
    assert b"<h1>Dashboard</h1>" in bytes(rendered_resp.body)
    assert b"http://localhost:5173/src/app.ts" in bytes(rendered_resp.body)


@pytest.mark.anyio
async def test_django_fastpath_async_views_cached_msgspec_view_wrap_asgi_and_send_raw_response() -> None:
    """Verify Django FastPath supports async def views, caches stateless MsgspecView, and uses send_raw_response."""

    @vertebra_view(methods=("POST",), status_code=201)
    async def create_async_order_view(
        request: Request,
        order_id: int,
        body: OrderInput,
    ) -> OrderRecord:
        assert request.method == "POST"
        await asyncio.sleep(0)
        return OrderRecord(id=order_id, sku=body.sku, qty=body.qty)

    class CachedAsyncItemView(MsgspecView):
        init_count: ClassVar[int] = 0

        def __init__(self, **kwargs: Any) -> None:
            type(self).init_count += 1
            for k, v in kwargs.items():
                setattr(self, k, v)

        def get(self, request: Request, item_id: int) -> OrderRecord:
            return OrderRecord(id=item_id, sku="SYNC-CBV", qty=1)

        async def post(self, request: Request, item_id: int, body: OrderInput) -> OrderRecord:
            return OrderRecord(id=item_id, sku=body.sku, qty=body.qty)

    cbv_fn = CachedAsyncItemView.as_view()
    assert CachedAsyncItemView.init_count == 1

    urlpatterns: list[Any] = [
        _MockURLPattern(
            "orders/<int:order_id>/",
            create_async_order_view,
            name="create_async_order",
        ),
        _MockURLPattern(
            "items/<int:item_id>/",
            cbv_fn,
            name="cached_item_view",
        ),
    ]
    compiler = RouteCompiler(urlpatterns=urlpatterns)
    fastpath = FastPath.from_app(SimpleNamespace(urlpatterns=urlpatterns))
    wrapped_wsgi = fastpath.wrap_wsgi()
    assert wrapped_wsgi.__vertebra_fastpath__ is fastpath
    assert not hasattr(wrapped_wsgi, "_vertebra_fastpath")
    assert len(compiler.entries) == len(fastpath.routes)

    get_req_1 = Request.from_parts(method="GET", path="/items/10/")
    get_req_2 = Request.from_parts(method="GET", path="/items/20/")
    resp_1 = fastpath.dispatch(get_req_1)
    resp_2 = fastpath.dispatch(get_req_2)
    assert resp_1 is not None and resp_2 is not None
    assert CachedAsyncItemView.init_count == 1

    post_cbv_req = Request.from_parts(
        method="POST",
        path="/items/30/",
        headers=[(b"content-type", b"application/json")],
        body=b'{"sku":"ASYNC-CBV","qty":4}',
    )
    post_cbv_resp = fastpath.dispatch(post_cbv_req)
    assert post_cbv_resp is not None
    assert decode_json(bytes(post_cbv_resp.body)) == {"id": 30, "sku": "ASYNC-CBV", "qty": 4}
    assert CachedAsyncItemView.init_count == 1

    raw_wsgi_calls: list[tuple[int, Any, bytes]] = []

    class _RawWsgiStartResponse:
        def __call__(self, _status_line: str, _headers: Any) -> None:
            raise AssertionError("Standard start_response must not be called when send_raw_response is present")

        def send_raw_response(self, status: int, headers: Any, body: bytes) -> list[bytes]:
            raw_wsgi_calls.append((status, headers, body))
            return [body]

    wsgi_env = WsgiEnviron.from_parts(
        method="POST",
        path="/items/40/",
        headers=[(b"content-type", b"application/json")],
        body=b'{"sku":"WSGI-RAW","qty":6}',
    )
    wsgi_chunks = fastpath.dispatch_wsgi(wsgi_env, _RawWsgiStartResponse())
    assert wsgi_chunks is not None
    assert len(raw_wsgi_calls) == 1
    assert raw_wsgi_calls[0][0] == 200
    assert decode_json(raw_wsgi_calls[0][2]) == {"id": 40, "sku": "WSGI-RAW", "qty": 6}

    asgi_app = fastpath.wrap_asgi()
    proto = AsgiProtocol(b'{"sku":"ASGI-DJ","qty":9}')
    scope = AsgiScope.from_parts(
        method="POST",
        path="/django/orders/55/",
        headers=[(b"content-type", b"application/json")],
        body=b'{"sku":"ASGI-DJ","qty":9}',
    )
    scope["root_path"] = "/django"
    await asgi_app(scope, proto.receive, proto.send)
    assert proto.complete is True
    assert proto.status == 201
    assert decode_json(proto.take_response().body) == {"id": 55, "sku": "ASGI-DJ", "qty": 9}


def test_cross_framework_shared_native_pool_and_lease_broker(tmp_path: Any) -> None:
    """Verify Django, SQLSpec, and SQLAlchemy (+vtb) share the same native driver pool, Cloud Dialer, and LeaseBroker."""
    alloy_uri = "projects/p/locations/us-central1/clusters/c/instances/i"
    dj_pg = PostgresDatabaseWrapper(
        {
            "HOST": "10.0.0.5",
            "PORT": 6432,
            "NAME": "appdb",
            "USER": "postgres",
            "OPTIONS": {
                "pgbouncer": True,
                "pooler_mode": "transaction",
                "alloydb_instance_uri": alloy_uri,
            },
        },
        alias="default",
    )
    sqlspec_pg = PostgresSyncConfig(
        dsn="postgresql://postgres@10.0.0.5:6432/appdb",
        pgbouncer=True,
        pooler_mode="transaction",
        alloydb_instance_uri=alloy_uri,
    )
    sa_pg = SyncDBAPIConnection(
        f"postgresql+vtb://postgres@10.0.0.5:6432/appdb?pgbouncer=true&pooler_mode=transaction&alloydb_instance_uri={alloy_uri}"
    )

    assert dj_pg.native_driver is sqlspec_pg.create_driver().native_driver
    assert dj_pg.native_driver is sa_pg.driver
    assert dj_pg.broker is DatabaseRegistry.default_broker()
    assert sa_pg.broker is DatabaseRegistry.default_broker()
    assert dj_pg.native_driver._dialer is not None

    db_file = tmp_path / "shared_cross_fw.db"
    dj_sqlite = SQLiteDatabaseWrapper({"NAME": str(db_file)})
    sqlspec_sqlite = SQLiteSyncConfig(path=str(db_file))
    sa_sqlite = SyncDBAPIConnection(f"sqlite+vtb:///{db_file}")

    assert dj_sqlite.native_driver is sqlspec_sqlite.create_driver().native_driver
    assert dj_sqlite.native_driver is sa_sqlite.driver
    assert dj_sqlite.broker is DatabaseRegistry.default_broker()

    isolated_dj = SQLiteDatabaseWrapper(
        {
            "NAME": str(db_file),
            "OPTIONS": {"share_pool": False},
        }
    )
    isolated_sqlspec = SQLiteSyncConfig(path=str(db_file), share_pool=False)
    isolated_sa = SyncDBAPIConnection(f"sqlite+vtb:///{db_file}", share_pool=False)

    assert isolated_dj.native_driver is not dj_sqlite.native_driver
    assert isolated_dj.broker is not DatabaseRegistry.default_broker()
    assert isolated_sqlspec.create_driver().native_driver is not dj_sqlite.native_driver
    assert isolated_sa.driver is not dj_sqlite.native_driver


def test_django_mcp_shares_wsgi_mcp_app_and_reuses_persistent_portal() -> None:
    """Django MCP uses canonical MCPConfig, WsgiMcpApp over MCPCore, and reuses the persistent Portal loop."""
    assert MCPConfig is CanonicalMCPConfig

    observed_loops: list[tuple[int, int]] = []

    @vertebra_view(methods=("POST",), status_code=200)
    @mcp_tool(name="django_probe_loop", description="Probe active event loop in Django")
    async def django_probe_loop(seq: int) -> dict[str, int]:
        loop = asyncio.get_running_loop()
        observed_loops.append((id(loop), threading.get_ident()))
        return {"seq": seq}

    urlpatterns: list[Any] = [
        _MockURLPattern(
            "probe/<int:seq>/",
            django_probe_loop,
            name="django_probe_loop",
        )
    ]
    plugin = VertebraPlugin(
        urlpatterns=urlpatterns,
        config=VertebraConfig(mcp=MCPConfig(server_name="django-mcp")),
    )
    assert isinstance(getattr(plugin, "_mcp_app", None), WsgiMcpApp)

    discover_resp = plugin.handle_request(
        Request.from_parts(
            method="POST",
            path="/mcp",
            headers=[(b"content-type", b"application/json")],
            body=b'{"jsonrpc":"2.0","id":1,"method":"server/discover"}',
        )
    )
    assert discover_resp.status == 200
    assert decode_json(bytes(discover_resp.body))["result"]["protocolVersion"] == "2026-07-28"

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
            body=b'{"jsonrpc":"2.0","id":2,"method":"tools/list"}',
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

    wrapped_wsgi = plugin.wrap_wsgi_application(lambda _e, sr: (sr("404 Not Found", []), [b""])[1])
    wsgi_get = run_wsgi_callable(wrapped_wsgi, WsgiEnviron.from_parts(method="GET", path="/mcp"))
    assert wsgi_get.status == 405

    for seq in (1, 2):
        wsgi_call = run_wsgi_callable(
            wrapped_wsgi,
            WsgiEnviron.from_parts(
                method="POST",
                path="/mcp",
                headers=[(b"content-type", b"application/json")],
                body=f'{{"jsonrpc":"2.0","id":{seq},"method":"tools/call","params":{{"name":"django_probe_loop","arguments":{{"seq":{seq}}}}}}}'.encode(),
            ),
        )
        assert wsgi_call.status == 200

    assert len(observed_loops) == 2
    assert observed_loops[0] == observed_loops[1]


def test_django_storage_deconstructs_and_requires_configuration(tmp_path: Path) -> None:
    """Django storage reconstructs through its public migration constructor."""
    with pytest.raises(ImproperConfigurationError):
        DjangoStorage()
    storage = DjangoStorage(LocalConfig(root=tmp_path), base_url="/media/")
    assert isinstance(storage, Storage)
    path, args, kwargs = storage.deconstruct()
    module, _, name = path.rpartition(".")
    restored = getattr(import_module(module), name)(*args, **kwargs)
    saved = restored.save("readme.txt", ContentFile(b"hello"))
    with restored.open(saved) as handle:
        assert isinstance(handle, File)
        assert handle.read() == b"hello"


@pytest.mark.parametrize("method", [DjangoStorage.aopen, DjangoStorage._open])
def test_django_storage_file_annotations_support_runtime_introspection(method: Any) -> None:
    """Runtime annotation resolution uses Django's concrete unsubscriptable File."""
    assert get_type_hints(method)["return"] is File


@pytest.mark.anyio
@pytest.mark.parametrize("backend", ["memory", "local"])
@pytest.mark.parametrize("consumer", ["field", "async"])
@pytest.mark.parametrize("name", ["pyproject.toml", "missing-from-cwd.txt"])
async def test_django_storage_reopens_through_native_store(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, backend: str, consumer: str, name: str
) -> None:
    """Closed Django files reopen their native key, including cwd name collisions."""
    directory = tmp_path / "cwd"
    directory.mkdir()
    (directory / "pyproject.toml").write_bytes(b"unrelated local file")
    monkeypatch.chdir(directory)
    config = MemoryConfig(namespace="field-reopen") if backend == "memory" else LocalConfig(root=tmp_path / "objects")
    storage = DjangoStorage(config, location="uploads")
    storage.save(name, b"native payload")
    handle = FieldFile(None, FileField(storage=storage), name) if consumer == "field" else await storage.aopen(name)
    handle.open("rb")
    assert handle.read() == b"native payload"
    handle.close()
    handle.open("rb")
    assert handle.read() == b"native payload"
    handle.close()


@pytest.mark.parametrize("backend", ["memory", "local"])
def test_django_storage_migration_writer_round_trips_configuration(tmp_path: Path, backend: str) -> None:
    """Django's migration serializer preserves config fields and storage options."""
    config = (
        MemoryConfig(namespace="migration-namespace")
        if backend == "memory"
        else LocalConfig(root=tmp_path, create_dirs=False)
    )
    storage = DjangoStorage(config, location="uploads", base_url="/media/", file_overwrite=True, expires_in=71)
    expression, imports = MigrationWriter.serialize(storage)
    namespace: dict[str, Any] = {}
    exec("\n".join(sorted(imports)), namespace)
    restored = eval(expression, namespace)
    assert isinstance(restored, Storage)
    _, args, kwargs = restored.deconstruct()
    assert args == (config,)
    assert kwargs == {"location": "uploads", "base_url": "/media/", "file_overwrite": True, "expires_in": 71}
    assert restored.store.options == storage.store.options
    saved = restored.save("migration.txt", ContentFile(b"migrated"))
    with restored.open(saved) as handle:
        assert handle.read() == b"migrated"
    assert restored.url(saved) == "/media/uploads/migration.txt"


def test_django_storage_migration_writer_preserves_provider_options() -> None:
    """Public config serialization retains provider options without contacting a cloud."""
    config = S3Config(
        bucket="migration-tests",
        region="eu-west-2",
        endpoint="http://storage.test:9000",
        access_key_id="fixture-access-key",
        secret_access_key="fixture-secret-key",
        session_token="fixture-session-token",
        virtual_hosted_style=True,
        allow_http=True,
    )
    expression, imports = MigrationWriter.serialize(config)
    namespace: dict[str, Any] = {}
    exec("\n".join(sorted(imports)), namespace)
    restored = eval(expression, namespace)
    assert restored == config
    assert restored.to_url_and_options() == config.to_url_and_options()


@pytest.mark.parametrize("config", [None, "", "   "])
def test_django_storage_rejects_empty_configuration(config: str | None) -> None:
    """Missing or blank configuration never creates an implicit memory store."""
    with pytest.raises(ImproperConfigurationError):
        DjangoStorage(config)


@pytest.mark.parametrize("target", ["uri", "sync", "async", "async_keyword", "precedence"])
def test_django_storage_explicit_targets_share_native_data(tmp_path: Path, target: str) -> None:
    """URI and explicit handle constructor forms use the selected native engine."""
    native = SyncObjectStore.from_local(tmp_path)
    if target == "uri":
        storage = DjangoStorage(native.url)
    elif target == "sync":
        storage = DjangoStorage(native)
    elif target == "async":
        storage = DjangoStorage(native.to_async())
    elif target == "async_keyword":
        storage = DjangoStorage(async_store=native.to_async())
    else:
        storage = DjangoStorage("", store=native, async_store=SyncObjectStore("memory://").to_async())
    assert storage.save("empty.bin", b"") == "empty.bin"
    assert native.exists("empty.bin")
    with storage.open("empty.bin") as handle:
        assert handle.read() == b""


def test_django_storage_named_text_content_file() -> None:
    """Django text files retain named-content saving and UTF-8 payloads."""
    storage = DjangoStorage(MemoryConfig())
    name = storage.save(None, ContentFile("héllo", name="named.txt"))
    assert name == "named.txt"
    with storage.open(name) as handle:
        assert handle.read() == "héllo".encode()


@pytest.mark.parametrize("module_name", ["vertebra.extensions.django", "vertebra.extensions.django.storage"])
def test_django_extension_modules_export_storage_adapter(module_name: str) -> None:
    """Django extension modules export the storage adapter and its registries."""
    exported = set(import_module(module_name).__all__)
    assert {"DjangoStorage", "SyncStorageRegistry", "AsyncStorageRegistry"} <= exported


@pytest.mark.parametrize(
    ("content", "expected"),
    [
        (b"raw-bytes", b"raw-bytes"),
        (bytearray(b"mutable-buffer"), b"mutable-buffer"),
        (memoryview(b"zero-copy-view"), b"zero-copy-view"),
        ("text-é", "text-é".encode()),
    ],
    ids=["bytes", "bytearray", "memoryview", "str"],
)
def test_django_storage_save_writes_buffers_and_text_directly(content: Any, expected: bytes) -> None:
    """Buffer and text payloads allocate a Django name and land in the store byte-for-byte."""
    storage = DjangoStorage(MemoryConfig(), location="uploads")
    first = storage.save("payload.bin", content)
    second = storage.save("payload.bin", content)
    assert first == "payload.bin"
    assert second != first
    assert storage.store.get("uploads/payload.bin") == expected
    with storage.open(second) as handle:
        assert handle.read() == expected


def test_django_storage_save_requires_name_for_buffer_payloads() -> None:
    """Anonymous buffer payloads fail before touching the store."""
    storage = DjangoStorage(MemoryConfig())
    with pytest.raises(ValueError, match="file name is required"):
        storage.save(None, b"anonymous")


@pytest.mark.anyio
@pytest.mark.parametrize(
    "payload", [b"anonymous", bytearray(b"anonymous"), "anonymous"], ids=["bytes", "bytearray", "str"]
)
async def test_django_storage_asave_requires_name_for_buffer_payloads(payload: bytes | bytearray | str) -> None:
    """Async anonymous buffer and text payloads fail with the same error as the sync path and write nothing."""
    store = SyncObjectStore("memory://")
    storage = DjangoStorage(store)
    with pytest.raises(ValueError, match="file name is required"):
        await storage.asave(None, payload)
    assert store.list() == []


def test_django_storage_save_streams_multi_chunk_uploads() -> None:
    """Multi-chunk uploads stream chunk by chunk and read back the exact concatenated bytes."""
    upload = _ChunkRecordingUpload(b"abcdefghijkl")
    assert upload.multiple_chunks() is True
    storage = DjangoStorage(MemoryConfig(), location="uploads")
    name = storage.save(None, upload)
    assert name == "upload.bin"
    assert upload.consumed == [b"abcd", b"efgh", b"ijkl"]
    with storage.open(name) as handle:
        assert handle.read() == b"abcdefghijkl"


def test_django_storage_naming_files_and_metadata(tmp_path: Path) -> None:
    """Native-backed files honor Django collision, namespace, and metadata contracts."""
    storage = DjangoStorage(LocalConfig(root=tmp_path), location="uploads", base_url="/media/")
    first = storage.save("sub/readme.txt", ContentFile(b"hello"))
    second = storage.save("sub/readme.txt", b"another")
    assert first != second
    with storage.open(first) as handle:
        assert isinstance(handle, File)
        assert handle.size == 5
        assert handle.read(2) == b"he"
        assert handle.seek(0) == 0
        assert handle.read() == b"hello"
    storage.save("root.txt", ContentFile(b"root"))
    storage.store.put("uploads-neighbor/outside.txt", b"outside")
    assert storage.listdir("") == (["sub"], ["root.txt"])
    assert storage.listdir("sub")[0] == []
    assert sorted(storage.listdir("sub")[1]) == sorted([Path(first).name, Path(second).name])
    assert storage.path("root.txt") == str(tmp_path / "uploads" / "root.txt")
    modified = storage.get_modified_time("root.txt")
    assert modified.tzinfo is not None
    assert modified.utcoffset().total_seconds() == 0
    with pytest.raises(NotImplementedError):
        storage.get_created_time("root.txt")
    with pytest.raises(NotImplementedError):
        storage.get_accessed_time("root.txt")
    assert storage.url("a b.txt") == "/media/uploads/a%20b.txt"
    short = storage.save("a-very-long-filename.txt", b"short", max_length=18)
    assert len(short) <= 18
    with pytest.raises(SuspiciousFileOperation):
        storage.save("long.txt", b"short", max_length=2)
    overwrite = DjangoStorage(store=storage.store, location="uploads", file_overwrite=True)
    assert overwrite.save(first, ContentFile(b"new")) == first
    with overwrite.open(first) as handle:
        assert handle.read() == b"new"
    with pytest.raises(ValueError):
        storage.open(first, "wb")
    with pytest.raises(NotImplementedError):
        DjangoStorage(MemoryConfig()).path("root.txt")
    with pytest.raises(ValueError):
        DjangoStorage(MemoryConfig()).url("root.txt")


@pytest.mark.parametrize("name", ["../outside.txt", "sub/../../outside.txt", "/absolute.txt", r"..\outside.txt"])
@pytest.mark.parametrize("operation", ["open", "save", "delete", "exists", "size", "url", "path", "listdir"])
def test_django_storage_rejects_namespace_escape(tmp_path: Path, name: str, operation: str) -> None:
    """Every public file operation rejects traversal before adding its location."""
    storage = DjangoStorage(LocalConfig(root=tmp_path), location="uploads", base_url="/media/")
    args = (name, b"payload") if operation == "save" else (name,)
    with pytest.raises(SuspiciousFileOperation):
        getattr(storage, operation)(*args)


@pytest.mark.anyio
async def test_django_storage_async_files_collisions_streams_and_urls(tmp_path: Path) -> None:
    """Async operations preserve native writes and return seekable Django handles."""
    storage = DjangoStorage(LocalConfig(root=tmp_path), location="uploads", base_url="/media/")
    first = await storage.asave("async.bin", ContentFile(b"xyz"))
    second = await storage.asave("async.bin", b"second")
    assert first != second
    with await storage.aopen(first) as handle:
        assert isinstance(handle, File)
        assert handle.read() == b"xyz"
    assert await storage.aexists(first)
    assert await storage.asize(first) == 3
    assert await storage.aurl("a b.txt") == "/media/uploads/a%20b.txt"
    await storage.adelete(first)
    assert not await storage.aexists(first)
    short = await storage.asave("very-long-filename.txt", b"short", max_length=18)
    assert len(short) <= 18
    with pytest.raises(SuspiciousFileOperation):
        await storage.asave("long.txt", b"short", max_length=2)
    with pytest.raises(SuspiciousFileOperation):
        await storage.asave("../outside", b"bad")
    with pytest.raises(ValueError):
        await storage.aopen(second, "wb")

    async def chunks() -> Any:
        yield b"one"
        yield b"two"

    streamed = await storage.asave("stream.bin", chunks())
    with storage.open(streamed) as handle:
        assert handle.read() == b"onetwo"
    overwrite = DjangoStorage(async_store=storage.async_store, location="uploads", file_overwrite=True)
    assert await overwrite.asave(second, b"replaced") == second
    with await overwrite.aopen(second) as handle:
        assert handle.read() == b"replaced"
    text_name = await storage.asave(None, ContentFile("héllo", name="named.txt"))
    with await storage.aopen(text_name) as handle:
        assert handle.read() == "héllo".encode()


@pytest.mark.anyio
@pytest.mark.parametrize("operation", ["aopen", "asave", "adelete", "aexists", "asize", "aurl"])
async def test_django_storage_async_rejects_namespace_escape(operation: str) -> None:
    """Async file operations reject traversal without uploading or probing it."""
    storage = DjangoStorage(MemoryConfig(), location="uploads", base_url="/media/")
    args = ("../outside", b"bad") if operation == "asave" else ("../outside",)
    with pytest.raises(SuspiciousFileOperation):
        await getattr(storage, operation)(*args)


@pytest.mark.anyio
async def test_django_storage_signs_qualified_keys() -> None:
    """Configured native signing takes precedence and honors URL expiry."""
    store = SyncObjectStore(
        "memory://", signing=SigningConfig(hmac_secret="django-secret", endpoint="https://files.test")
    )
    storage = DjangoStorage(store=store, location="uploads", expires_in=37)
    before = int(time.time())
    signed = storage.url("a b.txt")
    assert store.verify_presigned_url(signed)
    assert "/uploads/a%20b.txt" in signed
    expiry = int(parse_qs(urlparse(signed).query)["X-Vtb-Expires"][0])
    assert before + 37 <= expiry <= int(time.time()) + 37
    before = int(time.time())
    async_signed = await storage.aurl("a b.txt", expire=71)
    assert store.verify_presigned_url(async_signed)
    expiry = int(parse_qs(urlparse(async_signed).query)["X-Vtb-Expires"][0])
    assert before + 71 <= expiry <= int(time.time()) + 71
    assert signed != async_signed


def test_django_storage_import_without_optional_django() -> None:
    """A missing optional Django dependency fails only when constructing storage."""
    script = """
import sys
sys.modules['django'] = None
from vertebra.extensions.django.storage import DjangoStorage
from vertebra.typing import MissingDependencyError
try:
    DjangoStorage('memory://')
except MissingDependencyError as exc:
    assert 'django' in str(exc)
else:
    raise AssertionError('missing Django accepted')
"""
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr


def test_django_compiled_route_inherits_compiled_route_base() -> None:
    """Django CompiledRoute subclasses CompiledRouteBase."""
    assert issubclass(DjangoCompiledRoute, CompiledRouteBase)
