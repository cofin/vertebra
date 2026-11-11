"""Unit tests for vertebra.framework.* subdomains, Wasm host, and Litestar ecosystem bridges."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Annotated, Any, cast

import msgspec
import pytest

from vertebra._vertebra import Response
from vertebra.db.adapters.redis import AsyncRedisDriver, RedisConfig
from vertebra.extensions.litestar import VertebraConfig, VertebraPlugin
from vertebra.framework._app import App, _make_handler_id
from vertebra.framework.asyncapi import AsyncAPISpecBuilder, AsyncChannelDef
from vertebra.framework.channels import ChannelBus
from vertebra.framework.di import Container, DependencyGraph, Inject, Provide
from vertebra.framework.email import EmailConfig, EmailMessage, EmailService
from vertebra.framework.htmx import HTMXRequest, HTMXResponse
from vertebra.framework.openapi import OpenAPIConfig, OpenAPISpecBuilder
from vertebra.framework.procedures import Procedure, ProcedureRouter, mutation, query
from vertebra.framework.queues import CronSchedule, QueueConfig, QueueService, TaskRetryPolicy, task
from vertebra.framework.security import (
    CsrfConfig,
    CsrfMiddleware,
    Guard,
    PermissionDeniedError,
    Principal,
    RateLimitConfig,
    RateLimiter,
    SecurityContext,
    requires_capability,
    requires_role,
    requires_scope,
    requires_tenant,
)
from vertebra.framework.ssr import SSRFrame
from vertebra.framework.stores import KeyValueStore, NamespacedStore
from vertebra.typing import MissingDependencyError, UnsupportedBackendError
from vertebra.wasm import WASM_MAGIC_HEADER, WASMModuleConfig, WASMSandboxPolicy

if TYPE_CHECKING:
    from tests.unit.conftest import Resp3FakeServer


class ItemSchema(msgspec.Struct, frozen=True, gc=False):
    """Sample msgspec schema for OpenAPI 3.1.0 generation."""

    id: int
    name: str


@pytest.mark.anyio
async def test_di_container_topological_resolution_and_cycle_detection() -> None:
    """Verify DependencyGraph topological compilation, singleton/request scopes, and cycle detection."""
    calls = 0

    def provide_config() -> dict[str, str]:
        nonlocal calls
        calls += 1
        return {"env": "prod"}

    async def provide_service(config: dict[str, str]) -> str:
        return f"svc:{config['env']}"

    graph = DependencyGraph(
        {
            "config": Provide(provide_config, scope="singleton"),
            "service": Provide(provide_service, scope="request"),
        }
    )
    assert graph.topological_order == ("config", "service")
    container = Container(graph)
    assert await container.resolve("service") == "svc:prod"
    assert await container.resolve("service") == "svc:prod"
    assert calls == 1

    cycle_graph = DependencyGraph()
    cycle_graph.register("a", lambda b: b)
    cycle_graph.register("b", lambda a: a)
    with pytest.raises(ValueError, match="Circular dependency"):
        cycle_graph.compile()


@pytest.mark.anyio
async def test_di_type_resolution_generators_inject_markers_and_scopes() -> None:
    """Verify type-indexed DI resolution, Inject markers, sync/async generator teardown, and transient scope."""
    events: list[str] = []

    class DbConfig(msgspec.Struct, frozen=True, gc=False):
        dsn: str = "sqlite:///:memory:"

    class SyncSession:
        def __init__(self, dsn: str) -> None:
            self.dsn = dsn

    class AsyncSession:
        def __init__(self, dsn: str) -> None:
            self.dsn = dsn

    def provide_sync_session(cfg: Annotated[DbConfig, Inject("replica_cfg")]) -> Iterator[SyncSession]:
        events.append("sync_enter")
        try:
            yield SyncSession(cfg.dsn)
        finally:
            events.append("sync_exit")

    async def provide_async_session(cfg: DbConfig) -> AsyncIterator[AsyncSession]:
        events.append("async_enter")
        try:
            yield AsyncSession(cfg.dsn)
        finally:
            events.append("async_exit")

    counter = 0

    def provide_transient_id() -> int:
        nonlocal counter
        counter += 1
        return counter

    container = Container(
        {
            "cfg": DbConfig(dsn="postgres://primary"),
            "replica_cfg": DbConfig(dsn="postgres://replica"),
            "sync_session": provide_sync_session,
            "async_session": provide_async_session,
            "transient_id": Provide(provide_transient_id, scope="transient"),
        }
    )

    with container.scope() as sync_scope:
        s1 = sync_scope.resolve_sync(SyncSession)
        s2 = sync_scope.resolve_sync("sync_session")
        assert s1 is s2
        assert s1.dsn == "postgres://replica"
        t1 = sync_scope.resolve_sync("transient_id")
        t2 = sync_scope.resolve_sync("transient_id")
        assert (t1, t2) == (1, 2)
        assert "sync_exit" not in events
    assert events == ["sync_enter", "sync_exit"]

    events.clear()
    async with container.scope() as async_scope:
        a1 = await async_scope.resolve(AsyncSession)
        a2 = await async_scope.resolve("async_session")
        assert a1 is a2
        assert "async_exit" not in events
    assert events == ["async_enter", "async_exit"]

    rpc = ProcedureRouter(
        dependencies={"primary": DbConfig(dsn="postgres://rpc-primary")},
        container=container,
    )

    @rpc.query()
    async def read_dsn(cfg: Annotated[DbConfig, Inject("primary")]) -> str:
        return cfg.dsn

    assert isinstance(read_dsn, Procedure)
    assert await rpc.call("read_dsn") == "postgres://rpc-primary"
    await container.aclose()


def test_security_principal_and_guards() -> None:
    """Verify Principal and RBAC/ABAC guards (requires_role, requires_scope, requires_tenant, requires_capability)."""
    principal = Principal(
        subject="user-1",
        tenant_id="org-acme",
        roles=frozenset({"admin", "editor"}),
        scopes=frozenset({"items:read", "items:write"}),
        capabilities=frozenset({"approve_po"}),
    )
    ctx = SecurityContext(principal=principal)
    verified = ctx.enforce(
        requires_role("admin"),
        requires_scope("items:write"),
        requires_tenant("org-acme"),
        requires_capability("approve_po"),
    )
    assert verified.subject == "user-1"

    with pytest.raises(PermissionDeniedError):
        ctx.enforce(requires_role("superadmin"))


@pytest.mark.anyio
async def test_queues_channels_stores_email_and_procedures() -> None:
    """Verify QueueService, ChannelsBackend, NamespacedStore, EmailService, and ProcedureRouter."""
    qsvc = QueueService()

    @task(lane="cpu")
    async def compute_square(n: int) -> int:
        return n * n

    qsvc.register(compute_square)
    env = await qsvc.enqueue("compute_square", 9)
    assert env.lane == "cpu"
    assert await qsvc.drain_once() == [81]

    bus = ChannelBus(history=5)
    await bus.on_startup()
    await bus.publish(b"evt", ["orders"])
    assert await bus.get_history("orders") == [b"evt"]
    await bus.on_shutdown()

    kv = KeyValueStore()
    ns = NamespacedStore(kv, "tenant-a")
    await ns.set("token", b"xyz")
    assert await ns.get("token") == b"xyz"

    esvc = EmailService()
    receipt = await esvc.send(
        EmailMessage(subject="Welcome", to=("user@example.com",), from_email="noreply@example.com", text_body="Hello")
    )
    assert receipt == "msg-1"
    assert len(esvc.outbox) == 1

    router = ProcedureRouter()

    @query()
    async def get_version() -> str:
        return "0.1.0"

    @mutation()
    def double_value(val: int) -> int:
        return val * 2

    router.register(get_version)
    router.register(double_value)
    assert await router.call("get_version") == "0.1.0"
    assert await router.call("double_value", 21) == 42


def test_htmx_ssr_openapi_and_asyncapi() -> None:
    """Verify HTMXRequest/HTMXResponse, SSRFrame, OpenAPI 3.1.0, and AsyncAPI 3.0.0 builders."""
    hx_req = HTMXRequest.from_headers({"HX-Request": "true", "HX-Target": "main-panel"})
    assert hx_req.is_htmx is True
    assert hx_req.target == "main-panel"

    hx_resp = HTMXResponse(html="<div>Updated</div>", reswap="outerHTML", trigger={"saved": True})
    headers = hx_resp.to_headers()
    assert headers["HX-Reswap"] == "outerHTML"
    assert headers["HX-Trigger"] == '{"saved":true}'

    ssr_frame = SSRFrame.encode("Dashboard", {"count": 3})
    assert int.from_bytes(ssr_frame[:4], "big") == len(ssr_frame) - 4

    oa = OpenAPISpecBuilder()
    oa.add_operation("/items", "GET", summary="List items", response_schema=ItemSchema, tags=("items",))
    oa_doc = oa.build()
    assert oa_doc["openapi"] == "3.1.0"
    assert "ItemSchema" in oa_doc["components"]["schemas"]

    aa = AsyncAPISpecBuilder()
    aa.add_channel("orders", AsyncChannelDef(address="/ws/orders", protocol="ws"))
    aa_doc = aa.build()
    assert aa_doc["asyncapi"] == "3.0.0"
    assert aa_doc["channels"]["orders"]["address"] == "/ws/orders"


def test_wasm_module_config_and_litestar_mcp_a2a_delegation(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Verify WASMSandboxPolicy bounds, WASMModuleConfig validation, and LitestarPlugin MCP/A2A delegation."""
    with pytest.raises(ValueError, match="max_memory_bytes must be > 0"):
        WASMSandboxPolicy(max_memory_bytes=0)
    with pytest.raises(ValueError, match="max_fuel must be > 0"):
        WASMSandboxPolicy(max_fuel=0)
    with pytest.raises(ValueError, match="epoch_timeout_ms must be > 0"):
        WASMSandboxPolicy(epoch_timeout_ms=0)

    wasm_bytes = WASM_MAGIC_HEADER + b"\x00\x00\x00\x00"
    wasm_file = tmp_path / "validator.wasm"
    wasm_file.write_bytes(wasm_bytes)
    cfg = WASMModuleConfig(name="validator", wasm_source=str(wasm_file), policy=WASMSandboxPolicy(max_fuel=500))
    assert isinstance(cfg.wasm_source, Path)
    assert cfg.load_and_validate() == wasm_bytes

    bad_cfg = WASMModuleConfig(name="broken", wasm_source=b"not-wasm")
    with pytest.raises(ValueError, match="Invalid WebAssembly binary header"):
        bad_cfg.load_and_validate()

    monkeypatch.setattr("vertebra.extensions.litestar.plugin.import_optional", lambda _mod: None)
    plugin = VertebraPlugin(VertebraConfig(mcp_config={"name": "demo"}))
    fake_app_config = SimpleNamespace(
        state={},
        compression_config=None,
        cors_config=None,
        before_send=[],
        route_handlers=[],
        on_app_init=[],
        plugins=[],
    )
    with pytest.raises(MissingDependencyError, match="litestar-mcp") as mcp_exc:
        plugin.on_app_init(fake_app_config)
    assert mcp_exc.value.package == "litestar-mcp"
    assert mcp_exc.value.install_package == "vertebra-core[mcp]"

    fake_mcp_mod = SimpleNamespace(
        LitestarMCP=lambda cfg_obj: ("LitestarMCP", cfg_obj),
        LitestarA2A=lambda cfg_obj: ("LitestarA2A", cfg_obj),
    )
    monkeypatch.setattr("vertebra.extensions.litestar.plugin.import_optional", lambda _mod: fake_mcp_mod)
    plugin_ok = VertebraPlugin(VertebraConfig(mcp_config={"name": "demo"}, a2a_config={"agent": "orders"}))
    plugin_ok.on_app_init(fake_app_config)
    assert fake_app_config.state["litestar_mcp_plugin"] == ("LitestarMCP", {"name": "demo"})
    assert fake_app_config.state["litestar_a2a_plugin"] == ("LitestarA2A", {"agent": "orders"})


def test_guard_requires_and_security_middleware() -> None:
    """Verify compiled Guard.requires, CsrfMiddleware token validation, and RateLimiter token bucket."""
    principal = Principal(
        subject="admin-42",
        tenant_id="tenant-corp",
        roles=frozenset({"admin", "billing"}),
        scopes=frozenset({"finance:write"}),
        capabilities=frozenset({"reconcile"}),
    )
    ctx = SecurityContext(principal=principal)

    guard = Guard.requires(
        roles=("admin", "billing"),
        scopes=("finance:write",),
        capabilities=("reconcile",),
        tenants=("tenant-corp",),
    )
    assert ctx.enforce(guard).subject == "admin-42"

    failing_guard = Guard.requires(roles=("superadmin",))
    with pytest.raises(PermissionDeniedError):
        ctx.enforce(failing_guard)

    csrf = CsrfMiddleware(CsrfConfig(secret="test-secret"))
    token = csrf.generate_token()
    assert csrf.verify_request("GET", {}, {}) is True
    assert csrf.verify_request("POST", {"x-csrf-token": token}, {"vtb_csrf": token}) is True
    assert csrf.verify_request("POST", {"x-csrf-token": "bad"}, {"vtb_csrf": token}) is False
    assert csrf.verify_request("POST", {}, {"vtb_csrf": token}) is False

    limiter = RateLimiter(RateLimitConfig(requests_per_window=2, window_seconds=60))
    assert limiter.allow("client-1") is True
    assert limiter.allow("client-1") is True
    assert limiter.allow("client-1") is False
    assert limiter.allow("client-2") is True


@pytest.mark.parametrize("docs_path", ["/docs", "/swagger"])
def test_openapi_spec_builder_from_app_and_enable_docs(docs_path: str) -> None:
    """Verify OpenAPISpecBuilder.from_app route inspection and App.enable_docs route handlers."""
    app = App()

    @app.get("/items")
    def list_items() -> ItemSchema:
        """List all available items."""
        return ItemSchema(id=1, name="first")

    builder = OpenAPISpecBuilder.from_app(app)
    spec = builder.build()
    assert "/items" in spec["paths"]
    assert "get" in spec["paths"]["/items"]
    assert spec["paths"]["/items"]["get"]["summary"] == "List all available items."
    assert "ItemSchema" in spec["components"]["schemas"]

    app.enable_docs(OpenAPIConfig(title="Store API", schema_path="/custom-openapi.json"))
    route_paths = [r.path for r in app.routes]
    assert "/custom-openapi.json" in route_paths
    assert "/docs" in route_paths

    docs_id = _make_handler_id("GET", docs_path)
    docs_resp = app._handlers[docs_id]()
    assert isinstance(docs_resp, Response)
    assert docs_resp.status == 200
    assert (b"content-type", b"text/html; charset=utf-8") in docs_resp.headers
    assert b"<!DOCTYPE html>" in bytes(docs_resp.body) or b"<!doctype html>" in bytes(docs_resp.body).lower()


def test_asyncapi_spec_builder_from_channels() -> None:
    """Verify AsyncAPISpecBuilder.from_channels constructs valid AsyncAPI 3.0.0 documents."""
    bus = ChannelBus()
    bus._subscribed_channels.add("telemetry")
    bus._subscribed_channels.add("alerts")

    builder = AsyncAPISpecBuilder.from_channels(bus)
    doc = builder.build()
    assert doc["asyncapi"] == "3.0.0"
    assert "telemetry" in doc["channels"]
    assert "alerts" in doc["channels"]
    assert doc["channels"]["telemetry"]["address"] == "/channels/telemetry"


@pytest.mark.anyio
async def test_queue_and_email_backend_validation_and_policies(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify UnsupportedBackendError enforcement, QueueService retries/cron, and EmailService dispatch."""
    with pytest.raises(UnsupportedBackendError, match="Unsupported queue backend URL scheme"):
        QueueService(QueueConfig(backend_url="amqp://localhost:5672"))

    qsvc = QueueService()
    attempts = 0

    @task(retry=TaskRetryPolicy(max_retries=2, initial_backoff_seconds=0.0))
    async def flaky_task() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 2:
            msg = "transient failure"
            raise RuntimeError(msg)
        return "success"

    qsvc.register(flaky_task)
    await qsvc.enqueue("flaky_task")
    assert await qsvc.run_next() == "success"
    assert attempts == 2

    cron_ran = False

    @task(cron=CronSchedule(expression="15 10 * * *"))
    def daily_report() -> str:
        nonlocal cron_ran
        cron_ran = True
        return "report_done"

    qsvc.register(daily_report)
    due_results = await qsvc.run_due_schedules(minute=15, hour=10)
    assert due_results == ["report_done"]
    assert cron_ran is True

    not_due_results = await qsvc.run_due_schedules(minute=16, hour=10)
    assert not_due_results == []

    with pytest.raises(UnsupportedBackendError, match="Unsupported email backend"):
        EmailService(EmailConfig(backend=cast("Any", "postmark")))

    mem_svc = EmailService(EmailConfig(backend="memory"))
    receipt = await mem_svc.send(
        EmailMessage(subject="Test", to=("user@example.com",), from_email="bot@example.com", text_body="Hello")
    )
    assert receipt == "msg-1"
    assert len(mem_svc.outbox) == 1

    sent_messages: list[Any] = []

    class MockSMTP:
        def __init__(self, host: str, port: int, timeout: float = 10.0) -> None:
            self.host = host
            self.port = port
            self.timeout = timeout

        def __enter__(self) -> MockSMTP:
            return self

        def __exit__(self, *args: Any) -> None:
            pass

        def ehlo(self) -> None:
            pass

        def noop(self) -> tuple[int, bytes]:
            return (250, b"ok")

        def quit(self) -> None:
            pass

        def send_message(self, msg: Any, **_kwargs: Any) -> dict[str, Any]:
            sent_messages.append(msg)
            return {}

    monkeypatch.setattr("smtplib.SMTP", MockSMTP)
    smtp_svc = EmailService(EmailConfig(backend="smtp", smtp_host="127.0.0.1", smtp_port=2525, use_starttls=False))
    smtp_receipt = await smtp_svc.send(
        EmailMessage(
            subject="Invoice",
            to=("client@example.com",),
            from_email="billing@example.com",
            text_body="Here is invoice",
            html_body="<p>Invoice</p>",
        )
    )
    assert smtp_receipt.startswith("<") and smtp_receipt.endswith(">")
    assert len(sent_messages) == 1
    assert sent_messages[0]["Subject"] == "Invoice"


@pytest.mark.anyio
async def test_key_value_store_redis_invalidation_read_through_and_ttl_renewal(
    resp3_server: Resp3FakeServer,
) -> None:
    """KeyValueStore attaches AsyncRedisDriver invalidation, populates read-through pttl, and renews driver px TTL."""
    driver = AsyncRedisDriver(RedisConfig(url=resp3_server.redis_url))
    store = KeyValueStore(driver=driver)
    try:
        await store.set("session-1", b"tok", expires_in=10)
        assert "vertebra:session-1" in store._entries
        driver.handle_invalidation_push(["vertebra:session-1"])
        assert "vertebra:session-1" not in store._entries

        await driver.set("vertebra:warm", b"cached-payload", px=3000)
        assert "vertebra:warm" not in store._entries
        loaded = await store.get("warm")
        assert loaded == b"cached-payload"
        assert "vertebra:warm" in store._entries
        cached_val, cached_exp = store._entries["vertebra:warm"]
        assert cached_val == b"cached-payload"
        assert cached_exp is not None

        hit_renewed = await store.get("warm", renew_for=timedelta(seconds=20))
        assert hit_renewed == b"cached-payload"
        assert 5000 < await driver.pttl("vertebra:warm") <= 20000

        await driver.set("vertebra:miss-renew", b"fresh-payload", px=1500)
        miss_renewed = await store.get("miss-renew", renew_for=timedelta(seconds=25))
        assert miss_renewed == b"fresh-payload"
        assert 5000 < await driver.pttl("vertebra:miss-renew") <= 25000
    finally:
        await store.close()
