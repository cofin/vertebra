"""Unit tests for VertebraPlugin, anyio-compatible AsgiRunner, and BuildOrchestrator ecosystem export."""

import asyncio
import contextlib
import os
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import anyio
import click
import pytest
from click.testing import CliRunner

from vertebra.build import BuildOrchestrator
from vertebra.extensions.litestar import (
    ObjectStoreFileSystemAdapter,
    ObjectStoreLitestarStore,
    VertebraConfig,
    VertebraPlugin,
    enter_litestar_server_lifespan,
)
from vertebra.extensions.litestar import plugin as litestar_plugin_mod
from vertebra.extensions.litestar import store as litestar_store_mod
from vertebra.extensions.litestar.channels import ChannelsBackend, swap_channels_backend
from vertebra.server._asgi._runner import AsgiRunner
from vertebra.server._channels import ChannelBus
from vertebra.server._dispatch import _schedule_coros
from vertebra.storage import AsyncObjectStore, LocalConfig, MemoryConfig, StreamingStorageResponse
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import decode_json, encode_msgpack_bytes

litestar_mod = import_optional("litestar")
litestar_cli_mod = import_optional("litestar.cli.main")
litestar_compression_mod = import_optional("litestar.config.compression")
litestar_cors_mod = import_optional("litestar.config.cors")
litestar_response_mod = import_optional("litestar.response")
litestar_static_files_mod = import_optional("litestar.static_files")
litestar_testing_mod = import_optional("litestar.testing")
requires_litestar = pytest.mark.skipif(litestar_mod is None, reason="litestar is not installed")
requires_litestar_cli = pytest.mark.skipif(litestar_cli_mod is None, reason="litestar CLI is not installed")


class _LifecycleTrackerPlugin:
    """Test CLIPlugin recording entry and exit of server_lifespan."""

    __slots__ = ("entered", "exited", "seen_app_env")

    def __init__(self) -> None:
        self.entered = False
        self.exited = False
        self.seen_app_env: str | None = None

    @contextlib.contextmanager
    def server_lifespan(self, app: Any) -> Iterator[None]:
        _ = app
        self.entered = True
        self.seen_app_env = os.environ.get("LITESTAR_APP")
        try:
            yield
        finally:
            self.exited = True


@requires_litestar
def test_litestar_plugin_on_app_init_and_server_lifespan(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify VertebraPlugin preserves standalone Python compression and strips it only when vertebra-server is active."""
    monkeypatch.delenv("LITESTAR_APP", raising=False)
    monkeypatch.delenv("VERTEBRA_SERVER_ACTIVE", raising=False)
    monkeypatch.delenv("VERTEBRA_RUST_OFFLOAD", raising=False)
    tracker = _LifecycleTrackerPlugin()
    vtb_plugin = VertebraPlugin(VertebraConfig(enable_health_endpoint=True, health_path="/_vtb/healthz"))

    @litestar_mod.get("/ping", sync_to_thread=False)
    def ping() -> dict[str, str]:
        return {"status": "pong"}

    standalone_app = litestar_mod.Litestar(
        route_handlers=[ping],
        plugins=[vtb_plugin, tracker],
        compression_config=litestar_compression_mod.CompressionConfig(backend="zstd", minimum_size=512),
        cors_config=litestar_cors_mod.CORSConfig(allow_origins=["https://console.example.com"]),
    )

    assert standalone_app.compression_config is not None
    assert standalone_app.state["_vertebra_offloaded_compression_config"] is not None
    assert standalone_app.state["_vertebra_offloaded_cors_config"] is not None
    assert vtb_plugin.offload_plan is not None
    assert vtb_plugin.offload_plan.compression is not None
    assert vtb_plugin.offload_plan.compression.backend == "zstd"
    vtb_plugin.on_cli_init(None)

    with enter_litestar_server_lifespan(standalone_app, app_path="example.app:app"):
        assert tracker.entered is True
        assert tracker.seen_app_env == "example.app:app"
        assert os.environ["LITESTAR_APP"] == "example.app:app"
    assert tracker.exited is True

    monkeypatch.setenv("VERTEBRA_SERVER_ACTIVE", "1")
    active_plugin = VertebraPlugin(VertebraConfig(enable_health_endpoint=True, health_path="/_vtb/healthz"))
    active_app = litestar_mod.Litestar(
        route_handlers=[ping],
        plugins=[active_plugin],
        compression_config=litestar_compression_mod.CompressionConfig(backend="zstd", minimum_size=512),
        cors_config=litestar_cors_mod.CORSConfig(allow_origins=["https://console.example.com"]),
    )
    assert active_app.compression_config is None
    assert active_plugin.offload_plan is not None
    exported_cfg = decode_json(active_app.state["vertebra_offload_config_json"])
    assert exported_cfg["compression"]["backend"] == "zstd"
    assert exported_cfg["cors"]["allow_origins"] == ["https://console.example.com"]

    disabled_plugin = VertebraPlugin(
        VertebraConfig(
            offload_compression=False,
            offload_cors=False,
            offload_static_files=False,
            offload_openapi=False,
        )
    )
    app_no_offload = litestar_mod.Litestar(
        route_handlers=[ping],
        plugins=[disabled_plugin],
        compression_config=litestar_compression_mod.CompressionConfig(backend="zstd", minimum_size=512),
        cors_config=litestar_cors_mod.CORSConfig(allow_origins=["https://console.example.com"]),
    )
    assert app_no_offload.compression_config is not None
    assert app_no_offload.cors_config is not None
    assert disabled_plugin.offload_plan is not None
    assert disabled_plugin.offload_plan.compression is None
    assert disabled_plugin.offload_plan.cors is None
    assert disabled_plugin.offload_plan.static_mounts == ()
    assert disabled_plugin.offload_plan.openapi is None


@requires_litestar
def test_asgi_runner_invokes_attached_vertebra_fastpath(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify AsgiRunner invokes __vertebra_fastpath__ directly for eligible routes and falls back for unmatched paths."""
    monkeypatch.delenv("VERTEBRA_SERVER_ACTIVE", raising=False)
    vtb_plugin = VertebraPlugin()

    @litestar_mod.get("/accelerated", sync_to_thread=False)
    def accelerated() -> dict[str, str]:
        return {"fast": "yes"}

    litestar_app = litestar_mod.Litestar(route_handlers=[accelerated], plugins=[vtb_plugin])
    bypass = litestar_app.state["vertebra_fastpath_bypass"]

    class _FastPathOnlyAsgiWrapper:
        def __init__(self, inner: Any, fastpath: Any) -> None:
            self._inner = inner
            self.__vertebra_fastpath__ = fastpath
            self.fallback_paths: list[str] = []

        async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
            if scope["path"] == "/accelerated":
                raise AssertionError("outer ASGI router must be bypassed for eligible fastpath routes")
            self.fallback_paths.append(scope["path"])
            await self._inner(scope, receive, send)

    class _SimpleRequest:
        __slots__ = ("body", "handler_id", "headers", "method", "path", "query", "request_id")

        def __init__(self, path: str, request_id: int) -> None:
            self.method = "GET"
            self.path = path
            self.query = None
            self.body = b""
            self.handler_id = 1
            self.request_id = request_id
            self.headers: dict[str, str] = {}

    wrapper = _FastPathOnlyAsgiWrapper(litestar_app, bypass)
    runner = AsgiRunner(wrapper)
    try:
        responses = runner([_SimpleRequest("/accelerated", 1), _SimpleRequest("/missing", 2)])
        assert len(responses) == 2
        assert responses[0].status == 200
        assert decode_json(bytes(responses[0].body)) == {"fast": "yes"}
        assert responses[1].status == 404
        assert wrapper.fallback_paths == ["/missing"]
    finally:
        runner.close()


@requires_litestar
def test_asgi_runner_supports_anyio_task_group() -> None:
    """Verify AsgiRunner executes handlers that spawn anyio.create_task_group() without RuntimeError."""
    results_log: list[str] = []

    @litestar_mod.get("/concurrent")
    async def concurrent_handler() -> dict[str, int]:
        async def child_worker(label: str) -> None:
            await anyio.sleep(0)
            results_log.append(label)

        async with anyio.create_task_group() as tg:
            tg.start_soon(child_worker, "w1")
            tg.start_soon(child_worker, "w2")
        return {"count": len(results_log)}

    app = litestar_mod.Litestar(route_handlers=[concurrent_handler])

    class _SimpleRequest:
        __slots__ = ("body", "handler_id", "headers", "method", "path", "query", "request_id")

        def __init__(self) -> None:
            self.method = "GET"
            self.path = "/concurrent"
            self.query = None
            self.body = b""
            self.handler_id = 1
            self.request_id = 1
            self.headers: dict[str, str] = {}

    runner = AsgiRunner(app)
    try:
        responses = runner([_SimpleRequest()])
        assert len(responses) == 1
        assert responses[0].status == 200
        assert sorted(results_log) == ["w1", "w2"]
    finally:
        runner.close()


@pytest.mark.anyio
async def test_drive_coro_promotes_yielded_coroutine_to_asyncio_task() -> None:
    """Verify _drive_coro promotes a coroutine that yields into a real asyncio.Task for anyio compatibility."""
    observed: dict[str, Any] = {"has_task": False}
    finished = asyncio.Event()

    async def yielded_then_anyio() -> None:
        try:
            await asyncio.sleep(0)
            async with anyio.create_task_group() as tg:
                observed["has_task"] = asyncio.current_task() is not None
                _ = tg
        finally:
            finished.set()

    _schedule_coros(asyncio.get_running_loop(), [yielded_then_anyio()])
    await asyncio.wait_for(finished.wait(), 1)

    assert observed["has_task"] is True


@requires_litestar
def test_build_orchestrator_exports_litestar_vite_and_asyncapi(tmp_path: Path) -> None:
    """Verify BuildOrchestrator exports litestar-vite and litestar-asyncapi schemas when present on the app."""

    class _StubVitePlugin:
        __slots__ = ()

        def export_vite_metadata(self) -> dict[str, Any]:
            return {"ssr": False, "base": "/static/", "entry": "src/main.ts"}

    class _StubAsyncAPIPlugin:
        __slots__ = ()

        def export_asyncapi_schema(self) -> dict[str, Any]:
            return {"asyncapi": "3.0.0", "info": {"title": "Events", "version": "1.0.0"}}

    @litestar_mod.get("/status", sync_to_thread=False)
    def status_handler() -> dict[str, str]:
        return {"ok": "true"}

    app = litestar_mod.Litestar(route_handlers=[status_handler])
    app.state["vite_plugin"] = _StubVitePlugin()
    app.state["asyncapi_plugin"] = _StubAsyncAPIPlugin()

    orchestrator = BuildOrchestrator(tmp_path)
    exported = orchestrator.export_litestar_artifacts(app, output_dir=tmp_path)

    assert "vite" in exported
    assert "asyncapi" in exported
    assert decode_json(exported["vite"].read_text(encoding="utf-8"))["entry"] == "src/main.ts"
    assert decode_json(exported["asyncapi"].read_text(encoding="utf-8"))["asyncapi"] == "3.0.0"


@requires_litestar
def test_litestar_plugin_ensure_finalized_populates_openapi_and_binds_asgi_runner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify AsgiRunner finalizes VertebraPlugin lazily so __vertebra_fastpath__ and OpenAPI offload bind cleanly."""
    monkeypatch.setenv("VERTEBRA_SERVER_ACTIVE", "1")
    plugin = VertebraPlugin()

    @litestar_mod.get("/ready-fast", sync_to_thread=False)
    def ready_fast() -> dict[str, str]:
        return {"mode": "direct"}

    app = litestar_mod.Litestar(
        route_handlers=[ready_fast],
        plugins=[plugin],
        compression_config=litestar_compression_mod.CompressionConfig(backend="br", minimum_size=256),
    )

    runner = AsgiRunner(app)
    try:
        plugin.ensure_finalized(app)
        assert getattr(app, "__vertebra_fastpath__", None) is not None
        exported = decode_json(app.state["vertebra_offload_config_json"])
        assert exported["compression"]["backend"] == "brotli"
        assert exported["openapi"]["schema_path"] == "/schema/openapi.json"
        assert '"/ready-fast"' in exported["openapi"]["body"]
        assert runner.dispatch_app is not app
    finally:
        runner.close()


@pytest.fixture
def litestar_cli_app(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[dict[str, Any]]:
    """Write a Litestar app module, stub the supervisor, and restore the global `run` command afterwards."""
    captured: dict[str, Any] = {}

    def fake_build_cmd() -> list[str]:
        return ["/bin/true"]

    def fake_supervisor(*, server_cmd: list[str], env: dict[str, str], **_kwargs: Any) -> int:
        captured["cmd"] = server_cmd
        captured["env"] = dict(env)
        return 0

    monkeypatch.setattr(litestar_plugin_mod, "build_server_command", fake_build_cmd)
    monkeypatch.setattr(litestar_plugin_mod, "run_supervisor", fake_supervisor)
    (tmp_path / "ls_cli_app.py").write_text(
        "from litestar import Litestar, get\n"
        "from litestar.config.compression import CompressionConfig\n"
        "from vertebra.extensions.litestar import VertebraConfig, VertebraPlugin\n\n"
        "@get('/hello', sync_to_thread=False)\n"
        "def hello() -> dict[str, str]:\n"
        "    return {'hello': 'world'}\n\n"
        "app = Litestar(\n"
        "    route_handlers=[hello],\n"
        "    plugins=[VertebraPlugin(VertebraConfig())],\n"
        "    compression_config=CompressionConfig(backend='zstd', minimum_size=256),\n"
        ")\n",
        encoding="utf-8",
    )
    litestar_group = litestar_cli_mod.litestar_group
    original_run = litestar_group.commands.get("run")
    try:
        yield captured
    finally:
        if original_run is not None:
            litestar_group.commands["run"] = original_run


@requires_litestar_cli
def test_vertebra_plugin_overrides_litestar_cli_run_command(tmp_path: Path, litestar_cli_app: dict[str, Any]) -> None:
    """Verify loading the app through `litestar` binds the Vertebra `run` command, which launches the supervisor."""
    runner = CliRunner()
    help_result = runner.invoke(
        litestar_cli_mod.litestar_group,
        ["--app-dir", str(tmp_path), "--app", "ls_cli_app:app", "run", "--help"],
    )
    assert help_result.exit_code == 0, help_result.output
    assert "--topology" in help_result.output
    assert "--drain-timeout" in help_result.output
    assert litestar_cli_app == {}

    run_result = runner.invoke(
        litestar_cli_mod.litestar_group,
        [
            "--app-dir",
            str(tmp_path),
            "--app",
            "ls_cli_app:app",
            "run",
            "--port",
            "8123",
            "--workers",
            "2",
            "--topology",
            "shm",
            "--no-banner",
        ],
    )
    assert run_result.exit_code == 0, run_result.output
    env = litestar_cli_app["env"]
    assert litestar_cli_app["cmd"] == ["/bin/true"]
    assert env["VERTEBRA_APP"] == "ls_cli_app:app"
    assert env["VERTEBRA_PORT"] == "8123"
    assert env["VERTEBRA_WORKERS"] == "2"
    assert env["VERTEBRA_TOPOLOGY"] == "shm"
    assert decode_json(env["VERTEBRA_OFFLOAD_CONFIG"])["compression"]["backend"] == "zstd"


def test_channels_backend_accepts_engine_neutral_bus() -> None:
    """Litestar can swap its memory backend for the shared bus directly."""

    class MemoryChannelsBackend:
        history = 3

    class ChannelsPlugin:
        def __init__(self) -> None:
            self._backend: Any = MemoryChannelsBackend()

    plugin = ChannelsPlugin()
    app = SimpleNamespace(plugins=[plugin])
    assert issubclass(ChannelsBackend, ChannelBus)
    replacement = swap_channels_backend(app)
    assert isinstance(replacement, ChannelsBackend)
    bus = ChannelBus(history=2)
    assert swap_channels_backend(app, bus) is bus
    assert swap_channels_backend(app) is bus
    assert plugin._backend is bus


@pytest.mark.anyio
async def test_channels_stream_disconnect_releases_queue() -> None:
    """Cancelled Litestar consumers release queues through the protected cleanup hook."""

    class TrackedBackend(ChannelsBackend):
        def __init__(self) -> None:
            super().__init__()
            self.active: set[asyncio.Queue[tuple[str, bytes] | None]] = set()

        def _register_stream_queue(self) -> asyncio.Queue[tuple[str, bytes] | None]:
            queue = super()._register_stream_queue()
            self.active.add(queue)
            return queue

        def _unregister_stream_queue(self, queue: asyncio.Queue[tuple[str, bytes] | None]) -> None:
            super()._unregister_stream_queue(queue)
            self.active.discard(queue)

    backend = TrackedBackend()
    await backend.on_startup()
    await backend.subscribe(["orders"])
    stream = backend.stream_events()
    pending = asyncio.create_task(anext(stream))
    await asyncio.sleep(0)
    assert len(backend.active) == 1
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    assert not backend.active
    with anyio.CancelScope() as scope:
        scope.cancel()
        await backend.unsubscribe(["orders"])
        await backend.on_shutdown()
    assert backend.channels == ()


@pytest.fixture
def litestar_tls_cli(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[click.Group, dict[str, str]]:
    """Build a bare CLI group with the Vertebra `run` command and a capturing supervisor stub."""
    captured_env: dict[str, str] = {}

    def fake_build_cmd() -> list[str]:
        return ["/bin/true"]

    def fake_supervisor(*, server_cmd: list[str], env: dict[str, str], **_kwargs: Any) -> int:
        _ = server_cmd
        captured_env.update(env)
        return 0

    monkeypatch.setattr(litestar_plugin_mod, "build_server_command", fake_build_cmd)
    monkeypatch.setattr(litestar_plugin_mod, "run_supervisor", fake_supervisor)
    (tmp_path / "present.crt").write_text("CERT", encoding="utf-8")
    group = click.Group("litestar")
    VertebraPlugin().on_cli_init(group)
    return group, captured_env


@pytest.mark.parametrize(
    ("tls_args", "expected_error"),
    [
        (
            ["--ssl-certfile", "present.crt"],
            "Both --ssl-certfile and --ssl-keyfile must be provided together",
        ),
        (
            ["--ssl-keyfile", "missing.key"],
            "Both --ssl-certfile and --ssl-keyfile must be provided together",
        ),
        (
            ["--ssl-certfile", "missing.crt", "--ssl-keyfile", "missing.key"],
            "SSL certificate file does not exist",
        ),
        (
            ["--ssl-certfile", "present.crt", "--ssl-keyfile", "missing.key"],
            "SSL key file does not exist",
        ),
    ],
    ids=["only-certfile", "only-keyfile", "missing-certfile", "missing-keyfile"],
)
def test_litestar_cli_run_rejects_invalid_tls_cert_and_key(
    tmp_path: Path,
    litestar_tls_cli: tuple[click.Group, dict[str, str]],
    tls_args: list[str],
    expected_error: str,
) -> None:
    """Litestar run enforces mutual --ssl-certfile/--ssl-keyfile flags and file existence before launching."""
    group, captured_env = litestar_tls_cli
    resolved_args = [str(tmp_path / arg) if not arg.startswith("--") else arg for arg in tls_args]
    result = CliRunner().invoke(group, ["run", "--no-banner", *resolved_args])
    assert result.exit_code == 2, result.output
    assert expected_error in result.output
    assert captured_env == {}


def test_litestar_cli_run_self_signed_accepts_missing_tls_files(
    tmp_path: Path, litestar_tls_cli: tuple[click.Group, dict[str, str]]
) -> None:
    """Litestar run with --create-self-signed-cert accepts missing cert/key paths and exports them resolved."""
    group, captured_env = litestar_tls_cli
    missing_cert = tmp_path / "missing.crt"
    missing_key = tmp_path / "missing.key"
    result = CliRunner().invoke(
        group,
        [
            "run",
            "--no-banner",
            "--ssl-certfile",
            str(missing_cert),
            "--ssl-keyfile",
            str(missing_key),
            "--create-self-signed-cert",
        ],
    )
    assert result.exit_code == 0, result.output
    assert captured_env["VERTEBRA_TLS_CERT"] == str(missing_cert.resolve())
    assert captured_env["VERTEBRA_TLS_KEY"] == str(missing_key.resolve())
    assert captured_env["VERTEBRA_SELF_SIGNED_CERT"] == "1"


@pytest.mark.parametrize("config", [None, ""])
@pytest.mark.parametrize("adapter_name", ["ObjectStoreLitestarStore", "ObjectStoreFileSystemAdapter"])
def test_object_store_requires_explicit_config(config: Any, adapter_name: str) -> None:
    """Both public adapters fail closed without an explicit storage configuration."""
    adapter = {
        "ObjectStoreLitestarStore": ObjectStoreLitestarStore,
        "ObjectStoreFileSystemAdapter": ObjectStoreFileSystemAdapter,
    }[adapter_name]
    with pytest.raises(ImproperConfigurationError):
        adapter(config)


@pytest.mark.parametrize("namespace", ["", "..", "a/b", "a\\b", "a.b", "équipe"])
def test_object_store_rejects_invalid_namespace(namespace: str) -> None:
    """Each invalid namespace is rejected independently using a real native store."""
    root = ObjectStoreLitestarStore(AsyncObjectStore(MemoryConfig()))
    with pytest.raises(ValueError):
        root.with_namespace(namespace)


@pytest.mark.anyio
async def test_object_store_crud_and_namespace_deletion() -> None:
    """Logical keys and descendant namespaces stay confined through multiple delete pages."""
    storage = AsyncObjectStore(MemoryConfig())
    root = ObjectStoreLitestarStore(storage)
    child = root.with_namespace("s1")
    sibling = root.with_namespace("s10")
    descendant = child.with_namespace("child")
    await child.set("text", "héllo")
    assert await child.get("text") == "héllo".encode()
    await child.set("text", b"replacement")
    assert await child.get("text") == b"replacement"
    await child.delete("text")
    await child.delete("text")
    assert await child.get("text") is None
    assert not await child.exists("text")
    assert await child.expires_in("text") is None
    for index in range(1005):
        await child.set(str(index), b"paged")
    await descendant.set("descendant", b"child")
    await sibling.set("preserved", b"sibling")
    await root.set("s1/child", b"root-key")
    await storage.put("unrelated", b"outside")
    await child.delete_all()
    assert await descendant.get("descendant") is None
    assert await child.get("0") is None
    assert await child.get("1004") is None
    assert await sibling.get("preserved") == b"sibling"
    assert await root.get("s1/child") == b"root-key"
    await root.delete_all()
    assert await storage.list() == [await storage.head("unrelated")]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("key", "value", "expected"),
    [("", b"", b""), ("../child/ü", b"\xff\x00", b"\xff\x00"), ("text", "héllo", "héllo".encode())],
    ids=["empty-key", "traversal-unicode-key", "text-value"],
)
async def test_object_store_round_trips_logical_keys(key: str, value: bytes | str, expected: bytes) -> None:
    """Each logical key round-trips byte-exact inside its namespace and is removed by namespace deletion."""
    root = ObjectStoreLitestarStore(AsyncObjectStore(MemoryConfig()))
    child = root.with_namespace("s1")
    await child.set(key, value)
    assert await child.get(key) == expected
    assert await child.exists(key)
    await child.delete_all()
    assert await child.get(key) is None


@pytest.mark.anyio
async def test_object_store_local_ttl_survives_reconstruction(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Wall-clock expiry and renewals persist even when Local discards object attributes."""
    clock = [1000.0]
    monkeypatch.setattr(litestar_store_mod.time, "time", lambda: clock[0])
    config = LocalConfig(root=tmp_path)
    cache = ObjectStoreLitestarStore(config)
    await cache.set("ttl", b"value", expires_in=timedelta(milliseconds=1500))
    cache = ObjectStoreLitestarStore(AsyncObjectStore(config))
    assert await cache.expires_in("ttl") == 1
    clock[0] = 1000.6
    assert await cache.expires_in("ttl") == 0
    assert await cache.get("ttl", renew_for=2) == b"value"
    cache = ObjectStoreLitestarStore(config)
    clock[0] = 1001.5
    assert await cache.get("ttl") == b"value"
    clock[0] = 1002.6
    assert await cache.get("ttl") is None
    assert not await cache.exists("ttl")
    assert await cache.expires_in("ttl") is None
    await cache.set("immortal", b"forever")
    assert await cache.get("immortal", renew_for=1) == b"forever"
    assert await cache.expires_in("immortal") is None
    await cache.set("overwrite", b"old", expires_in=1)
    await cache.set("overwrite", b"new")
    clock[0] += 100
    assert await ObjectStoreLitestarStore(config).get("overwrite") == b"new"
    assert await cache.expires_in("overwrite") is None
    await cache.set("fraction", b"rounded", expires_in=timedelta(microseconds=1))
    assert await cache.get("fraction") == b"rounded"
    clock[0] += 0.001
    assert await cache.get("fraction") is None


@pytest.mark.anyio
async def test_object_store_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    """Persistent metadata and the authoritative envelope are inspectable."""
    monkeypatch.setattr(litestar_store_mod.time, "time", lambda: 1000.0)
    storage = AsyncObjectStore(MemoryConfig())
    cache = ObjectStoreLitestarStore(storage)
    await cache.set("ttl", b"payload", expires_in=2)
    object_key = "vertebra-store/~74746c"
    metadata = await storage.head(object_key)
    assert metadata.attributes["vertebra-expires-at-ms"] == "1002000"
    assert await storage.get(object_key) == b"VTBSTORE\x01" + encode_msgpack_bytes((1002000, b"payload"))


@pytest.mark.anyio
@pytest.mark.parametrize("ttl", [0, -1, timedelta(microseconds=-1)])
async def test_object_store_immediate_expiry(tmp_path: Path, ttl: int | timedelta) -> None:
    """Each nonpositive duration immediately expires on the real Local backend."""
    cache = ObjectStoreLitestarStore(LocalConfig(root=tmp_path))
    await cache.set("immediate", b"expired", expires_in=ttl)
    assert await cache.get("immediate") is None


@pytest.mark.anyio
@pytest.mark.parametrize(
    "corrupt",
    [b"raw", b"VTBSTORE\x02", b"VTBSTORE\x01\xc1", b"VTBSTORE\x01" + encode_msgpack_bytes(("wrong", 1))],
    ids=["wrong-magic", "unsupported-version", "malformed-msgpack", "invalid-envelope-types"],
)
async def test_object_store_invalid_envelopes(corrupt: bytes) -> None:
    """Each malformed native payload fails explicitly at the public Store seam."""
    storage = AsyncObjectStore(MemoryConfig())
    cache = ObjectStoreLitestarStore(storage)
    await storage.put("vertebra-store/~74746c", corrupt)
    with pytest.raises(ValueError):
        await cache.get("ttl")


@pytest.mark.anyio
@pytest.mark.parametrize("path", ["/../secret", "/nested/../../secret", "/../publicity/secret"])
@pytest.mark.parametrize("operation", ["info", "open"])
async def test_static_object_store_rejects_traversal(path: str, operation: str) -> None:
    """Each path and operation independently rejects escape from the virtual root."""
    storage = AsyncObjectStore(MemoryConfig())
    await storage.put("public/app.js", b"public")
    await storage.put("publicity/secret", b"secret")
    adapter = ObjectStoreFileSystemAdapter(storage, base_path="public")
    with pytest.raises(FileNotFoundError):
        await getattr(adapter, operation)(path)


@requires_litestar
@pytest.mark.anyio
async def test_static_object_store_reader_and_routes() -> None:
    """Litestar consumes the native async reader for GET/HEAD with a confined virtual root."""
    storage = AsyncObjectStore(MemoryConfig())
    payload = b"console.log('storage');"
    await storage.put("public/app.js", payload)
    await storage.put("public/équipe/item", b"nested")
    await storage.put("publicity/secret", b"secret")
    adapter = ObjectStoreFileSystemAdapter(storage, base_path="public")
    assert (await adapter.info("/"))["type"] == "directory"
    assert (await adapter.info("/équipe"))["type"] == "directory"
    assert isinstance((await adapter.info("/app.js"))["mtime"], float)
    assert (await adapter.info("/./app.js"))["size"] == len(payload)
    reader = await adapter.open(Path("/app.js"))
    async with reader:
        assert await reader.read(7) == payload[:7]
        assert reader.tell() == 7
        reader.seek(0)
        assert await reader.read() == payload
    assert reader.closed
    with pytest.raises(ValueError):
        await adapter.open("app.js", "w")
    with pytest.raises(ValueError):
        ObjectStoreFileSystemAdapter(storage, base_path="../public")
    with pytest.raises(FileNotFoundError):
        await adapter.info("missing")
    routes = litestar_static_files_mod.create_static_files_router(
        path="/assets", directories=["/"], file_system=adapter, resolve_symlinks=False
    )
    async with litestar_testing_mod.AsyncTestClient(litestar_mod.Litestar(route_handlers=[routes])) as client:
        response = await client.get("/assets/app.js")
        assert response.status_code == 200
        assert response.content == payload
        assert response.headers["content-length"] == str(len(payload))
        response = await client.head("/assets/app.js")
        assert response.status_code == 200
        assert response.content == b""
        assert response.headers["content-length"] == str(len(payload))
        assert (await client.get("/assets/missing")).status_code == 404


@requires_litestar
@pytest.mark.anyio
async def test_static_object_store_explicit_range_handler() -> None:
    """A supported Litestar Stream bridges storage full, partial, and unsatisfiable responses."""
    storage = AsyncObjectStore(MemoryConfig())
    payload = b"0123456789abcdefghij"
    await storage.put("app.js", payload)

    @litestar_mod.get("/download")
    async def download(request: litestar_mod.Request[Any, Any, Any]) -> litestar_response_mod.Stream:
        response = await StreamingStorageResponse.from_async_store(
            storage, "app.js", range_header=request.headers.get("range")
        )

        async def chunks() -> Any:
            async for chunk in response.aiter_chunks():
                yield bytes(chunk)

        headers = dict(response.headers)
        if response.content_range is not None:
            headers["content-range"] = response.content_range
        return litestar_response_mod.Stream(
            chunks(),
            status_code=response.status_code,
            headers=headers,
            media_type=response.content_type or response.media_type,
        )

    async with litestar_testing_mod.AsyncTestClient(litestar_mod.Litestar(route_handlers=[download])) as client:
        response = await client.get("/download")
        assert response.status_code == 200
        assert response.content == payload
        response = await client.get("/download", headers={"range": "bytes=0-7"})
        assert response.status_code == 206
        assert response.content == payload[:8]
        assert response.headers["content-range"] == f"bytes 0-7/{len(payload)}"
        assert response.headers["content-length"] == "8"
        response = await client.get("/download", headers={"range": "bytes=9999-"})
        assert response.status_code == 416
        assert response.content == b""
        assert response.headers["content-range"] == f"bytes */{len(payload)}"


@pytest.mark.anyio
@pytest.mark.parametrize("backend_kind", ["memory", "local"])
async def test_litestar_store_and_fs_adapter_preserve_percent_and_tilde_keys(
    tmp_path: Path,
    backend_kind: str,
) -> None:
    """ObjectStoreLitestarStore and ObjectStoreFileSystemAdapter preserve literal %25 and ~ keys without unquoting."""
    cfg = MemoryConfig() if backend_kind == "memory" else LocalConfig(root=tmp_path / "store")
    storage = AsyncObjectStore(cfg)
    kv_store = ObjectStoreLitestarStore(storage, namespace="cache")

    await kv_store.set("user~1%25a", b"ok-1")
    await kv_store.set("rate%25limit~bucket", b"ok-2")
    assert await kv_store.get("user~1%25a") == b"ok-1"
    assert await kv_store.exists("rate%25limit~bucket") is True
    await kv_store.delete_all()
    assert await kv_store.get("user~1%25a") is None
    assert await kv_store.exists("rate%25limit~bucket") is False

    await storage.put("public/dir%25v1~beta/rate%25limit~doc.txt", b"raw-bytes")
    adapter = ObjectStoreFileSystemAdapter(storage, base_path="public")
    dir_info = await adapter.info("dir%25v1~beta")
    assert dir_info["type"] == "directory"
    file_info = await adapter.info("dir%25v1~beta/rate%25limit~doc.txt")
    assert file_info["type"] == "file"
    assert file_info["size"] == 9
    async with await adapter.open("dir%25v1~beta/rate%25limit~doc.txt") as reader:
        assert await reader.read() == b"raw-bytes"
