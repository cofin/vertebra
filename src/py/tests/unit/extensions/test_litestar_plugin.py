"""Unit tests for VertebraPlugin, anyio-compatible AsgiRunner, and BuildOrchestrator ecosystem export."""

import asyncio
import contextlib
import inspect
import os
from collections.abc import Callable, Iterator, Sequence
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import anyio
import click
import msgspec
import pytest
from click.testing import CliRunner

from vertebra.build import BuildOrchestrator
from vertebra.extensions.litestar import (
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
from vertebra.utils.serializers import decode_json, encode_json_bytes, encode_msgpack_bytes


def _load_litestar_plugin_test_primitives() -> tuple[
    Any, Callable[..., Callable[[Callable[..., Any]], Any]], Any, Any, Any
]:
    """Load Litestar plugin test classes or fallback doubles when optional extra is absent."""
    litestar_mod = import_optional("litestar")
    comp_mod = import_optional("litestar.config.compression")
    cors_mod = import_optional("litestar.config.cors")
    plugins_mod = import_optional("litestar.plugins")
    if litestar_mod is not None and comp_mod is not None and cors_mod is not None and plugins_mod is not None:
        return (
            litestar_mod.Litestar,
            litestar_mod.get,
            comp_mod.CompressionConfig,
            cors_mod.CORSConfig,
            plugins_mod.CLIPlugin,
        )

    class StateStub:
        """Non-dict Litestar State double mirroring litestar.datastructures.State."""

        __slots__ = ("_data",)

        def __init__(self, initial: dict[str, Any] | None = None) -> None:
            self._data: dict[str, Any] = dict(initial or {})

        def __getitem__(self, key: str) -> Any:
            return self._data[key]

        def __setitem__(self, key: str, value: Any) -> None:
            self._data[key] = value

        def __contains__(self, key: object) -> bool:
            return key in self._data

        def __delitem__(self, key: str) -> None:
            del self._data[key]

        def __iter__(self) -> Iterator[str]:
            return iter(self._data)

        def __len__(self) -> int:
            return len(self._data)

        def get(self, key: str, default: Any = None) -> Any:
            return self._data.get(key, default)

        def keys(self) -> Any:
            return self._data.keys()

        def values(self) -> Any:
            return self._data.values()

        def items(self) -> Any:
            return self._data.items()

    class CompressionConfigStub(msgspec.Struct, frozen=True, gc=False):
        backend: str = "gzip"
        minimum_size: int = 500
        gzip_compress_level: int = 6
        brotli_quality: int = 4
        zstd_level: int = 3
        exclude: Sequence[str] | str | None = None
        exclude_opt_key: str | None = None

    class CORSConfigStub(msgspec.Struct, frozen=True, gc=False):
        allow_origins: Sequence[str] = ("*",)
        allow_methods: Sequence[str] = ("*",)
        allow_headers: Sequence[str] = ("*",)
        allow_credentials: bool = False
        expose_headers: Sequence[str] = ()
        max_age: int = 600
        allow_origin_regex: str | None = None

    class CLIPluginStub:
        """Fallback CLIPlugin base class."""

        __slots__ = ()

    def get_stub(path: str, **_kwargs: Any) -> Callable[[Callable[..., Any]], Any]:
        def wrap(fn: Callable[..., Any]) -> Any:
            return SimpleNamespace(
                paths={path},
                http_methods={"GET"},
                fn=fn,
                guards=(),
                middleware=(),
                opt={},
            )

        return wrap

    class LitestarStub:
        __slots__ = (
            "__vertebra_fastpath__",
            "before_send",
            "compression_config",
            "cors_config",
            "lifespan",
            "openapi_config",
            "openapi_schema",
            "plugins",
            "routes",
            "state",
        )

        def __init__(
            self,
            route_handlers: Sequence[Any] | None = None,
            *,
            plugins: Sequence[Any] | None = None,
            compression_config: Any = None,
            cors_config: Any = None,
            **_kwargs: Any,
        ) -> None:
            app_config = SimpleNamespace(
                route_handlers=list(route_handlers or ()),
                plugins=list(plugins or ()),
                compression_config=compression_config,
                cors_config=cors_config,
                before_send=None,
                state=StateStub(),
                lifespan=[],
            )
            for plugin in app_config.plugins:
                on_init = getattr(plugin, "on_app_init", None)
                if callable(on_init):
                    app_config = on_init(app_config)

            self.__vertebra_fastpath__: Any = None
            self.compression_config = app_config.compression_config
            self.cors_config = app_config.cors_config
            self.before_send = app_config.before_send
            self.state = StateStub(app_config.state._data)
            self.lifespan = list(app_config.lifespan)
            self.plugins = SimpleNamespace(
                plugins=list(app_config.plugins),
                cli=[p for p in app_config.plugins if hasattr(p, "server_lifespan")],
            )
            by_path: dict[str, dict[str, Any]] = {}
            for handler in app_config.route_handlers:
                for path in getattr(handler, "paths", ("/",)):
                    method_map = by_path.setdefault(path, {})
                    for method in getattr(handler, "http_methods", ("GET",)):
                        method_map[method] = handler
            self.routes = [
                SimpleNamespace(path=path, route_handler_map=method_map, path_parameters=())
                for path, method_map in by_path.items()
            ]
            self.openapi_config = SimpleNamespace(path="/schema")
            self.openapi_schema = SimpleNamespace(
                to_schema=lambda: {
                    "openapi": "3.1.0",
                    "paths": {r.path: {} for r in self.routes},
                }
            )

        async def __call__(self, scope: Any, _receive: Any, send: Any) -> None:
            path = scope["path"]
            method = scope["method"].upper()
            for route in self.routes:
                if route.path == path and method in route.route_handler_map:
                    handler = route.route_handler_map[method]
                    out = handler.fn()
                    if inspect.isawaitable(out):
                        out = await out
                    body = bytes(encode_json_bytes(out))
                    await send(
                        {
                            "type": "http.response.start",
                            "status": 200,
                            "headers": [(b"content-type", b"application/json")],
                        }
                    )
                    await send({"type": "http.response.body", "body": body, "more_body": False})
                    return
            await send({"type": "http.response.start", "status": 404, "headers": []})
            await send({"type": "http.response.body", "body": b"Not Found", "more_body": False})

    return (
        LitestarStub,
        get_stub,
        CompressionConfigStub,
        CORSConfigStub,
        CLIPluginStub,
    )


Litestar, get, CompressionConfig, CORSConfig, CLIPlugin = _load_litestar_plugin_test_primitives()


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


def test_litestar_plugin_on_app_init_and_server_lifespan(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify VertebraPlugin preserves standalone Python compression and strips it only when vertebra-server is active."""
    monkeypatch.delenv("LITESTAR_APP", raising=False)
    monkeypatch.delenv("VERTEBRA_SERVER_ACTIVE", raising=False)
    monkeypatch.delenv("VERTEBRA_RUST_OFFLOAD", raising=False)
    tracker = _LifecycleTrackerPlugin()
    vtb_plugin = VertebraPlugin(VertebraConfig(enable_health_endpoint=True, health_path="/_vtb/healthz"))

    @get("/ping", sync_to_thread=False)
    def ping() -> dict[str, str]:
        return {"status": "pong"}

    standalone_app = Litestar(
        route_handlers=[ping],
        plugins=[vtb_plugin, tracker],
        compression_config=CompressionConfig(backend="zstd", minimum_size=512),
        cors_config=CORSConfig(allow_origins=["https://console.example.com"]),
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
    active_app = Litestar(
        route_handlers=[ping],
        plugins=[active_plugin],
        compression_config=CompressionConfig(backend="zstd", minimum_size=512),
        cors_config=CORSConfig(allow_origins=["https://console.example.com"]),
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
    app_no_offload = Litestar(
        route_handlers=[ping],
        plugins=[disabled_plugin],
        compression_config=CompressionConfig(backend="zstd", minimum_size=512),
        cors_config=CORSConfig(allow_origins=["https://console.example.com"]),
    )
    assert app_no_offload.compression_config is not None
    assert app_no_offload.cors_config is not None
    assert disabled_plugin.offload_plan is not None
    assert disabled_plugin.offload_plan.compression is None
    assert disabled_plugin.offload_plan.cors is None
    assert disabled_plugin.offload_plan.static_mounts == ()
    assert disabled_plugin.offload_plan.openapi is None


def test_asgi_runner_invokes_attached_vertebra_fastpath(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify AsgiRunner invokes __vertebra_fastpath__ directly for eligible routes and falls back for unmatched paths."""
    monkeypatch.delenv("VERTEBRA_SERVER_ACTIVE", raising=False)
    vtb_plugin = VertebraPlugin()

    @get("/accelerated", sync_to_thread=False)
    def accelerated() -> dict[str, str]:
        return {"fast": "yes"}

    litestar_app = Litestar(route_handlers=[accelerated], plugins=[vtb_plugin])
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


def test_asgi_runner_supports_anyio_task_group() -> None:
    """Verify AsgiRunner executes handlers that spawn anyio.create_task_group() without RuntimeError."""
    results_log: list[str] = []

    @get("/concurrent")
    async def concurrent_handler() -> dict[str, int]:
        async def child_worker(label: str) -> None:
            await anyio.sleep(0)
            results_log.append(label)

        async with anyio.create_task_group() as tg:
            tg.start_soon(child_worker, "w1")
            tg.start_soon(child_worker, "w2")
        return {"count": len(results_log)}

    app = Litestar(route_handlers=[concurrent_handler])

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


def test_drive_coro_promotes_yielded_coroutine_to_asyncio_task() -> None:
    """Verify _drive_coro promotes a coroutine that yields into a real asyncio.Task for anyio compatibility."""
    observed: dict[str, Any] = {"has_task": False}

    async def yielded_then_anyio() -> None:
        await asyncio.sleep(0)
        async with anyio.create_task_group() as tg:
            observed["has_task"] = asyncio.current_task() is not None
            _ = tg

    async def _run() -> None:
        _schedule_coros(asyncio.get_running_loop(), [yielded_then_anyio()])
        await asyncio.sleep(0.02)

    asyncio.run(_run())

    assert observed["has_task"] is True


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

    @get("/status", sync_to_thread=False)
    def status_handler() -> dict[str, str]:
        return {"ok": "true"}

    app = Litestar(route_handlers=[status_handler])
    app.state["vite_plugin"] = _StubVitePlugin()
    app.state["asyncapi_plugin"] = _StubAsyncAPIPlugin()

    orchestrator = BuildOrchestrator(tmp_path)
    exported = orchestrator.export_litestar_artifacts(app, output_dir=tmp_path)

    assert "vite" in exported
    assert "asyncapi" in exported
    assert decode_json(exported["vite"].read_text(encoding="utf-8"))["entry"] == "src/main.ts"
    assert decode_json(exported["asyncapi"].read_text(encoding="utf-8"))["asyncapi"] == "3.0.0"


def test_litestar_plugin_ensure_finalized_populates_openapi_and_binds_asgi_runner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify AsgiRunner finalizes VertebraPlugin lazily so __vertebra_fastpath__ and OpenAPI offload bind cleanly."""
    monkeypatch.setenv("VERTEBRA_SERVER_ACTIVE", "1")
    plugin = VertebraPlugin()

    @get("/ready-fast", sync_to_thread=False)
    def ready_fast() -> dict[str, str]:
        return {"mode": "direct"}

    app = Litestar(
        route_handlers=[ready_fast],
        plugins=[plugin],
        compression_config=CompressionConfig(backend="br", minimum_size=256),
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


def test_vertebra_plugin_overrides_litestar_cli_run_command(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify VertebraPlugin.on_cli_init overrides `litestar run` to launch the Vertebra supervisor."""
    captured: dict[str, Any] = {}

    def fake_build_cmd() -> list[str]:
        return ["/bin/true"]

    def fake_supervisor(*, server_cmd: list[str], env: dict[str, str], **_kwargs: Any) -> int:
        captured["cmd"] = server_cmd
        captured["env"] = dict(env)
        return 0

    monkeypatch.setattr(litestar_plugin_mod, "build_server_command", fake_build_cmd)
    monkeypatch.setattr(litestar_plugin_mod, "run_supervisor", fake_supervisor)

    app_file = tmp_path / "ls_cli_app.py"
    app_file.write_text(
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

    litestar_cli_mod = import_optional("litestar.cli.main")
    runner = CliRunner()
    if litestar_cli_mod is not None:
        litestar_group = litestar_cli_mod.litestar_group
        original_run = litestar_group.commands.get("run")
        try:
            help_res = runner.invoke(
                litestar_group,
                ["--app-dir", str(tmp_path), "--app", "ls_cli_app:app", "run", "--help"],
            )
            assert help_res.exit_code == 0, help_res.output
            assert "--topology" in help_res.output
            assert "--drain-timeout" in help_res.output

            run_res = runner.invoke(
                litestar_group,
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
            assert run_res.exit_code == 0, run_res.output
            assert captured["env"]["VERTEBRA_APP"] == "ls_cli_app:app"
            assert captured["env"]["VERTEBRA_PORT"] == "8123"
            assert captured["env"]["VERTEBRA_WORKERS"] == "2"
            assert captured["env"]["VERTEBRA_TOPOLOGY"] == "shm"
            assert "VERTEBRA_OFFLOAD_CONFIG" in captured["env"]
            offload = decode_json(captured["env"]["VERTEBRA_OFFLOAD_CONFIG"])
            assert offload["compression"]["backend"] == "zstd"
        finally:
            if original_run is not None:
                litestar_group.commands["run"] = original_run
    else:
        group = click.Group("litestar")
        VertebraPlugin().on_cli_init(group)
        assert "run" in group.commands


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


@pytest.mark.parametrize(
    ("case", "expected_exit", "expected_error"),
    [
        ("only-certfile", 2, "Both --ssl-certfile and --ssl-keyfile must be provided together"),
        ("only-keyfile", 2, "Both --ssl-certfile and --ssl-keyfile must be provided together"),
        ("missing-certfile", 2, "SSL certificate file does not exist"),
        ("missing-keyfile", 2, "SSL key file does not exist"),
        ("self-signed-missing-files", 0, None),
    ],
)
def test_litestar_cli_run_tls_cert_and_key_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
    expected_exit: int,
    expected_error: str | None,
) -> None:
    """Litestar run enforces mutual --ssl-certfile/--ssl-keyfile flags and file existence unless self-signed."""
    captured_env: dict[str, str] = {}

    def fake_build_cmd() -> list[str]:
        return ["/bin/true"]

    def fake_supervisor(*, server_cmd: list[str], env: dict[str, str], **_kwargs: Any) -> int:
        _ = server_cmd
        captured_env.update(env)
        return 0

    monkeypatch.setattr(litestar_plugin_mod, "build_server_command", fake_build_cmd)
    monkeypatch.setattr(litestar_plugin_mod, "run_supervisor", fake_supervisor)

    existing_cert = tmp_path / "present.crt"
    existing_cert.write_text("CERT", encoding="utf-8")
    missing_cert = tmp_path / "missing.crt"
    missing_key = tmp_path / "missing.key"

    cli_args_by_case: dict[str, list[str]] = {
        "only-certfile": ["run", "--no-banner", "--ssl-certfile", str(existing_cert)],
        "only-keyfile": ["run", "--no-banner", "--ssl-keyfile", str(missing_key)],
        "missing-certfile": [
            "run",
            "--no-banner",
            "--ssl-certfile",
            str(missing_cert),
            "--ssl-keyfile",
            str(missing_key),
        ],
        "missing-keyfile": [
            "run",
            "--no-banner",
            "--ssl-certfile",
            str(existing_cert),
            "--ssl-keyfile",
            str(missing_key),
        ],
        "self-signed-missing-files": [
            "run",
            "--no-banner",
            "--ssl-certfile",
            str(missing_cert),
            "--ssl-keyfile",
            str(missing_key),
            "--create-self-signed-cert",
        ],
    }

    group = click.Group("litestar")
    VertebraPlugin().on_cli_init(group)
    result = CliRunner().invoke(group, cli_args_by_case[case])
    assert result.exit_code == expected_exit, result.output
    if expected_error is not None:
        assert expected_error in result.output
        assert captured_env == {}
    else:
        assert captured_env["VERTEBRA_TLS_CERT"] == str(missing_cert.resolve())
        assert captured_env["VERTEBRA_TLS_KEY"] == str(missing_key.resolve())
        assert captured_env["VERTEBRA_SELF_SIGNED_CERT"] == "1"


@pytest.mark.parametrize("config", [None, ""])
def test_object_store_requires_explicit_config(config: Any) -> None:
    """Both public adapters fail closed without an explicit storage configuration."""
    from vertebra.extensions.litestar import ObjectStoreFileSystemAdapter, ObjectStoreLitestarStore

    for adapter in (ObjectStoreLitestarStore, ObjectStoreFileSystemAdapter):
        with pytest.raises(ImproperConfigurationError):
            adapter(config)


@pytest.mark.anyio
async def test_object_store_crud_and_namespace_deletion() -> None:
    """Logical keys and descendant namespaces stay confined through multiple delete pages."""
    from vertebra.extensions.litestar import ObjectStoreLitestarStore

    storage = AsyncObjectStore(MemoryConfig())
    root = ObjectStoreLitestarStore(storage)
    child = root.with_namespace("s1")
    sibling = root.with_namespace("s10")
    descendant = child.with_namespace("child")
    for namespace in ("", "..", "a/b", "a\\b", "a.b", "équipe"):
        with pytest.raises(ValueError):
            root.with_namespace(namespace)
    for key, value in (("", b""), ("../child/ü", b"\xff\x00"), ("text", "héllo")):
        await child.set(key, value)
        assert await child.get(key) == (value.encode() if isinstance(value, str) else value)
        assert await child.exists(key)
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
async def test_object_store_local_ttl_survives_reconstruction(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Wall-clock expiry and renewals persist even when Local discards object attributes."""
    from vertebra.extensions.litestar import ObjectStoreLitestarStore

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
    for ttl in (0, -1, timedelta(microseconds=-1)):
        await cache.set("immediate", b"expired", expires_in=ttl)
        assert await cache.get("immediate") is None
    await cache.set("fraction", b"rounded", expires_in=timedelta(microseconds=1))
    assert await cache.get("fraction") == b"rounded"
    clock[0] += 0.001
    assert await cache.get("fraction") is None


@pytest.mark.anyio
async def test_object_store_metadata_and_invalid_envelopes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Persistent metadata is inspectable and corrupt envelopes fail explicitly."""
    from vertebra.extensions.litestar import ObjectStoreLitestarStore

    monkeypatch.setattr(litestar_store_mod.time, "time", lambda: 1000.0)
    storage = AsyncObjectStore(MemoryConfig())
    cache = ObjectStoreLitestarStore(storage)
    await cache.set("ttl", b"payload", expires_in=2)
    object_key = "vertebra-store/~74746c"
    metadata = await storage.head(object_key)
    assert metadata.attributes["vertebra-expires-at-ms"] == "1002000"
    assert await storage.get(object_key) == b"VTBSTORE\x01" + encode_msgpack_bytes((1002000, b"payload"))
    for corrupt in (b"raw", b"VTBSTORE\x02", b"VTBSTORE\x01\xc1", b"VTBSTORE\x01" + encode_msgpack_bytes(("wrong", 1))):
        await storage.put(object_key, corrupt)
        with pytest.raises(ValueError):
            await cache.get("ttl")


@pytest.mark.anyio
async def test_static_object_store_reader_and_routes() -> None:
    """Litestar consumes the native async reader for GET/HEAD with a confined virtual root."""
    pytest.importorskip("litestar")
    from litestar.static_files import create_static_files_router
    from litestar.testing import AsyncTestClient

    from vertebra.extensions.litestar import ObjectStoreFileSystemAdapter

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
    for path in ("/../secret", "/nested/../../secret", "/../publicity/secret"):
        with pytest.raises(FileNotFoundError):
            await adapter.info(path)
        with pytest.raises(FileNotFoundError):
            await adapter.open(path)
    with pytest.raises(ValueError):
        await adapter.open("app.js", "w")
    with pytest.raises(ValueError):
        ObjectStoreFileSystemAdapter(storage, base_path="../public")
    with pytest.raises(FileNotFoundError):
        await adapter.info("missing")
    routes = create_static_files_router(path="/assets", directories=["/"], file_system=adapter, resolve_symlinks=False)
    async with AsyncTestClient(Litestar(route_handlers=[routes])) as client:
        response = await client.get("/assets/app.js")
        assert response.status_code == 200
        assert response.content == payload
        assert response.headers["content-length"] == str(len(payload))
        response = await client.head("/assets/app.js")
        assert response.status_code == 200
        assert response.content == b""
        assert response.headers["content-length"] == str(len(payload))
        assert (await client.get("/assets/missing")).status_code == 404


@pytest.mark.anyio
async def test_static_object_store_explicit_range_handler() -> None:
    """A supported Litestar Stream bridges storage full, partial, and unsatisfiable responses."""
    pytest.importorskip("litestar")
    from litestar import Request
    from litestar.response import Stream
    from litestar.testing import AsyncTestClient

    storage = AsyncObjectStore(MemoryConfig())
    payload = b"0123456789abcdefghij"
    await storage.put("app.js", payload)

    @get("/download")
    async def download(request: Request[Any, Any, Any]) -> Stream:
        response = await StreamingStorageResponse.from_async_store(
            storage, "app.js", range_header=request.headers.get("range")
        )

        async def chunks() -> Any:
            async for chunk in response.aiter_chunks():
                yield bytes(chunk)

        headers = dict(response.headers)
        if response.content_range is not None:
            headers["content-range"] = response.content_range
        return Stream(
            chunks(),
            status_code=response.status_code,
            headers=headers,
            media_type=response.content_type or response.media_type,
        )

    async with AsyncTestClient(Litestar(route_handlers=[download])) as client:
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
