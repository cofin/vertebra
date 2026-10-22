"""Unit tests for LitestarPlugin, anyio-compatible AsgiRunner, and BuildOrchestrator ecosystem export."""

import asyncio
import contextlib
import importlib
import inspect
import os
from collections.abc import Callable, Iterator, Sequence
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import anyio
import msgspec
import pytest

from vertebra.build import BuildOrchestrator
from vertebra.extensions.litestar import (
    LitestarConfig,
    LitestarPlugin,
    enter_litestar_server_lifespan,
)
from vertebra.server._asgi._runner import AsgiRunner, _drive_coro
from vertebra.utils.serializers import decode_json, encode_json_bytes


def _load_litestar_plugin_test_primitives() -> tuple[
    Any, Callable[..., Callable[[Callable[..., Any]], Any]], Any, Any, Any
]:
    """Load Litestar plugin test classes or fallback doubles when optional extra is absent."""
    try:
        litestar_mod = importlib.import_module("litestar")
        comp_mod = importlib.import_module("litestar.config.compression")
        cors_mod = importlib.import_module("litestar.config.cors")
        plugins_mod = importlib.import_module("litestar.plugins")
        return (
            litestar_mod.Litestar,
            litestar_mod.get,
            comp_mod.CompressionConfig,
            cors_mod.CORSConfig,
            plugins_mod.CLIPlugin,
        )
    except ImportError:

        class CompressionConfigStub(msgspec.Struct, gc=False):
            backend: str = "gzip"
            minimum_size: int = 500
            gzip_compress_level: int = 6
            brotli_quality: int = 4
            zstd_level: int = 3
            exclude: Sequence[str] | str | None = None
            exclude_opt_key: str | None = None

        class CORSConfigStub(msgspec.Struct, gc=False):
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
                "before_send",
                "compression_config",
                "cors_config",
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
                    state={},
                    on_app_init=[],
                )
                for plugin in app_config.plugins:
                    on_init = getattr(plugin, "on_app_init", None)
                    if callable(on_init):
                        app_config = on_init(app_config)

                self.compression_config = app_config.compression_config
                self.cors_config = app_config.cors_config
                self.before_send = app_config.before_send
                self.state = dict(app_config.state)
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
                for hook in app_config.on_app_init:
                    hook(self)

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
    """Verify LitestarPlugin intercepts compression/CORS before trie build and enters CLIPlugin.server_lifespan."""
    monkeypatch.delenv("LITESTAR_APP", raising=False)
    tracker = _LifecycleTrackerPlugin()
    vtb_plugin = LitestarPlugin(LitestarConfig(enable_health_endpoint=True, health_path="/_vtb/healthz"))

    @get("/ping", sync_to_thread=False)
    def ping() -> dict[str, str]:
        return {"status": "pong"}

    app = Litestar(
        route_handlers=[ping],
        plugins=[vtb_plugin, tracker],
        compression_config=CompressionConfig(backend="zstd", minimum_size=512),
        cors_config=CORSConfig(allow_origins=["https://console.example.com"]),
    )

    assert app.compression_config is None
    assert app.state["_vertebra_offloaded_compression_config"] is not None
    assert app.state["_vertebra_offloaded_cors_config"] is not None
    assert vtb_plugin.offload_plan is not None
    assert vtb_plugin.offload_plan.compression is not None
    assert vtb_plugin.offload_plan.compression.backend == "zstd"
    vtb_plugin.on_cli_init(None)

    with enter_litestar_server_lifespan(app, app_path="example.app:app"):
        assert tracker.entered is True
        assert tracker.seen_app_env == "example.app:app"
        assert os.environ["LITESTAR_APP"] == "example.app:app"
    assert tracker.exited is True

    disabled_plugin = LitestarPlugin(
        LitestarConfig(
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
        _drive_coro(yielded_then_anyio())
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
