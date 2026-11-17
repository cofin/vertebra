"""Tests for the Vite fallback subprocess manager and neutral framework Vite/storage/channel/queue modules."""

from __future__ import annotations

import asyncio
import importlib
import sys
import weakref
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from vertebra.build._assets import AssetManifest, ManifestEntry
from vertebra.build._fallback import DevServerProcess, run_vite_build
from vertebra.framework.channels import ChannelBus, ChannelMessage
from vertebra.framework.queues import (
    MemoryQueueBackend,
    QueueConfig,
    QueueService,
    attach_queue_service,
    get_queue_service,
    queue_lifespan,
)
from vertebra.framework.storage import AsyncStorageRegistry, MemoryConfig, SyncStorageRegistry
from vertebra.framework.vite import (
    ViteBridge,
    ViteConfig,
    clear_vite_bridges,
    get_active_vite_bridge,
    get_vertebra_client_path,
    get_vertebra_client_vite_aliases,
    register_vite_bridge,
    vite_asset,
    vite_hmr_client,
)
from vertebra.server.lanes import LaneConfig


def test_vite_dev_server_proxy_url() -> None:
    server = DevServerProcess(Path("/tmp"), 5173)
    assert server.proxy_url == "http://127.0.0.1:5173"


def test_vite_dev_server_not_running_initially() -> None:
    server = DevServerProcess(Path("/tmp"), 5173)
    assert not server.is_running


def test_vite_dev_server_stop_when_not_started() -> None:
    """Stopping a server that was never started should be a no-op."""
    server = DevServerProcess(Path("/tmp"), 5173)
    server.stop()
    assert not server.is_running


def test_vite_dev_server_proxy_url_custom_port() -> None:
    server = DevServerProcess(Path("/tmp"), 3000)
    assert server.proxy_url == "http://127.0.0.1:3000"


def test_vite_dev_server_resolve_command_no_vite_no_npx() -> None:
    """Should raise RuntimeError when neither vite nor npx is found."""
    server = DevServerProcess(Path("/tmp"), 5173)
    with patch("shutil.which", return_value=None):
        try:
            server._resolve_command()
        except RuntimeError as exc:
            assert "Vite not found" in str(exc)
        else:
            raise AssertionError("Expected RuntimeError")


def test_vite_dev_server_resolve_command_uses_vite() -> None:
    server = DevServerProcess(Path("/tmp"), 5173)
    with patch("shutil.which", side_effect=lambda x: "/usr/bin/vite" if x == "vite" else None):
        cmd = server._resolve_command()
        assert "/usr/bin/vite" in cmd
        assert "--port 5173" in cmd


def test_vite_dev_server_resolve_command_falls_back_to_npx() -> None:
    server = DevServerProcess(Path("/tmp"), 5173)

    def mock_which(name: str) -> str | None:
        if name == "npx":
            return "/usr/bin/npx"
        return None

    with patch("shutil.which", side_effect=mock_which):
        cmd = server._resolve_command()
        assert "npx" in cmd
        assert "vite" in cmd


def test_run_vite_build_raises_when_vite_missing() -> None:
    with patch("shutil.which", return_value=None):
        try:
            run_vite_build(Path("/tmp"))
        except RuntimeError as exc:
            assert "Vite not found" in str(exc)
        else:
            raise AssertionError("Expected RuntimeError")


@pytest.mark.parametrize(
    "module_name",
    [
        "vertebra.framework.vite",
        "vertebra.framework.storage",
        "vertebra.framework.channels",
        "vertebra.framework.queues._asgi",
    ],
)
def test_neutral_framework_modules_import_without_optional_hosts(module_name: str) -> None:
    blocked = {
        "litestar": None,
        "starlette": None,
        "fastapi": None,
        "flask": None,
        "django": None,
    }
    saved = sys.modules.pop(module_name, None)
    try:
        with patch.dict(sys.modules, blocked):
            mod = importlib.import_module(module_name)
            assert mod is not None
    finally:
        if saved is not None:
            sys.modules[module_name] = saved


def test_vite_bridge_dev_hmr_manifest_escaping_and_registry(tmp_path: Path) -> None:
    clear_vite_bridges()
    try:
        dev_cfg = ViteConfig(
            manifest_path=str(tmp_path / "missing-manifest.json"),
            dev_mode=True,
            dev_server_url='http://localhost:5173/"bad"',
            csp_nonce='nonce-"123"<',
        )
        dev_bridge = ViteBridge(dev_cfg)
        assert dev_bridge.manifest is None
        hmr_html = dev_bridge.hmr_client_tag()
        assert "&quot;bad&quot;" in hmr_html
        assert "/@vite/client" in hmr_html
        assert "nonce-&quot;123&quot;&lt;" in hmr_html

        dev_asset_html = dev_bridge.asset_tags('src/main.ts?x="1"')
        assert "&quot;1&quot;" in dev_asset_html
        assert "http://localhost:5173/" in dev_asset_html

        register_vite_bridge(dev_bridge)
        assert get_active_vite_bridge() is dev_bridge
        assert vite_hmr_client() == hmr_html
        assert vite_asset("src/app.ts") == dev_bridge.asset_tags("src/app.ts")

        manifest = AssetManifest(
            entries={
                "src/main.ts": ManifestEntry(
                    file="assets/main-abc12345.js",
                    is_entry=True,
                    css=("assets/main-def67890.css",),
                    integrity="sha384-test",
                ),
            }
        )
        prod_bridge = ViteBridge(
            ViteConfig(asset_url='/static/"v1"/', dev_mode=False),
            manifest=manifest,
        )
        assert prod_bridge.hmr_client_tag() == ""
        prod_html = prod_bridge.asset_tags("src/main.ts")
        assert "assets/main-def67890.css" in prod_html
        assert "assets/main-abc12345.js" in prod_html
        assert "&quot;v1&quot;" in prod_html
        assert 'integrity="sha384-test"' in prod_html

        with pytest.raises(ValueError, match="empty"):
            prod_bridge.asset_tags("   ")
        with pytest.raises(KeyError, match="Unknown Vite bridge"):
            get_active_vite_bridge("unknown-bridge")

        client_path = get_vertebra_client_path()
        aliases = get_vertebra_client_vite_aliases()
        expected_keys = (
            "@vertebra/client",
            "@vertebra/client/react",
            "@vertebra/client/vue",
            "@vertebra/client/svelte",
            "@vertebra/client/vite",
            "@vertebra/client/router",
            "@vertebra/client/webmcp",
            "@vertebra/client/htmx",
        )
        for key in expected_keys:
            assert key in aliases
            assert str(client_path) in aliases[key]
            assert Path(aliases[key]).exists()
    finally:
        clear_vite_bridges()


@pytest.mark.parametrize("registry_cls", [SyncStorageRegistry, AsyncStorageRegistry])
def test_storage_registry_lookup_and_missing_alias_diagnostics(
    registry_cls: type[SyncStorageRegistry] | type[AsyncStorageRegistry],
) -> None:
    registry = registry_cls({"uploads": MemoryConfig(), "avatars": "memory://"})
    assert registry.names() == ("uploads", "avatars")
    assert "uploads" in registry
    assert len(registry) == 2
    assert registry["uploads"] is registry.get("uploads")
    assert registry.get("avatars") is registry["avatars"]

    with pytest.raises(KeyError, match="avatars") as exc_info:
        registry.get("missing-bucket")
    assert "uploads" in str(exc_info.value)
    assert "missing-bucket" in str(exc_info.value)


@pytest.mark.anyio
async def test_channel_bus_multi_subscriber_broadcast_and_unsubscribe_cleanup() -> None:
    bus = ChannelBus()
    await bus.on_startup()
    await bus.subscribe(["events"])

    sub1_q = bus._register_stream_queue()
    sub2_q = bus._register_stream_queue()
    sub1_ref = weakref.ref(sub1_q)
    sub2_ref = weakref.ref(sub2_q)

    await bus.publish(b"hello", ["events"])
    assert await sub1_q.get() == ("events", b"hello")
    assert await sub2_q.get() == ("events", b"hello")

    bus.close_subscriber(sub1_q)
    del sub1_q
    await bus.publish(b"second", ["events"])
    assert await sub2_q.get() == ("events", b"second")

    await bus.unsubscribe(["events"])
    assert "events" not in bus._subscribers

    bus.close_subscriber(sub2_q)
    del sub2_q
    await bus.unsubscribe(["events"])
    assert not bus._queues
    assert not bus._subscribers
    assert sub1_ref() is None
    assert sub2_ref() is None

    received: list[ChannelMessage] = []

    async def _consume_one() -> None:
        async with bus.subscription(["live"]) as stream:
            async for msg in stream:
                received.append(msg)
                break

    task = asyncio.create_task(_consume_one())
    await asyncio.sleep(0.02)
    await bus.publish("payload-1", ["live"])
    await task
    assert received == [ChannelMessage(channel="live", data=b"payload-1")]
    assert not bus._queues
    assert not bus._subscribers
    await bus.aclose()


class _TrackedMemoryBackend(MemoryQueueBackend):
    """MemoryQueueBackend recording close invocations for lifespan verification."""

    def __init__(self) -> None:
        super().__init__()
        self.close_calls = 0

    async def close(self) -> None:
        self.close_calls += 1
        await super().close()


@pytest.mark.parametrize("raise_on_body", [False, True])
@pytest.mark.anyio
async def test_asgi_queue_helpers_and_lifespan_cleanup(raise_on_body: bool) -> None:
    app = SimpleNamespace(state=SimpleNamespace())
    backend = _TrackedMemoryBackend()
    service = QueueService(config=QueueConfig(), backend=backend)
    attached = attach_queue_service(app, service)
    assert attached is service
    assert get_queue_service(app) is service

    lanes = [LaneConfig(queue_name="default", concurrency=1, lane_kind="io")]
    if raise_on_body:
        with pytest.raises(RuntimeError, match="boom"):
            async with queue_lifespan(app, lanes=lanes, run_worker=True) as active:
                assert active is service
                await asyncio.sleep(0.02)
                raise RuntimeError("boom")
    else:
        async with queue_lifespan(app, lanes=lanes, run_worker=True) as active:
            assert active is service
            await asyncio.sleep(0.02)

    assert backend.close_calls == 1
