"""Unit tests for Stage 1 subdomain consolidation, hot-path offload elimination, and 3.14t sharding."""

from __future__ import annotations

import asyncio
import inspect
from pathlib import Path
from typing import Any

import pytest

from vertebra.build import (
    BuildOrchestrator,
    CodegenConfig,
    CodegenOrchestrator,
    CodegenPlugin,
    OnefileBuilder,
    StandaloneBuilder,
    ValibotGenerator,
    ZodGenerator,
    generate_typescript,
)
from vertebra.builder import BuildOrchestrator as LegacyBuildOrchestrator
from vertebra.builder._codegen import CodegenOrchestrator as LegacyCodegenOrchestrator
from vertebra.extensions.alchemy.storage import AlchemyFileAttachment, VertebraAlchemyStorageBackend
from vertebra.extensions.codegen import CodegenPlugin as LegacyCodegenPlugin
from vertebra.extensions.litestar import (
    LitestarRouteCompiler,
    MiddlewareOffloadCompiler,
    compile_litestar_routes,
    compile_middleware_offload,
    create_fused_handler,
)
from vertebra.server._asgi._litestar_fastpath import create_fused_handler as shim_create_fused_handler
from vertebra.server._asgi._litestar_offload import compile_middleware_offload as shim_compile_offload
from vertebra.server._asgi._litestar_router import compile_litestar_routes as shim_compile_routes
from vertebra.server._gil_audit import (
    GilAuditReport,
    audit_gil_state,
    quarantine_unsafe_314t_modules,
    snapshot_native_extensions,
)
from vertebra.server._native import _instantiate_mount_runners
from vertebra.server._runtime_tuning import AsgiEventLoopPool
from vertebra.storage import VertebraStore
from vertebra.utils.portal import PortalProvider
from vertebra.utils.sync_tools import (
    CapacityLimiter,
    FreeThreadedSyncPool,
    LoopShardedResource,
    async_to_sync,
    sync_to_async,
)


def test_subdomain_consolidation_and_shims() -> None:
    """Verify canonical vertebra.build and vertebra.extensions.litestar exports match legacy shims."""
    assert BuildOrchestrator is LegacyBuildOrchestrator
    assert CodegenOrchestrator is LegacyCodegenOrchestrator
    assert CodegenPlugin is LegacyCodegenPlugin
    assert compile_litestar_routes is shim_compile_routes
    assert compile_middleware_offload is shim_compile_offload
    assert create_fused_handler is shim_create_fused_handler
    assert callable(generate_typescript)
    assert OnefileBuilder is not None
    assert StandaloneBuilder is not None
    assert LitestarRouteCompiler is not None
    assert MiddlewareOffloadCompiler is not None
    assert CodegenConfig is not None

    valibot_out = ValibotGenerator().emit_schema(
        "CreateItem",
        {"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]},
    )
    assert "CreateItem" in valibot_out

    zod_out = ZodGenerator().emit_schema(
        "CreateItem",
        {"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]},
    )
    assert "CreateItem" in zod_out


@pytest.mark.anyio
async def test_storage_and_alchemy_zero_to_thread(tmp_path: Path) -> None:
    """Verify VertebraStore and VertebraAlchemyStorageBackend execute without asyncio.to_thread."""
    import vertebra.extensions.alchemy.storage as alchemy_storage_mod
    import vertebra.storage._store as store_mod

    assert "to_thread" not in inspect.getsource(store_mod)
    assert "to_thread" not in inspect.getsource(alchemy_storage_mod)

    store = VertebraStore("memory://")
    sample_file = tmp_path / "sample.bin"
    sample_file.write_bytes(b"zero-copy-payload-12345")

    put_res = await store.put_file_async("uploads/sample.bin", sample_file)
    assert put_res.size == 23

    view = await store.get_view_async("uploads/sample.bin")
    assert isinstance(view, memoryview)
    assert bytes(view) == b"zero-copy-payload-12345"

    backend = VertebraAlchemyStorageBackend(store=store, prefix="attachments")
    attachment = AlchemyFileAttachment(path="doc.bin")
    saved = await backend.save_object_async(attachment, sample_file)
    assert saved.size == 23
    assert await backend.get_content_async("doc.bin") == b"zero-copy-payload-12345"


def test_portal_and_sync_tools_loop_sharding() -> None:
    """Verify Portal concurrent submission, LoopShardedResource, CapacityLimiter, and FreeThreadedSyncPool."""
    with PortalProvider() as portal:

        async def _double(x: int) -> int:
            await asyncio.sleep(0.001)
            return x * 2

        assert portal.call(_double, 21) == 42

    sync_fn = async_to_sync(lambda x: asyncio.sleep(0, result=x + 10))
    assert sync_fn(5) == 15

    sharded: LoopShardedResource[list[int]] = LoopShardedResource(lambda: [1, 2, 3])
    limiter = CapacityLimiter(4)

    async def _check_loop_affinity(expected_len: int) -> int:
        async with limiter:
            items = sharded.get()
            items.append(4)
            return len(items) if len(items) == expected_len else -1

    loop1 = asyncio.new_event_loop()
    loop2 = asyncio.new_event_loop()
    try:
        assert loop1.run_until_complete(_check_loop_affinity(4)) == 4
        assert loop2.run_until_complete(_check_loop_affinity(4)) == 4
    finally:
        loop1.close()
        loop2.close()

    pool = FreeThreadedSyncPool(max_workers=2)
    try:
        fut = pool.submit(lambda a, b: a + b, 19, 23)
        assert fut.result(timeout=5.0) == 42
    finally:
        pool.shutdown()

    async def _run_sync_to_async() -> int:
        wrapped = sync_to_async(lambda v: v * 3)
        return await wrapped(7)

    assert asyncio.run(_run_sync_to_async()) == 21


def test_gil_audit_and_strict_enforcement() -> None:
    """Verify GilAuditReport detects simulated GIL re-enablement and honours strict mode."""
    ext_snap = snapshot_native_extensions()
    assert isinstance(ext_snap, frozenset)
    assert isinstance(quarantine_unsafe_314t_modules(), tuple)

    report = audit_gil_state(before_extensions=ext_snap, gil_enabled_before=True, strict=False)
    assert isinstance(report, GilAuditReport)
    assert not report.gil_reenabled_on_import

    with pytest.raises(RuntimeError, match="CPython GIL was silently re-enabled"):
        audit_gil_state(
            before_extensions=frozenset(),
            gil_enabled_before=False,
            gil_enabled_after=True,
            strict=True,
        )


def test_asgi_event_loop_pool_sharded_mount_runners() -> None:
    """Verify AsgiEventLoopPool exposes loop shards and _instantiate_mount_runners shards ASGI mounts."""
    pool = AsgiEventLoopPool(size=2)
    try:
        assert len(pool.loops) == 2

        async def _dummy_asgi(scope: Any, receive: Any, send: Any) -> None:
            _ = (scope, receive)
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"ok"})

        mounts = _instantiate_mount_runners([("/api", _dummy_asgi, "asgi")], pool)
        assert len(mounts) == 1
        prefix, runner, proto = mounts[0]
        assert prefix == "/api"
        assert proto == "asgi"
        assert type(runner).__name__ == "_ShardedAsgiMountRunner"
    finally:
        pool.stop()
