"""Unit tests for Stage 1 subdomain consolidation, hot-path offload elimination, and 3.14t sharding."""

from __future__ import annotations

import asyncio
import importlib
import importlib.util
import inspect
import os
import socket
import sys
from pathlib import Path
from typing import Any

import pytest
from advanced_alchemy.types.file_object import FileObject

import vertebra
import vertebra.framework as vertebra_framework
import vertebra.storage._store as store_mod
from vertebra._vertebra import Request, init_transport
from vertebra.build import BuildOrchestrator
from vertebra.build.codegen import (
    CodegenConfig,
    CodegenPlugin,
    TypeScriptGenerator,
    ValibotGenerator,
    ZodGenerator,
    format_typescript,
)
from vertebra.build.packaging import (
    OnefileBuilder,
    StandaloneBuilder,
)
from vertebra.extensions.litestar import (
    RouteCompiler,
    compile_fastpath_routes,
    extract_routes,
    offload_middlewares,
)
from vertebra.server._asgi._runner import AsgiRunner
from vertebra.server._gil_audit import GilAuditReport
from vertebra.server._loops import EventLoopPool
from vertebra.server._rsgi._adapter import RsgiAdapter
from vertebra.server.platform import SHMAllocationPolicy
from vertebra.server.worker import _AsgiMountRunner, _instantiate_mount_runners
from vertebra.storage import AsyncObjectStore
from vertebra.utils.module_loader import import_optional
from vertebra.utils.portal import PortalProvider
from vertebra.utils.sync_tools import (
    CapacityLimiter,
    SyncLoopShardedResource,
    SyncPool,
    async_,
    run_,
)


def _load_alchemy_storage_module() -> Any:
    """Load vertebra.extensions.alchemy.storage directly or via standard import."""
    try:
        return importlib.import_module("vertebra.extensions.alchemy.storage")
    except ImportError:
        storage_path = Path(__file__).resolve().parents[3] / "vertebra" / "extensions" / "alchemy" / "storage.py"
        spec = importlib.util.spec_from_file_location(
            "vertebra_alchemy_storage_isolated",
            storage_path,
        )
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = mod
        spec.loader.exec_module(mod)
        return mod


def test_subdomain_consolidation_and_shims() -> None:
    """Verify canonical vertebra.build and vertebra.extensions.litestar exports."""
    assert BuildOrchestrator is not None
    assert CodegenPlugin is not None
    assert TypeScriptGenerator is not None
    assert callable(extract_routes)
    assert callable(offload_middlewares)
    assert callable(compile_fastpath_routes)
    assert callable(format_typescript)
    assert OnefileBuilder is not None
    assert StandaloneBuilder is not None
    assert RouteCompiler is not None
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
    """Verify ObjectStore and ObjectStoreBackend execute without asyncio.to_thread."""
    alchemy_storage_mod = _load_alchemy_storage_module()

    assert "to_thread" not in inspect.getsource(store_mod)
    assert "to_thread" not in inspect.getsource(alchemy_storage_mod)

    store = AsyncObjectStore("memory://")
    sample_file = tmp_path / "sample.bin"
    sample_file.write_bytes(b"zero-copy-payload-12345")

    put_res = await store.put_file("uploads/sample.bin", sample_file)
    assert put_res.size == 23

    view = await store.get_view("uploads/sample.bin")
    assert isinstance(view, memoryview)
    assert bytes(view) == b"zero-copy-payload-12345"

    backend = alchemy_storage_mod.ObjectStoreBackend(key="stage1_hotpath", store=store, prefix="attachments")
    attachment = FileObject(backend=backend, filename="doc.bin")
    saved = await backend.save_object_async(attachment, sample_file)
    assert saved.size == 23
    assert await backend.get_content_async("doc.bin") == b"zero-copy-payload-12345"


def test_portal_and_sync_tools_loop_sharding() -> None:
    """Verify Portal concurrent submission, LoopShardedResource, CapacityLimiter, and SyncPool."""
    with PortalProvider() as portal:

        async def _double(x: int) -> int:
            await asyncio.sleep(0.001)
            return x * 2

        assert portal.call(_double, 21) == 42

    sync_fn = run_(lambda x: asyncio.sleep(0, result=x + 10))
    assert sync_fn(5) == 15

    sharded: SyncLoopShardedResource[list[int]] = SyncLoopShardedResource(lambda: [1, 2, 3])
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

    pool = SyncPool(max_workers=2)
    try:
        fut = pool.submit(lambda a, b: a + b, 19, 23)
        assert fut.result(timeout=5.0) == 42
    finally:
        pool.shutdown()

    async def _run_sync_to_async() -> int:
        wrapped = async_(lambda v: v * 3)
        return await wrapped(7)

    assert asyncio.run(_run_sync_to_async()) == 21


def test_gil_audit_and_strict_enforcement() -> None:
    """Verify GilAuditReport detects simulated GIL re-enablement and honours strict mode."""
    assert isinstance(GilAuditReport.is_free_threaded_build(), bool)
    ext_snap = GilAuditReport.snapshot_extensions()
    assert isinstance(ext_snap, frozenset)
    assert isinstance(GilAuditReport.detect_unsafe_modules(), tuple)

    report = GilAuditReport.audit(before_extensions=ext_snap, gil_enabled_before=True, strict=False)
    assert isinstance(report, GilAuditReport)
    assert not report.gil_reenabled_on_import
    assert report.quarantine_unsafe_modules(4) == 4

    with pytest.raises(RuntimeError, match="CPython GIL was silently re-enabled"):
        GilAuditReport.audit(
            before_extensions=frozenset(),
            gil_enabled_before=False,
            gil_enabled_after=True,
            strict=True,
        )


def test_shm_allocation_policy_and_scm_rights() -> None:
    """Verify SHMAllocationPolicy allocates anonymous SHM fds and transfers them via SCM_RIGHTS."""
    policy = SHMAllocationPolicy(prefer_memfd=True)
    fd, backend = policy.allocate_anonymous_shm_fd("ring-test", 4096)
    try:
        assert fd >= 0
        assert backend in {"memfd", "tmpfile"}
        sock_a, sock_b = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            SHMAllocationPolicy.send_fd(sock_a, fd)
            received_fd = SHMAllocationPolicy.recv_fd(sock_b)
            try:
                assert os.fstat(received_fd).st_size == 4096
            finally:
                os.close(received_fd)
        finally:
            sock_a.close()
            sock_b.close()
    finally:
        os.close(fd)


def test_asgi_event_loop_pool_sharded_mount_runners() -> None:
    """Verify EventLoopPool exposes loop shards and _instantiate_mount_runners shards ASGI mounts."""
    pool = EventLoopPool(size=2)
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
        assert isinstance(runner, _AsgiMountRunner)
        assert type(runner).__name__ == "_AsgiMountRunner"
    finally:
        pool.stop()


def test_event_loop_pool_uses_uvloop_and_supports_inline_single_loop() -> None:
    """Verify EventLoopPool uses uvloop on POSIX when available and inline_single_loop avoids extra threads."""
    uvloop_mod = import_optional("uvloop")
    inline_pool = EventLoopPool(size=1, inline_single_loop=True)
    try:
        assert inline_pool.size == 1
        assert len(inline_pool._threads) == 0
        if sys.platform != "win32" and uvloop_mod is not None:
            assert isinstance(inline_pool.primary_loop, uvloop_mod.Loop)
    finally:
        inline_pool.stop()


def test_asgi_runner_and_rsgi_adapter_spawn_batch_use_rust_drivers() -> None:
    """Verify AsgiRunner.spawn_batch and RsgiAdapter.spawn_batch execute unary batches via Rust drivers."""
    init_transport()
    asgi_seen: list[str] = []
    rsgi_seen: list[str] = []

    async def fast_asgi(scope: Any, receive: Any, send: Any) -> None:
        await receive()
        asgi_seen.append(scope["path"])
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    class FastRsgi:
        async def __rsgi__(self, scope: Any, protocol: Any) -> None:
            rsgi_seen.append(scope.path)
            protocol.response_bytes(200, [], b"ok")

    loop = asyncio.new_event_loop()
    try:
        runner = AsgiRunner(fast_asgi, loop=loop, mount_prefix="/v1")
        rsgi_adapter = RsgiAdapter(FastRsgi(), mount_prefix="/v1")
        reqs = [
            Request.from_parts("GET", "/v1/one", b"", request_id=951),
            Request.from_parts("GET", "/v1/two", b"", request_id=952),
        ]
        runner.spawn_batch(reqs)
        inline_rsgi = rsgi_adapter.spawn_batch(reqs, loop=loop)
        assert asgi_seen == ["/one", "/two"]
        assert rsgi_seen == ["/one", "/two"]
        assert inline_rsgi == 2
    finally:
        loop.close()


def test_pep810_lazy_modules_declared_on_packages() -> None:
    """Verify vertebra and vertebra.framework declare PEP 810 __lazy_modules__."""
    top_lazy = getattr(vertebra, "__lazy_modules__", None)
    fw_lazy = getattr(vertebra_framework, "__lazy_modules__", None)
    assert isinstance(top_lazy, list | tuple)
    assert "vertebra.framework" in top_lazy
    assert isinstance(fw_lazy, list | tuple)
    assert "vertebra.framework.openapi" in fw_lazy
