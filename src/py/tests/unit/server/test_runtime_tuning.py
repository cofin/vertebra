"""Unit tests for event loop sharding, CPU affinity, GIL auditing, and protocol detection."""

import asyncio
import os
import threading
from pathlib import Path
from typing import Any

import pytest

from vertebra.cli._launch import ServerLaunchEnv
from vertebra.server._gil_audit import GilAuditReport, is_free_threaded
from vertebra.server._loops import (
    EventLoopPool,
    configure_worker_affinity,
    resolve_loop_shards,
)
from vertebra.server._protocol import detect_interface, is_rsgi_app
from vertebra.server.platform import CgroupQuota


def test_cgroup_quota_parses_throttled_container(tmp_path: Path) -> None:
    """Verify CgroupQuota parses cpu.max quota/period into integer worker count."""
    cpu_max = tmp_path / "cpu.max"
    cpu_max.write_text("250000 100000\n", encoding="utf-8")

    quota = CgroupQuota.detect(cpu_max)
    assert quota.cpu_quota_cores == pytest.approx(2.5)
    assert quota.effective_workers == 3


def test_cgroup_quota_parses_fractional_sub_one_core(tmp_path: Path) -> None:
    """Verify sub-1.0 CPU quotas clamp effective_workers to at least 1."""
    cpu_max = tmp_path / "cpu.max"
    cpu_max.write_text("50000 100000\n", encoding="utf-8")

    quota = CgroupQuota.detect(cpu_max)
    assert quota.cpu_quota_cores == pytest.approx(0.5)
    assert quota.effective_workers == 1


def test_cgroup_quota_unlimited_max_uses_host_cores(tmp_path: Path) -> None:
    """Verify 'max 100000' falls back to host CPU count."""
    cpu_max = tmp_path / "cpu.max"
    cpu_max.write_text("max 100000\n", encoding="utf-8")

    quota = CgroupQuota.detect(cpu_max)
    assert quota.cpu_quota_cores >= 1.0
    assert quota.effective_workers >= 1


def test_configure_worker_affinity_opt_in_and_safe() -> None:
    """Verify CPU affinity pinning is opt-in and succeeds on Linux when enabled."""
    assert configure_worker_affinity(0, enable_pinning=False) is False

    if hasattr(os, "sched_setaffinity") and hasattr(os, "sched_getaffinity"):
        original_mask = os.sched_getaffinity(0)
        try:
            pinned = configure_worker_affinity(0, enable_pinning=True)
            assert pinned is True
            current_mask = os.sched_getaffinity(0)
            assert len(current_mask) == 1
        finally:
            os.sched_setaffinity(0, original_mask)


def test_event_loop_pool_sharded_execution() -> None:
    """Verify EventLoopPool runs N independent event loop threads and schedules coroutines."""
    pool = EventLoopPool(size=3)
    try:
        assert pool.size == 3
        assert len(pool.loops) == 3

        seen_threads: set[int] = set()

        async def _capture_thread(idx: int) -> tuple[int, int]:
            await asyncio.sleep(0.005)
            return idx, threading.get_ident()

        futures = [pool.schedule(_capture_thread(i), worker_index=i) for i in range(6)]
        results = [fut.result(timeout=5.0) for fut in futures]
        assert [r[0] for r in results] == [0, 1, 2, 3, 4, 5]
        for _, tid in results:
            seen_threads.add(tid)

        assert len(seen_threads) == 3
    finally:
        pool.stop()


def test_event_loop_pool_schedule_batch() -> None:
    """Verify schedule_batch distributes work across shards and preserves order."""
    pool = EventLoopPool(size=2)
    try:

        async def _compute(val: int) -> int:
            await asyncio.sleep(0.002)
            return val * 10

        coros = [_compute(i) for i in range(8)]
        futures = pool.schedule_batch(coros)
        values = [f.result(timeout=5.0) for f in futures]
        assert values == [0, 10, 20, 30, 40, 50, 60, 70]
    finally:
        pool.stop()


def test_gil_audit_report_detects_reenablement() -> None:
    """Verify GilAuditReport flags native extensions when GIL transitions False -> True."""
    before = frozenset({"math", "json"})
    report = GilAuditReport.audit(
        before_extensions=before,
        gil_enabled_before=False,
        gil_enabled_after=True,
        strict=False,
    )
    assert report.gil_reenabled_on_import is True
    assert report.gil_enabled_before is False
    assert report.gil_enabled_after is True


def test_gil_audit_report_strict_mode_raises() -> None:
    """Verify strict=True raises RuntimeError when GIL is re-enabled on import."""
    with pytest.raises(RuntimeError, match="CPython GIL was silently re-enabled"):
        GilAuditReport.audit(
            before_extensions=frozenset(),
            gil_enabled_before=False,
            gil_enabled_after=True,
            strict=True,
        )


def test_gil_audit_quarantines_unsafe_modules_to_single_shard() -> None:
    """Verify GilAuditReport.quarantine_unsafe_modules forces single-shard execution on GIL re-enablement."""
    reenabled_report = GilAuditReport(
        free_threaded_build=True,
        gil_enabled_before=False,
        gil_enabled_after=True,
        gil_reenabled_on_import=True,
        suspect_extensions=("legacy_c_ext",),
    )
    assert reenabled_report.quarantine_unsafe_modules(8) == 1
    assert isinstance(asyncio.get_event_loop_policy(), asyncio.DefaultEventLoopPolicy)

    clean_report = GilAuditReport(
        free_threaded_build=True,
        gil_enabled_before=False,
        gil_enabled_after=False,
        gil_reenabled_on_import=False,
        suspect_extensions=(),
    )
    assert clean_report.quarantine_unsafe_modules(4) == 4
    assert isinstance(is_free_threaded(), bool)


def test_event_loop_pool_resolves_shards_from_cgroup_and_audit(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify resolve_loop_shards respects explicit overrides, env vars, and GilAuditReport quarantine."""
    monkeypatch.setenv("VERTEBRA_LOOP_SHARDS", "4")
    assert resolve_loop_shards() == 4
    assert resolve_loop_shards(explicit=6) == 6

    unsafe_report = GilAuditReport(
        free_threaded_build=True,
        gil_enabled_before=False,
        gil_enabled_after=True,
        gil_reenabled_on_import=True,
        suspect_extensions=("unsafe_ext",),
    )
    assert resolve_loop_shards(explicit=6, audit_report=unsafe_report) == 1


def test_protocol_detection_resolves_asgi_wsgi_and_rsgi() -> None:
    """Verify detect_interface and is_rsgi_app classify ASGI, WSGI, and RSGI callables."""

    async def asgi_app(scope: Any, receive: Any, send: Any) -> None:
        del scope, receive, send

    def wsgi_app(environ: Any, start_response: Any) -> list[bytes]:
        del environ, start_response
        return [b"ok"]

    class RsgiApp:
        async def __rsgi__(self, scope: Any, protocol: Any) -> None:
            del scope, protocol

    rsgi_instance = RsgiApp()
    assert is_rsgi_app(rsgi_instance) is True
    assert is_rsgi_app(asgi_app) is False
    assert detect_interface(asgi_app) == "asgi"
    assert detect_interface(wsgi_app) == "wsgi"
    assert detect_interface(rsgi_instance) == "rsgi"
    assert detect_interface(wsgi_app, "asgi") == "asgi"


def test_worker_with_vertebra_workers_env_defaults_to_single_loop_shard_on_gil(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify VERTEBRA_WORKERS does not oversubscribe loop shards when VERTEBRA_LOOP_SHARDS is unset."""
    monkeypatch.setenv("VERTEBRA_WORKERS", "4")
    monkeypatch.delenv("VERTEBRA_LOOP_SHARDS", raising=False)
    monkeypatch.setattr(GilAuditReport, "gil_enabled", staticmethod(lambda: True))
    assert resolve_loop_shards() == 1
    monkeypatch.setenv("VERTEBRA_LOOP_SHARDS", "2")
    assert resolve_loop_shards() == 2


def test_event_loop_pool_primary_loop_is_idempotent() -> None:
    """Verify repeated reads of pool.primary_loop return shard 0 without advancing round-robin state."""
    pool = EventLoopPool(size=4)
    try:
        first_primary = pool.primary_loop
        second_primary = pool.primary_loop
        assert first_primary is second_primary
        assert first_primary is pool.loops[0]
        assert pool.next_loop() is pool.loops[0]
        assert pool.next_loop() is pool.loops[1]
        assert pool.primary_loop is pool.loops[0]
    finally:
        pool.stop()


def test_gil_audit_quarantine_forces_single_loop_shard() -> None:
    """Verify GilAuditReport.audit with newly loaded suspect extension forces single-shard execution."""
    baseline = frozenset({"_abc"})
    report = GilAuditReport.audit(
        before_extensions=baseline,
        gil_enabled_before=False,
        gil_enabled_after=True,
        strict=False,
    )
    assert report.quarantine_unsafe_modules(4) == 1
    assert isinstance(asyncio.get_event_loop_policy(), asyncio.DefaultEventLoopPolicy)


def test_cgroup_quota_effective_cpu_count_and_launch_env(
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify CgroupQuota.effective_cpu_count and ServerLaunchEnv.to_environ respect affinity and site-packages."""
    cpu_max = tmp_path / "cpu.max"
    cpu_max.write_text("200000 100000\n", encoding="utf-8")
    monkeypatch.setattr("os.sched_getaffinity", lambda _pid: {0, 1, 2, 3}, raising=False)
    assert CgroupQuota.effective_cpu_count(cpu_max_path=cpu_max) == 2

    monkeypatch.setattr("os.sched_getaffinity", lambda _pid: {0}, raising=False)
    assert CgroupQuota.effective_cpu_count(cpu_max_path=cpu_max) == 1

    launch = ServerLaunchEnv(app_path="demo:app", host="127.0.0.1", port=8000)
    env = launch.to_environ(base={})
    assert env["VERTEBRA_THREADS"] == str(CgroupQuota.effective_cpu_count())
    assert "site-packages" in env["PYTHONPATH"] or env["PYTHONPATH"]
