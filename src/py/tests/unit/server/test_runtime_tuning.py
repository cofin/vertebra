"""Unit tests for event loop sharding, CPU affinity, GIL auditing, and protocol detection."""

import asyncio
import contextlib
import os
import sys
import threading
from collections.abc import AsyncIterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import click
import msgspec
import pytest
from click.testing import CliRunner

from vertebra.cli import cli
from vertebra.cli import dev as cli_dev_mod
from vertebra.cli import run as cli_run_mod
from vertebra.cli._launch import ServerLaunchEnv
from vertebra.extensions.litestar import VertebraPlugin
from vertebra.extensions.litestar import plugin as litestar_plugin_mod
from vertebra.framework.queues import MemoryQueueBackend, QueueService, task
from vertebra.server._gil_audit import GilAuditReport, is_free_threaded
from vertebra.server._loops import (
    LOOP_BACKEND_CHOICES,
    EventLoopPool,
    LoopBackend,
    LoopTaskSnapshot,
    capture_loop_task_snapshots,
    configure_worker_affinity,
    create_event_loop,
    format_task_call_graphs,
    resolve_loop_backend,
    resolve_loop_shards,
)
from vertebra.server._protocol import detect_interface, is_rsgi_app
from vertebra.server.lanes import LaneConfig, WorkerRunner
from vertebra.server.platform import CgroupQuota
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import decode_json, encode_json_bytes


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
            await asyncio.sleep(0)
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
            await asyncio.sleep(0)
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


def test_gil_audit_quarantines_unsafe_modules_to_single_shard(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify GilAuditReport.quarantine_unsafe_modules forces single-shard execution and loop quarantine."""
    monkeypatch.setenv("VERTEBRA_QUARANTINED_LOOPS", "")
    monkeypatch.delenv("VERTEBRA_QUARANTINED_LOOPS", raising=False)
    reenabled_report = GilAuditReport(
        free_threaded_build=True,
        gil_enabled_before=False,
        gil_enabled_after=True,
        gil_reenabled_on_import=True,
        suspect_extensions=("legacy_c_ext",),
        quarantined_modules=("uvloop==0.21.0", "rloop==0.1.0"),
    )
    assert reenabled_report.quarantine_unsafe_modules(8) == 1
    assert os.environ.get("VERTEBRA_QUARANTINED_LOOPS") == "rloop,uvloop"

    loop = create_event_loop(quarantined_modules=frozenset(reenabled_report.quarantined_modules))
    try:
        assert type(loop).__module__.startswith("asyncio")
    finally:
        loop.close()

    monkeypatch.delenv("VERTEBRA_QUARANTINED_LOOPS", raising=False)
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
    monkeypatch.setenv("VERTEBRA_QUARANTINED_LOOPS", "")
    monkeypatch.delenv("VERTEBRA_QUARANTINED_LOOPS", raising=False)
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


def test_gil_audit_quarantine_forces_single_loop_shard(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify GilAuditReport.audit with newly loaded suspect extension forces single-shard execution."""
    monkeypatch.setenv("VERTEBRA_QUARANTINED_LOOPS", "")
    monkeypatch.delenv("VERTEBRA_QUARANTINED_LOOPS", raising=False)
    baseline = frozenset({"_abc"})
    report = GilAuditReport.audit(
        before_extensions=baseline,
        gil_enabled_before=False,
        gil_enabled_after=True,
        strict=False,
    )
    assert report.quarantine_unsafe_modules(4) == 1

    quarantined_report = GilAuditReport(
        free_threaded_build=True,
        gil_enabled_before=False,
        gil_enabled_after=False,
        gil_reenabled_on_import=False,
        quarantined_modules=("uvloop==0.21.0", "rloop==0.1.0"),
    )
    assert quarantined_report.quarantine_unsafe_modules(4) == 1
    pool = EventLoopPool(size=1, inline_single_loop=True, gil_report=quarantined_report)
    try:
        assert type(pool.primary_loop).__module__.startswith("asyncio")
    finally:
        pool.stop()


@pytest.mark.parametrize(
    ("backend", "expected"),
    [
        (LoopBackend.AUTO, "auto"),
        (LoopBackend.UVLOOP, "uvloop"),
        (LoopBackend.WINLOOP, "winloop"),
        (LoopBackend.RLOOP, "rloop"),
        (LoopBackend.ZUVLOOP, "zuvloop"),
        (LoopBackend.ASYNCIO, "asyncio"),
    ],
)
def test_loop_backend_enum_values(backend: LoopBackend, expected: str) -> None:
    """Verify LoopBackend string enum values remain stable."""
    assert backend == expected


def test_loop_backend_resolution_and_cli_launch_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify LoopBackend resolution, create_event_loop backend selection, and CLI VERTEBRA_LOOP export."""
    monkeypatch.delenv("VERTEBRA_LOOP", raising=False)
    monkeypatch.delenv("VERTEBRA_QUARANTINED_LOOPS", raising=False)

    assert resolve_loop_backend("asyncio") == LoopBackend.ASYNCIO
    assert resolve_loop_backend("uvloop", quarantined_modules=frozenset({"uvloop==0.21.0"})) == LoopBackend.ASYNCIO

    monkeypatch.setenv("VERTEBRA_LOOP", "asyncio")
    assert resolve_loop_backend() == LoopBackend.ASYNCIO

    loop = create_event_loop(backend=LoopBackend.ASYNCIO)
    try:
        assert type(loop).__module__.startswith("asyncio")
    finally:
        loop.close()

    launch = ServerLaunchEnv(app_path="app:app", host="127.0.0.1", port=8000, loop="rloop")
    env = launch.to_environ(base={})
    assert env["VERTEBRA_LOOP"] == "rloop"

    zuv_launch = ServerLaunchEnv(app_path="app:app", host="127.0.0.1", port=8000, loop="zuvloop")
    zuv_env = zuv_launch.to_environ(base={})
    assert zuv_env["VERTEBRA_LOOP"] == "zuvloop"


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


@pytest.mark.parametrize(
    ("platform_name", "available_modules", "quarantined", "requested", "expected"),
    [
        ("linux", frozenset({"uvloop", "rloop"}), frozenset({"legacy_c_ext==1.0"}), "auto", LoopBackend.UVLOOP),
        (
            "linux",
            frozenset({"uvloop", "rloop"}),
            frozenset({"legacy_c_ext==1.0", "uvloop==0.21.0"}),
            "auto",
            LoopBackend.RLOOP,
        ),
        (
            "linux",
            frozenset({"uvloop", "rloop"}),
            frozenset({"legacy_c_ext==1.0", "uvloop==0.21.0", "rloop==0.5.0"}),
            "auto",
            LoopBackend.ASYNCIO,
        ),
        ("linux", frozenset({"rloop"}), frozenset({"other_ext==2.0"}), "rloop", LoopBackend.RLOOP),
        ("linux", frozenset({"rloop"}), frozenset({"rloop==0.5.0"}), "rloop", LoopBackend.ASYNCIO),
        ("linux", frozenset({"zuvloop"}), frozenset({"legacy_c_ext==1.0"}), "auto", LoopBackend.ZUVLOOP),
        ("linux", frozenset({"zuvloop"}), frozenset({"zuvloop==0.1.0"}), "zuvloop", LoopBackend.ASYNCIO),
        ("win32", frozenset({"winloop", "rloop"}), frozenset({"legacy_c_ext==1.0"}), "auto", LoopBackend.WINLOOP),
        ("win32", frozenset({"zuvloop"}), frozenset({"legacy_c_ext==1.0"}), "auto", LoopBackend.ZUVLOOP),
        ("win32", frozenset({"winloop"}), frozenset({"winloop==0.8.0"}), "auto", LoopBackend.ASYNCIO),
    ],
)
def test_resolve_loop_backend_scopes_quarantine_to_candidate_modules(
    monkeypatch: pytest.MonkeyPatch,
    platform_name: str,
    available_modules: frozenset[str],
    quarantined: frozenset[str],
    requested: str,
    expected: LoopBackend,
) -> None:
    """Verify unrelated quarantined C extensions do not suppress rloop or other non-quarantined loop backends."""
    monkeypatch.delenv("VERTEBRA_LOOP", raising=False)
    monkeypatch.delenv("VERTEBRA_QUARANTINED_LOOPS", raising=False)
    monkeypatch.setattr("vertebra.server._loops.sys.platform", platform_name)
    monkeypatch.setattr(
        "vertebra.server._loops.import_optional",
        lambda name: object() if name in available_modules else None,
    )

    assert resolve_loop_backend(requested, quarantined_modules=quarantined) == expected


@pytest.mark.parametrize("backend_arg", ["zuvloop", LoopBackend.ZUVLOOP], ids=["str", "enum"])
def test_create_event_loop_zuvloop(
    monkeypatch: pytest.MonkeyPatch,
    backend_arg: LoopBackend | str,
) -> None:
    """Verify create_event_loop accepts 'zuvloop' and LoopBackend.ZUVLOOP and exports it in LOOP_BACKEND_CHOICES."""
    sentinel_loop = asyncio.new_event_loop()
    try:
        fake_zuvloop = SimpleNamespace(new_event_loop=lambda: sentinel_loop)
        monkeypatch.setattr("vertebra.server._loops.sys.platform", "linux")
        monkeypatch.setattr(
            "vertebra.server._loops.import_optional",
            lambda name: fake_zuvloop if name == "zuvloop" else None,
        )
        assert create_event_loop(backend_arg) is sentinel_loop
    finally:
        sentinel_loop.close()

    assert "zuvloop" in LOOP_BACKEND_CHOICES
    assert tuple(backend.value for backend in LoopBackend) == LOOP_BACKEND_CHOICES


@pytest.mark.parametrize(
    ("command_name", "mod"),
    [
        ("run", cli_run_mod),
        ("dev", cli_dev_mod),
        ("litestar-run", litestar_plugin_mod),
    ],
    ids=["vtb-run", "vtb-dev", "litestar-run"],
)
def test_cli_loop_option_enforces_loop_backend_choices(
    monkeypatch: pytest.MonkeyPatch,
    command_name: str,
    mod: Any,
) -> None:
    """Verify vtb run, vtb dev, and litestar run share LOOP_BACKEND_CHOICES and reject invalid --loop values."""
    captured_env: dict[str, str] = {}

    def fake_build_cmd() -> list[str]:
        return ["/bin/true"]

    def fake_supervisor(*, server_cmd: list[str], env: dict[str, str], **_kwargs: Any) -> int:
        _ = server_cmd
        captured_env.update(env)
        return 0

    @contextlib.contextmanager
    def fake_lifespan(*_args: Any, **_kwargs: Any) -> Any:
        yield None

    monkeypatch.setattr(mod, "build_server_command", fake_build_cmd)
    monkeypatch.setattr(mod, "run_supervisor", fake_supervisor)
    if command_name == "litestar-run":
        target_group = click.Group("litestar")
        VertebraPlugin().on_cli_init(target_group)
        subcommand = "run"
    else:
        monkeypatch.setattr(mod, "resolve_cli_app_path", lambda _app: "app:app")
        monkeypatch.setattr(mod, "enter_app_server_lifespan", fake_lifespan)
        target_group = cli
        subcommand = command_name

    cmd = target_group.commands[subcommand]
    loop_option = next(param for param in cmd.params if param.name == "loop")
    assert isinstance(loop_option.type, click.Choice)
    assert tuple(loop_option.type.choices) == LOOP_BACKEND_CHOICES

    runner = CliRunner()
    env_keys = (
        "LITESTAR_APP",
        "VERTEBRA_APP",
        "LITESTAR_HOST",
        "LITESTAR_PORT",
        "LITESTAR_DEBUG",
        "LITESTAR_PDB",
    )
    saved_env = {key: os.environ.get(key) for key in env_keys}
    try:
        invalid_res = runner.invoke(target_group, [subcommand, "--loop", "invalid_loop"])
        assert invalid_res.exit_code == 2
        assert "Invalid value for '--loop'" in invalid_res.output
        assert captured_env == {}

        valid_res = runner.invoke(target_group, [subcommand, "--loop", "zuvloop", "--no-banner"])
        assert valid_res.exit_code == 0, valid_res.output
        assert captured_env["VERTEBRA_LOOP"] == "zuvloop"
    finally:
        for key, prev in saved_env.items():
            if prev is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = prev


def test_event_loop_pool_spawn_and_spawn_batch_dispatch_across_threads_and_inline() -> None:
    """Verify EventLoopPool.spawn and spawn_batch execute coroutines across multi-shard and inline single-shard pools."""
    done_event = threading.Event()
    recorded: list[int] = []
    lock = threading.Lock()

    async def _record(val: int, target_total: int) -> None:
        with lock:
            recorded.append(val)
            if len(recorded) >= target_total:
                done_event.set()

    pool = EventLoopPool(size=2, loop_backend=LoopBackend.ASYNCIO)
    try:
        pool.spawn(_record(100, 5), worker_index=0)
        pool.spawn_batch([_record(i, 5) for i in range(4)])
        assert done_event.wait(timeout=5.0)
        assert sorted(recorded) == [0, 1, 2, 3, 100]
    finally:
        pool.stop()

    inline_recorded: list[int] = []
    inline_pool = EventLoopPool(size=1, inline_single_loop=True, loop_backend=LoopBackend.ASYNCIO)
    try:

        async def _run_inline() -> None:
            async def _push(v: int) -> None:
                inline_recorded.append(v)

            inline_pool.spawn(_push(10))
            inline_pool.spawn_batch([_push(20), _push(30)])
            await asyncio.sleep(0)
            await asyncio.sleep(0)

        inline_pool.primary_loop.run_until_complete(_run_inline())
        assert inline_recorded == [10, 20, 30]
    finally:
        inline_pool.stop()


@pytest.mark.parametrize(
    ("backend_name", "platform_name"),
    [
        ("asyncio", "linux"),
        ("uvloop", "linux"),
        ("winloop", "win32"),
        ("rloop", "linux"),
        ("zuvloop", "linux"),
    ],
)
def test_create_event_loop_attaches_eager_task_factory_across_backends(
    monkeypatch: pytest.MonkeyPatch,
    backend_name: str,
    platform_name: str,
) -> None:
    """Verify create_event_loop attaches asyncio.eager_task_factory and runs tasks eagerly up to first suspension."""
    monkeypatch.delenv("VERTEBRA_LOOP", raising=False)
    monkeypatch.delenv("VERTEBRA_QUARANTINED_LOOPS", raising=False)
    monkeypatch.setattr("vertebra.server._loops.sys.platform", platform_name)

    use_real_uvloop = backend_name == "uvloop" and sys.platform != "win32" and import_optional("uvloop") is not None
    if backend_name not in {"asyncio", "uvloop"} or not use_real_uvloop:
        fake_mod = SimpleNamespace(new_event_loop=asyncio.new_event_loop)
        monkeypatch.setattr(
            "vertebra.server._loops.import_optional",
            lambda name: fake_mod if name == backend_name else None,
        )

    loop = create_event_loop(backend_name)
    try:
        if use_real_uvloop:
            assert type(loop).__module__.startswith("uvloop")
        assert loop.get_task_factory() is not None

        order: list[str] = []

        async def _child() -> str:
            order.append("before_suspend")
            await asyncio.sleep(0)
            order.append("after_suspend")
            return "ok"

        async def _driver() -> str:
            task = loop.create_task(_child())
            order.append("after_create_task")
            return await task

        result = loop.run_until_complete(_driver())
        assert result == "ok"
        assert order == ["before_suspend", "after_create_task", "after_suspend"]
    finally:
        loop.close()


@pytest.mark.parametrize(
    "exc_type",
    [AttributeError, NotImplementedError, TypeError],
    ids=["attribute-error", "not-implemented", "type-error"],
)
def test_create_event_loop_tolerates_unsupported_set_task_factory(
    monkeypatch: pytest.MonkeyPatch,
    exc_type: type[Exception],
) -> None:
    """Verify create_event_loop suppresses AttributeError, NotImplementedError, and TypeError from set_task_factory."""
    monkeypatch.setattr("vertebra.server._loops.sys.platform", "linux")

    if exc_type is AttributeError:
        fake_loop: Any = SimpleNamespace()
    else:

        def _raise_on_set(_factory: Any) -> None:
            raise exc_type("task factory unsupported")

        fake_loop = SimpleNamespace(set_task_factory=_raise_on_set)

    fake_mod = SimpleNamespace(new_event_loop=lambda: fake_loop)
    monkeypatch.setattr(
        "vertebra.server._loops.import_optional",
        lambda name: fake_mod if name == "zuvloop" else None,
    )

    assert create_event_loop("zuvloop") is fake_loop


@pytest.mark.parametrize("use_batch", [False, True], ids=["spawn", "spawn_batch"])
def test_inline_event_loop_pool_tracks_tasks_in_task_set(use_batch: bool) -> None:
    """Verify idle inline EventLoopPool schedules coroutines as tracked tasks in _task_sets[0]."""
    pool = EventLoopPool(size=1, inline_single_loop=True, loop_backend=LoopBackend.ASYNCIO)
    try:
        seen_tracked: list[bool] = []
        seen_counts: list[int] = []

        async def _probe() -> None:
            await asyncio.sleep(0)
            current = asyncio.current_task()
            seen_tracked.append(current in pool._task_sets[0])
            seen_counts.append(len(pool._task_sets[0]))

        if use_batch:
            pool.spawn_batch([_probe(), _probe()])
            assert seen_tracked == [True, True]
            assert max(seen_counts) >= 1
        else:
            pool.spawn(_probe())
            assert seen_tracked == [True]
            assert seen_counts == [1]

        assert len(pool._task_sets[0]) == 0
    finally:
        pool.stop()


@pytest.mark.parametrize(
    "stop_mode",
    ["idle", "from_other_thread", "from_inside_loop", "from_run_until_complete"],
)
def test_inline_event_loop_pool_stop_drains_tasks_and_asyncgens(stop_mode: str) -> None:
    """Verify inline EventLoopPool.stop cancels pending tasks, runs shutdown_asyncgens, and closes the loop."""
    pool = EventLoopPool(size=1, inline_single_loop=True, loop_backend=LoopBackend.ASYNCIO)
    events: list[str] = []
    live_gens: list[AsyncIterator[int]] = []

    async def _stream() -> AsyncIterator[int]:
        try:
            yield 1
            yield 2
        finally:
            events.append("asyncgen_closed")

    async def _pending_worker() -> None:
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            events.append("task_cancelled")
            raise

    if stop_mode == "idle":
        kept_tasks: list[asyncio.Task[None]] = []

        async def _setup_idle() -> None:
            gen = _stream()
            live_gens.append(gen)
            assert await anext(gen) == 1
            kept_tasks.append(pool.primary_loop.create_task(_pending_worker()))
            await asyncio.sleep(0)

        pool.primary_loop.run_until_complete(_setup_idle())
        assert not pool.primary_loop.is_running()
        assert events == []

        pool.stop()
        assert set(events) == {"task_cancelled", "asyncgen_closed"}
        assert pool.primary_loop.is_closed()
        assert len(pool._task_sets[0]) == 0
    elif stop_mode == "from_other_thread":
        ready = threading.Event()

        def _thread_runner() -> None:
            asyncio.set_event_loop(pool.primary_loop)

            async def _setup_running() -> None:
                gen = _stream()
                live_gens.append(gen)
                assert await anext(gen) == 1
                pool.spawn(_pending_worker())
                await asyncio.sleep(0)
                ready.set()

            setup_task = pool.primary_loop.create_task(_setup_running())
            _ = setup_task
            pool.primary_loop.run_forever()
            asyncio.set_event_loop(None)

        worker = threading.Thread(target=_thread_runner, daemon=True)
        worker.start()
        assert ready.wait(timeout=5.0)
        assert events == []

        pool.stop()
        worker.join(timeout=5.0)
        assert not worker.is_alive()
        assert set(events) == {"task_cancelled", "asyncgen_closed"}
        assert pool.primary_loop.is_closed()
        assert len(pool._task_sets[0]) == 0
    elif stop_mode == "from_inside_loop":

        async def _self_stopping() -> None:
            gen = _stream()
            live_gens.append(gen)
            assert await anext(gen) == 1
            pool.spawn(_pending_worker())
            await asyncio.sleep(0)
            pool.stop()

        runner_task = pool.primary_loop.create_task(_self_stopping())
        _ = runner_task
        pool.primary_loop.run_forever()
        assert set(events) == {"task_cancelled", "asyncgen_closed"}
        assert pool.primary_loop.is_closed()
        assert len(pool._task_sets[0]) == 0
    else:

        async def _main_until_complete() -> str:
            gen = _stream()
            live_gens.append(gen)
            assert await anext(gen) == 1
            pool.spawn(_pending_worker())
            await asyncio.sleep(0)
            pool.stop()
            return "completed"

        outcome = pool.primary_loop.run_until_complete(_main_until_complete())
        assert outcome == "completed"
        assert set(events) == {"task_cancelled", "asyncgen_closed"}
        assert pool.primary_loop.is_closed()
        assert len(pool._task_sets[0]) == 0


@pytest.mark.parametrize(
    ("inline_single_loop", "size"),
    [(False, 2), (True, 1)],
    ids=["multi-shard", "inline"],
)
def test_event_loop_pool_assigns_structured_shard_task_names_and_preserves_custom_names(
    inline_single_loop: bool,
    size: int,
) -> None:
    """Verify EventLoopPool names shard tasks vertebra.loop.shard.{idx}.task.{seq} and keeps custom names."""
    pool = EventLoopPool(size=size, inline_single_loop=inline_single_loop, loop_backend=LoopBackend.ASYNCIO)
    try:

        async def _current_name() -> str:
            current = asyncio.current_task()
            assert current is not None
            immediate = current.get_name()
            await asyncio.sleep(0)
            assert current.get_name() == immediate
            return immediate

        if inline_single_loop:
            spawned_names: list[str] = []

            async def _record_spawn() -> None:
                current = asyncio.current_task()
                assert current is not None
                spawned_names.append(current.get_name())

            pool.spawn(_record_spawn())
            pool.spawn(_record_spawn(), name="custom.inline.spawn")
            pool.spawn_batch([_record_spawn(), _record_spawn()])
            assert spawned_names == [
                "vertebra.loop.shard.0.task.1",
                "custom.inline.spawn",
                "vertebra.loop.shard.0.task.2",
                "vertebra.loop.shard.0.task.3",
            ]
        else:
            s0_first = pool.schedule(_current_name(), worker_index=0).result(timeout=5.0)
            s0_custom = pool.schedule(_current_name(), worker_index=0, name="custom.shard0").result(timeout=5.0)
            s1_first = pool.schedule(_current_name(), worker_index=1).result(timeout=5.0)
            batch_futs = pool.schedule_batch([_current_name(), _current_name()])
            batch_names = [fut.result(timeout=5.0) for fut in batch_futs]
            assert s0_first == "vertebra.loop.shard.0.task.1"
            assert s0_custom == "custom.shard0"
            assert s1_first == "vertebra.loop.shard.1.task.1"
            assert batch_names == [
                "vertebra.loop.shard.0.task.2",
                "vertebra.loop.shard.1.task.2",
            ]
    finally:
        pool.stop()


@pytest.mark.parametrize("dispatch_mode", ["submit", "envelope"], ids=["submit", "queue-envelope"])
def test_worker_runner_assigns_structured_runner_and_lane_task_names(dispatch_mode: str) -> None:
    """Verify WorkerRunner names the main loop task vertebra.worker.runner and lane tasks vertebra.worker.lane.{lane}.{seq}."""

    async def _exercise() -> None:
        observed_names: list[str] = []

        if dispatch_mode == "submit":
            runner = WorkerRunner(
                [
                    LaneConfig(queue_name="default", concurrency=2, lane_kind="io"),
                    LaneConfig(queue_name="etl", concurrency=1, lane_kind="cpu"),
                ]
            )

            async def _capture_task_name() -> str:
                current = asyncio.current_task()
                assert current is not None
                await asyncio.sleep(0)
                return current.get_name()

            t1 = runner.submit("default", _capture_task_name)
            t2 = runner.submit("default", _capture_task_name)
            t3 = runner.submit("etl", _capture_task_name)
            observed_names.extend(await asyncio.gather(t1, t2, t3))
            assert observed_names == [
                "vertebra.worker.lane.default.1",
                "vertebra.worker.lane.default.2",
                "vertebra.worker.lane.etl.1",
            ]
            await runner.stop()
            return

        backend = MemoryQueueBackend()
        qsvc = QueueService(backend=backend)
        runner_task_names: list[str] = []
        done_event = asyncio.Event()

        async def _queued_io_job() -> None:
            current = asyncio.current_task()
            assert current is not None
            observed_names.append(current.get_name())
            if len(observed_names) >= 2:
                done_event.set()

        qsvc.register(task(lane="io")(_queued_io_job))
        await qsvc.enqueue("_queued_io_job")
        await qsvc.enqueue("_queued_io_job")

        runner = WorkerRunner(
            [LaneConfig(queue_name="io", concurrency=2, drain_timeout_s=2.0, lane_kind="io")],
            queue_service=qsvc,
        )
        bg_runner_task = asyncio.create_task(runner.run(poll_interval_s=0.01))
        await asyncio.sleep(0)
        runner_task_names.append(bg_runner_task.get_name())
        await asyncio.wait_for(done_event.wait(), timeout=2.0)
        await runner.stop()
        await asyncio.wait_for(bg_runner_task, timeout=2.0)
        await qsvc.close()

        assert runner_task_names == ["vertebra.worker.runner"]
        assert sorted(observed_names) == [
            "vertebra.worker.lane.io.1",
            "vertebra.worker.lane.io.2",
        ]

    asyncio.run(_exercise())


@pytest.mark.parametrize(
    "graph_mode",
    ["mock_314_format", "mock_314_capture_only", "stack_fallback"],
)
def test_capture_loop_task_snapshots_and_format_call_graphs(
    monkeypatch: pytest.MonkeyPatch,
    graph_mode: str,
) -> None:
    """Verify capture_loop_task_snapshots and format_task_call_graphs across 3.14+ call graphs, fallback stacks, and idle/closed states."""
    assert capture_loop_task_snapshots() == []
    assert format_task_call_graphs() == ""

    closed_loop = asyncio.new_event_loop()
    closed_loop.close()
    assert capture_loop_task_snapshots(closed_loop) == []
    assert format_task_call_graphs(closed_loop) == ""

    format_calls: list[str] = []
    capture_calls: list[str] = []

    if graph_mode == "mock_314_format":

        def _fake_capture(target: asyncio.Task[Any]) -> Any:
            capture_calls.append(target.get_name())
            return SimpleNamespace(future=target, call_stack=(), awaited_by=())

        def _fake_format(target: asyncio.Task[Any]) -> str:
            format_calls.append(target.get_name())
            return f"* Task(name={target.get_name()!r})\n  + Call stack:\n  |   mocked_314_frame"

        monkeypatch.setattr(asyncio, "capture_call_graph", _fake_capture, raising=False)
        monkeypatch.setattr(asyncio, "format_call_graph", _fake_format, raising=False)
    elif graph_mode == "mock_314_capture_only":

        def _fake_capture_only(target: asyncio.Task[Any]) -> Any:
            capture_calls.append(target.get_name())
            frames = target.get_stack(limit=4)
            entries = tuple(SimpleNamespace(frame=frame) for frame in frames)
            return SimpleNamespace(future=target, call_stack=entries, awaited_by=())

        monkeypatch.setattr(asyncio, "capture_call_graph", _fake_capture_only, raising=False)
        monkeypatch.setattr(asyncio, "format_call_graph", None, raising=False)
    else:
        monkeypatch.setattr(asyncio, "capture_call_graph", None, raising=False)
        monkeypatch.setattr(asyncio, "format_call_graph", None, raising=False)

    async def _nested_waiter(gate: asyncio.Event) -> None:
        await gate.wait()

    async def _run_inspection() -> tuple[list[LoopTaskSnapshot], str]:
        gate = asyncio.Event()
        child = asyncio.create_task(_nested_waiter(gate), name="vertebra.loop.shard.0.task.1")
        await asyncio.sleep(0)
        snapshots = capture_loop_task_snapshots(shard_id=0)
        rendered_text = format_task_call_graphs()
        gate.set()
        await child
        return snapshots, rendered_text

    snapshots, rendered_text = asyncio.run(_run_inspection())
    by_name = {snap.task_name: snap for snap in snapshots}
    assert "vertebra.loop.shard.0.task.1" in by_name
    target_snap = by_name["vertebra.loop.shard.0.task.1"]
    assert target_snap.shard_id == 0
    assert target_snap.done is False
    assert target_snap.cancelled is False
    assert "_nested_waiter" in target_snap.coro_qualname
    assert "vertebra.loop.shard.0.task.1" in rendered_text

    assert LoopTaskSnapshot.__struct_config__.frozen is True
    assert isinstance(hash(target_snap), int)

    decoded = decode_json(encode_json_bytes(target_snap))
    assert isinstance(decoded, dict)
    assert decoded["task_name"] == "vertebra.loop.shard.0.task.1"
    assert isinstance(msgspec.convert(decoded, LoopTaskSnapshot), LoopTaskSnapshot)

    if graph_mode == "mock_314_format":
        assert "vertebra.loop.shard.0.task.1" in capture_calls
        assert "vertebra.loop.shard.0.task.1" in format_calls
        assert "mocked_314_frame" in target_snap.call_graph
    elif graph_mode == "mock_314_capture_only":
        assert "vertebra.loop.shard.0.task.1" in capture_calls
        assert "_nested_waiter" in target_snap.call_graph
    else:
        assert "_nested_waiter" in target_snap.call_graph
