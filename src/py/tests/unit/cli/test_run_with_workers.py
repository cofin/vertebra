"""Unit tests for co-located WorkerRunner in vtb run / vtb dev and vtb workers run --oneshot."""

from __future__ import annotations

import asyncio
import socket
import threading
import time
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from vertebra.cli import cli
from vertebra.cli import dev as cli_dev_mod
from vertebra.cli import run as cli_run_mod
from vertebra.cli import workers as cli_workers_mod
from vertebra.cli._launch import ServerLaunchEnv, load_workers_pyproject_config
from vertebra.cli.errors import EXIT_CONFIG_ERROR
from vertebra.framework.queues import QueueService, task
from vertebra.server.lanes import QUEUE_SERVICE_STATE_KEY, WorkerRunner
from vertebra.server.platform import HealthProbeServer, LameDuckController, ZPagesRegistry
from vertebra.server.worker import Worker
from vertebra.telemetry.otel import EventLoopRecorder
from vertebra.utils.serializers import decode_json


@pytest.fixture
def runner() -> CliRunner:
    """Return a Click CLI test runner."""
    return CliRunner()


def test_vtb_run_with_workers_cli_flags_populate_server_launch_env(
    runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify vtb run --with-workers --queues --lane-concurrency populates ServerLaunchEnv."""
    captured: dict[str, Any] = {}

    monkeypatch.setattr(cli_run_mod, "build_server_command", lambda: ["vertebra-server"])

    def fake_supervisor(*, server_cmd: list[str], env: dict[str, str], **_kwargs: Any) -> int:
        captured["cmd"] = server_cmd
        captured["env"] = dict(env)
        return 0

    monkeypatch.setattr(cli_run_mod, "run_supervisor", fake_supervisor)

    res = runner.invoke(
        cli,
        [
            "run",
            "sample_app:app",
            "--with-workers",
            "--queues",
            "default,embeddings",
            "--lane-concurrency",
            "default=4,embeddings=1",
            "--no-banner",
        ],
    )
    assert res.exit_code == 0, res.output
    assert captured["env"]["VERTEBRA_WITH_WORKERS"] == "1"
    assert captured["env"]["VERTEBRA_WORKER_QUEUES"] == "default,embeddings"
    assert captured["env"]["VERTEBRA_WORKER_LANE_CONCURRENCY"] == "default=4,embeddings=1"


def test_vtb_dev_with_workers_cli_flags_populate_server_launch_env(
    runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify vtb dev --with-workers --queues --lane-concurrency populates ServerLaunchEnv."""
    captured: dict[str, Any] = {}

    monkeypatch.setattr(cli_dev_mod, "build_server_command", lambda: ["vertebra-server"])
    monkeypatch.setattr(cli_dev_mod, "_start_frontend_dev", lambda *args, **kwargs: (None, None))

    def fake_supervisor(*, server_cmd: list[str], env: dict[str, str], **_kwargs: Any) -> int:
        captured["cmd"] = server_cmd
        captured["env"] = dict(env)
        return 0

    monkeypatch.setattr(cli_dev_mod, "run_supervisor", fake_supervisor)

    res = runner.invoke(
        cli,
        [
            "dev",
            "sample_app:app",
            "--with-workers",
            "--queues",
            "default,agent",
            "--lane-concurrency",
            "default=2,agent=1",
            "--no-banner",
        ],
    )
    assert res.exit_code == 0, res.output
    assert captured["env"]["VERTEBRA_WITH_WORKERS"] == "1"
    assert captured["env"]["VERTEBRA_WORKER_QUEUES"] == "default,agent"
    assert captured["env"]["VERTEBRA_WORKER_LANE_CONCURRENCY"] == "default=2,agent=1"


def test_pyproject_tool_vertebra_workers_config_resolution(
    runner: CliRunner,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify [tool.vertebra.workers] in pyproject.toml configures ServerLaunchEnv by default."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        """
[project]
name = "demo"

[tool.vertebra.workers]
enabled_in_run = true
queues = ["default", "embeddings"]
lanes = { default = 8, embeddings = 1 }
""".strip()
        + "\n",
        encoding="utf-8",
    )
    with_workers, queues, lane_map = load_workers_pyproject_config(tmp_path)
    assert with_workers is True
    assert queues == ("default", "embeddings")
    assert lane_map == {"default": 8, "embeddings": 1}

    launch_env = ServerLaunchEnv(
        app_path="demo:app",
        host="127.0.0.1",
        port=8000,
        with_workers=with_workers,
        worker_queues=queues,
        worker_lane_concurrency=lane_map,
    )
    env = launch_env.to_environ(base={})
    assert env["VERTEBRA_WITH_WORKERS"] == "1"
    assert env["VERTEBRA_WORKER_QUEUES"] == "default,embeddings"
    assert env["VERTEBRA_WORKER_LANE_CONCURRENCY"] == "default=8,embeddings=1"

    lanes = WorkerRunner.parse_lane_concurrency(
        env["VERTEBRA_WORKER_QUEUES"],
        env["VERTEBRA_WORKER_LANE_CONCURRENCY"],
    )
    by_name = {lane.queue_name: lane for lane in lanes}
    assert by_name["default"].concurrency == 8
    assert by_name["embeddings"].concurrency == 1

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli_run_mod, "build_server_command", lambda: ["vertebra-server"])
    captured: dict[str, Any] = {}
    monkeypatch.setattr(
        cli_run_mod,
        "run_supervisor",
        lambda *, env, **_kw: captured.update({"env": dict(env)}) or 0,
    )
    res = runner.invoke(cli, ["run", "demo:app", "--no-banner"])
    assert res.exit_code == 0, res.output
    assert captured["env"]["VERTEBRA_WITH_WORKERS"] == "1"
    assert captured["env"]["VERTEBRA_WORKER_QUEUES"] == "default,embeddings"
    assert captured["env"]["VERTEBRA_WORKER_LANE_CONCURRENCY"] == "default=8,embeddings=1"


def test_vtb_workers_run_oneshot_with_payload(
    runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify vtb workers run app:app --oneshot <job> --payload <json> executes job and exits 0."""
    received_payloads: list[Any] = []

    class DummyApp:
        def __init__(self) -> None:
            self.jobs = {
                "backfill_vectors": self._run_backfill,
            }
            self.state: dict[str, Any] = {}

        async def _run_backfill(self, payload: dict[str, Any] | None = None) -> None:
            received_payloads.append(payload)

    dummy_app = DummyApp()
    monkeypatch.setattr("vertebra.cli.workers.import_app", lambda _path: dummy_app)

    res = runner.invoke(
        cli,
        [
            "workers",
            "run",
            "app:app",
            "--oneshot",
            "backfill_vectors",
            "--payload",
            '{"batch":100}',
        ],
    )
    assert res.exit_code == 0, res.output
    assert received_payloads == [{"batch": 100}]


def test_worker_starts_and_drains_colocated_worker_runner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify Worker.run starts and drains a co-located WorkerRunner when VERTEBRA_WITH_WORKERS=1."""
    lifecycle_events: list[tuple[str, Any]] = []

    async def fake_run(self: WorkerRunner, **_kwargs: Any) -> None:
        lifecycle_events.append(("started", tuple(self.lanes.keys())))

    async def fake_stop(self: WorkerRunner) -> None:
        lifecycle_events.append(("stopped", self.is_stopping))

    monkeypatch.setattr(WorkerRunner, "run", fake_run)
    monkeypatch.setattr(WorkerRunner, "stop", fake_stop)
    monkeypatch.setenv("VERTEBRA_WITH_WORKERS", "1")
    monkeypatch.setenv("VERTEBRA_WORKER_QUEUES", "default,embeddings")
    monkeypatch.setenv("VERTEBRA_WORKER_LANE_CONCURRENCY", "default=4,embeddings=1")
    monkeypatch.setattr("vertebra.server.worker.get_request_notify_fd", lambda: -1)
    monkeypatch.setattr("vertebra.server.worker.get_batch", lambda _timeout: [object()])
    monkeypatch.setattr(Worker, "_process_batch", lambda *args, **kwargs: None)

    worker = Worker({}, max_batches=1)
    worker.run()

    assert ("started", ("default", "embeddings")) in lifecycle_events
    assert any(evt[0] == "stopped" for evt in lifecycle_events)


def test_worker_binds_resolved_queue_service_from_app_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Worker.run resolves QueueService from VERTEBRA_QUEUE_SERVICE state and executes queued tasks."""
    qs = QueueService()
    executed: list[int] = []
    task_done = threading.Event()

    @task(lane="io")
    async def process_item(item_id: int) -> int:
        executed.append(item_id)
        task_done.set()
        return item_id * 2

    qs.register(process_item)
    asyncio.run(qs.enqueue("process_item", 21))

    app_double = type("AppWithQueue", (), {"state": {QUEUE_SERVICE_STATE_KEY: qs}})()

    batches_seen = 0

    def wait_and_yield_one_batch(_timeout: int) -> list[object]:
        nonlocal batches_seen
        batches_seen += 1
        if batches_seen == 1:
            assert task_done.wait(timeout=2.0)
        return [object()]

    monkeypatch.setenv("VERTEBRA_WITH_WORKERS", "1")
    monkeypatch.setenv("VERTEBRA_WORKER_QUEUES", "io")
    monkeypatch.setenv("VERTEBRA_WORKER_LANE_CONCURRENCY", "io=1")
    monkeypatch.setattr("vertebra.server.worker.get_request_notify_fd", lambda: -1)
    monkeypatch.setattr("vertebra.server.worker.get_batch", wait_and_yield_one_batch)
    monkeypatch.setattr(Worker, "_process_batch", lambda *_args, **_kwargs: None)

    w = Worker({}, app_obj=app_double, max_batches=1)
    w.run()

    assert executed == [21]


def test_vtb_workers_run_continuous_mode_executes_queued_task_until_stopped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """execute_worker_cli with run_until_stopped=True processes queued work and stops cleanly."""
    qs = QueueService()
    done_event = threading.Event()
    async_waiters: list[tuple[asyncio.AbstractEventLoop, asyncio.Event]] = []
    executed: list[str] = []

    @task(lane="io")
    async def process_job(label: str) -> None:
        executed.append(label)
        done_event.set()
        for target_loop, target_event in async_waiters:
            target_loop.call_soon_threadsafe(target_event.set)

    qs.register(process_job)
    asyncio.run(qs.enqueue("process_job", "continuous"))

    app_double = type("WorkerApp", (), {"state": {QUEUE_SERVICE_STATE_KEY: qs}})()
    monkeypatch.setattr(cli_workers_mod, "import_app", lambda _path: app_double)

    orig_run = WorkerRunner.run

    async def stop_after_task(self: WorkerRunner, **kwargs: Any) -> None:
        async_done_event = asyncio.Event()
        async_waiters.append((asyncio.get_running_loop(), async_done_event))
        if done_event.is_set():
            async_done_event.set()

        async def _trigger_stop() -> None:
            await asyncio.wait_for(async_done_event.wait(), timeout=2.0)
            self._stopping = True

        stopper = asyncio.create_task(_trigger_stop())
        try:
            await orig_run(self, **kwargs)
        finally:
            await stopper

    monkeypatch.setattr(WorkerRunner, "run", stop_after_task)

    code = cli_workers_mod.execute_worker_cli(
        "worker_app:app",
        queues="io",
        concurrency="1",
        drain_timeout_s=2.0,
        run_until_stopped=True,
    )

    assert code == 0
    assert executed == ["continuous"]


@pytest.mark.parametrize(
    ("method", "path", "draining", "expected_status"),
    [
        ("GET", "/healthz", False, 200),
        ("GET", "/readyz", False, 200),
        ("GET", "/statusz", False, 200),
        ("GET", "/varz", False, 200),
        ("POST", "/healthz", False, 405),
        ("GET", "/missing", False, 404),
        ("GET", "/healthz", True, 503),
        ("GET", "/readyz", True, 503),
    ],
)
def test_health_probe_server_endpoints_and_lame_duck(
    method: str,
    path: str,
    draining: bool,
    expected_status: int,
) -> None:
    """HealthProbeServer responds with expected HTTP status codes across endpoints and drain mode."""
    zpages = ZPagesRegistry("vertebra-worker", lame_duck=LameDuckController())
    if draining:
        zpages.lame_duck.enter_lame_duck()

    async def _run_probe_check() -> None:
        probe = HealthProbeServer(zpages, host="127.0.0.1", port=0)
        await probe.start()
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", probe.bound_port)
            try:
                writer.write(
                    f"{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n".encode("ascii")
                )
                await writer.drain()
                raw = await reader.read()
            finally:
                writer.close()
                await writer.wait_closed()
            status_line = raw.split(b"\r\n", 1)[0].decode("ascii")
            assert f" {expected_status} " in status_line
            if expected_status == 405:
                assert b"Allow: GET\r\n" in raw
        finally:
            await probe.stop()

    asyncio.run(_run_probe_check())


def test_vtb_run_worker_only_serves_health_probe_and_exits_cleanly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """execute_worker_cli with health_probe serves HTTP /healthz until signalled."""
    qs = QueueService()
    app_state: dict[str, Any] = {QUEUE_SERVICE_STATE_KEY: qs}
    app_double = type("WorkerOnlyApp", (), {"state": app_state})()
    monkeypatch.setattr(cli_workers_mod, "import_app", lambda _path: app_double)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe_sock:
        probe_sock.bind(("127.0.0.1", 0))
        probe_port = probe_sock.getsockname()[1]

    probe_ready = threading.Event()
    orig_set_var = ZPagesRegistry.set_var

    def signal_probe_ready(self: ZPagesRegistry, key: str, value: Any) -> None:
        orig_set_var(self, key, value)
        if key == "worker_state" and value == "ready":
            probe_ready.set()

    monkeypatch.setattr(ZPagesRegistry, "set_var", signal_probe_ready)

    result_box: list[int] = []

    def run_worker_only_thread() -> None:
        code = cli_workers_mod.execute_worker_cli(
            "worker_only_app:app",
            queues="io",
            concurrency="1",
            drain_timeout_s=2.0,
            run_until_stopped=True,
            health_probe=("127.0.0.1", probe_port),
        )
        result_box.append(code)

    thread = threading.Thread(target=run_worker_only_thread, daemon=True)
    thread.start()
    try:
        assert probe_ready.wait(timeout=5.0)
        with socket.create_connection(("127.0.0.1", probe_port), timeout=2.0) as client:
            client.sendall(b"GET /healthz HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
            response_bytes = client.recv(4096)

        assert b"200 OK" in response_bytes
        runner = app_state.get("vertebra_worker_runner")
        assert runner is not None
        runner._stopping = True
        thread.join(timeout=3.0)
        assert not thread.is_alive()
    finally:
        runner = app_state.get("vertebra_worker_runner")
        if runner is not None:
            runner._stopping = True
        thread.join(timeout=1.0)

    assert result_box == [0]


@pytest.mark.parametrize(
    ("zpages", "zpages_token", "base", "expected_enabled", "expected_token"),
    [
        (False, None, {}, None, None),
        (True, " test-token-placeholder ", {}, "1", "test-token-placeholder"),
        (True, "   ", {}, "1", None),
        (
            False,
            None,
            {"VERTEBRA_ZPAGES_ENABLED": "1", "VERTEBRA_ZPAGES_TOKEN": "test-token-placeholder"},
            "1",
            "test-token-placeholder",
        ),
    ],
)
def test_server_launch_env_zpages_options(
    zpages: bool,
    zpages_token: str | None,
    base: dict[str, str],
    expected_enabled: str | None,
    expected_token: str | None,
) -> None:
    """ServerLaunchEnv emits VERTEBRA_ZPAGES_* only when configured and preserves base overrides."""
    launch_env = ServerLaunchEnv(
        app_path="app:app",
        host="127.0.0.1",
        port=8000,
        zpages=zpages,
        zpages_token=zpages_token,
    )
    env = launch_env.to_environ(base=base)
    if expected_enabled is None:
        assert "VERTEBRA_ZPAGES_ENABLED" not in env
    else:
        assert env["VERTEBRA_ZPAGES_ENABLED"] == expected_enabled
    if expected_token is None:
        assert "VERTEBRA_ZPAGES_TOKEN" not in env
    else:
        assert env["VERTEBRA_ZPAGES_TOKEN"] == expected_token


def test_vtb_run_zpages_cli_flags_populate_server_launch_env(
    runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify vtb run --zpages --zpages-token forwards Z-Pages env vars to the supervisor."""
    captured: dict[str, Any] = {}

    monkeypatch.setattr(cli_run_mod, "build_server_command", lambda: ["vertebra-server"])

    def fake_supervisor(*, server_cmd: list[str], env: dict[str, str], **_kwargs: Any) -> int:
        captured["cmd"] = server_cmd
        captured["env"] = dict(env)
        return 0

    monkeypatch.setattr(cli_run_mod, "run_supervisor", fake_supervisor)

    res = runner.invoke(
        cli,
        [
            "run",
            "sample_app:app",
            "--zpages",
            "--zpages-token",
            "test-token-placeholder",
            "--no-banner",
        ],
    )
    assert res.exit_code == 0, res.output
    assert captured["env"]["VERTEBRA_ZPAGES_ENABLED"] == "1"
    assert captured["env"]["VERTEBRA_ZPAGES_TOKEN"] == "test-token-placeholder"


def test_vtb_run_worker_only_rejects_zpages_token(runner: CliRunner) -> None:
    """Verify vtb run --worker-only --zpages-token fails closed with EXIT_CONFIG_ERROR."""
    res = runner.invoke(
        cli,
        [
            "run",
            "sample_app:app",
            "--worker-only",
            "--zpages-token",
            "test-token-placeholder",
        ],
    )
    assert res.exit_code == EXIT_CONFIG_ERROR, res.output
    assert "--zpages-token cannot be combined with --worker-only" in res.output


def _http_get_json(port: int, path: str) -> tuple[int, dict[str, Any]]:
    """Perform a simple HTTP/1.1 GET request against 127.0.0.1 and decode the JSON body."""
    with socket.create_connection(("127.0.0.1", port), timeout=2.0) as client:
        client.sendall(f"GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n".encode("ascii"))
        chunks: list[bytes] = []
        while True:
            chunk = client.recv(4096)
            if not chunk:
                break
            chunks.append(chunk)
    raw = b"".join(chunks)
    header_bytes, _, body_bytes = raw.partition(b"\r\n\r\n")
    status_line = header_bytes.split(b"\r\n", 1)[0].decode("ascii")
    status_code = int(status_line.split(" ", 2)[1])
    payload = decode_json(body_bytes)
    assert isinstance(payload, dict)
    return status_code, payload


def _allocate_ephemeral_port() -> int:
    """Bind an ephemeral localhost TCP port and return its port number."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.mark.parametrize("telemetry_env", ["1", "true"])
def test_vtb_run_worker_only_exports_loop_telemetry_when_enabled(
    monkeypatch: pytest.MonkeyPatch,
    telemetry_env: str,
) -> None:
    """execute_worker_cli samples loop lag and exports loop_telemetry on /varz when enabled."""
    monkeypatch.setenv("VERTEBRA_LOOP_TELEMETRY", telemetry_env)
    monkeypatch.setenv("VERTEBRA_LOOP_TELEMETRY_INTERVAL", "0.01")

    qs = QueueService()
    app_state: dict[str, Any] = {QUEUE_SERVICE_STATE_KEY: qs}
    app_double = type("WorkerOnlyTelemetryApp", (), {"state": app_state})()
    monkeypatch.setattr(cli_workers_mod, "import_app", lambda _path: app_double)

    probe_port = _allocate_ephemeral_port()
    probe_ready = threading.Event()
    orig_set_var = ZPagesRegistry.set_var

    def signal_probe_ready(self: ZPagesRegistry, key: str, value: Any) -> None:
        orig_set_var(self, key, value)
        if key == "worker_state" and value == "ready":
            probe_ready.set()

    monkeypatch.setattr(ZPagesRegistry, "set_var", signal_probe_ready)

    result_box: list[int] = []

    def run_worker_only_thread() -> None:
        code = cli_workers_mod.execute_worker_cli(
            "worker_only_telemetry_app:app",
            queues="io",
            concurrency="1",
            drain_timeout_s=2.0,
            run_until_stopped=True,
            health_probe=("127.0.0.1", probe_port),
        )
        result_box.append(code)

    thread = threading.Thread(target=run_worker_only_thread, daemon=True)
    thread.start()
    try:
        assert probe_ready.wait(timeout=5.0)
        deadline = time.monotonic() + 2.0
        payload: dict[str, Any] = {}
        while time.monotonic() < deadline:
            status_code, payload = _http_get_json(probe_port, "/varz")
            assert status_code == 200
            worker_telemetry = payload.get("loop_telemetry", {}).get("worker", {})
            if "0" in worker_telemetry.get("shards", {}):
                break
            time.sleep(0.01)

        assert "loop_telemetry" in payload
        worker_telemetry = payload["loop_telemetry"]["worker"]
        shard = worker_telemetry["shards"]["0"]
        assert shard["lag_ms"] >= 0.0
        assert shard["active_tasks"] >= 1
        assert worker_telemetry["lag_histogram"]["count"] >= 1
        assert isinstance(app_state.get("vertebra_loop_recorder"), EventLoopRecorder)

        runner = app_state.get("vertebra_worker_runner")
        assert runner is not None
        runner._stopping = True
        thread.join(timeout=3.0)
        assert not thread.is_alive()
    finally:
        runner = app_state.get("vertebra_worker_runner")
        if runner is not None:
            runner._stopping = True
        thread.join(timeout=1.0)

    assert result_box == [0]


@pytest.mark.parametrize("telemetry_env", [None, "0", "false"])
def test_vtb_run_worker_only_omits_loop_telemetry_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
    telemetry_env: str | None,
) -> None:
    """execute_worker_cli omits loop_telemetry from /varz and state when VERTEBRA_LOOP_TELEMETRY is off."""
    if telemetry_env is None:
        monkeypatch.delenv("VERTEBRA_LOOP_TELEMETRY", raising=False)
    else:
        monkeypatch.setenv("VERTEBRA_LOOP_TELEMETRY", telemetry_env)

    qs = QueueService()
    app_state: dict[str, Any] = {QUEUE_SERVICE_STATE_KEY: qs}
    app_double = type("WorkerOnlyNoTelemetryApp", (), {"state": app_state})()
    monkeypatch.setattr(cli_workers_mod, "import_app", lambda _path: app_double)

    probe_port = _allocate_ephemeral_port()
    probe_ready = threading.Event()
    orig_set_var = ZPagesRegistry.set_var

    def signal_probe_ready(self: ZPagesRegistry, key: str, value: Any) -> None:
        orig_set_var(self, key, value)
        if key == "worker_state" and value == "ready":
            probe_ready.set()

    monkeypatch.setattr(ZPagesRegistry, "set_var", signal_probe_ready)

    result_box: list[int] = []

    def run_worker_only_thread() -> None:
        code = cli_workers_mod.execute_worker_cli(
            "worker_only_no_telemetry_app:app",
            queues="io",
            concurrency="1",
            drain_timeout_s=2.0,
            run_until_stopped=True,
            health_probe=("127.0.0.1", probe_port),
        )
        result_box.append(code)

    thread = threading.Thread(target=run_worker_only_thread, daemon=True)
    thread.start()
    try:
        assert probe_ready.wait(timeout=5.0)
        status_code, payload = _http_get_json(probe_port, "/varz")
        assert status_code == 200
        assert "loop_telemetry" not in payload
        assert "vertebra_loop_recorder" not in app_state

        runner = app_state.get("vertebra_worker_runner")
        assert runner is not None
        runner._stopping = True
        thread.join(timeout=3.0)
        assert not thread.is_alive()
    finally:
        runner = app_state.get("vertebra_worker_runner")
        if runner is not None:
            runner._stopping = True
        thread.join(timeout=1.0)

    assert result_box == [0]


@pytest.mark.parametrize("raw_interval", ["invalid", "0", "-1", "nan", "inf"])
def test_vtb_run_worker_only_invalid_loop_telemetry_interval_falls_back(
    monkeypatch: pytest.MonkeyPatch,
    raw_interval: str,
) -> None:
    """Invalid VERTEBRA_LOOP_TELEMETRY_INTERVAL values fall back to 1.0s and still attach recorder."""
    monkeypatch.setenv("VERTEBRA_LOOP_TELEMETRY", "1")
    monkeypatch.setenv("VERTEBRA_LOOP_TELEMETRY_INTERVAL", raw_interval)

    qs = QueueService()
    app_state: dict[str, Any] = {QUEUE_SERVICE_STATE_KEY: qs}
    app_double = type("WorkerOnlyInvalidIntervalApp", (), {"state": app_state})()
    monkeypatch.setattr(cli_workers_mod, "import_app", lambda _path: app_double)

    probe_port = _allocate_ephemeral_port()
    probe_ready = threading.Event()
    observed_intervals: list[float] = []
    orig_interval = Worker._loop_telemetry_interval
    orig_set_var = ZPagesRegistry.set_var

    def capture_interval() -> float:
        resolved = orig_interval()
        observed_intervals.append(resolved)
        return resolved

    def signal_probe_ready(self: ZPagesRegistry, key: str, value: Any) -> None:
        orig_set_var(self, key, value)
        if key == "worker_state" and value == "ready":
            probe_ready.set()

    monkeypatch.setattr(Worker, "_loop_telemetry_interval", staticmethod(capture_interval))
    monkeypatch.setattr(ZPagesRegistry, "set_var", signal_probe_ready)

    result_box: list[int] = []

    def run_worker_only_thread() -> None:
        code = cli_workers_mod.execute_worker_cli(
            "worker_only_invalid_interval_app:app",
            queues="io",
            concurrency="1",
            drain_timeout_s=2.0,
            run_until_stopped=True,
            health_probe=("127.0.0.1", probe_port),
        )
        result_box.append(code)

    thread = threading.Thread(target=run_worker_only_thread, daemon=True)
    thread.start()
    try:
        assert probe_ready.wait(timeout=5.0)
        status_code, payload = _http_get_json(probe_port, "/varz")
        assert status_code == 200
        assert "loop_telemetry" in payload
        assert "worker" in payload["loop_telemetry"]
        assert isinstance(app_state.get("vertebra_loop_recorder"), EventLoopRecorder)
        assert observed_intervals == [1.0]

        runner = app_state.get("vertebra_worker_runner")
        assert runner is not None
        runner._stopping = True
        thread.join(timeout=3.0)
        assert not thread.is_alive()
    finally:
        runner = app_state.get("vertebra_worker_runner")
        if runner is not None:
            runner._stopping = True
        thread.join(timeout=1.0)

    assert result_box == [0]
