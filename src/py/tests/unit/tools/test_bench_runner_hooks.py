from __future__ import annotations

import json
import shutil
import socket
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from tools.benchmarks.runner import (
    BrokerHookConfig,
    BrokerHostHook,
    OhaRunner,
    ServerLifecycleHook,
    ServerRunner,
)

SAMPLE_OHA_JSON = json.dumps(
    {
        "summary": {
            "successRate": 1.0,
            "total": 2.0,
            "average": 0.0025,
            "slowest": 0.015,
        },
        "latencyPercentiles": {
            "p50": 0.002,
            "p95": 0.006,
            "p99": 0.010,
            "p99.9": 0.014,
        },
        "rps": {"mean": 2000.0},
        "statusCodeDistribution": {"200": 4000},
        "errorDistribution": {},
    }
)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class _RecordingHook:
    """Fake lifecycle hook recording pre_start/post_stop events."""

    def __init__(
        self,
        hook_id: int,
        events: list[tuple[str, int]],
        env_vars: dict[str, str] | None = None,
        *,
        fail_pre_start: Exception | None = None,
        fail_post_stop: Exception | None = None,
    ) -> None:
        self.hook_id = hook_id
        self.events = events
        self.env_vars = env_vars or {}
        self.fail_pre_start = fail_pre_start
        self.fail_post_stop = fail_post_stop
        self.post_stop_calls = 0

    def pre_start(self) -> dict[str, str]:
        self.events.append(("pre_start", self.hook_id))
        if self.fail_pre_start is not None:
            raise self.fail_pre_start
        return dict(self.env_vars)

    def post_stop(self) -> None:
        self.post_stop_calls += 1
        self.events.append(("post_stop", self.hook_id))
        if self.fail_post_stop is not None:
            raise self.fail_post_stop


class _FakeBrokerHost:
    """Offline fake BrokerHost creating SHM and socket files on start."""

    def __init__(
        self,
        config: BrokerHookConfig,
        *,
        fail_on_start: Exception | None = None,
        fail_on_stop: Exception | None = None,
    ) -> None:
        self.config = config
        self.fail_on_start = fail_on_start
        self.fail_on_stop = fail_on_stop
        self.start_calls = 0
        self.stop_calls = 0

    def start(self) -> None:
        self.start_calls += 1
        shm_path = Path(self.config.shm_dir) / self.config.shm_name
        shm_path.write_bytes(b"shm-segment")
        if self.config.socket_path is not None:
            Path(self.config.socket_path).write_bytes(b"uds-socket")
        if self.fail_on_start is not None:
            raise self.fail_on_start

    def stop(self) -> None:
        self.stop_calls += 1
        if self.fail_on_stop is not None:
            raise self.fail_on_stop


@pytest.mark.parametrize(
    ("rate", "expected_mode", "expect_q_flag", "expect_latency_correction"),
    [
        (2000.0, "open_loop", True, False),
        (None, "closed_loop", False, True),
    ],
)
def test_oha_runner_passes_open_loop_rate_to_driver(
    monkeypatch: pytest.MonkeyPatch,
    rate: float | None,
    expected_mode: str,
    expect_q_flag: bool,
    expect_latency_correction: bool,
) -> None:
    """Verify OhaRunner delegates to OhaDriver with open-loop rate or closed-loop latency correction."""
    captured: dict[str, Any] = {}

    def fake_which(binary: str) -> str:
        return f"/usr/bin/{binary}"

    def fake_run(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        captured["cmd"] = cmd
        captured["kwargs"] = kwargs
        return subprocess.CompletedProcess(args=cmd, returncode=0, stdout=SAMPLE_OHA_JSON, stderr="")

    monkeypatch.setattr(shutil, "which", fake_which)
    monkeypatch.setattr(subprocess, "run", fake_run)

    runner = OhaRunner(
        "http://127.0.0.1:8000/plaintext",
        duration=2,
        concurrency=32,
        method="GET",
        rate=rate,
    )
    metrics = runner.run()

    cmd = captured["cmd"]
    assert cmd[0] == "oha"
    assert ("-q" in cmd) is expect_q_flag
    if rate is not None:
        q_idx = cmd.index("-q")
        assert cmd[q_idx + 1] == str(rate)
    assert ("--latency-correction" in cmd) is expect_latency_correction
    assert metrics["load_mode"] == expected_mode
    assert metrics["target_rps"] == rate
    assert metrics["total_requests"] == 4000
    assert metrics["errors"] == 0
    assert metrics["rps"] == pytest.approx(2000.0)


def test_server_runner_hook_order_and_env_precedence(tmp_path: Path) -> None:
    """Verify ServerRunner runs pre_start in order, applies self.env over hook env, and stops in reverse."""
    port = _free_port()
    env_dump = tmp_path / "child_env.json"
    child_code = (
        "import json, os, socket, time; "
        f"open({str(env_dump)!r}, 'w').write(json.dumps({{"
        "'FROM_H1': os.environ.get('FROM_H1'), "
        "'FROM_H2': os.environ.get('FROM_H2'), "
        "'SHARED': os.environ.get('SHARED'), "
        "'OVERRIDE': os.environ.get('OVERRIDE')"
        "})); "
        "s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1); "
        f"s.bind(('127.0.0.1', {port})); s.listen(5); "
        "time.sleep(30)"
    )

    events: list[tuple[str, int]] = []
    hook1 = _RecordingHook(1, events, {"FROM_H1": "one", "SHARED": "h1", "OVERRIDE": "from_h1"})
    hook2 = _RecordingHook(2, events, {"FROM_H2": "two", "SHARED": "h2", "OVERRIDE": "from_h2"})
    assert isinstance(hook1, ServerLifecycleHook)
    assert isinstance(hook2, ServerLifecycleHook)

    runner = ServerRunner(
        [sys.executable, "-c", child_code],
        host="127.0.0.1",
        port=port,
        env={"OVERRIDE": "from_runner_env"},
        hooks=[hook1, hook2],
    )
    with runner:
        assert events == [("pre_start", 1), ("pre_start", 2)]

    assert events == [
        ("pre_start", 1),
        ("pre_start", 2),
        ("post_stop", 2),
        ("post_stop", 1),
    ]
    child_env = json.loads(env_dump.read_text(encoding="utf-8"))
    assert child_env == {
        "FROM_H1": "one",
        "FROM_H2": "two",
        "SHARED": "h2",
        "OVERRIDE": "from_runner_env",
    }

    runner.stop()
    assert hook1.post_stop_calls == 1
    assert hook2.post_stop_calls == 1


def test_server_runner_tears_down_hooks_on_startup_failure() -> None:
    """Verify ServerRunner tears down started hooks exactly once when child exits early."""
    events: list[tuple[str, int]] = []
    hook1 = _RecordingHook(1, events, {"K1": "V1"})
    hook2 = _RecordingHook(2, events, {"K2": "V2"})
    runner = ServerRunner(
        [sys.executable, "-c", "import sys; sys.exit(2)"],
        host="127.0.0.1",
        port=_free_port(),
        hooks=[hook1, hook2],
    )
    try:
        with pytest.raises(RuntimeError, match="Server process exited with code 2"):
            runner.start()
    finally:
        runner.stop()

    assert events == [
        ("pre_start", 1),
        ("pre_start", 2),
        ("post_stop", 2),
        ("post_stop", 1),
    ]
    assert hook1.post_stop_calls == 1
    assert hook2.post_stop_calls == 1


def test_server_runner_tears_down_first_hook_when_second_hook_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify failing second hook tears down first hook and never spawns subprocess.Popen."""
    popen_called = False

    def fail_if_popen(*args: Any, **kwargs: Any) -> Any:
        nonlocal popen_called
        popen_called = True
        msg = "subprocess.Popen should not be called when pre_start fails"
        raise AssertionError(msg)

    monkeypatch.setattr(subprocess, "Popen", fail_if_popen)

    events: list[tuple[str, int]] = []
    hook1 = _RecordingHook(1, events, {"K1": "V1"})
    hook2 = _RecordingHook(2, events, fail_pre_start=RuntimeError("hook 2 boom"))

    runner = ServerRunner(
        [sys.executable, "-c", "pass"],
        host="127.0.0.1",
        port=_free_port(),
        hooks=[hook1, hook2],
    )
    with pytest.raises(RuntimeError, match="hook 2 boom"):
        runner.start()

    assert popen_called is False
    assert events == [("pre_start", 1), ("pre_start", 2), ("post_stop", 1)]
    assert hook1.post_stop_calls == 1
    assert hook2.post_stop_calls == 0


def test_server_runner_stop_runs_all_hooks_even_if_one_raises() -> None:
    """Verify ServerRunner continues tearing down remaining hooks when one post_stop raises."""
    events: list[tuple[str, int]] = []
    hook1 = _RecordingHook(1, events)
    hook2 = _RecordingHook(2, events, fail_post_stop=RuntimeError("teardown error"))

    runner = ServerRunner(
        [sys.executable, "-c", "import sys; sys.exit(4)"],
        host="127.0.0.1",
        port=_free_port(),
        hooks=[hook1, hook2],
    )
    with pytest.raises(RuntimeError, match="Server process exited with code 4"):
        runner.start()

    assert events == [
        ("pre_start", 1),
        ("pre_start", 2),
        ("post_stop", 2),
        ("post_stop", 1),
    ]
    assert hook1.post_stop_calls == 1
    assert hook2.post_stop_calls == 1


def test_broker_host_hook_env_and_shm_socket_cleanup(tmp_path: Path) -> None:
    """Verify BrokerHostHook starts host, returns VERTEBRA_BROKER_* env, and cleans up files idempotently."""
    socket_file = tmp_path / "broker.sock"
    config = BrokerHookConfig(
        shm_name="vertebra_bench_test_shm",
        pool_size=40,
        max_overflow=0,
        lease_ttl_ms=30_000,
        socket_path=str(socket_file),
        attach_secret="test-token-placeholder",
        shm_dir=str(tmp_path),
    )
    created_hosts: list[_FakeBrokerHost] = []

    def factory(cfg: BrokerHookConfig) -> _FakeBrokerHost:
        host = _FakeBrokerHost(cfg)
        created_hosts.append(host)
        return host

    hook = BrokerHostHook(config, host_factory=factory)
    assert isinstance(hook, ServerLifecycleHook)

    env = hook.pre_start()
    shm_file = tmp_path / config.shm_name
    assert len(created_hosts) == 1
    assert created_hosts[0].start_calls == 1
    assert shm_file.exists()
    assert socket_file.exists()
    assert env == {
        "VERTEBRA_BROKER_SHM": "vertebra_bench_test_shm",
        "VERTEBRA_BROKER_POOL_SIZE": "40",
        "VERTEBRA_BROKER_SOCKET": str(socket_file),
        "VERTEBRA_BROKER_SECRET": "test-token-placeholder",
    }

    hook.post_stop()
    assert created_hosts[0].stop_calls == 1
    assert not shm_file.exists()
    assert not socket_file.exists()

    hook.post_stop()
    assert created_hosts[0].stop_calls == 1


@pytest.mark.parametrize("bad_secret", [None, ""])
def test_broker_host_hook_rejects_missing_attach_secret(tmp_path: Path, bad_secret: str | None) -> None:
    """Verify BrokerHostHook raises ValueError when socket_path is configured without attach_secret."""
    config = BrokerHookConfig(
        shm_name="vertebra_bench_invalid_secret",
        socket_path=str(tmp_path / "broker.sock"),
        attach_secret=bad_secret,
        shm_dir=str(tmp_path),
    )
    hook = BrokerHostHook(config, host_factory=_FakeBrokerHost)
    with pytest.raises(ValueError, match="attach_secret"):
        hook.pre_start()


def test_broker_host_hook_cleans_up_when_start_or_stop_raises(tmp_path: Path) -> None:
    """Verify BrokerHostHook cleans up SHM and socket files even when host.start() or host.stop() raises."""
    socket_start_fail = tmp_path / "start_fail.sock"
    start_fail_cfg = BrokerHookConfig(
        shm_name="vertebra_bench_start_fail",
        socket_path=str(socket_start_fail),
        attach_secret="test-token-placeholder",
        shm_dir=str(tmp_path),
    )
    start_fail_hosts: list[_FakeBrokerHost] = []

    def start_fail_factory(cfg: BrokerHookConfig) -> _FakeBrokerHost:
        host = _FakeBrokerHost(cfg, fail_on_start=RuntimeError("bind failed"))
        start_fail_hosts.append(host)
        return host

    start_fail_hook = BrokerHostHook(start_fail_cfg, host_factory=start_fail_factory)
    with pytest.raises(RuntimeError, match="bind failed"):
        start_fail_hook.pre_start()

    assert start_fail_hosts[0].stop_calls == 1
    assert not (tmp_path / start_fail_cfg.shm_name).exists()
    assert not socket_start_fail.exists()

    socket_stop_fail = tmp_path / "stop_fail.sock"
    stop_fail_cfg = BrokerHookConfig(
        shm_name="vertebra_bench_stop_fail",
        socket_path=str(socket_stop_fail),
        attach_secret="test-token-placeholder",
        shm_dir=str(tmp_path),
    )
    stop_fail_hook = BrokerHostHook(
        stop_fail_cfg,
        host_factory=lambda cfg: _FakeBrokerHost(cfg, fail_on_stop=RuntimeError("stop failed")),
    )
    stop_fail_hook.pre_start()
    assert (tmp_path / stop_fail_cfg.shm_name).exists()
    assert socket_stop_fail.exists()

    with pytest.raises(RuntimeError, match="stop failed"):
        stop_fail_hook.post_stop()

    assert not (tmp_path / stop_fail_cfg.shm_name).exists()
    assert not socket_stop_fail.exists()
