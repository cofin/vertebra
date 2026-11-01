"""Unit tests for co-located WorkerRunner in vtb run / vtb dev and vtb workers run --oneshot."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from vertebra.cli import cli
from vertebra.cli import dev as cli_dev_mod
from vertebra.cli import run as cli_run_mod
from vertebra.cli._launch import ServerLaunchEnv, load_workers_pyproject_config
from vertebra.server.lanes import WorkerRunner


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
    from vertebra.server.worker import Worker

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
