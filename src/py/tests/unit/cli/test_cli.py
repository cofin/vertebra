"""CLI smoke tests."""

from __future__ import annotations

import importlib
from typing import Any

import pytest
from click.testing import CliRunner

from vertebra.cli import cli


@pytest.fixture
def runner() -> CliRunner:
    """Return a Click CLI test runner."""
    return CliRunner()


def test_cli_help_shows_commands(runner: CliRunner) -> None:
    """Verify ``vtb --help`` lists ``run``, ``dev``, ``init``, and ``js``."""
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0, result.output
    assert "run" in result.output
    assert "dev" in result.output
    assert "init" in result.output
    assert "js" in result.output


def test_cli_version(runner: CliRunner) -> None:
    """Verify ``vtb --version`` prints a version string."""
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0, result.output
    assert any(ch.isdigit() for ch in result.output)


@pytest.mark.parametrize(
    "option",
    ["--host", "--port", "--workers", "--asgi", "--no-banner"],
    ids=["host", "port", "workers", "asgi", "no-banner"],
)
def test_run_help_shows_options(runner: CliRunner, option: str) -> None:
    """Verify ``vtb run --help`` displays key options."""
    result = runner.invoke(cli, ["run", "--help"])
    assert result.exit_code == 0, result.output
    assert option in result.output, f"Expected {option!r} in help output"


@pytest.mark.parametrize(
    "option",
    ["--host", "--port", "--no-reload", "--no-banner", "--no-tui", "--verbose"],
    ids=["host", "port", "no-reload", "no-banner", "no-tui", "verbose"],
)
def test_dev_help_shows_options(runner: CliRunner, option: str) -> None:
    """Verify ``vtb dev --help`` displays dev-specific options."""
    result = runner.invoke(cli, ["dev", "--help"])
    assert result.exit_code == 0, result.output
    assert option in result.output, f"Expected {option!r} in help output"


def test_run_sets_topology_env(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify ``vtb run`` sets ``VERTEBRA_TOPOLOGY`` and resolves workers."""
    run_module = importlib.import_module("vertebra.cli.run")
    launch_module = importlib.import_module("vertebra.cli._launch")
    captured: dict[str, Any] = {}

    def fake_build_server_command() -> list[str]:
        return ["vertebra-server"]

    def fake_run_supervisor(**kwargs: Any) -> int:
        captured["env"] = kwargs["env"]
        return 0

    monkeypatch.setattr(run_module, "build_server_command", fake_build_server_command)
    monkeypatch.setattr(run_module, "run_supervisor", fake_run_supervisor)
    monkeypatch.setattr(run_module, "render_banner", lambda **kwargs: None)
    monkeypatch.setattr(launch_module, "is_free_threaded_runtime", lambda: False)

    runner.invoke(cli, ["run", "sample_app:app", "--no-banner"])
    assert captured["env"]["VERTEBRA_TOPOLOGY"] == "embedded"
    assert captured["env"]["VERTEBRA_WORKERS"] == "0"

    runner.invoke(cli, ["run", "sample_app:app", "--topology", "shm", "--no-banner"])
    assert captured["env"]["VERTEBRA_TOPOLOGY"] == "shm"
    assert captured["env"]["VERTEBRA_WORKERS"] == "1"

    runner.invoke(cli, ["run", "sample_app:app", "--workers", "4", "--no-banner"])
    assert captured["env"]["VERTEBRA_TOPOLOGY"] == "shm"
    assert captured["env"]["VERTEBRA_WORKERS"] == "4"

    runner.invoke(cli, ["run", "sample_app:app", "--workers", "4", "--topology", "embedded", "--no-banner"])
    assert captured["env"]["VERTEBRA_TOPOLOGY"] == "embedded"
    assert captured["env"]["VERTEBRA_WORKERS"] == "4"

    monkeypatch.setattr(launch_module, "is_free_threaded_runtime", lambda: True)
    runner.invoke(cli, ["run", "sample_app:app", "--workers", "4", "--no-banner"])
    assert captured["env"]["VERTEBRA_TOPOLOGY"] == "embedded"
    assert captured["env"]["VERTEBRA_WORKERS"] == "4"

    runner.invoke(cli, ["run", "sample_app:app", "--threads", "8", "--no-banner"])
    assert captured["env"]["VERTEBRA_THREADS"] == "8"


def test_dev_sets_vertebra_dev_env(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify ``vtb dev`` sets ``VERTEBRA_DEV=1`` and starts with a single worker."""
    dev_module = importlib.import_module("vertebra.cli.dev")
    captured: dict[str, Any] = {}

    def fake_build_server_command() -> list[str]:
        return ["vertebra-server"]

    def fake_run_supervisor(**kwargs: Any) -> int:
        captured["env"] = kwargs["env"]
        return 0

    monkeypatch.setattr(dev_module, "build_server_command", fake_build_server_command)
    monkeypatch.setattr(dev_module, "run_supervisor", fake_run_supervisor)
    monkeypatch.setattr(dev_module, "render_banner", lambda **kwargs: None)
    monkeypatch.setattr(dev_module, "_start_frontend_dev", lambda *args, **kwargs: (None, None))

    result = runner.invoke(cli, ["dev", "sample_app:app", "--no-banner"])

    assert result.exit_code == 0, result.output
    assert captured["env"]["VERTEBRA_DEV"] == "1"
    assert captured["env"]["VERTEBRA_WORKERS"] == "1"
