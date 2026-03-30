"""CLI smoke tests."""

from typing import Any

import pytest
from click.testing import CliRunner

from vertebra.cli import cli


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def test_cli_help_shows_commands(runner: CliRunner) -> None:
    """vtb --help must list run, dev, init, js."""
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0, result.output
    assert "run" in result.output
    assert "dev" in result.output
    assert "init" in result.output
    assert "js" in result.output


def test_cli_version(runner: CliRunner) -> None:
    """vtb --version must print a version string."""
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0, result.output
    assert any(ch.isdigit() for ch in result.output)


def test_run_help_shows_options(runner: CliRunner) -> None:
    """vtb run --help must show key options."""
    result = runner.invoke(cli, ["run", "--help"])
    assert result.exit_code == 0, result.output
    for option in ["--host", "--port", "--workers", "--asgi", "--no-banner"]:
        assert option in result.output, f"Expected {option!r} in help output"


def test_dev_help_shows_options(runner: CliRunner) -> None:
    """vtb dev --help must show dev-specific options."""
    result = runner.invoke(cli, ["dev", "--help"])
    assert result.exit_code == 0, result.output
    for option in ["--host", "--port", "--no-reload", "--no-banner"]:
        assert option in result.output, f"Expected {option!r} in help output"


def test_run_sets_topology_env(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    """vtb run should set VERTEBRA_TOPOLOGY and resolve workers."""
    import importlib

    run_module = importlib.import_module("vertebra.cli.run")

    captured: dict[str, Any] = {}

    def fake_build_server_command() -> list[str]:
        return ["vertebra-server"]

    def fake_run_supervisor(**kwargs: Any) -> int:
        captured["env"] = kwargs["env"]
        return 0

    monkeypatch.setattr(run_module, "build_server_command", fake_build_server_command)
    monkeypatch.setattr(run_module, "run_supervisor", fake_run_supervisor)
    monkeypatch.setattr(run_module, "render_banner", lambda **kwargs: None)

    # 1. Default (embedded)
    runner.invoke(cli, ["run", "sample_app:app", "--no-banner"])
    assert captured["env"]["VERTEBRA_TOPOLOGY"] == "embedded"
    assert captured["env"]["VERTEBRA_WORKERS"] == "0"

    # 2. Explicit SHM
    runner.invoke(cli, ["run", "sample_app:app", "--topology", "shm", "--no-banner"])
    assert captured["env"]["VERTEBRA_TOPOLOGY"] == "shm"
    assert captured["env"]["VERTEBRA_WORKERS"] == "1"

    # 3. Explicit workers with embedded
    runner.invoke(cli, ["run", "sample_app:app", "--workers", "4", "--no-banner"])
    assert captured["env"]["VERTEBRA_TOPOLOGY"] == "embedded"
    assert captured["env"]["VERTEBRA_WORKERS"] == "4"

    # 4. Explicit threads
    runner.invoke(cli, ["run", "sample_app:app", "--threads", "8", "--no-banner"])
    assert captured["env"]["VERTEBRA_THREADS"] == "8"


def test_dev_sets_vertebra_dev_env(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    """vtb dev should set VERTEBRA_DEV=1 and start with single worker."""
    import importlib

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
    # Prevent actual frontend thread from starting
    monkeypatch.setattr(dev_module, "_start_frontend_dev", lambda *args, **kwargs: None)

    result = runner.invoke(cli, ["dev", "sample_app:app", "--no-banner"])

    assert result.exit_code == 0, result.output
    assert captured["env"]["VERTEBRA_DEV"] == "1"
    assert captured["env"]["VERTEBRA_WORKERS"] == "1"
