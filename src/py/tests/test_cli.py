"""CLI smoke tests for Ch3 project buildout."""

from __future__ import annotations

import pytest
from click.testing import CliRunner

from vertebra.cli import cli


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def test_cli_help_shows_commands(runner: CliRunner) -> None:
    """vertebra --help must list run, build, init."""
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0, result.output
    assert "run" in result.output
    assert "build" in result.output
    assert "init" in result.output


def test_cli_version(runner: CliRunner) -> None:
    """vertebra --version must print a version string."""
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0, result.output
    assert any(ch.isdigit() for ch in result.output)


def test_run_help_shows_options(runner: CliRunner) -> None:
    """vertebra run --help must show key options."""
    result = runner.invoke(cli, ["run", "--help"])
    assert result.exit_code == 0, result.output
    for option in ["--host", "--port", "--workers", "--asgi", "--no-banner"]:
        assert option in result.output, f"Expected {option!r} in help output"
