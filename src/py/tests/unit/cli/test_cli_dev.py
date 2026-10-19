"""Tests for the dev CLI command."""

from __future__ import annotations

import pytest
from click.testing import CliRunner

from vertebra.cli import cli


@pytest.fixture
def runner() -> CliRunner:
    """Return a Click CLI test runner."""
    return CliRunner()


def test_dev_command_exists(runner: CliRunner) -> None:
    """Verify the dev command is registered and shows help."""
    result = runner.invoke(cli, ["dev", "--help"])
    assert result.exit_code == 0, result.output
    assert "development mode" in result.output.lower()


def test_dev_command_has_expected_options(runner: CliRunner) -> None:
    """Verify the dev command exposes expected options."""
    result = runner.invoke(cli, ["dev", "--help"])
    assert result.exit_code == 0, result.output
    for option in ("--host", "--port", "--log-level", "--asgi", "--no-reload", "--no-banner", "--no-tui", "--verbose"):
        assert option in result.output, f"Expected {option!r} in dev --help output"


def test_run_command_has_no_dev_flag(runner: CliRunner) -> None:
    """Verify the run command does not have a ``--dev`` flag."""
    result = runner.invoke(cli, ["run", "--help"])
    assert result.exit_code == 0, result.output
    assert "--dev" not in result.output


def test_cli_help_lists_dev_and_js(runner: CliRunner) -> None:
    """Verify CLI help lists ``dev``, ``js``, ``run``, and ``init`` commands."""
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0, result.output
    assert "dev" in result.output
    assert "js" in result.output
    assert "run" in result.output
    assert "init" in result.output
