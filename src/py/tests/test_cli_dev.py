"""Tests for the dev CLI command."""

import pytest
from click.testing import CliRunner

from vertebra.cli import cli


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def test_dev_command_exists(runner: CliRunner) -> None:
    """Dev command should be registered and show help."""
    result = runner.invoke(cli, ["dev", "--help"])
    assert result.exit_code == 0, result.output
    assert "development mode" in result.output.lower()


def test_dev_command_has_expected_options(runner: CliRunner) -> None:
    """Dev command should expose --host, --port, --no-reload, --no-banner."""
    result = runner.invoke(cli, ["dev", "--help"])
    assert result.exit_code == 0, result.output
    for option in ["--host", "--port", "--log-level", "--asgi", "--no-reload", "--no-banner"]:
        assert option in result.output, f"Expected {option!r} in dev --help output"


def test_run_command_has_no_dev_flag(runner: CliRunner) -> None:
    """Run command should not have a --dev flag (replaced by standalone dev command)."""
    result = runner.invoke(cli, ["run", "--help"])
    assert result.exit_code == 0, result.output
    assert "--dev" not in result.output


def test_cli_help_lists_dev_and_js(runner: CliRunner) -> None:
    """CLI help should list dev, js, run, init commands (not build)."""
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0, result.output
    assert "dev" in result.output
    assert "js" in result.output
    assert "run" in result.output
    assert "init" in result.output


def test_cli_help_no_build_command(runner: CliRunner) -> None:
    """Build command should no longer be a top-level command."""
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0, result.output
    # build should not appear as a standalone command in the help
    # It is now under js build
    lines = result.output.splitlines()
    command_lines = [line.strip() for line in lines if line.strip().startswith("build")]
    assert not command_lines, "build should not be a top-level command"
