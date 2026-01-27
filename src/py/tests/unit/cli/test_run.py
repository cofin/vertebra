from __future__ import annotations

from unittest.mock import patch

import pytest
from click.testing import CliRunner

from vertebra.cli import main

pytestmark = pytest.mark.unit


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def test_run_command_sets_native_default(runner) -> None:
    with (
        patch("vertebra.cli.run.build_server_command", return_value=["vertebra-server"]) as build_cmd,
        patch("vertebra.cli.run.run_supervisor", return_value=0) as run_supervisor,
        patch("vertebra.cli.run.render_banner"),
    ):
        result = runner.invoke(main, ["run", "my.app:app", "--no-banner"])

    assert result.exit_code == 0
    build_cmd.assert_called_once()
    run_supervisor.assert_called_once()
    env = run_supervisor.call_args.kwargs["env"]
    assert env["VERTEBRA_PYTHON_APP"] == "my.app:app"
    assert env["VERTEBRA_NATIVE"] == "1"


def test_run_command_asgi_flag_disables_native(runner) -> None:
    with (
        patch("vertebra.cli.run.build_server_command", return_value=["vertebra-server"]),
        patch("vertebra.cli.run.run_supervisor", return_value=0) as run_supervisor,
        patch("vertebra.cli.run.render_banner"),
    ):
        result = runner.invoke(main, ["run", "my.app:app", "--asgi", "--no-banner"])

    assert result.exit_code == 0
    env = run_supervisor.call_args.kwargs["env"]
    assert env["VERTEBRA_NATIVE"] == "0"
