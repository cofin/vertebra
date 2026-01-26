from unittest.mock import AsyncMock, patch

import pytest
from click.testing import CliRunner

from vertebra.cli import main


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def test_build_command_one_file(runner, tmp_path) -> None:
    """Test standard build command invocation."""
    with patch("vertebra.core.build.bundler.Bundler") as mock_bundler:
        instance = mock_bundler.return_value
        instance.build = AsyncMock()

        result = runner.invoke(
            main,
            [
                "build",
                ".",
                "--one-file",
                "--target",
                "linux-x64",
                "--profile",
                "modern",
                "--output",
                str(tmp_path / "dist"),
            ],
        )

        assert result.exit_code == 0
        assert "Build completed successfully" in result.output

        mock_bundler.assert_called_once()
        call_kwargs = mock_bundler.call_args[1]
        assert call_kwargs["target"] == "linux-x64"
        assert call_kwargs["profile"] == "modern"

        instance.build.assert_called_once()


def test_build_command_missing_target(runner) -> None:
    """Test required arguments."""
    result = runner.invoke(main, ["build", ".", "--one-file"])
    assert result.exit_code != 0
    assert "Missing option '--target'" in result.output
