from unittest.mock import AsyncMock, patch

import pytest
from click.testing import CliRunner

from vertebra.cli import main


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def test_build_command_one_file(runner, tmp_path) -> None:
    """Test standard build command invocation."""
    with patch("vertebra.core.build.onefile.OneFileBuilder") as mock_builder:
        instance = mock_builder.return_value
        instance.build = AsyncMock()

        result = runner.invoke(
            main,
            [
                "build",
                ".",
                "--onefile",
                "--target",
                "linux-x64",
                "--profile",
                "modern",
                "--output",
                str(tmp_path / "dist"),
            ],
        )

        assert result.exit_code == 0
        assert "build completed successfully" in result.output.lower()

        mock_builder.assert_called_once()
        call_kwargs = mock_builder.call_args[1]
        assert call_kwargs["target"] == "linux-x64"
        assert call_kwargs["profile"] == "modern"

        instance.build.assert_called_once()


def test_build_command_missing_target(runner) -> None:
    """Test required arguments."""
    result = runner.invoke(main, ["build", ".", "--onefile"])
    assert result.exit_code != 0
    assert "--target is required" in result.output
