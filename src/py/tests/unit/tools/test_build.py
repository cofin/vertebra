"""Tests for build command and configuration."""

from pathlib import Path
from unittest import mock

import pytest
from click.testing import CliRunner

from vertebra.core.build.config import BuildConfig
from vertebra.cli import build


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def test_build_config_load(tmp_path: Path) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        """
[tool.vertebra]
profile = "baseline"

[tool.vertebra.core.build.targets.linux-x64]
profile = "modern"
"""
    )

    config = BuildConfig.load(tmp_path)
    assert config.profile == "baseline"
    assert config.targets["linux-x64"].profile == "modern"


def test_build_cli_one_file(runner: CliRunner, tmp_path: Path) -> None:
    with mock.patch("vertebra.core.build.bundler.Bundler") as mock_bundler_cls:
        mock_instance = mock_bundler_cls.return_value
        mock_instance.build = mock.AsyncMock()

        result = runner.invoke(
            build, [str(tmp_path), "--one-file", "--target", "linux-x64", "--output", str(tmp_path / "dist")]
        )

        assert result.exit_code == 0
        mock_bundler_cls.assert_called_once()
        mock_instance.build.assert_called_once()
