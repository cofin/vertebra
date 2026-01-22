"""Tests for 'vertebra init' command."""

from pathlib import Path

import pytest
from click.testing import CliRunner

from vertebra.cli import init


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def clean_cwd(tmp_path: Path) -> Path:
    """Run test in a temporary directory."""
    cwd = Path.cwd()
    os.chdir(tmp_path)
    yield tmp_path
    os.chdir(cwd)


import os


def test_init_default(runner: CliRunner, tmp_path: Path) -> None:
    with runner.isolated_filesystem(temp_dir=tmp_path):
        result = runner.invoke(init, ["my-app"], input="\n")
        assert result.exit_code == 0

        project_dir = Path("my-app")
        assert project_dir.exists()
        assert (project_dir / "pyproject.toml").exists()
        assert (project_dir / "src/py/my_app/app.py").exists()
        assert (project_dir / "src/py/my_app/__init__.py").exists()
        assert (project_dir / "src/py/my_app/__main__.py").exists()

        content = (project_dir / "pyproject.toml").read_text()
        assert 'name = "my-app"' in content
        assert "{{ project_slug }}" not in content


def test_init_interactive(runner: CliRunner, tmp_path: Path) -> None:
    with runner.isolated_filesystem(temp_dir=tmp_path):
        # Prompt for name
        result = runner.invoke(init, input="my-interactive-app\n")
        assert result.exit_code == 0

        project_dir = Path("my-interactive-app")
        assert project_dir.exists()
