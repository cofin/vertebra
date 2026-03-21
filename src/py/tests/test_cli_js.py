"""Tests for the js CLI command group."""

import pytest
from click.testing import CliRunner

from vertebra.cli import cli
from vertebra.cli.js import _parse_package_name


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


class TestJsGroup:
    """Tests for the js command group and subcommands."""

    def test_js_group_exists(self, runner: CliRunner) -> None:
        """JS command group should be registered."""
        result = runner.invoke(cli, ["js", "--help"])
        assert result.exit_code == 0, result.output

    def test_js_has_expected_subcommands(self, runner: CliRunner) -> None:
        """JS group should have add, remove, list, install, build, generate-types, exec."""
        result = runner.invoke(cli, ["js", "--help"])
        assert result.exit_code == 0, result.output
        for subcmd in ["add", "remove", "list", "install", "build", "generate-types", "exec"]:
            assert subcmd in result.output, f"Expected subcommand {subcmd!r} in js --help"

    def test_add_help(self, runner: CliRunner) -> None:
        """js add --help should show usage."""
        result = runner.invoke(cli, ["js", "add", "--help"])
        assert result.exit_code == 0, result.output
        assert "--dev" in result.output or "-D" in result.output

    def test_remove_help(self, runner: CliRunner) -> None:
        """js remove --help should show usage."""
        result = runner.invoke(cli, ["js", "remove", "--help"])
        assert result.exit_code == 0, result.output


class TestParsePackageName:
    """Tests for package spec parsing."""

    def test_simple_package(self) -> None:
        assert _parse_package_name("react") == "react"

    def test_package_with_version(self) -> None:
        assert _parse_package_name("react@18") == "react"

    def test_package_with_semver(self) -> None:
        assert _parse_package_name("react@18.2.0") == "react"

    def test_scoped_package(self) -> None:
        assert _parse_package_name("@types/react") == "@types/react"

    def test_scoped_package_with_version(self) -> None:
        assert _parse_package_name("@types/react@18") == "@types/react"


class TestJsAddRemove:
    """Integration tests for add/remove with a temp pyproject.toml."""

    def test_add_creates_dependency(self, runner: CliRunner, tmp_path: CliRunner) -> None:
        """js add should write to pyproject.toml."""
        import os

        project = tmp_path  # type: ignore[assignment]
        (project / "pyproject.toml").write_text("[project]\nname = 'test'\n", encoding="utf-8")  # type: ignore[union-attr]

        old_cwd = os.getcwd()
        try:
            os.chdir(str(project))
            result = runner.invoke(cli, ["js", "add", "react@18"])
            assert result.exit_code == 0, result.output
            assert "Added react@18" in result.output
        finally:
            os.chdir(old_cwd)

    def test_remove_removes_dependency(self, runner: CliRunner, tmp_path: CliRunner) -> None:
        """js remove should update pyproject.toml."""
        import os

        project = tmp_path  # type: ignore[assignment]
        (project / "pyproject.toml").write_text(  # type: ignore[union-attr]
            '[project]\nname = "test"\n\n[tool.vertebra.js]\ndependencies = ["react@18"]\n',
            encoding="utf-8",
        )

        old_cwd = os.getcwd()
        try:
            os.chdir(str(project))
            result = runner.invoke(cli, ["js", "remove", "react"])
            assert result.exit_code == 0, result.output
            assert "Removed react" in result.output
        finally:
            os.chdir(old_cwd)
