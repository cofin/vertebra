"""Tests for the js CLI command group."""

from __future__ import annotations

import os
import tomllib
from pathlib import Path

import pytest
from click.testing import CliRunner

from vertebra.cli import cli
from vertebra.cli.js import _parse_package_name


@pytest.fixture
def runner() -> CliRunner:
    """Return a Click CLI test runner."""
    return CliRunner()


def test_js_group_exists(runner: CliRunner) -> None:
    """Verify the JS command group is registered."""
    result = runner.invoke(cli, ["js", "--help"])
    assert result.exit_code == 0, result.output


def test_js_group_has_expected_subcommands(runner: CliRunner) -> None:
    """Verify the JS group has add, remove, list, install, build, generate-types, and exec."""
    result = runner.invoke(cli, ["js", "--help"])
    assert result.exit_code == 0, result.output
    for subcmd in ("add", "remove", "list", "install", "build", "generate-types", "exec"):
        assert subcmd in result.output, f"Expected subcommand {subcmd!r} in js --help"


def test_js_group_add_help(runner: CliRunner) -> None:
    """Verify ``js add --help`` displays usage."""
    result = runner.invoke(cli, ["js", "add", "--help"])
    assert result.exit_code == 0, result.output
    assert "--dev" in result.output or "-D" in result.output


def test_js_group_remove_help(runner: CliRunner) -> None:
    """Verify ``js remove --help`` displays usage."""
    result = runner.invoke(cli, ["js", "remove", "--help"])
    assert result.exit_code == 0, result.output


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        ("react", "react"),
        ("react@18", "react"),
        ("react@18.2.0", "react"),
        ("@types/react", "@types/react"),
        ("@types/react@18", "@types/react"),
    ],
)
def test_parse_package_name(spec: str, expected: str) -> None:
    """Verify package name parsing across unscoped and scoped package specs."""
    assert _parse_package_name(spec) == expected


def test_js_add_creates_dependency(runner: CliRunner, tmp_path: Path) -> None:
    """Verify ``js add`` writes the dependency to pyproject.toml."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text("[project]\nname = 'test'\n", encoding="utf-8")

    old_cwd = os.getcwd()
    try:
        os.chdir(str(tmp_path))
        result = runner.invoke(cli, ["js", "add", "react@18"])
        assert result.exit_code == 0, result.output or ""
        data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
        js_deps = data.get("tool", {}).get("vtb", {}).get("js", {}).get("dependencies", [])
        assert "react@18" in js_deps
    finally:
        os.chdir(old_cwd)


def test_js_remove_removes_dependency(runner: CliRunner, tmp_path: Path) -> None:
    """Verify ``js remove`` removes the dependency from pyproject.toml."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        '[project]\nname = "test"\n\n[tool.vtb.js]\ndependencies = ["react@18"]\n',
        encoding="utf-8",
    )

    old_cwd = os.getcwd()
    try:
        os.chdir(str(tmp_path))
        result = runner.invoke(cli, ["js", "remove", "react"])
        assert result.exit_code == 0, result.output or ""
        data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
        js_deps = data.get("tool", {}).get("vtb", {}).get("js", {}).get("dependencies", [])
        assert "react@18" not in js_deps
    finally:
        os.chdir(old_cwd)


def test_js_install_syncs_wheel_client_without_npm_deps(runner: CliRunner, tmp_path: Path) -> None:
    """Verify ``vtb js install`` writes .vertebra/import_map.json and .vertebra/client without NPM."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('[project]\nname = "zero-npm-app"\n', encoding="utf-8")

    old_cwd = os.getcwd()
    try:
        os.chdir(str(tmp_path))
        result = runner.invoke(cli, ["js", "install"])
        assert result.exit_code == 0, result.output or ""
        assert (tmp_path / ".vertebra" / "import_map.json").is_file()
        assert (tmp_path / ".vertebra" / "client" / "index.js").is_file()
        assert (tmp_path / ".vertebra" / "client" / "index.d.ts").is_file()
        assert (tmp_path / ".vertebra" / "client" / "package.json").is_file()
    finally:
        os.chdir(old_cwd)
