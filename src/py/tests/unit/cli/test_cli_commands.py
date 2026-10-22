"""Unit tests for newly added and hardened CLI commands and flags."""

import io
from pathlib import Path

from click.testing import CliRunner

from vertebra.build.packaging.standalone import StandaloneBuilder
from vertebra.build.packaging.targets import BuildTarget
from vertebra.cli import cli
from vertebra.cli._launch import ServerLaunchEnv
from vertebra.cli.banner.renderer import render_banner
from vertebra.cli.errors import CLIError
from vertebra.cli.theme import Console


def test_vtb_init_default_scaffolding(tmp_path: Path) -> None:
    """Verify vtb init creates pyproject.toml and app.py for default template."""
    runner = CliRunner()
    app_dir = tmp_path / "my_app"
    result = runner.invoke(cli, ["init", str(app_dir)])
    assert result.exit_code == 0
    assert app_dir.is_dir()
    pyproject = (app_dir / "pyproject.toml").read_text(encoding="utf-8")
    assert 'name = "' in pyproject
    assert "[tool.vertebra]" in pyproject
    assert 'app = "app:app"' in pyproject
    assert "entry =" not in pyproject

    app_py = (app_dir / "app.py").read_text(encoding="utf-8")
    assert "from vertebra import App" in app_py
    assert '@app.get("/health")' in app_py


def test_vtb_init_web_template_scaffolding(tmp_path: Path) -> None:
    """Verify vtb init --template web creates src/main.ts and sets entry."""
    runner = CliRunner()
    app_dir = tmp_path / "web_app"
    result = runner.invoke(cli, ["init", str(app_dir), "--template", "web"])
    assert result.exit_code == 0
    assert app_dir.is_dir()
    pyproject = (app_dir / "pyproject.toml").read_text(encoding="utf-8")
    assert 'entry = "src/main.ts"' in pyproject
    assert (app_dir / "src" / "main.ts").is_file()


def test_vtb_init_refuses_non_empty_directory(tmp_path: Path) -> None:
    """Verify vtb init fails when target directory is non-empty."""
    runner = CliRunner()
    app_dir = tmp_path / "existing_app"
    app_dir.mkdir()
    (app_dir / "existing.txt").write_text("content", encoding="utf-8")

    result = runner.invoke(cli, ["init", str(app_dir)])
    assert result.exit_code != 0


def test_server_launch_env_to_environ() -> None:
    """Verify ServerLaunchEnv populates all required VERTEBRA_* environment variables."""
    launch_env = ServerLaunchEnv(
        app_path="demo:app",
        host="0.0.0.0",
        port=9000,
        workers=4,
        log_level="debug",
        dev=True,
        native=True,
    )
    env = launch_env.to_environ()
    assert env["VERTEBRA_APP"] == "demo:app"
    assert env["LITESTAR_APP"] == "demo:app"
    assert env["VERTEBRA_HOST"] == "0.0.0.0"
    assert env["VERTEBRA_PORT"] == "9000"
    assert env["VERTEBRA_WORKERS"] == "4"
    assert env["VERTEBRA_LOG_LEVEL"] == "debug"
    assert env["VERTEBRA_DEV"] == "1"
    assert env["VERTEBRA_NATIVE"] == "1"
    assert "PYTHONPATH" in env


def test_cli_error_show_with_custom_file() -> None:
    """Verify CLIError.show respects custom file argument."""
    stream = io.StringIO()
    err = CLIError("Test failure message", hint="Try doing X")
    err.show(file=stream)
    rendered = stream.getvalue()
    assert "Test failure message" in rendered
    assert "Try doing X" in rendered


def test_standalone_builder_records_profile(tmp_path: Path) -> None:
    """Verify StandaloneBuilder records profile in build-manifest.json."""
    project_dir = tmp_path / "app"
    project_dir.mkdir()
    (project_dir / "pyproject.toml").write_text('[project]\nname = "test"\n', encoding="utf-8")
    (project_dir / "main.py").write_text("def run(): pass\n", encoding="utf-8")

    target = BuildTarget.detect_host()
    out = tmp_path / "out"
    builder = StandaloneBuilder(project_dir, target=target, entrypoint="main:run", profile="debug")
    builder.build(out)

    manifest_text = (out / "build-manifest.json").read_text(encoding="utf-8")
    assert '"profile": "debug"' in manifest_text


def test_render_banner_without_log_level() -> None:
    """Verify render_banner executes cleanly without log_level."""
    stream = io.StringIO()
    vc = Console(file=stream, force_terminal=True)
    render_banner(mode="serve", host="127.0.0.1", port=8000, workers=1, vc=vc)
    output = stream.getvalue()
    assert len(output) > 0
