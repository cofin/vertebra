"""Unit tests for newly added and hardened CLI commands and flags."""

import io
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from vertebra import App
from vertebra.build.packaging.standalone import StandaloneBuilder
from vertebra.build.packaging.targets import BuildTarget
from vertebra.cli import cli
from vertebra.cli import run as cli_run_mod
from vertebra.cli._launch import ServerLaunchEnv
from vertebra.cli.banner.renderer import render_banner
from vertebra.cli.errors import CLIError
from vertebra.cli.theme import Console
from vertebra.supervisor import _worker as supervisor_worker


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


def test_vtb_run_preserves_env_workers_topology_and_threads(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify omitting --workers, --topology, and --threads preserves VERTEBRA_* env vars."""
    captured: dict[str, Any] = {}

    def fake_build_cmd() -> list[str]:
        return ["/bin/true"]

    def fake_supervisor(*, server_cmd: list[str], env: dict[str, str], **_kwargs: Any) -> int:
        captured["cmd"] = server_cmd
        captured["env"] = dict(env)
        return 0

    monkeypatch.setattr(cli_run_mod, "build_server_command", fake_build_cmd)
    monkeypatch.setattr(cli_run_mod, "run_supervisor", fake_supervisor)
    monkeypatch.setenv("VERTEBRA_WORKERS", "4")
    monkeypatch.setenv("VERTEBRA_TOPOLOGY", "shm")
    monkeypatch.setenv("VERTEBRA_THREADS", "2")

    runner = CliRunner()
    result = runner.invoke(cli, ["run", "demo:app", "--no-banner"])
    assert result.exit_code == 0, result.output
    assert captured["env"]["VERTEBRA_WORKERS"] == "4"
    assert captured["env"]["VERTEBRA_TOPOLOGY"] == "shm"
    assert captured["env"]["VERTEBRA_THREADS"] == "2"

    auto_result = runner.invoke(cli, ["run", "demo:app", "--topology", "auto", "--workers", "4", "--no-banner"])
    assert auto_result.exit_code == 0, auto_result.output
    assert captured["env"]["VERTEBRA_TOPOLOGY"] == "auto"
    assert captured["env"]["VERTEBRA_WORKERS"] == "4"


def test_vtb_run_jit_flag_sets_python_jit_on_gil_and_skips_freethreaded(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify --jit configures PYTHON_JIT=1 on GIL builds and skips free-threaded builds."""
    captured: dict[str, Any] = {}

    def fake_build_cmd() -> list[str]:
        return ["/bin/true"]

    def fake_supervisor(*, server_cmd: list[str], env: dict[str, str], **_kwargs: Any) -> int:
        captured["cmd"] = server_cmd
        captured["env"] = dict(env)
        return 0

    monkeypatch.setattr(cli_run_mod, "build_server_command", fake_build_cmd)
    monkeypatch.setattr(cli_run_mod, "run_supervisor", fake_supervisor)
    monkeypatch.setattr(cli_run_mod, "_is_free_threaded_runtime", lambda: False, raising=False)

    runner = CliRunner()
    jit_on = runner.invoke(cli, ["run", "demo:app", "--jit", "--no-banner"])
    assert jit_on.exit_code == 0, jit_on.output
    assert captured["env"]["PYTHON_JIT"] == "1"

    jit_off = runner.invoke(cli, ["run", "demo:app", "--no-jit", "--no-banner"])
    assert jit_off.exit_code == 0, jit_off.output
    assert captured["env"]["PYTHON_JIT"] == "0"

    monkeypatch.setattr(cli_run_mod, "_is_free_threaded_runtime", lambda: True, raising=False)
    monkeypatch.delenv("PYTHON_JIT", raising=False)
    jit_ft = runner.invoke(cli, ["run", "demo:app", "--jit", "--no-banner"])
    assert jit_ft.exit_code == 0, jit_ft.output
    assert captured["env"].get("PYTHON_JIT") != "1"


def test_worker_main_asgi_app_avoids_double_mount(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify VERTEBRA_NATIVE=0 routes App once as ASGI without double-mounting or native handlers."""
    captured: list[tuple[dict[int, Any], list[tuple[str, Any, str]]]] = []

    async def raw_asgi(scope: dict[str, Any], receive: Any, send: Any) -> None:
        _ = (scope, receive, send)

    mounted_app = App()
    mounted_app.mount("/", raw_asgi, protocol="asgi")

    native_app = App()

    @native_app.get("/hello")
    def hello() -> bytes:
        return b"ok"

    def fake_run_worker(handlers: dict[int, Any], *, mounts: list[tuple[str, Any, str]], **_kwargs: Any) -> None:
        captured.append((dict(handlers), list(mounts)))

    monkeypatch.setattr(supervisor_worker, "run_worker", fake_run_worker)

    monkeypatch.setenv("VERTEBRA_APP", "demo:mounted")
    monkeypatch.setenv("VERTEBRA_NATIVE", "0")
    monkeypatch.setattr(supervisor_worker, "import_app", lambda _path: mounted_app)
    supervisor_worker.main()
    handlers_mounted, mounts_mounted = captured[-1]
    assert handlers_mounted == {}
    assert len(mounts_mounted) == 1
    assert mounts_mounted[0][1] is raw_asgi

    monkeypatch.setenv("VERTEBRA_APP", "demo:native")
    monkeypatch.setenv("VERTEBRA_NATIVE", "0")
    monkeypatch.setattr(supervisor_worker, "import_app", lambda _path: native_app)
    supervisor_worker.main()
    handlers_native_off, mounts_native_off = captured[-1]
    assert handlers_native_off == {}
    assert len(mounts_native_off) == 1
    assert callable(mounts_native_off[0][1])

    monkeypatch.setenv("VERTEBRA_APP", "demo:raw")
    monkeypatch.setenv("VERTEBRA_NATIVE", "1")
    monkeypatch.setattr(supervisor_worker, "import_app", lambda _path: raw_asgi)
    supervisor_worker.main()
    handlers_raw, mounts_raw = captured[-1]
    assert handlers_raw == {}
    assert len(mounts_raw) == 1
    assert mounts_raw[0][1] is raw_asgi
