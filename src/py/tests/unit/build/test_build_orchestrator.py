"""Tests for frontend build config and orchestration."""

import importlib
import textwrap
from pathlib import Path
from subprocess import CompletedProcess

import pytest
from click.testing import CliRunner

from vertebra.builder._config import BuildConfig
from vertebra.builder._orchestrator import BuildOrchestrator
from vertebra.cli import cli


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def test_build_config_defaults() -> None:
    """BuildConfig should default to the Vite 8 integration path."""
    cfg = BuildConfig()
    assert cfg.bundler == "vite"
    assert cfg.frontend_root == "."
    assert cfg.outdir == "static/dist"
    assert cfg.codegen is False
    assert cfg.dev_port == 5173


def test_build_config_load_frontend_fields(tmp_path: Path) -> None:
    """load() reads frontend integration fields from pyproject.toml."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        textwrap.dedent(
            """
            [tool.vertebra]
            app = "example.app:app"

            [tool.vertebra.build]
            root = "frontend"
            outdir = "dist"
            codegen = true
            codegen_output = "frontend/generated"
            bundler = "vite"
            dev_port = 4242
            config = "vite.config.ts"
            plugins = ["react"]
            """
        ),
        encoding="utf-8",
    )

    cfg = BuildConfig.load(tmp_path)
    assert cfg.app == "example.app:app"
    assert cfg.frontend_root == "frontend"
    assert cfg.outdir == "dist"
    assert cfg.codegen is True
    assert cfg.codegen_output == "frontend/generated"
    assert cfg.bundler == "vite"
    assert cfg.dev_port == 4242
    assert cfg.vite_config == "vite.config.ts"
    assert cfg.plugins == ["react"]


def test_build_config_rejects_unknown_bundler(tmp_path: Path) -> None:
    """Only Vite should be accepted as the frontend bundler."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        textwrap.dedent(
            """
            [tool.vertebra.build]
            bundler = "webpack"
            """
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Unsupported bundler"):
        BuildConfig.load(tmp_path)


def test_build_config_rejects_rolldown_alias(tmp_path: Path) -> None:
    """The initial release should not preserve a rolldown compatibility alias."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        textwrap.dedent(
            """
            [tool.vertebra.build]
            bundler = "rolldown"
            """
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Unsupported bundler"):
        BuildConfig.load(tmp_path)


def test_build_skips_without_frontend_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """build() should no-op when no Vite-compatible frontend project exists."""

    def fail_run(*args: object, **kwargs: object) -> CompletedProcess[bytes]:
        raise AssertionError("subprocess.run should not be called")

    monkeypatch.setattr("subprocess.run", fail_run)

    orch = BuildOrchestrator(tmp_path, config=BuildConfig(frontend_root="frontend"))
    assert orch.build() is False


def test_build_uses_vite_for_frontend_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Frontend builds should execute through Vite."""
    frontend_root = tmp_path / "frontend"
    frontend_root.mkdir()
    (frontend_root / "package.json").write_text('{"name":"frontend"}', encoding="utf-8")

    calls: list[tuple[list[str], Path]] = []

    def fake_run(cmd: list[str], *, cwd: Path, env: dict[str, str], check: bool) -> CompletedProcess[str]:
        calls.append((cmd, cwd))
        assert env
        assert check is False
        return CompletedProcess(cmd, 0)

    monkeypatch.setattr("subprocess.run", fake_run)

    orch = BuildOrchestrator(
        tmp_path,
        config=BuildConfig(frontend_root="frontend", bundler="vite"),
    )
    assert orch.build() is True
    assert calls == [(["vite", "build"], frontend_root)]


def test_codegen_only_writes_generated_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """codegen_only() should emit TypeScript, Zod, and manifest artifacts."""
    app_module = tmp_path / "sample_app.py"
    app_module.write_text(
        textwrap.dedent(
            """

            import msgspec

            from vertebra.framework._app import VertebraApp

            class User(msgspec.Struct):
                id: int
                name: str

            app = VertebraApp()

            @app.get("/users/{user_id}")
            def get_user(user_id: int) -> User:
                return User(id=user_id, name="Ada")
            """
        ),
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    cfg = BuildConfig(app="sample_app:app", codegen=True, codegen_output="generated")
    orch = BuildOrchestrator(tmp_path, config=cfg)

    output_dir = orch.codegen_only()

    assert (output_dir / "types.d.ts").exists()
    assert (output_dir / "schemas.ts").exists()
    assert (output_dir / "manifest.json").exists()


def test_codegen_only_uses_runtime_app_override(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """codegen_only() should accept an app path override from the runtime."""
    app_module = tmp_path / "sample_runtime_app.py"
    app_module.write_text(
        textwrap.dedent(
            """

            import msgspec

            from vertebra.framework._app import VertebraApp

            class Health(msgspec.Struct):
                status: str

            app = VertebraApp()

            @app.get("/health")
            def health() -> Health:
                return Health(status="ok")
            """
        ),
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    cfg = BuildConfig(codegen=True, codegen_output="generated")
    orch = BuildOrchestrator(tmp_path, config=cfg, app_path="sample_runtime_app:app")

    output_dir = orch.codegen_only()

    assert (output_dir / "types.d.ts").exists()
    assert (output_dir / "schemas.ts").exists()
    assert (output_dir / "manifest.json").exists()


def test_dev_command_includes_port_and_config(tmp_path: Path) -> None:
    """dev mode should produce a Vite command with config and port."""
    cfg = BuildConfig(frontend_root="frontend", dev_port=3000, vite_config="frontend/vite.config.ts")
    orch = BuildOrchestrator(tmp_path, config=cfg)

    cmd = orch._vite_command(dev=True)

    assert cmd[0] == "vite"
    assert "--port" in cmd
    assert "3000" in cmd
    assert "--config" in cmd
    assert str((tmp_path / "frontend" / "vite.config.ts").resolve()) in cmd


def test_cli_build_invokes_orchestrator(runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The CLI build command should hand off frontend builds to the orchestrator."""
    called: list[Path] = []

    class FakeOrchestrator:
        def __init__(self, project_dir: Path) -> None:
            called.append(project_dir)

        def build(self) -> bool:
            return True

    build_module = importlib.import_module("vertebra.cli.build")
    monkeypatch.setattr(build_module, "BuildOrchestrator", FakeOrchestrator)

    result = runner.invoke(cli, ["build", "--project-dir", str(tmp_path)])

    assert result.exit_code == 0, result.output
    assert called == [tmp_path.resolve()]
