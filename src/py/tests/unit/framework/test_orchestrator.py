"""Tests for the BuildOrchestrator."""

import importlib
import socket
import textwrap
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from vertebra.build import BuildConfig, BuildOrchestrator
from vertebra.cli.build import build


@pytest.fixture
def tmp_project(tmp_path: Path) -> Path:
    """Create a minimal project directory."""
    (tmp_path / "pyproject.toml").write_text(
        "[project]\nname = 'test'\n",
        encoding="utf-8",
    )
    return tmp_path


def test_find_available_port_finds_open_port() -> None:
    """Should return a port in the requested range."""
    port = BuildOrchestrator._find_available_port(start=15000, end=15100)
    assert 15000 <= port < 15100


def test_find_available_port_skips_occupied_port() -> None:
    """Should skip a port that is already in use."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        occupied_port = sock.getsockname()[1]
        port = BuildOrchestrator._find_available_port(start=occupied_port, end=occupied_port + 10)
        assert port != occupied_port


def test_find_available_port_raises_when_no_port_available() -> None:
    """Should raise RuntimeError when range is exhausted."""
    with pytest.raises(RuntimeError, match="No available port"):
        BuildOrchestrator._find_available_port(start=1, end=1)


def test_resolve_dev_port_returns_explicit_config_port(tmp_project: Path) -> None:
    """Should return the port from config when set."""
    config = BuildConfig(dev_port=9999)
    orchestrator = BuildOrchestrator(tmp_project, config)
    assert orchestrator._resolve_dev_port() == 9999


def test_resolve_dev_port_auto_selects_when_no_config(tmp_project: Path) -> None:
    """Should auto-select a port when config has no dev_port."""
    config = BuildConfig(dev_port=None)
    orchestrator = BuildOrchestrator(tmp_project, config)
    port = orchestrator._resolve_dev_port()
    assert 5173 <= port < 5200


def test_build_returns_false_with_no_entry(tmp_project: Path) -> None:
    """Should return False when no entry point is configured."""
    config = BuildConfig(entry="")
    orchestrator = BuildOrchestrator(tmp_project, config)
    assert orchestrator.build() is False


def test_build_returns_true_with_entry(tmp_project: Path) -> None:
    """Should return True when entry point is set (with mocked bundler)."""
    config = BuildConfig(entry="src/main.ts", bundler="native")
    orchestrator = BuildOrchestrator(tmp_project, config)

    with patch.object(orchestrator, "_run_bundler"):
        assert orchestrator.build() is True


def test_run_bundler_falls_back_to_vite_on_import_error(tmp_project: Path) -> None:
    """Should fall back to Vite when native bundler raises ImportError."""
    config = BuildConfig(entry="src/main.ts", bundler="native")
    orchestrator = BuildOrchestrator(tmp_project, config)

    with (
        patch.object(orchestrator, "_run_native", side_effect=ImportError("no native")),
        patch.object(orchestrator, "_run_vite_fallback") as mock_vite,
    ):
        orchestrator._run_bundler(dev=False)
        mock_vite.assert_called_once_with(dev=False)


def test_run_bundler_uses_vite_directly_when_configured(tmp_project: Path) -> None:
    """Should use Vite directly when bundler is set to 'vite'."""
    config = BuildConfig(entry="src/main.ts", bundler="vite")
    orchestrator = BuildOrchestrator(tmp_project, config)

    with patch.object(orchestrator, "_run_vite_fallback") as mock_vite:
        orchestrator._run_bundler(dev=False)
        mock_vite.assert_called_once_with(dev=False)


def test_run_bundler_noop_when_no_entry(tmp_project: Path) -> None:
    """Should do nothing when there is no entry point."""
    config = BuildConfig(entry="")
    orchestrator = BuildOrchestrator(tmp_project, config)

    with patch.object(orchestrator, "_run_native") as mock_native:
        orchestrator._run_bundler(dev=False)
        mock_native.assert_not_called()


def test_build_config_load_from_directory_and_vtb_section(tmp_path: Path) -> None:
    """BuildConfig.load should accept a project directory and support [tool.vtb]."""
    (tmp_path / "pyproject.toml").write_text(
        textwrap.dedent(
            """
            [tool.vtb]
            app = "example.app:app"
            entry = "src/main.ts"
            outdir = "static/dist"
            codegen = true
            codegen_output = "frontend/generated"
            dev_port = 4242
            """
        ),
        encoding="utf-8",
    )

    cfg = BuildConfig.load(tmp_path)
    assert cfg.app == "example.app:app"
    assert cfg.entry == "src/main.ts"
    assert cfg.outdir == "static/dist"
    assert cfg.codegen is True
    assert cfg.codegen_output == "frontend/generated"
    assert cfg.dev_port == 4242


def test_codegen_only_writes_generated_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """codegen_only() should emit TypeScript, Zod, and manifest artifacts."""
    app_module = tmp_path / "sample_app.py"
    app_module.write_text(
        textwrap.dedent(
            """
            import msgspec
            from vertebra.framework._app import App

            class User(msgspec.Struct):
                id: int
                name: str

            app = App()

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
    assert (output_dir / "schemas.zod.ts").exists()
    assert (output_dir / "manifest.json").exists()


def test_codegen_only_uses_runtime_app_override(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """codegen_only() should accept an app path override from the runtime."""
    app_module = tmp_path / "sample_runtime_app.py"
    app_module.write_text(
        textwrap.dedent(
            """
            import msgspec
            from vertebra.framework._app import App

            class Health(msgspec.Struct):
                status: str

            app = App()

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
    assert (output_dir / "schemas.zod.ts").exists()
    assert (output_dir / "manifest.json").exists()


def test_cli_build_invokes_orchestrator(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The CLI build command should hand off frontend builds to the orchestrator."""
    called: list[Path] = []

    class FakeOrchestrator:
        def __init__(self, project_dir: Path) -> None:
            called.append(project_dir)

        def build(self) -> bool:
            return True

    build_module = importlib.import_module("vertebra.cli.build")
    monkeypatch.setattr(build_module, "BuildOrchestrator", FakeOrchestrator)

    runner = CliRunner()
    result = runner.invoke(build, ["--project-dir", str(tmp_path)])

    assert result.exit_code == 0, result.output
    assert called == [tmp_path.resolve()]
