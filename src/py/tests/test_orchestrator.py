"""Tests for the BuildOrchestrator."""

import socket
from pathlib import Path
from unittest.mock import patch

import pytest

from vertebra.builder._config import BuildConfig
from vertebra.builder._orchestrator import BuildOrchestrator


@pytest.fixture
def tmp_project(tmp_path: Path) -> Path:
    """Create a minimal project directory."""
    (tmp_path / "pyproject.toml").write_text(
        "[project]\nname = 'test'\n",
        encoding="utf-8",
    )
    return tmp_path


class TestFindAvailablePort:
    """Tests for BuildOrchestrator._find_available_port."""

    def test_finds_open_port(self) -> None:
        """Should return a port in the requested range."""
        port = BuildOrchestrator._find_available_port(start=15000, end=15100)
        assert 15000 <= port < 15100

    def test_skips_occupied_port(self) -> None:
        """Should skip a port that is already in use."""
        # Bind a port first
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            occupied_port = sock.getsockname()[1]
            # Search a range starting at the occupied port
            port = BuildOrchestrator._find_available_port(start=occupied_port, end=occupied_port + 10)
            # Should not return the occupied port
            assert port != occupied_port

    def test_raises_when_no_port_available(self) -> None:
        """Should raise RuntimeError when range is exhausted."""
        with pytest.raises(RuntimeError, match="No available port"):
            # Use a range of 0 ports
            BuildOrchestrator._find_available_port(start=1, end=1)


class TestResolveDevPort:
    """Tests for BuildOrchestrator._resolve_dev_port."""

    def test_returns_explicit_config_port(self, tmp_project: Path) -> None:
        """Should return the port from config when set."""
        config = BuildConfig(dev_port=9999)
        orchestrator = BuildOrchestrator(tmp_project, config)
        assert orchestrator._resolve_dev_port() == 9999

    def test_auto_selects_when_no_config(self, tmp_project: Path) -> None:
        """Should auto-select a port when config has no dev_port."""
        config = BuildConfig(dev_port=None)
        orchestrator = BuildOrchestrator(tmp_project, config)
        port = orchestrator._resolve_dev_port()
        assert 5173 <= port < 5200


class TestBuild:
    """Tests for BuildOrchestrator.build."""

    def test_returns_false_with_no_entry(self, tmp_project: Path) -> None:
        """Should return False when no entry point is configured."""
        config = BuildConfig(entry="")
        orchestrator = BuildOrchestrator(tmp_project, config)
        assert orchestrator.build() is False

    def test_returns_true_with_entry(self, tmp_project: Path) -> None:
        """Should return True when entry point is set (with mocked bundler)."""
        config = BuildConfig(entry="src/main.ts", bundler="native")
        orchestrator = BuildOrchestrator(tmp_project, config)

        with patch.object(orchestrator, "_run_bundler"):
            assert orchestrator.build() is True


class TestRunBundler:
    """Tests for BuildOrchestrator._run_bundler fallback behavior."""

    def test_falls_back_to_vite_on_import_error(self, tmp_project: Path) -> None:
        """Should fall back to Vite when native bundler raises ImportError."""
        config = BuildConfig(entry="src/main.ts", bundler="native")
        orchestrator = BuildOrchestrator(tmp_project, config)

        with (
            patch.object(orchestrator, "_run_native", side_effect=ImportError("no native")),
            patch.object(orchestrator, "_run_vite_fallback") as mock_vite,
        ):
            orchestrator._run_bundler(dev=False)
            mock_vite.assert_called_once_with(dev=False)

    def test_uses_vite_directly_when_configured(self, tmp_project: Path) -> None:
        """Should use Vite directly when bundler is set to 'vite'."""
        config = BuildConfig(entry="src/main.ts", bundler="vite")
        orchestrator = BuildOrchestrator(tmp_project, config)

        with patch.object(orchestrator, "_run_vite_fallback") as mock_vite:
            orchestrator._run_bundler(dev=False)
            mock_vite.assert_called_once_with(dev=False)

    def test_noop_when_no_entry(self, tmp_project: Path) -> None:
        """Should do nothing when there is no entry point."""
        config = BuildConfig(entry="")
        orchestrator = BuildOrchestrator(tmp_project, config)

        with patch.object(orchestrator, "_run_native") as mock_native:
            orchestrator._run_bundler(dev=False)
            mock_native.assert_not_called()
