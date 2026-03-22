"""Tests for the Vite fallback subprocess manager."""

from pathlib import Path
from unittest.mock import patch

from vertebra.builder._fallback import ViteDevServer, run_vite_build


# ---------------------------------------------------------------------------
# ViteDevServer
# ---------------------------------------------------------------------------


def test_vite_dev_server_proxy_url() -> None:
    server = ViteDevServer(Path("/tmp"), 5173)
    assert server.proxy_url == "http://127.0.0.1:5173"


def test_vite_dev_server_not_running_initially() -> None:
    server = ViteDevServer(Path("/tmp"), 5173)
    assert not server.is_running


def test_vite_dev_server_stop_when_not_started() -> None:
    """Stopping a server that was never started should be a no-op."""
    server = ViteDevServer(Path("/tmp"), 5173)
    server.stop()  # should not raise
    assert not server.is_running


def test_vite_dev_server_proxy_url_custom_port() -> None:
    server = ViteDevServer(Path("/tmp"), 3000)
    assert server.proxy_url == "http://127.0.0.1:3000"


def test_vite_dev_server_resolve_command_no_vite_no_npx() -> None:
    """Should raise RuntimeError when neither vite nor npx is found."""
    server = ViteDevServer(Path("/tmp"), 5173)
    with patch("shutil.which", return_value=None):
        try:
            server._resolve_command()  # noqa: SLF001
        except RuntimeError as exc:
            assert "Vite not found" in str(exc)
        else:
            raise AssertionError("Expected RuntimeError")


def test_vite_dev_server_resolve_command_uses_vite() -> None:
    server = ViteDevServer(Path("/tmp"), 5173)
    with patch("shutil.which", side_effect=lambda x: "/usr/bin/vite" if x == "vite" else None):
        cmd = server._resolve_command()  # noqa: SLF001
        assert "/usr/bin/vite" in cmd
        assert "--port 5173" in cmd


def test_vite_dev_server_resolve_command_falls_back_to_npx() -> None:
    server = ViteDevServer(Path("/tmp"), 5173)

    def mock_which(name: str) -> "str | None":
        if name == "npx":
            return "/usr/bin/npx"
        return None

    with patch("shutil.which", side_effect=mock_which):
        cmd = server._resolve_command()  # noqa: SLF001
        assert "npx" in cmd
        assert "vite" in cmd


# ---------------------------------------------------------------------------
# RunViteBuild
# ---------------------------------------------------------------------------


def test_run_vite_build_raises_when_vite_missing() -> None:
    with patch("shutil.which", return_value=None):
        try:
            run_vite_build(Path("/tmp"))
        except RuntimeError as exc:
            assert "Vite not found" in str(exc)
        else:
            raise AssertionError("Expected RuntimeError")
