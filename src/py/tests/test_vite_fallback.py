"""Tests for the Vite fallback subprocess manager."""

from pathlib import Path
from unittest.mock import patch

from vertebra.builder._fallback import ViteDevServer, run_vite_build


class TestViteDevServer:
    def test_proxy_url(self) -> None:
        server = ViteDevServer(Path("/tmp"), 5173)
        assert server.proxy_url == "http://127.0.0.1:5173"

    def test_not_running_initially(self) -> None:
        server = ViteDevServer(Path("/tmp"), 5173)
        assert not server.is_running

    def test_stop_when_not_started(self) -> None:
        """Stopping a server that was never started should be a no-op."""
        server = ViteDevServer(Path("/tmp"), 5173)
        server.stop()  # should not raise
        assert not server.is_running

    def test_proxy_url_custom_port(self) -> None:
        server = ViteDevServer(Path("/tmp"), 3000)
        assert server.proxy_url == "http://127.0.0.1:3000"

    def test_resolve_command_no_vite_no_npx(self) -> None:
        """Should raise RuntimeError when neither vite nor npx is found."""
        server = ViteDevServer(Path("/tmp"), 5173)
        with patch("shutil.which", return_value=None):
            try:
                server._resolve_command()  # noqa: SLF001
            except RuntimeError as exc:
                assert "Vite not found" in str(exc)
            else:
                raise AssertionError("Expected RuntimeError")

    def test_resolve_command_uses_vite(self) -> None:
        server = ViteDevServer(Path("/tmp"), 5173)
        with patch("shutil.which", side_effect=lambda x: "/usr/bin/vite" if x == "vite" else None):
            cmd = server._resolve_command()  # noqa: SLF001
            assert "/usr/bin/vite" in cmd
            assert "--port 5173" in cmd

    def test_resolve_command_falls_back_to_npx(self) -> None:
        server = ViteDevServer(Path("/tmp"), 5173)

        def mock_which(name: str) -> str | None:
            if name == "npx":
                return "/usr/bin/npx"
            return None

        with patch("shutil.which", side_effect=mock_which):
            cmd = server._resolve_command()  # noqa: SLF001
            assert "npx" in cmd
            assert "vite" in cmd


class TestRunViteBuild:
    def test_raises_when_vite_missing(self) -> None:
        with patch("shutil.which", return_value=None):
            try:
                run_vite_build(Path("/tmp"))
            except RuntimeError as exc:
                assert "Vite not found" in str(exc)
            else:
                raise AssertionError("Expected RuntimeError")
