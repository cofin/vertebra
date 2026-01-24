"""Tests for banner renderer module."""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from vertebra.cli.banner.collector import ComponentStatus, ServerStatus
from vertebra.cli.banner.hooks import get_registry
from vertebra.cli.banner.renderer import VERSION, BannerConfig, BasicRenderer, TTYRenderer, render_banner
from vertebra.cli.banner.styles import TerminalCaps


@pytest.fixture(autouse=True)
def clear_registry() -> None:
    """Clear the hook registry before each test."""
    get_registry().clear()


class TestBannerConfig:
    """Test BannerConfig dataclass."""

    def test_serve_mode_config(self) -> None:
        config = BannerConfig(
            mode="serve",
            host="127.0.0.1",
            port=8000,
            workers=4,
            log_level="info",
        )
        assert config.mode == "serve"
        assert config.host == "127.0.0.1"
        assert config.port == 8000
        assert config.workers == 4
        assert config.log_level == "info"
        assert config.watched_paths is None

    def test_dev_mode_config(self) -> None:
        config = BannerConfig(
            mode="dev",
            host="0.0.0.0",  # noqa: S104
            port=3000,
            workers=1,
            log_level="debug",
            watched_paths=["src/", "app/"],
        )
        assert config.mode == "dev"
        assert config.watched_paths == ["src/", "app/"]


class TestBasicRenderer:
    """Test BasicRenderer (non-TTY) output."""

    @pytest.fixture
    def caps(self) -> TerminalCaps:
        return TerminalCaps(
            is_tty=False,
            supports_unicode=False,
            supports_256_color=False,
            supports_true_color=False,
            width=80,
        )

    @pytest.fixture
    def config(self) -> BannerConfig:
        return BannerConfig(
            mode="serve",
            host="127.0.0.1",
            port=8000,
            workers=4,
            log_level="info",
        )

    def test_outputs_json(self, caps: TerminalCaps, config: BannerConfig) -> None:
        renderer = BasicRenderer(caps)

        with patch("click.echo") as mock_echo:
            renderer.render(config)
            output = mock_echo.call_args[0][0]

        # Should be valid JSON
        data = json.loads(output)
        assert data["event"] == "server.startup"
        assert data["version"] == VERSION

    def test_includes_config_in_json(
        self, caps: TerminalCaps, config: BannerConfig
    ) -> None:
        renderer = BasicRenderer(caps)

        with patch("click.echo") as mock_echo:
            renderer.render(config)
            output = mock_echo.call_args[0][0]

        data = json.loads(output)
        assert data["host"] == "127.0.0.1"
        assert data["port"] == 8000
        assert data["workers"] == 4
        assert data["log_level"] == "info"
        assert data["mode"] == "serve"

    def test_includes_starting_components_without_status(
        self, caps: TerminalCaps, config: BannerConfig
    ) -> None:
        renderer = BasicRenderer(caps)

        with patch("click.echo") as mock_echo:
            renderer.render(config, status=None)
            output = mock_echo.call_args[0][0]

        data = json.loads(output)
        assert data["components"]["bun"] == "starting"
        assert data["components"]["ipc"] == "starting"
        assert data["components"]["broker"] == "starting"

    def test_includes_component_status(
        self, caps: TerminalCaps, config: BannerConfig
    ) -> None:
        renderer = BasicRenderer(caps)
        status = ServerStatus(
            bun=ComponentStatus(status="ready", version="1.1.38"),
            broker=ComponentStatus(status="ready"),
            ipc=ComponentStatus(status="ready", channels=4),
        )

        with patch("click.echo") as mock_echo:
            renderer.render(config, status=status)
            output = mock_echo.call_args[0][0]

        data = json.loads(output)
        assert data["components"]["bun"] == "ready"
        assert data["components"]["ipc"] == "ready"
        assert data["components"]["broker"] == "ready"


class TestTTYRenderer:
    """Test TTYRenderer output."""

    @pytest.fixture
    def caps_full(self) -> TerminalCaps:
        return TerminalCaps(
            is_tty=True,
            supports_unicode=True,
            supports_256_color=True,
            supports_true_color=False,
            width=120,
        )

    @pytest.fixture
    def caps_compact(self) -> TerminalCaps:
        return TerminalCaps(
            is_tty=True,
            supports_unicode=True,
            supports_256_color=True,
            supports_true_color=False,
            width=80,
        )

    @pytest.fixture
    def caps_minimal(self) -> TerminalCaps:
        return TerminalCaps(
            is_tty=True,
            supports_unicode=True,
            supports_256_color=True,
            supports_true_color=False,
            width=50,
        )

    @pytest.fixture
    def config(self) -> BannerConfig:
        return BannerConfig(
            mode="serve",
            host="127.0.0.1",
            port=8000,
            workers=4,
            log_level="info",
        )

    def test_full_width_includes_logo(
        self, caps_full: TerminalCaps, config: BannerConfig
    ) -> None:
        renderer = TTYRenderer(caps_full)
        output_lines: list[str] = []

        with patch("click.echo", side_effect=lambda x="": output_lines.append(x)):
            renderer.render(config)

        output = "\n".join(str(line) for line in output_lines)
        # Logo should contain block characters
        assert "█" in output or "▀" in output

    def test_full_width_includes_sections(
        self, caps_full: TerminalCaps, config: BannerConfig
    ) -> None:
        renderer = TTYRenderer(caps_full)
        output_lines: list[str] = []

        with patch("click.echo", side_effect=lambda x="": output_lines.append(x)):
            renderer.render(config)

        output = "\n".join(str(line) for line in output_lines)
        assert "Server" in output
        assert "Components" in output

    def test_full_width_shows_url(
        self, caps_full: TerminalCaps, config: BannerConfig
    ) -> None:
        renderer = TTYRenderer(caps_full)
        output_lines: list[str] = []

        with patch("click.echo", side_effect=lambda x="": output_lines.append(x)):
            renderer.render(config)

        output = "\n".join(str(line) for line in output_lines)
        assert "http://127.0.0.1:8000" in output

    def test_compact_width_layout(
        self, caps_compact: TerminalCaps, config: BannerConfig
    ) -> None:
        renderer = TTYRenderer(caps_compact)
        output_lines: list[str] = []

        with patch("click.echo", side_effect=lambda x="": output_lines.append(x)):
            renderer.render(config)

        output = "\n".join(str(line) for line in output_lines)
        # Should still have URL but in compact format
        assert "127.0.0.1:8000" in output

    def test_minimal_width_layout(
        self, caps_minimal: TerminalCaps, config: BannerConfig
    ) -> None:
        renderer = TTYRenderer(caps_minimal)
        output_lines: list[str] = []

        with patch("click.echo", side_effect=lambda x="": output_lines.append(x)):
            renderer.render(config)

        output = "\n".join(str(line) for line in output_lines)
        # Should show VERTEBRA text
        assert "VERTEBRA" in output
        assert "127.0.0.1:8000" in output

    def test_dev_mode_shows_dev_badge(self, caps_full: TerminalCaps) -> None:
        config = BannerConfig(
            mode="dev",
            host="127.0.0.1",
            port=8000,
            workers=1,
            log_level="debug",
            watched_paths=["src/"],
        )
        renderer = TTYRenderer(caps_full)
        output_lines: list[str] = []

        with patch("click.echo", side_effect=lambda x="": output_lines.append(x)):
            renderer.render(config)

        output = "\n".join(str(line) for line in output_lines)
        assert "[DEV]" in output

    def test_dev_mode_shows_development_section(self, caps_full: TerminalCaps) -> None:
        config = BannerConfig(
            mode="dev",
            host="127.0.0.1",
            port=8000,
            workers=1,
            log_level="debug",
            watched_paths=["src/"],
        )
        renderer = TTYRenderer(caps_full)
        output_lines: list[str] = []

        with patch("click.echo", side_effect=lambda x="": output_lines.append(x)):
            renderer.render(config)

        output = "\n".join(str(line) for line in output_lines)
        assert "Development" in output
        assert "__debug__" in output

    def test_shows_component_status(
        self, caps_full: TerminalCaps, config: BannerConfig
    ) -> None:
        renderer = TTYRenderer(caps_full)
        status = ServerStatus(
            bun=ComponentStatus(status="ready", version="1.1.38"),
            broker=ComponentStatus(status="ready"),
            ipc=ComponentStatus(status="ready", channels=4),
        )
        output_lines: list[str] = []

        with patch("click.echo", side_effect=lambda x="": output_lines.append(x)):
            renderer.render(config, status=status)

        output = "\n".join(str(line) for line in output_lines)
        assert "ready" in output
        assert "1.1.38" in output


class TestRenderBanner:
    """Test the main render_banner function."""

    def test_uses_tty_renderer_when_tty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("sys.stdout.isatty", lambda: True)
        monkeypatch.setenv("LANG", "en_US.UTF-8")

        with patch.object(TTYRenderer, "render") as mock_render:
            render_banner(host="127.0.0.1", port=8000)
            mock_render.assert_called_once()

    def test_uses_basic_renderer_when_not_tty(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("sys.stdout.isatty", lambda: False)

        with patch.object(BasicRenderer, "render") as mock_render:
            render_banner(host="127.0.0.1", port=8000)
            mock_render.assert_called_once()

    def test_passes_config_to_renderer(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("sys.stdout.isatty", lambda: False)

        with patch.object(BasicRenderer, "render") as mock_render:
            render_banner(
                mode="dev",
                host="0.0.0.0",  # noqa: S104
                port=3000,
                workers=2,
                log_level="debug",
            )

            config = mock_render.call_args[0][0]
            assert config.mode == "dev"
            assert config.host == "0.0.0.0"  # noqa: S104
            assert config.port == 3000
            assert config.workers == 2
            assert config.log_level == "debug"


class TestTTYRendererEdgeCases:
    """Test edge cases and less common code paths in TTYRenderer."""

    @pytest.fixture
    def caps(self) -> TerminalCaps:
        return TerminalCaps(
            is_tty=True,
            supports_unicode=True,
            supports_256_color=True,
            supports_true_color=False,
            width=120,
        )

    @pytest.fixture
    def config(self) -> BannerConfig:
        return BannerConfig(
            mode="serve",
            host="127.0.0.1",
            port=8000,
            workers=4,
            log_level="info",
        )

    def test_shows_broker_with_pools(self, caps: TerminalCaps, config: BannerConfig) -> None:
        renderer = TTYRenderer(caps)
        status = ServerStatus(
            bun=ComponentStatus(status="ready"),
            broker=ComponentStatus(status="ready", pools={"sql": 10, "redis": 5}),
            ipc=ComponentStatus(status="ready"),
        )
        output_lines: list[str] = []

        with patch("click.echo", side_effect=lambda x="": output_lines.append(x)):
            renderer.render(config, status=status)

        output = "\n".join(str(line) for line in output_lines)
        # Should show pool counts
        assert "sql" in output.lower() or "10" in output

    def test_shows_ipc_with_channels(self, caps: TerminalCaps, config: BannerConfig) -> None:
        renderer = TTYRenderer(caps)
        status = ServerStatus(
            bun=ComponentStatus(status="ready"),
            broker=ComponentStatus(status="ready"),
            ipc=ComponentStatus(status="ready", channels=8),
        )
        output_lines: list[str] = []

        with patch("click.echo", side_effect=lambda x="": output_lines.append(x)):
            renderer.render(config, status=status)

        output = "\n".join(str(line) for line in output_lines)
        # Should show channel count
        assert "8" in output or "channels" in output.lower()

    def test_get_overall_status_when_bun_not_ready(self, caps: TerminalCaps) -> None:
        renderer = TTYRenderer(caps)
        status = ServerStatus(
            bun=ComponentStatus(status="starting"),
            broker=ComponentStatus(status="ready"),
            ipc=ComponentStatus(status="ready"),
        )

        result = renderer._get_overall_status(status)
        assert result == "starting"

    def test_get_overall_status_when_ipc_not_ready(self, caps: TerminalCaps) -> None:
        renderer = TTYRenderer(caps)
        status = ServerStatus(
            bun=ComponentStatus(status="ready"),
            broker=ComponentStatus(status="ready"),
            ipc=ComponentStatus(status="error"),
        )

        result = renderer._get_overall_status(status)
        assert result == "error"

    def test_get_overall_status_when_all_ready(self, caps: TerminalCaps) -> None:
        renderer = TTYRenderer(caps)
        status = ServerStatus(
            bun=ComponentStatus(status="ready"),
            broker=ComponentStatus(status="ready"),
            ipc=ComponentStatus(status="ready"),
        )

        result = renderer._get_overall_status(status)
        assert result == "ready"

    def test_get_status_details_with_no_details(self, caps: TerminalCaps) -> None:
        renderer = TTYRenderer(caps)
        status = ServerStatus(
            bun=ComponentStatus(status="ready"),
            broker=ComponentStatus(status="ready"),
            ipc=ComponentStatus(status="ready"),
        )

        # Test component with no extra details
        result = renderer._get_status_details(status, "broker")
        assert result == ""

    def test_get_status_details_when_status_none(self, caps: TerminalCaps) -> None:
        renderer = TTYRenderer(caps)

        result = renderer._get_status_details(None, "bun")
        assert result == ""

    def test_compact_layout_with_dev_mode(self, config: BannerConfig) -> None:
        caps = TerminalCaps(
            is_tty=True,
            supports_unicode=True,
            supports_256_color=True,
            supports_true_color=False,
            width=80,
        )
        dev_config = BannerConfig(
            mode="dev",
            host="127.0.0.1",
            port=8000,
            workers=1,
            log_level="debug",
        )
        renderer = TTYRenderer(caps)
        output_lines: list[str] = []

        with patch("click.echo", side_effect=lambda x="": output_lines.append(x)):
            renderer.render(dev_config)

        output = "\n".join(str(line) for line in output_lines)
        assert "[DEV]" in output

    def test_minimal_layout_with_dev_mode(self) -> None:
        caps = TerminalCaps(
            is_tty=True,
            supports_unicode=True,
            supports_256_color=True,
            supports_true_color=False,
            width=50,
        )
        dev_config = BannerConfig(
            mode="dev",
            host="127.0.0.1",
            port=8000,
            workers=1,
            log_level="debug",
        )
        renderer = TTYRenderer(caps)
        output_lines: list[str] = []

        with patch("click.echo", side_effect=lambda x="": output_lines.append(x)):
            renderer.render(dev_config)

        output = "\n".join(str(line) for line in output_lines)
        assert "[DEV]" in output

    def test_get_status_text_when_component_none(self, caps: TerminalCaps) -> None:
        renderer = TTYRenderer(caps)
        # Create status with None component (edge case)
        status = ServerStatus(
            bun=ComponentStatus(status="ready"),
            broker=ComponentStatus(status="ready"),
            ipc=ComponentStatus(status="ready"),
        )

        # Test with a non-existent component
        result = renderer._get_status_text(status, "nonexistent")
        assert result == "unknown"


class TestBasicRendererExtensions:
    """Test BasicRenderer with extensions."""

    @pytest.fixture
    def caps(self) -> TerminalCaps:
        return TerminalCaps(
            is_tty=False,
            supports_unicode=False,
            supports_256_color=False,
            supports_true_color=False,
            width=80,
        )

    @pytest.fixture
    def config(self) -> BannerConfig:
        return BannerConfig(
            mode="serve",
            host="127.0.0.1",
            port=8000,
            workers=4,
            log_level="info",
        )

    def test_includes_extensions_in_json(self, caps: TerminalCaps, config: BannerConfig) -> None:
        renderer = BasicRenderer(caps)

        # Register an extension
        from vertebra.cli.banner.hooks import banner_section

        @banner_section("Custom")
        def custom_section() -> dict[str, str]:
            return {"key": "value"}

        with patch("click.echo") as mock_echo:
            renderer.render(config)
            output = mock_echo.call_args[0][0]

        data = json.loads(output)
        assert "extensions" in data
        assert "Custom" in data["extensions"]
