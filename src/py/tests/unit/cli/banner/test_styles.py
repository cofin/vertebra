"""Tests for CLI theme module."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.cli.theme import (
    CHARS,
    CHARS_ASCII,
    COLORS_256,
    COLORS_BASIC,
    COLORS_RGB,
    TerminalCaps,
    apply_color,
    color_256,
    color_rgb,
    detect_terminal_capabilities,
    get_chars,
    get_status_color,
)

if TYPE_CHECKING:
    import pytest


class TestColorConstants:
    """Test color constant definitions."""

    def test_colors_rgb_has_required_keys(self) -> None:
        required = ["slate", "gold", "success", "warning", "error", "muted", "text", "dim"]
        for key in required:
            assert key in COLORS_RGB
            assert isinstance(COLORS_RGB[key], tuple)
            assert len(COLORS_RGB[key]) == 3

    def test_colors_256_has_required_keys(self) -> None:
        required = ["slate", "gold", "success", "warning", "error", "muted", "text", "dim"]
        for key in required:
            assert key in COLORS_256
            assert isinstance(COLORS_256[key], int)

    def test_colors_basic_has_required_keys(self) -> None:
        required = ["slate", "gold", "success", "warning", "error", "muted", "text", "dim"]
        for key in required:
            assert key in COLORS_BASIC
            assert isinstance(COLORS_BASIC[key], str)


class TestCharConstants:
    """Test character constant definitions."""

    def test_chars_has_required_keys(self) -> None:
        required = ["status_ready", "status_empty", "arrow", "line_heavy", "line_light", "dots"]
        for key in required:
            assert key in CHARS
            assert isinstance(CHARS[key], str)

    def test_chars_ascii_has_required_keys(self) -> None:
        required = ["status_ready", "status_empty", "arrow", "line_heavy", "line_light", "dots"]
        for key in required:
            assert key in CHARS_ASCII
            assert isinstance(CHARS_ASCII[key], str)


class TestTerminalCaps:
    """Test TerminalCaps dataclass."""

    def test_color_mode_none_when_not_tty(self) -> None:
        caps = TerminalCaps(
            is_tty=False, supports_unicode=True, supports_256_color=True, supports_true_color=True, width=120
        )
        assert caps.color_mode == "none"

    def test_color_mode_true_when_true_color_supported(self) -> None:
        caps = TerminalCaps(
            is_tty=True, supports_unicode=True, supports_256_color=True, supports_true_color=True, width=120
        )
        assert caps.color_mode == "true"

    def test_color_mode_256_when_only_256_supported(self) -> None:
        caps = TerminalCaps(
            is_tty=True, supports_unicode=True, supports_256_color=True, supports_true_color=False, width=120
        )
        assert caps.color_mode == "256"

    def test_color_mode_basic_when_minimal_support(self) -> None:
        caps = TerminalCaps(
            is_tty=True, supports_unicode=False, supports_256_color=False, supports_true_color=False, width=80
        )
        assert caps.color_mode == "basic"


class TestDetectTerminalCapabilities:
    """Test terminal capability detection."""

    def test_detects_non_tty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("sys.stdout.isatty", lambda: False)
        caps = detect_terminal_capabilities()
        assert caps.is_tty is False

    def test_detects_tty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("sys.stdout.isatty", lambda: True)
        caps = detect_terminal_capabilities()
        assert caps.is_tty is True

    def test_detects_unicode_from_lang(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("LANG", "en_US.UTF-8")
        caps = detect_terminal_capabilities()
        assert caps.supports_unicode is True

    def test_no_unicode_without_utf8_lang(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("LANG", "C")
        caps = detect_terminal_capabilities()
        assert caps.supports_unicode is False

    def test_detects_true_color_from_colorterm(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("COLORTERM", "truecolor")
        caps = detect_terminal_capabilities()
        assert caps.supports_true_color is True

    def test_detects_24bit_color_from_colorterm(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("COLORTERM", "24bit")
        caps = detect_terminal_capabilities()
        assert caps.supports_true_color is True

    def test_detects_256_color_from_term(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TERM", "xterm-256color")
        monkeypatch.setenv("COLORTERM", "")
        caps = detect_terminal_capabilities()
        assert caps.supports_256_color is True
        assert caps.supports_true_color is False


class TestGetChars:
    """Test character set selection."""

    def test_returns_unicode_chars_when_supported(self) -> None:
        caps = TerminalCaps(
            is_tty=True, supports_unicode=True, supports_256_color=True, supports_true_color=False, width=80
        )
        chars = get_chars(caps)
        assert chars == CHARS

    def test_returns_ascii_chars_when_unicode_not_supported(self) -> None:
        caps = TerminalCaps(
            is_tty=True, supports_unicode=False, supports_256_color=True, supports_true_color=False, width=80
        )
        chars = get_chars(caps)
        assert chars == CHARS_ASCII


class TestColorFunctions:
    """Test color application functions."""

    def test_color_rgb_applies_ansi_codes(self) -> None:
        result = color_rgb("test", 255, 128, 64)
        assert result == "\033[38;2;255;128;64mtest\033[0m"

    def test_color_256_applies_ansi_codes(self) -> None:
        result = color_256("test", 178)
        assert result == "\033[38;5;178mtest\033[0m"

    def test_apply_color_returns_plain_text_when_no_color(self) -> None:
        caps = TerminalCaps(
            is_tty=False, supports_unicode=True, supports_256_color=True, supports_true_color=True, width=80
        )
        result = apply_color("test", "gold", caps)
        assert result == "test"

    def test_apply_color_uses_true_color_when_supported(self) -> None:
        caps = TerminalCaps(
            is_tty=True, supports_unicode=True, supports_256_color=True, supports_true_color=True, width=80
        )
        result = apply_color("test", "gold", caps)
        # Should contain RGB escape sequence
        assert "\033[38;2;" in result

    def test_apply_color_uses_256_color_when_true_not_supported(self) -> None:
        caps = TerminalCaps(
            is_tty=True, supports_unicode=True, supports_256_color=True, supports_true_color=False, width=80
        )
        result = apply_color("test", "gold", caps)
        # Should contain 256-color escape sequence
        assert "\033[38;5;" in result


class TestGetStatusColor:
    """Test status to color mapping."""

    def test_ready_returns_success(self) -> None:
        assert get_status_color("ready") == "success"

    def test_connected_returns_success(self) -> None:
        assert get_status_color("connected") == "success"

    def test_healthy_returns_success(self) -> None:
        assert get_status_color("healthy") == "success"

    def test_starting_returns_warning(self) -> None:
        assert get_status_color("starting") == "warning"

    def test_degraded_returns_warning(self) -> None:
        assert get_status_color("degraded") == "warning"

    def test_error_returns_error(self) -> None:
        assert get_status_color("error") == "error"

    def test_failed_returns_error(self) -> None:
        assert get_status_color("failed") == "error"

    def test_unknown_returns_muted(self) -> None:
        assert get_status_color("unknown") == "muted"
        assert get_status_color("whatever") == "muted"

    def test_case_insensitive(self) -> None:
        assert get_status_color("READY") == "success"
        assert get_status_color("Ready") == "success"
