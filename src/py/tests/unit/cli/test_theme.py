"""Tests for CLI theme module."""

import re

from vertebra.cli.theme import (
    CHARS,
    CHARS_ASCII,
    COLORS,
    COLORS_256,
    COLORS_BASIC,
    COLORS_RGB,
    TerminalCaps,
    apply_color,
    get_chars,
    get_status_color,
)


class TestColors:
    """Test color definitions."""

    def test_colors_rgb_are_valid_tuples(self) -> None:
        """All RGB colors should be 3-tuples of integers 0-255."""
        for name, rgb in COLORS_RGB.items():
            assert isinstance(rgb, tuple), f"{name} should be a tuple"
            assert len(rgb) == 3, f"{name} should have 3 components"
            for component in rgb:
                assert isinstance(component, int), f"{name} components should be integers"
                assert 0 <= component <= 255, f"{name} components should be 0-255"

    def test_colors_hex_are_valid(self) -> None:
        """All hex colors should be valid #RRGGBB format."""
        hex_pattern = re.compile(r"^#[0-9a-f]{6}$")
        for name, color in COLORS.items():
            assert hex_pattern.match(color), f"{name} should be #RRGGBB format, got {color}"

    def test_colors_hex_match_rgb(self) -> None:
        """Hex colors should be derived from RGB tuples."""
        for name in COLORS_RGB:
            r, g, b = COLORS_RGB[name]
            expected = f"#{r:02x}{g:02x}{b:02x}"
            assert COLORS[name] == expected, f"{name}: expected {expected}, got {COLORS[name]}"

    def test_colors_256_are_valid(self) -> None:
        """256-color codes should be 0-255."""
        for name, code in COLORS_256.items():
            assert isinstance(code, int), f"{name} should be an integer"
            assert 0 <= code <= 255, f"{name} should be 0-255"

    def test_colors_basic_are_valid(self) -> None:
        """Basic ANSI colors should be valid names."""
        valid_colors = {"black", "red", "green", "yellow", "blue", "magenta", "cyan", "white"}
        for name, color in COLORS_BASIC.items():
            assert color in valid_colors, f"{name}: {color} is not a valid ANSI color"

    def test_all_color_dicts_have_same_keys(self) -> None:
        """All color dictionaries should have the same keys."""
        keys = set(COLORS_RGB.keys())
        assert set(COLORS.keys()) == keys
        assert set(COLORS_256.keys()) == keys
        assert set(COLORS_BASIC.keys()) == keys


class TestChars:
    """Test character definitions."""

    def test_chars_have_ascii_fallbacks(self) -> None:
        """All Unicode characters should have ASCII fallbacks."""
        for name in CHARS:
            assert name in CHARS_ASCII, f"{name} missing ASCII fallback"

    def test_ascii_chars_are_ascii(self) -> None:
        """ASCII fallbacks should only contain ASCII characters."""
        for name, char in CHARS_ASCII.items():
            assert all(ord(c) < 128 for c in char), f"{name} contains non-ASCII"


class TestTerminalCaps:
    """Test terminal capability detection."""

    def test_terminal_caps_color_mode_none_for_non_tty(self) -> None:
        """Non-TTY terminals should have 'none' color mode."""
        caps = TerminalCaps(
            is_tty=False, supports_unicode=True, supports_256_color=True, supports_true_color=True, width=80
        )
        assert caps.color_mode == "none"

    def test_terminal_caps_color_mode_true(self) -> None:
        """True color terminals should have 'true' color mode."""
        caps = TerminalCaps(
            is_tty=True, supports_unicode=True, supports_256_color=True, supports_true_color=True, width=80
        )
        assert caps.color_mode == "true"

    def test_terminal_caps_color_mode_256(self) -> None:
        """256-color terminals should have '256' color mode."""
        caps = TerminalCaps(
            is_tty=True, supports_unicode=True, supports_256_color=True, supports_true_color=False, width=80
        )
        assert caps.color_mode == "256"

    def test_terminal_caps_color_mode_basic(self) -> None:
        """Basic terminals should have 'basic' color mode."""
        caps = TerminalCaps(
            is_tty=True, supports_unicode=False, supports_256_color=False, supports_true_color=False, width=80
        )
        assert caps.color_mode == "basic"


class TestGetChars:
    """Test character selection based on capabilities."""

    def test_get_chars_unicode(self) -> None:
        """Unicode-capable terminals should get Unicode chars."""
        caps = TerminalCaps(
            is_tty=True, supports_unicode=True, supports_256_color=True, supports_true_color=True, width=80
        )
        assert get_chars(caps) == CHARS

    def test_get_chars_ascii(self) -> None:
        """Non-Unicode terminals should get ASCII chars."""
        caps = TerminalCaps(
            is_tty=True, supports_unicode=False, supports_256_color=True, supports_true_color=True, width=80
        )
        assert get_chars(caps) == CHARS_ASCII


class TestApplyColor:
    """Test color application functions."""

    def test_apply_color_none_mode(self) -> None:
        """None mode should return plain text."""
        caps = TerminalCaps(
            is_tty=False, supports_unicode=True, supports_256_color=True, supports_true_color=True, width=80
        )
        result = apply_color("test", "success", caps)
        assert result == "test"
        assert "\033[" not in result

    def test_apply_color_true_mode(self) -> None:
        """True color mode should return RGB ANSI codes."""
        caps = TerminalCaps(
            is_tty=True, supports_unicode=True, supports_256_color=True, supports_true_color=True, width=80
        )
        result = apply_color("test", "success", caps)
        assert "\033[38;2;" in result
        assert "test" in result

    def test_apply_color_256_mode(self) -> None:
        """256 color mode should return 256-color ANSI codes."""
        caps = TerminalCaps(
            is_tty=True, supports_unicode=True, supports_256_color=True, supports_true_color=False, width=80
        )
        result = apply_color("test", "success", caps)
        assert "\033[38;5;" in result
        assert "test" in result


class TestGetStatusColor:
    """Test status to color mapping."""

    def test_ready_states_are_success(self) -> None:
        """Ready-like states should map to success color."""
        for status in ["ready", "connected", "healthy", "READY", "Connected"]:
            assert get_status_color(status) == "success"

    def test_warning_states_are_warning(self) -> None:
        """Warning-like states should map to warning color."""
        for status in ["starting", "degraded", "warning", "STARTING"]:
            assert get_status_color(status) == "warning"

    def test_error_states_are_error(self) -> None:
        """Error-like states should map to error color."""
        for status in ["error", "failed", "disconnected", "ERROR"]:
            assert get_status_color(status) == "error"

    def test_unknown_states_are_muted(self) -> None:
        """Unknown states should map to muted color."""
        for status in ["unknown", "pending", "whatever"]:
            assert get_status_color(status) == "muted"
