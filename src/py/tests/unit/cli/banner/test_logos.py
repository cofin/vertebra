"""Tests for banner logos module."""

from __future__ import annotations

from vertebra.cli.banner.logos import LOGO_COMPACT, LOGO_FULL, LOGO_MINIMAL, get_logo, get_logo_lines, get_logo_variant


class TestLogoConstants:
    """Test logo constant definitions."""

    def test_logo_full_is_multiline(self) -> None:
        assert "\n" in LOGO_FULL
        lines = LOGO_FULL.splitlines()
        assert len(lines) == 2

    def test_logo_compact_is_multiline(self) -> None:
        assert "\n" in LOGO_COMPACT
        lines = LOGO_COMPACT.splitlines()
        assert len(lines) == 2

    def test_logo_minimal_is_single_line(self) -> None:
        assert "\n" not in LOGO_MINIMAL
        assert len(LOGO_MINIMAL) == 3  # "▐█▌"


class TestGetLogoVariant:
    """Test logo variant selection based on width."""

    def test_full_at_100_columns(self) -> None:
        assert get_logo_variant(100) == "full"

    def test_full_at_120_columns(self) -> None:
        assert get_logo_variant(120) == "full"

    def test_full_at_200_columns(self) -> None:
        assert get_logo_variant(200) == "full"

    def test_compact_at_99_columns(self) -> None:
        assert get_logo_variant(99) == "compact"

    def test_compact_at_80_columns(self) -> None:
        assert get_logo_variant(80) == "compact"

    def test_compact_at_60_columns(self) -> None:
        assert get_logo_variant(60) == "compact"

    def test_minimal_at_59_columns(self) -> None:
        assert get_logo_variant(59) == "minimal"

    def test_minimal_at_40_columns(self) -> None:
        assert get_logo_variant(40) == "minimal"

    def test_minimal_at_very_narrow(self) -> None:
        assert get_logo_variant(20) == "minimal"


class TestGetLogo:
    """Test logo retrieval by variant name."""

    def test_get_full_logo(self) -> None:
        assert get_logo("full") == LOGO_FULL

    def test_get_compact_logo(self) -> None:
        assert get_logo("compact") == LOGO_COMPACT

    def test_get_minimal_logo(self) -> None:
        assert get_logo("minimal") == LOGO_MINIMAL

    def test_unknown_variant_returns_full(self) -> None:
        # Default to full for unknown variants
        assert get_logo("unknown") == LOGO_FULL


class TestGetLogoLines:
    """Test logo retrieval as list of lines."""

    def test_full_logo_has_two_lines(self) -> None:
        lines = get_logo_lines("full")
        assert len(lines) == 2

    def test_compact_logo_has_two_lines(self) -> None:
        lines = get_logo_lines("compact")
        assert len(lines) == 2

    def test_minimal_logo_has_one_line(self) -> None:
        lines = get_logo_lines("minimal")
        assert len(lines) == 1
        assert lines[0] == LOGO_MINIMAL
