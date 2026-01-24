"""Logo variants for responsive banner rendering.

Three variants are provided:
1. LOGO_FULL: Full Unicode block art (width >= 100 columns)
2. LOGO_COMPACT: Abbreviated block letters (width 60-99 columns)
3. LOGO_MINIMAL: Single stylized "V" (width < 60 columns)
"""

from __future__ import annotations

# Full logo using Unicode block art - 35 characters wide
# Uses: █ ░ ▀ ▄ for block construction
LOGO_FULL = """\
█░█ █▀▀ █▀█ ▀█▀ █▀▀ █▄▄ █▀█ ▄▀█
▀▄▀ ██▄ █▀▄  █  ██▄ █▄█ █▀▄ █▀█"""

# Compact logo - same as full, but used when we need tighter layout
LOGO_COMPACT = LOGO_FULL

# Minimal logo - stylized "V" block, only 3 characters wide
LOGO_MINIMAL = "▐█▌"


def get_logo_variant(width: int) -> str:
    """Select logo variant based on terminal width.

    Args:
        width: Terminal width in columns

    Returns:
        Logo variant name: "full", "compact", or "minimal"
    """
    if width >= 100:
        return "full"
    if width >= 60:
        return "compact"
    return "minimal"


def get_logo(variant: str) -> str:
    """Get logo string for the specified variant.

    Args:
        variant: One of "full", "compact", "minimal"

    Returns:
        Logo string (may be multiline)
    """
    if variant == "minimal":
        return LOGO_MINIMAL
    if variant == "compact":
        return LOGO_COMPACT
    return LOGO_FULL


def get_logo_lines(variant: str) -> list[str]:
    """Get logo as list of lines.

    Args:
        variant: One of "full", "compact", "minimal"

    Returns:
        List of logo lines
    """
    return get_logo(variant).splitlines()
