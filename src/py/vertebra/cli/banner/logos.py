"""Logo variants for responsive banner rendering.

Two variants based on terminal width:
1. ``LOGO_FULL``: Block-letter VERTEBRA (width >= 80 columns)
2. ``LOGO_COMPACT``: Stylized V mark (width < 80 columns)
"""

__all__ = ("LOGO_COMPACT", "LOGO_FULL", "LOGO_WIDTH_THRESHOLD", "get_logo")

LOGO_FULL = """\
█░█ █▀▀ █▀█ ▀█▀ █▀▀ █▄▄ █▀█ ▄▀█
▀▄▀ ██▄ █▀▄  █  ██▄ █▄█ █▀▄ █▀█"""

LOGO_COMPACT = "▜▄▛"

LOGO_WIDTH_THRESHOLD = 80


def get_logo(width: int) -> str:
    """Select the logo variant based on terminal width."""
    if width >= LOGO_WIDTH_THRESHOLD:
        return LOGO_FULL
    return LOGO_COMPACT
