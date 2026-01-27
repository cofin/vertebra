"""Centralized theme definitions for Vertebra CLI.

This module is the single source of truth for all CLI styling:
- Colors (RGB tuples, 256-color codes, basic ANSI, and hex strings)
- Characters (Unicode with ASCII fallbacks)
- Terminal capability detection

The banner system and all CLI commands import from here.
"""

from __future__ import annotations

import os
import shutil
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal

__all__ = [
    # Character sets
    "CHARS",
    "CHARS_ASCII",
    # Hex color dict for rich-click
    "COLORS",
    "COLORS_256",
    "COLORS_BASIC",
    # RGB/256/basic color dicts
    "COLORS_RGB",
    # Terminal capabilities
    "TerminalCaps",
    # Color utilities
    "apply_color",
    "color_256",
    "color_rgb",
    "detect_terminal_capabilities",
    "get_chars",
    "get_status_color",
]

# =============================================================================
# Color Definitions
# =============================================================================
# The color scheme follows Litestar branding guidelines:
# - Primary: Deep slate blue (#1e3a5f)
# - Accent: Gold (#d4af37) - used sparingly for version and [DEV] badge only
# - Status colors: Green (ready), Yellow (warning), Red (error)

# RGB color tuples for 24-bit true color terminals
COLORS_RGB: dict[str, tuple[int, int, int]] = {
    # Primary colors
    "slate": (30, 58, 95),  # #1e3a5f - deep blue
    "gold": (212, 175, 55),  # #d4af37 - accent gold (use sparingly)
    # Status colors
    "success": (74, 222, 128),  # #4ade80 - green
    "warning": (250, 204, 21),  # #facc15 - yellow
    "error": (248, 113, 113),  # #f87171 - red
    "muted": (148, 163, 184),  # #94a3b8 - gray
    "info": (56, 189, 248),  # #38bdf8 - sky blue
    # Text colors
    "text": (241, 245, 249),  # #f1f5f9 - light text
    "dim": (100, 116, 139),  # #64748b - dim text
}

# 256-color palette codes (approximate matches)
COLORS_256: dict[str, int] = {
    "slate": 24,  # closest to #1e3a5f
    "gold": 178,  # closest to #d4af37
    "success": 78,  # closest to #4ade80
    "warning": 220,  # closest to #facc15
    "error": 203,  # closest to #f87171
    "muted": 145,  # closest to #94a3b8
    "info": 75,  # closest to #38bdf8
    "text": 255,  # white
    "dim": 102,  # gray
}

# Basic ANSI color fallbacks
COLORS_BASIC: dict[str, str] = {
    "slate": "blue",
    "gold": "yellow",
    "success": "green",
    "warning": "yellow",
    "error": "red",
    "muted": "white",
    "info": "blue",
    "text": "white",
    "dim": "white",
}

# Hex color strings for rich-click compatibility
# Example: (30, 58, 95) -> "#1e3a5f"
COLORS: dict[str, str] = {name: f"#{r:02x}{g:02x}{b:02x}" for name, (r, g, b) in COLORS_RGB.items()}

# =============================================================================
# Character Definitions
# =============================================================================

# Unicode characters for rendering
CHARS: dict[str, str] = {
    # Status indicators
    "status_ready": "●",  # U+25CF filled circle
    "status_empty": "○",  # U+25CB empty circle
    "arrow": "▸",  # U+25B8 right-pointing triangle
    # Separators
    "line_heavy": "━",  # U+2501 heavy horizontal
    "line_light": "─",  # U+2500 light horizontal
    "dots": "·",  # U+00B7 middle dot (for leader lines)
}

# ASCII fallback characters
CHARS_ASCII: dict[str, str] = {
    "status_ready": "*",
    "status_empty": "o",
    "arrow": ">",
    "line_heavy": "=",
    "line_light": "-",
    "dots": ".",
}

# =============================================================================
# Terminal Capability Detection
# =============================================================================


@dataclass(frozen=True, slots=True)
class TerminalCaps:
    """Terminal capabilities detection result."""

    is_tty: bool
    supports_unicode: bool
    supports_256_color: bool
    supports_true_color: bool
    width: int

    @property
    def color_mode(self) -> Literal["true", "256", "basic", "none"]:
        """Get the best color mode for this terminal."""
        if not self.is_tty:
            return "none"
        if self.supports_true_color:
            return "true"
        if self.supports_256_color:
            return "256"
        return "basic"


def detect_terminal_capabilities() -> TerminalCaps:
    """Detect terminal capabilities for banner rendering.

    Examines environment variables and terminal state to determine:
    - Whether stdout is a TTY
    - Unicode support (via LANG environment)
    - Color depth (true color, 256, basic, or none)
    - Terminal width for responsive layout
    """
    is_tty = sys.stdout.isatty()
    term = os.environ.get("TERM", "")
    colorterm = os.environ.get("COLORTERM", "")
    lang = os.environ.get("LANG", "")

    # Unicode detection: check for UTF-8 in LANG
    supports_unicode = "UTF-8" in lang.upper() or "UTF8" in lang.upper()

    # Color detection
    supports_true_color = colorterm in ("truecolor", "24bit")
    supports_256_color = (
        "256color" in term
        or "xterm" in term
        or "screen" in term
        or "tmux" in term
        or is_tty  # Assume basic color support for any TTY
    )

    # Terminal width detection
    try:
        width = shutil.get_terminal_size().columns
    except (ValueError, OSError):
        width = 80  # Fallback to common default

    return TerminalCaps(
        is_tty=is_tty,
        supports_unicode=supports_unicode,
        supports_256_color=supports_256_color,
        supports_true_color=supports_true_color,
        width=width,
    )


def get_chars(caps: TerminalCaps) -> dict[str, str]:
    """Get appropriate character set based on terminal capabilities."""
    if caps.supports_unicode:
        return CHARS
    return CHARS_ASCII


# =============================================================================
# Color Application Utilities
# =============================================================================


def color_rgb(text: str, r: int, g: int, b: int) -> str:
    """Apply 24-bit true color to text using ANSI escape codes."""
    return f"\033[38;2;{r};{g};{b}m{text}\033[0m"


def color_256(text: str, code: int) -> str:
    """Apply 256-color palette to text using ANSI escape codes."""
    return f"\033[38;5;{code}m{text}\033[0m"


def apply_color(text: str, color_name: str, caps: TerminalCaps) -> str:
    """Apply color to text based on terminal capabilities.

    Args:
        text: The text to colorize
        color_name: Key from COLORS_RGB/COLORS_256/COLORS_BASIC dicts
        caps: Terminal capabilities

    Returns:
        Colorized text string with appropriate ANSI codes
    """
    mode = caps.color_mode

    if mode == "none":
        return text

    if mode == "true":
        rgb = COLORS_RGB.get(color_name)
        if rgb:
            return color_rgb(text, *rgb)
        return text

    if mode == "256":
        code = COLORS_256.get(color_name)
        if code is not None:
            return color_256(text, code)
        return text

    # Basic mode - return text as-is (Click will handle basic colors)
    return text


def get_status_color(status: str) -> str:
    """Map status string to color name.

    Args:
        status: Status value like "ready", "starting", "error"

    Returns:
        Color name key for the color dicts
    """
    status_lower = status.lower()
    if status_lower in ("ready", "connected", "healthy"):
        return "success"
    if status_lower in ("starting", "degraded", "warning"):
        return "warning"
    if status_lower in ("error", "failed", "disconnected"):
        return "error"
    return "muted"
