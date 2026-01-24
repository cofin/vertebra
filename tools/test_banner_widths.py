#!/usr/bin/env python
"""Visual verification script for banner rendering at different terminal widths.

Run this script to see how the banner renders at various breakpoints:
    python tools/test_banner_widths.py

This helps verify that responsive layouts work correctly across terminal sizes.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Add src/py to path for development
sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "py"))

from vertebra.cli.banner import render_banner
from vertebra.cli.theme import detect_terminal_capabilities


def test_all_widths() -> None:
    """Test banner rendering at all breakpoints."""
    # Get actual terminal caps
    detect_terminal_capabilities()

    # Test each breakpoint
    widths = [120, 100, 80, 60, 40]

    for width in widths:
        # Override terminal width via environment
        os.environ["COLUMNS"] = str(width)

        # Render in serve mode
        render_banner(mode="serve", host="127.0.0.1", port=8000, workers=4, log_level="info")

        # Render in dev mode
        render_banner(
            mode="dev", host="127.0.0.1", port=8000, workers=1, log_level="debug", watched_paths=["src/", "app/"]
        )


def test_non_tty() -> None:
    """Test JSON output for non-TTY mode."""

    # Simulate non-TTY by checking current mode
    caps = detect_terminal_capabilities()
    if caps.is_tty:
        pass
    else:
        render_banner(mode="serve", host="127.0.0.1", port=8000, workers=4, log_level="info")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Test banner rendering at different widths")
    parser.add_argument("--json", action="store_true", help="Test JSON output mode")
    args = parser.parse_args()

    if args.json:
        test_non_tty()
    else:
        test_all_widths()
