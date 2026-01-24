#!/usr/bin/env python
"""Visual verification script for banner rendering at different terminal widths.

Run this script to see how the banner renders at various breakpoints:
    python tools/test_banner_widths.py

This helps verify that responsive layouts work correctly across terminal sizes.
"""

from __future__ import annotations

import os
import sys

# Add src/py to path for development
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "py"))

from vertebra.cli.banner import render_banner
from vertebra.cli.banner.styles import TerminalCaps, detect_terminal_capabilities


def test_all_widths() -> None:
    """Test banner rendering at all breakpoints."""
    # Get actual terminal caps
    real_caps = detect_terminal_capabilities()
    print(f"Actual terminal: {real_caps.width} columns, TTY={real_caps.is_tty}")
    print()

    # Test each breakpoint
    widths = [120, 100, 80, 60, 40]

    for width in widths:
        print("=" * width)
        print(f"WIDTH: {width} columns")
        print("=" * width)

        # Override terminal width via environment
        os.environ["COLUMNS"] = str(width)

        # Render in serve mode
        print("\n[SERVE MODE]")
        render_banner(
            mode="serve",
            host="127.0.0.1",
            port=8000,
            workers=4,
            log_level="info",
        )

        # Render in dev mode
        print("[DEV MODE]")
        render_banner(
            mode="dev",
            host="127.0.0.1",
            port=8000,
            workers=1,
            log_level="debug",
            watched_paths=["src/", "app/"],
        )

        print()


def test_non_tty() -> None:
    """Test JSON output for non-TTY mode."""
    print("=" * 60)
    print("NON-TTY MODE (JSON OUTPUT)")
    print("=" * 60)
    print()

    # Simulate non-TTY by checking current mode
    caps = detect_terminal_capabilities()
    if caps.is_tty:
        print("Note: Running in TTY mode. To test non-TTY, pipe output:")
        print("  python tools/test_banner_widths.py | cat")
        print()
    else:
        render_banner(
            mode="serve",
            host="127.0.0.1",
            port=8000,
            workers=4,
            log_level="info",
        )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Test banner rendering at different widths")
    parser.add_argument("--json", action="store_true", help="Test JSON output mode")
    args = parser.parse_args()

    if args.json:
        test_non_tty()
    else:
        test_all_widths()
