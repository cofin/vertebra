"""Server startup banner system.

This module provides a responsive, extensible banner for Vertebra CLI commands.
It displays server configuration, component status, and user-defined sections.

Public API:
    - render_banner: Render the startup banner to stdout
    - banner_section: Decorator to register custom banner sections
"""

from __future__ import annotations

from vertebra.cli.banner.hooks import banner_section
from vertebra.cli.banner.renderer import render_banner

__all__ = ["banner_section", "render_banner"]
