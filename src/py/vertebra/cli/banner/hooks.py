"""User extension hooks for custom banner sections.

Users can register custom sections using the @banner_section decorator:

    from vertebra import banner_section

    @banner_section("My App", order=50)
    def my_app_status() -> dict[str, str]:
        return {
            "Version": "1.2.3",
            "Environment": os.environ.get("ENV", "development"),
        }
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal

logger = logging.getLogger(__name__)

# Type alias for section callback functions
SectionCallback = Callable[[], dict[str, str]]


@dataclass(slots=True)
class BannerSection:
    """Registered banner section definition."""

    name: str
    callback: SectionCallback
    order: int = 100
    mode: Literal["all", "serve", "dev"] = "all"


@dataclass
class HookRegistry:
    """Registry for user-defined banner sections.

    Sections are collected during import time via the @banner_section decorator.
    At render time, sections are sorted by order and their callbacks are invoked.
    """

    _sections: list[BannerSection] = field(default_factory=list)

    def register(
        self,
        name: str,
        callback: SectionCallback,
        order: int = 100,
        mode: Literal["all", "serve", "dev"] = "all",
    ) -> None:
        """Register a new banner section.

        Args:
            name: Display name for the section header
            callback: Function returning dict[str, str] of key-value pairs
            order: Sort order (lower = earlier). Built-in sections use 0-50.
            mode: Which modes to display in ("all", "serve", or "dev")
        """
        self._sections.append(BannerSection(name, callback, order, mode))
        # Keep sections sorted by order
        self._sections.sort(key=lambda s: s.order)

    def collect(
        self, mode: str = "serve", timeout: float = 1.0
    ) -> list[tuple[str, dict[str, str]]]:
        """Collect all registered sections for the given mode.

        Args:
            mode: Current run mode ("serve" or "dev")
            timeout: Maximum time in seconds for each callback (not enforced yet)

        Returns:
            List of (name, data) tuples where data is the callback result
        """
        results: list[tuple[str, dict[str, str]]] = []

        for section in self._sections:
            # Filter by mode
            if section.mode not in ("all", mode):
                continue

            try:
                data = section.callback()
                if data:  # Only include non-empty sections
                    results.append((section.name, data))
            except Exception:
                # Log error but don't crash - show error indicator
                logger.exception("Error in banner section '%s'", section.name)
                results.append((section.name, {"Error": "Failed to collect"}))

        return results

    def clear(self) -> None:
        """Clear all registered sections. Mainly for testing."""
        self._sections.clear()


# Global registry instance
_registry = HookRegistry()


def banner_section(
    name: str,
    *,
    order: int = 100,
    mode: Literal["all", "serve", "dev"] = "all",
) -> Callable[[SectionCallback], SectionCallback]:
    """Decorator to register a function as a banner section.

    The decorated function should take no arguments and return a dict[str, str]
    mapping labels to values that will be displayed in the banner.

    Args:
        name: Display name for the section header
        order: Sort order (lower = earlier). Built-in sections use 0-50.
        mode: Which modes to display in ("all", "serve", or "dev")

    Returns:
        Decorated function unchanged

    Example:
        @banner_section("Database", order=60)
        def db_status() -> dict[str, str]:
            return {"PostgreSQL": "connected", "Redis": "connected"}
    """

    def decorator(func: SectionCallback) -> SectionCallback:
        _registry.register(name, func, order, mode)
        return func

    return decorator


def get_registry() -> HookRegistry:
    """Get the global hook registry.

    Returns:
        The global HookRegistry instance
    """
    return _registry
