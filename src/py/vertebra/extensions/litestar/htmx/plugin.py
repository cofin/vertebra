"""First-class integration for litestar-htmx."""

from __future__ import annotations

from typing import Any

from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import ensure_module

try:
    from litestar_htmx import HTMXConfig, HTMXPlugin
except ImportError as exc:
    raise MissingDependencyError(
        "litestar-htmx",
        install_package="litestar-htmx",
        extra="htmx",
    ) from exc


def create_htmx_plugin(
    config: HTMXConfig | None = None,
    **kwargs: Any,
) -> HTMXPlugin:
    """Create an HTMXPlugin instance configured for Vertebra applications."""
    ensure_module("litestar_htmx", extra="htmx")
    if config is not None:
        return HTMXPlugin(config)

    resolved_config = HTMXConfig(**kwargs)
    return HTMXPlugin(resolved_config)
