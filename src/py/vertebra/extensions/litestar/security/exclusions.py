"""Safe route exclusion helpers for litestar-security."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from collections.abc import Sequence

    from litestar.config.app import AppConfig
    from litestar_security import SecurityConfig


def configure_vertebra_exclusions(
    security_config: SecurityConfig[Any],
    *,
    has_vertebra_routes: bool = False,
    routes: Sequence[Any] | None = None,
) -> SecurityConfig[Any]:
    """Safely configure /__vertebra route exclusions without triggering unmatched-exclusion warnings."""
    ensure_module("litestar_security", extra="security")
    should_exclude = has_vertebra_routes
    if not should_exclude and routes:
        for r in routes:
            path = str(getattr(r, "path", "") or "")
            if path.startswith("/__vertebra"):
                should_exclude = True
                break
    if should_exclude:
        current_exclude = list(security_config.exclude or ())
        pattern = "^/__vertebra"
        if pattern not in current_exclude:
            current_exclude.append(pattern)
            security_config.exclude = tuple(current_exclude)
    return security_config


def exempt_vertebra_routes(app_config: AppConfig) -> AppConfig:
    """Exempt all registered /__vertebra routes from authentication via route opt."""
    ensure_module("litestar_security", extra="security")
    for handler in getattr(app_config, "route_handlers", ()):
        paths = getattr(handler, "paths", None)
        if paths and any(str(p).startswith("/__vertebra") for p in paths):
            opt = getattr(handler, "opt", None)
            if isinstance(opt, dict):
                opt["exclude_from_auth"] = True
    return app_config
