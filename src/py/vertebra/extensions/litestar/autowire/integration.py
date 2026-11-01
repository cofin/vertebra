"""Vertebra bridge integration for litestar-autowire."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import ensure_module

try:
    from litestar_autowire import AutowireConfig, AutowirePlugin
except ImportError as exc:
    raise MissingDependencyError(
        "litestar-autowire",
        install_package="litestar-autowire",
        extra="autowire",
    ) from exc

if TYPE_CHECKING:
    from collections.abc import Iterable

    from litestar.config.app import AppConfig
    from litestar_autowire.integrations import AutowireContext


class AutowireBridgeIntegration:
    """Autowire integration bridging Vertebra components with deterministic route compilation."""

    __slots__ = ("_name",)

    def __init__(self, name: str = "vertebra") -> None:
        """Initialize the autowire bridge integration."""
        ensure_module("litestar_autowire", extra="autowire")
        self._name = name

    @property
    def name(self) -> str:
        """Return the integration identifier."""
        return self._name

    def on_autowire(self, context: AutowireContext) -> None:
        """Apply Vertebra bridge behaviors to the Autowire discovery context."""
        state = getattr(context.app_config, "state", None)
        if isinstance(state, dict) or state is not None and hasattr(state, "__setitem__"):
            state["vertebra_autowired"] = True


def ensure_autowire_before_route_compiler(app_config: AppConfig) -> AppConfig:
    """Ensure AutowirePlugin runs on_app_init before RouteCompiler extracts routes."""
    ensure_module("litestar_autowire", extra="autowire")
    plugins = getattr(app_config, "plugins", None)
    if not isinstance(plugins, list):
        return app_config

    autowire_idx = -1
    litestar_idx = -1
    for i, p in enumerate(plugins):
        name = type(p).__name__
        if name == "AutowirePlugin":
            autowire_idx = i
        elif name == "LitestarPlugin":
            litestar_idx = i

    if autowire_idx != -1 and litestar_idx != -1 and autowire_idx > litestar_idx:
        autowire_plugin = plugins.pop(autowire_idx)
        plugins.insert(litestar_idx, autowire_plugin)

    return app_config


def create_autowire_plugin(
    domain_packages: str | Iterable[str],
    *,
    integrations: Iterable[Any] = (),
    discover_controllers: bool = True,
    discover_listeners: bool = True,
    **kwargs: Any,
) -> AutowirePlugin:
    """Create an AutowirePlugin pre-configured with AutowireBridgeIntegration."""
    ensure_module("litestar_autowire", extra="autowire")
    bridge = AutowireBridgeIntegration()
    all_integrations = (bridge, *integrations)

    config = AutowireConfig(
        domain_packages=domain_packages,
        integrations=all_integrations,
        discover_controllers=discover_controllers,
        discover_listeners=discover_listeners,
        **kwargs,
    )
    return AutowirePlugin(config)
