"""Application helpers for broker plugin registration."""

import asyncio
import inspect
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence


class ProviderPlugin(Protocol):
    """Protocol for broker plugins that register and serve providers."""

    def register(self, *args: Any, **kwargs: Any) -> "Awaitable[None]": ...

    def serve(self) -> "Awaitable[None]": ...


class Vertebra:
    """Application container for broker-managed plugins."""

    def __init__(
        self,
        client_config: "BrokerClientConfig | None" = None,
        plugins: "Sequence[ProviderPlugin | type[ProviderPlugin]] | None" = None,
    ) -> None:
        if client_config is None:
            from vertebra.resources import BrokerClientConfig as _BrokerClientConfig

            self._client_config = _BrokerClientConfig()
        else:
            self._client_config = client_config
        self._plugins: list[ProviderPlugin] = []
        for plugin in plugins or ():
            self.register_plugin(plugin)

    def register_plugin(
        self,
        plugin_or_factory: "ProviderPlugin | Callable[..., ProviderPlugin] | type[ProviderPlugin]",
        *args: Any,
        **kwargs: Any,
    ) -> ProviderPlugin:
        """Register a provider plugin or plugin factory."""
        if inspect.isclass(plugin_or_factory) or (callable(plugin_or_factory) and (args or kwargs)):
            plugin = plugin_or_factory(*args, **kwargs)
        else:
            plugin = plugin_or_factory
        self._plugins.append(plugin)
        return plugin

    async def register_plugins(self) -> None:
        for plugin in self._plugins:
            await self._register_plugin(plugin)

    async def serve(self) -> None:
        await self.register_plugins()
        tasks = []
        for plugin in self._plugins:
            serve = getattr(plugin, "serve", None)
            if callable(serve):
                tasks.append(serve())
        if tasks:
            await asyncio.gather(*tasks)

    async def _register_plugin(self, plugin: ProviderPlugin) -> None:
        register = getattr(plugin, "register", None)
        if register is None:
            return
        params = list(inspect.signature(register).parameters.values())
        if params:
            await register(self._client_config)
        else:
            await register()

    @property
    def plugins(self) -> "tuple[ProviderPlugin, ...]":
        return tuple(self._plugins)


__all__ = ("ProviderPlugin", "Vertebra")
