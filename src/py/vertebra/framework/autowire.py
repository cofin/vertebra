"""Zero-runtime-reflection domain package autowiring."""

from __future__ import annotations

import pkgutil
from typing import TYPE_CHECKING, Any

import msgspec

from vertebra.framework._router import Controller, Router
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from vertebra.framework.di import Scope

__all__ = (
    "AutowireConfig",
    "AutowireLoader",
)


class AutowireConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for startup-time domain package scanning and route/service discovery."""

    domain_packages: tuple[str, ...] = ()
    controller_suffixes: tuple[str, ...] = ("Controller", "Router")
    service_suffixes: tuple[str, ...] = ("Service", "Repository")
    ignore_private_modules: bool = True


class AutowireLoader:
    """Startup-only package scanner caching discovered controllers and services."""

    __slots__ = ("_controllers", "_services", "config")

    def __init__(self, config: AutowireConfig | None = None) -> None:
        """Initialize autowire loader with scanning configuration."""
        self.config = config or AutowireConfig()
        self._controllers: list[type[Any]] = []
        self._services: list[type[Any]] = []

    @property
    def controllers(self) -> tuple[type[Any], ...]:
        """Return discovered controller classes."""
        return tuple(self._controllers)

    @property
    def services(self) -> tuple[type[Any], ...]:
        """Return discovered service and repository classes."""
        return tuple(self._services)

    def scan(self) -> tuple[tuple[type[Any], ...], tuple[type[Any], ...]]:
        """Import configured domain_packages once at startup and collect controllers and services."""
        self._controllers.clear()
        self._services.clear()
        for pkg_name in self.config.domain_packages:
            pkg = ensure_module(pkg_name)
            self._inspect_module(pkg)
            pkg_path = getattr(pkg, "__path__", None)
            if pkg_path is not None:
                for mod_info in pkgutil.walk_packages(pkg_path, prefix=f"{pkg_name}."):
                    short = mod_info.name.rsplit(".", 1)[-1]
                    if self.config.ignore_private_modules and short.startswith("_"):
                        continue
                    mod = ensure_module(mod_info.name)
                    self._inspect_module(mod)
        return self.controllers, self.services

    def register_all(
        self,
        app: Any,
        *,
        service_scope: Scope = "singleton",
    ) -> tuple[tuple[type[Any], ...], tuple[type[Any], ...]]:
        """Scan configured domain packages and register all services and controllers on app."""
        if not self._controllers and not self._services and self.config.domain_packages:
            self.scan()
        container = getattr(app, "container", None)
        if container is not None:
            for svc_cls in self._services:
                container.graph.register_type(svc_cls, scope=service_scope)
            container.graph.compile()
        for ctrl_cls in self._controllers:
            if issubclass(ctrl_cls, Controller | Router):
                app.register(ctrl_cls)
        return self.controllers, self.services

    def _inspect_module(self, module: Any) -> None:
        """Collect public controller and service classes defined in module."""
        for attr_name, obj in vars(module).items():
            if attr_name.startswith("_") or not isinstance(obj, type):
                continue
            if obj.__module__ != module.__name__:
                continue
            if obj in (Controller, Router):
                continue
            if obj.__name__.endswith(self.config.controller_suffixes) or issubclass(obj, Controller):
                self._controllers.append(obj)
            elif obj.__name__.endswith(self.config.service_suffixes):
                self._services.append(obj)
