"""Zero-runtime-reflection domain package autowiring."""

from __future__ import annotations

import pkgutil
from typing import Any

import msgspec

from vertebra.utils.module_loader import ensure_module

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

    def _inspect_module(self, module: Any) -> None:
        for attr_name, obj in vars(module).items():
            if attr_name.startswith("_") or not isinstance(obj, type):
                continue
            if obj.__module__ != module.__name__:
                continue
            if obj.__name__.endswith(self.config.controller_suffixes):
                self._controllers.append(obj)
            elif obj.__name__.endswith(self.config.service_suffixes):
                self._services.append(obj)
