"""Zero-runtime-reflection domain controller and service autowiring."""

from __future__ import annotations

import importlib
import pkgutil
from dataclasses import dataclass, field
from typing import Any

__all__ = (
    "AutowireConfig",
    "AutowireLoader",
)


@dataclass(frozen=True, slots=True)
class AutowireConfig:
    """Configuration for startup-time domain package scanning and route/service discovery."""

    domain_packages: tuple[str, ...] = ()
    controller_suffixes: tuple[str, ...] = ("Controller", "Router")
    service_suffixes: tuple[str, ...] = ("Service", "Repository")
    ignore_private_modules: bool = True


@dataclass(slots=True)
class AutowireLoader:
    """Startup-only package scanner caching discovered controllers and services."""

    config: AutowireConfig = field(default_factory=AutowireConfig)
    _controllers: list[type[Any]] = field(default_factory=list, init=False)
    _services: list[type[Any]] = field(default_factory=list, init=False)

    @property
    def controllers(self) -> tuple[type[Any], ...]:
        """Return discovered controller classes."""
        return tuple(self._controllers)

    @property
    def services(self) -> tuple[type[Any], ...]:
        """Return discovered service and repository classes."""
        return tuple(self._services)

    def scan(self) -> tuple[tuple[type[Any], ...], tuple[type[Any], ...]]:
        """Import configured ``domain_packages`` once at startup and collect controllers and services."""
        self._controllers.clear()
        self._services.clear()
        for pkg_name in self.config.domain_packages:
            pkg = importlib.import_module(pkg_name)
            self._inspect_module(pkg)
            pkg_path = getattr(pkg, "__path__", None)
            if pkg_path is not None:
                for mod_info in pkgutil.walk_packages(pkg_path, prefix=f"{pkg_name}."):
                    short = mod_info.name.rsplit(".", 1)[-1]
                    if self.config.ignore_private_modules and short.startswith("_"):
                        continue
                    mod = importlib.import_module(mod_info.name)
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
