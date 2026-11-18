"""Django AppConfig for the Vertebra Django extension."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import msgspec

from vertebra.extensions.django.config import VertebraConfig
from vertebra.extensions.django.plugin import VertebraPlugin
from vertebra.utils.module_loader import ensure_module, import_optional

_apps_module = import_optional("django.apps")

if TYPE_CHECKING:
    from types import ModuleType

    from django.apps import AppConfig as _AppConfigBase
else:
    _AppConfigBase = _apps_module.AppConfig if _apps_module is not None else object

__all__ = ("VertebraAppConfig",)


def __getattr__(name: str) -> Any:
    """Lazily resolve optional Django AppConfig classes on demand."""
    if name == "AppConfig":
        return ensure_module("django.apps", package="django", extra="django").AppConfig
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)


class VertebraAppConfig(_AppConfigBase):
    """Django ``AppConfig`` that registers Vertebra's routing, fastpath, and template bridges."""

    name: str = "vertebra.extensions.django"
    label: str = "vertebra_django"
    verbose_name = "Vertebra Django Extension"
    default_auto_field = "django.db.models.BigAutoField"

    def __init__(
        self,
        app_name: str | None = None,
        app_module: ModuleType | None = None,
    ) -> None:
        if _apps_module is None:
            ensure_module("django.apps", package="django", extra="django")
        resolved_name = app_name or type(self).name
        resolved_module = app_module if app_module is not None else ensure_module(resolved_name)
        self.plugin: VertebraPlugin | None = None
        super().__init__(resolved_name, resolved_module)

    def ready(self) -> None:
        """Initialize ``VertebraPlugin`` from ``django.conf.settings`` when Django starts."""
        django_conf = import_optional("django.conf")
        if django_conf is None:
            return
        settings = getattr(django_conf, "settings", None)
        if settings is None or not getattr(settings, "configured", False):
            return
        raw_cfg = getattr(settings, "VERTEBRA", None)
        if raw_cfg is not None:
            cfg = raw_cfg if isinstance(raw_cfg, VertebraConfig) else VertebraConfig(**dict(raw_cfg))
        else:
            cfg = VertebraConfig()
        root_urlconf = getattr(settings, "ROOT_URLCONF", None)
        if cfg.root_urlconf is None and isinstance(root_urlconf, str) and root_urlconf:
            cfg = msgspec.structs.replace(cfg, root_urlconf=root_urlconf)
        self.plugin = VertebraPlugin(settings=settings, config=cfg)
