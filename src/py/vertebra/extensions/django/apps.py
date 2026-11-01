"""Django AppConfig for the Vertebra Django extension."""

from __future__ import annotations

from typing import Any

from vertebra.utils.module_loader import import_optional

__all__ = ("DjangoAppConfig",)


class DjangoAppConfig:
    """Django ``AppConfig`` that registers Vertebra's routing, fastpath, and template bridges."""

    name: str = "vertebra.extensions.django"
    label: str = "vertebra_django"
    verbose_name: str = "Vertebra Django Extension"
    default_auto_field: str = "django.db.models.BigAutoField"

    def __init__(self, app_name: str = "vertebra.extensions.django", app_module: Any = None) -> None:
        self.name = app_name
        self.module = app_module if app_module is not None else import_optional(app_name)

    def ready(self) -> None:
        """Initialize ``DjangoPlugin`` from ``django.conf.settings`` when Django starts."""
        django_conf = import_optional("django.conf")
        if django_conf is None:
            return
        settings = getattr(django_conf, "settings", None)
        if settings is None or not getattr(settings, "configured", False):
            return
        raw_cfg = getattr(settings, "VERTEBRA", None)
        if raw_cfg is not None:
            from vertebra.extensions.django.config import DjangoConfig
            from vertebra.extensions.django.plugin import DjangoPlugin

            cfg = raw_cfg if isinstance(raw_cfg, DjangoConfig) else DjangoConfig(**dict(raw_cfg))
            DjangoPlugin(settings=settings, config=cfg)
