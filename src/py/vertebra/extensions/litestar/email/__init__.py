"""Vertebra email extension for litestar-email."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
    from vertebra.extensions.litestar.email.backend import SESEmailBackend, SESEmailConfig

__all__ = (
    "SESEmailBackend",
    "SESEmailConfig",
)

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "SESEmailBackend": ("vertebra.extensions.litestar.email.backend", "SESEmailBackend"),
    "SESEmailConfig": ("vertebra.extensions.litestar.email.backend", "SESEmailConfig"),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
