"""Django template tags (`{% load vertebra_vite %}`) for Vite HMR and asset tags."""

from __future__ import annotations

from typing import Any

from vertebra.extensions.django.vite import get_active_vite_bridge
from vertebra.utils.module_loader import import_optional

__all__ = (
    "register",
    "vite_asset",
    "vite_hmr_client",
)


def _mark_safe(html: str) -> Any:
    """Wrap ``html`` in Django ``mark_safe`` when Django is installed."""
    safestring = import_optional("django.utils.safestring")
    if safestring is not None and hasattr(safestring, "mark_safe"):
        return safestring.mark_safe(html)
    return html


def vite_hmr_client() -> Any:
    """Render the Vite HMR client script tag."""
    return _mark_safe(get_active_vite_bridge().hmr_client_tag())


def vite_asset(entry: str) -> Any:
    """Render Vite module script and CSS tags for ``entry``."""
    return _mark_safe(get_active_vite_bridge().asset_tags(entry))


def _build_template_library() -> Any:
    """Create and populate a Django ``template.Library`` when Django is available."""
    django_template = import_optional("django.template")
    if django_template is not None and hasattr(django_template, "Library"):
        lib = django_template.Library()
        lib.simple_tag(vite_hmr_client, name="vite_hmr_client")
        lib.simple_tag(vite_asset, name="vite_asset")
        return lib
    return None


register = _build_template_library()
