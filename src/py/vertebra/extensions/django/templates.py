"""Template rendering bridge for Django DTL and Jinja2."""

from __future__ import annotations

from typing import Any

from vertebra._vertebra import Response
from vertebra.extensions.django.vite import get_active_vite_bridge
from vertebra.utils.module_loader import import_optional

__all__ = ("TemplateBridge",)


class TemplateBridge:
    """Renders templates via Django's template loader or an explicit Jinja2 environment."""

    __slots__ = ("_default_context", "_jinja_env")

    def __init__(
        self,
        *,
        jinja_env: Any = None,
        default_context: dict[str, Any] | None = None,
    ) -> None:
        self._jinja_env = jinja_env
        self._default_context = dict(default_context or {})

    def build_context(self, context: dict[str, Any] | None = None) -> dict[str, Any]:
        """Merge ``default_context``, Vite helpers, and caller ``context``."""
        bridge = get_active_vite_bridge()
        merged: dict[str, Any] = {
            "vite_hmr_client": bridge.hmr_client_tag,
            "vite_asset": bridge.asset_tags,
            **self._default_context,
        }
        if context:
            merged.update(context)
        return merged

    def render_to_string(
        self,
        template_name: str,
        context: dict[str, Any] | None = None,
        *,
        request: Any = None,
    ) -> str:
        """Render ``template_name`` to an HTML string."""
        ctx = self.build_context(context)
        if self._jinja_env is not None:
            tmpl = self._jinja_env.get_template(template_name)
            return str(tmpl.render(**ctx))
        django_loader = import_optional("django.template.loader")
        if django_loader is not None and hasattr(django_loader, "render_to_string"):
            return str(django_loader.render_to_string(template_name, ctx, request=request))
        msg = "Neither a Jinja2 environment nor django.template.loader is available"
        raise RuntimeError(msg)

    def render_to_response(
        self,
        template_name: str,
        context: dict[str, Any] | None = None,
        *,
        status: int = 200,
        content_type: str = "text/html; charset=utf-8",
        request: Any = None,
    ) -> Response:
        """Render ``template_name`` directly into a Vertebra ``Response``."""
        html = self.render_to_string(template_name, context, request=request)
        return Response(
            status=status,
            headers=[("content-type", content_type)],
            body=html.encode("utf-8"),
        )
