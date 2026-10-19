"""SSG prerendering for Vertebra.

Renders routes to static HTML files at build time using the SSR engine.
"""

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from vertebra.utils.serializers._json import encode_json

if TYPE_CHECKING:
    from vertebra.framework._ssr._backends import SsrClient

__all__ = ("prerender_routes",)

logger = logging.getLogger(__name__)


def prerender_routes(
    *,
    ssr_client: "SsrClient",
    routes: list[dict[str, Any]],
    outdir: Path,
    version: str = "prerendered",
    root_template: str | None = None,
) -> list[Path]:
    """Render a list of routes to static HTML files."""
    written: list[Path] = []

    for route in routes:
        route_path: str = route.get("path", "/")
        component: str = route.get("component", "")
        props: dict[str, Any] = route.get("props", {})

        page: dict[str, Any] = {
            "component": component,
            "props": {**props, "errors": {}},
            "url": route_path,
            "version": version,
        }

        try:
            result = ssr_client.render(page)
        except Exception:
            logger.exception("Failed to prerender %s", route_path)
            continue

        if root_template:
            html = root_template
            html = html.replace("<!--ssr-head-->", "\n".join(result.head))
            html = html.replace("<!--ssr-body-->", result.body)
        else:
            head_str = "\n".join(result.head)
            page_json = encode_json(page)
            html = (
                "<!DOCTYPE html>\n"
                "<html>\n"
                "<head>\n"
                '<meta charset="utf-8">\n'
                f"{head_str}\n"
                "</head>\n"
                "<body>\n"
                '<div id="app">\n'
                f"{result.body}\n"
                '<script data-page="app" type="application/json">\n'
                f"{page_json}\n"
                "</script>\n"
                "</div>\n"
                "</body>\n"
                "</html>"
            )

        clean_path = route_path.strip("/")
        out_path = outdir / "index.html" if not clean_path else outdir / clean_path / "index.html"

        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(html, encoding="utf-8")
        written.append(out_path)
        logger.info("Pre-rendered: %s -> %s", route_path, out_path)

    logger.info("Pre-rendered %d pages to %s", len(written), outdir)
    return written
