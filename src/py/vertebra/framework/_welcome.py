"""Self-contained development welcome page renderer and runtime snapshot models.

Renders an offline-ready HTML5 landing page for unclaimed ``GET /`` requests in
``vtb dev`` and ``App(debug=True)`` modes featuring a fluid Zensical-inspired
slate/indigo backdrop, the Vertebra Isometric Axial Vertebrae SVG mark, a
quickstart route snippet, active endpoint pills, and documentation links.
"""

from __future__ import annotations

import functools
import html
import importlib.resources
import os
import platform
import re
import sys
from collections.abc import Callable
from typing import TYPE_CHECKING

import msgspec

from vertebra import __version__
from vertebra._vertebra import FREE_THREADED, Response
from vertebra.utils.config_tools import get_env, is_env_set
from vertebra.utils.humanize import pluralize

if TYPE_CHECKING:
    from vertebra.framework._app import App

__all__ = (
    "WelcomePageRenderer",
    "WelcomeRouteItem",
    "WelcomeSnapshot",
)

_TEMPLATE_TOKEN_RE = re.compile(
    r"\{\{(version|gil_tag|topology|worker_label|loop_backend|endpoints_html|docs_card_href|docs_card_meta)\}\}"
)


class WelcomeRouteItem(msgspec.Struct, frozen=True, gc=False):
    """Metadata for a single registered route displayed on the welcome page."""

    method: str
    """HTTP method verb in uppercase."""

    path: str
    """Normalized route path template."""

    handler_name: str
    """Qualified name of the bound route handler."""

    tags: tuple[str, ...] = ()
    """Optional route tags associated with the operation."""


class WelcomeSnapshot(msgspec.Struct, frozen=True, gc=False):
    """Point-in-time runtime and routing metadata rendered by the development welcome page."""

    version: str
    """Vertebra package version string."""

    python_version: str
    """Active Python interpreter version string."""

    free_threaded: bool
    """Whether the interpreter is running with the GIL disabled (3.14t/3.15t)."""

    topology: str
    """Active runtime worker topology label (Embedded or SHM)."""

    loop_backend: str
    """Configured event-loop backend name."""

    workers: int
    """Configured worker process or thread count."""

    hmr_enabled: bool
    """Whether browser Hot Module Replacement is enabled."""

    openapi_enabled: bool
    """Whether OpenAPI documentation routes are enabled on the App."""

    docs_url: str | None = None
    """Primary interactive documentation URL path when enabled."""

    schema_url: str | None = None
    """OpenAPI JSON schema URL path when enabled."""

    routes: tuple[WelcomeRouteItem, ...] = ()
    """User-registered HTTP routes on the application."""

    procedures: tuple[str, ...] = ()
    """Registered RPC procedure names."""

    mounts: tuple[str, ...] = ()
    """Mounted sub-application prefixes and protocol labels."""


class WelcomePageRenderer:
    """Renders the self-contained Vertebra development welcome page."""

    __slots__ = ()

    @classmethod
    def collect_snapshot(cls, app: App) -> WelcomeSnapshot:
        """Collect runtime topology, interpreter state, and registered routes from ``app``."""
        gil_enabled_fn = getattr(sys, "_is_gil_enabled", None)
        is_free_threaded = bool(FREE_THREADED or (callable(gil_enabled_fn) and not gil_enabled_fn()))
        raw_topology = str(get_env("VERTEBRA_TOPOLOGY", "embedded")).strip().lower()
        topology = "SHM" if raw_topology == "shm" else "Embedded"
        loop_backend = str(get_env("VERTEBRA_LOOP", "auto")).strip() or "auto"
        try:
            workers = max(1, int(get_env("VERTEBRA_WORKERS", 1, int)))
        except ValueError:
            workers = 1

        try:
            dev_env = is_env_set("VERTEBRA_DEV", default=False)
        except ValueError:
            dev_env = False
        raw_hmr = os.environ.get("VERTEBRA_HMR", "1").strip().lower()
        hmr_enabled = (bool(getattr(app, "debug", False)) or dev_env) and raw_hmr not in {
            "0",
            "false",
            "no",
            "off",
        }

        openapi_cfg = getattr(app, "_openapi_config", None)
        openapi_enabled = openapi_cfg is not None
        docs_url: str | None = None
        schema_url: str | None = None
        if openapi_enabled:
            docs_url = str(getattr(openapi_cfg, "docs_path", "/docs") or "/docs")
            schema_url = str(getattr(openapi_cfg, "schema_path", "/openapi.json") or "/openapi.json")

        route_items = tuple(
            WelcomeRouteItem(
                method=route.method.upper(),
                path=route.path,
                handler_name=getattr(
                    route.handler,
                    "__qualname__",
                    getattr(route.handler, "__name__", repr(route.handler)),
                ),
                tags=tuple(str(tag) for tag in route.tags),
            )
            for route in app.routes
        )
        procedures = tuple(sorted(app.procedures.keys()))
        mounts = tuple(f"{prefix or '/'} ({proto})" for prefix, _, proto in app.mounts)

        return WelcomeSnapshot(
            version=__version__,
            python_version=platform.python_version(),
            free_threaded=is_free_threaded,
            topology=topology,
            loop_backend=loop_backend,
            workers=workers,
            hmr_enabled=hmr_enabled,
            openapi_enabled=openapi_enabled,
            docs_url=docs_url,
            schema_url=schema_url,
            routes=route_items,
            procedures=procedures,
            mounts=mounts,
        )

    @classmethod
    def render_html(cls, app: App) -> str:
        """Render the complete offline-ready HTML5 welcome page for ``app``."""
        snap = cls.collect_snapshot(app)
        esc = cls._escape
        gil_tag = "3.14t/3.15t Free-Threaded" if snap.free_threaded else f"Python {esc(snap.python_version)}"
        worker_label = pluralize("worker", snap.workers, include_count=True)
        endpoints_html = cls._render_endpoints_strip(snap)

        if snap.openapi_enabled and snap.docs_url:
            docs_card_href = esc(snap.docs_url)
            docs_card_meta = f"Open {esc(snap.docs_url)} · Schema at {esc(snap.schema_url or '/openapi.json')}"
        else:
            docs_card_href = "/__vertebra/health"
            docs_card_meta = "Call app.enable_docs() to serve Scalar & OpenAPI"

        tokens = {
            "version": esc(snap.version),
            "gil_tag": gil_tag,
            "topology": esc(snap.topology),
            "worker_label": esc(worker_label),
            "loop_backend": esc(snap.loop_backend),
            "endpoints_html": endpoints_html,
            "docs_card_href": docs_card_href,
            "docs_card_meta": docs_card_meta,
        }
        return _TEMPLATE_TOKEN_RE.sub(lambda match: tokens[match.group(1)], _load_compiled_welcome_template())

    @classmethod
    def make_handler(cls, app: App) -> Callable[[], Response]:
        """Create the zero-argument ``GET /`` handler returning an HTML ``Response`` for ``app``."""

        def handle_welcome() -> Response:
            payload = cls.render_html(app).encode("utf-8")
            return Response(200, payload, [("content-type", "text/html; charset=utf-8")])

        handle_welcome.__name__ = "handle_welcome"
        handle_welcome.__qualname__ = "WelcomePageRenderer.handle_welcome"
        return handle_welcome

    @staticmethod
    def _escape(value: str) -> str:
        """Escape a string for safe HTML text and attribute interpolation."""
        return html.escape(str(value), quote=True)

    @classmethod
    def _render_endpoints_strip(cls, snap: WelcomeSnapshot) -> str:
        """Render compact chips for active routes, RPC procedures, mounts, and the health probe."""
        esc = cls._escape
        chips: list[str] = []
        for item in snap.routes:
            if item.path.startswith(("/vertebra.rpc.ProcedureService/", "/_rpc/")):
                continue
            method_upper = esc(item.method.upper())
            path_esc = esc(item.path)
            tags_part = f' <span class="chip-tag">({", ".join(esc(t) for t in item.tags)})</span>' if item.tags else ""
            if item.method.upper() == "GET" and "{" not in item.path:
                chips.append(
                    f'<a class="endpoint-chip mono" href="{path_esc}">'
                    f'<span class="chip-method">{method_upper}</span>'
                    f"<span>{path_esc}</span>{tags_part}</a>"
                )
            else:
                chips.append(
                    f'<span class="endpoint-chip mono">'
                    f'<span class="chip-method">{method_upper}</span>'
                    f"<span>{path_esc}</span>{tags_part}</span>"
                )

        for proc in snap.procedures:
            proc_esc = esc(proc)
            chips.append(
                f'<span class="endpoint-chip mono">'
                f'<span class="chip-method">RPC</span>'
                f"<span>/_rpc/{proc_esc}</span></span>"
            )

        for mount in snap.mounts:
            mount_esc = esc(mount)
            chips.append(
                f'<span class="endpoint-chip mono">'
                f'<span class="chip-method">MOUNT</span>'
                f"<span>{mount_esc}</span></span>"
            )

        chips.append(
            '<a class="endpoint-chip mono" href="/__vertebra/health">'
            '<span class="chip-method">GET</span>'
            "<span>/__vertebra/health</span></a>"
        )
        return "\n      ".join(chips)


@functools.lru_cache(maxsize=1)
def _load_compiled_welcome_template() -> str:
    """Load and inline ``welcome.css`` into ``welcome.html`` from ``vertebra.framework`` package data."""
    assets = importlib.resources.files("vertebra.framework").joinpath("assets")
    html_template = assets.joinpath("welcome.html").read_text(encoding="utf-8")
    css_styles = assets.joinpath("welcome.css").read_text(encoding="utf-8").rstrip()
    indented_css = "\n".join(f"    {line}" if line else "" for line in css_styles.splitlines())
    return html_template.replace("{{styles}}", indented_css, 1)
