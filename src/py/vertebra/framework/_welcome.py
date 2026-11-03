"""Self-contained development welcome page renderer and runtime snapshot models.

Renders an offline-ready HTML5 landing page for unclaimed ``GET /`` requests in
``vtb dev`` and ``App(debug=True)`` modes featuring a fluid Zensical-inspired
slate/indigo backdrop, the Vertebra Isometric Axial Vertebrae SVG mark, a
quickstart route snippet, active endpoint pills, and documentation links.
"""

from __future__ import annotations

import html
import os
import platform
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

        return f"""<!DOCTYPE html>
<html lang="en" data-theme="dark">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta name="color-scheme" content="dark light" />
  <title>Vertebra v{esc(snap.version)} — Development Server</title>
  <style>
    :root, [data-theme="dark"] {{
      --bg-canvas: #0f172a;
      --bg-surface: rgba(30, 41, 59, 0.72);
      --bg-code: #0b1120;
      --bg-code-header: #111827;
      --bg-pill: rgba(30, 41, 59, 0.9);
      --text-primary: #f1f5f9;
      --text-secondary: #94a3b8;
      --text-muted: #64748b;
      --accent-primary: #3b82f6;
      --accent-indigo: #818cf8;
      --border-subtle: rgba(148, 163, 184, 0.16);
      --border-hover: rgba(96, 165, 250, 0.45);
      --glow-a: rgba(37, 99, 235, 0.16);
      --glow-b: rgba(99, 102, 241, 0.12);
      --wave-stroke: rgba(96, 165, 250, 0.12);
      --hl-kw: #c4b5fd;
      --hl-fn: #60a5fa;
      --hl-str: #86efac;
      --hl-dec: #2dd4bf;
    }}
    [data-theme="light"] {{
      --bg-canvas: #ffffff;
      --bg-surface: rgba(248, 250, 252, 0.88);
      --bg-code: #f8fafc;
      --bg-code-header: #f1f5f9;
      --bg-pill: #f1f5f9;
      --text-primary: #0f172a;
      --text-secondary: #475569;
      --text-muted: #64748b;
      --accent-primary: #1d4ed8;
      --accent-indigo: #4338ca;
      --border-subtle: #e2e8f0;
      --border-hover: #93c5fd;
      --glow-a: rgba(37, 99, 235, 0.07);
      --glow-b: rgba(67, 56, 202, 0.06);
      --wave-stroke: rgba(37, 99, 235, 0.09);
      --hl-kw: #6d28d9;
      --hl-fn: #1d4ed8;
      --hl-str: #15803d;
      --hl-dec: #0f766e;
    }}
    * {{
      box-sizing: border-box;
      margin: 0;
      padding: 0;
    }}
    html, body {{
      min-height: 100vh;
      font-family: -apple-system, BlinkMacSystemFont, "Inter", "Segoe UI", Roboto, sans-serif;
      background-color: var(--bg-canvas);
      color: var(--text-primary);
      line-height: 1.55;
      -webkit-font-smoothing: antialiased;
    }}
    body {{
      position: relative;
      overflow-x: hidden;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
      background-image:
        radial-gradient(ellipse 60% 45% at 25% 15%, var(--glow-a), transparent),
        radial-gradient(ellipse 55% 45% at 78% 82%, var(--glow-b), transparent);
    }}
    code, pre, .mono {{
      font-family: "JetBrains Mono", ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    }}
    .ambient-waves {{
      position: fixed;
      inset: 0;
      width: 100%;
      height: 100%;
      pointer-events: none;
      z-index: 0;
    }}
    .page {{
      position: relative;
      z-index: 1;
      max-width: 820px;
      width: 100%;
      margin: 0 auto;
      padding: 1.75rem 1.5rem 2.5rem;
      display: flex;
      flex-direction: column;
      gap: 2rem;
      flex: 1;
      justify-content: center;
    }}
    .topbar {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 1rem;
      flex-wrap: wrap;
    }}
    .status-badge {{
      display: inline-flex;
      align-items: center;
      gap: 0.55rem;
      padding: 0.35rem 0.85rem;
      border-radius: 999px;
      background: var(--bg-pill);
      border: 1px solid var(--border-subtle);
      font-size: 0.78rem;
      color: var(--text-secondary);
      backdrop-filter: blur(8px);
    }}
    .status-dot {{
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: #10b981;
      box-shadow: 0 0 8px rgba(16, 185, 129, 0.7);
    }}
    .status-sep {{
      color: var(--text-muted);
    }}
    .theme-btn {{
      display: inline-flex;
      align-items: center;
      gap: 0.4rem;
      padding: 0.35rem 0.8rem;
      border-radius: 8px;
      border: 1px solid var(--border-subtle);
      background: var(--bg-pill);
      color: var(--text-secondary);
      font-size: 0.78rem;
      font-weight: 500;
      cursor: pointer;
      transition: border-color 0.15s ease, color 0.15s ease;
    }}
    .theme-btn:hover {{
      border-color: var(--border-hover);
      color: var(--text-primary);
    }}
    .hero {{
      text-align: center;
      padding: 0.5rem 0;
    }}
    .hero-lockup {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 1.35rem;
      margin-bottom: 1.15rem;
    }}
    .hero-logo {{
      width: 64px;
      height: 64px;
      flex-shrink: 0;
    }}
    .hero-divider {{
      width: 1px;
      height: 48px;
      background: var(--border-subtle);
    }}
    .brand-title {{
      font-size: 2.5rem;
      font-weight: 300;
      letter-spacing: 0.24em;
      text-transform: uppercase;
      color: var(--text-primary);
      line-height: 1;
    }}
    .hero-lead {{
      max-width: 36rem;
      margin: 0 auto;
      font-size: 0.96rem;
      color: var(--text-secondary);
    }}
    .hero-lead code {{
      padding: 0.12rem 0.4rem;
      border-radius: 5px;
      background: var(--bg-pill);
      border: 1px solid var(--border-subtle);
      color: var(--text-primary);
      font-size: 0.84rem;
    }}
    .code-card {{
      background: var(--bg-code);
      border: 1px solid var(--border-subtle);
      border-radius: 12px;
      overflow: hidden;
      box-shadow: 0 14px 32px -12px rgba(2, 6, 23, 0.35);
    }}
    .code-header {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0.55rem 1rem;
      background: var(--bg-code-header);
      border-bottom: 1px solid var(--border-subtle);
      font-size: 0.76rem;
      color: var(--text-muted);
    }}
    .copy-btn {{
      padding: 0.2rem 0.6rem;
      border-radius: 6px;
      border: 1px solid var(--border-subtle);
      background: transparent;
      color: var(--text-secondary);
      font-size: 0.72rem;
      font-weight: 600;
      cursor: pointer;
    }}
    .copy-btn:hover {{
      border-color: var(--border-hover);
      color: var(--text-primary);
    }}
    pre.code-body {{
      padding: 1.1rem 1.25rem;
      font-size: 0.83rem;
      line-height: 1.65;
      overflow-x: auto;
      color: var(--text-primary);
    }}
    .kw {{ color: var(--hl-kw); }}
    .fn {{ color: var(--hl-fn); }}
    .str {{ color: var(--hl-str); }}
    .dec {{ color: var(--hl-dec); }}
    .endpoints-bar {{
      display: flex;
      align-items: center;
      justify-content: center;
      flex-wrap: wrap;
      gap: 0.5rem;
    }}
    .endpoint-chip {{
      display: inline-flex;
      align-items: center;
      gap: 0.45rem;
      padding: 0.28rem 0.65rem;
      border-radius: 7px;
      background: var(--bg-surface);
      border: 1px solid var(--border-subtle);
      font-size: 0.75rem;
      color: var(--text-secondary);
      text-decoration: none;
      transition: border-color 0.15s ease, color 0.15s ease;
    }}
    a.endpoint-chip:hover {{
      border-color: var(--border-hover);
      color: var(--text-primary);
    }}
    .chip-method {{
      font-weight: 700;
      font-size: 0.68rem;
      color: var(--accent-primary);
    }}
    .chip-tag {{
      color: var(--text-muted);
      font-size: 0.68rem;
    }}
    .links-grid {{
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 0.9rem;
    }}
    @media (max-width: 680px) {{
      .links-grid {{
        grid-template-columns: 1fr;
      }}
      .brand-title {{
        font-size: 1.85rem;
        letter-spacing: 0.16em;
      }}
    }}
    .link-card {{
      display: flex;
      flex-direction: column;
      gap: 0.25rem;
      padding: 1rem 1.15rem;
      border-radius: 11px;
      background: var(--bg-surface);
      border: 1px solid var(--border-subtle);
      text-decoration: none;
      color: var(--text-primary);
      backdrop-filter: blur(8px);
      transition: border-color 0.15s ease, transform 0.15s ease;
    }}
    .link-card:hover {{
      border-color: var(--border-hover);
      transform: translateY(-1px);
    }}
    .link-card-title {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      font-size: 0.88rem;
      font-weight: 600;
      color: var(--text-primary);
    }}
    .link-card-arrow {{
      color: var(--accent-primary);
      font-size: 0.9rem;
    }}
    .link-card-desc {{
      font-size: 0.78rem;
      color: var(--text-secondary);
    }}
  </style>
</head>
<body>
  <svg class="ambient-waves" viewBox="0 0 1440 900" fill="none" preserveAspectRatio="none" aria-hidden="true">
    <path d="M-100 220 C 320 120, 680 340, 1540 160" stroke="var(--wave-stroke)" stroke-width="1.5" />
    <path d="M-100 680 C 420 560, 880 780, 1540 610" stroke="var(--wave-stroke)" stroke-width="1.5" />
  </svg>

  <main class="page">
    <header class="topbar">
      <div class="status-badge mono">
        <span class="status-dot" aria-hidden="true"></span>
        <span>v{esc(snap.version)}</span>
        <span class="status-sep">·</span>
        <span>{gil_tag}</span>
        <span class="status-sep">·</span>
        <span>{esc(snap.topology)} ({esc(worker_label)}, {esc(snap.loop_backend)})</span>
      </div>
      <button type="button" class="theme-btn" id="theme-toggle" data-theme-toggle aria-label="Toggle color theme">
        <span id="theme-label">Light mode</span>
      </button>
    </header>

    <section class="hero">
      <div class="hero-lockup">
        <svg class="hero-logo" viewBox="0 0 256 256" fill="none" role="img" aria-label="Vertebra Logo">
          <path d="M128 26L168 54L128 82L88 54Z" stroke="#EDB641" stroke-width="14" stroke-linecap="round" stroke-linejoin="round"/>
          <path d="M88 54L36 40" stroke="#EDB641" stroke-width="14" stroke-linecap="round" stroke-linejoin="round"/>
          <path d="M168 54L220 40" stroke="#EDB641" stroke-width="14" stroke-linecap="round" stroke-linejoin="round"/>
          <path d="M128 100L164 128L128 156L92 128Z" stroke="#EDB641" stroke-width="14" stroke-linecap="round" stroke-linejoin="round"/>
          <path d="M92 128L44 114" stroke="#EDB641" stroke-width="14" stroke-linecap="round" stroke-linejoin="round"/>
          <path d="M164 128L212 114" stroke="#EDB641" stroke-width="14" stroke-linecap="round" stroke-linejoin="round"/>
          <path d="M128 174L160 202L128 230L96 202Z" stroke="#EDB641" stroke-width="14" stroke-linecap="round" stroke-linejoin="round"/>
          <path d="M96 202L54 188" stroke="#EDB641" stroke-width="14" stroke-linecap="round" stroke-linejoin="round"/>
          <path d="M160 202L202 188" stroke="#EDB641" stroke-width="14" stroke-linecap="round" stroke-linejoin="round"/>
          <path d="M128 82V100" stroke="#EDB641" stroke-width="14" stroke-linecap="round" stroke-linejoin="round"/>
          <path d="M128 156V174" stroke="#EDB641" stroke-width="14" stroke-linecap="round" stroke-linejoin="round"/>
        </svg>
        <div class="hero-divider" aria-hidden="true"></div>
        <h1 class="brand-title">VERTEBRA</h1>
      </div>
      <p class="hero-lead">
        The development server is running. Register a <code>@app.get("/")</code> route to replace this welcome page.
      </p>
    </section>

    <section class="code-card" aria-label="Quickstart route example">
      <div class="code-header">
        <span class="mono">app.py</span>
        <button type="button" class="copy-btn" id="copy-btn">Copy</button>
      </div>
      <pre class="code-body mono" id="quickstart-code"><span class="kw">from</span> vertebra <span class="kw">import</span> App

app = <span class="fn">App</span>()


<span class="dec">@app.get</span>(<span class="str">"/"</span>)
<span class="kw">def</span> <span class="fn">index</span>() -&gt; <span class="fn">dict</span>[<span class="fn">str</span>, <span class="fn">str</span>]:
    <span class="kw">return</span> {{<span class="str">"status"</span>: <span class="str">"ok"</span>}}</pre>
    </section>

    <section class="endpoints-bar" data-endpoint-tester aria-label="Registered endpoints">
      {endpoints_html}
    </section>

    <nav class="links-grid" aria-label="Resources">
      <a class="link-card" href="https://cofin.github.io/vertebra/" target="_blank" rel="noopener">
        <div class="link-card-title">
          <span>Documentation</span>
          <span class="link-card-arrow">→</span>
        </div>
        <div class="link-card-desc">Quickstart, routing, shared-memory workers, and API reference</div>
      </a>
      <a class="link-card" href="{docs_card_href}">
        <div class="link-card-title">
          <span>OpenAPI &amp; Health</span>
          <span class="link-card-arrow">→</span>
        </div>
        <div class="link-card-desc">{docs_card_meta}</div>
      </a>
      <a class="link-card" href="https://github.com/litestar-org" target="_blank" rel="noopener">
        <div class="link-card-title">
          <span>Litestar Ecosystem</span>
          <span class="link-card-arrow">→</span>
        </div>
        <div class="link-card-desc">Litestar, msgspec, SQLSpec, and Advanced Alchemy on GitHub</div>
      </a>
    </nav>
  </main>

  <script>
    (function () {{
      var root = document.documentElement;
      var themeBtn = document.getElementById("theme-toggle");
      var themeLabel = document.getElementById("theme-label");

      function applyTheme(theme) {{
        root.setAttribute("data-theme", theme);
        if (themeLabel) {{
          themeLabel.textContent = theme === "dark" ? "Light mode" : "Dark mode";
        }}
        try {{
          localStorage.setItem("vertebra-welcome-theme", theme);
        }} catch (_err) {{}}
      }}

      try {{
        var saved = localStorage.getItem("vertebra-welcome-theme");
        if (saved === "light" || saved === "dark") {{
          applyTheme(saved);
        }}
      }} catch (_err) {{}}

      if (themeBtn) {{
        themeBtn.addEventListener("click", function () {{
          var current = root.getAttribute("data-theme") === "light" ? "light" : "dark";
          applyTheme(current === "dark" ? "light" : "dark");
        }});
      }}

      var copyBtn = document.getElementById("copy-btn");
      var codeEl = document.getElementById("quickstart-code");
      if (copyBtn && codeEl) {{
        copyBtn.addEventListener("click", function () {{
          var raw = codeEl.textContent || "";
          if (navigator.clipboard && navigator.clipboard.writeText) {{
            navigator.clipboard.writeText(raw).then(function () {{
              copyBtn.textContent = "Copied";
              setTimeout(function () {{ copyBtn.textContent = "Copy"; }}, 1400);
            }});
          }}
        }});
      }}
    }})();
  </script>
</body>
</html>"""

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
