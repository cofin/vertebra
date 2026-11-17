"""Server-Side Rendering (SSR) and SSG prerendering engine for Vertebra.

Provides native QuickJS isolate pools via vertebra-bundler with graceful
Node/Bun sidecar subprocess fallbacks.
"""

from __future__ import annotations

import contextlib
import logging
import os
import re
import shutil
import signal
import socket
import subprocess
import time
import urllib.request
from pathlib import Path
from typing import Any, Literal, Protocol

import msgspec

from vertebra.build._assets import ResolvedAssetTags, escape_html_json
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import decode_json, encode_json
from vertebra.utils.sync_tools import SyncPool

__all__ = (
    "AsyncEmbeddedSSRClient",
    "AsyncSSRClient",
    "AsyncSidecarSSRClient",
    "SSRConfig",
    "SSRFrame",
    "SSRRenderTimeoutError",
    "SSRResult",
    "SyncEmbeddedSSRClient",
    "SyncSSRClient",
    "SyncSidecarSSRClient",
    "create_async_ssr_client",
    "create_ssr_client",
    "create_sync_ssr_client",
    "prerender_routes",
    "render_page_shell",
)

logger = logging.getLogger(__name__)

_bundler = import_optional("vertebra._bundler")


class SSRRenderTimeoutError(TimeoutError):
    """Raised when a native QuickJS or sidecar SSR render exceeds its configured deadline."""


class SSRFrame(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Length-prefixed binary frame for SSR IPC communication."""

    component: str
    props: dict[str, Any] = msgspec.field(default_factory=dict)

    @classmethod
    def encode(cls, component: str, props: dict[str, Any] | None = None) -> bytes:
        """Encode a component name and props dictionary into a length-prefixed frame."""
        payload = encode_json({"component": component, "props": props or {}}).encode("utf-8")
        prefix = len(payload).to_bytes(4, "big")
        return prefix + payload

    @classmethod
    def decode(cls, data: bytes) -> SSRFrame:
        """Decode a length-prefixed binary frame into an SSRFrame."""
        payload = data[4:]
        parsed = decode_json(payload)
        return cls(component=parsed.get("component", ""), props=parsed.get("props", {}))


class SSRConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for SSR execution backends and static site prerendering."""

    mode: Literal["native", "sidecar"] = "native"
    entry: str = ""
    bundle_path: str | None = None
    sidecar_url: str = "http://127.0.0.1:13714/render"
    sidecar_port: int = 13714
    pool_size: int = 4
    timeout_ms: int = 1000
    timeout: float = 2.0
    prerender_routes: tuple[str, ...] = ()
    fallback_to_csr: bool = True


class SSRResult(msgspec.Struct, frozen=True, gc=False):
    """Result of an SSR render operation."""

    html: str = ""
    head: list[str] = msgspec.field(default_factory=list)
    body: str = ""
    status: int = 200
    fallback_csr: bool = False


class SyncSSRClient(Protocol):
    """Synchronous SSR client protocol implemented by native and sidecar engines."""

    def render(self, page: dict[str, Any]) -> SSRResult: ...
    def invalidate(self, paths: list[str] | None = None) -> None: ...
    def stop(self) -> None: ...

    @staticmethod
    def from_config(config: SSRConfig) -> SyncSSRClient | None:
        """Instantiate a SyncSSRClient from an SSRConfig."""
        return create_sync_ssr_client(
            mode=config.mode,
            entry=config.entry or config.bundle_path,
            pool_size=config.pool_size,
            sidecar_port=config.sidecar_port,
        )


class AsyncSSRClient(Protocol):
    """Asynchronous SSR client protocol implemented by native and sidecar engines."""

    async def render(self, page: dict[str, Any]) -> SSRResult: ...
    def invalidate(self, paths: list[str] | None = None) -> None: ...
    def stop(self) -> None: ...

    @staticmethod
    def from_config(config: SSRConfig) -> AsyncSSRClient | None:
        """Instantiate an AsyncSSRClient from an SSRConfig."""
        return create_async_ssr_client(
            mode=config.mode,
            entry=config.entry or config.bundle_path,
            pool_size=config.pool_size,
            sidecar_port=config.sidecar_port,
        )


class SyncEmbeddedSSRClient:
    """Embedded QuickJS isolate pool in the vertebra-bundler Rust extension (sync)."""

    __slots__ = ("_pool",)

    def __init__(self, bundle_path: str, pool_size: int = 4, timeout_ms: int = 1000) -> None:
        if _bundler is None:
            msg = "vertebra._bundler native extension is required for SyncEmbeddedSSRClient"
            raise ImportError(msg)
        self._pool = _bundler.SSRPool(bundle_path, pool_size, timeout_ms)

    @staticmethod
    def from_config(config: SSRConfig) -> SyncEmbeddedSSRClient | None:
        """Instantiate a SyncEmbeddedSSRClient from an SSRConfig."""
        entry = config.entry or config.bundle_path
        if not entry:
            return None
        return SyncEmbeddedSSRClient(entry, pool_size=config.pool_size, timeout_ms=config.timeout_ms)

    def render(self, page: dict[str, Any]) -> SSRResult:
        """Render an Inertia page object via the native QuickJS pool."""
        try:
            result = self._pool.render(
                page.get("component", ""),
                encode_json(page),
            )
        except Exception as exc:
            if "timed out" in str(exc).lower():
                raise SSRRenderTimeoutError(str(exc)) from exc
            raise
        return SSRResult(
            html=result.body,
            head=list(result.head),
            body=result.body,
            status=200,
            fallback_csr=False,
        )

    def invalidate(self, paths: list[str] | None = None) -> None:
        """Invalidate cached modules in the QuickJS pool."""
        _ = paths
        self._pool.invalidate()

    def stop(self) -> None:
        """Shut down the QuickJS isolate pool."""
        self._pool.shutdown()


class AsyncEmbeddedSSRClient:
    """Embedded QuickJS isolate pool in the vertebra-bundler Rust extension (async)."""

    __slots__ = ("_sync_client",)

    def __init__(self, bundle_path: str, pool_size: int = 4, timeout_ms: int = 1000) -> None:
        self._sync_client = SyncEmbeddedSSRClient(bundle_path, pool_size=pool_size, timeout_ms=timeout_ms)

    @staticmethod
    def from_config(config: SSRConfig) -> AsyncEmbeddedSSRClient | None:
        """Instantiate an AsyncEmbeddedSSRClient from an SSRConfig."""
        entry = config.entry or config.bundle_path
        if not entry:
            return None
        return AsyncEmbeddedSSRClient(entry, pool_size=config.pool_size, timeout_ms=config.timeout_ms)

    async def render(self, page: dict[str, Any]) -> SSRResult:
        """Render an Inertia page object asynchronously via the native QuickJS pool."""
        return await SyncPool.default().submit_async(self._sync_client.render, page)

    def invalidate(self, paths: list[str] | None = None) -> None:
        """Invalidate cached modules in the QuickJS pool."""
        self._sync_client.invalidate(paths)

    def stop(self) -> None:
        """Shut down the QuickJS isolate pool."""
        self._sync_client.stop()


class _BaseSidecarSSRClient:
    """Base class for Inertia SSR server managed as an external Node.js or Bun subprocess."""

    __slots__ = ("_entry", "_port", "_process")

    _STARTUP_TIMEOUT: float = 30.0
    _SHUTDOWN_TIMEOUT: float = 5.0

    def __init__(self, entry: str, port: int = 13714) -> None:
        self._entry = entry
        self._port = port
        self._process: subprocess.Popen[bytes] | None = None
        self._start()

    @property
    def _base_url(self) -> str:
        return f"http://127.0.0.1:{self._port}"

    def invalidate(self, paths: list[str] | None = None) -> None:
        """Invalidate cached modules in sidecar mode."""
        _ = paths

    def stop(self) -> None:
        """Gracefully terminate the sidecar subprocess."""
        with contextlib.suppress(Exception):
            req = urllib.request.Request(
                f"{self._base_url}/shutdown",
                data=b"",
                method="POST",
            )
            urllib.request.urlopen(req, timeout=1)

        process = self._process
        if process is None or process.poll() is not None:
            return
        self._process = None

        with contextlib.suppress(OSError, ProcessLookupError, subprocess.TimeoutExpired):
            if hasattr(os, "killpg"):
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)
            else:
                process.terminate()
            process.wait(timeout=self._SHUTDOWN_TIMEOUT)

    def _render_sync(self, page: dict[str, Any]) -> SSRResult:
        """Render an Inertia page object synchronously via HTTP POST to the sidecar."""
        data = encode_json(page, as_bytes=True)
        req = urllib.request.Request(
            f"{self._base_url}/render",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                result = decode_json(resp.read())
                body_content = result.get("body", "")
                return SSRResult(
                    html=body_content,
                    head=result.get("head", []),
                    body=body_content,
                    status=200,
                    fallback_csr=False,
                )
        except Exception as exc:
            msg = f"SSR sidecar render failed: {exc}"
            raise RuntimeError(msg) from exc

    def _start(self) -> None:
        runtime = shutil.which("bun") or shutil.which("node")
        if not runtime:
            msg = "Node.js or Bun required for SSR sidecar mode"
            raise RuntimeError(msg)

        env = {**os.environ, "PORT": str(self._port)}

        self._process = subprocess.Popen(
            [runtime, self._entry],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )

        if not self._wait_for_health():
            self.stop()
            msg = f"SSR server failed to start on port {self._port}"
            raise RuntimeError(msg)

        logger.info("SSR sidecar ready on port %d", self._port)

    def _wait_for_health(self, timeout: float | None = None) -> bool:
        if timeout is None:
            timeout = self._STARTUP_TIMEOUT
        deadline = time.monotonic() + timeout
        interval = 0.1
        while time.monotonic() < deadline:
            if self._process is not None and self._process.poll() is not None:
                return False
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(0.5)
                try:
                    s.connect(("127.0.0.1", self._port))
                    return True
                except (ConnectionRefusedError, OSError):
                    pass
            time.sleep(interval)
            interval = min(interval * 1.5, 1.0)
        return False


class SyncSidecarSSRClient(_BaseSidecarSSRClient):
    """Inertia SSR server managed as an external Node.js or Bun subprocess (sync)."""

    @staticmethod
    def from_config(config: SSRConfig) -> SyncSidecarSSRClient | None:
        """Instantiate a SyncSidecarSSRClient from an SSRConfig."""
        entry = config.entry or config.bundle_path
        if not entry:
            return None
        return SyncSidecarSSRClient(entry, port=config.sidecar_port)

    def render(self, page: dict[str, Any]) -> SSRResult:
        """Render an Inertia page object via HTTP POST to the sidecar /render endpoint."""
        return self._render_sync(page)


class AsyncSidecarSSRClient(_BaseSidecarSSRClient):
    """Inertia SSR server managed as an external Node.js or Bun subprocess (async)."""

    @staticmethod
    def from_config(config: SSRConfig) -> AsyncSidecarSSRClient | None:
        """Instantiate an AsyncSidecarSSRClient from an SSRConfig."""
        entry = config.entry or config.bundle_path
        if not entry:
            return None
        return AsyncSidecarSSRClient(entry, port=config.sidecar_port)

    async def render(self, page: dict[str, Any]) -> SSRResult:
        """Render an Inertia page object asynchronously via HTTP POST to the sidecar."""
        return await SyncPool.default().submit_async(self._render_sync, page)


def create_sync_ssr_client(
    *,
    mode: str = "native",
    entry: str | None = None,
    pool_size: int = 4,
    sidecar_port: int = 13714,
) -> SyncSSRClient | None:
    """Create the appropriate synchronous SSR client based on mode."""
    if not entry:
        return None

    if mode == "native":
        try:
            return SyncEmbeddedSSRClient(entry, pool_size)
        except ImportError:
            logger.warning("Native SSR (vertebra._bundler) unavailable. Falling back to sidecar mode.")
            return SyncSidecarSSRClient(entry, sidecar_port)
        except Exception:
            logger.exception("Native SSR init failed, falling back to sidecar")
            return SyncSidecarSSRClient(entry, sidecar_port)

    return SyncSidecarSSRClient(entry, sidecar_port)


def create_async_ssr_client(
    *,
    mode: str = "native",
    entry: str | None = None,
    pool_size: int = 4,
    sidecar_port: int = 13714,
) -> AsyncSSRClient | None:
    """Create the appropriate asynchronous SSR client based on mode."""
    if not entry:
        return None

    if mode == "native":
        try:
            return AsyncEmbeddedSSRClient(entry, pool_size)
        except ImportError:
            logger.warning("Native SSR (vertebra._bundler) unavailable. Falling back to sidecar mode.")
            return AsyncSidecarSSRClient(entry, sidecar_port)
        except Exception:
            logger.exception("Native SSR init failed, falling back to sidecar")
            return AsyncSidecarSSRClient(entry, sidecar_port)

    return AsyncSidecarSSRClient(entry, sidecar_port)


def create_ssr_client(
    *,
    mode: str = "native",
    entry: str | None = None,
    pool_size: int = 4,
    sidecar_port: int = 13714,
    async_mode: bool = False,
) -> SyncSSRClient | AsyncSSRClient | None:
    """Create the appropriate SSR client based on mode and concurrency model."""
    if async_mode:
        return create_async_ssr_client(
            mode=mode,
            entry=entry,
            pool_size=pool_size,
            sidecar_port=sidecar_port,
        )
    return create_sync_ssr_client(
        mode=mode,
        entry=entry,
        pool_size=pool_size,
        sidecar_port=sidecar_port,
    )


def render_page_shell(
    page: dict[str, Any],
    result: SSRResult | None = None,
    root_template: str | None = None,
    *,
    asset_tags: ResolvedAssetTags | None = None,
    csp_nonce: str | None = None,
    static_props: dict[str, Any] | None = None,
) -> str:
    """Render full HTML page shell for Inertia or prerendering with XSS-safe JSON escaping."""
    head_items = list(result.head) if result else []
    if asset_tags is not None:
        head_items.extend(asset_tags.tags)
    if static_props is not None:
        nonce_attr = f' nonce="{csp_nonce}"' if csp_nonce else ""
        escaped_static = escape_html_json(encode_json(static_props))
        head_items.append(
            f'<script id="vertebra-static-props" type="application/json"{nonce_attr}>{escaped_static}</script>'
        )
    body_content = result.body or result.html if result else ""
    page_json = escape_html_json(encode_json(page))
    nonce_attr = f' nonce="{csp_nonce}"' if csp_nonce else ""
    app_div = (
        f'<div id="app">\n{body_content}\n'
        f'<script data-page="app" type="application/json"{nonce_attr}>\n{page_json}\n</script>\n</div>'
    )
    if root_template and (
        "<!--ssr-body-->" in root_template
        or "<!--inertia-->" in root_template
        or "@inertia" in root_template
        or "<body" in root_template.lower()
    ):
        html = root_template
        head_str = "\n".join(head_items)
        head_tokens = ("<!--ssr-head-->", "<!--inertia-head-->", "@inertiaHead")
        has_head_marker = any(token in html for token in head_tokens)
        for token in head_tokens:
            html = html.replace(token, head_str)
        if not has_head_marker and head_str and "</head>" in html.lower():
            html = re.sub(
                r"</head>",
                lambda match: f"{head_str}\n{match.group(0)}",
                html,
                count=1,
                flags=re.IGNORECASE,
            )
        has_inertia_marker = "<!--inertia-->" in html or "@inertia" in html
        if has_inertia_marker:
            if "<!--ssr-body-->" in html:
                html = html.replace("<!--ssr-body-->", body_content)
            replaced = False
            for token in ("<!--inertia-->", "@inertia"):
                if token in html:
                    html = html.replace(token, "" if replaced else app_div, 1)
                    replaced = True
            return html
        if "<!--ssr-body-->" in html:
            return html.replace("<!--ssr-body-->", app_div)
        if "</body>" in html.lower():
            return re.sub(
                r"</body>",
                lambda match: f"{app_div}\n{match.group(0)}",
                html,
                count=1,
                flags=re.IGNORECASE,
            )
        return re.sub(
            r"<body[^>]*>",
            lambda match: f"{match.group(0)}\n{app_div}",
            html,
            count=1,
            flags=re.IGNORECASE,
        )
    head_str = "\n".join(head_items)
    return (
        "<!DOCTYPE html>\n"
        "<html>\n"
        "<head>\n"
        '<meta charset="utf-8">\n'
        f"{head_str}\n"
        "</head>\n"
        "<body>\n"
        f"{app_div}\n"
        "</body>\n"
        "</html>"
    )


def prerender_routes(
    *,
    ssr_client: SyncSSRClient,
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

        html = render_page_shell(page, result, root_template)

        clean_path = route_path.strip("/")
        out_path = outdir / "index.html" if not clean_path else outdir / clean_path / "index.html"

        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(html, encoding="utf-8")
        written.append(out_path)
        logger.info("Pre-rendered: %s -> %s", route_path, out_path)

    logger.info("Pre-rendered %d pages to %s", len(written), outdir)
    return written
