"""Build orchestrator for Vertebra frontend integration.

Defaults to the native bundler (Rolldown via vertebra._bundler) with
automatic fallback to Vite when the native extension is unavailable.
"""

import atexit
import contextlib
import logging
import os
import socket
import threading
from pathlib import Path
from typing import Any

from vertebra.build._config import BuildConfig
from vertebra.build._fallback import DevServerProcess, run_vite_build
from vertebra.build._import_map import ImportMapGenerator
from vertebra.build.codegen._manifest import ManifestGenerator
from vertebra.build.codegen._metadata import RouteCollector, RouteMetadata
from vertebra.build.codegen._typescript import TypeScriptGenerator
from vertebra.build.codegen._watch import HmrNotifier
from vertebra.build.codegen.zod import ZodGenerator
from vertebra.utils.module_loader import ensure_module, import_app
from vertebra.utils.serializers import encode_json, format_json

__all__ = ("BuildOrchestrator",)

logger = logging.getLogger("vertebra.build")


class BuildOrchestrator:
    """Orchestrate frontend builds with native-first, Vite-fallback strategy."""

    def __init__(
        self,
        project_dir: Path,
        config: BuildConfig | None = None,
        *,
        app_path: str | None = None,
        hmr_base_url: str | None = None,
    ) -> None:
        self.project_dir = project_dir.resolve()
        self.config = config or BuildConfig.load(self.project_dir)
        self._app_path = app_path or self.config.app
        self._hmr_base_url = hmr_base_url
        self._dev_port: int | None = None
        self._vite_server: DevServerProcess | None = None
        self._native_bundler: Any = None
        self._dev_watch_thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    @property
    def is_watching(self) -> bool:
        """Return True when the native bundler dev watcher loop is actively running."""
        return (
            self._native_bundler is not None
            and getattr(self._native_bundler, "is_dev_mode", False)
            and self._dev_watch_thread is not None
            and self._dev_watch_thread.is_alive()
        )

    def stop(self) -> None:
        """Stop background dev watchers, dev threads, and Vite server processes."""
        self._stop_event.set()
        if self._dev_watch_thread is not None and self._dev_watch_thread.is_alive():
            self._dev_watch_thread.join(timeout=1.0)
            self._dev_watch_thread = None
        if self._native_bundler is not None:
            with contextlib.suppress(Exception):
                self._native_bundler.stop_dev()
            self._native_bundler = None
        if self._vite_server is not None:
            with contextlib.suppress(Exception):
                self._vite_server.stop()
            self._vite_server = None

    def build(self) -> bool:
        """Run a production build, returning True if a frontend entry was built."""
        if not self.config.entry:
            return False
        if self.config.codegen:
            self._run_codegen()
        if self.config.js.dependencies:
            self._resolve_js_deps()
        self._run_bundler(dev=False)
        return True

    def dev(self) -> None:
        """Start dev mode with native watcher and HMR."""
        self._dev_port = self._resolve_dev_port()
        if self.config.codegen:
            self._run_codegen()
        if self.config.js.dependencies:
            self._resolve_js_deps()
        self._run_bundler(dev=True)

    def codegen_only(self, *, output_dir: Path | None = None) -> Path:
        """Run only type codegen and return the output directory."""
        out = output_dir or (self.project_dir / self.config.codegen_output)
        out.mkdir(parents=True, exist_ok=True)
        self._run_codegen(output_dir=out)
        return out

    def export_litestar_artifacts(
        self,
        app: Any,
        *,
        output_dir: Path | None = None,
    ) -> dict[str, Path]:
        """Auto-export litestar-vite and litestar-asyncapi artifacts when present on app."""
        out = output_dir or (self.project_dir / self.config.codegen_output)
        out.mkdir(parents=True, exist_ok=True)
        exported: dict[str, Path] = {}

        candidates: list[Any] = []
        plugins = getattr(app, "plugins", None)
        if plugins is not None:
            raw_list = getattr(plugins, "plugins", plugins)
            if isinstance(raw_list, (list, tuple, set)):
                candidates.extend(raw_list)
        state = getattr(app, "state", None)
        if isinstance(state, dict) or (state is not None and hasattr(state, "values")):
            candidates.extend(state.values())

        for item in candidates:
            vite_fn = getattr(item, "export_vite_metadata", None)
            if callable(vite_fn) and "vite" not in exported:
                vite_path = out / "litestar-vite.json"
                vite_path.write_text(format_json(vite_fn(), indent=2), encoding="utf-8")
                exported["vite"] = vite_path

            asyncapi_fn = getattr(item, "export_asyncapi_schema", None)
            if callable(asyncapi_fn) and "asyncapi" not in exported:
                asyncapi_path = out / "asyncapi.json"
                asyncapi_path.write_text(format_json(asyncapi_fn(), indent=2), encoding="utf-8")
                exported["asyncapi"] = asyncapi_path

        return exported

    @staticmethod
    def _find_available_port(start: int = 5173, end: int = 5200) -> int:
        for port in range(start, end):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                try:
                    sock.bind(("127.0.0.1", port))
                except OSError:
                    continue
                else:
                    return port
        msg = f"No available port in range {start}-{end}"
        raise RuntimeError(msg)

    def _resolve_dev_port(self) -> int:
        if self.config.dev_port is not None:
            return self.config.dev_port
        port = self._find_available_port()
        logger.info("Auto-selected dev port: %d", port)
        return port

    def _run_bundler(self, *, dev: bool) -> None:
        if not self.config.entry:
            return
        if self.config.bundler == "native":
            try:
                self._run_native(dev=dev)
            except ImportError:
                logger.warning("Native bundler unavailable, falling back to Vite")
                self._run_vite_fallback(dev=dev)
        else:
            self._run_vite_fallback(dev=dev)

    def _run_native(self, *, dev: bool) -> None:
        bundler_mod = ensure_module("vertebra._bundler", package="vertebra-core[bundler]", extra="bundler")
        bundle_config = bundler_mod.BundleConfig(
            entry=str(self.project_dir / self.config.entry),
            outdir=str(self.project_dir / self.config.outdir),
            minify=True,
            sourcemap=True,
        )
        dev_config = (
            bundler_mod.BundleDevConfig(
                entry=str(self.project_dir / self.config.entry),
                watch_dirs=[str(self.project_dir / "src")],
                hmr_port=self._dev_port or 5173,
            )
            if dev
            else None
        )
        bundler = bundler_mod.Bundler(bundle_config, dev_config=dev_config)
        result = bundler.build()
        logger.info("Build: %d files in %dms", len(result.outputs), result.duration_ms)

        if dev:
            self._stop_event.clear()
            self._native_bundler = bundler
            bundler.start_dev()
            atexit.register(self.stop)

            def _dev_loop() -> None:
                while not self._stop_event.is_set():
                    try:
                        changed = bundler.poll_events(200)
                    except Exception:
                        if self._stop_event.is_set():
                            break
                        logger.warning("Native bundler poll error", exc_info=True)
                        continue
                    if not changed:
                        continue
                    try:
                        bundler.build()
                        if self._hmr_base_url:
                            HmrNotifier(self._hmr_base_url).notify_reload(reason=changed[0])
                    except Exception:
                        logger.exception("Rebuild on file change failed")

            self._dev_watch_thread = threading.Thread(
                target=_dev_loop,
                name="vertebra-bundler-dev-watch",
                daemon=True,
            )
            self._dev_watch_thread.start()

    def _run_vite_fallback(self, *, dev: bool) -> None:
        if dev:
            self._vite_server = DevServerProcess(self.project_dir, self._dev_port or 5173)
            self._vite_server.start()
            os.environ["VERTEBRA_VITE_PROXY"] = self._vite_server.proxy_url
            atexit.register(self.stop)
        else:
            run_vite_build(self.project_dir)

    def _resolve_js_deps(self) -> None:
        vertebra_dir = self.project_dir / ".vertebra"
        ImportMapGenerator.write(self.config.js, vertebra_dir, include_client=True)
        ImportMapGenerator.sync_wheel_client(vertebra_dir)

    def _run_codegen(self, *, output_dir: Path | None = None) -> None:
        if not self._app_path:
            logger.warning("No app path configured for codegen")
            return

        out = output_dir or (self.project_dir / self.config.codegen_output)
        out.mkdir(parents=True, exist_ok=True)

        logger.info("Running type codegen -> %s", out)

        try:
            app = import_app(self._app_path)
            collector = RouteCollector()
            routes = collector.collect(app)

            ts_gen = TypeScriptGenerator()
            ts_content = ts_gen.generate(routes)
            (out / "types.d.ts").write_text(ts_content, encoding="utf-8")

            zod_gen = ZodGenerator()
            zod_content = zod_gen.generate(routes)
            (out / "schemas.zod.ts").write_text(zod_content, encoding="utf-8")

            manifest_gen = ManifestGenerator()
            manifest_content = manifest_gen.generate(routes)
            (out / "manifest.json").write_text(manifest_content, encoding="utf-8")

            (out / "routes.ts").write_text(self._render_routes_ts(routes), encoding="utf-8")
            self.export_litestar_artifacts(app, output_dir=out)

            logger.info("Codegen complete: %d routes", len(routes))
        except Exception:
            logger.exception("Codegen failed")
            raise

    @staticmethod
    def _render_routes_ts(routes: list[RouteMetadata]) -> str:
        """Render typed TypeScript HTTP and WebSocket route registries and wsUrl helper."""
        lines = [
            "// Auto-generated route tree",
            "export const routes = {",
        ]
        for route in sorted(routes, key=lambda r: r.operation_id):
            escaped_op = encode_json(route.operation_id)
            lines.append(f"  {escaped_op}: {{ method: {encode_json(route.method)}, path: {encode_json(route.path)} }},")
        lines.append("} as const;")

        ws_routes = sorted(
            (route for route in routes if route.protocol == "websocket"),
            key=lambda r: r.channel_key or r.operation_id,
        )
        if ws_routes:
            lines.extend(
                [
                    "",
                    "export const websocketRoutes = {",
                ]
            )
            for route in ws_routes:
                key = encode_json(route.channel_key or route.operation_id)
                lines.append(f'  {key}: {{ method: "WS", path: {encode_json(route.path)} }},')
            lines.extend(
                [
                    "} as const;",
                    "",
                    "export type WebSocketRouteName = keyof typeof websocketRoutes;",
                    "",
                    "export function wsUrl<K extends WebSocketRouteName>(",
                    "  name: K,",
                    "  params?: Record<string, string | number | boolean>,",
                    "  options?: { baseUrl?: string; query?: Record<string, string | number | boolean | null | undefined> },",
                    "): string {",
                    "  const entry = websocketRoutes[name];",
                    "  if (!entry) {",
                    "    throw new Error(`Unknown WebSocket route: ${name}`);",
                    "  }",
                    "  let path: string = entry.path;",
                    "  if (params) {",
                    "    for (const [k, v] of Object.entries(params)) {",
                    "      const encoded = encodeURIComponent(String(v));",
                    "      path = path",
                    '        .replace(new RegExp(`\\\\{${k}(?::[^}]+)?\\\\}`, "g"), encoded)',
                    '        .replace(new RegExp(`:${k}(?=/|$)`, "g"), encoded);',
                    "    }",
                    "  }",
                    '  if (!path.startsWith("/")) {',
                    "    path = `/${path}`;",
                    "  }",
                    "  const rawBase =",
                    '    options?.baseUrl ?? (typeof window !== "undefined" && window.location ? window.location.origin : "ws://localhost");',
                    "  const wsBase = rawBase",
                    '    .replace(/^http:/i, "ws:")',
                    '    .replace(/^https:/i, "wss:")',
                    '    .replace(/\\/+$/, "");',
                    "  const url = `${wsBase}${path}`;",
                    "  if (!options?.query) {",
                    "    return url;",
                    "  }",
                    "  const pairs: string[] = [];",
                    "  for (const [k, v] of Object.entries(options.query)) {",
                    "    if (v === undefined || v === null) continue;",
                    "    pairs.push(`${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`);",
                    "  }",
                    '  return pairs.length > 0 ? `${url}?${pairs.join("&")}` : url;',
                    "}",
                ]
            )

        lines.append("")
        return "\n".join(lines)
