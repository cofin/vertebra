"""Build orchestrator for Vertebra frontend integration.

Defaults to the native bundler (Rolldown via vertebra._bundler) with
automatic fallback to Vite when the native extension is unavailable.
"""

import atexit
import importlib
import json
import logging
import os
import socket
from pathlib import Path
from typing import Any

from vertebra.builder._codegen._manifest import ManifestGenerator
from vertebra.builder._codegen._metadata import RouteCollector
from vertebra.builder._codegen._typescript import TypeScriptGenerator
from vertebra.builder._codegen._zod import ZodGenerator
from vertebra.builder._config import BuildConfig
from vertebra.builder._fallback import DevServerProcess, run_vite_build
from vertebra.builder._import_map import write_import_map
from vertebra.utils.module_loader import import_app

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
                vite_path.write_text(json.dumps(vite_fn(), indent=2), encoding="utf-8")
                exported["vite"] = vite_path

            asyncapi_fn = getattr(item, "export_asyncapi_schema", None)
            if callable(asyncapi_fn) and "asyncapi" not in exported:
                asyncapi_path = out / "asyncapi.json"
                asyncapi_path.write_text(json.dumps(asyncapi_fn(), indent=2), encoding="utf-8")
                exported["asyncapi"] = asyncapi_path

        return exported

    @staticmethod
    def _find_available_port(start: int = 5173, end: int = 5200) -> int:
        """Find an available port in the given range."""
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
        """Resolve the dev server port from config or auto-select."""
        if self.config.dev_port is not None:
            return self.config.dev_port
        port = self._find_available_port()
        logger.info("Auto-selected dev port: %d", port)
        return port

    def _run_bundler(self, *, dev: bool) -> None:
        """Run the configured bundler with native-first fallback."""
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
        """Run the native Rolldown bundler."""
        bundler_mod = importlib.import_module("vertebra._bundler")
        bundler = bundler_mod.Bundler()
        if dev:
            dev_config = bundler_mod.DevConfig(
                entry=str(self.project_dir / self.config.entry),
                watch_dirs=[str(self.project_dir / "src")],
                hmr_port=self._dev_port or 5173,
            )
            bundler.start_dev(dev_config)
        else:
            bundle_config = bundler_mod.BundleConfig(
                entry=str(self.project_dir / self.config.entry),
                outdir=str(self.project_dir / self.config.outdir),
                minify=True,
                sourcemap=True,
            )
            result = bundler.build(bundle_config)
            logger.info("Build: %d files in %dms", len(result.outputs), result.duration_ms)

    def _run_vite_fallback(self, *, dev: bool) -> None:
        """Run Vite as fallback bundler."""
        if dev:
            self._vite_server = DevServerProcess(self.project_dir, self._dev_port or 5173)
            self._vite_server.start()
            os.environ["VERTEBRA_VITE_PROXY"] = self._vite_server.proxy_url
            atexit.register(self._vite_server.stop)
        else:
            run_vite_build(self.project_dir)

    def _resolve_js_deps(self) -> None:
        """Resolve JavaScript dependencies via import map."""
        vertebra_dir = self.project_dir / ".vertebra"
        write_import_map(self.config.js, vertebra_dir)

    def _run_codegen(self, *, output_dir: Path | None = None) -> None:
        """Run type codegen if configured."""
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
            (out / "types.d.ts").write_text(ts_content)

            zod_gen = ZodGenerator()
            zod_content = zod_gen.generate(routes)
            (out / "schemas.zod.ts").write_text(zod_content)

            manifest_gen = ManifestGenerator()
            manifest_content = manifest_gen.generate(routes)
            (out / "manifest.json").write_text(manifest_content)

            (out / "routes.ts").write_text("// Auto-generated route tree\nexport const routes = {};\n")
            self.export_litestar_artifacts(app, output_dir=out)

            logger.info("Codegen complete: %d routes", len(routes))
        except Exception:
            logger.exception("Codegen failed")
            raise
