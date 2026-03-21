"""Build orchestrator for Vertebra frontend integration.

Defaults to the native bundler (Rolldown via vertebra._bundler) with
automatic fallback to Vite when the native extension is unavailable.
"""

import logging
import socket
from pathlib import Path

from vertebra.builder._config import BuildConfig

logger = logging.getLogger("vertebra.build")

__all__ = ["BuildOrchestrator"]


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
        self._vite_server: object | None = None

    def build(self) -> bool:
        """Production build. Returns True if frontend was built."""
        if not self.config.entry:
            return False
        if self.config.codegen:
            self._run_codegen()
        if self.config.js.dependencies:
            self._resolve_js_deps()
        self._run_bundler(dev=False)
        return True

    def dev(self) -> None:
        """Dev mode: native watcher + HMR."""
        self._dev_port = self._resolve_dev_port()
        if self.config.codegen:
            self._run_codegen()
        if self.config.js.dependencies:
            self._resolve_js_deps()
        self._run_bundler(dev=True)

    def codegen_only(self, *, output_dir: Path | None = None) -> Path:
        """Run only type codegen, return output directory."""
        out = output_dir or (self.project_dir / self.config.codegen_output)
        out.mkdir(parents=True, exist_ok=True)
        self._run_codegen(output_dir=out)
        return out

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
        from vertebra._bundler import BundleConfig, DevConfig, RolldownBundler

        bundler = RolldownBundler()
        if dev:
            dev_config = DevConfig(
                entry=str(self.project_dir / self.config.entry),
                watch_dirs=[str(self.project_dir / "src")],
                hmr_port=self._dev_port or 5173,
            )
            bundler.start_dev(dev_config)
        else:
            bundle_config = BundleConfig(
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
            from vertebra.builder._fallback import ViteDevServer

            self._vite_server = ViteDevServer(self.project_dir, self._dev_port or 5173)
            self._vite_server.start()

            import atexit
            import os

            os.environ["VERTEBRA_VITE_PROXY"] = self._vite_server.proxy_url
            atexit.register(self._vite_server.stop)
        else:
            from vertebra.builder._fallback import run_vite_build

            run_vite_build(self.project_dir)

    def _resolve_js_deps(self) -> None:
        """Resolve JavaScript dependencies via import map."""
        from vertebra.builder._import_map import write_import_map

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
            from vertebra.builder._codegen._collector import RouteCollector
            from vertebra.builder._codegen._manifest import RouteManifest
            from vertebra.builder._codegen._typescript import TypeScriptGenerator
            from vertebra.builder._codegen._zod import ZodGenerator
            from vertebra.supervisor._worker import _import_app

            # 1. Load app and collect routes
            app = _import_app(self._app_path)
            collector = RouteCollector()
            routes = collector.collect(app)

            # 2. Generate assets
            ts_gen = TypeScriptGenerator()
            ts_content = ts_gen.generate(routes)
            (out / "types.d.ts").write_text(ts_content)

            zod_gen = ZodGenerator()
            zod_content = zod_gen.generate(routes)
            (out / "schemas.zod.ts").write_text(zod_content)

            manifest_gen = RouteManifest()
            manifest_content = manifest_gen.generate(routes)
            (out / "manifest.json").write_text(manifest_content)

            # Also generate a basic routes.ts for convenience
            (out / "routes.ts").write_text("// Auto-generated route tree\nexport const routes = {};\n")

            logger.info("Codegen complete: %d routes", len(routes))

        except Exception:
            logger.exception("Codegen failed")
            raise
