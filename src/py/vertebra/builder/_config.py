"""Build configuration from ``[tool.vertebra]`` in *pyproject.toml*.

Reads project-level build settings including JavaScript dependency
declarations and bundler preferences.
"""

import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# JavaScript dependency configuration
# ---------------------------------------------------------------------------


@dataclass
class JsConfig:
    """JavaScript dependency configuration from ``[tool.vertebra.js]``."""

    dependencies: tuple[str, ...] = ()
    dev_dependencies: tuple[str, ...] = ()


# ---------------------------------------------------------------------------
# Top-level build configuration
# ---------------------------------------------------------------------------


@dataclass
class BuildConfig:
    """Build configuration loaded from ``[tool.vertebra]`` in *pyproject.toml*.

    Attributes:
        entry: Frontend entry point (e.g. ``"src/main.ts"``). Empty string means no frontend.
        outdir: Output directory for bundled assets (relative to project root).
        bundler: The bundler backend to use (``"native"`` by default).
        dev_port: HMR dev-server port.  ``None`` means auto-select.
        codegen: Whether to run type codegen before bundling.
        codegen_output: Directory for generated type files.
        app: Application import path (e.g. ``"myapp:app"``).
        js: JavaScript/npm dependency declarations.
    """

    entry: str = ""
    outdir: str = "dist"
    bundler: str = "native"
    dev_port: int | None = None
    codegen: bool = False
    codegen_output: str = ".vertebra/generated"
    app: str = ""
    js: JsConfig = field(default_factory=JsConfig)

    # -- factory ----------------------------------------------------------

    @classmethod
    def load(cls, pyproject_path: Path | str) -> "BuildConfig":
        """Load build configuration from a *pyproject.toml* file.

        Returns a default :class:`BuildConfig` when the file is missing or
        the ``[tool.vertebra]`` section is absent.

        Args:
            pyproject_path: Path to the *pyproject.toml* file.

        Returns:
            A fully-populated :class:`BuildConfig`.
        """
        pyproject_path = Path(pyproject_path)

        if not pyproject_path.exists():
            return cls()

        with open(pyproject_path, "rb") as fh:
            data: dict[str, Any] = tomllib.load(fh)

        vertebra_cfg = data.get("tool", {}).get("vertebra", {})
        if not vertebra_cfg:
            return cls()

        entry: str = vertebra_cfg.get("entry", "")
        outdir: str = vertebra_cfg.get("outdir", "dist")
        bundler: str = vertebra_cfg.get("bundler", "native")
        dev_port_raw = vertebra_cfg.get("dev-port") or vertebra_cfg.get("dev_port")
        dev_port: int | None = int(dev_port_raw) if dev_port_raw is not None else None
        codegen: bool = bool(vertebra_cfg.get("codegen", False))
        codegen_output: str = vertebra_cfg.get(
            "codegen-output", vertebra_cfg.get("codegen_output", ".vertebra/generated")
        )
        app_path: str = vertebra_cfg.get("app", "")

        # -- JS section ---------------------------------------------------
        js_section: dict[str, Any] = vertebra_cfg.get("js", {})
        dependencies = tuple(js_section.get("dependencies", ()))

        # Accept both hyphenated and underscored keys
        dev_deps_raw = js_section.get("dev-dependencies") or js_section.get("dev_dependencies", ())
        dev_dependencies = tuple(dev_deps_raw)

        js = JsConfig(dependencies=dependencies, dev_dependencies=dev_dependencies)

        return cls(
            entry=entry,
            outdir=outdir,
            bundler=bundler,
            dev_port=dev_port,
            codegen=codegen,
            codegen_output=codegen_output,
            app=app_path,
            js=js,
        )


__all__ = ("BuildConfig", "JsConfig")
