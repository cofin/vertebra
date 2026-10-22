"""Build configuration from ``[tool.vertebra]`` in *pyproject.toml*."""

import tomllib
from pathlib import Path
from typing import Any

import msgspec

__all__ = ("BuildConfig", "JsConfig")


class JsConfig(msgspec.Struct, frozen=True, gc=False):
    """JavaScript dependency configuration from ``[tool.vertebra.js]``."""

    dependencies: tuple[str, ...] = ()
    dev_dependencies: tuple[str, ...] = ()


class BuildConfig(msgspec.Struct, frozen=True, gc=False):
    """Build configuration loaded from ``[tool.vertebra]`` in *pyproject.toml*."""

    entry: str = ""
    outdir: str = "dist"
    bundler: str = "native"
    dev_port: int | None = None
    codegen: bool = False
    codegen_output: str = ".vertebra/generated"
    app: str = ""
    js: JsConfig = msgspec.field(default_factory=JsConfig)

    @property
    def has_frontend(self) -> bool:
        """Return True if a frontend entrypoint is configured."""
        return bool(self.entry)

    @classmethod
    def load(cls, pyproject_path: Path | str) -> "BuildConfig":
        """Load build configuration from a project directory or *pyproject.toml* file."""
        pyproject_path = Path(pyproject_path)
        if pyproject_path.is_dir():
            pyproject_path = pyproject_path / "pyproject.toml"

        if not pyproject_path.is_file():
            return cls()

        with open(pyproject_path, "rb") as fh:
            data: dict[str, Any] = tomllib.load(fh)

        tool_section = data.get("tool", {})
        if not isinstance(tool_section, dict):
            return cls()

        vertebra_cfg = tool_section.get("vertebra") or tool_section.get("vtb") or {}
        if not isinstance(vertebra_cfg, dict) or not vertebra_cfg:
            return cls()

        entry: str = vertebra_cfg.get("entry", "")
        outdir: str = vertebra_cfg.get("outdir", "dist")
        bundler: str = vertebra_cfg.get("bundler", "native")
        dev_port_raw = vertebra_cfg.get("dev-port") or vertebra_cfg.get("dev_port")
        dev_port: int | None = int(dev_port_raw) if dev_port_raw is not None else None
        codegen: bool = bool(vertebra_cfg.get("codegen", False))
        codegen_output: str = str(
            vertebra_cfg.get("codegen-output") or vertebra_cfg.get("codegen_output") or ".vertebra/generated"
        )
        app_path: str = vertebra_cfg.get("app", "")

        js_section: dict[str, Any] = vertebra_cfg.get("js", {})
        dependencies = tuple(js_section.get("dependencies", ()))
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
