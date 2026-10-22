"""JS/TS dependency management and frontend tooling for Vertebra.

Provides CLI commands for managing JavaScript dependencies in
pyproject.toml ``[tool.vtb.js]`` and running frontend build tools.
"""

import logging
import shutil
import subprocess
import tomllib
from pathlib import Path
from typing import Any

import click

from vertebra.build import BuildConfig, BuildOrchestrator
from vertebra.build._import_map import ImportMapGenerator
from vertebra.cli.errors import CLIError
from vertebra.cli.theme import console

__all__ = ("js",)

logger = logging.getLogger("vertebra.cli.js")


@click.group()
def js() -> None:
    """JS/TS dependency management and frontend tooling."""


@js.command()
@click.argument("packages", nargs=-1, required=True)
@click.option("--dev", "-D", is_flag=True, help="Add as dev dependency.")
def add(packages: tuple[str, ...], dev: bool) -> None:
    """Add ESM dependencies to ``[tool.vtb.js]`` in pyproject.toml.

    PACKAGES are package names, optionally with versions (e.g., ``react@18``).
    """
    project_dir = Path.cwd()
    _, data = _read_pyproject(project_dir)
    js_section = _get_js_section(data)

    deps_key = "dev-dependencies" if dev else "dependencies"
    existing = list(js_section.get(deps_key, []))

    existing_names = {_parse_package_name(p) for p in existing}
    for pkg in packages:
        pkg_name = _parse_package_name(pkg)
        if pkg_name in existing_names:
            existing = [p for p in existing if _parse_package_name(p) != pkg_name]
        existing.append(pkg)
        existing_names.add(pkg_name)

    js_section[deps_key] = existing
    _write_pyproject_js(project_dir, js_section)

    for pkg in packages:
        console.success(f"Added {pkg}")


@js.command()
@click.argument("packages", nargs=-1, required=True)
def remove(packages: tuple[str, ...]) -> None:
    """Remove ESM dependencies from ``[tool.vtb.js]`` in pyproject.toml.

    PACKAGES are package names to remove.
    """
    project_dir = Path.cwd()
    _, data = _read_pyproject(project_dir)
    js_section = _get_js_section(data)

    pkg_names = {_parse_package_name(p) for p in packages}

    for key in ("dependencies", "dev-dependencies"):
        if key in js_section:
            js_section[key] = [p for p in js_section[key] if _parse_package_name(p) not in pkg_names]

    _write_pyproject_js(project_dir, js_section)

    for pkg in packages:
        console.success(f"Removed {pkg}")


@js.command(name="list")
def list_deps() -> None:
    """List ESM dependencies from ``[tool.vtb.js]`` in pyproject.toml."""
    project_dir = Path.cwd()
    _, data = _read_pyproject(project_dir)
    js_section = _get_js_section(data)

    deps = js_section.get("dependencies", [])
    dev_deps = js_section.get("dev-dependencies", [])

    if deps:
        console.info("Dependencies:")
        for dep in sorted(deps):
            console.step(dep)

    if dev_deps:
        console.info("Dev Dependencies:")
        for dep in sorted(dev_deps):
            console.step(f"{dep} (dev)")

    if not deps and not dev_deps:
        console.info("No dependencies found")


@js.command()
def install() -> None:
    """Resolve JS dependencies and generate import map."""
    project_dir = Path.cwd()

    try:
        config = BuildConfig.load(project_dir)
        if config.js.dependencies or config.js.dev_dependencies:
            with console.status("Resolving JS dependencies…"):
                out = ImportMapGenerator.write(config.js, project_dir / ".vertebra", include_dev=True)
            console.success(f"Generated import map at {out}")
        else:
            console.info("No JS dependencies found in pyproject.toml")
    except Exception as exc:
        raise CLIError(f"Failed to resolve dependencies: {exc}") from None


@js.command()
def build() -> None:
    """Build frontend assets for production."""
    project_dir = Path.cwd()

    try:
        orchestrator = BuildOrchestrator(project_dir)
        with console.status("Building frontend…"):
            built = orchestrator.build()
        if built:
            console.success("Frontend build completed successfully")
        else:
            console.info("No frontend entry point configured — nothing to build")
    except Exception as exc:
        raise CLIError(f"Build failed: {exc}") from None


@js.command(name="generate-types")
@click.option("--output", "-o", type=click.Path(path_type=Path), default=None, help="Output directory.")
def generate_types(output: Path | None) -> None:
    """Generate TypeScript types from Python route handlers."""
    project_dir = Path.cwd()

    try:
        orchestrator = BuildOrchestrator(project_dir)
        with console.status("Generating TypeScript types…"):
            out = orchestrator.codegen_only(output_dir=output)
        console.success(f"Types generated in {out}")
    except Exception as exc:
        raise CLIError(f"Type generation failed: {exc}") from None


@js.command(name="exec")
@click.argument("cmd", nargs=-1, required=True)
def exec_cmd(cmd: tuple[str, ...]) -> None:
    """Execute a command with npx/bunx.

    CMD is the command and arguments to run.
    """
    runtime = _detect_runtime()
    if runtime is None:
        raise CLIError(
            "No JavaScript runtime found",
            hint="Install Node.js, Bun, or Deno.",
        )

    full_cmd = ["bunx", *cmd] if runtime == "bun" else ["npx", *cmd]

    try:
        result = subprocess.run(full_cmd, cwd=Path.cwd(), check=False)
        raise SystemExit(result.returncode)
    except FileNotFoundError:
        raise CLIError(f"Command not found: {full_cmd[0]}") from None


def _parse_package_name(spec: str) -> str:
    """Extract package name from spec like ``react@18`` or ``@scope/pkg@1.0``."""
    return ImportMapGenerator.parse_spec(spec)[0]


def _read_pyproject(project_dir: Path) -> tuple[str, dict[str, Any]]:
    """Read pyproject.toml, returning ``(raw_text, parsed_data)``."""
    pyproject = project_dir / "pyproject.toml"
    if not pyproject.exists():
        return "", {}
    content = pyproject.read_text(encoding="utf-8")
    data = tomllib.loads(content)
    return content, data


def _get_js_section(data: dict[str, Any]) -> dict[str, Any]:
    """Get the ``[tool.vtb.js]`` section, creating it if needed."""
    tool: dict[str, Any] = data.setdefault("tool", {})
    vtb: dict[str, Any] = tool.setdefault("vtb", {})
    js_sec: dict[str, Any] = vtb.setdefault("js", {})
    return js_sec


def _write_pyproject_js(project_dir: Path, js_data: dict[str, Any]) -> None:
    """Write updated ``[tool.vtb.js]`` section back to ``pyproject.toml``."""
    pyproject = project_dir / "pyproject.toml"
    lines: list[str] = []
    if pyproject.exists():
        content = pyproject.read_text(encoding="utf-8")
        in_js_section = False
        for line in content.splitlines():
            stripped = line.strip()
            if stripped == "[tool.vtb.js]":
                in_js_section = True
                continue
            if in_js_section and stripped.startswith("["):
                in_js_section = False
            if not in_js_section:
                lines.append(line)

    lines.append("")
    lines.append("[tool.vtb.js]")
    for key in ("dependencies", "dev-dependencies"):
        deps = js_data.get(key, [])
        if deps:
            items = ", ".join(f'"{d}"' for d in deps)
            lines.append(f"{key} = [{items}]")

    pyproject.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _detect_runtime() -> str | None:
    """Detect best available JS runtime."""
    for runtime, binary in (("bun", "bun"), ("node", "node"), ("deno", "deno")):
        if shutil.which(binary):
            return runtime
    return None
