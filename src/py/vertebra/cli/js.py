"""JS/TS dependency management and frontend tooling for Vertebra.

Provides CLI commands for managing JavaScript dependencies in
pyproject.toml [tool.vtb.js] and running frontend build tools.
"""

import logging
import shutil
import subprocess
import tomllib
from pathlib import Path
from typing import Any

import click

logger = logging.getLogger("vertebra.cli.js")

__all__ = ["js"]


@click.group()
def js() -> None:
    """JS/TS dependency management and frontend tooling."""


# ---------------------------------------------------------------------------
# Package name parsing
# ---------------------------------------------------------------------------

_MIN_SCOPED_PKG_PARTS = 2


def _parse_package_name(spec: str) -> str:
    """Extract package name from spec like 'react@18' or '@scope/pkg@1.0'."""
    if spec.startswith("@"):
        parts = spec.split("@")
        if len(parts) >= _MIN_SCOPED_PKG_PARTS:
            return f"@{parts[1]}"
    if "@" in spec:
        return spec.rsplit("@", 1)[0]
    return spec


# ---------------------------------------------------------------------------
# pyproject.toml helpers
# ---------------------------------------------------------------------------


def _read_pyproject(project_dir: Path) -> tuple[str, dict[str, Any]]:
    """Read pyproject.toml, returning (raw_text, parsed_data)."""
    pyproject = project_dir / "pyproject.toml"
    if not pyproject.exists():
        return "", {}
    content = pyproject.read_text(encoding="utf-8")
    data = tomllib.loads(content)
    return content, data


def _get_js_section(data: dict[str, Any]) -> dict[str, Any]:
    """Get the [tool.vtb.js] section, creating if needed."""
    tool = data.setdefault("tool", {})
    vtb = tool.setdefault("vtb", {})
    return vtb.setdefault("js", {})


def _write_pyproject_js(project_dir: Path, js_data: dict[str, Any]) -> None:
    """Write updated JS section back to pyproject.toml.

    Uses tomlkit for formatting preservation if available, otherwise
    falls back to manual text manipulation.
    """
    pyproject = project_dir / "pyproject.toml"

    try:
        import tomlkit

        doc = tomlkit.parse(pyproject.read_text(encoding="utf-8")) if pyproject.exists() else tomlkit.document()

        if "tool" not in doc:
            doc["tool"] = tomlkit.table()
        tool_section = doc["tool"]  # type: ignore[index]
        if "vtb" not in tool_section:
            tool_section["vtb"] = tomlkit.table()  # type: ignore[index]
        vtb_section = tool_section["vtb"]  # type: ignore[index]
        if "js" not in vtb_section:
            vtb_section["js"] = tomlkit.table()  # type: ignore[index]
        js_toml = vtb_section["js"]  # type: ignore[index]

        for key in ("dependencies", "dev-dependencies"):
            if key in js_data and js_data[key]:
                arr = tomlkit.array()
                arr.extend(js_data[key])
                arr.multiline(True)
                js_toml[key] = arr  # type: ignore[index]
            elif key in js_toml:
                del js_toml[key]  # type: ignore[arg-type]

        pyproject.write_text(tomlkit.dumps(doc), encoding="utf-8")
    except ImportError:
        _write_pyproject_js_manual(pyproject, js_data)


def _write_pyproject_js_manual(pyproject: Path, js_data: dict[str, Any]) -> None:
    """Fallback writer using simple text manipulation."""
    lines: list[str] = []
    if pyproject.exists():
        content = pyproject.read_text(encoding="utf-8")
        # Strip any existing [tool.vtb.js] section and rewrite
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

    # Append updated js section
    lines.append("")
    lines.append("[tool.vtb.js]")
    for key in ("dependencies", "dev-dependencies"):
        deps = js_data.get(key, [])
        if deps:
            items = ", ".join(f'"{d}"' for d in deps)
            lines.append(f"{key} = [{items}]")

    pyproject.write_text("\n".join(lines) + "\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# add
# ---------------------------------------------------------------------------


@js.command()
@click.argument("packages", nargs=-1, required=True)
@click.option("--dev", "-D", is_flag=True, help="Add as dev dependency.")
def add(packages: tuple[str, ...], dev: bool) -> None:
    """Add ESM dependencies to [tool.vtb.js] in pyproject.toml.

    PACKAGES are package names, optionally with versions (e.g., react@18).
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
        click.echo(f"Added {pkg}")


# ---------------------------------------------------------------------------
# remove
# ---------------------------------------------------------------------------


@js.command()
@click.argument("packages", nargs=-1, required=True)
def remove(packages: tuple[str, ...]) -> None:
    """Remove ESM dependencies from [tool.vtb.js] in pyproject.toml.

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
        click.echo(f"Removed {pkg}")


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


@js.command(name="list")
def list_deps() -> None:
    """List ESM dependencies from [tool.vtb.js] in pyproject.toml."""
    project_dir = Path.cwd()
    _, data = _read_pyproject(project_dir)
    js_section = _get_js_section(data)

    deps = js_section.get("dependencies", [])
    dev_deps = js_section.get("dev-dependencies", [])

    if deps:
        click.echo("Dependencies:")
        for dep in sorted(deps):
            click.echo(f"  {dep}")

    if dev_deps:
        click.echo("Dev Dependencies:")
        for dep in sorted(dev_deps):
            click.echo(f"  {dep} (dev)")

    if not deps and not dev_deps:
        click.echo("No dependencies found.")


# ---------------------------------------------------------------------------
# install
# ---------------------------------------------------------------------------


@js.command()
def install() -> None:
    """Resolve JS dependencies and generate import map."""
    project_dir = Path.cwd()

    try:
        from vertebra.builder._config import BuildConfig
        from vertebra.builder._import_map import write_import_map

        config = BuildConfig.load(project_dir)
        if config.js.dependencies or config.js.dev_dependencies:
            out = write_import_map(config.js, project_dir / ".vertebra", include_dev=True)
            click.echo(f"Generated import map at {out}")
        else:
            click.echo("No JS dependencies found in pyproject.toml.")
    except Exception as exc:
        click.echo(f"Failed to resolve dependencies: {exc}", err=True)
        raise SystemExit(1) from None


# ---------------------------------------------------------------------------
# build
# ---------------------------------------------------------------------------


@js.command()
def build() -> None:
    """Build frontend assets for production."""
    project_dir = Path.cwd()

    try:
        from vertebra.builder._orchestrator import BuildOrchestrator

        orchestrator = BuildOrchestrator(project_dir)
        built = orchestrator.build()
        if built:
            click.echo("Frontend build completed successfully.")
        else:
            click.echo("No frontend entry point configured. Nothing to build.")
    except Exception as exc:
        click.echo(f"Build failed: {exc}", err=True)
        raise SystemExit(1) from None


# ---------------------------------------------------------------------------
# generate-types
# ---------------------------------------------------------------------------


@js.command(name="generate-types")
@click.option("--output", "-o", type=click.Path(path_type=Path), default=None, help="Output directory.")
def generate_types(output: Path | None) -> None:
    """Generate TypeScript types from Python route handlers."""
    project_dir = Path.cwd()

    try:
        from vertebra.builder._orchestrator import BuildOrchestrator

        orchestrator = BuildOrchestrator(project_dir)
        out = orchestrator.codegen_only(output_dir=output)
        click.echo(f"Types generated in {out}")
    except Exception as exc:
        click.echo(f"Type generation failed: {exc}", err=True)
        raise SystemExit(1) from None


# ---------------------------------------------------------------------------
# exec
# ---------------------------------------------------------------------------


@js.command(name="exec")
@click.argument("cmd", nargs=-1, required=True)
def exec_cmd(cmd: tuple[str, ...]) -> None:
    """Execute a command with npx/bunx.

    CMD is the command and arguments to run.
    """
    runtime = _detect_runtime()
    if runtime is None:
        click.echo("No JavaScript runtime found. Install Node.js, Bun, or Deno.", err=True)
        raise SystemExit(1)

    full_cmd = ["bunx", *cmd] if runtime == "bun" else ["npx", *cmd]

    try:
        result = subprocess.run(full_cmd, cwd=Path.cwd(), check=False)
        raise SystemExit(result.returncode)
    except FileNotFoundError:
        click.echo(f"Command not found: {full_cmd[0]}", err=True)
        raise SystemExit(1) from None


def _detect_runtime() -> str | None:
    """Detect best available JS runtime."""
    for runtime, binary in (("bun", "bun"), ("node", "node"), ("deno", "deno")):
        if shutil.which(binary):
            return runtime
    return None
