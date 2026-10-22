"""Init command for project scaffolding."""

from pathlib import Path

import click

from vertebra.cli.errors import CLIError
from vertebra.cli.theme import console

__all__ = ("init",)


@click.command()
@click.argument("name")
@click.option("--template", default="default", help="Project template: default, web")
def init(name: str, template: str) -> None:
    """Initialize a new Vertebra project."""
    target_dir = Path(name).resolve()
    if target_dir.exists() and any(target_dir.iterdir()):
        raise CLIError(f"Directory '{name}' already exists and is not empty")

    target_dir.mkdir(parents=True, exist_ok=True)

    tool_lines = [
        "[tool.vertebra]",
        'app = "app:app"',
    ]
    if template == "web":
        tool_lines.append('entry = "src/main.ts"')

    pyproject_content = (
        "[project]\n"
        f'name = "{name}"\n'
        'version = "0.1.0"\n'
        'description = "Vertebra application"\n'
        'dependencies = ["vertebra"]\n\n' + "\n".join(tool_lines) + "\n"
    )
    (target_dir / "pyproject.toml").write_text(pyproject_content, encoding="utf-8")

    app_content = (
        '"""Vertebra application entrypoint."""\n\n'
        "from vertebra import App\n\n"
        "app = App()\n\n\n"
        '@app.get("/health")\n'
        "def health() -> dict[str, str]:\n"
        '    """Health check endpoint."""\n'
        '    return {"status": "ok"}\n'
    )
    (target_dir / "app.py").write_text(app_content, encoding="utf-8")

    if template == "web":
        src_dir = target_dir / "src"
        src_dir.mkdir(parents=True, exist_ok=True)
        main_ts_content = '// Vertebra frontend entrypoint\nconsole.log("Hello from Vertebra!");\n'
        (src_dir / "main.ts").write_text(main_ts_content, encoding="utf-8")

    console.success(f"Project '{name}' initialized successfully with template '{template}'")
