---
type: Concept
title: "`vtb` Command-Line Interface"
description: "CLI commands, options, environment preservation, entry-point plugin discovery, Rich TUI/theme utilities, and structured logging for the vtb executable."
status: stable
tags: [domains, framework, cli, developer-experience]
sources:
  - resource: src/py/vertebra/cli/__init__.py
  - resource: src/py/vertebra/cli/banner/renderer.py
  - resource: src/py/vertebra/cli/run.py
  - resource: src/py/vertebra/cli/_launch.py
  - resource: src/py/vertebra/cli/dev.py
  - resource: src/py/vertebra/cli/build.py
  - resource: src/py/vertebra/cli/init.py
  - resource: src/py/vertebra/cli/js.py
  - resource: src/py/vertebra/cli/mcp.py
  - resource: src/py/vertebra/cli/workers.py
  - resource: src/py/vertebra/cli/plugin_registry.py
  - resource: src/py/vertebra/cli/theme.py
  - resource: src/py/vertebra/cli/tui.py
  - resource: src/py/vertebra/cli/_logging.py
  - resource: src/py/vertebra/cli/errors.py
---

# `vtb` Command-Line Interface

[`vertebra.cli`](../../../../../src/py/vertebra/cli/__init__.py) (`[project.scripts] vtb = "vertebra.cli:cli"` in `pyproject.toml`, `@click.version_option(version=__version__, package_name="vertebra-core", prog_name="vertebra")`, and `importlib.metadata.version("vertebra-core")` in [`cli/banner/renderer.py`](../../../../../src/py/vertebra/cli/banner/renderer.py)) is a `rich-click` command group driving production server execution, HMR development, frontend package management, standalone/onefile builds, MCP stdio bridging, background worker lanes, and third-party CLI plugins.

## 1. Command Matrix

| Command | Module | Options & Behavior |
|---|---|---|
| `vtb run [APP]` | [`cli/run.py`](../../../../../src/py/vertebra/cli/run.py), [`cli/_launch.py`](../../../../../src/py/vertebra/cli/_launch.py) | Resolves `APP` (`--app`, `VERTEBRA_APP`), `--host`, `--port`, `--workers`, `--threads`, `--topology auto\|embedded\|shm`, `--interface auto\|asgi\|rsgi\|wsgi` (`--asgi` sets `VERTEBRA_NATIVE=0`), `--jit / --no-jit` (configures `PYTHON_JIT=1` on non-free-threaded Python 3.15+ interpreters; warns and skips on free-threaded builds), `--drain-timeout`, `--shutdown-timeout`, preserves inherited `VERTEBRA_WORKERS`, `VERTEBRA_TOPOLOGY`, `VERTEBRA_THREADS`, and `VERTEBRA_JIT` environment defaults when CLI flags are omitted, and launches `run_supervisor`. |
| `vtb dev [APP]` | [`cli/dev.py`](../../../../../src/py/vertebra/cli/dev.py) | Runs `BuildOrchestrator` in dev/HMR mode (`SourceWatcher` or Vite dev server), enables `--reload`, and launches `run_supervisor` with the interactive Rich TUI. |
| `vtb build` | [`cli/build.py`](../../../../../src/py/vertebra/cli/build.py) | Builds `--target frontend\|backend\|all\|<triple>`, `--standalone`, `--onefile`, `--entrypoint`, `--pgo`, `--profdata`, `--server-binary`, `--python-version` (`3.12`–`3.15` / `3.14t` / `3.15t`), and `--output` via `BuildOrchestrator`, `StandaloneBuilder`, and `OnefileBuilder`. |
| `vtb js <subcmd>` | [`cli/js.py`](../../../../../src/py/vertebra/cli/js.py) | Frontend toolchain subcommands: `add`, `remove`, `list`, `install`, `build`, `generate-types` (`TypeExtractor` + TS/Zod/Valibot/OpenAPI/Protobuf/procedures), and `exec`. |
| `vtb init` | [`cli/init.py`](../../../../../src/py/vertebra/cli/init.py) | Scaffolds a new Vertebra project layout declaring `dependencies = ["vertebra-core"]` in the generated `pyproject.toml`. |
| `vtb mcp bridge` | [`cli/mcp.py`](../../../../../src/py/vertebra/cli/mcp.py) | Bridges newline-delimited JSON-RPC 2.0 over `stdin`/`stdout` to an application's `McpServer` (`execute_stdio_bridge`). |
| `vtb workers run APP` | [`cli/workers.py`](../../../../../src/py/vertebra/cli/workers.py) | Launches admission-controlled [`WorkerRunner`](../server/worker-lanes.md) lanes (`--queues`, `--concurrency`, `--drain-timeout`). |

## 2. Plugin Discovery, TUI, Theme & Logging

- **Entry-Point & App Plugin Discovery ([`plugin_registry.py`](../../../../../src/py/vertebra/cli/plugin_registry.py))**: `discover_entry_point_commands(cli)` loads `click.Command` / `click.Group` plugins registered under the `vertebra.cli_plugins` entry-point group; `register_app_cli_plugins(cli, app)` attaches plugins implementing `on_cli_init(cli)`.
- **Theme, TUI & Logging ([`theme.py`](../../../../../src/py/vertebra/cli/theme.py), [`tui.py`](../../../../../src/py/vertebra/cli/tui.py), [`_logging.py`](../../../../../src/py/vertebra/cli/_logging.py), [`errors.py`](../../../../../src/py/vertebra/cli/errors.py))**: `Console` (`console`) wraps `rich.console.Console` on `stderr` (honoring `NO_COLOR`) alongside `register_startup_block` / `register_shutdown_block`; `run_dev_tui` (`LogBuffer`, `DevLogFormatter`) drives the keyboard-interactive dev console; `JsonFormatter` / `StructuredFormatter` format logs; and `CLIError` / `ConfigResolverError` provide structured error output.
