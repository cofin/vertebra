---
type: Concept
title: "`vtb` Command-Line Interface"
description: "CLI commands, options, environment preservation, entry-point plugin discovery, Rich TUI/theme utilities, and structured logging for the vtb executable."
status: stable
tags: [domains, framework, cli, developer-experience]
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-review
    at: "2026-11-06T06:10:00Z"
    commit: 7657e8b7c2c96bdda6821aec70a2b603f438a127
sources:
  - id: src-cli-init-py
    resource: src/py/vertebra/cli/__init__.py
    title: "Python implementation module (src/py/vertebra/cli/__init__.py)"
  - id: src-cli-banner-renderer-py
    resource: src/py/vertebra/cli/banner/renderer.py
    title: "Python implementation module (src/py/vertebra/cli/banner/renderer.py)"
  - id: src-cli-run-py
    resource: src/py/vertebra/cli/run.py
    title: "Python implementation module (src/py/vertebra/cli/run.py)"
  - id: src-cli-launch-py
    resource: src/py/vertebra/cli/_launch.py
    title: "Python implementation module (src/py/vertebra/cli/_launch.py)"
  - id: src-cli-dev-py
    resource: src/py/vertebra/cli/dev.py
    title: "Python implementation module (src/py/vertebra/cli/dev.py)"
  - id: src-cli-build-py
    resource: src/py/vertebra/cli/build.py
    title: "Python implementation module (src/py/vertebra/cli/build.py)"
  - id: src-cli-init-py-2
    resource: src/py/vertebra/cli/init.py
    title: "Python implementation module (src/py/vertebra/cli/init.py)"
  - id: src-cli-js-py
    resource: src/py/vertebra/cli/js.py
    title: "Python implementation module (src/py/vertebra/cli/js.py)"
  - id: src-cli-ai-py
    resource: src/py/vertebra/cli/ai.py
    title: "Python implementation module (src/py/vertebra/cli/ai.py)"
  - id: src-cli-workers-py
    resource: src/py/vertebra/cli/workers.py
    title: "Python implementation module (src/py/vertebra/cli/workers.py)"
  - id: src-cli-plugin-registry-py
    resource: src/py/vertebra/cli/plugin_registry.py
    title: "Python implementation module (src/py/vertebra/cli/plugin_registry.py)"
  - id: src-cli-theme-py
    resource: src/py/vertebra/cli/theme.py
    title: "Python implementation module (src/py/vertebra/cli/theme.py)"
  - id: src-cli-tui-py
    resource: src/py/vertebra/cli/tui.py
    title: "Python implementation module (src/py/vertebra/cli/tui.py)"
  - id: src-cli-logging-py
    resource: src/py/vertebra/cli/_logging.py
    title: "Python implementation module (src/py/vertebra/cli/_logging.py)"
  - id: src-cli-errors-py
    resource: src/py/vertebra/cli/errors.py
    title: "Python implementation module (src/py/vertebra/cli/errors.py)"
---

# `vtb` Command-Line Interface

<!-- truth: start -->
- `vertebra.cli` exposes the `vtb` Click entrypoint (`run`, `dev`, `init`, `js`, `build`, `ai`, `workers`) via `VertebraExtensionGroup`, discovering `vertebra.cli_plugins` entry points and `CLIPluginProtocol.on_cli_init` hooks.
- `ServerLaunchEnv` resolves `[tool.vertebra]` (`[tool.vtb]` fallback) settings, auto-selects `shm` vs `embedded` topology, and propagates `VERTEBRA_*` variables cleanly to child processes.
<!-- truth: end -->

[`vertebra.cli`](../../../../../src/py/vertebra/cli/__init__.py)[^src-cli-init-py] (`[project.scripts] vtb = "vertebra.cli:cli"` in `pyproject.toml`, `@click.version_option(version=__version__, package_name="vertebra-core", prog_name="vertebra")`, and `importlib.metadata.version("vertebra-core")` in [`cli/banner/renderer.py`](../../../../../src/py/vertebra/cli/banner/renderer.py)[^src-cli-banner-renderer-py]) is a `rich-click` command group driving production server execution, HMR development, frontend package management, standalone/onefile builds, MCP stdio bridging, background worker lanes, and third-party CLI plugins.

## 1. Command Matrix

| Command | Module | Options & Behavior |
|---|---|---|
| `vtb run [APP]` | [`cli/run.py`](../../../../../src/py/vertebra/cli/run.py)[^src-cli-run-py], [`cli/_launch.py`](../../../../../src/py/vertebra/cli/_launch.py)[^src-cli-launch-py] | Resolves `APP` (`--app`, `VERTEBRA_APP`), `--host`, `--port`, `--workers`, `--threads`, `--topology auto\|embedded\|shm`, `--interface auto\|asgi\|rsgi\|wsgi` (`--asgi` sets `VERTEBRA_NATIVE=0`), `--jit / --no-jit`, `--with-workers / --no-workers`, `--queues`, `--lane-concurrency` (plus `[tool.vertebra.workers]` in `pyproject.toml`), `--drain-timeout`, `--shutdown-timeout`, preserves inherited `VERTEBRA_WORKERS`, `VERTEBRA_TOPOLOGY`, `VERTEBRA_THREADS`, and `VERTEBRA_JIT` environment defaults when CLI flags are omitted, and launches `run_supervisor`. |
| `vtb dev [APP]` | [`cli/dev.py`](../../../../../src/py/vertebra/cli/dev.py)[^src-cli-dev-py] | Runs `BuildOrchestrator` in dev/HMR mode (`SourceWatcher` or Vite dev server), enables `--reload`, supports `--with-workers / --no-workers`, `--queues`, and `--lane-concurrency`, and launches `run_supervisor` with the interactive Rich TUI. |
| `vtb build` | [`cli/build.py`](../../../../../src/py/vertebra/cli/build.py)[^src-cli-build-py] | Builds `--target frontend\|backend\|all\|<triple>`, `--profile debug\|release`, `--standalone`, `--onefile`, `--server-binary`, `--entrypoint`, `--python-version` (`3.12`–`3.15` / `3.14t` / `3.15t`), `--output`, and `--project-dir` via `BuildOrchestrator`, `StandaloneBuilder`, and `OnefileBuilder`. |
| `vtb js <subcmd>` | [`cli/js.py`](../../../../../src/py/vertebra/cli/js.py)[^src-cli-js-py] | Frontend toolchain subcommands: `add`, `remove`, `list`, `install`, `build`, `generate-types` (`TypeExtractor` + TS/Zod/Valibot/OpenAPI/Protobuf/procedures), and `exec`. |
| `vtb init` | [`cli/init.py`](../../../../../src/py/vertebra/cli/init.py)[^src-cli-init-py-2] | Scaffolds a new Vertebra project layout declaring `dependencies = ["vertebra-core"]` in the generated `pyproject.toml`. |
| `vtb ai models <subcmd>` | [`cli/ai.py`](../../../../../src/py/vertebra/cli/ai.py)[^src-cli-ai-py] | Embedding model lifecycle subcommands: `list` (`--provider`, `--json`), `pull` (`--quant`, `--cache-dir`, `--source`), `stage` (`--to`, `--quant`), and `warm` (`--from`, `--shm`) with SHA-256 manifest verification and `/dev/shm` `1x` RAM warming. |
| `vtb ai mcp <subcmd>` | [`cli/ai.py`](../../../../../src/py/vertebra/cli/ai.py) | Model Context Protocol subcommands: `bridge` (`--url`, `--token`, `--timeout`) and `serve` (`--name`) over newline-delimited JSON-RPC 2.0 `stdin`/`stdout`. |
| `vtb workers run [APP]` | [`cli/workers.py`](../../../../../src/py/vertebra/cli/workers.py)[^src-cli-workers-py] | Launches admission-controlled [`WorkerRunner`](../server/worker-lanes.md) lanes (`--queues`, `--concurrency`, `--drain-timeout`) or executes a single background job via `--oneshot <job> [--payload <json>]`. |

## 2. Pluggable CLI Extension System, TUI, Theme & Logging

- **Entry-Point & App Plugin Discovery ([`plugin_registry.py`](../../../../../src/py/vertebra/cli/plugin_registry.py)[^src-cli-plugin-registry-py])**: `VertebraExtensionGroup` (extending `VertebraGroup` / `RichGroup`) accepts root `--app` and `--app-dir` options, resolves `VertebraEnv` (`resolve_cli_app_path` across positional `APP`, `--app`, `VERTEBRA_APP`, `LITESTAR_APP`, `pyproject.toml` `[tool.vertebra].app`, and `app.py` autodiscovery), automatically attaches `VertebraPlugin` onto unmodified host-framework apps via `ensure_vertebra_integration`, loads entry-point commands via `discover_entry_point_commands(cli)`, and registers application plugins implementing `CLIPluginProtocol.on_cli_init(cli)` (`CLIPlugin`, `register_app_cli_plugins`) while injecting `app`, `env: VertebraEnv`, and `ctx` into plugin command callbacks (`_inject_cli_args`). `enter_app_server_lifespan` enters registered `server_lifespan` context managers across `vertebra.App` and host frameworks.
- **Theme, TUI & Logging ([`theme.py`](../../../../../src/py/vertebra/cli/theme.py)[^src-cli-theme-py], [`tui.py`](../../../../../src/py/vertebra/cli/tui.py)[^src-cli-tui-py], [`_logging.py`](../../../../../src/py/vertebra/cli/_logging.py)[^src-cli-logging-py], [`errors.py`](../../../../../src/py/vertebra/cli/errors.py)[^src-cli-errors-py])**: `Console` (`console`) resolves `sys.stderr` dynamically on each write through `rich.console.Console` (honoring `NO_COLOR` and Click `CliRunner` stream redirection) alongside `register_startup_block` / `register_shutdown_block`; `run_dev_tui` (`LogBuffer`, `DevLogFormatter`) drives the keyboard-interactive dev console; `JsonFormatter` / `StructuredFormatter` format logs; and `CLIError` / `ConfigResolverError` provide structured error output.

[^src-cli-init-py]: `src/py/vertebra/cli/__init__.py` — Python implementation module (src/py/vertebra/cli/__init__.py).
[^src-cli-banner-renderer-py]: `src/py/vertebra/cli/banner/renderer.py` — Python implementation module (src/py/vertebra/cli/banner/renderer.py).
[^src-cli-run-py]: `src/py/vertebra/cli/run.py` — Python implementation module (src/py/vertebra/cli/run.py).
[^src-cli-launch-py]: `src/py/vertebra/cli/_launch.py` — Python implementation module (src/py/vertebra/cli/_launch.py).
[^src-cli-dev-py]: `src/py/vertebra/cli/dev.py` — Python implementation module (src/py/vertebra/cli/dev.py).
[^src-cli-build-py]: `src/py/vertebra/cli/build.py` — Python implementation module (src/py/vertebra/cli/build.py).
[^src-cli-init-py-2]: `src/py/vertebra/cli/init.py` — Python implementation module (src/py/vertebra/cli/init.py).
[^src-cli-js-py]: `src/py/vertebra/cli/js.py` — Python implementation module (src/py/vertebra/cli/js.py).
[^src-cli-ai-py]: `src/py/vertebra/cli/ai.py` — Python implementation module (src/py/vertebra/cli/ai.py).
[^src-cli-workers-py]: `src/py/vertebra/cli/workers.py` — Python implementation module (src/py/vertebra/cli/workers.py).
[^src-cli-plugin-registry-py]: `src/py/vertebra/cli/plugin_registry.py` — Python implementation module (src/py/vertebra/cli/plugin_registry.py).
[^src-cli-theme-py]: `src/py/vertebra/cli/theme.py` — Python implementation module (src/py/vertebra/cli/theme.py).
[^src-cli-tui-py]: `src/py/vertebra/cli/tui.py` — Python implementation module (src/py/vertebra/cli/tui.py).
[^src-cli-logging-py]: `src/py/vertebra/cli/_logging.py` — Python implementation module (src/py/vertebra/cli/_logging.py).
[^src-cli-errors-py]: `src/py/vertebra/cli/errors.py` — Python implementation module (src/py/vertebra/cli/errors.py).
