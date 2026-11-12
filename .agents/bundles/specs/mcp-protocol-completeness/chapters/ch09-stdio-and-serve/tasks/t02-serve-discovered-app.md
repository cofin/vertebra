---
type: Task
id: "mcp-protocol-completeness:ch09:t02"
chapter_id: ch09-stdio-and-serve
title: Serve the discovered application's MCP surface
description: Replace the empty-service vtb ai mcp serve with application discovery through VertebraEnv and resolve_app_mcp_core, delegate Litestar apps with LitestarMCP to litestar-mcp's stdio runner, fail loud when no MCP surface exists, and remove the unused --name option.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "mcp-protocol-completeness:ch09:t01"
read_set:
  - src/py/vertebra/cli/ai.py
  - src/py/vertebra/cli/plugin_registry.py
  - src/py/vertebra/agent/plugin.py
  - src/py/vertebra/extensions/starlette/mcp.py
  - src/py/vertebra/extensions/flask/mcp.py
  - src/py/tests/unit/cli/test_mcp_cli.py
files:
  - src/py/vertebra/cli/ai.py
tests:
  - src/py/tests/unit/cli/test_mcp_cli.py
estimated_loc: 140
plan_revision: 1
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T00:30:00Z"
updated_at: "2026-11-12T00:30:00Z"
commit: null
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:30:00Z"
verified: []
sources:
  - id: cli-serve
    resource: src/py/vertebra/cli/ai.py::serve
    title: Current serve command
  - id: vertebra-env
    resource: src/py/vertebra/cli/plugin_registry.py::VertebraEnv.from_env
    title: Application loader
tags:
  - cli
  - stdio
---

# Task t02: Serve the Discovered Application's MCP Surface

## Objective & Scope Boundary

`vtb ai mcp serve` serves the project's actual tools, resources, prompts, and skills over stdio [^cli-serve] [^vertebra-env]. Out of scope: bridge changes (`ch10`).

## Bounded Read Manifest

- `src/py/vertebra/cli/ai.py` — `mcp` group, `serve`, imports, `__all__`.
- `src/py/vertebra/cli/plugin_registry.py` — `VertebraEnv.from_env`, `VertebraEnv.resolve_app_path`.
- `src/py/vertebra/agent/plugin.py::AgentPlugin` — `mcp_service` attribute.
- `src/py/vertebra/extensions/starlette/mcp.py::mount_mcp_routes` — `state.mcp_core`.
- `src/py/vertebra/extensions/flask/mcp.py::attach_mcp_routes` — `extensions["vertebra_mcp_app"]`.
- `src/py/tests/unit/cli/test_mcp_cli.py`.

## Symbol & Signature Contracts

- `resolve_app_mcp_core(target: Any) -> MCPCore | None` (public, in `__all__`), checked in order: `MCPCore` instance; object with a `core` attribute that is an `MCPCore` (`AsgiMcpApp`, `WsgiMcpApp`); object with `_mcp_app.core` (`MCPRegistry` from Flask/Django mounts); `AgentPlugin` in `getattr(target, "plugins", ())` with non-`None` `mcp_service`; `state.mcp_core` attribute or `state["mcp_core"]` key; Flask `extensions["vertebra_mcp_app"].core`.
- `is_litestar_mcp_app(target: Any) -> bool` (public): `True` when any plugin in `getattr(target, "plugins", ())` has `type(plugin).__name__ == "LitestarMCP"`.
- `serve(ctx: click.Context, app_path: str | None) -> None` with `@click.option("--app", "app_path", default=None, help="Application import path (module:attr); defaults to VERTEBRA_APP, pyproject, or autodiscovery")` and `@click.pass_context`: uses `ctx.find_root().obj` when it is a `VertebraEnv`, else `VertebraEnv.from_env(app_path)`; Litestar apps call `litestar_mcp.mcp.stdio.run_stdio(env.app)` (imported via `ensure_module("litestar_mcp.mcp.stdio", package="litestar-mcp", extra="mcp")`); otherwise `anyio.run(run_stdio_server, core)`; `None` raises `click.ClickException(f"No MCP server found on {env.app_path}; mount AgentPlugin, mount_mcp_routes, attach_mcp_routes, or LitestarMCP")`; non-zero exit codes call `sys.exit(code)`.
- The `--name` option and the unused `MCPService` import are removed.

## Edge Cases & Error Semantics

- `AgentPlugin` with `mcp=None` is treated as no MCP surface.
- `VertebraEnv.from_env` failures propagate their own `click` errors.

## Public-Seam Test Blueprint

- **File**: `src/py/tests/unit/cli/test_mcp_cli.py` (new parametrized `test_resolve_app_mcp_core` and `test_vtb_mcp_serve_command`; `VertebraEnv.from_env` and `run_stdio_server` monkeypatched to capture arguments).
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Direct core | `resolve_app_mcp_core(core)` | `core` |
| Adapter | `resolve_app_mcp_core(AsgiMcpApp(core))` | `core` |
| Registry carrier | Registry returned by `attach_django_mcp([])` | Its `_mcp_app.core` |
| Native plugin | `App(plugins=[AgentPlugin()])` | `plugin.mcp_service` |
| Starlette state | `SimpleNamespace(state=SimpleNamespace(mcp_core=core))` | `core` |
| Flask extensions | `SimpleNamespace(extensions={"vertebra_mcp_app": WsgiMcpApp(core)})` | `core` |
| Nothing | `SimpleNamespace()` | `None` |
| Serve native | `CliRunner().invoke(mcp, ["serve", "--app", "pkg:app"])` with patched env app `App(plugins=[AgentPlugin()])` | Exit `0`; captured core `is plugin.mcp_service` |
| Serve Litestar | Patched env app with a plugin of class name `LitestarMCP`; patched `run_stdio` returning `0` | Exit `0`; `run_stdio` called with the app |
| Serve missing | Patched env app `SimpleNamespace()` | Exit `1`; output contains `"No MCP server found"` |
| Name removed | `CliRunner().invoke(mcp, ["serve", "--name", "x"])` | Exit `2`; output contains `"No such option"` |

## Verification

- **Red**: `uv run --no-sync pytest src/py/tests/unit/cli/test_mcp_cli.py -q -k "resolve_app_mcp_core or mcp_serve"` fails.
- **Green**: `uv run --no-sync pytest src/py/tests/unit/cli -q && uv run --no-sync ruff check src/py/vertebra/cli/ai.py && uv run --no-sync mypy src/py/vertebra/cli/ai.py && PYTHONPATH="src/py:." uv run --no-sync python -m tools.docs.generate_cli_docs --check || true` (the CLI reference regenerates in `ch11:t02`; the `--check` output is recorded as expected drift).

## Acceptance Criteria

- [ ] R15.2: serve resolves every supported mount carrier, delegates Litestar, fails loud otherwise, and drops `--name`.

## Notes & Discoveries

- [2026-11-12 00:30] Django projects have no single app object; pointing `--app` at the registry returned by `attach_mcp_routes` serves them.

[^cli-serve]: `src/py/vertebra/cli/ai.py::serve` — Current serve command.
[^vertebra-env]: `src/py/vertebra/cli/plugin_registry.py::VertebraEnv.from_env` — Application loader.
