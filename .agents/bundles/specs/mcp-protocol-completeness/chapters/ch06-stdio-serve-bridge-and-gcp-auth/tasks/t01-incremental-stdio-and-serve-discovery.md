---
type: Task
id: "mcp-protocol-completeness:ch06:t01"
flow_id: mcp-protocol-completeness
chapter_id: ch06-stdio-serve-bridge-and-gcp-auth
title: Make Stdio Incremental/Session-Aware and Discover App MCP Surface in serve
description: "Upgrade run_stdio_server for incremental concurrent frames, cancellation, and session notifications, and wire vtb ai mcp serve to discovered app MCPCore."
status: stable
state: completed
commit: 7607f4d8
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: []
files:
  - src/py/vertebra/agent/mcp/stdio.py
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/cli/ai.py
tests:
  - src/py/tests/unit/cli/test_mcp_cli.py
read_set:
  - src/py/vertebra/agent/mcp/stdio.py
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/cli/ai.py
  - src/py/vertebra/cli/plugin_registry.py
  - src/py/tests/unit/cli/test_mcp_cli.py
estimated_loc: 240
tags:
  - mcp
  - stdio
  - cli
  - agent
sources:
  - id: stdio-py
    resource: "src/py/vertebra/agent/mcp/stdio.py::run_stdio_server"
    title: Stdio runner reading full stdin before dispatch
  - id: cli-serve
    resource: "src/py/vertebra/cli/ai.py::serve"
    title: vtb ai mcp serve CLI command
---

# Task `t01`: Make Stdio Incremental/Session-Aware and Discover App MCP Surface in serve

## Objective & Scope Boundary

Deliver the `stdio` and `vtb ai mcp serve` half of [#114](https://github.com/cofin/vertebra/issues/114) per [`../spec.md` §2](../spec.md) [^stdio-py] [^cli-serve].

## Bounded Read Manifest

- `src/py/vertebra/agent/mcp/{stdio,service}.py` & `src/py/vertebra/cli/{ai,plugin_registry}.py`.

## Contracts & Edge Cases

- `run_stdio_server` reads lines incrementally before EOF, spawns concurrent tasks per request under a stdout write lock, cancels in-flight requests on `notifications/cancelled`, emits `-32600` on lines exceeding `max_message_bytes`, relays legacy session notifications/progress to stdout, and deletes the session at EOF.
- `resolve_app_mcp_core(target)` probes `MCPCore`, `.core`, `AgentPlugin` (`mcp_service`), Starlette/FastAPI/Flask/Django mounted `MCPCore` carriers, and `is_litestar_mcp_app(target)` delegates to `litestar_mcp.mcp.stdio.run_stdio`; `vtb ai mcp serve` removes `--name` and raises `click.ClickException` (exit `1`) when no MCP surface exists.

## Test Matrix

- Parameterized tests in `test_mcp_cli.py` covering pre-EOF response streaming, `notifications/cancelled` suppression, oversized line `-32600`, legacy stdout notification relay + EOF cleanup, discovered `AgentPlugin`/Starlette app serving, and fail-loud exit `1` on non-MCP apps.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/agent/mcp/stdio.py src/py/vertebra/agent/mcp/service.py src/py/vertebra/cli/ai.py && uv run --no-sync mypy src/py/vertebra/agent/mcp src/py/vertebra/cli/ai.py && uv run --no-sync pytest src/py/tests/unit/cli/test_mcp_cli.py -q
```

## Acceptance Criteria

- [x] `run_stdio_server` answers interactive frames while stdin remains open and cancels in-flight requests.
- [x] `vtb ai mcp serve` serves the discovered app's MCP surface and removes `--name`.

[^stdio-py]: `src/py/vertebra/agent/mcp/stdio.py::run_stdio_server` — Stdio runner reading full stdin before dispatch.
[^cli-serve]: `src/py/vertebra/cli/ai.py::serve` — vtb ai mcp serve CLI command.
