---
type: Spec
parent_flow: mcp-protocol-completeness
chapter_id: ch09-stdio-and-serve
title: "Chapter 9: Stdio and Serve"
description: Make the stdio server read incrementally with concurrent requests, cancellation, legacy initialize sessions, notification relay, and progress over stdout, and make vtb ai mcp serve serve the discovered application's MCP surface or fail loud.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch08-litestar-delegation-parity
replan_trigger: null
current_task: null
plan_revision: 1
plan_commit: null
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-12T00:30:00Z"
updated_at: "2026-11-12T00:30:00Z"
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:30:00Z"
verified: []
sources:
  - id: stdio
    resource: src/py/vertebra/agent/mcp/stdio.py::run_stdio_server
    title: Stdio runner that reads all stdin before dispatching
  - id: handle-payload
    resource: src/py/vertebra/agent/mcp/service.py::MCPCore.handle_payload
    title: Header-free dispatch path
  - id: cli-serve
    resource: src/py/vertebra/cli/ai.py::serve
    title: vtb ai mcp serve building an empty MCPService
  - id: vertebra-env
    resource: src/py/vertebra/cli/plugin_registry.py::VertebraEnv
    title: Application discovery for CLI commands
tags:
  - mcp
  - stdio
  - cli
---

# Chapter 9: Stdio and Serve

<!-- truth: start -->
- **Incremental stdio**: `run_stdio_server` reads stdin line by line (`anyio.wrap_file(sys.stdin.buffer)` by default), dispatches each request in its own task, serializes stdout writes with an `anyio.Lock`, honours `notifications/cancelled {"requestId": ...}` by cancelling the in-flight request without writing a response, and waits for in-flight requests at EOF [^stdio].
- **Stdio protocol**: `MCPCore.handle_payload` applies the header-free negotiation rule (`_meta` version or bound session); a legacy `initialize` over stdio binds the created session to the stdio context; server notifications (list changes, resource updates, log messages) are relayed to stdout from the session stream, or from `anon-<stdio session id>` for modern clients; progress tokens write `notifications/progress` lines directly [^handle-payload].
- **Serve**: `vtb ai mcp serve [--app module:attr]` resolves the application through `VertebraEnv`, finds its MCP surface with `resolve_app_mcp_core` (an `MCPCore`, `AsgiMcpApp` / `WsgiMcpApp`, a registry carrying `_mcp_app`, `AgentPlugin` in `app.plugins`, `app.state.mcp_core`, or Flask `app.extensions["vertebra_mcp_app"]`), delegates Litestar apps with `LitestarMCP` to `litestar_mcp.mcp.stdio.run_stdio`, and otherwise raises `click.ClickException` (exit code `1`); the unused `--name` option is removed [^cli-serve] [^vertebra-env].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`vtb ai mcp serve` constructs `MCPService()` with an empty registry and discards `--name`, so it never serves the project's tools ([#114](https://github.com/cofin/vertebra/issues/114)). The default stdio path collects every stdin line before dispatching, so interactive hosts receive no responses until they close stdin. Stdio has no session handling, notifications, cancellation, or progress.

### 1.2 Upstream Execution Baseline

- Depends on `ch03`–`ch05` (sessions, streams, subscriptions) and `ch08` (Litestar delegation through `litestar-mcp>=0.20.0`).

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined | Seam |
| --- | --- | --- |
| Stdio | `src/py/vertebra/agent/mcp/stdio.py::run_stdio_server` [^stdio] | Pipe I/O; tests inject async iterables and collectors. |
| Core | `src/py/vertebra/agent/mcp/service.py::MCPCore.handle_payload` [^handle-payload] | In-process. |
| CLI | `src/py/vertebra/cli/ai.py::serve`, `mcp` group [^cli-serve] | Click; tests use `CliRunner`. |
| Discovery | `src/py/vertebra/cli/plugin_registry.py::VertebraEnv.from_env` [^vertebra-env] | Application import. |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [`vtb` CLI](../../../../knowledge/domains/framework/cli.md), [Agent harness](../../../../knowledge/domains/agent/mcp-and-a2a-protocol-harness.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R15.1**: Incremental, concurrent, cancellable stdio with sessions, notification relay, and progress.
- **R15.2**: `vtb ai mcp serve` serves the discovered app's MCP surface or fails loud.

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```python
# stdio.py
async def run_stdio_server(service: MCPCore, *, stdin: AsyncIterable[bytes] | None = None, stdout: Callable[[bytes], Awaitable[None]] | None = None, max_message_bytes: int = 4_194_304, context: AgentContext | None = None) -> int: ...

# cli/ai.py
def resolve_app_mcp_core(target: Any) -> MCPCore | None: ...
def is_litestar_mcp_app(target: Any) -> bool: ...
@mcp.command()
@click.option("--app", "app_path", default=None)
@click.pass_context
def serve(ctx: click.Context, app_path: str | None) -> None: ...
```

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `vtb ai mcp serve` -> `VertebraEnv.from_env(app_path)` -> `resolve_app_mcp_core(env.app)` -> `anyio.run(run_stdio_server, core)`; Litestar -> `litestar_mcp.mcp.stdio.run_stdio(env.app)`.
- `run_stdio_server` -> per-request `AgentContext.fork()` -> `MCPCore.handle_payload` -> stdout writer; session relay via `SseStreamBroker.subscribe`.

### 2.4 Edge-Case & Failure-Mode Matrix

| Scenario | Trigger | Expected Behavior | Invariant |
| --- | --- | --- | --- |
| Interactive host | One line written, stdin left open | Response written before EOF | Incremental I/O |
| Cancellation | `notifications/cancelled {"requestId": 5}` while request `5` runs | No response for `5` | Spec cancellation |
| Oversized line | Line longer than `max_message_bytes` | Error frame `-32600` | Bounded input |
| EOF with session | stdin closes after `initialize` | Session deleted, relay stopped, exit `0` | Clean shutdown |
| No MCP surface | App without MCP | `ClickException`, exit `1` | Fail loud |
| Legacy-only in modern stdio | `ping` with `_meta` version `2026-07-28` | Error frame `-32601` | Mode rule |

### 2.5 Acceptance Scenarios

- Given an app with `AgentPlugin` and tool `echo`, when a host runs `vtb ai mcp serve` and writes `tools/list`, then the response lists `echo` while stdin stays open.
- Given a legacy host that sends `initialize` and then subscribes to `config://app`, when the app calls `notify_resource_updated`, then a `notifications/resources/updated` line appears on stdout.

### 2.6 Alternatives Considered

| Decision | Chosen | Rejected & Why |
| --- | --- | --- |
| Concurrency | Task per request with write lock | Sequential handling blocks cancellation and long tools. |
| App resolution | Probe known mount carriers | A registry of serve hooks adds a plugin API for a single command. |
| `--name` | Remove | Keeping an ignored option misleads users; the served name comes from the app's `MCPConfig`. |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/0 files documented in ch09]` — CLI usage and reference regeneration land in `ch11:t02`; CLI OKF in `ch11:t04`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R15.1 | [t01](tasks/t01-incremental-stdio-sessions.md) | `behavior_tdd` | `src/py/tests/unit/cli/test_mcp_cli.py` |
| R15.2 | [t02](tasks/t02-serve-discovered-app.md) | `behavior_tdd` | `src/py/tests/unit/cli/test_mcp_cli.py` |

## 5. Implementation Plan

- [ ] mcp-protocol-completeness:ch09:t01 — Make stdio incremental, concurrent, and session-aware (tasks/t01-incremental-stdio-sessions.md)
- [ ] mcp-protocol-completeness:ch09:t02 — Serve the discovered application's MCP surface (tasks/t02-serve-discovered-app.md)

**Chapter Recovery Checkpoint:** `uv run --no-sync pytest src/py/tests/unit/cli/test_mcp_cli.py src/py/tests/unit/agent/test_mcp_conformance.py -q && uv run --no-sync ruff check src/py/vertebra/agent/mcp src/py/vertebra/cli/ai.py && uv run --no-sync mypy src/py/vertebra/agent/mcp src/py/vertebra/cli/ai.py` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `c8722c76`; `0/2` tasks completed.
- **Downstream Dependents**: `ch11:t02` regenerates `docs/reference/cli.md` after the option change.

[^stdio]: `src/py/vertebra/agent/mcp/stdio.py::run_stdio_server` — Stdio runner that reads all stdin before dispatching.
[^handle-payload]: `src/py/vertebra/agent/mcp/service.py::MCPCore.handle_payload` — Header-free dispatch path.
[^cli-serve]: `src/py/vertebra/cli/ai.py::serve` — `vtb ai mcp serve` building an empty `MCPService`.
[^vertebra-env]: `src/py/vertebra/cli/plugin_registry.py::VertebraEnv` — Application discovery for CLI commands.
