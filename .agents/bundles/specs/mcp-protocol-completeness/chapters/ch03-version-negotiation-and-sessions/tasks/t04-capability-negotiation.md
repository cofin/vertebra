---
type: Task
id: "mcp-protocol-completeness:ch03:t04"
chapter_id: ch03-version-negotiation-and-sessions
title: Build capabilities once and capture client capabilities
description: Add one _server_capabilities builder used by server/discover and initialize, omit disabled capability families and unregister their methods, use spec shapes for tasks, logging, and completions, add register_extension, capture client capabilities into AgentContext metadata, and add legacy logging/setLevel.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "mcp-protocol-completeness:ch03:t03"
read_set:
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/mcp/models.py
  - src/py/vertebra/agent/mcp/sessions.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
files:
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/mcp/models.py
tests:
  - src/py/tests/unit/agent/test_mcp_conformance.py
estimated_loc: 180
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
  - id: mcp-discover
    resource: src/py/vertebra/agent/mcp/service.py::MCPCore._handle_discover
    title: Current capability manifest
  - id: mcp-config
    resource: src/py/vertebra/agent/mcp/models.py::MCPConfig
    title: Capability toggles
tags:
  - mcp
  - capabilities
---

# Task t04: Build Capabilities Once and Capture Client Capabilities

## Objective & Scope Boundary

`server/discover` and `initialize` advertise the same honest capability object, client capabilities become available to tools through the bound context, and legacy clients can set a session log level [^mcp-discover] [^mcp-config]. Out of scope: moving `capabilities.skills` under `extensions` (`ch06:t02`), UI extension (`ch07:t02`), delivering log notifications (`ch04:t03`), and `resources.subscribe` (`ch05:t01` registers the handler that turns it on).

## Bounded Read Manifest

- `src/py/vertebra/agent/mcp/service.py` — `_handle_discover`, `_register_handlers`, `_server_capabilities` from `t02`, `handle`.
- `src/py/vertebra/agent/mcp/models.py` — `MCPConfig`, `McpLogLevel`.
- `src/py/vertebra/agent/mcp/sessions.py::MCPSessionStore.update`.
- `src/py/tests/unit/agent/test_mcp_conformance.py`.

## Symbol & Signature Contracts

- `models.py::MCPConfig`: `logging: bool = False` (field docstring: advertise `capabilities.logging` and accept `logging/setLevel`).
- `service.py`:
  - `MCPCore._extensions: dict[str, Callable[[], dict[str, Any]]]`; `register_extension(self, name: str, payload: Callable[[], dict[str, Any]]) -> None` raises `ValueError` for a duplicate name.
  - Private `_server_capabilities(self) -> dict[str, Any]` returns only enabled keys: `tools: {"listChanged": True}` when `tools_enabled`; `resources: {"listChanged": True, "subscribe": <resource_subscriptions and router.has_method("resources/subscribe")>}` when `resources_enabled`; `prompts: {"listChanged": True}` when `prompts_enabled`; `tasks: {"list": {}, "cancel": {}, "requests": {"tools": {"call": {}}}}` when `tasks_enabled`; `logging: {}` when `config.logging`; `completions: {}`; `extensions: {name: payload()}` when any extension is registered; the existing `skills` key stays until `ch06:t02`.
  - `_register_handlers` registers `tools/*`, `resources/*`, `prompts/*`, and `tasks/*` families only when their toggles are on, and `logging/setLevel` only when `config.logging`.
  - Client capability capture in `handle`: modern requests copy `params._meta["io.modelcontextprotocol/clientCapabilities"]` (when a `Mapping`) into `ctx.metadata["mcp.client_capabilities"]`; legacy requests with a session copy `session.client_capabilities`; `initialize` copies `params["capabilities"]`.
  - `_handle_logging_set_level(self, params: Any = None, context: Any = None) -> dict[str, Any]`: requires `params["level"]` in `McpLogLevel` values else `-32602` `"Invalid log level"`; requires `context.metadata["mcp.session"]` else `-32602` `"logging/setLevel requires an initialized session"`; updates the session via `self.sessions.update(..., log_level=level)`; returns `{}`.

## Edge Cases & Error Semantics

- A failing extension payload callable propagates its exception (fail loud; no silent omission).
- Disabled families return `200` / `-32601` through the `t01` unknown-method rule.
- `logging/setLevel` in modern mode returns HTTP `200` / `-32601` (legacy-only method).

## Public-Seam Test Blueprint

- **File**: `src/py/tests/unit/agent/test_mcp_conformance.py` (new parametrized `test_mcp_capabilities_and_client_capture`).
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Discover equals initialize | Default config; `server/discover` (modern) and `initialize {"protocolVersion": "2025-06-18"}` | `discover.capabilities == initialize.capabilities` |
| Disabled tools | `MCPConfig(tools_enabled=False)`; `server/discover`; `tools/list` modern | `"tools" not in capabilities`; `tools/list` -> `200`, `-32601` |
| Tasks shape | Default | `capabilities["tasks"] == {"list": {}, "cancel": {}, "requests": {"tools": {"call": {}}}}` |
| Logging off | Default | `"logging" not in capabilities` |
| Logging on | `MCPConfig(logging=True)` | `capabilities["logging"] == {}` |
| Extension | `core.register_extension("io.example/x", lambda: {"v": 1})` | `capabilities["extensions"] == {"io.example/x": {"v": 1}}` |
| Duplicate extension | register same name twice | `ValueError` |
| Modern client capture | Tool reads `AgentContext.current().metadata["mcp.client_capabilities"]`; `_meta["io.modelcontextprotocol/clientCapabilities"] = {"elicitation": {}}` | Result text `"{'elicitation': {}}"` (repr) |
| Legacy client capture | `initialize` with `capabilities={"roots": {}}`; tool call with session | Result text `"{'roots': {}}"` |
| setLevel | `MCPConfig(logging=True)`; session; `logging/setLevel {"level": "warning"}` | `result == {}`; `await core.sessions.get(sid, owner)` has `log_level == "warning"` |
| Bad level | `{"level": "loud"}` | `-32602`, `"Invalid log level"` |
| No session | `logging/setLevel` with header `2025-06-18` and no session | `-32602`, `"logging/setLevel requires an initialized session"` |

## Verification

- **Red**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q -k "capabilities_and_client_capture"` fails.
- **Green**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q && uv run --no-sync ruff check src/py/vertebra/agent/mcp && uv run --no-sync mypy src/py/vertebra/agent/mcp`.

## Acceptance Criteria

- [ ] R8.1: shared builder, omitted disabled families, spec shapes, extensions, client capture, and `logging/setLevel`.

## Notes & Discoveries

- [2026-11-12 00:30] At `c8722c76` `server/discover` emits `{}` for disabled families and `tasks: {"listChanged": true}`.

[^mcp-discover]: `src/py/vertebra/agent/mcp/service.py::MCPCore._handle_discover` — Current capability manifest.
[^mcp-config]: `src/py/vertebra/agent/mcp/models.py::MCPConfig` — Capability toggles.
