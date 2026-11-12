---
type: Task
id: "mcp-protocol-completeness:ch04:t03"
chapter_id: ch04-get-sse-streams
title: Broadcast list changes and level-gated log messages
description: Pass the changed kind to ToolRegistry listeners, broadcast notifications/{tools,resources,prompts}/list_changed from MCPCore to every live session stream, and add MCPCore.log publishing RFC 5424 level-gated notifications/message to the caller's session stream.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "mcp-protocol-completeness:ch04:t01"
read_set:
  - src/py/vertebra/agent/mcp/registry.py
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/core/sse.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
files:
  - src/py/vertebra/agent/mcp/registry.py
  - src/py/vertebra/agent/mcp/service.py
tests:
  - src/py/tests/unit/agent/test_mcp_conformance.py
estimated_loc: 170
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
  - id: registry
    resource: src/py/vertebra/agent/mcp/registry.py::ToolRegistry._notify
    title: Registry mutation notification
  - id: mcp-core
    resource: src/py/vertebra/agent/mcp/service.py::MCPCore
    title: Core notification publishing
tags:
  - notifications
  - logging
---

# Task t03: Broadcast List Changes and Level-Gated Log Messages

## Objective & Scope Boundary

Registry mutations reach connected legacy clients, and tool code can emit protocol log messages [^registry] [^mcp-core]. Out of scope: progress (`t04`), skills list changes (`ch06:t02`), resource updates (`ch05:t02`), and modern `subscriptions/listen` delivery (`ch05:t03`).

## Bounded Read Manifest

- `src/py/vertebra/agent/mcp/registry.py` — `add_listener`, `_notify`, `register_tool`, `register_resource`, `register_resource_template`, `register_prompt`, `register_controller`.
- `src/py/vertebra/agent/mcp/service.py` — `__init__`, `sessions`, `stream_broker`.
- `src/py/vertebra/agent/core/sse.py` — `SseFrame`, `broadcast`, `publish`.
- `src/py/tests/unit/agent/test_mcp_conformance.py`.

## Symbol & Signature Contracts

- `registry.py`: `type RegistryChange = Literal["tools", "resources", "prompts"]` (exported); `add_listener(self, listener: Callable[[RegistryChange], None]) -> None`; `_notify(self, kind: RegistryChange) -> None`; `register_tool` -> `"tools"`, `register_resource` / `register_resource_template` -> `"resources"`, `register_prompt` -> `"prompts"`; `register_controller` emits each kind it changed once.
- `service.py`:
  - `MCPCore.__init__` calls `self.registry.add_listener(self._on_registry_change)`.
  - Private `_on_registry_change(self, kind: RegistryChange) -> None` broadcasts `SseFrame(data=<json>)` with `{"jsonrpc": "2.0", "method": f"notifications/{kind}/list_changed"}` through `self.stream_broker.broadcast(frame)`; returns without publishing when the matching `*_enabled` toggle is off.
  - Module constant `LOG_LEVEL_ORDER: tuple[McpLogLevel, ...] = ("debug", "info", "notice", "warning", "error", "critical", "alert", "emergency")`.
  - `async def log(self, level: McpLogLevel, data: Any, *, logger: str | None = None) -> int`: returns `0` when `config.logging` is false or the bound context has no `"mcp.session"`; compares `LOG_LEVEL_ORDER.index(level)` with the session's current level (re-read through `self.sessions.get`); publishes `{"jsonrpc": "2.0", "method": "notifications/message", "params": {"level": level, "logger": logger, "data": data}}` (omitting `logger` when `None`) to the session stream; returns the number of streams reached (`1` or `0`). Unknown `level` raises `ValueError`.
  - Private `_publish_to_session(self, session_id: str, payload: dict[str, Any]) -> int` shared by `log`.

## Edge Cases & Error Semantics

- Broadcasts with no open streams return quietly (buffers still record frames for replay).
- `log` from a modern request without a session returns `0`; `t04` adds the request-scoped sink.
- Registry listeners run synchronously; listener exceptions propagate (fail loud).

## Public-Seam Test Blueprint

- **File**: `src/py/tests/unit/agent/test_mcp_conformance.py` (new parametrized `test_mcp_list_changed_and_log_notifications`).
- Fixture: legacy session `sid` with an open stream from `core.stream_broker.open_stream(sid, owner_id=None)`.
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Tool registered | `core.registry.register_tool(<tool echo>)` | Next stream chunk JSON `{"jsonrpc": "2.0", "method": "notifications/tools/list_changed"}` |
| Resource registered | `register_resource(ResourceEntry(uri="config://app", name="cfg"))` | `notifications/resources/list_changed` |
| Prompt registered | `register_prompt(PromptEntry(name="p"))` | `notifications/prompts/list_changed` |
| Disabled family | `MCPConfig(prompts_enabled=False)`; register prompt | No frame within `0.05` s |
| Log delivered | `MCPConfig(logging=True)`; session level `info`; tool calls `await core.log("warning", {"msg": "x"}, logger="db")` | Stream chunk params `{"level": "warning", "logger": "db", "data": {"msg": "x"}}`; `log` returns `1` |
| Log filtered | Session level `error`; `log("warning", "x")` | Returns `0`; no frame |
| Logging disabled | Default config; `log("error", "x")` | Returns `0` |
| Bad level | `log("loud", "x")` | `ValueError` |

## Verification

- **Red**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q -k "list_changed_and_log"` fails.
- **Green**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q && uv run --no-sync ruff check src/py/vertebra/agent/mcp && uv run --no-sync mypy src/py/vertebra/agent/mcp/registry.py src/py/vertebra/agent/mcp/service.py`.

## Acceptance Criteria

- [ ] R10.1: list-changed broadcasts and level-gated log messages match the matrix.
- [ ] `add_listener` callbacks receive the changed kind.

## Notes & Discoveries

- [2026-11-12 00:30] `ToolRegistry.add_listener` has no callers at `c8722c76`, so the signature change affects no existing code.

[^registry]: `src/py/vertebra/agent/mcp/registry.py::ToolRegistry._notify` — Registry mutation notification.
[^mcp-core]: `src/py/vertebra/agent/mcp/service.py::MCPCore` — Core notification publishing.
