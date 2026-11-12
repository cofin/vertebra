---
type: Task
id: "mcp-protocol-completeness:ch01:t02"
chapter_id: ch01-agent-context-propagation
title: Bind contexts in MCPCore and isolate promoted tasks
description: Bind the request AgentContext (or an anonymous one) for every MCPCore.handle and handle_payload call, run promoted tasks under a forked context, cancel running tasks on tasks/cancel, and keep terminal task records immutable.
status: draft
state: pending
priority: P0
verification_strategy: regression_tdd
depends_on:
  - "mcp-protocol-completeness:ch01:t01"
read_set:
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/mcp/tasks.py
  - src/py/vertebra/agent/core/context.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
files:
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/mcp/tasks.py
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
  - id: mcp-core
    resource: src/py/vertebra/agent/mcp/service.py::MCPCore
    title: MCP core dispatch, tool calls, and task handlers
  - id: task-store
    resource: src/py/vertebra/agent/mcp/tasks.py::MCPTaskStore
    title: Task record transitions
tags:
  - mcp
  - context
  - tasks
---

# Task t02: Bind Contexts in MCPCore and Isolate Promoted Tasks

## Objective & Scope Boundary

Make `MCPCore` the single place that binds `AgentContext` for request dispatch, give promoted tasks their own forked context and cancellation path, and stop terminal task records from being overwritten [^mcp-core] [^task-store]. Out of scope: guard ordering and confirmation semantics (`ch02`), protocol versions (`ch03`).

## Bounded Read Manifest

- `src/py/vertebra/agent/mcp/service.py` — `MCPCore.handle`, `handle_payload`, `_handle_tools_call`, `_handle_tasks_cancel`, `_caller_owner`, `_background_tasks`.
- `src/py/vertebra/agent/mcp/tasks.py` — `MCPTaskStore.cancel`, `complete`, `fail`, `wait_for_result`, `TaskRecord.is_terminal`.
- `src/py/vertebra/agent/core/context.py` — `bind_agent_context`, `reset_agent_context`, `AgentContext.fork` from `t01`.
- `src/py/tests/unit/agent/test_mcp_conformance.py::test_mcp_tasks_store_and_async_promotion` — Task test patterns.

## Symbol & Signature Contracts

- `MCPCore.handle(self, request: McpHttpRequest) -> McpHttpResponse`: resolves `ctx = request.context if isinstance(request.context, AgentContext) else AgentContext()`, binds it with `token = bind_agent_context(ctx)` before origin checks, dispatches with `self.router.dispatch_payload(request.body, ctx)`, and calls `reset_agent_context(token)` in `finally`.
- `MCPCore.handle_payload(self, raw_bytes: bytes, context: AgentContext | None = None) -> bytes | None`: same binding rule around `self.router.dispatch_payload`.
- `MCPCore._task_runs: dict[str, tuple[asyncio.Task[None], AgentContext]]` replaces the anonymous `_background_tasks` set; entries are removed in the task's done callback.
- `MCPCore._handle_tools_call` task path: `task_ctx = ctx.fork()`; the coroutine calls `bind_agent_context(task_ctx)` first, invokes the tool with `context=task_ctx`, and on `asyncio.CancelledError` re-raises without calling `complete` or `fail`.
- `MCPCore._handle_tasks_cancel`: after `self.task_store.cancel(...)`, when `task_id in self._task_runs`, calls `task_ctx.cancellation_token.cancel(reason or "Task cancelled by client")` and `run.cancel()`.
- `MCPTaskStore.complete(self, task_id: str, result: dict[str, Any]) -> TaskRecord` and `fail(self, task_id: str, error: dict[str, Any]) -> TaskRecord` return the current record unchanged when `current.is_terminal`.

## Edge Cases & Error Semantics

- A handler raising inside `handle` still resets the context variable (`finally`).
- `tasks/cancel` for a task that already finished leaves its record and `_task_runs` untouched.
- `tasks/cancel` from another owner keeps raising `TaskLookupError` -> `-32602` before any cancellation.
- Cancellation never writes `failed`; the record stays `cancelled`.

## Public-Seam Test Blueprint

- **File**: `src/py/tests/unit/agent/test_mcp_conformance.py` (in-process; new `test_mcp_core_binds_agent_context`, parametrized over `handle` and `handle_payload`, and `test_mcp_task_cancel_stops_execution`).
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Bound identity | Tool `whoami() -> str` returning `AgentContext.current().identity.user_id`; `core.handle(McpHttpRequest(body=<tools/call whoami>, headers={"MCP-Protocol-Version": "2026-07-28", "Mcp-Method": "tools/call", "Mcp-Name": "whoami"}, context=AgentContext(identity=CallerIdentity(user_id="ana"))))` | Result `content[0].text == "ana"`; `current_agent_context() is None` after return |
| Anonymous default | Same request with `context=None` | Tool sees a non-`None` context with `auth_scheme == "anonymous"` (returns `"None"` text for `user_id`) |
| Payload path | `core.handle_payload(<tools/call whoami>, context=None)` | Decoded result text `"None"`; no exception |
| Task inherits identity | `tools/call whoami` with `_meta={"io.modelcontextprotocol/task": True}` and context for `ana`; `await core.task_store.wait_for_result(task_id, owner_id="default:ana")` | Task `result.content[0].text == "ana"`, task `owner_id == "default:ana"` |
| Cancel running task | Tool awaits `anyio.Event().wait()`; promote; `tasks/cancel {"taskId": id}` as `ana`; `await asyncio.sleep(0)` twice | `status == "cancelled"`, captured task context `cancellation_token.is_cancelled is True`, background task `cancelled() is True`, status still `"cancelled"` |
| Terminal immutability | `store.cancel(id)` then `store.complete(id, {"content": []})` | Returned record `status == "cancelled"` |

## Verification

- **Red**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q -k "binds_agent_context or task_cancel_stops"` fails.
- **Green**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q && uv run --no-sync ruff check src/py/vertebra/agent/mcp && uv run --no-sync mypy src/py/vertebra/agent/mcp/service.py src/py/vertebra/agent/mcp/tasks.py`.

## Acceptance Criteria

- [ ] R2.1: contexts are bound and restored for `handle` and `handle_payload`.
- [ ] R2.2: promoted tasks use forked contexts, cancellation stops execution, terminal records are immutable.
- [ ] Existing conformance tests stay green.

## Notes & Discoveries

- [2026-11-12 00:30] `MCPTaskStore.complete` at `c8722c76` overwrites `cancelled` records because it never checks `is_terminal`.

[^mcp-core]: `src/py/vertebra/agent/mcp/service.py::MCPCore` — MCP core dispatch, tool calls, and task handlers.
[^task-store]: `src/py/vertebra/agent/mcp/tasks.py::MCPTaskStore` — Task record transitions.
