---
type: Task
id: "mcp-protocol-completeness:ch02:t01"
chapter_id: ch02-guard-and-confirmation-correctness
title: Run guards once per call with typed errors
description: Split Tool into authorize and call, make two-parameter guards fail closed without a connection, and authorize MCP tool calls once before confirmation and task promotion with -32004 and -32005 error mapping.
status: draft
state: pending
priority: P0
verification_strategy: regression_tdd
depends_on:
  - "mcp-protocol-completeness:ch01:t04"
read_set:
  - src/py/vertebra/agent/core/schema.py
  - src/py/vertebra/agent/core/guards.py
  - src/py/vertebra/agent/mcp/service.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
  - src/py/tests/unit/agent/test_core_and_observability.py
files:
  - src/py/vertebra/agent/core/schema.py
  - src/py/vertebra/agent/core/guards.py
  - src/py/vertebra/agent/mcp/service.py
tests:
  - src/py/tests/unit/agent/test_mcp_conformance.py
  - src/py/tests/unit/agent/test_core_and_observability.py
estimated_loc: 190
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
  - id: tool
    resource: src/py/vertebra/agent/core/schema.py::Tool.invoke
    title: Tool invocation and guard loop
  - id: guards
    resource: src/py/vertebra/agent/core/guards.py::GuardChain.execute
    title: Guard chain arity handling
  - id: tools-call
    resource: src/py/vertebra/agent/mcp/service.py::MCPCore._handle_tools_call
    title: MCP tools/call flow
tags:
  - guards
  - mcp
---

# Task t01: Run Guards Once per Call with Typed Errors

## Objective & Scope Boundary

Guards execute exactly once per MCP `tools/call`, with the bound caller context, before confirmation checks and task promotion, and authorization failures surface as dedicated JSON-RPC errors [^tool] [^guards] [^tools-call]. Out of scope: confirmation ticket semantics (`t02`).

## Bounded Read Manifest

- `src/py/vertebra/agent/core/schema.py` — `Tool.__init__`, `Tool.invoke`, `execute_tool_calls`.
- `src/py/vertebra/agent/core/guards.py` — `PermissionDeniedError`, `RateLimitExceededError`, `BudgetExceededError`, `GuardChain.execute`, `authorize`.
- `src/py/vertebra/agent/mcp/service.py` — `CONFIRMATION_REQUIRED`, `__all__`, `_handle_tools_call`.
- `src/py/tests/unit/agent/test_mcp_conformance.py::test_guarded_skill_controller_and_agent_plugin_preserve_webmcp_and_guards`.
- `src/py/tests/unit/agent/test_core_and_observability.py::test_agent_context_identity_and_guard_chain_inheritance`.

## Symbol & Signature Contracts

- `src/py/vertebra/agent/core/schema.py::Tool`:
  - `async def authorize(self, context: AgentContext | None = None) -> None` calls `vertebra.agent.core.guards.authorize(context if isinstance(context, AgentContext) else None, self.spec)` (module-level import; `guards.py` imports only `vertebra.agent.core.context`, so no cycle).
  - `async def call(self, arguments: dict[str, Any], context: Any = None) -> Any` holds the current argument binding and execution body without any guard loop.
  - `async def invoke(self, arguments: dict[str, Any], context: Any = None) -> Any` is `await self.authorize(context)` followed by `return await self.call(arguments, context)`.
- `src/py/vertebra/agent/core/guards.py::GuardChain.execute`: when a guard target has exactly two parameters and `connection is None`, raise `PermissionDeniedError("Guard requires an HTTP connection")`; otherwise unchanged.
- `src/py/vertebra/agent/mcp/service.py` module constants beside `CONFIRMATION_REQUIRED`: `PERMISSION_DENIED: int = -32004` and `RATE_LIMITED: int = -32005`, both listed in `__all__`.
- `src/py/vertebra/agent/mcp/service.py::MCPCore._handle_tools_call` ordering: resolve tool -> `await target_tool.authorize(ctx)` inside `try` mapping `PermissionDeniedError` to `JSONRPCError(code=PERMISSION_DENIED, message="Permission denied", data={"tool": tool_name, "reason": str(exc)})` and `RateLimitExceededError | BudgetExceededError` to `JSONRPCError(code=RATE_LIMITED, message="Rate limit exceeded", data={"tool": tool_name, "reason": str(exc)})` -> confirmation check -> task promotion or `await target_tool.call(arguments, context=ctx)`. The separate `authorize(context, spec)` call is removed.

## Edge Cases & Error Semantics

- Exceptions raised by `Tool.call` keep the existing `isError: true` result behaviour; only `Tool.authorize` failures map to `-32004` / `-32005`.
- A guarded tool requested as a task is denied synchronously and creates no task record.
- `AgentRuntime` and `execute_tool_calls` keep calling `Tool.invoke`, so their guard behaviour is unchanged.

## Public-Seam Test Blueprint

- **Files**: `src/py/tests/unit/agent/test_mcp_conformance.py` (new parametrized `test_mcp_tool_guards_run_once_and_map_errors`), `src/py/tests/unit/agent/test_core_and_observability.py` (two-parameter guard case).
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Single pass | Guard class counting calls in `calls: list[str]`; `tools/call guarded` as `AgentContext(identity=CallerIdentity(user_id="ana"))` | Success; `calls == ["guard"]` |
| Token bucket | `TokenBucketGuard(rate_per_second=0.0, capacity=1.0)`; first call | Success (one token consumed); second call returns error code `-32005` |
| Role denial | `RequireRoles("admin")`, caller roles `()` | Error code `-32004`, `data.tool == "guarded"`, tool body list stays `[]` |
| Denied task promotion | Same guard, `_meta={"io.modelcontextprotocol/task": True}` | Error `-32004`; `core.task_store.list_for_owner(None)[0] == []` |
| Body error unchanged | Tool raising `ValueError("boom")` | Result `isError is True`, `content[0].text == "Error: boom"` |
| Two-parameter guard | `GuardChain([lambda connection, route_handler: None]).execute(AgentContext())` | Raises `PermissionDeniedError` with message `"Guard requires an HTTP connection"` |

## Verification

- **Red**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py src/py/tests/unit/agent/test_core_and_observability.py -q -k "guards_run_once or guard_chain"` fails.
- **Green**: `uv run --no-sync pytest src/py/tests/unit/agent -q && uv run --no-sync ruff check src/py/vertebra/agent && uv run --no-sync mypy src/py/vertebra/agent/core src/py/vertebra/agent/mcp`.

## Acceptance Criteria

- [ ] R4.1: guards run once, before confirmation and promotion.
- [ ] R4.2: `-32004` / `-32005` mapping with `data.tool` and `data.reason`.
- [ ] R4.3: two-parameter guards fail closed without a connection.

## Notes & Discoveries

- [2026-11-12 00:30] At `c8722c76` the `TokenBucketGuard` consumes two tokens per MCP call because `authorize` and `Tool.invoke` both run guards.

[^tool]: `src/py/vertebra/agent/core/schema.py::Tool.invoke` — Tool invocation and guard loop.
[^guards]: `src/py/vertebra/agent/core/guards.py::GuardChain.execute` — Guard chain arity handling.
[^tools-call]: `src/py/vertebra/agent/mcp/service.py::MCPCore._handle_tools_call` — MCP tools/call flow.
