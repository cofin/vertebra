---
type: Spec
parent_flow: mcp-protocol-completeness
chapter_id: ch02-guard-and-confirmation-correctness
title: "Chapter 2: Guard and Confirmation Correctness"
description: Run guards once per tool call with typed JSON-RPC errors and make confirmation tickets pending-only, owner-exact, single-use, expiring, and bounded.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch01-agent-context-propagation
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
  - id: tool
    resource: src/py/vertebra/agent/core/schema.py::Tool.invoke
    title: Tool invocation with inline guard loop
  - id: guards
    resource: src/py/vertebra/agent/core/guards.py::GuardChain.execute
    title: Guard chain execution
  - id: tools-call
    resource: src/py/vertebra/agent/mcp/service.py::MCPCore._handle_tools_call
    title: MCP tools/call handler
  - id: gate
    resource: src/py/vertebra/agent/runtime.py::ConfirmationGate
    title: Confirmation ticket store
  - id: provider-tools
    resource: src/py/vertebra/agent/tools/__init__.py::build_tools
    title: ToolProvider adaptation with caller-bound confirmation
tags:
  - mcp
  - guards
  - confirmation
  - security
---

# Chapter 2: Guard and Confirmation Correctness

<!-- truth: start -->
- **Single-pass guards**: `Tool.authorize(context)` runs the tool's guards through `vertebra.agent.core.guards.authorize`; `Tool.call` executes without guards; `Tool.invoke` composes both. `MCPCore._handle_tools_call` calls `authorize` once before confirmation and task promotion, then `call` [^tool] [^tools-call].
- **Typed errors**: `PermissionDeniedError` maps to JSON-RPC `PERMISSION_DENIED = -32004`; `RateLimitExceededError` and `BudgetExceededError` map to `RATE_LIMITED = -32005`; `data` carries `tool` and `reason` [^tools-call].
- **Guard arity**: two-parameter (Litestar-style) guards called without a connection raise `PermissionDeniedError("Guard requires an HTTP connection")` [^guards].
- **Ticket lifecycle**: `approve` / `reject` act only on `pending`, unexpired tickets; owner-bound tickets require an equal `owner_id`; `consume_call_approval` atomically matches tool, arguments, and owner and moves the ticket to `consumed`; tickets expire after `ttl_seconds=600`; the store holds at most `max_tickets=10_000` live tickets [^gate].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`MCPCore._handle_tools_call` calls `authorize(context, spec)` and then `Tool.invoke`, whose own loop calls each guard with the raw context, so guards run twice per call (a `TokenBucketGuard` consumes two tokens) and the second pass can receive `None`. Guard exceptions escape the handler and become `-32603 Internal error`. `GuardChain.execute` calls two-parameter guards with one argument when no connection is supplied, raising `TypeError`.

`ConfirmationGate.approve(token, owner_id=None)` decides owner-bound tickets because the owner check short-circuits on `None`; rejected tickets can be re-approved; approved tickets never expire; `is_call_approved` lets one approval authorize unlimited repeats; the ticket map grows without bound.

### 1.2 Upstream Execution Baseline

- Depends on `ch01-agent-context-propagation` (bound contexts and owner keys).

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined | Seam |
| --- | --- | --- |
| Tool | `src/py/vertebra/agent/core/schema.py::Tool.invoke` [^tool] | In-process. |
| Guards | `src/py/vertebra/agent/core/guards.py::GuardChain.execute`, `authorize` [^guards] | In-process. |
| MCP | `src/py/vertebra/agent/mcp/service.py::MCPCore._handle_tools_call`, `_require_confirmation` [^tools-call] | In-process. |
| Gate | `src/py/vertebra/agent/runtime.py::ConfirmationGate`, `AgentRuntime.run_turn`, `AgentRuntime.stream_turn` [^gate] | In-process; clock injected. |
| Providers | `src/py/vertebra/agent/tools/__init__.py::build_tools` (`invoke` closure) [^provider-tools] | In-process. |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Agent harness](../../../../knowledge/domains/agent/mcp-and-a2a-protocol-harness.md) (confirmation and provider bridge sections).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R4.1**: Guards run once per `tools/call`, before confirmation and before task promotion.
- **R4.2**: Guard denials and rate limits map to `-32004` / `-32005`.
- **R4.3**: Two-parameter guards fail closed without a connection.
- **R5.1**: Decisions only from `pending`, unexpired tickets; exact owner match for owner-bound tickets.
- **R5.2**: Approvals are consumed once; tickets expire; the store is bounded.

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```python
# schema.py
class Tool:
    async def authorize(self, context: AgentContext | None = None) -> None: ...
    async def call(self, arguments: dict[str, Any], context: Any = None) -> Any: ...
    async def invoke(self, arguments: dict[str, Any], context: Any = None) -> Any: ...

# service.py (beside CONFIRMATION_REQUIRED = -32003)
PERMISSION_DENIED: int = -32004
RATE_LIMITED: int = -32005

# runtime.py
class ConfirmationTicket:
    status: Literal["pending", "approved", "rejected", "consumed", "expired"] = "pending"
    expires_at_unix_ms: int = 0
class ConfirmationCapacityError(VertebraError, RuntimeError): ...
class ConfirmationGate:
    def __init__(self, *, ttl_seconds: float = 600.0, max_tickets: int = 10_000, clock: Callable[[], float] = time.time) -> None: ...
    def request_confirmation(self, tool_name: str, call_id: str, arguments: dict[str, Any], *, owner_id: str | None = None, reason: str = "") -> ConfirmationTicket: ...
    def approve(self, token: str, *, owner_id: str | None = None) -> bool: ...
    def reject(self, token: str, *, owner_id: str | None = None, reason: str = "") -> bool: ...
    def is_approved(self, token: str | None, *, owner_id: str | None = None) -> bool: ...
    def consume_call_approval(self, token: str | None, *, tool_name: str, arguments: Mapping[str, Any], owner_id: str | None) -> bool: ...
    def get_ticket(self, token: str) -> ConfirmationTicket | None: ...
```

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `MCPCore._handle_tools_call` -> `Tool.authorize(ctx)` -> `guards.authorize` -> `GuardChain.execute`.
- `MCPCore._require_confirmation` -> `ConfirmationGate.consume_call_approval` -> `request_confirmation` on miss.
- `AgentRuntime.run_turn` / `stream_turn` and `build_tools` `invoke` closure -> `consume_call_approval`.
- `AgentChatController.decide_confirmation` -> `approve` / `reject` (unchanged call site).

### 2.4 Edge-Case & Failure-Mode Matrix

| Scenario | Trigger | Expected Behavior | Invariant |
| --- | --- | --- | --- |
| Double guard run | Token bucket capacity `1`, one call | Call succeeds; bucket empty afterwards | Single pass |
| Anonymous on owned ticket | `approve(token, owner_id=None)` on `owner_id="default:ana"` | `False`, status `pending` | Exact owner |
| Re-approve rejected | `reject` then `approve` | `False`, status `rejected` | Pending-only decisions |
| Replay | Same token twice | Second call blocked with new ticket | Single use |
| Expiry | Clock advanced `601` s | `approve` returns `False`, status `expired` | TTL |
| Capacity | `max_tickets=2`, three pending | Third issue prunes expired/terminal tickets, else raises `ConfirmationCapacityError` | Bounded store |
| Guard denial on task path | Guarded tool with `_meta` task flag | `-32004` synchronously; no task created | Guards before promotion |

### 2.5 Acceptance Scenarios

- Given an approved ticket for `drop_workspace(w1)`, when the call runs once, then a second identical call returns `-32003` with a different token.
- Given a guard requiring role `admin`, when an anonymous caller calls the tool, then the response is `-32004` and the tool body never runs.

### 2.6 Alternatives Considered

| Decision | Chosen | Rejected & Why |
| --- | --- | --- |
| De-duplicate guards | `authorize` / `call` split | Removing guards from `Tool.invoke` breaks `AgentRuntime` callers that rely on them. |
| Replay control | Consume on match | Use counters add policy without a requirement. |
| Error mapping | Dedicated codes | Returning `isError` results hides authorization failures inside tool output. |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/0 files documented in ch02]` — confirmation lifecycle and error codes are documented in `ch11:t01` (`docs/usage/a2a-and-workflows.md`) and `ch11:t03` (harness chapter).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R4.1–R4.3 | [t01](tasks/t01-single-pass-guard-execution.md) | `regression_tdd` | `test_mcp_conformance.py`, `test_core_and_observability.py` |
| R5.1, R5.2 | [t02](tasks/t02-confirmation-ticket-lifecycle.md) | `regression_tdd` | `test_mcp_conformance.py`, `test_runtime_and_workflows.py` |

## 5. Implementation Plan

- [ ] mcp-protocol-completeness:ch02:t01 — Run guards once per call with typed errors (tasks/t01-single-pass-guard-execution.md)
- [ ] mcp-protocol-completeness:ch02:t02 — Make confirmation tickets pending-only, owner-exact, single-use, and expiring (tasks/t02-confirmation-ticket-lifecycle.md)

**Chapter Recovery Checkpoint:** `uv run --no-sync pytest src/py/tests/unit/agent -q && uv run --no-sync ruff check src/py/vertebra/agent && uv run --no-sync mypy src/py/vertebra/agent` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `c8722c76`; `0/2` tasks completed.
- **Downstream Dependents**: `ch05:t01` reuses `PERMISSION_DENIED` for subscription authorizer denials.

[^tool]: `src/py/vertebra/agent/core/schema.py::Tool.invoke` — Tool invocation with inline guard loop.
[^guards]: `src/py/vertebra/agent/core/guards.py::GuardChain.execute` — Guard chain execution.
[^tools-call]: `src/py/vertebra/agent/mcp/service.py::MCPCore._handle_tools_call` — MCP tools/call handler.
[^gate]: `src/py/vertebra/agent/runtime.py::ConfirmationGate` — Confirmation ticket store.
[^provider-tools]: `src/py/vertebra/agent/tools/__init__.py::build_tools` — ToolProvider adaptation with caller-bound confirmation.
