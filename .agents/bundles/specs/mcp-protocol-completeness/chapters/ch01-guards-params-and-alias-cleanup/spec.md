---
type: Spec
flow_id: mcp-protocol-completeness
chapter_id: ch01-guards-params-and-alias-cleanup
title: "Chapter 01: Guards, Mcp-Param Validation, Confirmation Lifecycle, and Alias Cleanup"
description: "Execute tool guards once with typed JSON-RPC errors, validate Mcp-Param-* headers, enforce single-use expiring confirmation tickets, and remove the no-op MCPService alias subclass."
status: stable
state: completed
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
depends_on_chapters: []
replan_trigger: null
plan_commit: 18baadcd
last_reviewed_commit: 1f269a58
updated_at: "2026-11-13T02:55:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:code-reviewer
    at: "2026-11-13T02:55:00Z"
    commit: 1f269a58
tags:
  - mcp
  - guards
  - confirmation
  - agent
sources:
  - id: tool-schema
    resource: "src/py/vertebra/agent/core/schema.py::Tool.invoke"
    title: Single-pass Tool.authorize and Tool.call composition
  - id: guards-py
    resource: "src/py/vertebra/agent/core/guards.py::GuardChain.execute"
    title: Fail-closed 2-arg connection guard and GuardChain execution
  - id: mcp-service
    resource: "src/py/vertebra/agent/mcp/service.py::MCPCore"
    title: MCPCore HTTP/tools handler and Mcp-Param-* validation
  - id: conf-gate
    resource: "src/py/vertebra/agent/runtime.py::ConfirmationGate"
    title: Expiring single-use ConfirmationGate ticket store
  - id: a2a-bridge
    resource: "src/py/vertebra/agent/bridges/a2a.py::A2aAgentBridge"
    title: A2A bridge bound directly to MCPCore
---

# Chapter 01: Guards, Mcp-Param Validation, Confirmation Lifecycle, and Alias Cleanup

<!-- truth: start -->
- `Tool.authorize(context)` runs guards once before confirmation and task promotion (`PERMISSION_DENIED = -32004`, `RATE_LIMITED = -32005`), `Tool.call` executes without guards, `Tool.invoke` composes both, and 2-arg connection guards fail closed without an HTTP connection [^tool-schema] [^guards-py].
- `MCPCore.handle` decodes every `Mcp-Param-<name>` header via `decode_mcp_header_value` and rejects missing or mismatched `params` / `params['arguments']` entries with HTTP `400` and `HEADER_MISMATCH = -32020` [^mcp-service].
- `ConfirmationGate` (`ttl_seconds=600.0`, `max_tickets=10_000`) approves/rejects only `pending` unexpired tickets with exact owner match and consumes approvals atomically via `consume_call_approval` [^conf-gate].
- The empty `MCPService(MCPCore)` alias subclass is deleted across `agent/` and `cli/ai.py` in favor of `MCPCore` (`AgentPlugin` also switches internal registry instantiation to `ToolRegistry`) [^mcp-service] [^a2a-bridge].
<!-- truth: end -->

## 1. Specification, Context & Provenance

`MCPCore._handle_tools_call` runs `Tool.authorize(context)` once before confirmation and task promotion (mapping `PermissionDeniedError` to `-32004` and `RateLimitExceededError`/`BudgetExceededError` to `-32005`) and then invokes `Tool.call` without re-running guards [^tool-schema] [^mcp-service]. `MCPCore.handle` validates `Mcp-Method`, `Mcp-Name`, and every `Mcp-Param-*` header (including strict nested scalar-type checks without bool/int coercion). `ConfirmationGate` enforces exact `owner_id` matching, `ttl_seconds` expiry, `max_tickets` capacity, and non-destructive multi-call eligibility checks before single-use `consume_call_approval` [^conf-gate]. All callers across `agent/` and `cli/ai.py` bind directly to `MCPCore` and `ToolRegistry` [^mcp-service] [^a2a-bridge].

## 2. Requirements, Concrete Contracts & Architecture

- **`R1a` (`t01`)**: Add `Tool.authorize(self, context: AgentContext | None = None) -> None` and `Tool.call(self, arguments: dict[str, Any], context: Any = None) -> Any` (`Tool.invoke` = `authorize` + `call`); raise `PermissionDeniedError("Guard requires an HTTP connection")` on 2-arg guards when `connection is None`; map `PermissionDeniedError` -> `-32004` (`PERMISSION_DENIED`) and `RateLimitExceededError` / `BudgetExceededError` -> `-32005` (`RATE_LIMITED`) with `data={"tool": tool_name, "reason": ...}`; validate `Mcp-Param-<key>` headers in `MCPCore.handle` against `params` (and `params["arguments"]` on `tools/call` / `prompts/get`) using RFC 2047 `decode_mcp_header_value`, returning HTTP `400` / `-32020` on mismatch or absence.
- **`R2a` (`t02`)**: Add `status: Literal["pending", "approved", "rejected", "consumed", "expired"]` and `expires_at_unix_ms: int` to `ConfirmationTicket`, `ConfirmationCapacityError(VertebraError, RuntimeError)`, and `ConfirmationGate(ttl_seconds=600.0, max_tickets=10_000, clock=time.time).consume_call_approval(...)` (removing `is_call_approved`); update `MCPCore._require_confirmation`, `AgentRuntime.{run_turn,stream_turn}`, and `agent/tools/__init__.py::build_tools`.
- **`R2b` (`t03`, `t04`)**: Migrate `agent/bridges/a2a.py`, `agent/plugin.py` (including `ToolRegistry()`), `agent/mcp/stdio.py`, and `cli/ai.py` to `MCPCore`, and delete `MCPService` from `agent/mcp/service.py` and `agent/mcp/__init__.py`.

## 3. Documentation & OKF Knowledge Plan

Guard error codes, `Mcp-Param-*` validation, single-use confirmation tickets, and canonical `MCPCore` naming are documented in `ch07`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Files | Verification |
| --- | --- | --- | --- |
| `R1a` | `t01` | `agent/core/{schema,guards}.py`, `agent/mcp/service.py` | `pytest test_mcp_conformance.py test_core_and_observability.py` |
| `R2a` | `t02` | `agent/runtime.py`, `agent/mcp/service.py`, `agent/tools/__init__.py` | `pytest test_mcp_conformance.py test_runtime_and_workflows.py` |
| `R2b` | `t03`, `t04` | `agent/bridges/a2a.py`, `agent/plugin.py`, `agent/mcp/{stdio,service,__init__}.py`, `cli/ai.py` | `pytest test_a2a_protocol.py test_mcp_conformance.py test_mcp_cli.py` |

## 5. Implementation Plan

- [x] [e22452c5] [t01-guards-and-mcp-param-validation.md](tasks/t01-guards-and-mcp-param-validation.md) — Split `Tool.authorize`/`Tool.call`, map `-32004`/`-32005`, and validate `Mcp-Param-*` headers
- [x] [e22452c5] [t02-confirmation-ticket-lifecycle.md](tasks/t02-confirmation-ticket-lifecycle.md) — Enforce pending-only, exact-owner, single-use, expiring `ConfirmationGate` tickets
- [x] [e22452c5] [t03-mcp-service-caller-cutover.md](tasks/t03-mcp-service-caller-cutover.md) — Migrate `a2a.py`, `plugin.py`, and `stdio.py` from `MCPService` to `MCPCore` and `ToolRegistry`
- [x] [e22452c5] [t04-remove-mcp-service-alias.md](tasks/t04-remove-mcp-service-alias.md) — Remove the `MCPService` alias subclass from `service.py`, `agent/mcp/__init__.py`, and `cli/ai.py`

## 6. Current State & Remaining Plan Snapshot

- **Completed & Reviewed (`1f269a58`)**: All `4` tasks in `ch01-guards-params-and-alias-cleanup` (`179 passed`, `mypy` clean) passed zero-context correctness and quality review. Next chapter: `ch02-version-negotiation-and-sessions`.
- **Fast Chapter Gate (`< 15s`)**: `uv run --no-sync ruff check src/py/vertebra/agent src/py/vertebra/cli/ai.py && uv run --no-sync mypy src/py/vertebra/agent src/py/vertebra/cli/ai.py && uv run --no-sync pytest src/py/tests/unit/agent src/py/tests/unit/cli/test_mcp_cli.py -q`.

[^tool-schema]: `src/py/vertebra/agent/core/schema.py::Tool.invoke` — Single-pass `Tool.authorize` and `Tool.call` composition.
[^guards-py]: `src/py/vertebra/agent/core/guards.py::GuardChain.execute` — Fail-closed 2-arg connection guard and `GuardChain` execution.
[^mcp-service]: `src/py/vertebra/agent/mcp/service.py::MCPCore` — `MCPCore` HTTP/tools handler and `Mcp-Param-*` validation.
[^conf-gate]: `src/py/vertebra/agent/runtime.py::ConfirmationGate` — Expiring single-use `ConfirmationGate` ticket store.
[^a2a-bridge]: `src/py/vertebra/agent/bridges/a2a.py::A2aAgentBridge` — A2A bridge bound directly to `MCPCore`.
