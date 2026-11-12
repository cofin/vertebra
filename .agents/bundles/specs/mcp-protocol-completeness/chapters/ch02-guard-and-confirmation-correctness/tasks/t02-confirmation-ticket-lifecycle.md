---
type: Task
id: "mcp-protocol-completeness:ch02:t02"
chapter_id: ch02-guard-and-confirmation-correctness
title: Make confirmation tickets pending-only, owner-exact, single-use, and expiring
description: Rework ConfirmationGate so decisions require pending unexpired tickets and an exact owner for owner-bound tickets, replace is_call_approved with consume_call_approval, add TTL expiry and a bounded store, and switch every caller.
status: draft
state: pending
priority: P0
verification_strategy: regression_tdd
depends_on:
  - "mcp-protocol-completeness:ch02:t01"
read_set:
  - src/py/vertebra/agent/runtime.py
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/tools/__init__.py
  - src/py/vertebra/agent/controller.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
  - src/py/tests/unit/agent/test_runtime_and_workflows.py
files:
  - src/py/vertebra/agent/runtime.py
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/tools/__init__.py
tests:
  - src/py/tests/unit/agent/test_mcp_conformance.py
  - src/py/tests/unit/agent/test_runtime_and_workflows.py
estimated_loc: 200
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
  - id: gate
    resource: src/py/vertebra/agent/runtime.py::ConfirmationGate
    title: Confirmation ticket store
  - id: require-confirmation
    resource: src/py/vertebra/agent/mcp/service.py::MCPCore._require_confirmation
    title: MCP confirmation check
  - id: provider-tools
    resource: src/py/vertebra/agent/tools/__init__.py::build_tools
    title: Provider tool confirmation closure
tags:
  - confirmation
  - security
---

# Task t02: Make Confirmation Tickets Pending-Only, Owner-Exact, Single-Use, and Expiring

## Objective & Scope Boundary

Close the confirmation gaps in `ConfirmationGate` and switch the three approval-check call sites (`MCPCore._require_confirmation`, `AgentRuntime.run_turn` / `stream_turn`, the `build_tools` closure) to single-use consumption [^gate] [^require-confirmation] [^provider-tools]. Out of scope: the HTTP confirmation endpoint in `AgentChatController` (unchanged; it already calls `approve` / `reject`).

## Bounded Read Manifest

- `src/py/vertebra/agent/runtime.py` — `ConfirmationTicket`, `ConfirmationGate`, `AgentRuntime.run_turn`, `AgentRuntime.stream_turn` approval checks.
- `src/py/vertebra/agent/mcp/service.py::MCPCore._require_confirmation`.
- `src/py/vertebra/agent/tools/__init__.py::build_tools` (`invoke` closure).
- `src/py/vertebra/agent/controller.py::AgentChatController.decide_confirmation`, `_resolve_owner` (read-only, `ANONYMOUS_OWNER`).
- `src/py/tests/unit/agent/test_mcp_conformance.py::test_mcp_tool_call_requires_approved_confirmation_token`.
- `src/py/tests/unit/agent/test_runtime_and_workflows.py::test_confirmation_gate_pause_and_approval`.

## Symbol & Signature Contracts

- `ConfirmationTicket.status: Literal["pending", "approved", "rejected", "consumed", "expired"] = "pending"`; new field `expires_at_unix_ms: int = 0` with a field docstring.
- `class ConfirmationCapacityError(VertebraError, RuntimeError)` in `runtime.py`, exported in `__all__`.
- `ConfirmationGate.__init__(self, *, ttl_seconds: float = 600.0, max_tickets: int = 10_000, clock: Callable[[], float] = time.time) -> None`; `ttl_seconds <= 0` or `max_tickets < 1` raises `ValueError`.
- `request_confirmation(...)`: sets `expires_at_unix_ms = int((clock() + ttl_seconds) * 1000)`; when the store holds `max_tickets` entries, drops tickets whose status is `rejected`, `consumed`, or `expired` or whose expiry passed; raises `ConfirmationCapacityError` when still full.
- Private `_live_ticket(self, token: str) -> ConfirmationTicket | None` (below public methods): returns `None` for unknown tokens; marks and stores `expired` for `pending` / `approved` tickets past expiry and returns the expired ticket.
- Private static `_owner_matches(ticket_owner: str | None, caller_owner: str | None) -> bool`: `ticket_owner is None or ticket_owner == caller_owner`.
- `approve(token, *, owner_id=None) -> bool` and `reject(token, *, owner_id=None, reason="") -> bool`: `True` only when the live ticket is `pending` and `_owner_matches`; otherwise `False` with no state change.
- `is_approved(token, *, owner_id=None) -> bool`: live ticket is `approved` and `_owner_matches`.
- `consume_call_approval(self, token: str | None, *, tool_name: str, arguments: Mapping[str, Any], owner_id: str | None) -> bool`: `True` only when the live ticket is `approved`, `_owner_matches`, `ticket.tool_name == tool_name`, and `arguments_equal(ticket.arguments, dict(arguments))`; on `True` stores the ticket with `status="consumed"`.
- `is_call_approved` is deleted (no alias); `MCPCore._require_confirmation`, both `AgentRuntime` checks, and the `build_tools` closure call `consume_call_approval` with identical keyword arguments.

## Edge Cases & Error Semantics

- `consume_call_approval(None, ...)` returns `False` without raising.
- Non-matching arguments leave an approved ticket `approved` (no consumption).
- Anonymous MCP callers (`owner_id=None`) can still approve and consume unowned tickets through `AgentChatController` (`owner="anonymous"`); the limitation is recorded in `ch11:t03`.
- `arguments_equal` behaviour is unchanged.

## Public-Seam Test Blueprint

- **Files**: `src/py/tests/unit/agent/test_mcp_conformance.py` (extend `test_mcp_tool_call_requires_approved_confirmation_token`; new parametrized `test_confirmation_gate_lifecycle`), `src/py/tests/unit/agent/test_runtime_and_workflows.py` (replay case through `AgentRuntime`).
- Gate under test: `ConfirmationGate(ttl_seconds=600.0, max_tickets=2, clock=lambda: now[0])` with `now = [1_000.0]`.
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Replay blocked | Approve token for `drop_workspace({"workspace_id": "w1"})`; call twice via `service.handle_tool_call` | First returns `"dropped:w1"`; second raises `-32003` with `data.confirmationToken != token`; `calls == ["w1"]` |
| Anonymous on owned ticket | Ticket with `owner_id="default:ana"`; `gate.approve(token, owner_id=None)` | `False`; `get_ticket(token).status == "pending"` |
| Wrong owner | `gate.approve(token, owner_id="default:mallory")` | `False` |
| Re-approve rejected | `gate.reject(token, owner_id="default:ana")` then `gate.approve(token, owner_id="default:ana")` | `True` then `False`; status `"rejected"` |
| Expiry | Issue at `now=1000`; set `now[0] = 1601.0`; `gate.approve(token)` | `False`; status `"expired"` |
| Argument mismatch keeps approval | Approved ticket; `consume_call_approval(token, tool_name="drop_workspace", arguments={"workspace_id": "w2"}, owner_id=None)` | `False`; status stays `"approved"` |
| Capacity prune | `max_tickets=2`; issue two, reject one, issue a third | Third issued; rejected ticket removed (`get_ticket(rejected) is None`) |
| Capacity exhausted | `max_tickets=2`; two pending; issue a third | Raises `ConfirmationCapacityError` |
| Invalid config | `ConfirmationGate(ttl_seconds=0)` | Raises `ValueError` |
| Runtime replay | `AgentRuntime` turn approved with `confirmation_token`; resubmit identical turn with the same token | Second turn `status == "confirmation_required"` |

## Verification

- **Red**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py src/py/tests/unit/agent/test_runtime_and_workflows.py -q -k "confirmation"` fails on replay, anonymous-owner, expiry, and capacity cases.
- **Green**: `uv run --no-sync pytest src/py/tests/unit -q -k "confirmation or provider" && uv run --no-sync ruff check src/py/vertebra/agent && uv run --no-sync mypy src/py/vertebra/agent/runtime.py src/py/vertebra/agent/mcp/service.py src/py/vertebra/agent/tools/__init__.py`.

## Acceptance Criteria

- [ ] R5.1: decisions require `pending`, unexpired tickets and exact owners for owned tickets.
- [ ] R5.2: approvals are single-use, tickets expire, the store is bounded.
- [ ] `is_call_approved` has no remaining references in `src/py`.

## Notes & Discoveries

- [2026-11-12 00:30] Tests in `test_gcp_adapters_and_sqlalchemy.py`, `test_bigquery.py`, `test_oracle_apex.py`, and `test_mcp_enterprise_tools.py` construct `ConfirmationGate()` with no arguments; keyword-only defaults keep them valid.

[^gate]: `src/py/vertebra/agent/runtime.py::ConfirmationGate` — Confirmation ticket store.
[^require-confirmation]: `src/py/vertebra/agent/mcp/service.py::MCPCore._require_confirmation` — MCP confirmation check.
[^provider-tools]: `src/py/vertebra/agent/tools/__init__.py::build_tools` — Provider tool confirmation closure.
