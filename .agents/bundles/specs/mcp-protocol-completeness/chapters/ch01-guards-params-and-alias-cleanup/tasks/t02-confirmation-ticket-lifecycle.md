---
type: Task
id: "mcp-protocol-completeness:ch01:t02"
flow_id: mcp-protocol-completeness
chapter_id: ch01-guards-params-and-alias-cleanup
title: "Enforce Pending-Only, Owner-Exact, Single-Use, Expiring Confirmation Tickets"
description: "Harden ConfirmationGate with ttl_seconds, max_tickets, exact owner matching, and consume_call_approval across MCPCore, AgentRuntime, and ToolProvider bridges."
status: stable
state: completed
commit: 54f39ff4
last_verified_commit: 54f39ff4
last_verified_at: "2026-11-13T01:59:00Z"
verification_evidence: "ruff + mypy + pytest (177 passed) + review-check (exit 0)"
claimed_by: null
claimed_at: null
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "mcp-protocol-completeness:ch01:t01"
files:
  - src/py/vertebra/agent/runtime.py
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/tools/__init__.py
tests:
  - src/py/tests/unit/agent/test_mcp_conformance.py
  - src/py/tests/unit/agent/test_runtime_and_workflows.py
read_set:
  - src/py/vertebra/agent/runtime.py
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/tools/__init__.py
  - src/py/tests/unit/agent/test_runtime_and_workflows.py
estimated_loc: 195
tags:
  - mcp
  - confirmation
  - security
  - agent
sources:
  - id: conf-gate
    resource: "src/py/vertebra/agent/runtime.py::ConfirmationGate"
    title: Confirmation ticket store
  - id: provider-tools
    resource: "src/py/vertebra/agent/tools/__init__.py::build_tools"
    title: ToolProvider confirmation bridge
---

# Task `t02`: Enforce Pending-Only, Owner-Exact, Single-Use, Expiring Confirmation Tickets

## Objective & Scope Boundary

Make `ConfirmationGate` tickets pending-only, exact-owner, single-use, expiring, and bounded per [`../spec.md` §2](../spec.md) [^conf-gate] [^provider-tools].

## Bounded Read Manifest

- `src/py/vertebra/agent/runtime.py` — `ConfirmationTicket`, `ConfirmationGate`, and `AgentRuntime`.
- `src/py/vertebra/agent/mcp/service.py` & `agent/tools/__init__.py` — `_require_confirmation` and `build_tools`.

## Contracts & Edge Cases

- `ConfirmationGate(ttl_seconds=600.0, max_tickets=10_000, clock=time.time)` transitions expired tickets to `'expired'`, rejects `approve`/`reject` unless `status == 'pending'`, requires `ticket.owner_id == owner_id` whenever `ticket.owner_id is not None` (rejecting `owner_id=None`), and prunes terminal/expired entries before raising `ConfirmationCapacityError` at `max_tickets`.
- Replace `is_call_approved` with `consume_call_approval(token, *, tool_name, arguments, owner_id) -> bool`, transitioning matched `'approved'` tickets to `'consumed'` so replay fails with `-32003` and issues a fresh ticket.

## Test Matrix

- Parameterized tests in `test_mcp_conformance.py` and `test_runtime_and_workflows.py` covering replay rejection, `owner_id=None` rejection on owned tickets, `reject` -> `approve` immutability, TTL expiry via injected clock, and `max_tickets` pruning vs `ConfirmationCapacityError`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/agent/runtime.py src/py/vertebra/agent/mcp/service.py src/py/vertebra/agent/tools/__init__.py && uv run --no-sync mypy src/py/vertebra/agent && uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py src/py/tests/unit/agent/test_runtime_and_workflows.py -q
```

## Acceptance Criteria

- [x] Approved confirmation tokens work once and reject replays, wrong owners, and expired tickets.
- [x] `is_call_approved` is removed with zero remaining callers.

[^conf-gate]: `src/py/vertebra/agent/runtime.py::ConfirmationGate` — Confirmation ticket store.
[^provider-tools]: `src/py/vertebra/agent/tools/__init__.py::build_tools` — ToolProvider confirmation bridge.
