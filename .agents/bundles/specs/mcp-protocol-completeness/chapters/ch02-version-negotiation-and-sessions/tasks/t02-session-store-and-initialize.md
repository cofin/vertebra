---
type: Task
id: "mcp-protocol-completeness:ch02:t02"
flow_id: mcp-protocol-completeness
chapter_id: ch02-version-negotiation-and-sessions
title: Create Owner-Scoped MCPSessionStore and Legacy initialize Handshake
description: "Implement MCPSession, SessionLimitExceededError, and MCPSessionStore in sessions.py and wire legacy initialize, ping, and notifications/initialized."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "mcp-protocol-completeness:ch02:t01"
files:
  - src/py/vertebra/agent/mcp/sessions.py
  - src/py/vertebra/agent/mcp/models.py
  - src/py/vertebra/agent/mcp/service.py
tests:
  - src/py/tests/unit/agent/test_mcp_conformance.py
read_set:
  - src/py/vertebra/agent/mcp/models.py
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/mcp/tasks.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
estimated_loc: 230
tags:
  - mcp
  - sessions
  - protocol
  - agent
sources:
  - id: mcp-service
    resource: "src/py/vertebra/agent/mcp/service.py::MCPCore"
    title: MCPCore initialization and method registration
  - id: task-store
    resource: "src/py/vertebra/agent/mcp/tasks.py::MCPTaskStore"
    title: Owner-key scoping precedent
---

# Task `t02`: Create Owner-Scoped MCPSessionStore and Legacy initialize Handshake

## Objective & Scope Boundary

Create `src/py/vertebra/agent/mcp/sessions.py` (`MCPSessionStore`) and wire legacy `initialize`, `ping`, and `notifications/initialized` ([#109](https://github.com/cofin/vertebra/issues/109)) per [`../spec.md` §2](../spec.md) [^mcp-service] [^task-store].

## Bounded Read Manifest

- `src/py/vertebra/agent/mcp/{models,service,tasks}.py` — config fields, handler table, and owner key rules.

## Contracts & Edge Cases

- `MCPSession(msgspec.Struct, frozen=True, gc=False, kw_only=True)` stores `session_id`, `owner_id`, `protocol_version`, `client_capabilities`, `client_info`, `log_level`, `subscriptions`, `created_at_unix_ms`, and `expires_at_unix_ms`.
- `MCPSessionStore(ttl_seconds=3600, max_sessions=10_000, clock=time.monotonic)` prunes expired entries, enforces exact owner matching (anonymous cannot read owned sessions), maintains reverse subscription index hooks, and raises `SessionLimitExceededError` -> HTTP `503` / `-32000` when full.
- Legacy `initialize` negotiates requested legacy version (falling back to `2025-11-25` when client requests `2026-07-28` bare), stores client capabilities, and sets response `Mcp-Session-Id`.

## Test Matrix

- Parameterized tests in `test_mcp_conformance.py` covering `initialize` across `2025-11-25`/`2025-06-18`/`2025-03-26`, bare `initialize` requesting `2026-07-28` -> `2025-11-25`, `ping`, `notifications/initialized` (`202`), TTL expiry, and `max_sessions` `503` / `-32000`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/agent/mcp && uv run --no-sync mypy src/py/vertebra/agent/mcp && uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q
```

## Acceptance Criteria

- [ ] `MCPSessionStore` enforces TTL, `max_sessions`, and owner isolation.
- [ ] Legacy `initialize` issues `Mcp-Session-Id` and returns spec-compliant capabilities.

[^mcp-service]: `src/py/vertebra/agent/mcp/service.py::MCPCore` — MCPCore initialization and method registration.
[^task-store]: `src/py/vertebra/agent/mcp/tasks.py::MCPTaskStore` — Owner-key scoping precedent.
