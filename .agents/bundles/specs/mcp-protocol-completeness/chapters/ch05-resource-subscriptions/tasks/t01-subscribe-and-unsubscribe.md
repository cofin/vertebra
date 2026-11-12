---
type: Task
id: "mcp-protocol-completeness:ch05:t01"
chapter_id: ch05-resource-subscriptions
title: Subscribe and unsubscribe with fail-closed authorization
description: Register legacy resources/subscribe and resources/unsubscribe handlers that require a session, resolve the URI, enforce a fail-closed subscription authorizer and a per-session limit, and store subscriptions on the session.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "mcp-protocol-completeness:ch04:t04"
read_set:
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/mcp/models.py
  - src/py/vertebra/agent/mcp/sessions.py
  - src/py/vertebra/agent/mcp/registry.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
files:
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/mcp/models.py
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
  - id: resources-read
    resource: src/py/vertebra/agent/mcp/service.py::MCPCore._handle_resources_read
    title: Resource resolution order
  - id: session-update
    resource: src/py/vertebra/agent/mcp/sessions.py::MCPSessionStore.update
    title: Session subscription updates
tags:
  - resources
  - subscriptions
---

# Task t01: Subscribe and Unsubscribe with Fail-Closed Authorization

## Objective & Scope Boundary

Legacy clients can subscribe to resources they are allowed to watch [^resources-read] [^session-update]. Out of scope: delivery (`t02`) and modern listen streams (`t03`).

## Bounded Read Manifest

- `src/py/vertebra/agent/mcp/service.py` — `_register_handlers`, `_handle_resources_read`, `_server_capabilities`, `PERMISSION_DENIED`.
- `src/py/vertebra/agent/mcp/models.py::MCPConfig`.
- `src/py/vertebra/agent/mcp/sessions.py::MCPSessionStore.update`.
- `src/py/vertebra/agent/mcp/registry.py` — `get_resource`, `match_resource_template`.
- `src/py/tests/unit/agent/test_mcp_conformance.py`.

## Symbol & Signature Contracts

- `models.py::MCPConfig`: `resource_subscriptions: bool = True`, `max_subscriptions_per_session: int = 256`, `subscription_authorizer: Callable[[AgentContext, str], bool | Awaitable[bool]] | None = None` (field docstrings; `AgentContext` imported under `TYPE_CHECKING`-free module import from `vertebra.agent.core.context`).
- `service.py`:
  - Handlers `resources/subscribe` and `resources/unsubscribe` registered only when `resources_enabled and resource_subscriptions`.
  - Private `_resolve_resource_uri(self, uri: str) -> bool`: `True` for `registry.get_resource(uri)`, `registry.match_resource_template(uri)`, or a `skill://<name>` URI whose skill exists in `self.skill_catalog`.
  - Private `async _authorize_subscription(self, context: AgentContext, uri: str) -> None`: no authorizer -> allow; callable result (awaited when awaitable) `True` -> allow; `False` -> `JSONRPCErrorException(PERMISSION_DENIED, "Permission denied", data={"uri": uri})`; any exception -> logged with `logger.exception` and the same `-32004` error with `data={"uri": uri, "reason": str(exc)}`.
  - `_handle_resources_subscribe(self, params: Any = None, context: Any = None) -> dict[str, Any]`: `uri` must be a non-empty `str` (`-32602` `"Missing required 'uri'"`); session required (`-32602` `"resources/subscribe requires an initialized session"`); unknown URI -> `-32602` `f"Resource not found: {uri}"`; authorization; limit check against `max_subscriptions_per_session` (`-32602` `"Subscription limit reached"`) unless already subscribed; `await self.sessions.update(sid, owner, subscriptions=session.subscriptions | {uri})`; returns `{}`.
  - `_handle_resources_unsubscribe`: same `uri` and session checks; removes the URI if present; returns `{}`.

## Edge Cases & Error Semantics

- Re-subscribing to the same URI is idempotent and does not count against the limit.
- Unsubscribing a URI that was never subscribed returns `{}`.
- `capabilities.resources.subscribe` becomes `True` automatically through `router.has_method("resources/subscribe")` (`ch03:t04`).

## Public-Seam Test Blueprint

- **File**: `src/py/tests/unit/agent/test_mcp_conformance.py` (new parametrized `test_mcp_resource_subscribe`).
- Fixture: `ResourceEntry(uri="config://app", name="cfg")`, `ResourceTemplateEntry(uriTemplate="users://{id}", name="user")`, legacy session `sid` (`2025-06-18`).
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Static URI | `resources/subscribe {"uri": "config://app"}` with session | `result == {}`; session `subscriptions == frozenset({"config://app"})` |
| Template URI | `{"uri": "users://42"}` | `result == {}` |
| Unknown | `{"uri": "config://missing"}` | `-32602`; message `"Resource not found: config://missing"` |
| No session | header `2025-06-18`, no `Mcp-Session-Id` | `-32602`; `"resources/subscribe requires an initialized session"` |
| Authorizer denies | `subscription_authorizer=lambda ctx, uri: False` | `-32004`; `data.uri == "config://app"`; subscriptions empty |
| Authorizer raises | `lambda ctx, uri: (_ for _ in ()).throw(RuntimeError("db down"))` | `-32004`; `data.reason == "db down"` |
| Async authorizer | `async def allow(ctx, uri): return ctx.identity.user_id == "ana"` with context `ana` | `{}` |
| Limit | `max_subscriptions_per_session=1`; subscribe `config://app` then `users://1` | Second `-32602` `"Subscription limit reached"` |
| Unsubscribe | Subscribe then `resources/unsubscribe {"uri": "config://app"}` | `{}`; subscriptions empty |
| Capability | Default config `server/discover` | `capabilities.resources.subscribe is True` |
| Disabled | `MCPConfig(resource_subscriptions=False)` | `subscribe is False`; subscribe -> `200` / `-32601` |

## Verification

- **Red**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q -k "resource_subscribe"` fails.
- **Green**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q && uv run --no-sync ruff check src/py/vertebra/agent/mcp && uv run --no-sync mypy src/py/vertebra/agent/mcp/service.py src/py/vertebra/agent/mcp/models.py`.

## Acceptance Criteria

- [ ] R11.1: validation, fail-closed authorization, limit, and capability flag match the matrix.

## Notes & Discoveries

- [2026-11-12 00:30] `resources/read` reports missing resources as `-32602`; subscribe uses the same code for consistency.

[^resources-read]: `src/py/vertebra/agent/mcp/service.py::MCPCore._handle_resources_read` — Resource resolution order.
[^session-update]: `src/py/vertebra/agent/mcp/sessions.py::MCPSessionStore.update` — Session subscription updates.
