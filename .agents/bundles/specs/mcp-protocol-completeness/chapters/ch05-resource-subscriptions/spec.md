---
type: Spec
parent_flow: mcp-protocol-completeness
chapter_id: ch05-resource-subscriptions
title: "Chapter 5: Resource Subscriptions"
description: Implement resources/subscribe and resources/unsubscribe behind a fail-closed authorizer, fan resource updates out to subscribed sessions with store-backed cleanup, and add stateless 2026-07-28 subscriptions/listen streams.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch04-get-sse-streams
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
  - id: notify
    resource: src/py/vertebra/agent/mcp/service.py::MCPCore.notify_resource_updated
    title: Resource update fan-out over an unpopulated map
  - id: resources-read
    resource: src/py/vertebra/agent/mcp/service.py::MCPCore._handle_resources_read
    title: Resource resolution (static, template, skill)
  - id: registry
    resource: src/py/vertebra/agent/mcp/registry.py::ToolRegistry.match_resource_template
    title: Template matching
  - id: sessions
    resource: src/py/vertebra/agent/mcp/sessions.py::MCPSessionStore
    title: Session store from ch03
tags:
  - mcp
  - resources
  - subscriptions
---

# Chapter 5: Resource Subscriptions

<!-- truth: start -->
- **Subscribe**: legacy `resources/subscribe {"uri": ...}` requires an initialized session, a resolvable URI (static, template match, or `skill://` skill), a passing `MCPConfig.subscription_authorizer`, and fewer than `max_subscriptions_per_session=256` subscriptions; it stores the URI on the `MCPSession` and returns `{}`; `resources/unsubscribe` is idempotent [^notify] [^resources-read].
- **Fail closed**: an authorizer returning `False` or raising yields `-32004` and stores nothing; `resource_subscriptions=False` leaves the methods unregistered and `capabilities.resources.subscribe == False` [^notify].
- **Fan-out**: `await MCPCore.notify_resource_updated(uri)` publishes `notifications/resources/updated {"uri": uri}` to every live subscribed session and matching `subscriptions/listen` stream and returns the delivery count; `MCPSessionStore` keeps a reverse URI index that drops entries on unsubscribe, `DELETE`, and expiry [^sessions].
- **Stateless listen**: `2026-07-28` `subscriptions/listen` opens an SSE response stream for the requested notification kinds and resource URIs and stamps `params._meta["io.modelcontextprotocol/subscriptionId"]` on every frame [^notify].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`MCPCore.notify_resource_updated` iterates `self._resource_subscriptions`, which no handler ever writes, and `resources/subscribe` returns `-32601` ([#112](https://github.com/cofin/vertebra/issues/112)). This chapter makes subscriptions real for legacy sessions and adds the stateless listen stream for `2026-07-28` clients, reusing the session store (`ch03`) and stream broker (`ch04`).

### 1.2 Upstream Execution Baseline

- Depends on `ch04` (streams, notification publishing) and `ch02` (`PERMISSION_DENIED`).

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined | Seam |
| --- | --- | --- |
| Fan-out | `src/py/vertebra/agent/mcp/service.py::MCPCore.notify_resource_updated`, `_resource_subscriptions` [^notify] | In-process. |
| Resolution | `src/py/vertebra/agent/mcp/service.py::MCPCore._handle_resources_read`, `src/py/vertebra/agent/mcp/registry.py::ToolRegistry.get_resource`, `match_resource_template` [^resources-read] [^registry] | In-process. |
| Store | `src/py/vertebra/agent/mcp/sessions.py::MCPSessionStore.update`, `subscribed_session_ids` [^sessions] | In-process. |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Agent harness](../../../../knowledge/domains/agent/mcp-and-a2a-protocol-harness.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R11.1**: Subscribe/unsubscribe validation, authorization, limits, and capability flag.
- **R11.2**: Fan-out to subscribed sessions with reverse-index cleanup.
- **R11.3**: `subscriptions/listen` streams with subscription ids, per-kind filtering, and authorization.

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```python
# models.py
class MCPConfig:  # added
    resource_subscriptions: bool = True
    max_subscriptions_per_session: int = 256
    subscription_authorizer: Callable[[AgentContext, str], bool | Awaitable[bool]] | None = None

# service.py
class MCPCore:
    async def notify_resource_updated(self, uri: str) -> int: ...

class ListenRegistration(msgspec.Struct, frozen=True, gc=False, kw_only=True):  # service.py, private to the module's public API surface via __all__ exclusion
    subscription_id: str
    owner_id: str | None
    resource_uris: frozenset[str]
    kinds: frozenset[Literal["tools", "resources", "prompts"]]
```

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `resources/subscribe` -> `_resolve_resource_uri` -> `_authorize_subscription` -> `MCPSessionStore.update(subscriptions=...)`.
- Application code -> `MCPCore.notify_resource_updated` -> `MCPSessionStore.subscribed_session_ids` + `_listen_registrations` -> `SseStreamBroker.publish`.
- `subscriptions/listen` -> `SseStreamBroker.open_stream("listen-<id>")` -> `McpHttpResponse.stream`.

### 2.4 Edge-Case & Failure-Mode Matrix

| Scenario | Trigger | Expected Behavior | Invariant |
| --- | --- | --- | --- |
| No session | Legacy subscribe without `Mcp-Session-Id` | `-32602` `"resources/subscribe requires an initialized session"` | Session-bound state |
| Unknown URI | `uri="config://missing"` | `-32602` `"Resource not found: config://missing"` | Only resolvable URIs |
| Authorizer raises | `RuntimeError` | `-32004`, nothing stored | Fail closed |
| Limit | 257th subscription | `-32602` `"Subscription limit reached"` | Bounded state |
| Modern subscribe | `2026-07-28` header | `200`, `-32601` | Legacy-only method |
| Disabled | `resource_subscriptions=False` | `200`, `-32601`; `subscribe: false` | Honest capabilities |
| Session deleted | `DELETE` then `notify_resource_updated` | Returns `0` | Cleanup |
| Listen without SSE accept | `Accept: application/json` | `406` | Content negotiation |

### 2.5 Acceptance Scenarios

- Given a legacy session subscribed to `config://app` with an open `GET` stream, when `await core.notify_resource_updated("config://app")` runs, then the stream yields one `notifications/resources/updated` frame and the call returns `1`.
- Given a `2026-07-28` client listening for `resourceSubscriptions: ["config://app"]`, when the resource updates, then its stream frame carries its subscription id in `_meta`.

### 2.6 Alternatives Considered

| Decision | Chosen | Rejected & Why |
| --- | --- | --- |
| Storage | Subscriptions on `MCPSession` plus reverse index | A separate subscription map duplicates session lifetime handling. |
| Authorization | Config callable, fail closed | Reusing tool guards ties resources to tool specs that do not exist for resources. |
| Template subscriptions | Concrete URIs only | Pattern subscriptions need wildcard fan-out semantics the spec does not define. |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/0 files documented in ch05]` — subscription usage and the authorizer hook are documented in `ch11:t01`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R11.1 | [t01](tasks/t01-subscribe-and-unsubscribe.md) | `behavior_tdd` | `test_mcp_conformance.py` |
| R11.2 | [t02](tasks/t02-resource-update-fanout.md) | `behavior_tdd` | `test_mcp_conformance.py` |
| R11.3 | [t03](tasks/t03-stateless-subscriptions-listen.md) | `behavior_tdd` | `test_mcp_conformance.py` |

## 5. Implementation Plan

- [ ] mcp-protocol-completeness:ch05:t01 — Subscribe and unsubscribe with fail-closed authorization (tasks/t01-subscribe-and-unsubscribe.md)
- [ ] mcp-protocol-completeness:ch05:t02 — Fan resource updates out with reverse-index cleanup (tasks/t02-resource-update-fanout.md)
- [ ] mcp-protocol-completeness:ch05:t03 — Add stateless subscriptions/listen streams (tasks/t03-stateless-subscriptions-listen.md)

**Chapter Recovery Checkpoint:** `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q && uv run --no-sync ruff check src/py/vertebra/agent && uv run --no-sync mypy src/py/vertebra/agent` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `c8722c76`; `0/3` tasks completed.
- **Downstream Dependents**: `ch06:t02` publishes skills list changes to listen streams that request `skills`; `ch09:t01` relays subscription notifications over stdio.

[^notify]: `src/py/vertebra/agent/mcp/service.py::MCPCore.notify_resource_updated` — Resource update fan-out over an unpopulated map.
[^resources-read]: `src/py/vertebra/agent/mcp/service.py::MCPCore._handle_resources_read` — Resource resolution (static, template, skill).
[^registry]: `src/py/vertebra/agent/mcp/registry.py::ToolRegistry.match_resource_template` — Template matching.
[^sessions]: `src/py/vertebra/agent/mcp/sessions.py::MCPSessionStore` — Session store from ch03.
