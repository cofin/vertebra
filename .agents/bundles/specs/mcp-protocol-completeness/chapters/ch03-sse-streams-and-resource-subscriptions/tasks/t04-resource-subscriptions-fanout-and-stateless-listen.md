---
type: Task
id: "mcp-protocol-completeness:ch03:t04"
flow_id: mcp-protocol-completeness
chapter_id: ch03-sse-streams-and-resource-subscriptions
title: "Implement Resource Subscriptions, Update Fan-Out, and Stateless subscriptions/listen"
description: "Add authorized resources/subscribe and unsubscribe, async notify_resource_updated fan-out with session cleanup, and 2026-07-28 subscriptions/listen SSE streams."
status: stable
state: completed
commit: 8c964cc3
last_verified_commit: 8c964cc3
last_verified_at: "2026-11-13T05:02:00Z"
verification_evidence: "ruff + mypy (41 files) + pytest (251 passed)"
claimed_by: null
claimed_at: null
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "mcp-protocol-completeness:ch03:t03"
files:
  - src/py/vertebra/agent/mcp/sessions.py
  - src/py/vertebra/agent/mcp/models.py
  - src/py/vertebra/agent/mcp/service.py
tests:
  - src/py/tests/unit/agent/test_mcp_conformance.py
read_set:
  - src/py/vertebra/agent/mcp/sessions.py
  - src/py/vertebra/agent/mcp/models.py
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/mcp/registry.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
estimated_loc: 245
tags:
  - mcp
  - resources
  - subscriptions
  - sse
sources:
  - id: notify-py
    resource: "src/py/vertebra/agent/mcp/service.py::MCPCore.notify_resource_updated"
    title: Resource update notification method
  - id: resources-read
    resource: "src/py/vertebra/agent/mcp/service.py::MCPCore._handle_resources_read"
    title: Resource and template URI lookup
---

# Task `t04`: Implement Resource Subscriptions, Update Fan-Out, and Stateless subscriptions/listen

## Objective & Scope Boundary

Close [#112](https://github.com/cofin/vertebra/issues/112) with authorized `resources/subscribe`, `notify_resource_updated` fan-out, and `2026-07-28` `subscriptions/listen` per [`../spec.md` §2](../spec.md) [^notify-py] [^resources-read].

## Bounded Read Manifest

- `src/py/vertebra/agent/mcp/{sessions,models,service,registry}.py`.

## Contracts & Edge Cases

- `resources/subscribe` requires an initialized legacy session, a resolvable static/template/`skill://` URI (`-32602` on missing), passing `MCPConfig.subscription_authorizer` (`-32004` on `False` or raised exception), and `< max_subscriptions_per_session` (`256`); `resources/unsubscribe` is idempotent; `resource_subscriptions=False` disables both (`-32601`, `subscribe: False`).
- Delete dead `_resource_subscriptions`; `await MCPCore.notify_resource_updated(uri) -> int` publishes `notifications/resources/updated` to `MCPSessionStore.subscribed_session_ids(uri)` (auto-cleaned on `DELETE` and expiry) and matching `subscriptions/listen` streams.
- `2026-07-28` `subscriptions/listen` (`Accept: text/event-stream`, else `406`) authorizes requested `resourceSubscriptions` and streams matching resource/list-changed notifications stamped with `params._meta['io.modelcontextprotocol/subscriptionId']`.

## Test Matrix

- Parameterized tests in `test_mcp_conformance.py` covering subscribe/unsubscribe, unknown URI `-32602`, authorizer `False`/exception `-32004`, `257th` subscription limit, `DELETE`/expiry cleanup returning `0` deliveries, and `2026-07-28` `subscriptions/listen` `_meta` stamping.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/agent/mcp && uv run --no-sync mypy src/py/vertebra/agent/mcp && uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q
```

## Acceptance Criteria

- [x] `notify_resource_updated` delivers frames to subscribed legacy sessions and stateless `subscriptions/listen` streams.
- [x] Session expiry or `DELETE` removes reverse URI subscriptions with zero leaks.

[^notify-py]: `src/py/vertebra/agent/mcp/service.py::MCPCore.notify_resource_updated` — Resource update notification method.
[^resources-read]: `src/py/vertebra/agent/mcp/service.py::MCPCore._handle_resources_read` — Resource and template URI lookup.
