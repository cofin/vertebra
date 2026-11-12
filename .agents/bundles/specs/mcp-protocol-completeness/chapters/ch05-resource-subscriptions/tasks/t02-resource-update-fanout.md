---
type: Task
id: "mcp-protocol-completeness:ch05:t02"
chapter_id: ch05-resource-subscriptions
title: Fan resource updates out with reverse-index cleanup
description: Make MCPCore.notify_resource_updated async and deliver notifications/resources/updated to every live subscribed session through a reverse URI index in MCPSessionStore that drops entries on unsubscribe, DELETE, and expiry.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "mcp-protocol-completeness:ch05:t01"
read_set:
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/mcp/sessions.py
  - src/py/vertebra/agent/core/sse.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
files:
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/mcp/sessions.py
tests:
  - src/py/tests/unit/agent/test_mcp_conformance.py
estimated_loc: 140
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
  - id: notify
    resource: src/py/vertebra/agent/mcp/service.py::MCPCore.notify_resource_updated
    title: Existing synchronous fan-out over _resource_subscriptions
  - id: sessions
    resource: src/py/vertebra/agent/mcp/sessions.py::MCPSessionStore.subscribed_session_ids
    title: Subscriber lookup
tags:
  - resources
  - notifications
---

# Task t02: Fan Resource Updates Out with Reverse-Index Cleanup

## Objective & Scope Boundary

Application code awaits `MCPCore.notify_resource_updated(uri)` and every live subscriber receives exactly one frame [^notify] [^sessions]. Out of scope: listen streams (`t03` extends the same method).

## Bounded Read Manifest

- `src/py/vertebra/agent/mcp/service.py` — `notify_resource_updated`, `_resource_subscriptions`, `_publish_to_session`.
- `src/py/vertebra/agent/mcp/sessions.py` — `update`, `delete`, `get`, expiry purge, `subscribed_session_ids`.
- `src/py/vertebra/agent/core/sse.py::SseStreamBroker.publish`.
- `src/py/tests/unit/agent/test_mcp_conformance.py`.

## Symbol & Signature Contracts

- `sessions.py::MCPSessionStore`: private `self._uri_index: dict[str, set[str]]` maintained by `update(subscriptions=...)` (diff old vs new), `delete`, and every expiry purge; `subscribed_session_ids(uri)` returns `tuple(sorted(...))` of live session ids from the index after purging expired sessions.
- `service.py`:
  - `_resource_subscriptions` is removed.
  - `async def notify_resource_updated(self, uri: str) -> int`: for each id in `await self.sessions.subscribed_session_ids(uri)` publishes `{"jsonrpc": "2.0", "method": "notifications/resources/updated", "params": {"uri": uri}}` via `_publish_to_session`; returns the number of sessions published to (frames are buffered for replay even without an open stream).

## Edge Cases & Error Semantics

- Unknown URI or no subscribers -> returns `0`.
- Session expired between subscribe and notify -> excluded and removed from the index.
- Duplicate subscribe never yields duplicate frames.

## Public-Seam Test Blueprint

- **File**: `src/py/tests/unit/agent/test_mcp_conformance.py` (new parametrized `test_mcp_resource_update_fanout`).
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| One subscriber | Session `s1` subscribed to `config://app` with an open stream; `await core.notify_resource_updated("config://app")` | Returns `1`; stream chunk JSON `{"jsonrpc": "2.0", "method": "notifications/resources/updated", "params": {"uri": "config://app"}}` |
| Two subscribers | `s1`, `s2` subscribed | Returns `2` |
| Other URI | `notify_resource_updated("config://other")` | Returns `0` |
| After unsubscribe | `s1` unsubscribes, notify | Returns `0` |
| After DELETE | `DELETE` `s1`, notify | Returns `0`; `core.sessions._uri_index.get("config://app", set()) == set()` |
| After expiry | Store clock advanced past TTL, notify | Returns `0` |
| Replay without stream | Subscribed session with no open stream; notify; then `open_stream(s1, last_event_id=None)` | First chunk is the update frame |

## Verification

- **Red**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q -k "resource_update_fanout"` fails.
- **Green**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q && uv run --no-sync ruff check src/py/vertebra/agent/mcp && uv run --no-sync mypy src/py/vertebra/agent/mcp/service.py src/py/vertebra/agent/mcp/sessions.py`.

## Acceptance Criteria

- [ ] R11.2: fan-out reaches live subscribers once and the index stays consistent across unsubscribe, `DELETE`, and expiry.
- [ ] `_resource_subscriptions` no longer exists.

## Notes & Discoveries

- [2026-11-12 00:30] `notify_resource_updated` changes from sync to async; it has no callers outside `service.py` at `c8722c76`.

[^notify]: `src/py/vertebra/agent/mcp/service.py::MCPCore.notify_resource_updated` — Existing synchronous fan-out over `_resource_subscriptions`.
[^sessions]: `src/py/vertebra/agent/mcp/sessions.py::MCPSessionStore.subscribed_session_ids` — Subscriber lookup.
