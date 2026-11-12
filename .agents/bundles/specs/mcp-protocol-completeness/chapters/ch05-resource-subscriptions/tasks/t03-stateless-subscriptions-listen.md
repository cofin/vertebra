---
type: Task
id: "mcp-protocol-completeness:ch05:t03"
chapter_id: ch05-resource-subscriptions
title: Add stateless subscriptions/listen streams
description: Implement 2026-07-28 subscriptions/listen as a POST SSE stream filtered by requested notification kinds and authorized resource URIs, stamp each frame with the subscription id, and target list-changed broadcasts at session, anonymous, and matching listen streams only.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "mcp-protocol-completeness:ch05:t02"
read_set:
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/core/sse.py
  - src/py/vertebra/agent/mcp/sessions.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
files:
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/core/sse.py
tests:
  - src/py/tests/unit/agent/test_mcp_conformance.py
estimated_loc: 210
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
  - id: mcp-core
    resource: src/py/vertebra/agent/mcp/service.py::MCPCore._on_registry_change
    title: List-changed broadcast from ch04
  - id: sse-broker
    resource: src/py/vertebra/agent/core/sse.py::SseStreamBroker.open_stream
    title: Stream primitives from ch04
tags:
  - subscriptions
  - stateless
---

# Task t03: Add Stateless Subscriptions/Listen Streams

## Objective & Scope Boundary

`2026-07-28` clients receive server notifications without sessions by holding a `subscriptions/listen` POST stream [^mcp-core] [^sse-broker]. The method name, params shape, and `_meta["io.modelcontextprotocol/subscriptionId"]` follow the `litestar-mcp` `0.20.0` implementation that Litestar apps use; the coordinator confirms the shape against the published `2026-07-28` schema (open decision). Out of scope: legacy sessions (unchanged).

## Bounded Read Manifest

- `src/py/vertebra/agent/mcp/service.py` — `handle` POST branch, `_on_registry_change`, `notify_resource_updated`, `_authorize_subscription`, `_resolve_resource_uri`.
- `src/py/vertebra/agent/core/sse.py` — `open_stream`, `publish`, buffer keys.
- `src/py/vertebra/agent/mcp/sessions.py::active_session_ids`.
- `src/py/tests/unit/agent/test_mcp_conformance.py`.

## Symbol & Signature Contracts

- `sse.py::SseStreamBroker.stream_keys(self, prefix: str) -> tuple[str, ...]` returns sorted buffer keys starting with `prefix`.
- `service.py`:
  - `class ListenRegistration(msgspec.Struct, frozen=True, gc=False, kw_only=True)` with `subscription_id: str`, `owner_id: str | None`, `resource_uris: frozenset[str]`, `kinds: frozenset[Literal["tools", "resources", "prompts", "skills"]]`.
  - `MCPCore._listen_registrations: dict[str, ListenRegistration]`.
  - `subscriptions/listen` is handled in `handle` before router dispatch, modern mode only (legacy -> `200` / `-32601`): requires `Accept` with `text/event-stream` (`406` otherwise); params `notifications` mapping with optional `resourceSubscriptions: list[str]`, `toolsListChanged`, `resourcesListChanged`, `promptsListChanged`, `skillsListChanged` booleans; every URI must resolve (`-32602`) and pass `_authorize_subscription` (`-32004`) before the stream opens; registration key `listen-<subscription_id>` with `subscription_id = secrets.token_urlsafe(16)`; stream opened through `open_stream` (owner limits apply, `503` / `-32000`); the generator removes the registration and calls `close_session` on exit; response headers `Content-Type: text/event-stream`, `MCP-Protocol-Version: 2026-07-28`.
  - Private `_publish_to_listeners(self, kind: str, payload: dict[str, Any], *, uri: str | None = None) -> int` copies the payload, sets `params._meta["io.modelcontextprotocol/subscriptionId"]`, and publishes to registrations whose `kinds` contain `kind` (and, for resource updates, whose `resource_uris` contain `uri`).
  - `_on_registry_change(kind)` targets `await`-free snapshots: session ids from a synchronous `self.sessions` snapshot helper, `stream_keys("anon-")`, plus `_publish_to_listeners(kind, ...)`; it no longer calls `broadcast()` over every buffer.
  - `notify_resource_updated(uri)` adds `_publish_to_listeners("resources_updated", ..., uri=uri)` to its count; registrations listing `uri` in `resource_uris` match regardless of `kinds`.

## Edge Cases & Error Semantics

- Empty `notifications` mapping opens a stream that only receives heartbeats.
- Owner limit counts listen streams together with `GET` streams.
- A listen stream never receives notification kinds it did not request.

## Public-Seam Test Blueprint

- **File**: `src/py/tests/unit/agent/test_mcp_conformance.py` (new parametrized `test_mcp_subscriptions_listen`).
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Listen resource | POST `subscriptions/listen {"notifications": {"resourceSubscriptions": ["config://app"]}}`, header `2026-07-28`, `Accept: text/event-stream`; then `await core.notify_resource_updated("config://app")` | `200` stream; frame params `{"uri": "config://app", "_meta": {"io.modelcontextprotocol/subscriptionId": <id>}}`; notify returns `1` |
| Listen tools only | `{"toolsListChanged": true}`; register a tool, then a prompt | One `notifications/tools/list_changed` frame; no prompts frame within `0.05` s |
| Unknown URI | `resourceSubscriptions: ["config://missing"]` | `-32602` before any stream |
| Denied URI | Authorizer returning `False` | `-32004` |
| JSON accept | `Accept: application/json` | `406` |
| Legacy listen | header `2025-06-18` | `200`; `-32601` |
| Cleanup | Close the stream generator | `core._listen_registrations == {}`; `stream_keys("listen-") == ()` |
| Broadcast targeting | Legacy session stream plus a listen stream for prompts only; register a tool | Session stream receives `tools/list_changed`; listen stream receives nothing |

## Verification

- **Red**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q -k "subscriptions_listen"` fails.
- **Green**: `uv run --no-sync pytest src/py/tests/unit/agent -q && uv run --no-sync ruff check src/py/vertebra/agent && uv run --no-sync mypy src/py/vertebra/agent/mcp/service.py src/py/vertebra/agent/core/sse.py`.

## Acceptance Criteria

- [ ] R11.3: listen streams honour requested kinds and URIs, stamp subscription ids, and clean up on close.
- [ ] List-changed broadcasts reach only session, anonymous, and matching listen streams.

## Notes & Discoveries

- [2026-11-12 00:30] The listen shape mirrors `litestar-mcp` `0.20.0` (`SubscriptionManager.open`, `_meta["io.modelcontextprotocol/subscriptionId"]`); confirm against the published schema before execution.

[^mcp-core]: `src/py/vertebra/agent/mcp/service.py::MCPCore._on_registry_change` — List-changed broadcast from ch04.
[^sse-broker]: `src/py/vertebra/agent/core/sse.py::SseStreamBroker.open_stream` — Stream primitives from ch04.
