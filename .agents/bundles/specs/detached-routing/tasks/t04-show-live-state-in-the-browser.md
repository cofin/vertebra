---
type: Task
id: t04
chapter_id: null
title: "Show live state in the browser"
description: "Show server state reaching a browser and document the transport supplied by the application."
status: stable
state: completed
priority: normal
verification_strategy: "Read initial and updated snapshots from the documented native event stream"
worksheet_format: compact
depends_on: []
read_set:
  - src/py/vertebra/framework/live.py
  - src/py/vertebra/framework/_app.py
  - src/py/vertebra/server/_dispatch.py
  - src/py/tests/unit/server/test_asgi_sse.py
  - src/ts/vertebra-client/src/live/index.ts
  - src/ts/vertebra-client/tests/live.test.ts
  - src/py/tests/unit/framework/test_live_state.py
  - docs/examples/live_state_and_ssr.py
  - docs/usage/queues-and-realtime.md
  - docs/usage/typescript-sdk.md
  - .agents/bundles/knowledge/domains/framework/live-state.md
files:
  - docs/examples/live_state_and_ssr.py
  - docs/usage/queues-and-realtime.md
  - docs/usage/typescript-sdk.md
  - .agents/bundles/knowledge/domains/framework/live-state.md
tests: []
estimated_loc: 155
plan_revision: 4
plan_commit: d48d6c86464c83fa98b289216dafe28a190f439e
commit: a5c80d8cdcc047a41f34cac0a69507e053b9b57e
sources:
  - id: live-server
    resource: src/py/vertebra/framework/live.py
    title: "Live-state room and action APIs"
  - id: live-client
    resource: src/ts/vertebra-client/src/live/index.ts
    title: "Transport-neutral client store"
tags: [live-state, sse, documentation]
---

# Show live state in the browser

## Objective & Scope Boundary

Implement [R4](../spec.md#r4-live-state-and-the-ui) with a browser page that receives a live server update. Keep the existing runtime and client interfaces.

## Bounded Read Manifest

Read `LiveStateRoom`, the native streaming response path, `LiveStateClient`, and the linked guides. Check the Python and client frame tests before documenting the wire format.

## Contracts & Edge Cases

- Retain the existing snippet file. Expose `app = App(route_handlers=[counter_page, counter_events])` with standalone handlers for `GET /live` and `GET /live/events`. Serve the page as HTML and return the existing native streaming tuple `(status, headers, async_iterator)` for SSE. Document `vtb run docs.examples.live_state_and_ssr:app` from the repository root.
- Each stream owns a counter and room. It yields the initial snapshot, invokes the `@live_action` increment on timer ticks, and calls `LiveStateRoom.broadcast_pending()` to drain changes. An emitted change becomes a JSON snapshot in an SSE data event. Show the optional `publish_fn` as the hook for delivering encoded frames to an application transport. The page uses a same-origin `EventSource`, renders values using `textContent`, and closes the source after a designated completion event from the bounded stream.
- Explain the browser example's JSON projection, per-subscription ownership, and the room's binary MessagePack interface for applications that wire a shared transport. Clarify that Inertia provides page props and that live updates use the explicitly assembled transport.
- In the SDK guide remove the unsupported `url` option. Show a UI subscription and how a transport calls `applyServerDelta` with a decoded `StateDeltaMessage`, including `bigint` fields. Keep the TypeScript state name aligned with the Python class name whenever they exchange deltas. Describe `onSend` and `connected` only as client options.

## Test Matrix

The harness in [t05](t05-rewrite-examples-as-applications.md) imports the app and drives the native streaming entry point. It verifies the SSE content type, the initial snapshot, an updated value, and completion. A fresh subscription starts with its own state. The finish smoke opens the documented page through `vtb run` and checks the rendered value.

## Verification

Run the chapter gate. Use the realtime and TypeScript guides to verify the protocol and client method names against their actual signatures.

## Acceptance Criteria

- The published page visibly receives a server push through the documented event stream.
- Readers can identify the app's transport, the Python room's binary frame format, and the client API for decoded messages.

[^live-server]: `src/py/vertebra/framework/live.py` provides server state and publishing.
[^live-client]: `src/ts/vertebra-client/src/live/index.ts` provides the browser state store.
