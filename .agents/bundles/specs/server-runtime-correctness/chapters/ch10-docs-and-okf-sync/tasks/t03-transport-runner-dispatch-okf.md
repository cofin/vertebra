---
type: Task
id: "server-runtime-correctness:ch10:t03"
chapter_id: ch10-docs-and-okf-sync
title: Sync bridge, runner, and dispatch OKF chapters to stream frames and supervisor stop
description: Update the tiered-rings-and-batch-bridge, asgi-and-wsgi-runners, and server-and-dispatch OKF chapters to describe ResponseStreamFrame, StreamSink, VRSF frames, ResponseStreamRegistry, progressive ASGI/RSGI/Python flush, removal of the ResponseSlot routing path, and the supervisor startup-failure stop.
status: draft
state: pending
priority: P2
verification_strategy: documentation_validation
depends_on:
  - "server-runtime-correctness:ch10:t02"
read_set:
  - .agents/bundles/specs/server-runtime-correctness/chapters/ch01-bridge-stream-frames/spec.md
  - .agents/bundles/specs/server-runtime-correctness/chapters/ch03-asgi-rsgi-incremental-flush/spec.md
  - .agents/bundles/specs/server-runtime-correctness/chapters/ch08-fastpath-cleanup/spec.md
  - .agents/bundles/knowledge/architecture/ipc/tiered-rings-and-batch-bridge.md
  - .agents/bundles/knowledge/architecture/worker/asgi-and-wsgi-runners.md
  - .agents/bundles/knowledge/domains/http/server-and-dispatch.md
  - .agents/skills/review-core/SKILL.md
files:
  - .agents/bundles/knowledge/architecture/ipc/tiered-rings-and-batch-bridge.md
  - .agents/bundles/knowledge/architecture/worker/asgi-and-wsgi-runners.md
  - .agents/bundles/knowledge/domains/http/server-and-dispatch.md
tests: []
estimated_loc: 150
plan_revision: 2
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
created_at: "2026-11-12T00:45:00Z"
updated_at: "2026-11-12T00:45:00Z"
commit: null
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:45:00Z"
sources:
  - id: bridge-okf
    resource: .agents/bundles/knowledge/architecture/ipc/tiered-rings-and-batch-bridge.md
    title: Tiered rings and batch bridge architecture chapter
  - id: runners-okf
    resource: .agents/bundles/knowledge/architecture/worker/asgi-and-wsgi-runners.md
    title: ASGI and WSGI runner architecture chapter
  - id: dispatch-okf
    resource: .agents/bundles/knowledge/domains/http/server-and-dispatch.md
    title: HTTP server and dispatch domain chapter
tags:
  - okf
  - streaming
---

# Task t03: Sync Bridge, Runner, and Dispatch OKF Chapters to Stream Frames and Supervisor Stop

## Objective & Scope Boundary

Rewrite affected sections of three OKF v0.2 chapters in present tense, refresh `verified` to the implementing commit, and add `sources` + footnotes for new symbols. Out of scope: worker/lanes chapters and limitations (`t04`).

## Bounded Read Manifest

- [ch01 spec](../../ch01-bridge-stream-frames/spec.md) and [ch03 spec](../../ch03-asgi-rsgi-incremental-flush/spec.md).
- The three target chapters [^bridge-okf] [^runners-okf] [^dispatch-okf].
- `.agents/skills/review-core/SKILL.md` — OKF review checklist used by `make review-check`.

## Symbol & Signature Contracts

- `tiered-rings-and-batch-bridge.md`: section "Response Stream Frames" — `ResponseStreamFrame` flags, `StreamSink` blocking contract, `BatchBridge::submit_stream_frame`; `VRSF` layout (`22`-byte header), `RingPair::{send_stream_frame,send_stream_frame_blocking,max_stream_frame_payload,drain_frames_with}`; mermaid sequence (Python send → bridge or ring → registry → hyper body).
- `asgi-and-wsgi-runners.md`: progressive flush state machine for `AsgiProtocol` (unary vs streaming, trailers, error frame, implicit final), RSGI `RsgiStreamContext`, `AsgiSend(stream_submitter=...)`, `submit_stream_frame`, buffered batch path note; rewrite the `attach_framework_fastpath` bullet so it reuses an existing `FastPath` only from `__vertebra_fastpath__` (attribute, or `state["__vertebra_fastpath__"]` for slotted Litestar apps) via `get_attached_fastpath` / `set_attached_fastpath`, with no `fastpath`, `vertebra_fastpath_bypass`, or `extensions["vertebra"].fastpath` lookups.
- `server-and-dispatch.md`: `ResponseStreamRegistry` (`deliver_blocking`, `deliver`), `BridgeCorrelation::register_pending`, `drain_worker` stream routing, removal of `handle_raw_response_slot`/`PendingRequests`, and `Supervisor` exit-code `78` stop with `startup_failed`.
- Each new symbol gets a `sources` entry (`path::Symbol`) and a matching `[^id]:` footnote.

## Edge Cases & Error Semantics

- Remove statements that describe `ShmDispatcher::handle_raw_response_slot` or `PendingRequests` as live paths.
- Keep diagrams at implementation level here (OKF), not in public docs.

## Public-Seam Test Blueprint

- **Verification category**: `documentation_validation`.

| Check | Concrete Input | Expected Output |
| --- | --- | --- |
| Review gate | `make review-check` | Exit `0` (frontmatter, links, privacy) |
| New symbols | `git grep -n "ResponseStreamRegistry\|VRSF\|submit_stream_frame" -- .agents/bundles/knowledge` | Matches in the three chapters |
| Removed paths | `git grep -n "handle_raw_response_slot\|PendingRequests" -- .agents/bundles/knowledge` | No matches |
| Footnotes | Every `[^id]` reference has a definition | Checked by `make review-check` |

## Verification

- Initial baseline: `git grep -n "handle_raw_response_slot\|PendingRequests" -- .agents/bundles/knowledge`
- Final:
  ```bash
  make review-check
  git grep -n "ResponseStreamRegistry" -- .agents/bundles/knowledge
  ```
- Planned local commit: `docs(okf): document response stream frames and supervisor startup stop`

## Acceptance Criteria

- [ ] The three chapters describe stream frames, the registry, progressive flush, and supervisor stop with v0.2 provenance (R18).
- [ ] No chapter references the removed `ResponseSlot` routing path (R18).

## Notes & Discoveries

[^bridge-okf]: `.agents/bundles/knowledge/architecture/ipc/tiered-rings-and-batch-bridge.md` — Tiered rings and batch bridge architecture chapter.
[^runners-okf]: `.agents/bundles/knowledge/architecture/worker/asgi-and-wsgi-runners.md` — ASGI and WSGI runner architecture chapter.
[^dispatch-okf]: `.agents/bundles/knowledge/domains/http/server-and-dispatch.md` — HTTP server and dispatch domain chapter.
