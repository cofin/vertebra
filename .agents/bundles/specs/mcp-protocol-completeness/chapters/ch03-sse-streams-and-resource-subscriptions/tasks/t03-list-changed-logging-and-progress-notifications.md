---
type: Task
id: "mcp-protocol-completeness:ch03:t03"
flow_id: mcp-protocol-completeness
chapter_id: ch03-sse-streams-and-resource-subscriptions
title: "Publish List-Changed, Level-Gated Logging, and Progress Notifications"
description: "Wire ToolRegistry mutation kinds to list_changed broadcasts, add level-gated MCPCore.log, and deliver notifications/progress over POST SSE or session streams."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "mcp-protocol-completeness:ch03:t01"
files:
  - src/py/vertebra/agent/mcp/registry.py
  - src/py/vertebra/agent/mcp/service.py
tests:
  - src/py/tests/unit/agent/test_mcp_conformance.py
read_set:
  - src/py/vertebra/agent/mcp/registry.py
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/core/context.py
  - src/py/vertebra/agent/core/sse.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
estimated_loc: 225
tags:
  - mcp
  - notifications
  - logging
  - progress
sources:
  - id: registry-py
    resource: "src/py/vertebra/agent/mcp/registry.py::ToolRegistry.add_listener"
    title: Registry mutation listener registration
  - id: progress-py
    resource: "src/py/vertebra/agent/core/context.py::ProgressReporter"
    title: Context progress callback carrier
---

# Task `t03`: Publish List-Changed, Level-Gated Logging, and Progress Notifications

## Objective & Scope Boundary

Connect `ToolRegistry` mutations, `MCPCore.log`, and `ProgressReporter` to SSE streams per [`../spec.md` §2](../spec.md) [^registry-py] [^progress-py].

## Bounded Read Manifest

- `src/py/vertebra/agent/mcp/{registry,service}.py` & `agent/core/{context,sse}.py`.

## Contracts & Edge Cases

- `ToolRegistry.add_listener` passes `kind: Literal['tools', 'resources', 'prompts']` (supporting 0-arg and 1-arg listeners); `MCPCore` broadcasts `notifications/{kind}/list_changed` when that capability family is enabled.
- `await MCPCore.log(level, data, *, logger=None) -> int` checks RFC 5424 severity ordering against each session's `log_level` (or `default_log_level` when `config.logging=True`) and publishes `notifications/message`.
- Requests carrying `params._meta.progressToken` bind a `ProgressReporter` callback: when `POST` `Accept` includes `text/event-stream`, `McpHttpResponse.stream` yields `notifications/progress` frames followed by the final JSON-RPC response event; otherwise legacy sessions receive progress on their session stream.

## Test Matrix

- Parameterized tests in `test_mcp_conformance.py` covering `tools`/`resources`/`prompts` `list_changed` frames, `logging/setLevel` severity filtering in `MCPCore.log`, POST SSE progress + final result framing, and legacy session-stream progress delivery.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/agent/mcp/registry.py src/py/vertebra/agent/mcp/service.py && uv run --no-sync mypy src/py/vertebra/agent/mcp && uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q
```

## Acceptance Criteria

- [ ] Registry mutations, `MCPCore.log`, and `_meta.progressToken` emit spec-compliant JSON-RPC notifications.
- [ ] Log messages below a session's configured severity are suppressed.

[^registry-py]: `src/py/vertebra/agent/mcp/registry.py::ToolRegistry.add_listener` — Registry mutation listener registration.
[^progress-py]: `src/py/vertebra/agent/core/context.py::ProgressReporter` — Context progress callback carrier.
