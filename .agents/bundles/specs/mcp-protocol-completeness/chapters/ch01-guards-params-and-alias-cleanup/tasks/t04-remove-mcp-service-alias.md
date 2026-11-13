---
type: Task
id: "mcp-protocol-completeness:ch01:t04"
flow_id: mcp-protocol-completeness
chapter_id: ch01-guards-params-and-alias-cleanup
title: Delete Empty MCPService Alias Subclass Across agent/mcp and CLI
description: "Remove class MCPService(MCPCore) from service.py, agent/mcp/__init__.py, and cli/ai.py, updating unit tests to canonical MCPCore."
status: stable
state: completed
commit: e22452c5
last_verified_commit: e22452c5
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
  - "mcp-protocol-completeness:ch01:t03"
files:
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/mcp/__init__.py
  - src/py/vertebra/cli/ai.py
tests:
  - src/py/tests/unit/agent/test_mcp_conformance.py
  - src/py/tests/unit/cli/test_mcp_cli.py
read_set:
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/mcp/__init__.py
  - src/py/vertebra/cli/ai.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
  - src/py/tests/unit/cli/test_mcp_cli.py
estimated_loc: 60
tags:
  - mcp
  - agent
  - cli
  - cleanup
sources:
  - id: mcp-service
    resource: "src/py/vertebra/agent/mcp/service.py::MCPService"
    title: Empty MCPService(MCPCore) alias subclass
  - id: mcp-init
    resource: src/py/vertebra/agent/mcp/__init__.py
    title: Lazy exports of vertebra.agent.mcp
  - id: cli-ai
    resource: "src/py/vertebra/cli/ai.py::serve"
    title: CLI serve command importing MCPService
---

# Task `t04`: Delete Empty MCPService Alias Subclass Across agent/mcp and CLI

## Objective & Scope Boundary

Delete the empty `MCPService(MCPCore)` subclass and update `agent/mcp/__init__.py`, `cli/ai.py`, and unit tests to `MCPCore` per [`../spec.md` §2](../spec.md) [^mcp-service] [^mcp-init] [^cli-ai].

## Bounded Read Manifest

- `src/py/vertebra/agent/mcp/{service,__init__}.py` & `src/py/vertebra/cli/ai.py`.
- `src/py/tests/unit/agent/test_mcp_conformance.py` & `src/py/tests/unit/cli/test_mcp_cli.py`.

## Contracts & Edge Cases

- Delete `class MCPService(MCPCore)` and remove `'MCPService'` from `service.__all__` and `vertebra.agent.mcp.__all__` / `_EXPORT_MAP`.
- Switch `cli/ai.py` and test fixtures in `test_mcp_conformance.py` / `test_mcp_cli.py` to `MCPCore` (`ToolRegistry` where appropriate).

## Test Matrix

- Verify `hasattr(vertebra.agent.mcp, 'MCPService')` is `False` and all conformance + CLI tests pass with `MCPCore`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/agent src/py/vertebra/cli/ai.py && uv run --no-sync mypy src/py/vertebra/agent src/py/vertebra/cli/ai.py && uv run --no-sync pytest src/py/tests/unit/agent src/py/tests/unit/cli/test_mcp_cli.py -q
```

## Acceptance Criteria

- [x] Zero `MCPService` definitions or references remain in `src/py/vertebra` or `src/py/tests`.
- [x] Chapter fast gate passes cleanly.

[^mcp-service]: `src/py/vertebra/agent/mcp/service.py::MCPService` — Empty MCPService(MCPCore) alias subclass.
[^mcp-init]: `src/py/vertebra/agent/mcp/__init__.py` — Lazy exports of vertebra.agent.mcp.
[^cli-ai]: `src/py/vertebra/cli/ai.py::serve` — CLI serve command importing MCPService.
