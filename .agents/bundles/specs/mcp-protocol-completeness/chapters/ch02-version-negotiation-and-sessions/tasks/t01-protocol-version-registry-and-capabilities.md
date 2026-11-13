---
type: Task
id: "mcp-protocol-completeness:ch02:t01"
flow_id: mcp-protocol-completeness
chapter_id: ch02-version-negotiation-and-sessions
title: Implement Protocol Version Negotiation and Unified Capability Builder
description: "Add SUPPORTED_PROTOCOL_VERSIONS, NegotiatedProtocol, mode detection, -32022 renegotiation payload, shared _server_capabilities, and logging/setLevel."
status: stable
state: completed
commit: 5f8fd173
last_verified_commit: 5f8fd173
last_verified_at: "2026-11-13T04:43:05Z"
verification_evidence: "ruff + mypy + pytest (206 agent + flask tests passed)"
claimed_by: null
claimed_at: null
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: []
files:
  - src/py/vertebra/agent/mcp/models.py
  - src/py/vertebra/agent/mcp/service.py
tests:
  - src/py/tests/unit/agent/test_mcp_conformance.py
read_set:
  - src/py/vertebra/agent/mcp/models.py
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/core/jsonrpc.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
estimated_loc: 225
tags:
  - mcp
  - protocol
  - capabilities
  - jsonrpc
sources:
  - id: mcp-models
    resource: "src/py/vertebra/agent/mcp/models.py::MCPConfig"
    title: Protocol constants and MCPConfig
  - id: mcp-discover
    resource: "src/py/vertebra/agent/mcp/service.py::MCPCore._handle_discover"
    title: Current server/discover capability dictionary
---

# Task `t01`: Implement Protocol Version Negotiation and Unified Capability Builder

## Objective & Scope Boundary

Implement `SUPPORTED_PROTOCOL_VERSIONS` negotiation, `register_extension`, unified `_server_capabilities`, and `logging/setLevel` ([#109](https://github.com/cofin/vertebra/issues/109)) per [`../spec.md` §2](../spec.md) [^mcp-models] [^mcp-discover].

## Bounded Read Manifest

- `src/py/vertebra/agent/mcp/{models,service}.py` & `agent/core/jsonrpc.py`.

## Contracts & Edge Cases

- Modern mode applies when header/`_meta` equals `2026-07-28`, or when both are absent with no `Mcp-Session-Id` and method not in `{'initialize', 'ping', 'notifications/initialized'}`; unsupported versions return HTTP `400` / `-32022` with `data={'supported': list(SUPPORTED_PROTOCOL_VERSIONS), 'supportedVersions': ..., 'requested': ...}`.
- Unknown methods and `LEGACY_ONLY_METHODS` in modern mode return HTTP `200` with JSON-RPC `-32601`.
- `_server_capabilities` omits disabled families (`tools_enabled=False` -> `-32601` on `tools/*`), formats standard `tasks`/`logging`/`extensions`, and registers `register_extension(name, payload)`.

## Test Matrix

- Parameterized tests in `test_mcp_conformance.py` across all 4 supported versions, unsupported `1900-01-01` (`-32022`), header/`_meta` conflict (`-32020`), `initialize` in `2026-07-28` (`200` / `-32601`), disabled capability omission, and invalid `logging/setLevel` (`-32602`).

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/agent/mcp && uv run --no-sync mypy src/py/vertebra/agent/mcp && uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q
```

## Acceptance Criteria

- [x] All 4 protocol versions negotiate accurately and echo `MCP-Protocol-Version`.
- [x] `_server_capabilities` omits disabled families and enforces `-32601`.

[^mcp-models]: `src/py/vertebra/agent/mcp/models.py::MCPConfig` — Protocol constants and MCPConfig.
[^mcp-discover]: `src/py/vertebra/agent/mcp/service.py::MCPCore._handle_discover` — Current server/discover capability dictionary.
