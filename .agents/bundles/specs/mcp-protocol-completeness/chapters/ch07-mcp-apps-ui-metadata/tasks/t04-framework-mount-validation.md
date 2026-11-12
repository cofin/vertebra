---
type: Task
id: "mcp-protocol-completeness:ch07:t04"
chapter_id: ch07-mcp-apps-ui-metadata
title: Validate UI links in framework mounts
description: Call MCPCore.validate after route discovery in the Starlette/FastAPI, Flask, and Django MCP mount functions so broken ui:// links fail at application startup.
status: draft
state: pending
priority: P2
verification_strategy: behavior_tdd
depends_on:
  - "mcp-protocol-completeness:ch07:t03"
read_set:
  - src/py/vertebra/extensions/starlette/mcp.py
  - src/py/vertebra/extensions/flask/mcp.py
  - src/py/vertebra/extensions/django/mcp.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
files:
  - src/py/vertebra/extensions/starlette/mcp.py
  - src/py/vertebra/extensions/flask/mcp.py
  - src/py/vertebra/extensions/django/mcp.py
tests:
  - src/py/tests/unit/agent/test_mcp_conformance.py
estimated_loc: 60
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
  - id: starlette-mount
    resource: src/py/vertebra/extensions/starlette/mcp.py::mount_mcp_routes
    title: Starlette and FastAPI mount
  - id: flask-mount
    resource: src/py/vertebra/extensions/flask/mcp.py::attach_mcp_routes
    title: Flask mount
  - id: django-mount
    resource: src/py/vertebra/extensions/django/mcp.py::attach_mcp_routes
    title: Django mount
tags:
  - mcp-apps
  - validation
---

# Task t04: Validate UI Links in Framework Mounts

## Objective & Scope Boundary

Every framework mount fails at startup on broken UI links, matching `AgentPlugin` [^starlette-mount] [^flask-mount] [^django-mount]. Out of scope: any other mount behaviour.

## Bounded Read Manifest

- `src/py/vertebra/extensions/starlette/mcp.py::mount_mcp_routes`.
- `src/py/vertebra/extensions/flask/mcp.py::attach_mcp_routes`.
- `src/py/vertebra/extensions/django/mcp.py::attach_mcp_routes`, `dispatch_mcp_request`.
- `src/py/tests/unit/agent/test_mcp_conformance.py`.

## Symbol & Signature Contracts

- Each mount calls `core.validate()` immediately after constructing `MCPCore` from the discovered registry and before registering any route; `dispatch_mcp_request` calls `validate()` whenever it builds a new cached app.

## Edge Cases & Error Semantics

- `ImproperConfigurationError` propagates unchanged; no routes are attached when validation fails.

## Public-Seam Test Blueprint

- **File**: `src/py/tests/unit/agent/test_mcp_conformance.py` (new parametrized `test_mcp_framework_mounts_validate_ui_links`, parametrized over `"starlette"`, `"flask"`, `"django"`).
- Fixture: a pre-built `MCPRegistry` containing a tool with `meta={"ui": {"resourceUri": "ui://missing"}}` passed as `registry=` (Starlette) or a decorated view (`@mcp_tool(ui_resource_uri="ui://missing")`) for Flask and Django.
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Starlette broken | `mount_starlette_mcp(SimpleNamespace(routes=[], state=SimpleNamespace()), registry=broken_registry)` | `ImproperConfigurationError`; `app.routes == []` |
| Flask broken | `attach_flask_mcp(_DummyFlask with decorated view)` | `ImproperConfigurationError`; `"vertebra_mcp_rpc" not in flask_views` |
| Django broken | `attach_django_mcp([decorated_view])` | `ImproperConfigurationError` |
| Valid mounts | Same mounts with a registered `ui://missing` resource using `UI_MIME_TYPE` | No exception; routes attached |

## Verification

- **Red**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q -k "mounts_validate_ui_links"` fails.
- **Green**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q && uv run --no-sync ruff check src/py/vertebra/extensions && uv run --no-sync mypy src/py/vertebra/extensions/starlette/mcp.py src/py/vertebra/extensions/flask/mcp.py src/py/vertebra/extensions/django/mcp.py`.

## Acceptance Criteria

- [ ] R13.4: all three framework mounts validate UI links before attaching routes.

## Notes & Discoveries

- [2026-11-12 00:30] FastAPI shares the Starlette mount function and needs no separate edit.

[^starlette-mount]: `src/py/vertebra/extensions/starlette/mcp.py::mount_mcp_routes` — Starlette and FastAPI mount.
[^flask-mount]: `src/py/vertebra/extensions/flask/mcp.py::attach_mcp_routes` — Flask mount.
[^django-mount]: `src/py/vertebra/extensions/django/mcp.py::attach_mcp_routes` — Django mount.
