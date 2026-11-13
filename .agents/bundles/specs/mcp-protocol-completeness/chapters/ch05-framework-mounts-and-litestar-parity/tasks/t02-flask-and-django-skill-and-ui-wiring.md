---
type: Task
id: "mcp-protocol-completeness:ch05:t02"
flow_id: mcp-protocol-completeness
chapter_id: ch05-framework-mounts-and-litestar-parity
title: Wire Skills and UI Link Validation in Flask and Django MCP Mounts
description: "Pass skills=MCPSkillsConfig | SkillCatalog | None through Flask and Django MCP helpers, emit well-known skill names, and call MCPCore.validate() at mount."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "mcp-protocol-completeness:ch05:t01"
files:
  - src/py/vertebra/extensions/flask/mcp.py
  - src/py/vertebra/extensions/django/mcp.py
tests:
  - src/py/tests/unit/agent/test_mcp_conformance.py
read_set:
  - src/py/vertebra/extensions/flask/mcp.py
  - src/py/vertebra/extensions/django/mcp.py
  - src/py/vertebra/agent/mcp/routes.py
  - src/py/vertebra/agent/mcp/skills.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
estimated_loc: 140
tags:
  - mcp
  - flask
  - django
  - skills
sources:
  - id: flask-mcp
    resource: "src/py/vertebra/extensions/flask/mcp.py::attach_mcp_routes"
    title: Flask MCP route mount helper
  - id: django-mcp
    resource: "src/py/vertebra/extensions/django/mcp.py::attach_mcp_routes"
    title: Django MCP route mount helper
---

# Task `t02`: Wire Skills and UI Link Validation in Flask and Django MCP Mounts

## Objective & Scope Boundary

Bring Flask and Django MCP mounts to parity with skill catalog wiring, well-known `skills`, and mount-time `MCPCore.validate()` per [`../spec.md` §2](../spec.md) [^flask-mcp] [^django-mcp].

## Bounded Read Manifest

- `src/py/vertebra/extensions/{flask,django}/mcp.py` & `src/py/vertebra/agent/mcp/{routes,skills}.py`.

## Contracts & Edge Cases

- Flask `attach_mcp_routes(app, config=None, *, skills=None)` and Django `attach_mcp_routes(compiler_or_views=None, config=None, *, skills=None)` / `dispatch_mcp_request(..., skills=None)` coerce `skills`, populate well-known `skill_names`, and call `core.validate()` after view discovery.

## Test Matrix

- Parameterized tests in `test_mcp_conformance.py` verifying Flask and Django `skills/list`, `/.well-known/mcp.json` `skills` list, and `ImproperConfigurationError` on dangling `ui://` links.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/flask/mcp.py src/py/vertebra/extensions/django/mcp.py && uv run --no-sync mypy src/py/vertebra/extensions/flask/mcp.py src/py/vertebra/extensions/django/mcp.py && uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q
```

## Acceptance Criteria

- [ ] Flask and Django MCP mounts expose configured skills and validate `ui://` links at mount.
- [ ] Well-known discovery manifests include sorted skill names.

[^flask-mcp]: `src/py/vertebra/extensions/flask/mcp.py::attach_mcp_routes` — Flask MCP route mount helper.
[^django-mcp]: `src/py/vertebra/extensions/django/mcp.py::attach_mcp_routes` — Django MCP route mount helper.
