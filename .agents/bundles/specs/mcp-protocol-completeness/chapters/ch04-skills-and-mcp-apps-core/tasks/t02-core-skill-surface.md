---
type: Task
id: "mcp-protocol-completeness:ch04:t02"
flow_id: mcp-protocol-completeness
chapter_id: ch04-skills-and-mcp-apps-core
title: Expose io.modelcontextprotocol/skills Surface in MCPCore
description: "Advertise capabilities.extensions['io.modelcontextprotocol/skills'], paginate/filter skills/list, project skill resources/prompts, read binary blobs, and notify skill changes."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "mcp-protocol-completeness:ch04:t01"
files:
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/mcp/models.py
tests:
  - src/py/tests/unit/agent/test_mcp_conformance.py
read_set:
  - src/py/vertebra/agent/mcp/skills.py
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/mcp/models.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
estimated_loc: 225
tags:
  - mcp
  - skills
  - resources
  - prompts
sources:
  - id: skills-handlers
    resource: "src/py/vertebra/agent/mcp/service.py::MCPCore._handle_skills_list"
    title: MCPCore skill and resource handlers
---

# Task `t02`: Expose io.modelcontextprotocol/skills Surface in MCPCore

## Objective & Scope Boundary

Wire `SkillCatalog` into `MCPCore` under `SKILLS_EXTENSION = 'io.modelcontextprotocol/skills'` ([#111](https://github.com/cofin/vertebra/issues/111)) per [`../spec.md` §2](../spec.md) [^skills-handlers].

## Bounded Read Manifest

- `src/py/vertebra/agent/mcp/{skills,service,models}.py`.

## Contracts & Edge Cases

- Replace `capabilities.skills` with `capabilities.extensions['io.modelcontextprotocol/skills'] = {'listChanged': ..., 'directoryRead': ...}` when `skills_enabled=True` and a catalog is present; return HTTP `200` / `-32601` on `skills/*` when disabled.
- Paginate (`50`) and filter `skills/list`; include skill entries in `resources/list` and `skill://{name}/{path}` in `resources/templates/list`; encode non-UTF-8 skill files as base64 `blob` with detected MIME type; enforce `directory_read=False` (`-32602`).
- Project skills into `prompts/list` and `prompts/get` when `expose_as_prompts=True` (explicit prompts win collisions), and publish `notifications/skills/list_changed` + `notifications/resources/list_changed` on catalog updates.

## Test Matrix

- Parameterized tests in `test_mcp_conformance.py` covering extension capability shape, pagination + tag/query filters, PNG binary `blob` read, `directory_read=False` `-32602`, prompt collision precedence, and `skills/list_changed` SSE delivery.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/agent/mcp && uv run --no-sync mypy src/py/vertebra/agent/mcp && uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q
```

## Acceptance Criteria

- [ ] `capabilities.skills` is replaced by `capabilities.extensions['io.modelcontextprotocol/skills']`.
- [ ] Binary skill assets, directory-read restrictions, and prompt projection pass conformance tests.

[^skills-handlers]: `src/py/vertebra/agent/mcp/service.py::MCPCore._handle_skills_list` — MCPCore skill and resource handlers.
