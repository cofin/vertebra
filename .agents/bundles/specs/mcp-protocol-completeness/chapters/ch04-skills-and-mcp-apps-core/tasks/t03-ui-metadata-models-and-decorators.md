---
type: Task
id: "mcp-protocol-completeness:ch04:t03"
flow_id: mcp-protocol-completeness
chapter_id: ch04-skills-and-mcp-apps-core
title: Add SEP-1865 UI Metadata Models and Decorator Parameters
description: "Add McpIcon, UI_EXTENSION, UI_MIME_TYPE, title/icons/meta on resources and ToolSpec, and UI metadata arguments on @tool, @mcp_tool, and @mcp_resource."
status: stable
state: completed
commit: b0c0ae12
last_verified_commit: b0c0ae12
last_verified_at: "2026-11-13T05:17:00Z"
verification_evidence: "ruff + mypy (39 files) + pytest (270 passed)"
claimed_by: null
claimed_at: null
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "mcp-protocol-completeness:ch04:t02"
files:
  - src/py/vertebra/agent/mcp/models.py
  - src/py/vertebra/agent/core/schema.py
  - src/py/vertebra/agent/mcp/controllers.py
tests:
  - src/py/tests/unit/agent/test_core_and_observability.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
read_set:
  - src/py/vertebra/agent/mcp/models.py
  - src/py/vertebra/agent/core/schema.py
  - src/py/vertebra/agent/mcp/controllers.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
estimated_loc: 190
tags:
  - mcp
  - mcp-apps
  - ui
  - decorators
sources:
  - id: models-py
    resource: "src/py/vertebra/agent/mcp/models.py::ResourceEntry"
    title: ResourceEntry and ResourceTemplateEntry structs
  - id: decorators-py
    resource: "src/py/vertebra/agent/mcp/controllers.py::mcp_tool"
    title: MCP route and controller decorators
---

# Task `t03`: Add SEP-1865 UI Metadata Models and Decorator Parameters

## Objective & Scope Boundary

Introduce SEP-1865 UI metadata structs, `ToolSpec.meta`, and authoring decorator options ([#110](https://github.com/cofin/vertebra/issues/110)) per [`../spec.md` §2](../spec.md) [^models-py] [^decorators-py].

## Bounded Read Manifest

- `src/py/vertebra/agent/mcp/{models,controllers}.py` & `src/py/vertebra/agent/core/schema.py`.

## Contracts & Edge Cases

- Add `UI_EXTENSION = 'io.modelcontextprotocol/ui'`, `UI_MIME_TYPE = 'text/html;profile=mcp-app'`, `McpIcon(src, mimeType=None, sizes=())`, `MCPConfig.{ui_mime_types,hide_app_only_tools}`, `title`/`icons`/`meta` on `ResourceEntry` and `ResourceTemplateEntry`, and `meta: dict[str, Any] | None = None` on `ToolSpec`.
- `@mcp_tool` accepts `meta`, `ui_resource_uri`, and `ui_visibility` (validating tokens in `{'model', 'app'}`, omitting `visibility` when empty `[]`); `@tool` accepts `meta`; `@mcp_resource` and `@resource` accept `title`, `icons`, and `meta`.

## Test Matrix

- Parameterized tests in `test_core_and_observability.py` and `test_mcp_conformance.py` covering `McpIcon` serialization, `ui_resource_uri` + `ui_visibility` merging into `ToolSpec.meta['ui']`, empty `ui_visibility=[]` omission, and `ValueError` on invalid visibility `'robot'`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/agent/core/schema.py src/py/vertebra/agent/mcp/models.py src/py/vertebra/agent/mcp/controllers.py && uv run --no-sync mypy src/py/vertebra/agent && uv run --no-sync pytest src/py/tests/unit/agent/test_core_and_observability.py src/py/tests/unit/agent/test_mcp_conformance.py -q
```

## Acceptance Criteria

- [x] Tools, resources, and templates carry typed SEP-1865 `title`, `icons`, and `meta` fields.
- [x] Invalid `ui_visibility` values fail loud at decoration time.

[^models-py]: `src/py/vertebra/agent/mcp/models.py::ResourceEntry` — ResourceEntry and ResourceTemplateEntry structs.
[^decorators-py]: `src/py/vertebra/agent/mcp/controllers.py::mcp_tool` — MCP route and controller decorators.
