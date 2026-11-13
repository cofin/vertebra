---
type: Task
id: "mcp-protocol-completeness:ch04:t04"
flow_id: mcp-protocol-completeness
chapter_id: ch04-skills-and-mcp-apps-core
title: "Project SEP-1865 UI Metadata, Validate ui:// Links, and Filter App-Only Tools"
description: "Propagate UI metadata in ToolRegistry and ETag digest, add validate_ui_links and MCPCore.validate(), advertise io.modelcontextprotocol/ui, and filter app-only tools."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "mcp-protocol-completeness:ch04:t03"
files:
  - src/py/vertebra/agent/mcp/registry.py
  - src/py/vertebra/agent/mcp/webmcp.py
  - src/py/vertebra/agent/mcp/service.py
tests:
  - src/py/tests/unit/agent/test_mcp_conformance.py
read_set:
  - src/py/vertebra/agent/mcp/registry.py
  - src/py/vertebra/agent/mcp/webmcp.py
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/mcp/models.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
estimated_loc: 230
tags:
  - mcp
  - mcp-apps
  - webmcp
  - validation
sources:
  - id: registry-py
    resource: "src/py/vertebra/agent/mcp/registry.py::ToolRegistry"
    title: Tool and resource registration and ETag digest
  - id: webmcp-py
    resource: "src/py/vertebra/agent/mcp/webmcp.py::build_webmcp_manifest"
    title: Browser WebMCP manifest projection
---

# Task `t04`: Project SEP-1865 UI Metadata, Validate ui:// Links, and Filter App-Only Tools

## Objective & Scope Boundary

Complete registry ingestion, `validate_ui_links` / `MCPCore.validate()`, wire projection, and app-only filtering ([#110](https://github.com/cofin/vertebra/issues/110)) per [`../spec.md` §2](../spec.md) [^registry-py] [^webmcp-py].

## Bounded Read Manifest

- `src/py/vertebra/agent/mcp/{registry,webmcp,service,models}.py`.

## Contracts & Edge Cases

- `ToolRegistry` propagates `title`/`icons`/`meta` from decorated handlers/controllers, includes them in `etag()`, and implements `validate_ui_links(*, allowed_mime_types)` raising `ImproperConfigurationError` on missing `ui://` targets (both `_meta.ui.resourceUri` and legacy `_meta['ui/resourceUri']`), non-`ui://` schemes, or disallowed MIME types.
- `MCPCore.validate()` validates UI links, advertises `capabilities.extensions['io.modelcontextprotocol/ui'] = {'mimeTypes': [...]}` (omitted when `ui_mime_types=()`), emits `title`/`icons`/`_meta` on tool/resource wire dicts, and hides app-only tools from `tools/list` when `hide_app_only_tools=True` and client capabilities lack `UI_EXTENSION` while keeping `tools/call` enabled.
- `build_webmcp_manifest` excludes app-only tools and types `source` against `ToolRegistry | Sequence[Tool | ToolSpec]`.

## Test Matrix

- Parameterized tests in `test_mcp_conformance.py` covering dangling `ui://` `ImproperConfigurationError`, wrong MIME error, `ETag` change on `meta` edit, app-only tool hiding vs UI-capable client listing, `tools/call` on app-only tool, and WebMCP exclusion.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/agent/mcp && uv run --no-sync mypy src/py/vertebra/agent/mcp && uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q
```

## Acceptance Criteria

- [ ] `MCPCore.validate()` fails loud on broken `ui://` links and invalid MIME types.
- [ ] UI metadata projects across `tools/list`, `resources/*`, `ETag`, and app-only visibility filters.

[^registry-py]: `src/py/vertebra/agent/mcp/registry.py::ToolRegistry` — Tool and resource registration and ETag digest.
[^webmcp-py]: `src/py/vertebra/agent/mcp/webmcp.py::build_webmcp_manifest` — Browser WebMCP manifest projection.
