---
type: Task
id: "mcp-protocol-completeness:ch07:t03"
chapter_id: ch07-mcp-apps-ui-metadata
title: Project UI metadata, advertise the UI extension, and validate in AgentPlugin
description: Emit title, icons, and _meta on tools, resources, templates, and resource reads, register the io.modelcontextprotocol/ui extension from configured or detected MIME types, hide app-only tools from clients without UI capability, add MCPCore.validate, and call it from AgentPlugin mounts.
status: draft
state: pending
priority: P2
verification_strategy: behavior_tdd
depends_on:
  - "mcp-protocol-completeness:ch07:t02"
read_set:
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/plugin.py
  - src/py/vertebra/agent/mcp/registry.py
  - src/py/vertebra/agent/mcp/models.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
files:
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/plugin.py
tests:
  - src/py/tests/unit/agent/test_mcp_conformance.py
estimated_loc: 190
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
  - id: list-handlers
    resource: src/py/vertebra/agent/mcp/service.py::MCPCore._handle_tools_list
    title: Tool and resource list projection
  - id: agent-plugin
    resource: src/py/vertebra/agent/plugin.py::AgentPlugin.mount_native
    title: Native and Litestar mount entry points
tags:
  - mcp-apps
  - capabilities
---

# Task t03: Project UI Metadata, Advertise the UI Extension, and Validate in AgentPlugin

## Objective & Scope Boundary

Clients see SEP-1865 metadata on the wire and the server refuses to start with broken UI links [^list-handlers] [^agent-plugin]. Out of scope: Starlette, Flask, and Django mount validation (`t04`).

## Bounded Read Manifest

- `src/py/vertebra/agent/mcp/service.py` — `_handle_tools_list`, `_handle_resources_list`, `_handle_resources_templates_list`, `_handle_resources_read`, `register_extension`, `__init__`.
- `src/py/vertebra/agent/plugin.py` — `mount_native`, `on_app_init`.
- `src/py/vertebra/agent/mcp/registry.py::validate_ui_links`.
- `src/py/vertebra/agent/mcp/models.py` — `UI_EXTENSION`, `UI_MIME_TYPE`.
- `src/py/tests/unit/agent/test_mcp_conformance.py`.

## Symbol & Signature Contracts

- `service.py`:
  - Private `_resolved_ui_mime_types(self) -> list[str]`: `list(dict.fromkeys(config.ui_mime_types))` when configured (empty tuple disables), else `[UI_MIME_TYPE]` when any registered resource URI starts with `ui://`, else `[]`.
  - `__init__` registers `UI_EXTENSION` with payload `lambda: {"mimeTypes": self._resolved_ui_mime_types()}`; `_server_capabilities` omits extension entries whose payload callable returns `{"mimeTypes": []}` for the UI extension.
  - `def validate(self) -> None` calls `self.registry.validate_ui_links(allowed_mime_types=self._resolved_ui_mime_types() or [UI_MIME_TYPE])`.
  - `tools/list` items add `"_meta": spec.meta` when non-empty; with `hide_app_only_tools` and a client capability map lacking `extensions[UI_EXTENSION]`, tools whose `meta["ui"]["visibility"]` is a non-empty list without `"model"` are skipped (empty or absent visibility means both); `tools/call` never applies this filter.
  - `resources/list` and `resources/templates/list` items add `"title"` when non-empty, `"icons"` (`msgspec.to_builtins`) when non-empty, `"_meta"` when non-empty; `resources/read` `contents[0]` adds `"_meta"` from the entry.
- `plugin.py`: `mount_native` calls `srv.validate()` after `discover_app_routes`; `on_app_init` calls `srv.validate()` after `_discover_handlers`.

## Edge Cases & Error Semantics

- `validate()` failures propagate as `ImproperConfigurationError` from mount (fail loud at startup).
- Clients sending no capability map are treated as lacking UI capability.
- Tools without `meta["ui"]` are always listed.

## Public-Seam Test Blueprint

- **File**: `src/py/tests/unit/agent/test_mcp_conformance.py` (new parametrized `test_mcp_ui_projection_and_validation`).
- Fixture: `ResourceEntry(uri="ui://dash", name="dash", title="Dashboard", mimeType=UI_MIME_TYPE, meta={"ui": {"prefersBorder": True}}, reader=lambda: "<html></html>")`; tool `dash_tool` with `meta={"ui": {"resourceUri": "ui://dash", "visibility": ["app"]}}`; tool `plain`.
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Extension detected | `server/discover` | `capabilities["extensions"]["io.modelcontextprotocol/ui"] == {"mimeTypes": ["text/html;profile=mcp-app"]}` |
| Extension disabled | `MCPConfig(ui_mime_types=())` | No UI extension entry |
| Hidden without UI cap | Modern `tools/list` without client capabilities | Names `["plain"]` |
| Shown with UI cap | `_meta["io.modelcontextprotocol/clientCapabilities"] = {"extensions": {"io.modelcontextprotocol/ui": {}}}` | Names include `"dash_tool"` with `_meta.ui.resourceUri == "ui://dash"` |
| Call still works | `tools/call dash_tool` without UI cap | Success result |
| Resource projection | `resources/list` | Item `{"uri": "ui://dash", "name": "dash", "title": "Dashboard", "mimeType": "text/html;profile=mcp-app", "_meta": {"ui": {"prefersBorder": True}}, ...}` |
| Read meta | `resources/read {"uri": "ui://dash"}` | `contents[0]["_meta"] == {"ui": {"prefersBorder": True}}`, `text == "<html></html>"` |
| Native mount fails | Tool linked to `ui://missing`; `AgentPlugin().mount_native(App())` after registering | `ImproperConfigurationError` |

## Verification

- **Red**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q -k "ui_projection"` fails.
- **Green**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q && uv run --no-sync ruff check src/py/vertebra/agent && uv run --no-sync mypy src/py/vertebra/agent/mcp/service.py src/py/vertebra/agent/plugin.py`.

## Acceptance Criteria

- [ ] R13.3: projection, UI extension, app-only filtering, and plugin validation match the matrix.

## Notes & Discoveries

- [2026-11-12 00:30] Client UI capability detection reads `ctx.metadata["mcp.client_capabilities"]` populated in `ch03:t04`.

[^list-handlers]: `src/py/vertebra/agent/mcp/service.py::MCPCore._handle_tools_list` — Tool and resource list projection.
[^agent-plugin]: `src/py/vertebra/agent/plugin.py::AgentPlugin.mount_native` — Native and Litestar mount entry points.
