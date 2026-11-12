---
type: Task
id: "mcp-protocol-completeness:ch07:t02"
chapter_id: ch07-mcp-apps-ui-metadata
title: Carry metadata through decorators, registry, and WebMCP
description: Extend mcp_tool, mcp_resource, and resource with UI and metadata arguments, propagate them through ToolRegistry route and controller ingestion and the ETag digest, add ToolRegistry.validate_ui_links, and exclude app-only tools from WebMCP manifests.
status: draft
state: pending
priority: P2
verification_strategy: behavior_tdd
depends_on:
  - "mcp-protocol-completeness:ch07:t01"
read_set:
  - src/py/vertebra/agent/mcp/controllers.py
  - src/py/vertebra/agent/mcp/registry.py
  - src/py/vertebra/agent/mcp/webmcp.py
  - src/py/vertebra/agent/mcp/models.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
files:
  - src/py/vertebra/agent/mcp/controllers.py
  - src/py/vertebra/agent/mcp/registry.py
  - src/py/vertebra/agent/mcp/webmcp.py
tests:
  - src/py/tests/unit/agent/test_mcp_conformance.py
estimated_loc: 200
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
  - id: decorators
    resource: src/py/vertebra/agent/mcp/controllers.py::mcp_resource
    title: Resource and tool decorators
  - id: registry
    resource: src/py/vertebra/agent/mcp/registry.py::ToolRegistry.register_route_handler
    title: Route metadata ingestion and digests
  - id: webmcp
    resource: src/py/vertebra/agent/mcp/webmcp.py::build_webmcp_manifest
    title: WebMCP manifest builder
tags:
  - mcp-apps
  - registry
---

# Task t02: Carry Metadata through Decorators, Registry, and WebMCP

## Objective & Scope Boundary

Authors declare UI links and resource metadata with decorators, and the registry stores, digests, and validates them [^decorators] [^registry] [^webmcp]. Out of scope: wire projection and `MCPCore.validate` (`t03`).

## Bounded Read Manifest

- `src/py/vertebra/agent/mcp/controllers.py` — `mcp_tool`, `mcp_resource`, `resource`, `SkillController.get_tools`, `get_resources`, `get_resource_templates`.
- `src/py/vertebra/agent/mcp/registry.py` — `register_tool`, `register_resource`, `register_resource_template`, `register_route_handler`, `register_controller`.
- `src/py/vertebra/agent/mcp/webmcp.py::build_webmcp_manifest`.
- `src/py/vertebra/agent/mcp/models.py` — `McpIcon`, `UI_MIME_TYPE`.
- `src/py/tests/unit/agent/test_mcp_conformance.py::test_webmcp_manifest_projects_strictly_from_toolspec_exposure`.

## Symbol & Signature Contracts

- `controllers.py`:
  - `mcp_tool(..., meta: Mapping[str, Any] | None = None, ui_resource_uri: str | None = None, ui_visibility: Sequence[Literal["model", "app"]] | None = None)`: merges `{"ui": {"resourceUri": ..., "visibility": [...]}}` into a copy of `meta`; `ui_visibility` values outside `{"model", "app"}` raise `ValueError` at decoration; an empty `ui_visibility` omits the `visibility` key (SEP-1865: empty or absent means both); stores `"meta"` in `_mcp_tool_meta`.
  - `mcp_resource(uri, name=None, description=None, mime_type="text/plain", *, title: str = "", icons: Sequence[McpIcon] = (), meta: Mapping[str, Any] | None = None)` and `resource(...)` with the same keyword-only additions store `title`, `icons`, and `meta` in `_mcp_resource` / `_mcp_resource_template`.
  - `SkillController.get_tools`, `get_resources`, and `get_resource_templates` pass the stored values into `Tool(..., meta=...)`, `ResourceEntry(...)`, and `ResourceTemplateEntry(...)`.
- `registry.py`:
  - `register_route_handler` forwards `meta`, `title`, `icons` from decorator metadata.
  - Digest payloads in `register_tool`, `register_resource`, and `register_resource_template` include `meta`, `title`, and `icons`.
  - `validate_ui_links(self, *, allowed_mime_types: Iterable[str]) -> None`: collects every tool whose `spec.meta["ui"]["resourceUri"]` (or the legacy flat key `spec.meta["ui/resourceUri"]`) is not a registered static resource starting with `ui://`, every tool whose `ui.visibility` contains values other than `"model"` / `"app"`, every registered `ui://` resource whose `mimeType` is outside `allowed_mime_types`, and raises one `ImproperConfigurationError` listing all problems (tool names and URIs) when any exist.
- `webmcp.py::build_webmcp_manifest`: skips tools whose `spec.meta["ui"]["visibility"]` is a non-empty list without `"model"`.

## Edge Cases & Error Semantics

- `ui_resource_uri` combined with a caller `meta` that already has a `"ui"` mapping merges keys; explicit arguments win.
- Tools without `meta["ui"]` are never reported by `validate_ui_links`.

## Public-Seam Test Blueprint

- **File**: `src/py/tests/unit/agent/test_mcp_conformance.py` (new parametrized `test_mcp_ui_metadata_registry`).
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Decorator merge | `@mcp_tool(name="dash", meta={"ui": {"x": 1}}, ui_resource_uri="ui://dash", ui_visibility=["model", "app"])` on a route; `registry.register_route_handler(fn)` | `get_spec("dash").meta == {"ui": {"x": 1, "resourceUri": "ui://dash", "visibility": ["model", "app"]}}` |
| Bad visibility | `mcp_tool(ui_visibility=["robot"])` | `ValueError` |
| Empty visibility | `@mcp_tool(name="both", ui_resource_uri="ui://dash", ui_visibility=[])` | `get_spec("both").meta == {"ui": {"resourceUri": "ui://dash"}}` |
| Legacy flat key | Tool with `meta={"ui/resourceUri": "ui://missing"}`; `validate_ui_links(allowed_mime_types=[UI_MIME_TYPE])` | `ImproperConfigurationError` containing `"ui://missing"` |
| Resource metadata | `@mcp_resource("ui://dash", mime_type=UI_MIME_TYPE, title="Dashboard", meta={"ui": {"prefersBorder": True}})` registered | `get_resource("ui://dash").title == "Dashboard"`; `meta == {"ui": {"prefersBorder": True}}` |
| ETag covers meta | Register tool `t` with `meta={"a": 1}`, read `etag()`, re-register with `meta={"a": 2}` | ETags differ |
| Valid links | Tool linked to registered `ui://dash` (`UI_MIME_TYPE`); `validate_ui_links(allowed_mime_types=[UI_MIME_TYPE])` | No exception |
| Dangling link | Tool linked to `ui://missing` | `ImproperConfigurationError` message contains `"dash"` and `"ui://missing"` |
| Wrong MIME | `ui://dash` registered as `text/plain` | `ImproperConfigurationError` containing `"ui://dash"` |
| WebMCP exclusion | `@mcp_tool(webmcp=True, ui_visibility=["app"])` | Tool absent from `build_webmcp_manifest(registry).tools` |

## Verification

- **Red**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q -k "ui_metadata_registry"` fails.
- **Green**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q && uv run --no-sync ruff check src/py/vertebra/agent/mcp && uv run --no-sync mypy src/py/vertebra/agent/mcp/controllers.py src/py/vertebra/agent/mcp/registry.py src/py/vertebra/agent/mcp/webmcp.py`.

## Acceptance Criteria

- [ ] R13.2: decorator metadata reaches the registry, digests cover it, link validation reports all problems, WebMCP excludes app-only tools.

## Notes & Discoveries

- [2026-11-12 00:30] `register_route_handler` copies decorator dictionaries field by field at `c8722c76`; every new key must be forwarded explicitly.

[^decorators]: `src/py/vertebra/agent/mcp/controllers.py::mcp_resource` — Resource and tool decorators.
[^registry]: `src/py/vertebra/agent/mcp/registry.py::ToolRegistry.register_route_handler` — Route metadata ingestion and digests.
[^webmcp]: `src/py/vertebra/agent/mcp/webmcp.py::build_webmcp_manifest` — WebMCP manifest builder.
