---
type: Spec
parent_flow: mcp-protocol-completeness
chapter_id: ch07-mcp-apps-ui-metadata
title: "Chapter 7: MCP Apps UI Metadata"
description: Add SEP-1865 metadata (title, icons, _meta) to resources, templates, and tools, carry it through decorators and the registry, validate ui:// links at mount, advertise the io.modelcontextprotocol/ui extension, and hide app-only tools from clients without UI capability and from WebMCP manifests.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch06-skills-extension
replan_trigger: null
current_task: null
plan_revision: 1
plan_commit: null
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-12T00:30:00Z"
updated_at: "2026-11-12T00:30:00Z"
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:30:00Z"
verified: []
sources:
  - id: models
    resource: src/py/vertebra/agent/mcp/models.py::ResourceEntry
    title: Resource descriptors without title, icons, or _meta
  - id: toolspec
    resource: src/py/vertebra/agent/core/schema.py::ToolSpec
    title: Tool specification without _meta
  - id: decorators
    resource: src/py/vertebra/agent/mcp/controllers.py::mcp_tool
    title: Route and controller decorators
  - id: registry
    resource: src/py/vertebra/agent/mcp/registry.py::ToolRegistry.register_route_handler
    title: Decorator metadata ingestion
  - id: webmcp
    resource: src/py/vertebra/agent/mcp/webmcp.py::build_webmcp_manifest
    title: WebMCP manifest projection
tags:
  - mcp
  - mcp-apps
  - ui
---

# Chapter 7: MCP Apps UI Metadata

<!-- truth: start -->
- **Models**: `McpIcon(src, mimeType=None, sizes=())`, `UI_EXTENSION = "io.modelcontextprotocol/ui"`, `UI_MIME_TYPE = "text/html;profile=mcp-app"`; `ResourceEntry` and `ResourceTemplateEntry` gain `title`, `icons`, and `meta` (`_meta` on the wire); `ToolSpec.meta` carries tool `_meta` including `ui.resourceUri` and `ui.visibility` [^models] [^toolspec].
- **Authoring**: `@mcp_tool(meta=..., ui_resource_uri=..., ui_visibility=...)`, `@tool(meta=...)`, and `@mcp_resource` / `@resource(title=..., icons=..., meta=...)` flow through `ToolRegistry`, which includes the metadata in its `ETag` digest [^decorators] [^registry].
- **Validation**: `MCPCore.validate()` (called by `AgentPlugin` and every framework mount after discovery) raises `ImproperConfigurationError` when `_meta.ui.resourceUri` does not name a registered `ui://` resource, when a `ui://` resource uses a MIME type outside the advertised set, or when `ui.visibility` contains values other than `"model"` and `"app"` [^registry].
- **Projection**: `tools/list`, `resources/list`, `resources/templates/list`, and `resources/read` emit `title`, `icons`, and `_meta`; `capabilities.extensions["io.modelcontextprotocol/ui"] = {"mimeTypes": [...]}` from `MCPConfig.ui_mime_types` or detected `ui://` resources; with `hide_app_only_tools=True`, `tools/list` omits tools whose visibility excludes `"model"` unless the client advertises the UI extension, while `tools/call` stays callable; WebMCP manifests exclude app-only tools [^webmcp].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

MCP Apps (SEP-1865) describe interactive UI as `ui://` resources linked from tool `_meta.ui`. Vertebra resources and tools carry no `title`, `icons`, or `_meta`, decorators cannot express them, and nothing advertises the UI extension ([#110](https://github.com/cofin/vertebra/issues/110)). Litestar apps receive SEP-1865 support from `litestar-mcp>=0.20.0`; this chapter brings the native, Starlette, FastAPI, Flask, and Django paths to parity using the same field names.

### 1.2 Upstream Execution Baseline

- Depends on `ch06` (`register_extension` is shared with the skills extension) and `ch03:t04` (client capability capture).

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined | Seam |
| --- | --- | --- |
| Models | `src/py/vertebra/agent/mcp/models.py::ResourceEntry`, `ResourceTemplateEntry` [^models] | In-process. |
| Tools | `src/py/vertebra/agent/core/schema.py::ToolSpec`, `Tool.__init__`, `tool` [^toolspec] | In-process. |
| Decorators | `src/py/vertebra/agent/mcp/controllers.py::mcp_tool`, `mcp_resource`, `resource`, `SkillController.get_resources` [^decorators] | In-process. |
| Registry | `src/py/vertebra/agent/mcp/registry.py::ToolRegistry.register_route_handler`, `register_tool`, `register_resource` [^registry] | In-process. |
| WebMCP | `src/py/vertebra/agent/mcp/webmcp.py::build_webmcp_manifest` [^webmcp] | In-process. |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Agent harness](../../../../knowledge/domains/agent/mcp-and-a2a-protocol-harness.md), [`typescript-client`](../../../../../skills/typescript-client/SKILL.md) (WebMCP manifest consumer).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R13.1**: Metadata models and tool `_meta`.
- **R13.2**: Decorator and registry propagation, digest coverage, link validation primitives, WebMCP exclusion.
- **R13.3**: Core projection, UI extension capability, app-only filtering, and plugin validation.
- **R13.4**: Framework mount validation.

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```python
# models.py
UI_EXTENSION: str = "io.modelcontextprotocol/ui"
UI_MIME_TYPE: str = "text/html;profile=mcp-app"
class McpIcon(FrozenBaseStruct, frozen=True, kw_only=True):
    src: str
    mimeType: str | None = None
    sizes: tuple[str, ...] = ()
class MCPConfig:  # added
    ui_mime_types: tuple[str, ...] | None = None
    hide_app_only_tools: bool = True

# schema.py
class ToolSpec:  # added
    meta: dict[str, Any] | None = None
def tool(..., meta: Mapping[str, Any] | None = None) -> Callable[[Callable[..., Any]], Tool]: ...

# controllers.py
def mcp_tool(name=None, description=None, *, title=None, requires_confirmation=False, timeout_seconds=None, annotations=None, webmcp=False, meta: Mapping[str, Any] | None = None, ui_resource_uri: str | None = None, ui_visibility: Sequence[Literal["model", "app"]] | None = None) -> Callable[[Callable[..., Any]], Callable[..., Any]]: ...
def mcp_resource(uri: str, name: str | None = None, description: str | None = None, mime_type: str = "text/plain", *, title: str = "", icons: Sequence[McpIcon] = (), meta: Mapping[str, Any] | None = None) -> Callable[[Callable[..., Any]], Callable[..., Any]]: ...

# registry.py
class ToolRegistry:
    def validate_ui_links(self, *, allowed_mime_types: Iterable[str]) -> None: ...

# service.py
class MCPCore:
    def validate(self) -> None: ...
```

### 2.3 Caller -> Seam -> Collaborator Integration Map

- Decorators -> `ToolRegistry.register_route_handler` / `register_controller` -> `ToolSpec.meta`, `ResourceEntry.meta`.
- `AgentPlugin.mount_native` / `on_app_init`, `mount_mcp_routes`, Flask / Django `attach_mcp_routes` -> `MCPCore.validate` -> `ToolRegistry.validate_ui_links`.
- `MCPCore._handle_tools_list` -> app-only filter using `ctx.metadata["mcp.client_capabilities"]`.

### 2.4 Edge-Case & Failure-Mode Matrix

| Scenario | Trigger | Expected Behavior | Invariant |
| --- | --- | --- | --- |
| Dangling link | `ui_resource_uri="ui://missing"` | `ImproperConfigurationError` naming the tool at mount | Fail loud |
| Non-`ui://` link | `ui_resource_uri="https://x"` | `ImproperConfigurationError` | Links target `ui://` |
| Bad visibility | `ui_visibility=["robot"]` | `ValueError` at decoration | Validated input |
| Empty visibility | `ui_visibility=[]` | `visibility` key omitted (visible to model and app) | SEP-1865 default |
| Legacy flat link | `_meta["ui/resourceUri"] = "ui://missing"` | `ImproperConfigurationError` at mount | Legacy key validated |
| Wrong MIME | `ui://dash` with `text/plain` | `ImproperConfigurationError` | Advertised MIME set |
| App-only hidden | Visibility `["app"]`, client without UI extension | Omitted from `tools/list`; `tools/call` works | Model surface stays clean |
| App-only shown | Client advertises `extensions["io.modelcontextprotocol/ui"]` | Listed with `_meta.ui` | Hosts filter for apps |
| ETag | Change only a tool's `meta` | `ETag` changes | Cache correctness |
| Explicit empty MIME list | `ui_mime_types=()` | No UI extension advertised | Opt-out |

### 2.5 Acceptance Scenarios

- Given a `ui://dashboard` resource and a tool linked to it, when a UI-capable client lists tools and reads the resource, then the tool carries `_meta.ui.resourceUri == "ui://dashboard"` and the read returns HTML with the resource `_meta`.
- Given a tool linked to `ui://missing`, when `AgentPlugin.mount_native(app)` runs, then it raises `ImproperConfigurationError`.

### 2.6 Alternatives Considered

| Decision | Chosen | Rejected & Why |
| --- | --- | --- |
| Metadata carrier | Generic `meta` dict plus typed helper arguments | A typed UI struct per field duplicates an evolving extension schema. |
| Validation time | Explicit `validate()` at mount | Validating in `register_tool` fails when tools register before their resources. |
| App-only filtering | Client-capability based | Always hiding app-only tools breaks UI-capable hosts. |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/0 files documented in ch07]` — MCP Apps authoring is documented in `ch11:t01`; field-name provenance is recorded in the limitations chapter (`ch11:t03`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R13.1 | [t01](tasks/t01-ui-metadata-models.md) | `behavior_tdd` | `test_mcp_conformance.py` |
| R13.2 | [t02](tasks/t02-decorator-and-registry-metadata.md) | `behavior_tdd` | `test_mcp_conformance.py` |
| R13.3 | [t03](tasks/t03-ui-projection-and-plugin-validation.md) | `behavior_tdd` | `test_mcp_conformance.py` |
| R13.4 | [t04](tasks/t04-framework-mount-validation.md) | `behavior_tdd` | `test_mcp_conformance.py` |

## 5. Implementation Plan

- [ ] mcp-protocol-completeness:ch07:t01 — Add UI metadata models and tool _meta (tasks/t01-ui-metadata-models.md)
- [ ] mcp-protocol-completeness:ch07:t02 — Carry metadata through decorators, registry, and WebMCP (tasks/t02-decorator-and-registry-metadata.md)
- [ ] mcp-protocol-completeness:ch07:t03 — Project UI metadata, advertise the UI extension, and validate in AgentPlugin (tasks/t03-ui-projection-and-plugin-validation.md)
- [ ] mcp-protocol-completeness:ch07:t04 — Validate UI links in framework mounts (tasks/t04-framework-mount-validation.md)

**Chapter Recovery Checkpoint:** `uv run --no-sync pytest src/py/tests/unit/agent -q && uv run --no-sync ruff check src/py/vertebra/agent src/py/vertebra/extensions && uv run --no-sync mypy src/py/vertebra/agent src/py/vertebra/extensions/starlette/mcp.py src/py/vertebra/extensions/flask/mcp.py src/py/vertebra/extensions/django/mcp.py` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `c8722c76`; `0/4` tasks completed.
- **Downstream Dependents**: `ch11:t01` documents MCP Apps authoring.

[^models]: `src/py/vertebra/agent/mcp/models.py::ResourceEntry` — Resource descriptors without title, icons, or `_meta`.
[^toolspec]: `src/py/vertebra/agent/core/schema.py::ToolSpec` — Tool specification without `_meta`.
[^decorators]: `src/py/vertebra/agent/mcp/controllers.py::mcp_tool` — Route and controller decorators.
[^registry]: `src/py/vertebra/agent/mcp/registry.py::ToolRegistry.register_route_handler` — Decorator metadata ingestion.
[^webmcp]: `src/py/vertebra/agent/mcp/webmcp.py::build_webmcp_manifest` — WebMCP manifest projection.
