---
type: Spec
flow_id: mcp-protocol-completeness
chapter_id: ch04-skills-and-mcp-apps-core
title: "Chapter 04: Skills Catalog Disclosure and MCP Apps UI Metadata Core"
description: "Complete SkillCatalog filtering/projection, expose io.modelcontextprotocol/skills, and add SEP-1865 io.modelcontextprotocol/ui metadata models, decorators, validation, and projection."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
depends_on_chapters:
  - ch03-sse-streams-and-resource-subscriptions
replan_trigger: null
plan_commit: 18baadcd
last_reviewed_commit: null
updated_at: "2026-11-12T23:45:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:45:00Z"
    commit: 18baadcd
tags:
  - mcp
  - skills
  - mcp-apps
  - ui
sources:
  - id: skills-py
    resource: "src/py/vertebra/agent/mcp/skills.py::SkillCatalog"
    title: Skill catalog and filesystem skill source
  - id: mcp-service
    resource: "src/py/vertebra/agent/mcp/service.py::MCPCore._handle_skills_list"
    title: Skills and resources JSON-RPC handlers
  - id: decorators-py
    resource: "src/py/vertebra/agent/mcp/controllers.py::mcp_tool"
    title: MCP tool and resource authoring decorators
  - id: registry-py
    resource: "src/py/vertebra/agent/mcp/registry.py::ToolRegistry"
    title: Tool and resource registry
  - id: webmcp-py
    resource: "src/py/vertebra/agent/mcp/webmcp.py::build_webmcp_manifest"
    title: Browser WebMCP manifest builder
---

# Chapter 04: Skills Catalog Disclosure and MCP Apps UI Metadata Core

<!-- truth: start -->
- `SkillCatalog` adds `coerce`, `list_skills(*, tags=None, query=None)`, `get_skill(name_or_uri)`, `resource_entries()`, `add_listener`, `unregister_skill`, `refresh()`, `directory_read`, and `expose_as_prompts` ([#111](https://github.com/cofin/vertebra/issues/111)) [^skills-py].
- `MCPCore` advertises `capabilities.extensions['io.modelcontextprotocol/skills']` (removing non-standard `capabilities.skills`), paginates/filters `skills/list`, projects skills into `resources/*` and opt-in `prompts/*`, encodes binary skill assets as base64 `blob`, and publishes `notifications/skills/list_changed` [^mcp-service].
- SEP-1865 UI metadata (`McpIcon`, `title`/`icons`/`meta` on `ResourceEntry` & `ResourceTemplateEntry`, `ToolSpec.meta`, `@mcp_tool(ui_resource_uri=..., ui_visibility=...)`, `@mcp_resource`) flows through `ToolRegistry` (including `ETag` digest), `MCPCore.validate()`, `capabilities.extensions['io.modelcontextprotocol/ui']`, app-only `tools/list` filtering, and WebMCP exclusion ([#110](https://github.com/cofin/vertebra/issues/110)) [^decorators-py] [^registry-py] [^webmcp-py].
<!-- truth: end -->

## 1. Specification, Context & Provenance

`SkillCatalog` currently loads once without tag/query filtering, listeners, refresh, resource projection, or `directory_read=False` enforcement, and `MCPCore` advertises a non-standard top-level `capabilities.skills` key ([#111](https://github.com/cofin/vertebra/issues/111)) [^skills-py] [^mcp-service]. Resources and tools carry no `title`, `icons`, or `_meta`, preventing SEP-1865 MCP Apps UI linked `ui://` resources ([#110](https://github.com/cofin/vertebra/issues/110)) [^decorators-py] [^registry-py] [^webmcp-py].

## 2. Requirements, Concrete Contracts & Architecture

- **`R5a` (`t01`)**: Extend `Skill` (`title`, `tags`, `icons`, `meta`), `MCPSkillsConfig` (`expose_as_prompts: bool = False`, `list_changed: bool = True`), `SkillAccessError(VertebraError, PermissionError)`, and `SkillCatalog` (`coerce`, `list_skills`, `get_skill` supporting `name`, `skill://name`, `skill://name/SKILL.md`, `resource_entries`, `add_listener`, `unregister_skill`, `refresh`).
- **`R5b` (`t02`)**: Add `SKILLS_EXTENSION = "io.modelcontextprotocol/skills"` in `models.py`; register the skills extension in `MCPCore` (removing `capabilities.skills`), paginate/filter `skills/list`, project skill files into `resources/list` and `skill://{name}/{path}` into `resources/templates/list`, return base64 `blob` for binary skill files, honor `directory_read=False` (`-32602`), project prompts when `expose_as_prompts=True` (explicit prompts win collisions), and publish `notifications/skills/list_changed`.
- **`R5c` (`t03`)**: Add `UI_EXTENSION = "io.modelcontextprotocol/ui"`, `UI_MIME_TYPE = "text/html;profile=mcp-app"`, `McpIcon`, `MCPConfig.{ui_mime_types,hide_app_only_tools}`, `ResourceEntry` / `ResourceTemplateEntry` `title`/`icons`/`meta`, `ToolSpec.meta`, and `@tool` / `@mcp_tool(meta=..., ui_resource_uri=..., ui_visibility=...)` / `@mcp_resource` / `@resource` metadata arguments (raising `ValueError` on invalid `ui_visibility` tokens).
- **`R5d` (`t04`)**: Include tool/resource metadata in `ToolRegistry.etag()`, add `ToolRegistry.validate_ui_links(*, allowed_mime_types)` and `MCPCore.validate()` raising `ImproperConfigurationError` on dangling/non-`ui://` links (including `_meta['ui/resourceUri']`) or disallowed `ui://` MIME types, project `title`/`icons`/`_meta` in `tools/list`, `resources/list`, `resources/templates/list`, and `resources/read`, advertise `UI_EXTENSION`, hide app-only tools (`visibility == ['app']`) unless the client advertises `UI_EXTENSION` (keeping `tools/call` callable), and exclude app-only tools from `build_webmcp_manifest`.

## 3. Documentation & OKF Knowledge Plan

Skills and MCP Apps authoring are documented in `ch07` (`docs/usage/agent-protocols.md`, `docs/reference/agent.md`, `mcp-and-a2a-protocol-harness.md`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Files | Verification |
| --- | --- | --- | --- |
| `R5a` | `t01` | `agent/mcp/skills.py` | `pytest test_mcp_conformance.py` |
| `R5b` | `t02` | `agent/mcp/service.py`, `agent/mcp/models.py` | `pytest test_mcp_conformance.py` |
| `R5c` | `t03` | `agent/mcp/models.py`, `agent/core/schema.py`, `agent/mcp/controllers.py` | `pytest test_core_and_observability.py test_mcp_conformance.py` |
| `R5d` | `t04` | `agent/mcp/{registry,webmcp,service}.py` | `pytest test_mcp_conformance.py` |

## 5. Implementation Plan

- [ ] [t01-skill-catalog-disclosure.md](tasks/t01-skill-catalog-disclosure.md) — Add `SkillCatalog` filtering, URI resolution, resource projection, listeners, and `refresh()`
- [ ] [t02-core-skill-surface.md](tasks/t02-core-skill-surface.md) — Expose `io.modelcontextprotocol/skills`, binary skill blobs, prompt projection, and skill notifications in `MCPCore`
- [ ] [t03-ui-metadata-models-and-decorators.md](tasks/t03-ui-metadata-models-and-decorators.md) — Add `McpIcon`, resource/tool `meta`, and SEP-1865 decorator arguments
- [ ] [t04-registry-webmcp-and-core-ui-projection.md](tasks/t04-registry-webmcp-and-core-ui-projection.md) — Add `validate_ui_links` / `MCPCore.validate()`, UI extension projection, app-only filtering, and WebMCP exclusion

## 6. Current State & Remaining Plan Snapshot

- **Fast Chapter Gate (`< 15s`)**: `uv run --no-sync ruff check src/py/vertebra/agent && uv run --no-sync mypy src/py/vertebra/agent && uv run --no-sync pytest src/py/tests/unit/agent/test_core_and_observability.py src/py/tests/unit/agent/test_mcp_conformance.py -q`.

[^skills-py]: `src/py/vertebra/agent/mcp/skills.py::SkillCatalog` — Skill catalog and filesystem skill source.
[^mcp-service]: `src/py/vertebra/agent/mcp/service.py::MCPCore._handle_skills_list` — Skills and resources JSON-RPC handlers.
[^decorators-py]: `src/py/vertebra/agent/mcp/controllers.py::mcp_tool` — MCP tool and resource authoring decorators.
[^registry-py]: `src/py/vertebra/agent/mcp/registry.py::ToolRegistry` — Tool and resource registry.
[^webmcp-py]: `src/py/vertebra/agent/mcp/webmcp.py::build_webmcp_manifest` — Browser WebMCP manifest builder.
