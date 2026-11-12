---
type: Spec
parent_flow: mcp-protocol-completeness
chapter_id: ch06-skills-extension
title: "Chapter 6: Skills Extension"
description: Complete skill catalog disclosure (filters, URI resolution, resource projection, listeners, refresh), expose skills through the io.modelcontextprotocol/skills extension with resources, templates, optional prompt projection, and list-change notifications, and wire skill catalogs through AgentPlugin and every framework mount.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch05-resource-subscriptions
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
  - id: skills
    resource: src/py/vertebra/agent/mcp/skills.py::SkillCatalog
    title: Skill catalog and filesystem source
  - id: skills-handlers
    resource: src/py/vertebra/agent/mcp/service.py::MCPCore._handle_skills_list
    title: skills/list, skills/get, and skill resource reads
  - id: agent-plugin
    resource: src/py/vertebra/agent/plugin.py::AgentPluginConfig
    title: Agent plugin configuration without skills
  - id: well-known
    resource: src/py/vertebra/agent/mcp/routes.py::build_well_known_mcp_document
    title: Canonical well-known document
  - id: litestar-well-known
    resource: src/py/vertebra/extensions/litestar/mcp/plugin.py::create_mcp_plugin
    title: Litestar well-known merge of registry tool names (done)
tags:
  - mcp
  - skills
---

# Chapter 6: Skills Extension

<!-- truth: start -->
- **Catalog**: `SkillCatalog.list_skills(*, tags=None, query=None)`, `get_skill(name_or_uri)` (`name`, `skill://name`, `skill://name/SKILL.md`), `resource_entries()`, `add_listener`, `unregister_skill`, `refresh()`, `directory_read`, `expose_as_prompts`, `list_changed`, and `SkillCatalog.coerce(MCPSkillsConfig | SkillCatalog | None)` [^skills].
- **Core surface**: `capabilities.extensions["io.modelcontextprotocol/skills"] = {"listChanged": ..., "directoryRead": ...}` replaces `capabilities.skills`; `skills/list` paginates (`50`) and filters by `tags` / `query`; `resources/list` includes skill files; `resources/templates/list` includes `skill://{name}/{path}`; binary skill files read as `blob`; `expose_as_prompts=True` projects skills into `prompts/*` with registered prompts winning collisions; catalog changes publish `notifications/skills/list_changed` and `notifications/resources/list_changed` [^skills-handlers].
- **Wiring**: `AgentPluginConfig.skills`, `mount_mcp_routes(..., skills=...)`, Flask and Django `attach_mcp_routes(..., skills=...)`, and Django `dispatch_mcp_request(..., skills=...)` pass a catalog to `MCPCore`; `build_well_known_mcp_document(..., skill_names=...)` always emits a `skills` list [^agent-plugin] [^well-known].
- **Litestar**: the Litestar well-known handler already merges registry tool names into `tools` [^litestar-well-known]; Litestar skill names come from the `litestar-mcp` catalog in `ch08`.
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`MCPCore` accepts a `skill_catalog` argument that `AgentPlugin` and every framework mount leave as `None`, so `skills/list` always returns an empty list outside hand-built services ([#111](https://github.com/cofin/vertebra/issues/111)). The catalog loads once, cannot filter, notify, or refresh, and advertises a non-standard `capabilities.skills` key. Skill files never appear in `resources/list`, binary skill files are decoded as UTF-8 with replacement characters, and `directory_read=False` in `MCPSkillsConfig` is ignored.

### 1.2 Upstream Execution Baseline

- Depends on `ch05` (listen kinds include `skills`) and `ch03:t04` (`register_extension`).

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined | Seam |
| --- | --- | --- |
| Catalog | `src/py/vertebra/agent/mcp/skills.py::SkillCatalog`, `FilesystemSkillSource`, `MCPSkillsConfig`, `Skill` [^skills] | In-process, filesystem local-substitutable (`tmp_path`). |
| Core | `src/py/vertebra/agent/mcp/service.py::MCPCore._handle_skills_list`, `_handle_skills_get`, `_handle_resources_read`, `_handle_resources_directory_read`, `_handle_discover` [^skills-handlers] | In-process. |
| Mounts | `src/py/vertebra/agent/plugin.py::AgentPluginConfig`, `src/py/vertebra/extensions/starlette/mcp.py::mount_mcp_routes`, `src/py/vertebra/extensions/flask/mcp.py::attach_mcp_routes`, `src/py/vertebra/extensions/django/mcp.py::attach_mcp_routes` [^agent-plugin] | Framework objects. |
| Well-known | `src/py/vertebra/agent/mcp/routes.py::build_well_known_mcp_document` [^well-known] | In-process. |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Agent harness](../../../../knowledge/domains/agent/mcp-and-a2a-protocol-harness.md), [Litestar extensions](../../../../knowledge/domains/extensions/litestar.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R12.1**: Catalog disclosure API, listeners, refresh, and access flags.
- **R12.2**: Core skills surface, extension capability, resource/template/prompt projection, binary reads, and change notifications.
- **R12.3**: `AgentPlugin`, Starlette/FastAPI, and well-known wiring.
- **R12.4**: Flask and Django wiring.

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```python
# skills.py
class Skill:  # added fields
    title: str = ""
    tags: tuple[str, ...] = ()
    icons: tuple[dict[str, Any], ...] = ()
    meta: dict[str, Any] | None = None
class MCPSkillsConfig:  # added
    expose_as_prompts: bool = False
    list_changed: bool = True
class SkillAccessError(VertebraError, PermissionError): ...
class SkillCatalog:
    def __init__(self, skills: Sequence[Skill] = (), telemetry: AgentTelemetryRecorder | None = None, *, directory_read: bool = True, expose_as_prompts: bool = False, list_changed: bool = True, sources: Sequence[SkillSource] = ()) -> None: ...
    @classmethod
    def coerce(cls, value: "MCPSkillsConfig | SkillCatalog | None") -> "SkillCatalog | None": ...
    def list_skills(self, *, tags: Iterable[str] | None = None, query: str | None = None) -> list[Skill]: ...
    def get_skill(self, name_or_uri: str) -> Skill | None: ...
    def resource_entries(self) -> list[ResourceEntry]: ...
    def add_listener(self, listener: Callable[[], None]) -> None: ...
    def unregister_skill(self, name: str) -> bool: ...
    def refresh(self) -> bool: ...

# models.py
SKILLS_EXTENSION: str = "io.modelcontextprotocol/skills"

# routes.py
def build_well_known_mcp_document(config: MCPConfig, registry: ToolRegistry | None = None, *, tool_names: Iterable[str] | None = None, skill_names: Iterable[str] | None = None) -> dict[str, Any]: ...

# plugin.py
class AgentPluginConfig:  # added
    skills: MCPSkillsConfig | SkillCatalog | None = None
```

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `AgentPlugin.__init__` / mount functions -> `SkillCatalog.coerce` -> `MCPCore(skill_catalog=...)`.
- `MCPCore.__init__` -> `register_extension(SKILLS_EXTENSION, ...)` and `skill_catalog.add_listener(self._on_skills_change)`.
- `_on_skills_change` -> session/anonymous streams and listen streams requesting `skills` / `resources`.

### 2.4 Edge-Case & Failure-Mode Matrix

| Scenario | Trigger | Expected Behavior | Invariant |
| --- | --- | --- | --- |
| URI forms | `get_skill("skill://lint/SKILL.md")` | Returns `lint` | One resolution rule |
| Directory read disabled | `resources/directory/read` with `directory_read=False` | `-32602` `"Directory listing disabled"` | Config honoured |
| Binary file | `skill://lint/logo.png` | `contents[0].blob` base64, `mimeType == "image/png"` | No lossy decoding |
| Prompt collision | Registered prompt `lint` and skill `lint`, `expose_as_prompts=True` | Registered prompt wins | Explicit registration wins |
| Skills disabled | `skills_enabled=False` | No extension entry; `skills/*` HTTP `200` / `-32601` | Honest capabilities |
| Refresh no change | `refresh()` with identical sources | Returns `False`, no notification | No spurious events |
| Invalid coerce input | `SkillCatalog.coerce("path")` | `TypeError` | Fail loud |

### 2.5 Acceptance Scenarios

- Given `AgentPlugin(AgentPluginConfig(skills=MCPSkillsConfig(paths=(tmp_path,))))` with one skill directory, when a client calls `skills/list`, then the skill appears with `uri == "skill://<name>/SKILL.md"` and `server/discover` lists the skills extension.
- Given an open legacy stream, when `catalog.register_skill(...)` runs, then the stream yields `notifications/skills/list_changed`.

### 2.6 Alternatives Considered

| Decision | Chosen | Rejected & Why |
| --- | --- | --- |
| Capability location | `extensions["io.modelcontextprotocol/skills"]` | Keeping `capabilities.skills` invents a top-level capability outside the spec. |
| Async sources | Keep the synchronous `SkillSource` protocol plus `refresh()` | Request-aware async sources belong to the Litestar path where `litestar-mcp` already provides them. |
| Prompt projection | Opt-in flag | Always projecting floods `prompts/list` for apps that only want resources. |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/0 files documented in ch06]` — skills configuration and extension behaviour are documented in `ch11:t01`; OKF in `ch11:t03`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R12.1 | [t01](tasks/t01-skill-catalog-disclosure.md) | `behavior_tdd` | `test_mcp_conformance.py` |
| R12.2 | [t02](tasks/t02-core-skill-surface.md) | `behavior_tdd` | `test_mcp_conformance.py` |
| R12.3 | [t03](tasks/t03-plugin-and-asgi-skill-wiring.md) | `behavior_tdd` | `test_mcp_conformance.py` |
| R12.4 | [t04](tasks/t04-wsgi-mount-skill-wiring.md) | `behavior_tdd` | `test_mcp_conformance.py` |

## 5. Implementation Plan

- [ ] mcp-protocol-completeness:ch06:t01 — Complete skill catalog disclosure (tasks/t01-skill-catalog-disclosure.md)
- [ ] mcp-protocol-completeness:ch06:t02 — Expose the skills extension through MCPCore (tasks/t02-core-skill-surface.md)
- [ ] mcp-protocol-completeness:ch06:t03 — Wire skills through AgentPlugin, Starlette, and the well-known document (tasks/t03-plugin-and-asgi-skill-wiring.md)
- [ ] mcp-protocol-completeness:ch06:t04 — Wire skills through Flask and Django mounts (tasks/t04-wsgi-mount-skill-wiring.md)

**Chapter Recovery Checkpoint:** `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q && uv run --no-sync ruff check src/py/vertebra/agent src/py/vertebra/extensions && uv run --no-sync mypy src/py/vertebra/agent src/py/vertebra/extensions/starlette/mcp.py src/py/vertebra/extensions/flask/mcp.py src/py/vertebra/extensions/django/mcp.py` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `c8722c76`; `0/4` tasks completed.
- **Downstream Dependents**: `ch08` adds Litestar skill names to the Litestar well-known document; `ch09:t02` serves skill-enabled apps over stdio.

[^skills]: `src/py/vertebra/agent/mcp/skills.py::SkillCatalog` — Skill catalog and filesystem source.
[^skills-handlers]: `src/py/vertebra/agent/mcp/service.py::MCPCore._handle_skills_list` — skills/list, skills/get, and skill resource reads.
[^agent-plugin]: `src/py/vertebra/agent/plugin.py::AgentPluginConfig` — Agent plugin configuration without skills.
[^well-known]: `src/py/vertebra/agent/mcp/routes.py::build_well_known_mcp_document` — Canonical well-known document.
[^litestar-well-known]: `src/py/vertebra/extensions/litestar/mcp/plugin.py::create_mcp_plugin` — Litestar well-known merge of registry tool names (done).
