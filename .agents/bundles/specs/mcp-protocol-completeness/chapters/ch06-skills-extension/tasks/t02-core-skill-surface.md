---
type: Task
id: "mcp-protocol-completeness:ch06:t02"
chapter_id: ch06-skills-extension
title: Expose the skills extension through MCPCore
description: Replace capabilities.skills with the io.modelcontextprotocol/skills extension, paginate and filter skills/list, project skill files into resources/list and a skill URI template, read binary skill files as blobs, honour directory_read, project skills into prompts when enabled, and publish skills and resources list changes.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "mcp-protocol-completeness:ch06:t01"
read_set:
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/mcp/models.py
  - src/py/vertebra/agent/mcp/skills.py
  - src/py/vertebra/agent/core/cursor.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
files:
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/mcp/models.py
tests:
  - src/py/tests/unit/agent/test_mcp_conformance.py
estimated_loc: 230
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
  - id: skills-handlers
    resource: src/py/vertebra/agent/mcp/service.py::MCPCore._handle_skills_list
    title: Current skills handlers and capability key
  - id: cursor
    resource: src/py/vertebra/agent/core/cursor.py::paginate_sequence
    title: Cursor pagination helper
tags:
  - skills
  - capabilities
---

# Task t02: Expose the Skills Extension through MCPCore

## Objective & Scope Boundary

`MCPCore` presents the catalog from `t01` through the standard extension capability and the existing resource and prompt surfaces [^skills-handlers] [^cursor]. Out of scope: mounts (`t03`, `t04`).

## Bounded Read Manifest

- `src/py/vertebra/agent/mcp/service.py` — `__init__`, `_server_capabilities`, `_register_handlers`, `_handle_skills_list`, `_handle_skills_get`, `_handle_resources_list`, `_handle_resources_templates_list`, `_handle_resources_read`, `_handle_resources_directory_read`, `_handle_prompts_list`, `_handle_prompts_get`, `_on_registry_change`, `_publish_to_listeners`.
- `src/py/vertebra/agent/mcp/models.py` — constants.
- `src/py/vertebra/agent/mcp/skills.py` — catalog API from `t01`.
- `src/py/vertebra/agent/core/cursor.py::paginate_sequence`.
- `src/py/tests/unit/agent/test_mcp_conformance.py`.

## Symbol & Signature Contracts

- `models.py`: `SKILLS_EXTENSION: str = "io.modelcontextprotocol/skills"` in `__all__`.
- `service.py`:
  - `__init__`: when `skill_catalog is not None and config.skills_enabled`, `register_extension(SKILLS_EXTENSION, lambda: {"listChanged": catalog.list_changed, "directoryRead": catalog.directory_read})` and `catalog.add_listener(self._on_skills_change)`; the `skills` key is removed from `_server_capabilities`; `skills/*` and `resources/directory/read` are registered only when the catalog is present and `skills_enabled`.
  - `skills/list` params `cursor`, `tags` (list of strings), `query` (string); page size `50`; items `{"name", "title", "description", "uri": f"skill://{name}/SKILL.md", "files": sorted(files), "tags": list(tags), "frontmatter"}` plus `"icons"` and `"_meta"` when non-empty; `nextCursor` when more pages exist.
  - `skills/get` resolves `name` or `uri` through `catalog.get_skill`.
  - `resources/list` appends `catalog.resource_entries()` after registry resources before pagination; `resources/templates/list` appends `{"uriTemplate": "skill://{name}/{path}", "name": "skill-file", "description": "Files inside skill packages", "mimeType": "text/markdown"}`.
  - `resources/read` for `skill://` URIs returns `{"uri", "mimeType", "text"}` when the file MIME type starts with `text/` or equals `text/markdown`, else `{"uri", "mimeType", "blob": base64}`; errors stay `-32602`.
  - `resources/directory/read` maps `SkillAccessError` to `-32602` `"Directory listing disabled"`.
  - When `catalog.expose_as_prompts`, `prompts/list` appends one prompt per skill whose name is not already registered (`{"name": skill.name, "description": skill.description, "arguments": []}`); `prompts/get` for such a name returns `{"description": skill.description, "messages": [{"role": "user", "content": {"type": "text", "text": skill.body}}]}`.
  - Private `_on_skills_change(self) -> None` publishes `notifications/skills/list_changed` (when `catalog.list_changed`) and `notifications/resources/list_changed` to session and anonymous streams and to listen registrations requesting `skills` / `resources`.

## Edge Cases & Error Semantics

- No catalog -> no extension, `skills/list` `200` / `-32601`.
- Registered prompts always win name collisions with projected skills.
- Pagination cursor errors follow `paginate_sequence` (`-32602`).

## Public-Seam Test Blueprint

- **File**: `src/py/tests/unit/agent/test_mcp_conformance.py` (extend `test_mcp_skills_catalog_and_integrity_error`; new parametrized `test_mcp_skills_extension_surface`).
- Fixture: catalog with `lint` (tags `python`, file `logo.png` bytes `b"\x89PNG"`) and `docs` (tag `writing`).
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Extension | `server/discover` | `capabilities["extensions"]["io.modelcontextprotocol/skills"] == {"listChanged": True, "directoryRead": True}`; `"skills" not in capabilities` |
| List filter | `skills/list {"tags": ["python"]}` | One item; `uri == "skill://lint/SKILL.md"` |
| Query | `skills/list {"query": "doc"}` | Names `["docs"]` |
| Resources | `resources/list` | URIs include `"skill://lint/SKILL.md"` and `"skill://lint/logo.png"` |
| Template | `resources/templates/list` | Contains `uriTemplate == "skill://{name}/{path}"` |
| Binary read | `resources/read {"uri": "skill://lint/logo.png"}` | `contents[0] == {"uri": "skill://lint/logo.png", "mimeType": "image/png", "blob": "iVBORw=="}` |
| Directory disabled | Catalog `directory_read=False`; `resources/directory/read {"uri": "skill://lint"}` | `-32602`; `"Directory listing disabled"` |
| Prompt projection | `expose_as_prompts=True`; `prompts/get {"name": "lint"}` | `messages[0].content.text == "Run ruff."` |
| Collision | Registered `PromptEntry(name="lint", description="registered")` | `prompts/list` has one `lint` with `description == "registered"` |
| Change notification | Open legacy stream; `catalog.register_skill(Skill.build("x", "y", body="z"))` | Stream yields `notifications/skills/list_changed` then `notifications/resources/list_changed` |
| No catalog | `MCPCore()`; `skills/list` modern | `200`; `-32601` |

## Verification

- **Red**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q -k "skills"` fails.
- **Green**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q && uv run --no-sync ruff check src/py/vertebra/agent/mcp && uv run --no-sync mypy src/py/vertebra/agent/mcp/service.py src/py/vertebra/agent/mcp/models.py`.

## Acceptance Criteria

- [ ] R12.2: extension capability, filters, projection, binary reads, directory flag, prompt projection, and notifications match the matrix.
- [ ] `capabilities.skills` no longer appears.

## Notes & Discoveries

- [2026-11-12 00:30] `_handle_resources_read` at `c8722c76` decodes every skill file with `errors="replace"` and labels it `text/markdown`.

[^skills-handlers]: `src/py/vertebra/agent/mcp/service.py::MCPCore._handle_skills_list` — Current skills handlers and capability key.
[^cursor]: `src/py/vertebra/agent/core/cursor.py::paginate_sequence` — Cursor pagination helper.
