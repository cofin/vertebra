---
type: Task
id: "mcp-protocol-completeness:ch06:t01"
chapter_id: ch06-skills-extension
title: Complete skill catalog disclosure
description: Add frontmatter-derived title, tags, icons, and meta to Skill, catalog filters and URI resolution, resource projection, listeners, unregister and refresh, directory-read and prompt-projection flags, SkillAccessError, and SkillCatalog.coerce.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "mcp-protocol-completeness:ch05:t03"
read_set:
  - src/py/vertebra/agent/mcp/skills.py
  - src/py/vertebra/agent/mcp/models.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
files:
  - src/py/vertebra/agent/mcp/skills.py
tests:
  - src/py/tests/unit/agent/test_mcp_conformance.py
estimated_loc: 210
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
  - id: skills
    resource: src/py/vertebra/agent/mcp/skills.py::SkillCatalog.from_config
    title: One-shot catalog loading
  - id: resource-entry
    resource: src/py/vertebra/agent/mcp/models.py::ResourceEntry
    title: Resource descriptor used for skill projection
tags:
  - skills
---

# Task t01: Complete Skill Catalog Disclosure

## Objective & Scope Boundary

`SkillCatalog` becomes a live, filterable, observable catalog that `MCPCore` and the mounts can consume [^skills] [^resource-entry]. Out of scope: protocol handlers (`t02`) and wiring (`t03`, `t04`).

## Bounded Read Manifest

- `src/py/vertebra/agent/mcp/skills.py` — `Skill`, `Skill.build`, `SkillSource`, `parse_skill_frontmatter`, `FilesystemSkillSource.load_skills`, `MCPSkillsConfig`, `SkillCatalog`.
- `src/py/vertebra/agent/mcp/models.py::ResourceEntry`.
- `src/py/tests/unit/agent/test_mcp_conformance.py::test_mcp_skills_catalog_and_integrity_error`.

## Symbol & Signature Contracts

- `Skill` gains `title: str = ""`, `tags: tuple[str, ...] = ()`, `icons: tuple[dict[str, Any], ...] = ()`, `meta: dict[str, Any] | None = None`; `FilesystemSkillSource.load_skills` fills them from frontmatter keys `title`, `tags` (list of strings), `icons` (list of mappings), `_meta` (mapping); malformed types raise `SkillIntegrityError` naming the skill and key.
- `MCPSkillsConfig` gains `expose_as_prompts: bool = False` and `list_changed: bool = True`.
- `class SkillAccessError(VertebraError, PermissionError)` (exported).
- `SkillCatalog.__init__(self, skills: Sequence[Skill] = (), telemetry: AgentTelemetryRecorder | None = None, *, directory_read: bool = True, expose_as_prompts: bool = False, list_changed: bool = True, sources: Sequence[SkillSource] = ())`.
- `SkillCatalog.from_config(config, telemetry=None)` passes the flags and keeps the filesystem source plus `config.sources` in `sources` for `refresh()`; first source wins on duplicate names (later duplicates are skipped).
- `SkillCatalog.coerce(cls, value: MCPSkillsConfig | SkillCatalog | None) -> SkillCatalog | None`: returns catalogs unchanged, builds from configs, returns `None` for `None`, raises `TypeError` otherwise.
- `list_skills(self, *, tags: Iterable[str] | None = None, query: str | None = None) -> list[Skill]`: sorted by name; `tags` requires every tag; `query` matches case-insensitively against `name`, `title`, and `description`.
- `get_skill(self, name_or_uri: str) -> Skill | None` accepts `name`, `skill://name`, and `skill://name/<path>`.
- `resource_entries(self) -> list[ResourceEntry]`: one entry per skill file, `uri=f"skill://{skill.name}/{path}"`, `name=f"{skill.name}/{path}"`, `description=skill.description` for `SKILL.md` and `""` otherwise, `mimeType=file.mime_type`, `reader=None`.
- `add_listener(self, listener: Callable[[], None]) -> None`; `register_skill` and `unregister_skill(self, name: str) -> bool` notify listeners on change.
- `refresh(self) -> bool`: reloads from `sources`, replaces the skill map, notifies and returns `True` only when names or file digests changed.
- `read_directory` raises `SkillAccessError("Directory listing disabled")` when `directory_read` is `False`.

## Edge Cases & Error Semantics

- `get_skill("skill://")` returns `None`.
- `list_skills(tags=[])` returns every skill.
- Listener exceptions propagate (fail loud).

## Public-Seam Test Blueprint

- **File**: `src/py/tests/unit/agent/test_mcp_conformance.py` (new parametrized `test_skill_catalog_disclosure`; filesystem via `tmp_path`, local-substitutable).
- Fixture: `tmp_path/"lint"/"SKILL.md"` with frontmatter `name: lint`, `description: Lint code`, `title: Linter`, `tags: [python, quality]`, `_meta: {"owner": "qa"}` and body `Run ruff.`; `tmp_path/"lint"/"logo.png"` with bytes `b"\x89PNG"`; `tmp_path/"docs"/"SKILL.md"` with `name: docs`, `description: Write docs`, `tags: [writing]`.
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Frontmatter fields | `SkillCatalog.from_config(MCPSkillsConfig(paths=(tmp_path,))).get_skill("lint")` | `title == "Linter"`, `tags == ("python", "quality")`, `meta == {"owner": "qa"}` |
| Tag filter | `list_skills(tags=["python"])` | Names `["lint"]` |
| Query filter | `list_skills(query="WRITE")` | Names `["docs"]` |
| URI forms | `get_skill("skill://lint")`, `get_skill("skill://lint/SKILL.md")` | Both return `lint` |
| Resource projection | `[e.uri for e in catalog.resource_entries()]` | `["skill://docs/SKILL.md", "skill://lint/SKILL.md", "skill://lint/logo.png"]`; PNG entry `mimeType == "image/png"` |
| Listener | `catalog.add_listener(lambda: events.append(1)); catalog.register_skill(Skill.build("x", "y", body="z"))` | `events == [1]` |
| Unregister | `unregister_skill("x")`, `unregister_skill("x")` | `True`, then `False` |
| Refresh change | Add `tmp_path/"new"/"SKILL.md"`; `refresh()` | Returns `True`; `get_skill("new") is not None` |
| Refresh no change | `refresh()` again | Returns `False` |
| Directory disabled | `SkillCatalog(directory_read=False).read_directory("skill://lint")` | Raises `SkillAccessError` |
| Coerce | `SkillCatalog.coerce("path")` | Raises `TypeError` |
| Bad tags | Frontmatter `tags: python` (string) | `SkillIntegrityError` mentioning `tags` |

## Verification

- **Red**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q -k "skill_catalog_disclosure"` fails.
- **Green**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q && uv run --no-sync ruff check src/py/vertebra/agent/mcp/skills.py && uv run --no-sync mypy src/py/vertebra/agent/mcp/skills.py`.

## Acceptance Criteria

- [ ] R12.1: filters, URI resolution, projection, listeners, refresh, flags, and coercion match the matrix.

## Notes & Discoveries

- [2026-11-12 00:30] `MCPSkillsConfig.directory_read` exists at `c8722c76` but nothing reads it.

[^skills]: `src/py/vertebra/agent/mcp/skills.py::SkillCatalog.from_config` — One-shot catalog loading.
[^resource-entry]: `src/py/vertebra/agent/mcp/models.py::ResourceEntry` — Resource descriptor used for skill projection.
