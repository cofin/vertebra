---
type: Task
id: "mcp-protocol-completeness:ch07:t01"
chapter_id: ch07-mcp-apps-ui-metadata
title: Add UI metadata models and tool _meta
description: Add McpIcon, UI_EXTENSION, UI_MIME_TYPE, and title, icons, and meta fields on ResourceEntry and ResourceTemplateEntry, plus ToolSpec.meta threaded through Tool and the tool decorator.
status: draft
state: pending
priority: P2
verification_strategy: behavior_tdd
depends_on:
  - "mcp-protocol-completeness:ch06:t04"
read_set:
  - src/py/vertebra/agent/mcp/models.py
  - src/py/vertebra/agent/core/schema.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
  - src/py/tests/unit/agent/test_core_and_observability.py
files:
  - src/py/vertebra/agent/mcp/models.py
  - src/py/vertebra/agent/core/schema.py
tests:
  - src/py/tests/unit/agent/test_core_and_observability.py
estimated_loc: 110
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
  - id: models
    resource: src/py/vertebra/agent/mcp/models.py::ResourceEntry
    title: Resource descriptor fields
  - id: toolspec
    resource: src/py/vertebra/agent/core/schema.py::ToolSpec
    title: Tool specification struct and Tool constructor
tags:
  - mcp-apps
  - models
---

# Task t01: Add UI Metadata Models and Tool _meta

## Objective & Scope Boundary

Data structures can express SEP-1865 metadata [^models] [^toolspec]. Out of scope: decorators and registry propagation (`t02`), wire projection (`t03`).

## Bounded Read Manifest

- `src/py/vertebra/agent/mcp/models.py` — `ResourceEntry`, `ResourceTemplateEntry`, `MCPConfig`, `__all__`.
- `src/py/vertebra/agent/core/schema.py` — `ToolSpec`, `Tool.__init__` (spec construction), `tool`.
- `src/py/tests/unit/agent/test_mcp_conformance.py` (read-only patterns).
- `src/py/tests/unit/agent/test_core_and_observability.py::test_toolspec_and_msgspec_schema_components_without_eval`.

## Symbol & Signature Contracts

- `models.py`: `UI_EXTENSION`, `UI_MIME_TYPE`, `class McpIcon(FrozenBaseStruct, frozen=True, kw_only=True)` with `src: str`, `mimeType: str | None = None`, `sizes: tuple[str, ...] = ()`; `ResourceEntry` / `ResourceTemplateEntry` gain `title: str = ""`, `icons: tuple[McpIcon, ...] = ()`, `meta: dict[str, Any] | None = None` (field docstrings; `meta` serializes as `_meta` in `t03`); `MCPConfig.ui_mime_types: tuple[str, ...] | None = None`, `MCPConfig.hide_app_only_tools: bool = True`.
- `schema.py`: `ToolSpec.meta: dict[str, Any] | None = None`; `Tool.__init__(..., meta: Mapping[str, Any] | None = None)` stores `dict(meta)` into the spec (copied; `None` stays `None`); `tool(..., meta: Mapping[str, Any] | None = None)` forwards it.

## Edge Cases & Error Semantics

- `meta={}` is stored as `{}` and projected as an empty `_meta` only when non-empty (`t03`).
- Mutating the caller's mapping after decoration does not change the spec.

## Public-Seam Test Blueprint

- **File**: `src/py/tests/unit/agent/test_core_and_observability.py` (new parametrized `test_toolspec_meta_and_resource_metadata`).
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Tool meta | `@tool(name="dash", meta={"ui": {"resourceUri": "ui://dash"}})` | `dash.spec.meta == {"ui": {"resourceUri": "ui://dash"}}` |
| Copy | Decorate with `m = {"a": 1}`; then `m["a"] = 2` | `spec.meta == {"a": 1}` |
| Default | `@tool(name="plain")` | `spec.meta is None` |
| Resource fields | `ResourceEntry(uri="ui://dash", name="dash", title="Dashboard", mimeType=UI_MIME_TYPE, icons=(McpIcon(src="https://x/i.png", sizes=("48x48",)),), meta={"ui": {"prefersBorder": True}})` | Fields round-trip; `UI_MIME_TYPE == "text/html;profile=mcp-app"` |
| Template fields | `ResourceTemplateEntry(uriTemplate="ui://dash/{id}", name="d", title="T")` | `title == "T"`, `icons == ()`, `meta is None` |
| Config defaults | `MCPConfig()` | `ui_mime_types is None`, `hide_app_only_tools is True` |

## Verification

- **Red**: `uv run --no-sync pytest src/py/tests/unit/agent/test_core_and_observability.py -q -k "toolspec_meta"` fails.
- **Green**: `uv run --no-sync pytest src/py/tests/unit/agent -q && uv run --no-sync ruff check src/py/vertebra/agent/mcp/models.py src/py/vertebra/agent/core/schema.py && uv run --no-sync mypy src/py/vertebra/agent/mcp/models.py src/py/vertebra/agent/core/schema.py`.

## Acceptance Criteria

- [ ] R13.1: models, constants, and tool `meta` match the matrix.

## Notes & Discoveries

- [2026-11-12 00:30] Field names (`resourceUri`, `visibility`, `prefersBorder`, `csp`) and the `text/html;profile=mcp-app` MIME type match `litestar-mcp` `0.20.0` and the MCP Apps host rules (empty or absent `visibility` means both; the legacy flat key `_meta["ui/resourceUri"]` is still read by hosts).

[^models]: `src/py/vertebra/agent/mcp/models.py::ResourceEntry` — Resource descriptor fields.
[^toolspec]: `src/py/vertebra/agent/core/schema.py::ToolSpec` — Tool specification struct and Tool constructor.
