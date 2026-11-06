---
type: Task
id: mcp-webmcp-core:t01
title: Implement ToolSpec and msgspec defstruct/schema_components builder without eval
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: []
files:
- src/py/vertebra/agent/core/schema.py
- src/py/vertebra/agent/core/__init__.py
- src/py/vertebra/agent/__init__.py
tests:
- src/py/tests/unit/agent/test_core_and_observability.py
- src/py/tests/unit/agent/test_mcp_enterprise_tools.py
plan_revision: 2
plan_commit: "7657e8b7c2c96bdda6821aec70a2b603f438a127"
state_revision: 2
claimed_by: agent:flow-implement
claimed_at: '2026-11-06T21:32:00Z'
blocked_reason: null
unblock_condition: null
next_step: Completed
last_verified_at: '2026-11-06T21:39:00Z'
last_verified_commit: "6b819b6d0d5e67d1b647d7873f9e536b2c510a29"
verification_evidence: "uv run --no-sync pytest src/py/tests/unit/agent/test_core_and_observability.py src/py/tests/unit/agent/test_mcp_enterprise_tools.py -q (20 passed) && ruff check && mypy"
created_at: '2026-11-06T06:36:00Z'
updated_at: '2026-11-06T21:39:00Z'
commit: "6b819b6d0d5e67d1b647d7873f9e536b2c510a29"
tags:
- mcp
- schema
- msgspec
requirements:
- R1
- R2
---

# Implement ToolSpec and msgspec defstruct/schema_components builder without eval

## Objective

Define immutable `ToolSpec` and `ToolAnnotations` structs in `src/py/vertebra/agent/core/schema.py`, derive input and output JSON Schemas via `typing.get_type_hints(..., include_extras=True)` -> `msgspec.defstruct` -> `msgspec.json.schema_components`, coerce arguments via `msgspec.convert`, and remove all `eval()` calls.

## Context

Read `../spec.md`, `.agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md`, and `.agents/skills/msgspec/SKILL.md`. Preserve `AgentContext | None` stripping and `Provide` dependency injection in `Tool.invoke`.

## Steps

1. Write failing unit tests in `test_core_and_observability.py` and `test_mcp_enterprise_tools.py` covering `ToolSpec` construction (`exposure` defaulting to `frozenset({"mcp", "a2a"})` unless `webmcp=True` is passed), `from __future__ import annotations` forward-ref resolution, `Annotated[..., msgspec.Meta(...)]` constraints, output schema generation, `msgspec.convert` argument coercion, and verification that `eval` is absent from `schema.py`.
2. Add `ToolAnnotations(msgspec.Struct, frozen=True, gc=False)` and `ToolSpec(msgspec.Struct, frozen=True, gc=False)` to `src/py/vertebra/agent/core/schema.py` and cache resolved parameter structs/hints on `Tool`.
3. Replace manual `dataclasses`/`TypedDict` schema traversal and string `eval()` in `SchemaBuilder` and `Tool._convert_argument` with `get_type_hints(fn, include_extras=True)`, `msgspec.defstruct`, `msgspec.json.schema_components`, and `msgspec.convert`.
4. Run verification and commit `feat(agent): add ToolSpec and msgspec schema_components pipeline without eval`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/agent/test_core_and_observability.py src/py/tests/unit/agent/test_mcp_enterprise_tools.py -q
uv run --no-sync ruff check src/py/vertebra/agent/core/schema.py src/py/vertebra/agent/core/__init__.py src/py/vertebra/agent/__init__.py
uv run --no-sync mypy src/py/vertebra/agent/core/schema.py src/py/vertebra/agent/core/__init__.py src/py/vertebra/agent/__init__.py
```

## Acceptance Criteria

- [x] R1, R2: `ToolSpec` and `ToolAnnotations` are exported cleanly, input/output JSON Schemas derive through `msgspec.defstruct`/`schema_components`, `msgspec.convert` validates arguments, and zero `eval()` calls remain.

## Notes & Discoveries

- `SchemaBuilder` uses `typing.get_type_hints(fn, include_extras=True)` with `msgspec.defstruct` and `msgspec.json.schema_components`, delegating all JSON Schema 2020-12 component derivation and `$defs` emission to `msgspec` without string `eval()`.
